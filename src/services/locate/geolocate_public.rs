//! Contains the main geolocalization service.
//!
//! To geolocate a request `Locator` first tries to locate based on the
//! surrounding WiFi networks.
//! A weight is determined by the WiFi signal strength reported by the client.
//! The center of the bounding boxes of the networks are queried and the
//! center position is averaged based on the weight.
//!
//! At least two WiFi networks have to been known to accurately determine the
//! position.
//! If this is not the case the position of the current cell tower is returned.
//!
//! If the cell tower is not known to `Locator` the location is estimated
//! using the client's ip.
//!
//! WiFi networks are ignored if the bounding box if spans more than CONFIG.radius_wifi_detection (500m)
//! to filter out moving access points.

use std::collections::HashSet;

use actix_web::{HttpRequest, HttpResponse, post, web};
use chrono::{DateTime, Utc};
use log::{debug, error};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::json;

use super::dbscan::{check_outlier, detect_outliers};
use crate::services::locate::dbscan::Point;
use crate::{
    CONFIG,
    constants::{Collection, SIGNAL_DROP_COEFFICIENT},
    db::{
        model::CellRadio,
        t38::{fget_wifi_many_from_pipeline, track::get_wifi_track_one},
        transmitter::TransmitterLocation,
    },
    error::{ApiError, create_error_response},
    lbs::{
        model::Cell,
        yandex::{
            detect_yandex_outliers, estimate_location_by_yandex_responses,
            yandex_lbs_request_by_individual_wifi,
        },
        yandex_cell::get_cell,
    },
    services::{
        helper::custom_deserialize::{date_time_utc_from_str, default_timestamp_ms, mac_address},
        rate_limiter::RateLimitersApp,
        submission::{geosubmit_public::PositionPublic, report::is_valid_gps_relative_cell},
    },
    tasks::{t38::T38ConnectionManageMessage, yandex::YandexApiMessage},
};

/// Serde representation of the client's request
#[allow(unused)]
#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
struct LocationRequestPublic {
    #[serde(deserialize_with = "date_time_utc_from_str")]
    #[serde(default = "default_timestamp_ms")]
    timestamp: DateTime<Utc>,

    device_id: Option<String>,
    gnss: Option<PositionPublic>,
    /// List of cell towers around the client
    #[serde(default)]
    cell: Option<Cell>,

    /// List of access points around the client
    #[serde(default)]
    wifi: Vec<AccessPoint>,

    /// Whether using the client's ip address to locate is allowed
    consider_ip: Option<bool>,
    fallbacks: Option<FallbackOptions>,
}

#[allow(unused)]
#[derive(Debug, Deserialize, Default)]
struct FallbackOptions {
    ipf: Option<bool>,
}

#[allow(unused)]
// Serde representation of cell towers in the client's request
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
struct CellTower {
    #[serde(alias = "type")]
    radio_type: CellRadio,
    mcc: i16,
    mnc: i16,
    lac: i32,
    cid: i64,
    psc: Option<i16>,
    #[serde(flatten)]
    extra: Value,
}

// Serde representation of access points in the client's request
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct AccessPoint {
    #[serde(deserialize_with = "mac_address")]
    mac: String,
    #[serde(rename = "type")]
    radio_type: Option<String>,
    age: Option<i32>,
    channel: Option<u16>,
    frequency: Option<f64>,
    rssi: Option<f64>,
    snr: Option<f64>,
    ssid: Option<String>,
    #[serde(flatten)]
    extra: Value,
}

/// Struct for representing the server's response
#[derive(Debug, Serialize)]
struct LocationResponsePublic {
    location: LocationPublic,
    accuracy: i64,
}

impl LocationResponsePublic {
    /// Create a new location response from a position and an accuracy.
    fn new(lat: f64, lon: f64, accuracy: f64) -> Self {
        // round to 6 decimal places
        let lat = (lat * 1_000_000.0).round() / 1_000_000.0;
        let lon = (lon * 1_000_000.0).round() / 1_000_000.0;

        LocationResponsePublic {
            location: LocationPublic {
                latitude: lat,
                longitude: lon,
            },
            accuracy: (accuracy.round() as i64),
        }
    }

    /// Convert the response into a HTTP response
    fn respond(self) -> actix_web::Result<HttpResponse> {
        if self.location.latitude.is_nan() || self.location.longitude.is_nan() {
            Ok(HttpResponse::InternalServerError().finish())
        } else {
            Ok(HttpResponse::Ok().json(self))
        }
    }
}

/// Serde representation of a location
#[derive(Debug, Serialize)]
struct LocationPublic {
    latitude: f64,
    longitude: f64,
}

#[post("/locate")]
pub async fn service(
    data: Option<web::Json<LocationRequestPublic>>,
    tx_t38_conn: web::Data<flume::Sender<T38ConnectionManageMessage>>,
    rl_app_web: web::Data<RateLimitersApp>,
    yandex_client_web: web::Data<reqwest::Client>,
    tx_yandex_api_web: web::Data<flume::Sender<YandexApiMessage>>,
    _req: HttpRequest,
) -> actix_web::Result<HttpResponse> {
    let mut data = match data.map(|x| x.into_inner()) {
        Some(loc_request) => loc_request,
        None => {
            return Ok(HttpResponse::BadRequest().json(json!(
                {
                    "error": {
                        "domain": "locate",
                        "reason": "invalid request",
                        "message": "request deserialization error",
                        "code": 400,
                    }
                }
            )));
        }
    };

    let mut macs_set = HashSet::with_capacity(data.wifi.len());
    data.wifi.iter().for_each(|m| {
        macs_set.insert(m.mac.as_str());
    });
    let macs = macs_set.into_iter().collect::<Vec<&str>>();

    let collection = Collection::Wifi.as_ref();
    let tx_t38c = (*tx_t38_conn.into_inner()).clone();
    let yandex_client = (*yandex_client_web.into_inner()).clone();
    let tx_yandex_api = (*tx_yandex_api_web.into_inner()).clone();
    let rl_app = (*rl_app_web.into_inner()).clone();

    let ylrs_cell_opt = match get_cell(
        data.cell.take(),
        tx_t38c.clone(),
        yandex_client.clone(),
        tx_yandex_api.clone(),
        rl_app.clone(),
    )
    .await
    {
        Err(e) => {
            return Ok(create_error_response(e, "locate"));
        }
        Ok(c) => c,
    };

    // validate GPS relative Cell
    if let Some(gnss) = &data.gnss {
        let p_gnss = Point {
            id: 0,
            lat: gnss.latitude,
            lon: gnss.longitude,
        };
        let threshold = 500.0;
        if let Some(valid_gps) =
            is_valid_gps_relative_cell(ylrs_cell_opt.as_ref(), p_gnss, threshold)
        {
            if valid_gps {
                // accuracy = 0.0
                return LocationResponsePublic::new(gnss.latitude, gnss.longitude, 0.0).respond();
            }
        }
    }

    let tls =
        fget_wifi_many_from_pipeline::<TransmitterLocation>(tx_t38c.clone(), collection, &macs)
            .await
            .map_err(|e| ApiError::Tile38Error(e.to_string()))?;

    let mut lat_weight = 0.0;
    let mut lon_weight = 0.0;
    let mut r_weight = 0.0;
    let mut w_weight = 0.0;
    let mut c = 0;

    let outliers_opt = match detect_outliers(
        &tls,
        tx_t38c.clone(),
        yandex_client.clone(),
        tx_yandex_api.clone(),
        rl_app.clone(),
        ylrs_cell_opt.clone(),
    )
    .await
    {
        Err(e) => {
            return Ok(create_error_response(e, "locate"));
        }
        Ok(o) => o,
    };

    for tl_opt in tls.iter() {
        if let Some(tl) = tl_opt {
            // skip the outlier TransmitterLocation
            if check_outlier(outliers_opt.as_ref(), &tl) {
                continue;
            }

            if tl.valid() {
                let wap_signal_strength = data
                    .wifi
                    .iter()
                    .find(|wap| *wap.mac == tl.mac)
                    .and_then(|wap| wap.rssi);
                // At this point, we can use the real coordinates
                let weight = 10_f64
                    .powf(wap_signal_strength.unwrap_or(-90.0) / (10.0 * SIGNAL_DROP_COEFFICIENT));
                lat_weight = lat_weight + tl.lat * weight;
                lon_weight = lon_weight + tl.lon * weight;
                r_weight = r_weight + tl.accuracy * weight;
                w_weight = w_weight + weight;
                c = c + 1;
            }
        }
    }

    if c >= 1 {
        lat_weight = lat_weight / w_weight;
        lon_weight = lon_weight / w_weight;
        r_weight = r_weight / w_weight;

        if lat_weight.is_nan() || lon_weight.is_nan() {
            dbg!(r_weight, w_weight);
        } else {
            return LocationResponsePublic::new(lat_weight, lon_weight, r_weight).respond();
        }
    }

    // localization using our own database didn't work, so we're using Yandex LBS
    if CONFIG.yandex_lbs.enabled {
        let mut wifi_track = None;
        if let Some(device_id) = data.device_id {
            match get_wifi_track_one(tx_t38c.clone(), collection, &device_id).await {
                Err(e) => {
                    error!("get wifi track for device id '{}': {}", device_id, e);
                }
                Ok(wt) => {
                    wifi_track = wt;
                }
            }
        }

        let mut wms = Vec::with_capacity(data.wifi.len());
        data.wifi.iter().for_each(|m| {
            wms.push(crate::lbs::yandex::WifiMeasurement {
                bssid: m.mac.clone(),
                signal_strength: m.rssi.unwrap_or(-90.0).round(),
            });
        });

        match yandex_lbs_request_by_individual_wifi(
            tx_t38c,
            &CONFIG,
            wms,
            yandex_client,
            tx_yandex_api,
            rl_app,
        )
        .await
        {
            Err(e) => {
                error!("Yandex LBS request by individual access points: {e}");
                return Ok(create_error_response(e, "locate"));
            }
            Ok(mut yandex_lbs_responses) => {
                if yandex_lbs_responses.len() > 0 {
                    // remove Yandex outliers
                    let outliers_opt =
                        detect_yandex_outliers(&yandex_lbs_responses, ylrs_cell_opt, wifi_track);
                    if let Some(outliers) = outliers_opt {
                        for (mac, _ylr_opt) in outliers.outliers {
                            if let Some(outlier_opt) = yandex_lbs_responses.remove(&mac) {
                                if let Some(outlier) = outlier_opt {
                                    debug!("detect Yandex outlier: {:?}", outlier);
                                }
                            }
                        }
                    }
                    if let Some(estimated_yandex_lbs_response) =
                        estimate_location_by_yandex_responses(&yandex_lbs_responses)
                    {
                        return LocationResponsePublic::new(
                            estimated_yandex_lbs_response.location.point.lat,
                            estimated_yandex_lbs_response.location.point.lon,
                            estimated_yandex_lbs_response.location.accuracy,
                        )
                        .respond();
                    }
                }
            }
        }
    }

    Ok(HttpResponse::NotFound().json(json!(
        {
            "error": {
                "domain": "locate",
                "reason": "not found",
                "message": "no location could be estimated based on the data provided",
                "code": 404,
            }
        }
    )))
}
