use std::collections::HashSet;

use actix_web::{HttpRequest, HttpResponse, post, web};
use chrono::{DateTime, Utc};
use log::{debug, error};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::lbs::http_client::HttpClient;
use crate::{
    CONFIG,
    constants::{Collection, DEFAULT_RSSI, SIGNAL_DROP_COEFFICIENT},
    db::{
        pg::transmitter::TransmitterLocation,
        t38::{fget_wifi_many_from_pipeline, track::get_wifi_track_one},
    },
    error::{ApiError, create_error_response},
    lbs::{
        model::{Cell, valid_cell},
        yandex::cell::get_cell,
        yandex::wifi::{
            WifiMeasurement, detect_yandex_outliers, estimate_location_by_yandex_responses,
            yandex_lbs_cache_wifi, yandex_lbs_request_by_individual_wifi,
        },
    },
    services::{
        helper::custom_deserialize::{
            date_time_utc_from_str, default_timestamp_ms, mac_address, validate_rssi,
        },
        locate::dbscan::Point,
        rate_limiter::RateLimitersApp,
        submission::{geosubmit_public::PositionPublic, report::is_gps_valid_relative_cell},
    },
    tasks::{
        blobasaur::BAConnectionManageMessage, t38::T38ConnectionManageMessage,
        yandex::YandexApiMessage,
    },
};

use super::dbscan::{check_outlier, detect_outliers};

/// Serde representation of the client's request
#[derive(Debug, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
struct LocationRequestPublic {
    #[serde(deserialize_with = "date_time_utc_from_str")]
    #[serde(default = "default_timestamp_ms")]
    #[allow(unused)]
    timestamp: DateTime<Utc>,

    device_id: Option<String>,
    gnss: Option<PositionPublic>,

    /// List of access points around the client
    #[serde(default)]
    wifi: Vec<AccessPoint>,

    /// List of cell towers around the client
    #[serde(default)]
    cell: Option<Cell>,
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
    #[serde(deserialize_with = "validate_rssi")]
    rssi: Option<f64>,
    snr: Option<f64>,
    ssid: Option<String>,
    #[serde(flatten)]
    extra: Value,
}

/// Struct for representing the server's response
#[derive(Debug, Serialize)]
pub struct LocationResponsePublic {
    location: LocationPublic,
    accuracy: i64,
}

impl LocationResponsePublic {
    /// Create a new location response from a position and an accuracy.
    pub fn new(lat: f64, lon: f64, accuracy: f64) -> Self {
        // round to 6 decimal places
        let lat = (lat * 1_000_000.0).round() / 1_000_000.0;
        let lon = (lon * 1_000_000.0).round() / 1_000_000.0;

        // DEBUG
        // TODO: remove after tests by Whoosh
        let r = LocationResponsePublic {
            location: LocationPublic {
                latitude: lat,
                longitude: lon,
            },
            accuracy: (accuracy.round() as i64),
        };
        let json_r = serde_json::to_string(&r).unwrap();
        info!("Locate: {}", json_r);

        r
    }

    /// Convert the response into a HTTP response
    pub fn respond(self) -> actix_web::Result<HttpResponse> {
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
    tx_ba_conn: web::Data<flume::Sender<BAConnectionManageMessage>>,
    rl_app_web: web::Data<RateLimitersApp>,
    yandex_client_web: web::Data<HttpClient>,
    tx_yandex_api_web: web::Data<flume::Sender<YandexApiMessage>>,
    _req: HttpRequest,
) -> actix_web::Result<HttpResponse> {
    let mut data = match data.map(|x| x.into_inner()) {
        Some(lr) => lr,
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

    if !valid_cell(data.cell.as_ref()) {
        return Ok(HttpResponse::UnprocessableEntity().json(json!(
            {
                "error": {
                    "domain": "locate",
                    "reason": "invalid request",
                    "message": "unsupported mobile country code",
                    "code": 422,
                }
            }
        )));
    }

    let count_wifi = data.wifi.len();

    // DEBUG
    // TODO: remove after tests by Whoosh
    {
        let mut wms = Vec::with_capacity(count_wifi);
        data.wifi.iter().for_each(|m| {
            wms.push(WifiMeasurement {
                bssid: m.mac.clone(),
                signal_strength: m.rssi.unwrap_or(DEFAULT_RSSI).round(),
            });
        });
        let json_w = serde_json::to_string(&wms)?;
        let json_c = serde_json::to_string(&data.cell.as_ref().unwrap_or(&Cell::default()))?;
        info!("wifi: {}", json_w);
        info!("cell: {}", json_c);
    }

    let mut macs_set = HashSet::with_capacity(count_wifi);
    data.wifi.iter().for_each(|m| {
        macs_set.insert(m.mac.as_str());
    });
    let macs = macs_set.into_iter().collect::<Vec<&str>>();

    let collection = Collection::Wifi.as_ref();
    let tx_t38c = (*tx_t38_conn.into_inner()).clone();
    let tx_ba_c = (*tx_ba_conn.into_inner()).clone();
    let yandex_client = (*yandex_client_web.into_inner()).clone();
    let tx_yandex_api = (*tx_yandex_api_web.into_inner()).clone();
    let rl_app = (*rl_app_web.into_inner()).clone();

    let ylrs_cell_opt = match get_cell(
        data.cell.take(),
        tx_t38c.clone(),
        tx_ba_c.clone(),
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
        if let Some(valid_gps) = is_gps_valid_relative_cell(ylrs_cell_opt.as_ref(), p_gnss)
            && valid_gps
        {
            // accuracy = 0.0
            return LocationResponsePublic::new(gnss.latitude, gnss.longitude, 0.0).respond();
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
        tx_ba_c.clone(),
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

    for tl in tls.iter().flatten() {
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
            let weight = 10_f64.powf(
                wap_signal_strength.unwrap_or(DEFAULT_RSSI) / (10.0 * SIGNAL_DROP_COEFFICIENT),
            );
            lat_weight = lat_weight + tl.lat * weight;
            lon_weight = lon_weight + tl.lon * weight;
            r_weight = r_weight + tl.accuracy * weight;
            w_weight = w_weight + weight;
            c = c + 1;
        }
    }

    let mut wms = Vec::with_capacity(count_wifi);
    data.wifi.iter().for_each(|m| {
        wms.push(WifiMeasurement {
            bssid: m.mac.clone(),
            signal_strength: m.rssi.unwrap_or(DEFAULT_RSSI).round(),
        });
    });

    if c >= 1 {
        // additional assessment by Yandex points
        if count_wifi > 1
            && let Ok(mut ylr) = yandex_lbs_cache_wifi(tx_t38c.clone(), &wms).await
        {
            let outliers_opt = detect_yandex_outliers(&ylr, ylrs_cell_opt.clone(), None);
            if let Some(outliers) = outliers_opt.as_ref() {
                for (mac, _ylr_opt) in outliers.outliers() {
                    if let Some(Some(outlier)) = ylr.remove(mac) {
                        debug!("detect Yandex outlier: {:?}", outlier);
                    }
                }
            }

            let count_cache_wifi = ylr.len();
            // In Yandex data the number of points should be greater than in Whoosh data
            if count_cache_wifi > c {
                if let Some(estimated_yandex_lbs_response) =
                    estimate_location_by_yandex_responses(&ylr, &wms)
                {
                    // DEBUG
                    // TODO: remove after tests by Whoosh
                    warn!("Estimate by Yandex Cache: {c}, {count_cache_wifi}");

                    return LocationResponsePublic::new(
                        estimated_yandex_lbs_response.location.point.lat,
                        estimated_yandex_lbs_response.location.point.lon,
                        estimated_yandex_lbs_response.location.accuracy,
                    )
                    .respond();
                }
                // fallback: check that some outliers points are combined into one cluster
                if let Some(outliers) = outliers_opt
                    && let Some(lr) = outliers.try_estimate_location(ylrs_cell_opt.clone(), &wms)
                {
                    // DEBUG
                    // TODO: remove after tests by Whoosh
                    warn!("Fallback location by Yandex Cache: {c}, {count_cache_wifi}");

                    return lr.respond();
                }
            }
        }

        lat_weight = lat_weight / w_weight;
        lon_weight = lon_weight / w_weight;
        r_weight = r_weight / w_weight;
        if lat_weight.is_nan() || lon_weight.is_nan() {
            dbg!(r_weight, w_weight);
        } else {
            // DEBUG
            // TODO: remove after tests by Whoosh
            warn!("Estimate by Reports");

            // location based on reports
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

        match yandex_lbs_request_by_individual_wifi(
            tx_t38c,
            tx_ba_c,
            &wms,
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
                if !yandex_lbs_responses.is_empty() {
                    // remove Yandex outliers
                    let outliers_opt = detect_yandex_outliers(
                        &yandex_lbs_responses,
                        ylrs_cell_opt.clone(),
                        wifi_track,
                    );
                    if let Some(outliers) = outliers_opt.as_ref() {
                        for (mac, _ylr_opt) in outliers.outliers() {
                            if let Some(Some(outlier)) = yandex_lbs_responses.remove(mac) {
                                debug!("detect Yandex outlier: {:?}", outlier);
                            }
                        }
                    }
                    if let Some(estimated_yandex_lbs_response) =
                        estimate_location_by_yandex_responses(&yandex_lbs_responses, &wms)
                    {
                        // DEBUG
                        // TODO: remove after tests by Whoosh
                        warn!("Estimate by Yandex Request");
                        return LocationResponsePublic::new(
                            estimated_yandex_lbs_response.location.point.lat,
                            estimated_yandex_lbs_response.location.point.lon,
                            estimated_yandex_lbs_response.location.accuracy,
                        )
                        .respond();
                    }
                    // fallback: check that some outliers points are combined into one cluster
                    if let Some(outliers) = outliers_opt
                        && let Some(lr) = outliers.try_estimate_location(ylrs_cell_opt, &wms)
                    {
                        // DEBUG
                        // TODO: remove after tests by Whoosh
                        warn!("FALLBACK location by Yandex Request");

                        return lr.respond();
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
