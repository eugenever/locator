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
use log::{debug, error};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::dbscan::{check_outlier, detect_outliers};
use crate::{
    CONFIG,
    constants::SIGNAL_DROP_COEFFICIENT,
    db::{model::CellRadio, t38::fget_wifi_many_from_pipeline},
    error::{ApiError, create_error_response},
    lbs::yandex::{
        detect_yandex_outliers, estimate_location_by_yandex_responses,
        yandex_lbs_request_by_individual_wifi,
    },
    services::{helper::custom_deserialize::mac_address, rate_limiter::RateLimitersApp},
    tasks::{t38::T38ConnectionManageMessage, yandex::YandexApiMessage},
};

/// Serde representation of the client's request
#[allow(unused)]
#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct LocationRequest {
    /// List of cell towers around the client
    #[serde(default)]
    cell_towers: Vec<CellTower>,

    /// List of access points around the client
    #[serde(default)]
    wifi_access_points: Vec<AccessPoint>,

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
#[serde(rename_all = "camelCase")]
struct CellTower {
    radio_type: CellRadio,
    mobile_country_code: i16,
    mobile_network_code: i16,
    location_area_code: i32,
    cell_id: i64,
    psc: Option<i16>,
}

// Serde representation of access points in the client's request
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccessPoint {
    #[serde(deserialize_with = "mac_address")]
    mac_address: String,
    signal_strength: Option<f64>,
}

/// Struct for representing the server's response
#[derive(Debug, Serialize)]
struct LocationResponse {
    location: Location,
    accuracy: i64,
}

impl LocationResponse {
    /// Create a new location response from a position and an accuracy.
    fn new(lat: f64, lon: f64, accuracy: f64) -> Self {
        // round to 6 decimal places
        let lat = (lat * 1_000_000.0).round() / 1_000_000.0;
        let lon = (lon * 1_000_000.0).round() / 1_000_000.0;

        LocationResponse {
            location: Location { lat, lng: lon },
            accuracy: (accuracy.round() as i64),
        }
    }

    /// Convert the response into a HTTP response
    fn respond(self) -> actix_web::Result<HttpResponse> {
        if self.location.lat.is_nan() || self.location.lng.is_nan() {
            Ok(HttpResponse::InternalServerError().finish())
        } else {
            Ok(HttpResponse::Ok().json(self))
        }
    }
}

/// Serde representation of a location
#[derive(Debug, Serialize)]
struct Location {
    lat: f64,
    lng: f64,
}

/// Main entrypoint to geolocate a client.
#[post("/api/mls/v1/geolocate")]
pub async fn service(
    data: Option<web::Json<LocationRequest>>,
    tx_t38_conn: web::Data<flume::Sender<T38ConnectionManageMessage>>,
    rl_app_web: web::Data<RateLimitersApp>,
    yandex_client_web: web::Data<reqwest::Client>,
    tx_yandex_api_web: web::Data<flume::Sender<YandexApiMessage>>,
    _req: HttpRequest,
) -> actix_web::Result<HttpResponse> {
    let data = match data.map(|x| x.into_inner()) {
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

    let mut macs_set = HashSet::with_capacity(data.wifi_access_points.len());
    data.wifi_access_points.iter().for_each(|m| {
        macs_set.insert(m.mac_address.as_str());
    });
    let macs = macs_set.into_iter().collect::<Vec<&str>>();

    let collection = crate::constants::Collection::Wifi.as_ref();
    let tx_t38c = (*tx_t38_conn.into_inner()).clone();

    let tls = fget_wifi_many_from_pipeline(tx_t38c.clone(), collection, &macs)
        .await
        .map_err(|e| ApiError::Tile38Error(e.to_string()))?;

    let mut lat_weight = 0.0;
    let mut lon_weight = 0.0;
    let mut r_weight = 0.0;
    let mut w_weight = 0.0;
    let mut c = 0;

    let yandex_client = (*yandex_client_web.into_inner()).clone();
    let tx_yandex_api = (*tx_yandex_api_web.into_inner()).clone();
    let rl_app = (*rl_app_web.into_inner()).clone();

    let outliers_opt = match detect_outliers(
        &tls,
        tx_t38c.clone(),
        yandex_client.clone(),
        tx_yandex_api.clone(),
        rl_app.clone(),
        None,
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
                    .wifi_access_points
                    .iter()
                    .find(|wap| *wap.mac_address == tl.mac)
                    .and_then(|wap| wap.signal_strength);

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
            return LocationResponse::new(lat_weight, lon_weight, r_weight).respond();
        }
    }

    // localization using our own database didn't work, so we're using Yandex LBS
    if CONFIG.yandex_lbs.enabled {
        let mut wms = Vec::with_capacity(data.wifi_access_points.len());
        data.wifi_access_points.iter().for_each(|m| {
            wms.push(crate::lbs::yandex::WifiMeasurement {
                bssid: m.mac_address.clone(),
                signal_strength: m.signal_strength.unwrap_or(-90.0),
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
                    let outliers_opt = detect_yandex_outliers(&yandex_lbs_responses, None, None);
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
                        return LocationResponse::new(
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
                "message": "No location could be estimated based on the data provided",
                "code": 404,
            }
        }
    )))
}
