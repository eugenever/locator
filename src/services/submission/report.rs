#![allow(unused)]

use std::{collections::HashMap, fmt::Display, str::FromStr};

use actix_web::{HttpRequest, HttpResponse, Responder, http::StatusCode, post, web};
use chrono::{DateTime, Utc};
use log::{debug, error, info};
use redis::RedisError;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::json;
use tokio_postgres::GenericClient;

use crate::{
    CONFIG,
    constants::{
        Collection, DEFAULT_RSSI, GPS_VALID_DISTANCE_BY_CELL, GPS_VALID_DISTANCE_BY_WIFI,
        MAX_DISTANCE_REPORT_LBS, WIFI_SSID_IGNORED,
    },
    db::{
        blobasaur::set_ba_lbs_yandex_wifi_one,
        model::{CellRadio, Transmitter},
        pg::{get_reports_by_range_id, get_required_reports, transmitter::TransmitterLocation},
        t38::{
            fget_wifi_many_from_pipeline, set_yandex_lbs_wifi_one,
            track::{
                Gnss, WifiTrack, WifiTrackRecord, get_wifi_track_one, set_wifi_track_record_one,
            },
        },
    },
    error::ApiError,
    lbs::{
        altergeo::altergeo_lbs_request,
        http_client::HttpClient,
        model::{self, create_cell_measurement},
        yandex::{
            cell::yandex_lbs_request_by_individual_cell,
            wifi::{
                WifiMeasurement, YandexLbsResponse, YandexLocation, YandexPoint,
                yandex_lbs_request_by_individual_wifi,
            },
        },
    },
    services::{
        helper::{self, macaddr::MacAddr},
        locate::dbscan::{Point, Proximity, distance_factor_cell},
        rate_limiter::RateLimitersApp,
        submission::process::run,
    },
    tasks::{
        blobasaur::BAConnectionManageMessage, t38::T38ConnectionManageMessage,
        yandex::YandexApiMessage,
    },
};

const TRACK_SIZE_THRESHOLD: u16 = 3;

/// Serde representation to deserialize report
#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Report {
    #[serde(deserialize_with = "timestamp_from_utc_str")]
    pub timestamp: i64,
    pub device_id: Option<String>,
    pub position: Position,
    pub cell: Option<model::Cell>,
    pub cell_towers: Option<Vec<Cell>>,
    pub wifi_access_points: Option<Vec<Wifi>>,
    pub bluetooth_beacons: Option<Vec<Bluetooth>>,
}

#[derive(Deserialize, Debug, Default)]
pub struct GeoFence {
    pub lat_min: f64,
    pub lat_max: f64,
    pub lon_min: f64,
    pub lon_max: f64,
}

impl GeoFence {
    pub fn validate(&self) -> bool {
        self.lat_max >= self.lat_min && self.lon_max >= self.lon_min
    }
}

impl Display for GeoFence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(lat_min: {}, lat_max: {}, lon_min: {}, lon_max: {})",
            self.lat_min, self.lat_max, self.lon_min, self.lon_max
        )
    }
}

#[post("/process_report")]
pub async fn process_report(
    data: web::Json<GeoFence>,
    pool_tp: web::Data<deadpool_postgres::Pool>,
    tx_t38_conn: web::Data<flume::Sender<T38ConnectionManageMessage>>,
    tx_ba_conn: web::Data<flume::Sender<BAConnectionManageMessage>>,
    yandex_client: web::Data<HttpClient>,
    tx_yandex_api: web::Data<flume::Sender<YandexApiMessage>>,
    rl_app: web::Data<RateLimitersApp>,
    _req: HttpRequest,
) -> actix_web::Result<impl Responder> {
    let gf = data.into_inner();
    let mut geo_fence = None;
    if gf.lat_min.round() != 0.0
        && gf.lat_max.round() != 0.0
        && gf.lon_min.round() != 0.0
        && gf.lon_max.round() != 0.0
    {
        geo_fence = Some(gf);
    }

    let result = run(
        (*pool_tp.into_inner()).clone(),
        (*tx_t38_conn.into_inner()).clone(),
        (*tx_ba_conn.into_inner()).clone(),
        (*yandex_client.into_inner()).clone(),
        (*tx_yandex_api.into_inner()).clone(),
        (*rl_app.into_inner()).clone(),
        geo_fence,
    )
    .await
    .map_err(|e| ApiError::DatabaseError(e.to_string()))
    .map(|_| HttpResponse::new(StatusCode::OK));

    Ok(result)
}

#[derive(Deserialize)]
pub struct RequredReport {
    pub ids: Vec<u64>,
}

#[post("/extract_report")]
pub async fn extract_report(
    data: web::Json<RequredReport>,
    pool_tp: web::Data<deadpool_postgres::Pool>,
    _req: HttpRequest,
) -> actix_web::Result<impl Responder> {
    let er = data.into_inner();
    let mapper = pool_tp
        .get()
        .await
        .map_err(|e| ApiError::DatabaseError(e.to_string()))?;
    let client = mapper.client();

    match get_required_reports(client, &er.ids).await {
        Err(e) => Ok(HttpResponse::new(StatusCode::BAD_REQUEST)),
        Ok(reports) => {
            let mut rs = Vec::with_capacity(reports.len());
            reports.into_iter().for_each(|r| {
                if let Ok(r) = serde_json::from_slice::<'_, Report>(&r.raw) {
                    rs.push(r);
                }
            });
            Ok(HttpResponse::Ok().json(rs))
        }
    }
}

// scan reports by condition
#[post("/scan_reports")]
pub async fn scan_reports(
    data: web::Json<RequredReport>,
    pool_tp: web::Data<deadpool_postgres::Pool>,
    _req: HttpRequest,
) -> actix_web::Result<impl Responder> {
    let sr = data.into_inner();
    let mapper = pool_tp
        .get()
        .await
        .map_err(|e| ApiError::DatabaseError(e.to_string()))?;
    let client = mapper.client();

    match get_reports_by_range_id(client, &sr.ids).await {
        Err(e) => Ok(HttpResponse::new(StatusCode::BAD_REQUEST)),
        Ok(reports) => {
            let mut rs = Vec::with_capacity(reports.len());
            reports.into_iter().for_each(|r| {
                if let Ok(r) = serde_json::from_slice::<'_, Report>(&r.raw) {
                    rs.push(r);
                }
            });

            let mut c = 0;
            rs.iter().for_each(|r| {
                // invalid rxLev
                /*
                    if let Some(cell) = r.cell.as_ref()
                        && let Some(gsm) = cell.gsm.as_ref()
                    {
                        if !gsm.is_empty() {
                            if gsm[0].rxlev >= 0.0 {
                                c += 1;
                            }
                        }
                    }
                */

                // invalid RSSI
                if let Some(waps) = r.wifi_access_points.as_ref() {
                    waps.iter().for_each(|wap| {
                        if let Some(rssi) = wap.signal_strength
                            && rssi >= 0.0
                        {
                            c += 1;
                        }
                    });
                }
            });

            Ok(HttpResponse::Ok().json(json!({
                    // "count_reports_where_rxlev_greater_than_0": c
                    "count_wifi_where_rssi_greater_than_0": c
            })))
        }
    }
}

pub fn timestamp_from_utc_str<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    if let Ok(dt) = s.parse::<DateTime<Utc>>() {
        Ok(dt.timestamp_millis())
    } else {
        Err(serde::de::Error::custom(ApiError::DateTimeParseFailed(s)))
    }
}

/// Serde representation to deserialize a position in a report
#[derive(Deserialize, Serialize, Debug)]
pub struct Position {
    pub latitude: f64,
    pub longitude: f64,
    #[serde(default)]
    pub speed: Option<f64>,
    // Tower Collector does not send age field
    #[serde(default)]
    pub age: Option<i32>,
    pub accuracy: Option<f64>,
    pub heading: Option<f64>,
}

/// Serde representation to deserialize a cell tower in a report
#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Cell {
    radio_type: RadioType,
    mobile_country_code: u16,
    mobile_network_code: u16,
    // NeoStumbler/18 send {"locationAreaCode":null}
    #[serde(default)]
    location_area_code: Option<u32>, // u24 in db
    // NeoStumbler/18 send {"cellId":null}
    #[serde(default)]
    cell_id: Option<u64>,
    // NeoStumbler/18 send {"primaryScramblingCode":null}
    #[serde(default)]
    primary_scrambling_code: Option<u16>,
    // Tower Collector does not send age field
    #[serde(default)]
    age: Option<i32>,

    // Signal can be between -44 dBm and -140 dBm according to
    // https://android.stackexchange.com/questions/167650/acceptable-signal-strength-ranges-for-2g-3g-and-4g
    // so we need to store it on an i16 as i8 would overflow
    signal_strength: Option<f64>,

    // Arbitrary Strength Unit, which can be parsed into signal strength based
    // on the underlying network
    asu: Option<i16>,
}

impl Cell {
    fn signal_strength(&self) -> Option<f64> {
        if self.signal_strength.is_some() {
            return Some(self.signal_strength.unwrap());
        }
        // If signal strength is not available, we need to extract it from the ASU
        // Info about this process: https://en.wikipedia.org/wiki/Mobile_phone_signal#ASU
        if let Some(asu) = self.asu {
            // 99 means unknown
            if asu == 99 {
                return None;
            }

            return match self.radio_type {
                // Seems to be fine (match what's given on my phone -83 dBm 15 ASU)
                RadioType::Gsm => Some((2.0 * asu as f64) - 113.0),

                // // TODO: According to Wikipedia, Android use GMS formula for UMTS,
                // // we need to figure out the best way to extract in this case.
                // RadioType::Umts => Some(asu - 115),
                // Based on my testing on Pixel 6a GrapheneOS with Android 16, the
                // formula is a bit different, but seems to match what's shown in
                // Android settings (type *#*#4636#*#* in dialer then select Phone
                // Info to get more detailed settings, I can't force 2G or 3G using
                // normal settings).
                RadioType::Umts => Some(asu as f64 - 120.0),

                // Value is between asu-140 and asu-143, we just take the highest
                // value, as middle point would be a floating point
                RadioType::Lte => Some(asu as f64 - 140.0),

                // Formula for 5G is not available on Wikipedia, but this post seems
                // to say it's the same as LTE formula. I don't know if it's
                // trustworthy as the same post also says ASU is linear and dBm is
                // logarithmic, which is obviously wrong as the conversion is an
                // affine function, which can't cancel a logarithm.
                // https://www.linkedin.com/pulse/what-arbitrary-signal-unit-why-does-matter-telecom-hassan-bin-tila-oap2c
                // I didn't verify this formula, as I don't have access to 5G networks
                RadioType::Nr => Some(asu as f64 - 140.0),
            };
        }

        None
    }
}

/// Serde representation to deserialize a radio type
#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "lowercase")]
enum RadioType {
    Gsm,
    #[serde(rename = "wcdma")]
    Umts,
    Lte,
    Nr,
}

/// Serde representation to deserialize a wifi network in a report
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Wifi {
    pub mac_address: String,
    pub ssid: Option<String>,
    #[serde(default)]
    pub age: Option<i32>,
    pub signal_strength: Option<f64>,
}

impl Wifi {
    fn signal_strength(&self) -> Option<f64> {
        if let Some(signal_strength) = self.signal_strength {
            return Some(signal_strength);
        }
        None
    }

    async fn should_be_ignored(
        &self,
        report: &Report,
        yandex_lbs_responses: &HashMap<String, Option<YandexLbsResponse>>,
        yandex_client: HttpClient,
        tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
        tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
        ylrs_cell_opt: Option<&HashMap<String, Option<YandexLbsResponse>>>,
    ) -> bool {
        if CONFIG.locator.laa_filter {
            // check mac address (LAA)
            if let Ok(m) = MacAddr::from_str(&self.mac_address) {
                // ignore locally administered addresses
                if m.is_local() {
                    return true;
                }
            }
        }

        // reject based on ssid
        if let Some(ssid) = self.ssid.as_ref()
            && WIFI_SSID_IGNORED
                .iter()
                .any(|ssid_ignored| ssid.to_lowercase().contains(ssid_ignored))
        {
            return true;
        }

        let p_origin = Point {
            id: 0,
            lat: report.position.latitude,
            lon: report.position.longitude,
        };

        let ignore_by_cell = is_ignore_by_cell(
            yandex_lbs_responses,
            ylrs_cell_opt,
            report,
            p_origin,
            &self.mac_address,
            tx_t38_conn.clone(),
        )
        .await;

        // check by gps and cell, exclude the request in AlterGeo
        if let Some(ignore) = ignore_by_cell
            && ignore
        {
            return true;
        }

        if let Some(Some(ylr)) = yandex_lbs_responses.get(&self.mac_address) {
            let p_yandex = Point {
                id: 1,
                lat: ylr.location.point.lat,
                lon: ylr.location.point.lon,
            };
            let d_yandex = p_origin.distance(&p_yandex);

            if d_yandex > MAX_DISTANCE_REPORT_LBS {
                // lastly trying to check through AlterGeo
                if CONFIG.altergeo_lbs.enabled {
                    let collection = Collection::LbsYandexWifi.as_ref();
                    let wm = WifiMeasurement {
                        bssid: self.mac_address.clone(),
                        signal_strength: self.signal_strength.unwrap_or(DEFAULT_RSSI).round(),
                    };
                    if let Ok(ag_response) = altergeo_lbs_request(vec![wm], yandex_client).await {
                        if let Some(iamhere) = ag_response.iamhere {
                            let p_ag = Point {
                                id: 2,
                                lat: iamhere.latitude,
                                lon: iamhere.longitude,
                            };
                            let d_ag = p_origin.distance(&p_ag);
                            if d_ag < MAX_DISTANCE_REPORT_LBS {
                                // correct Yandex LBS response in our database
                                let correct_yandex_lbs_response = YandexLbsResponse {
                                    location: YandexLocation {
                                        accuracy: ylr.location.accuracy,
                                        point: YandexPoint {
                                            lat: iamhere.latitude,
                                            lon: iamhere.longitude,
                                        },
                                    },
                                };
                                if let Err(e) = set_yandex_lbs_wifi_one(
                                    tx_t38_conn,
                                    collection,
                                    &correct_yandex_lbs_response,
                                    &self.mac_address,
                                )
                                .await
                                {
                                    error!("save correct Yandex LBS response: {}", e);
                                }
                                info!(
                                    "AlterGeo check: mac '{}', distance: {:.2}",
                                    self.mac_address, d_ag
                                );
                                // save yandex response in blobasaur
                                if CONFIG.blobasaur.enabled {
                                    let namespace = Collection::BaLbsYandexWifi.as_ref();
                                    if let Err(e) = set_ba_lbs_yandex_wifi_one(
                                        tx_ba_conn.clone(),
                                        namespace,
                                        correct_yandex_lbs_response,
                                        &self.mac_address,
                                    )
                                    .await
                                    {
                                        error!("save Yandex LBS response in blobasaur: {}", e);
                                    }
                                }
                                // AlterGeo has coordinates close to the original GNSS, so we consider the validation successful
                                return false;
                            }
                        }
                        if let Some(ag_error) = ag_response.error {
                            info!(
                                "AlterGeo code {}, error {}",
                                ag_error.code, ag_error.message
                            );
                        }
                    }
                }
                // ignore by distance to Yandex point
                return true;
            } else {
                return false;
            }
        }
        // ignore access points that are not in the LBS
        true
    }
}

/// Serde representation to deserialize a bluetooth beacon in a report
#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Bluetooth {
    mac_address: String,
    #[serde(default)]
    age: Option<i32>,
    signal_strength: Option<f64>,
}

impl Bluetooth {
    fn signal_strength(&self) -> Option<f64> {
        if let Some(signal_strength) = self.signal_strength {
            return Some(signal_strength);
        }
        None
    }
}

fn should_be_ignored(position: &Position, transmitter_age: Option<i32>) -> bool {
    if let Some(transmitter_age) = transmitter_age
        && let Some(position_age) = position.age
    {
        let position_transmitter_diff_age: u32 = position_age.abs_diff(transmitter_age);
        // trasmitter is observed more than 30 seconds from position
        // Since Neostumbler/18 (1.4.0), age is limited to 30 seconds, before it, the age is not limited
        if position_transmitter_diff_age > 30_000 {
            return true;
        }
        if position.speed.unwrap_or(0.0) * position_transmitter_diff_age as f64 > 150_000.0 {
            return true;
        }
    }
    // the age field is optional, so for now observations without an age are still considered valid.
    // ideally with a future weighted algorithm observations with no age field have little weight / high uncertainty
    false
}

/// Extract the position and the submitted transmitters from the raw data
pub async fn extract(
    raw: &[u8],
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
    yandex_client: HttpClient,
    tx_yandex_api: flume::Sender<YandexApiMessage>,
    rl_app: RateLimitersApp,
) -> Result<(Position, Vec<Transmitter>), ApiError> {
    let report: Report = serde_json::from_slice(raw)?;
    extract_from_report(
        report,
        tx_t38_conn,
        tx_ba_conn,
        yandex_client,
        tx_yandex_api,
        rl_app,
    )
    .await
}

/// Extract the position and the submitted transmitters from the Report
pub async fn extract_from_report(
    mut report: Report,
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
    yandex_client: HttpClient,
    tx_yandex_api: flume::Sender<YandexApiMessage>,
    rl_app: RateLimitersApp,
) -> Result<(Position, Vec<Transmitter>), ApiError> {
    let ylrs_cell = match extract_cell(
        report.cell.take(),
        tx_t38_conn.clone(),
        tx_ba_conn.clone(),
        yandex_client.clone(),
        tx_yandex_api.clone(),
        rl_app.clone(),
    )
    .await
    {
        Err(e) => {
            error!("Yandex LBS request by individual cells: {}", e);
            return Err(e);
        }
        Ok(ylrs) => ylrs,
    };

    let mut transmitters = Vec::new();

    if let Some(wifi_vec) = report.wifi_access_points.take() {
        // let mut macs = Vec::new();
        let mut wms = Vec::with_capacity(wifi_vec.len());
        wifi_vec.iter().for_each(|m| {
            wms.push(WifiMeasurement {
                bssid: m.mac_address.clone(),
                signal_strength: m.signal_strength.unwrap_or(DEFAULT_RSSI).round(),
            });
        });

        let p_origin = Point {
            id: 0,
            lat: report.position.latitude,
            lon: report.position.longitude,
        };

        let yandex_lbs_responses = match yandex_lbs_request_by_individual_wifi(
            tx_t38_conn.clone(),
            tx_ba_conn.clone(),
            &wms,
            yandex_client.clone(),
            tx_yandex_api,
            rl_app,
        )
        .await
        {
            Err(e) => {
                error!("Yandex LBS request by individual access points: {e}");
                return Err(e);
            }
            Ok(map) => {
                // when validating input data, we do not perform Yandex outlier's analysis
                map
            }
        };

        let mut wifi_valid = vec![];
        for wifi in wifi_vec {
            // check the rules of ignoring
            if wifi
                .should_be_ignored(
                    &report,
                    &yandex_lbs_responses,
                    yandex_client.clone(),
                    tx_t38_conn.clone(),
                    tx_ba_conn.clone(),
                    Some(&ylrs_cell),
                )
                .await
            {
                continue;
            }
            if should_be_ignored(&report.position, wifi.age) {
                continue;
            }

            // add only valid access points to the track
            wifi_valid.push(wifi.clone());

            // register all networks including hidden
            transmitters.push(Transmitter::Wifi {
                mac: wifi.mac_address.clone(),
                signal_strength: wifi.signal_strength(),
                age: wifi.age.map(Into::into),
            });
        }

        // save only valid points in the track
        if let Err(e) =
            process_wifi_track(wifi_valid, tx_t38_conn, &report, &yandex_lbs_responses).await
        {
            error!("process wifi track: {}", e);
        };
    }

    Ok((report.position, transmitters))
}

async fn is_ignore_by_cell(
    yandex_lbs_responses: &HashMap<String, Option<YandexLbsResponse>>,
    ylrs_cell_opt: Option<&HashMap<String, Option<YandexLbsResponse>>>,
    report: &Report,
    p_origin: Point,
    mac: &str,
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
) -> Option<bool> {
    let collection = Collection::LbsYandexWifi.as_ref();
    if let Some(ylrs_cell) = ylrs_cell_opt {
        let mut distance_cell_point = None;
        let dfc = distance_factor_cell(ylrs_cell);

        if let Some(Some(ylr_cell)) = ylrs_cell.values().next() {
            let p_cell = Point {
                id: 2,
                lat: ylr_cell.location.point.lat,
                lon: ylr_cell.location.point.lon,
            };
            distance_cell_point = Some(p_cell.distance(&p_origin));
        }

        // exclude the request in Altergeo
        // filter Yandex outlier
        if let Some(d) = distance_cell_point {
            if d < CONFIG.locator.max_distance_cell * dfc {
                // don`t ignore point, she's most likely in the BS service area
                return Some(false);
            } else {
                // ignore incidents of outliers at Sheremetyevo
                return Some(true);
            }
        }
    }
    None
}

async fn is_gps_valid_relative_wifi(
    macs: &[&str],
    p_origin: Point,
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
) -> Result<bool, ApiError> {
    let collection = Collection::Wifi.as_ref();
    let transmitters_existing =
        fget_wifi_many_from_pipeline::<TransmitterLocation>(tx_t38_conn.clone(), collection, macs)
            .await
            .map_err(|e| ApiError::Tile38Error(e.to_string()))?;

    let mut valid_gps = false;
    if transmitters_existing.is_empty() {
        return Ok(valid_gps);
    }

    transmitters_existing.into_iter().for_each(|t_opt| {
        if let Some(t) = t_opt {
            let p_t = Point {
                id: 1,
                lat: t.lat,
                lon: t.lon,
            };
            let d_origin_t = p_origin.distance(&p_t);
            // to improve accuracy, it is necessary to severely limit the GPS_VALID_DISTANCE_BY_WIFI = 50 meters
            if d_origin_t < GPS_VALID_DISTANCE_BY_WIFI {
                valid_gps = true;
            }
        }
    });

    Ok(valid_gps)
}

pub fn is_gps_valid_relative_cell(
    ylrs_cell_opt: Option<&HashMap<String, Option<YandexLbsResponse>>>,
    p_origin: Point,
) -> Option<bool> {
    if let Some(ylrs_cell) = ylrs_cell_opt {
        let mut distance_cell_point = None;

        if let Some(Some(ylr_cell)) = ylrs_cell.values().next() {
            let p_cell = Point {
                id: 2,
                lat: ylr_cell.location.point.lat,
                lon: ylr_cell.location.point.lon,
            };
            distance_cell_point = Some(p_cell.distance(&p_origin));
        }

        if let Some(d) = distance_cell_point {
            if d <= GPS_VALID_DISTANCE_BY_CELL {
                // GPS is valid
                return Some(true);
            } else {
                return Some(false);
            }
        }
    }
    None
}

pub async fn extract_cell(
    cell_opt: Option<model::Cell>,
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
    yandex_client: HttpClient,
    tx_yandex_api: flume::Sender<YandexApiMessage>,
    rl_app: RateLimitersApp,
) -> Result<HashMap<String, Option<YandexLbsResponse>>, ApiError> {
    let mut cms = vec![];
    if let Some(cell) = cell_opt {
        cms = create_cell_measurement(&cell);
    }
    yandex_lbs_request_by_individual_cell(
        tx_t38_conn,
        tx_ba_conn,
        cms,
        yandex_client,
        tx_yandex_api,
        rl_app,
    )
    .await
}

async fn process_wifi_track(
    wifi_valid: Vec<Wifi>,
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    report: &Report,
    yandex_lbs_responses: &HashMap<String, Option<YandexLbsResponse>>,
) -> Result<(), RedisError> {
    if !wifi_valid.is_empty()
        && let Some(device_id) = &report.device_id
    {
        let collection = Collection::DeviceWhoosh.as_ref();
        match get_wifi_track_one(tx_t38_conn.clone(), collection, device_id).await {
            Err(e) => {
                error!("get wifi track for device id '{}': {}", device_id, e);
            }
            Ok(wifi_track_opt) => {
                let w = wifi_valid
                    .into_iter()
                    .map(|wv| {
                        if let Some(Some(ylr)) = yandex_lbs_responses.get(&wv.mac_address) {
                            let yandex_gnss = Gnss {
                                lat: helper::round(ylr.location.point.lat, 6),
                                lon: helper::round(ylr.location.point.lon, 6),
                            };
                            crate::db::t38::track::Wifi {
                                m: wv.mac_address,
                                r: wv.signal_strength.unwrap_or(DEFAULT_RSSI),
                                g: yandex_gnss,
                            }
                        } else {
                            crate::db::t38::track::Wifi::default()
                        }
                    })
                    .collect::<Vec<_>>();
                let wtr = WifiTrackRecord {
                    gnss: Some(Gnss {
                        lat: report.position.latitude,
                        lon: report.position.longitude,
                    }),
                    ts: report.timestamp,
                    wifi: w,
                };

                let wifi_track = if let Some(mut wt) = wifi_track_opt {
                    let track_size = wt.records.len() as u16;
                    if track_size > TRACK_SIZE_THRESHOLD {
                        // at the moment the track is sorted by timestamp
                        // does not preserve the order of elements, but works faster
                        wt.records.swap_remove(0);
                        // after calling swap_remove, sorting is disrupted
                    }
                    wt.records.push(wtr);
                    wt
                } else {
                    WifiTrack {
                        device_id: device_id.clone(),
                        records: vec![wtr],
                    }
                };
                if let Err(e) =
                    set_wifi_track_record_one(tx_t38_conn.clone(), collection, wifi_track).await
                {
                    error!("set wifi track for device id '{}': {}", device_id, e);
                }
            }
        }
    }
    Ok(())
}

/// Extract the position and the submitted transmitters from the raw data
pub async fn _extract_all_transmitter_types(
    raw: &[u8],
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
    yandex_client: HttpClient,
    tx_yandex_api: flume::Sender<YandexApiMessage>,
    rl_app: RateLimitersApp,
) -> Result<(Position, Vec<Transmitter>), ApiError> {
    let report: Report = serde_json::from_slice(raw)?;

    let mut transmitters = Vec::new();

    for cell in report.cell_towers.as_ref().unwrap_or(&vec![]) {
        if should_be_ignored(&report.position, cell.age) {
            continue;
        }
        if cell.mobile_country_code == 0
                // || cell.mobile_network_code == 0 // this is valid
                || cell.location_area_code.unwrap_or(0) == 0
                || cell.cell_id.unwrap_or(0) == 0
                || cell.primary_scrambling_code.is_none()
        {
            // TODO: reuse previous cell tower data
            continue;
        }

        transmitters.push(Transmitter::Cell {
            radio: match cell.radio_type {
                RadioType::Gsm => CellRadio::Gsm,
                RadioType::Umts => CellRadio::Wcdma,
                RadioType::Lte => CellRadio::Lte,
                RadioType::Nr => CellRadio::Nr,
            },
            // postgres uses signed integers
            country: cell.mobile_country_code as i16,
            network: cell.mobile_network_code as i16,
            area: cell.location_area_code.unwrap() as i32,
            cell: cell.cell_id.unwrap() as i64,
            unit: cell.primary_scrambling_code.unwrap() as i16,
            signal_strength: cell.signal_strength(),
            age: cell.age.map(Into::into),
        })
    }

    if let Some(wifi_vec) = &report.wifi_access_points {
        let mut wms = Vec::with_capacity(wifi_vec.len());
        wifi_vec.iter().for_each(|m| {
            wms.push(WifiMeasurement {
                bssid: m.mac_address.clone(),
                signal_strength: m.signal_strength.unwrap_or(DEFAULT_RSSI).round(),
            });
        });

        let mut yandex_lbs_responses = HashMap::new();
        match yandex_lbs_request_by_individual_wifi(
            tx_t38_conn.clone(),
            tx_ba_conn.clone(),
            &wms,
            yandex_client.clone(),
            tx_yandex_api,
            rl_app,
        )
        .await
        {
            Err(e) => {
                error!("Yandex LBS request by individual access points: {e}");
                return Err(e);
            }
            Ok(map) => {
                // when validating input data, we do not perform Yandex outlier's analysis
                yandex_lbs_responses = map;
            }
        }

        // let valid_gps = false;

        for wifi in wifi_vec {
            // check the rules of ignoring
            if wifi
                .should_be_ignored(
                    &report,
                    &yandex_lbs_responses,
                    yandex_client.clone(),
                    tx_t38_conn.clone(),
                    tx_ba_conn.clone(),
                    None,
                )
                .await
            {
                continue;
            }
            if should_be_ignored(&report.position, wifi.age) {
                continue;
            }
            // register all networks including hidden
            transmitters.push(Transmitter::Wifi {
                mac: wifi.mac_address.clone(),
                signal_strength: wifi.signal_strength(),
                age: wifi.age.map(Into::into),
            });
        }
    }

    for bt in report.bluetooth_beacons.unwrap_or_default() {
        if should_be_ignored(&report.position, bt.age) {
            continue;
        }
        transmitters.push(Transmitter::Bluetooth {
            mac: bt.mac_address.clone(),
            signal_strength: bt.signal_strength(),
            age: bt.age.map(Into::into),
        })
    }

    Ok((report.position, transmitters))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_signal_strength() {
        // 2G: -83 dBm = 15 asu
        assert_eq!(
            Cell {
                mobile_country_code: 0,
                mobile_network_code: 0,
                location_area_code: None,
                cell_id: None,
                primary_scrambling_code: None,
                age: None,
                signal_strength: None,

                radio_type: RadioType::Gsm,
                asu: Some(15),
            }
            .signal_strength(),
            Some(-83.0)
        );

        // 3G: -85 dBm = 35 asu
        assert_eq!(
            Cell {
                mobile_country_code: 0,
                mobile_network_code: 0,
                location_area_code: None,
                cell_id: None,
                primary_scrambling_code: None,
                age: None,
                signal_strength: None,

                radio_type: RadioType::Umts,
                asu: Some(35),
            }
            .signal_strength(),
            Some(-85.0)
        );

        // 4G: -108 dBm = 32 asu
        assert_eq!(
            Cell {
                mobile_country_code: 0,
                mobile_network_code: 0,
                location_area_code: None,
                cell_id: None,
                primary_scrambling_code: None,
                age: None,
                signal_strength: None,

                radio_type: RadioType::Lte,
                asu: Some(32),
            }
            .signal_strength(),
            Some(-108.0)
        );

        // Always prefer signal strength to ASU
        assert_eq!(
            Cell {
                mobile_country_code: 0,
                mobile_network_code: 0,
                location_area_code: None,
                cell_id: None,
                primary_scrambling_code: None,
                age: None,

                radio_type: RadioType::Lte,
                signal_strength: Some(-20.0),
                asu: Some(32),
            }
            .signal_strength(),
            Some(-20.0)
        );

        // Ignore ASU 99 (error)
        assert_eq!(
            Cell {
                mobile_country_code: 0,
                mobile_network_code: 0,
                location_area_code: None,
                cell_id: None,
                primary_scrambling_code: None,
                age: None,
                signal_strength: None,

                radio_type: RadioType::Lte,
                asu: Some(99),
            }
            .signal_strength(),
            None
        );

        // TODO: Test 5G/NR
    }
}
