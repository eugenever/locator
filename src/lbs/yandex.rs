#![allow(dead_code)]

use std::collections::HashMap;
use std::time::Duration;

use clusters::Proximity;
use log::{error, info};
use once_cell::sync::Lazy;
use redis::{FromRedisValue, ParsingError, Value};
use serde::{Deserialize, Serialize};

use crate::{
    CONFIG,
    config::Config,
    constants::{Collection, HOUR, MAX_DISTANCE, MAX_SCOOTER_SPEED},
    db::t38::{
        cmd::REDIS_NO_DATA, get_wifi_many_from_pipeline, get_yandex_lbs_wifi_one,
        set_yandex_lbs_wifi_one, track::WifiTrack,
    },
    error::ApiError,
    services::{
        locate::dbscan::{Algorithm, DBSCAN, Point, create_cell_points},
        rate_limiter::RateLimitersApp,
    },
    tasks::{
        t38::T38ConnectionManageMessage,
        yandex::{InvalidApiKey, YandexApiMessage},
    },
};

pub static YANDEX_LBS_URL: Lazy<String> = Lazy::new(|| {
    let yandex_lbs_url = format!("{}{}", CONFIG.yandex_lbs.url, CONFIG.yandex_lbs.api_keys[0]);
    yandex_lbs_url
});

macro_rules! not_convertible_error {
    ($v:expr, $det:expr) => {
        ParsingError::from(format!("{:?} (response was {:?})", $det, $v))
    };
}

#[derive(Debug, Clone, Serialize)]
pub struct YandexLbsRequest {
    pub wifi: Vec<WifiMeasurement>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WifiMeasurement {
    pub bssid: String,
    pub signal_strength: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct YandexData {
    pub ylr: YandexLbsResponse,
    pub mac: String,
}

impl FromRedisValue for YandexData {
    /*
    [
        bulk-string('"{\"type\":\"Point\",\"coordinates\":[55.68643406911036,37.900367606988134]}"'),
        array([
            bulk-string('"data"'),
            bulk-string('"{\"location\":{\"point\":{\"lat\":55.68643406911036,\"lon\":37.900367606988134},\"accuracy\":55.69628143310547}}"'),
            bulk-string('"mac"'),
            bulk-string('"00:02:6f:aa:a3:97"')
        ])
    ]
    */

    fn from_redis_value(v: redis::Value) -> Result<YandexData, ParsingError> {
        // Tile38 pipe sends an empty string when there is no data
        if let redis::Value::BulkString(ref bulk_string) = v {
            if bulk_string.len() == 0 {
                return Err(ParsingError::from(REDIS_NO_DATA));
            }
        }

        // discard first element bulk-string('"{\"type\":\"Point\",\"coordinates\":[55.68643406911036,37.900367606988134]}"'),
        let mut array_it = v
            .as_sequence()
            .ok_or_else(|| not_convertible_error!(v, ""))?
            .iter()
            .skip(1);

        match array_it.next() {
            Some(fields) => {
                let mut fields_it = fields
                    .as_sequence()
                    .ok_or_else(|| not_convertible_error!(fields, ""))?
                    .iter()
                    .skip(1)
                    .step_by(2);

                let data = match fields_it.next() {
                    Some(data_value) => match data_value {
                        Value::BulkString(ylr_bytes) => {
                            match serde_json::from_slice::<YandexLbsResponse>(&ylr_bytes) {
                                Err(e) => {
                                    error!("deserialize YandexLbsResponse from BulkString: {}", e);
                                    Err(not_convertible_error!(
                                        data_value,
                                        "expect an YandexLbsResponse"
                                    ))
                                }
                                Ok(ylr) => Ok(ylr),
                            }
                        }
                        _ => Err(not_convertible_error!(
                            data_value,
                            "expect an bulk-string data"
                        )),
                    },
                    None => {
                        return Err(not_convertible_error!(
                            fields_it,
                            "YandexLbsResponse is None"
                        ));
                    }
                };

                let mac = match fields_it.next() {
                    Some(d) => match d {
                        Value::BulkString(mac) => String::from_utf8(mac.clone())
                            .map_err(|e| not_convertible_error!(d, e.to_string())),
                        _ => Err(not_convertible_error!(d, "MAC address must be a string")),
                    },
                    None => {
                        return Err(not_convertible_error!(fields_it, "MAC address is None"));
                    }
                };

                match (data, mac) {
                    (Ok(ylr), Ok(m)) => {
                        let yandex_data = YandexData { ylr, mac: m };
                        Ok(yandex_data)
                    }
                    _ => Err(not_convertible_error!(
                        v,
                        "YandexLbsResponse or MAC address is invalid"
                    )),
                }
            }
            None => Err(not_convertible_error!(v, "expect an arbitrary binary data")),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct YandexLbsResponse {
    pub location: YandexLocation,
}

/*
impl FromRedisValue for YandexLbsResponse {
    /*
        bulk-string('"{\"location\":{\"point\":{\"lat\":55.68643406911036,\"lon\":37.900367606988134},\"accuracy\":55.69628143310547}}"')

    */
    fn from_redis_value(v: redis::Value) -> Result<YandexLbsResponse, ParsingError> {
        if let redis::Value::BulkString(ref bulk_string) = v {
            // empty response:
            // crud.go, row 972, return empty bulk-string:
            // return resp.StringValue(""), nil
            // alternative for ID_NOT_FOUND_ERROR
            if bulk_string.len() == 0 {
                return Err(ParsingError::from(REDIS_NO_DATA));
            }

            match serde_json::from_slice::<YandexLbsResponse>(&bulk_string) {
                Err(e) => {
                    error!("deserialize YandexLbsResponse from BulkString: {}", e);
                    return Err(not_convertible_error!(
                        "",
                        "Expect an arbitrary binary data"
                    ));
                }
                Ok(ylr) => {
                    return Ok(ylr);
                }
            }
        }
        Err(ParsingError::from(REDIS_NO_DATA))
    }
}
*/

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct YandexLocation {
    pub point: YandexPoint,
    pub accuracy: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct YandexPoint {
    pub lat: f64,
    pub lon: f64,
}

pub async fn yandex_lbs_request(
    config: &Config,
    wms: Vec<WifiMeasurement>,
) -> Result<YandexLbsResponse, anyhow::Error> {
    let yandex_lbs_url = format!("{}{}", config.yandex_lbs.url, config.yandex_lbs.api_keys[0]);
    let yandex_lbs_request = YandexLbsRequest { wifi: wms };
    let yandex_lbs_response: YandexLbsResponse = reqwest::Client::new()
        .post(yandex_lbs_url)
        .json(&yandex_lbs_request)
        .send()
        .await?
        .json()
        .await?;
    Ok(yandex_lbs_response)
}

pub async fn yandex_lbs_request_by_individual_wifi(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    _config: &Config,
    wms: Vec<WifiMeasurement>,
    yandex_client: reqwest::Client,
    tx_yandex_api: flume::Sender<YandexApiMessage>,
    rl_app: RateLimitersApp,
) -> Result<HashMap<String, Option<YandexLbsResponse>>, ApiError> {
    let mut lbs_responses: HashMap<String, Option<YandexLbsResponse>> =
        HashMap::with_capacity(wms.len());

    let collection = Collection::LbsYandexWifi.as_ref();

    for wm in wms {
        let mac = wm.bssid.clone();
        // check whether the specified access point is in the database
        match get_yandex_lbs_wifi_one(tx_t38_conn.clone(), collection, &wm.bssid).await {
            Err(_e) => {
                // don`t repeat the request in Yandex LBS
                lbs_responses.insert(mac, None);
                continue;
            }
            Ok(ylr_opt) => {
                if ylr_opt.is_some() {
                    // retrieve a previously saved Yandex response from the database
                    lbs_responses.insert(mac, ylr_opt);
                    continue;
                }
                // in case of None make a request to Yandex LBS
            }
        }

        /*
            // temporarily for Yandex revalidation via AlterGeo
            lbs_responses.insert(mac, None);
            continue;
        */

        let yandex_lbs_request = YandexLbsRequest { wifi: vec![wm] };

        let (tx, rx) = tokio::sync::oneshot::channel();
        let yandex_api_message = YandexApiMessage::GetApiKey { tx };

        if let Err(e) = tx_yandex_api.send_async(yandex_api_message).await {
            error!("send yandex api message: {}", e);
            lbs_responses.insert(mac, None);
            continue;
        }

        if let Ok(Some(api_key)) = rx.await {
            let yandex_lbs_url = format!("{}{}", CONFIG.yandex_lbs.url, &api_key.key);

            // Acquire permit before processing request
            // Permit released automatically when the handler is completed
            let _permit = rl_app.yandex_lbs.acquire().await;

            let count_attempts = 5;
            let mut attempt = 0;
            while attempt <= count_attempts {
                attempt = attempt + 1;

                match yandex_client
                    .post(&yandex_lbs_url)
                    .json(&yandex_lbs_request)
                    .send()
                    .await
                {
                    Err(e) => {
                        // undefined request error
                        error!("Yandex LBS request for MAC '{}': {}", mac.clone(), e);
                        return Err(ApiError::LbsRequestError());
                    }
                    Ok(response) => {
                        // 400, 403, 429, 500, 504
                        let status = response.status().as_u16();
                        if status != 200 {
                            // repeat the request in Yandex LBS
                            if status == 429 || status == 500 || status == 504 {
                                if attempt == count_attempts {
                                    return Err(ApiError::LbsError(status));
                                }
                                info!("mac: {}, status: {}, attempt: {}", &mac, status, attempt);
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                continue;
                            }
                            if status == 403 {
                                info!("Yandex: number of requests has been exceeded");
                                let yandex_api_message_invalid_key =
                                    YandexApiMessage::InvalidApiKey {
                                        invalid_api_key: InvalidApiKey {
                                            error: Some(format!("StatusCode {}", status)),
                                            i: api_key.i,
                                            key: api_key.key,
                                        },
                                    };
                                if let Err(e) = tx_yandex_api
                                    .send_async(yandex_api_message_invalid_key)
                                    .await
                                {
                                    error!("send yandex api invalid key message: {}", e);
                                }
                            }
                            info!("MAC: {}, Yandex response status: {}", mac, status);
                            return Err(ApiError::LbsError(status));
                        }

                        match response.json::<serde_json::Value>().await {
                            Err(e) => {
                                error!("deserialize Yandex LBS response: {}", e);
                                lbs_responses.insert(mac.clone(), None);
                            }
                            Ok(value) => {
                                // successful response
                                if let serde_json::Value::Object(ref object) = value {
                                    if object.is_empty() {
                                        // no data available for the requested access point
                                        lbs_responses.insert(mac.clone(), None);
                                        // exit from while loop
                                        break;
                                    }
                                }

                                if let Ok(yandex_lbs_response) =
                                    serde_json::from_value::<YandexLbsResponse>(value)
                                {
                                    let ylr = yandex_lbs_response.clone();
                                    lbs_responses.insert(mac.clone(), Some(ylr));

                                    // save Yandex LBS response in our database
                                    if let Err(e) = set_yandex_lbs_wifi_one(
                                        tx_t38_conn.clone(),
                                        collection,
                                        yandex_lbs_response,
                                        &mac,
                                    )
                                    .await
                                    {
                                        error!("save Yandex LBS response: {}", e);
                                    }
                                }
                            }
                        }
                        // exit from while loop
                        break;
                    }
                }
            }
        } else {
            return Err(ApiError::LbsError(403));
        }
    }

    Ok(lbs_responses)
}

pub async fn yandex_lbs_request_by_individual_wifi_pipe(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    _config: &Config,
    wms: Vec<WifiMeasurement>,
    yandex_client: reqwest::Client,
    tx_yandex_api: flume::Sender<YandexApiMessage>,
) -> Result<HashMap<String, Option<YandexLbsResponse>>, ApiError> {
    let mut lbs_responses: HashMap<String, Option<YandexLbsResponse>> =
        HashMap::with_capacity(wms.len());

    let collection = Collection::LbsYandexWifi.as_ref();

    let mut macs: Vec<&str> = Vec::with_capacity(wms.len());
    wms.iter().for_each(|w| {
        macs.push(&w.bssid);
    });

    let mut yandex_data_filter = vec![];
    match get_wifi_many_from_pipeline::<YandexData>(tx_t38_conn.clone(), collection, &macs).await {
        Err(e) => {
            error!("get Yandex wifi from pipeline: {}", e);
        }
        Ok(yandex_data_vec) => {
            yandex_data_filter = yandex_data_vec
                .into_iter()
                .filter(|yd| yd.is_some())
                .flat_map(|yd| yd)
                .collect::<Vec<_>>();
        }
    }

    for wm in wms {
        if let Some(yd) = yandex_data_filter.iter().find(|&yd| yd.mac == wm.bssid) {
            lbs_responses.insert(yd.mac.clone(), Some(yd.ylr.clone()));
        } else {
            let mac = wm.bssid.clone();
            let yandex_lbs_request = YandexLbsRequest { wifi: vec![wm] };

            let (tx, rx) = tokio::sync::oneshot::channel();
            let yandex_api_message = YandexApiMessage::GetApiKey { tx };

            if let Err(e) = tx_yandex_api.send_async(yandex_api_message).await {
                error!("send yandex api message: {}", e);
                lbs_responses.insert(mac, None);
                continue;
            }

            if let Ok(Some(api_key)) = rx.await {
                let yandex_lbs_url = format!("{}{}", CONFIG.yandex_lbs.url, &api_key.key);

                let count_attempts = 5;
                let mut attempt = 0;
                while attempt <= count_attempts {
                    attempt = attempt + 1;

                    match yandex_client
                        .post(&yandex_lbs_url)
                        .json(&yandex_lbs_request)
                        .send()
                        .await
                    {
                        Err(e) => {
                            // undefined request error
                            error!("Yandex LBS request for MAC '{}': {}", &mac, e);
                            lbs_responses.insert(mac.clone(), None);

                            let yandex_api_message_invalid_key = YandexApiMessage::InvalidApiKey {
                                invalid_api_key: InvalidApiKey {
                                    error: Some(e.to_string()),
                                    i: api_key.i,
                                    key: api_key.key.clone(),
                                },
                            };
                            if let Err(e) = tx_yandex_api
                                .send_async(yandex_api_message_invalid_key)
                                .await
                            {
                                error!("send yandex api invalid key message: {}", e);
                            }
                        }
                        Ok(response) => {
                            // 400, 403, 429, 500, 504
                            let status = response.status().as_u16();
                            if status != 200 {
                                // repeat the request in Yandex LBS
                                if status == 429 || status == 500 || status == 504 {
                                    if attempt == count_attempts {
                                        return Err(ApiError::LbsError(status));
                                    }
                                    info!(
                                        "mac: {}, status: {}, attempt: {}",
                                        &mac, status, attempt
                                    );
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                    continue;
                                }
                                if status == 403 {
                                    info!("Yandex: number of requests has been exceeded");
                                    let yandex_api_message_invalid_key =
                                        YandexApiMessage::InvalidApiKey {
                                            invalid_api_key: InvalidApiKey {
                                                error: Some(format!("StatusCode {}", status)),
                                                i: api_key.i,
                                                key: api_key.key,
                                            },
                                        };
                                    if let Err(e) = tx_yandex_api
                                        .send_async(yandex_api_message_invalid_key)
                                        .await
                                    {
                                        error!("send yandex api invalid key message: {}", e);
                                    }
                                }
                                info!("MAC: {}, Yandex response status: {}", &mac, status);
                                return Err(ApiError::LbsError(status));
                            }

                            match response.json::<serde_json::Value>().await {
                                Err(e) => {
                                    // TODO: disable error message after tests
                                    error!("deserialize Yandex LBS response: {}", e);
                                    lbs_responses.insert(mac.clone(), None);
                                }
                                Ok(value) => {
                                    // successful response
                                    if let serde_json::Value::Object(ref object) = value {
                                        if object.is_empty() {
                                            // no data available for the requested access point
                                            lbs_responses.insert(mac.clone(), None);
                                            continue;
                                        }
                                    }

                                    if let Ok(yandex_lbs_response) =
                                        serde_json::from_value::<YandexLbsResponse>(value)
                                    {
                                        let ylr = yandex_lbs_response.clone();
                                        lbs_responses.insert(mac.clone(), Some(ylr));

                                        // save Yandex LBS response in our database
                                        if let Err(e) = set_yandex_lbs_wifi_one(
                                            tx_t38_conn.clone(),
                                            collection,
                                            yandex_lbs_response,
                                            &mac,
                                        )
                                        .await
                                        {
                                            error!("save Yandex LBS response: {}", e);
                                        }
                                    }
                                }
                            }
                            // exit from while loop
                            break;
                        }
                    }
                }
            } else {
                return Err(ApiError::LbsError(403));
            }
        }
    }

    Ok(lbs_responses)
}

#[derive(Debug, Clone)]
pub struct OutliersYandex {
    pub outliers: HashMap<String, YandexLbsResponse>,
}

pub fn detect_yandex_outliers(
    yandex_lbs_responses: &HashMap<String, Option<YandexLbsResponse>>,
    cell_opt: Option<HashMap<String, Option<YandexLbsResponse>>>,
    _wifi_track: Option<WifiTrack>,
) -> Option<OutliersYandex> {
    let ylrs_filtered = yandex_lbs_responses
        .iter()
        .filter(|(_mac, ylr)| ylr.is_some())
        .collect::<HashMap<_, _>>();

    let cell = cell_opt.unwrap_or_default();
    let cell_points = create_cell_points(&cell);

    let cap = ylrs_filtered.len();
    let mut points: Vec<Point> = Vec::with_capacity(cap);
    let mut macs: Vec<&str> = Vec::with_capacity(cap);

    ylrs_filtered
        .iter()
        .enumerate()
        .for_each(|(i, (&mac, &ylr_opt))| {
            if let Some(ylr) = ylr_opt {
                points.push(Point {
                    id: i as u32,
                    lat: ylr.location.point.lat,
                    lon: ylr.location.point.lon,
                });
                macs.push(mac);
            }
        });

    if ylrs_filtered.len() == 1 {
        if cell_points.len() > 0 {
            let distance_point_cell = points[0].distance(&cell_points[0]);
            if distance_point_cell > CONFIG.locator.max_distance_cell {
                let mut outliers = HashMap::with_capacity(1);
                macs.iter().for_each(|&mac| {
                    if let Some(ylr_opt) = yandex_lbs_responses.get(mac) {
                        if let Some(ylr) = ylr_opt {
                            outliers.insert(mac.to_string(), ylr.clone());
                        }
                    }
                });
                return Some(OutliersYandex { outliers });
            }
        }
        return None;
    }

    if points.len() == 2 {
        let d = points[0].distance(&points[1]);
        if d > CONFIG.yandex_lbs.max_distance_in_cluster {
            let mut outliers = HashMap::with_capacity(2);
            macs.iter().for_each(|&mac| {
                if let Some(ylr_opt) = yandex_lbs_responses.get(mac) {
                    if let Some(ylr) = ylr_opt {
                        if cell_points.len() > 0 {
                            let yandex_point = Point {
                                id: 0,
                                lat: ylr.location.point.lat,
                                lon: ylr.location.point.lon,
                            };
                            let distance_point_cell = yandex_point.distance(&cell_points[0]);
                            if distance_point_cell > CONFIG.locator.max_distance_cell {
                                outliers.insert(mac.to_string(), ylr.clone());
                            }
                        } else {
                            // without data on base stations, ignore both points
                            outliers.insert(mac.to_string(), ylr.clone());
                        }
                    }
                }
            });
            if outliers.len() > 0 {
                return Some(OutliersYandex { outliers });
            } else {
                return None;
            }
        }
        return None;
    }

    let mut discarded_by_cell_points = Vec::new();
    let mut filtered_by_cell_points = Vec::new();
    if cell_points.len() > 0 {
        points.iter().for_each(|p| {
            let distance_point_cell = p.distance(&cell_points[0]);
            if distance_point_cell > CONFIG.locator.max_distance_cell {
                discarded_by_cell_points.push(p.clone());
            } else {
                filtered_by_cell_points.push(p.clone());
            }
        });

        // all points outside the BS service area
        // TODO: incorrect base station coordinates?
        if discarded_by_cell_points.len() > 0 && discarded_by_cell_points.len() == points.len() {
            filtered_by_cell_points = points;
            discarded_by_cell_points.clear();
        }
    } else {
        filtered_by_cell_points = points;
    }

    let algorithm = DBSCAN::new(CONFIG.yandex_lbs.max_distance_in_cluster, 0);
    let clusters = algorithm.cluster(&filtered_by_cell_points);
    let actual_clusters = clusters.clusters();
    let mut noise = clusters.noise();

    // add points discarded by BS
    noise.append(&mut discarded_by_cell_points);

    if actual_clusters.len() > 1 {
        let mut main_cluster_id = 0;
        let mut main_cluster_len = 0;
        for (i, cluster) in actual_clusters.iter().enumerate() {
            let cl = cluster.len();
            if cl > main_cluster_len {
                main_cluster_id = i;
                main_cluster_len = cl;
            }
        }
        // the main cluster is the one with the most elements
        for (i, mut cluster) in actual_clusters.into_iter().enumerate() {
            if i == main_cluster_id {
                continue;
            }
            noise.append(&mut cluster);
        }
    }

    if noise.len() == 0 {
        None
    } else {
        let mut outliers = HashMap::with_capacity(noise.len());
        noise.into_iter().for_each(|p| {
            let mac = macs[p.id as usize];
            if let Some(ylr_opt) = ylrs_filtered.get(&mac.to_string()) {
                if let Some(ylr) = ylr_opt {
                    outliers.insert(mac.to_string(), ylr.clone());
                }
            }
        });
        Some(OutliersYandex { outliers })
    }
}

pub fn check_point_by_track(
    wifi_track: Option<&WifiTrack>,
    ylrs_filtered: HashMap<&String, &Option<YandexLbsResponse>>,
) -> Option<OutliersYandex> {
    if let Some(wt) = wifi_track {
        // wifi track is sorted in ascending order, with the latest track entry being the most recent
        if let Some(wrt_last) = wt.records.last() {
            let ts_now = chrono::Utc::now().timestamp();
            let mut outliers = HashMap::new();

            for (mac, ylr_opt) in ylrs_filtered {
                if let Some(ylr) = ylr_opt {
                    if wrt_last.wifi.len() > 0 {
                        // diff_ts in hours
                        let diff_ts = (ts_now - wrt_last.ts) as f64 / HOUR as f64;
                        // max distance in meters
                        let d_max = f64::min(MAX_DISTANCE, MAX_SCOOTER_SPEED * diff_ts * 1000.0);
                        let p_last = Point {
                            id: 0,
                            lat: wrt_last.wifi[0].g.lat,
                            lon: wrt_last.wifi[0].g.lon,
                        };
                        let p_yandex = Point {
                            id: 1,
                            lat: ylr.location.point.lat,
                            lon: ylr.location.point.lon,
                        };
                        let d_yandex_last = p_yandex.distance(&p_last);
                        if d_yandex_last > d_max {
                            outliers.insert(mac.clone(), ylr.clone());
                        }
                    }
                }
            }
            if outliers.len() > 0 {
                return Some(OutliersYandex { outliers });
            }
        }
    }
    None
}

pub async fn yandex_lbs_request_by_individual_wifi_no_save(
    _config: &Config,
    wms: Vec<WifiMeasurement>,
    debug: bool,
) -> Result<HashMap<String, Option<YandexLbsResponse>>, anyhow::Error> {
    let mut lbs_responses: HashMap<String, Option<YandexLbsResponse>> =
        HashMap::with_capacity(wms.len());

    let client = reqwest::Client::new();

    for wm in wms {
        let mac = wm.bssid.clone();
        let yandex_lbs_request = YandexLbsRequest { wifi: vec![wm] };
        match client
            .post(&*YANDEX_LBS_URL)
            .json(&yandex_lbs_request)
            .send()
            .await
        {
            Err(e) => {
                // undefined request error
                error!(
                    "Yandex LBS request for MAC '{}': {}",
                    yandex_lbs_request.wifi[0].bssid, e
                );
                lbs_responses.insert(mac, None);
            }
            Ok(response) => match response.json::<YandexLbsResponse>().await {
                Err(e) => {
                    // no data available for the requested access point
                    error!("deserialize Yandex LBS response: {}", e);
                    lbs_responses.insert(mac, None);
                }
                Ok(yandex_lbs_response) => {
                    // successful response
                    if debug {
                        println!(
                            "individual mac '{}': {:?}",
                            yandex_lbs_request.wifi[0].bssid, yandex_lbs_response
                        );
                    }
                    lbs_responses.insert(mac, Some(yandex_lbs_response));
                }
            },
        }
    }

    Ok(lbs_responses)
}

// integrated location assessment for individual access points
pub fn estimate_location_by_yandex_responses(
    yandex_lbs_responses: &HashMap<String, Option<YandexLbsResponse>>,
) -> Option<YandexLbsResponse> {
    let mut lat_weight = 0.0;
    let mut lon_weight = 0.0;
    let mut w_weight = 0.0;
    let mut accuracy = std::f64::MAX;

    for (_, ylr_opt) in yandex_lbs_responses {
        if let Some(ylr) = ylr_opt {
            lat_weight = lat_weight + ylr.location.point.lat * (1.0 / ylr.location.accuracy);
            lon_weight = lon_weight + ylr.location.point.lon * (1.0 / ylr.location.accuracy);
            w_weight = w_weight + (1.0 / ylr.location.accuracy);
            if accuracy > ylr.location.accuracy {
                accuracy = ylr.location.accuracy
            }
        }
    }

    if w_weight > 0.0 && lat_weight > 0.0 && lon_weight > 0.0 {
        Some(YandexLbsResponse {
            location: YandexLocation {
                point: YandexPoint {
                    lat: lat_weight / w_weight,
                    lon: lon_weight / w_weight,
                },
                accuracy,
            },
        })
    } else {
        None
    }
}

#[allow(unused)]
mod tests {
    use std::collections::HashMap;

    use clusters::Proximity;

    use super::{
        CONFIG, WifiMeasurement, detect_yandex_outliers, estimate_location_by_yandex_responses,
        yandex_lbs_request, yandex_lbs_request_by_individual_wifi_no_save,
    };
    use crate::services::locate::dbscan::Point;

    const DEBUG: bool = true;

    // with debug mode
    async fn do_yandex_lbs_requests(wms: Vec<WifiMeasurement>) {
        match yandex_lbs_request(&crate::CONFIG, wms.clone()).await {
            Err(err) => {
                println!("Error Yandex LBS request: {}", err)
            }
            Ok(resp) => {
                println!("Yandex LBS location: {:?}", resp);
                match yandex_lbs_request_by_individual_wifi_no_save(&CONFIG, wms, DEBUG).await {
                    Err(e) => {
                        println!("Error Yandex LBS request by individual access points: {e}")
                    }
                    Ok(mut yandex_lbs_responses) => {
                        // remove Yandex outliers
                        let cell_opt = None;
                        let wifi_track = None;
                        let outliers_opt =
                            detect_yandex_outliers(&yandex_lbs_responses, cell_opt, wifi_track);
                        if let Some(outliers) = outliers_opt {
                            for (mac, _ylr_opt) in outliers.outliers {
                                if let Some(outlier_opt) = yandex_lbs_responses.remove(&mac) {
                                    if let Some(outlier) = outlier_opt {
                                        println!("detect Yandex outlier: {:?}", outlier);
                                    }
                                }
                            }
                        }

                        if let Some(estimated_location) =
                            estimate_location_by_yandex_responses(&yandex_lbs_responses)
                        {
                            println!(
                                "Estimated location by individual AP: {:?}",
                                estimated_location
                            );
                            let point_yandex = Point {
                                id: 0,
                                lat: resp.location.point.lat,
                                lon: resp.location.point.lon,
                            };
                            let point_estimated = Point {
                                id: 1,
                                lat: estimated_location.location.point.lat,
                                lon: estimated_location.location.point.lon,
                            };
                            let error_distance = point_yandex.distance(&point_estimated);
                            println!("Error distance, meters: {}", error_distance);
                        } else {
                            println!("Estimated location by individual AP is None");
                        }
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn test_yandex_lbs_request() {
        let wms = vec![
            WifiMeasurement {
                bssid: "ae:84:c6:a9:45:d2".to_string(),
                signal_strength: -62.0,
            },
            WifiMeasurement {
                bssid: "00:a2:b0:8c:90:e5".to_string(),
                signal_strength: -57.0,
            },
        ];
        let yandex_lbs_response = yandex_lbs_request(&crate::CONFIG, wms).await;
        match yandex_lbs_response {
            Err(err) => {
                println!("Error Yandex LBS request: {}", err)
            }
            Ok(resp) => {
                println!("Yandex LBS location: {:?}", resp)
            }
        }
    }

    #[tokio::test]
    async fn test_yandex_lbs_request_individual() {
        let wms = vec![
            WifiMeasurement {
                bssid: "ae:84:c6:a9:45:d2".to_string(),
                signal_strength: -62.0,
            },
            WifiMeasurement {
                bssid: "00:a2:b0:8c:90:e5".to_string(),
                signal_strength: -57.0,
            },
        ];
        /*
            Yandex LBS location: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.75248410167931, lon: 37.79280932042006 }, accuracy: 36.46961975097656 } }

            individual mac 'ae:84:c6:a9:45:d2': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.75248410167931, lon: 37.79280932042006 }, accuracy: 36.46961975097656 } }

            Estimated location by individual AP: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.75248410167931, lon: 37.79280932042006 }, accuracy: 36.46961975097656 } }
            Error distance, meters: 0
        */
        do_yandex_lbs_requests(wms).await;
    }

    #[tokio::test]
    async fn test_yandex_lbs_request_individual_2() {
        let wms = vec![
            WifiMeasurement {
                bssid: "50:ff:20:4b:5c:c9".to_string(),
                signal_strength: -78.0,
            },
            WifiMeasurement {
                bssid: "50:ff:20:9f:2a:be".to_string(),
                signal_strength: -65.0,
            },
            WifiMeasurement {
                bssid: "52:ff:20:ff:2a:be".to_string(),
                signal_strength: -63.0,
            },
            WifiMeasurement {
                bssid: "10:3c:59:07:29:a5".to_string(),
                signal_strength: -63.0,
            },
        ];
        /*
            Yandex LBS location: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.70779908392132, lon: 37.781316846246064 }, accuracy: 24.719993591308594 } }

            individual mac '50:ff:20:4b:5c:c9': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.70779908392132, lon: 37.781316846246064 }, accuracy: 24.719993591308594 } }
            individual mac '50:ff:20:9f:2a:be': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.70757293701172, lon: 37.78075408935547 }, accuracy: 140.0 } }
            individual mac '52:ff:20:ff:2a:be': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.707542419433594, lon: 37.78071975708008 }, accuracy: 140.0 } }
            individual mac '10:3c:59:07:29:a5': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.70766830444336, lon: 37.781211853027344 }, accuracy: 140.0 } }

            Estimated location by individual AP: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.70772825853904, lon: 37.781170848760134 }, accuracy: 24.719993591308594 } }
            Error distance, meters: 12.069894778377828
        */
        do_yandex_lbs_requests(wms).await;
    }

    #[tokio::test]
    async fn test_yandex_lbs_request_individual_3() {
        let wms = vec![
            WifiMeasurement {
                bssid: "a0:dd:6c:02:49:c0".to_string(),
                signal_strength: -81.0,
            },
            WifiMeasurement {
                bssid: "70:f8:2b:5c:33:10".to_string(),
                signal_strength: -79.0,
            },
            WifiMeasurement {
                bssid: "52:ff:20:f2:49:b1".to_string(),
                signal_strength: -64.0,
            },
            WifiMeasurement {
                bssid: "50:ff:20:c2:49:b1".to_string(),
                signal_strength: -64.0,
            },
            WifiMeasurement {
                bssid: "1c:61:b4:6b:59:18".to_string(),
                signal_strength: -62.0,
            },
            WifiMeasurement {
                bssid: "1e:61:b4:6b:59:18".to_string(),
                signal_strength: -60.0,
            },
        ];
        /*
            Yandex LBS location: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.720996190611224, lon: 37.79074113104516 }, accuracy: 24.109622955322266 } }

            individual mac 'a0:dd:6c:02:49:c0': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.72099304199219, lon: 37.79054260253906 }, accuracy: 140.0 } }
            individual mac '70:f8:2b:5c:33:10': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.721042763083105, lon: 37.791274455577984 }, accuracy: 37.99554443359375 } }
            individual mac '52:ff:20:f2:49:b1': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.721046447753906, lon: 37.79063034057617 }, accuracy: 140.0 } }
            individual mac '50:ff:20:c2:49:b1': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.72101974487305, lon: 37.79063034057617 }, accuracy: 140.0 } }
            individual mac '1c:61:b4:6b:59:18': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.72108101355771, lon: 37.79077821539954 }, accuracy: 24.109622955322266 } }
            individual mac '1e:61:b4:6b:59:18': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.72102929446199, lon: 37.79073502572218 }, accuracy: 24.109622955322266 } }

            Estimated location by individual AP: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.72104685357446, lon: 37.79083539628855 }, accuracy: 24.109622955322266 } }
            Error distance, meters: 8.160183379566377r distance, meters: 8.160183379566377
        */
        do_yandex_lbs_requests(wms).await;
    }

    #[tokio::test]
    async fn test_yandex_lbs_request_individual_4() {
        let wms = vec![
            WifiMeasurement {
                bssid: "da:af:80:75:f6:f1".to_string(),
                signal_strength: -83.0,
            },
            WifiMeasurement {
                bssid: "28:ee:52:59:6f:f0".to_string(),
                signal_strength: -81.0,
            },
            WifiMeasurement {
                bssid: "d8:07:b6:26:e6:f7".to_string(),
                signal_strength: -79.0,
            },
            WifiMeasurement {
                bssid: "28:ee:52:59:7f:44".to_string(),
                signal_strength: -78.0,
            },
            WifiMeasurement {
                bssid: "da:af:81:75:f7:ad".to_string(),
                signal_strength: -78.0,
            },
            WifiMeasurement {
                bssid: "84:d8:1b:ef:f6:76".to_string(),
                signal_strength: -76.0,
            },
            WifiMeasurement {
                bssid: "84:d8:1b:f0:07:ad".to_string(),
                signal_strength: -76.0,
            },
            WifiMeasurement {
                bssid: "da:af:80:75:f7:8d".to_string(),
                signal_strength: -76.0,
            },
            WifiMeasurement {
                bssid: "28:ee:52:59:a0:c8".to_string(),
                signal_strength: -75.0,
            },
            WifiMeasurement {
                bssid: "c4:34:6b:f1:24:f8".to_string(),
                signal_strength: -75.0,
            },
            WifiMeasurement {
                bssid: "84:d8:1b:ef:f3:ee".to_string(),
                signal_strength: -71.0,
            },
            WifiMeasurement {
                bssid: "84:d8:1b:ef:f4:03".to_string(),
                signal_strength: -70.0,
            },
        ];
        /*
            Yandex LBS location: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.65232228272678, lon: 37.50738539614436 }, accuracy: 29.908140182495117 } }

            individual mac 'da:af:80:75:f6:f1': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.652437860567275, lon: 37.5069795189269 }, accuracy: 29.908140182495117 } }
            individual mac '28:ee:52:59:6f:f0': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.652239713891575, lon: 37.50719434473688 }, accuracy: 29.908140182495117 } }
            individual mac 'd8:07:b6:26:e6:f7': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.65202916511373, lon: 37.50741577658153 }, accuracy: 29.908140182495117 } }
            individual mac '28:ee:52:59:7f:44': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.652235482203885, lon: 37.50819249574688 }, accuracy: 29.908140182495117 } }
            individual mac 'da:af:81:75:f7:ad': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.65221382366053, lon: 37.50715074325165 }, accuracy: 29.908140182495117 } }
            individual mac '84:d8:1b:ef:f6:76': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.652214741629685, lon: 37.50732057720361 }, accuracy: 29.908140182495117 } }
            individual mac '84:d8:1b:f0:07:ad': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.65239915469976, lon: 37.507568226439865 }, accuracy: 29.908140182495117 } }
            individual mac 'da:af:80:75:f7:8d': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.65258989987602, lon: 37.50834081631198 }, accuracy: 29.908140182495117 } }
            individual mac '28:ee:52:59:a0:c8': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.65138878134035, lon: 37.505469405163296 }, accuracy: 29.908140182495117 } }
            individual mac '84:d8:1b:ef:f3:ee': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.6523489222717, lon: 37.507529315714386 }, accuracy: 29.908140182495117 } }
            individual mac '84:d8:1b:ef:f4:03': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.6523495905465, lon: 37.50752773441103 }, accuracy: 29.908140182495117 } }

            Estimated location by individual AP: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.65222246689099, lon: 37.5073353594989 }, accuracy: 29.908140182495117 } }
            Error distance, meters: 11.53442579589515
        */
        do_yandex_lbs_requests(wms).await;
    }

    // Outlier Enem Adygeya
    #[tokio::test]
    async fn test_yandex_lbs_request_individual_5() {
        let wms = vec![
            WifiMeasurement {
                bssid: "f2:a7:31:21:2d:7a".to_string(),
                signal_strength: -93.0,
            },
            WifiMeasurement {
                bssid: "ce:2d:e0:5e:c3:9f".to_string(),
                signal_strength: -76.0,
            },
            WifiMeasurement {
                bssid: "50:ff:20:d6:01:89".to_string(),
                signal_strength: -75.0,
            },
            WifiMeasurement {
                bssid: "18:fd:74:1e:70:93".to_string(),
                signal_strength: -75.0,
            },
            // ======== Outlier Enem Adygeya ========
            WifiMeasurement {
                bssid: "52:ff:20:f6:01:89".to_string(),
                signal_strength: -74.0,
            },
            // ======================================
            WifiMeasurement {
                bssid: "72:8b:ac:1e:d2:fd".to_string(),
                signal_strength: -74.0,
            },
            WifiMeasurement {
                bssid: "1c:61:b4:d1:89:e2".to_string(),
                signal_strength: -57.0,
            },
        ];
        /*
            Yandex LBS location: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 53.94493534230817, lon: 37.74566350448864 }, accuracy: 50.0 } }

            individual mac 'f2:a7:31:21:2d:7a': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.012383804484784, lon: 37.478643195928626 }, accuracy: 29.29776954650879 } }
            individual mac '50:ff:20:d6:01:89': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.01203536987305, lon: 37.4781608581543 }, accuracy: 140.0 } }
            individual mac '18:fd:74:1e:70:93': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.012468433653986, lon: 37.47830429808279 }, accuracy: 29.29776954650879 } }
            individual mac '52:ff:20:f6:01:89': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 44.92532577689285, lon: 38.91061576553182 }, accuracy: 29.14517593383789 } }
            individual mac '72:8b:ac:1e:d2:fd': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.01240205257118, lon: 37.47842345450153 }, accuracy: 29.29776954650879 } }
            individual mac '1c:61:b4:d1:89:e2': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.01179298392634, lon: 37.479671545569055 }, accuracy: 29.29776954650879 } }
            detect Yandex outlier: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 44.92532577689285, lon: 38.91061576553182 }, accuracy: 29.14517593383789 } }

            Estimated location by individual AP: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.012250560438375, lon: 37.47873080533962 }, accuracy: 29.29776954650879 } }
            Error distance, meters: 230505.03158639238
        */
        do_yandex_lbs_requests(wms).await;
    }

    #[tokio::test]
    async fn test_yandex_lbs_request_individual_6() {
        let wms = vec![
            WifiMeasurement {
                bssid: "f2:a7:31:21:2d:7a".to_string(),
                signal_strength: -93.0,
            },
            WifiMeasurement {
                bssid: "ce:2d:e0:5e:c3:9f".to_string(),
                signal_strength: -76.0,
            },
            WifiMeasurement {
                bssid: "50:ff:20:d6:01:89".to_string(),
                signal_strength: -75.0,
            },
            WifiMeasurement {
                bssid: "18:fd:74:1e:70:93".to_string(),
                signal_strength: -75.0,
            },
            WifiMeasurement {
                bssid: "72:8b:ac:1e:d2:fd".to_string(),
                signal_strength: -74.0,
            },
            WifiMeasurement {
                bssid: "1c:61:b4:d1:89:e2".to_string(),
                signal_strength: -57.0,
            },
        ];
        /*
            Yandex LBS location: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.01247524692217, lon: 37.47855142328488 }, accuracy: 29.29776954650879 } }

            individual mac 'f2:a7:31:21:2d:7a': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.012383804484784, lon: 37.478643195928626 }, accuracy: 29.29776954650879 } }
            individual mac '50:ff:20:d6:01:89': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.01203536987305, lon: 37.4781608581543 }, accuracy: 140.0 } }
            individual mac '18:fd:74:1e:70:93': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.012468433653986, lon: 37.47830429808279 }, accuracy: 29.29776954650879 } }
            individual mac '72:8b:ac:1e:d2:fd': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.01240205257118, lon: 37.47842345450153 }, accuracy: 29.29776954650879 } }
            individual mac '1c:61:b4:d1:89:e2': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.01179298392634, lon: 37.479671545569055 }, accuracy: 29.29776954650879 } }

            Estimated location by individual AP: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.012250560438375, lon: 37.47873080533962 }, accuracy: 29.29776954650879 } }
            Error distance, meters: 27.359302400115965
        */
        do_yandex_lbs_requests(wms).await;
    }

    #[tokio::test]
    async fn test_yandex_lbs_request_individual_7() {
        let wms = vec![
            WifiMeasurement {
                bssid: "aa:a9:30:b9:d1:be".to_string(),
                signal_strength: -81.0,
            },
            WifiMeasurement {
                bssid: "a4:a9:30:b9:d1:be".to_string(),
                signal_strength: -76.0,
            },
            WifiMeasurement {
                bssid: "60:31:97:eb:0a:38".to_string(),
                signal_strength: -75.0,
            },
        ];
        /*
            Yandex LBS location: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.00336879733515, lon: 37.45118444668652 }, accuracy: 38.758506774902344 } }

            individual mac 'aa:a9:30:b9:d1:be': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.00313341959473, lon: 37.45090200582552 }, accuracy: 38.758506774902344 } }
            individual mac 'a4:a9:30:b9:d1:be': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.00327384698639, lon: 37.45115473435263 }, accuracy: 38.758506774902344 } }
            individual mac '60:31:97:eb:0a:38': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.0039914393215, lon: 37.44892133030629 }, accuracy: 32.807395935058594 } }

            Estimated location by individual AP: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.00349618115274, lon: 37.45024593131636 }, accuracy: 32.807395935058594 } }
            Error distance, meters: 60.04580044663918
        */
        do_yandex_lbs_requests(wms).await;
    }

    // Outlier "52:ff:20:fe:f3:f2"
    #[tokio::test]
    async fn test_yandex_lbs_request_individual_8() {
        let wms = vec![
            WifiMeasurement {
                bssid: "50:ff:20:31:92:66".to_string(),
                signal_strength: -80.0,
            },
            WifiMeasurement {
                bssid: "f0:a7:31:ab:5a:53".to_string(),
                signal_strength: -78.0,
            },
            WifiMeasurement {
                bssid: "38:6b:1c:30:f0:d6".to_string(),
                signal_strength: -76.0,
            },
            WifiMeasurement {
                bssid: "f2:a7:31:ab:5a:53".to_string(),
                signal_strength: -75.0,
            },
            WifiMeasurement {
                bssid: "52:ff:20:ff:b1:a7".to_string(),
                signal_strength: -72.0,
            },
            WifiMeasurement {
                bssid: "50:ff:20:de:f3:f2".to_string(),
                signal_strength: -71.0,
            },
            WifiMeasurement {
                bssid: "52:ff:20:fe:f3:f2".to_string(),
                signal_strength: -71.0,
            },
            WifiMeasurement {
                bssid: "50:ff:20:cf:b1:a7".to_string(),
                signal_strength: -71.0,
            },
            WifiMeasurement {
                bssid: "8a:57:4a:e5:59:3d".to_string(),
                signal_strength: -60.0,
            },
        ];
        /*
            Yandex LBS location: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.0126810819504, lon: 37.48264901644817 }, accuracy: 29.602954864501953 } }

            individual mac '50:ff:20:31:92:66': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.012799644722286, lon: 37.48317581240861 }, accuracy: 29.602954864501953 } }
            individual mac 'f0:a7:31:ab:5a:53': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.01258308373989, lon: 37.48241651489407 }, accuracy: 29.602954864501953 } }
            individual mac '38:6b:1c:30:f0:d6': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.013221402698015, lon: 37.483082579398186 }, accuracy: 29.602954864501953 } }
            individual mac 'f2:a7:31:ab:5a:53': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.01255174685367, lon: 37.48252034993136 }, accuracy: 29.602954864501953 } }
            individual mac '50:ff:20:de:f3:f2': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.01296615600586, lon: 37.48269271850586 }, accuracy: 140.0 } }
            individual mac '52:ff:20:fe:f3:f2': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.572170594494814, lon: 37.46972175754701 }, accuracy: 54.32294845581055 } }
            individual mac '50:ff:20:cf:b1:a7': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.01292419433594, lon: 37.482666015625 }, accuracy: 140.0 } }
            detect Yandex outlier: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.572170594494814, lon: 37.46972175754701 }, accuracy: 54.32294845581055 } }

            Estimated location by individual AP: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.012803905243736, lon: 37.482787393120766 }, accuracy: 29.602954864501953 } }
            Error distance, meters: 16.140210908265963
        */
        do_yandex_lbs_requests(wms).await;
    }

    // Remove outlier "52:ff:20:fe:f3:f2"
    #[tokio::test]
    async fn test_yandex_lbs_request_individual_9() {
        let wms = vec![
            WifiMeasurement {
                bssid: "50:ff:20:31:92:66".to_string(),
                signal_strength: -80.0,
            },
            WifiMeasurement {
                bssid: "f0:a7:31:ab:5a:53".to_string(),
                signal_strength: -78.0,
            },
            WifiMeasurement {
                bssid: "38:6b:1c:30:f0:d6".to_string(),
                signal_strength: -76.0,
            },
            WifiMeasurement {
                bssid: "f2:a7:31:ab:5a:53".to_string(),
                signal_strength: -75.0,
            },
            WifiMeasurement {
                bssid: "52:ff:20:ff:b1:a7".to_string(),
                signal_strength: -72.0,
            },
            WifiMeasurement {
                bssid: "50:ff:20:de:f3:f2".to_string(),
                signal_strength: -71.0,
            },
            WifiMeasurement {
                bssid: "50:ff:20:cf:b1:a7".to_string(),
                signal_strength: -71.0,
            },
            WifiMeasurement {
                bssid: "8a:57:4a:e5:59:3d".to_string(),
                signal_strength: -60.0,
            },
        ];
        /*
            Yandex LBS location: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.0126810819504, lon: 37.48264901644817 }, accuracy: 29.602954864501953 } }

            individual mac '50:ff:20:31:92:66': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.012799644722286, lon: 37.48317581240861 }, accuracy: 29.602954864501953 } }
            individual mac 'f0:a7:31:ab:5a:53': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.01258308373989, lon: 37.48241651489407 }, accuracy: 29.602954864501953 } }
            individual mac '38:6b:1c:30:f0:d6': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.013221402698015, lon: 37.483082579398186 }, accuracy: 29.602954864501953 } }
            individual mac 'f2:a7:31:ab:5a:53': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.01255174685367, lon: 37.48252034993136 }, accuracy: 29.602954864501953 } }
            individual mac '50:ff:20:de:f3:f2': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.01296615600586, lon: 37.48269271850586 }, accuracy: 140.0 } }
            individual mac '50:ff:20:cf:b1:a7': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.01292419433594, lon: 37.482666015625 }, accuracy: 140.0 } }

            Estimated location by individual AP: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 56.012803905243736, lon: 37.482787393120766 }, accuracy: 29.602954864501953 } }
            Error distance, meters: 16.140210908265963
        */
        do_yandex_lbs_requests(wms).await;
    }

    // ts = 1758982178, mac '00904cc10000' present in the main database
    #[tokio::test]
    async fn test_yandex_lbs_request_individual_10() {
        let wms = vec![
            WifiMeasurement {
                bssid: "089ac7b2a2f8".to_string(),
                signal_strength: -80.0,
            },
            WifiMeasurement {
                bssid: "50ff2004b608".to_string(),
                signal_strength: -78.0,
            },
            WifiMeasurement {
                bssid: "a0cff5c77162".to_string(),
                signal_strength: -76.0,
            },
            WifiMeasurement {
                bssid: "0c37479fab67".to_string(),
                signal_strength: -75.0,
            },
            WifiMeasurement {
                bssid: "cc32e5625061".to_string(),
                signal_strength: -72.0,
            },
            WifiMeasurement {
                bssid: "4829e4147569".to_string(),
                signal_strength: -71.0,
            },
            WifiMeasurement {
                bssid: "58ea1f3a54f6".to_string(),
                signal_strength: -71.0,
            },
            WifiMeasurement {
                bssid: "00904cc10000".to_string(),
                signal_strength: -71.0,
            },
            WifiMeasurement {
                bssid: "e03f498b051c".to_string(),
                signal_strength: -60.0,
            },
            WifiMeasurement {
                bssid: "000e8fef5ecc".to_string(),
                signal_strength: -60.0,
            },
            WifiMeasurement {
                bssid: "24a65ea9dc80".to_string(),
                signal_strength: -60.0,
            },
        ];
        /*
            Yandex LBS location: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.56183068380736, lon: 37.58262912173469 }, accuracy: 29.908140182495117 } }

            individual mac '089ac7b2a2f8': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.56171631855045, lon: 37.58370351107839 }, accuracy: 29.908140182495117 } }
            individual mac '50ff2004b608': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.56151732412798, lon: 37.58321072298568 }, accuracy: 37.38517379760742 } }
            individual mac 'a0cff5c77162': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.86615696937176, lon: 37.44420018587141 }, accuracy: 28.382213592529297 } }
            individual mac '0c37479fab67': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.56150750269601, lon: 37.58276553836352 }, accuracy: 37.38517379760742 } }
            individual mac 'cc32e5625061': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.56226152847327, lon: 37.58379700048492 }, accuracy: 29.908140182495117 } }
            individual mac '4829e4147569': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.56183516077993, lon: 37.582711459743685 }, accuracy: 29.908140182495117 } }
            individual mac '58ea1f3a54f6': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.56187057495117, lon: 37.58335876464844 }, accuracy: 140.0 } }
            individual mac '00904cc10000': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 59.93345832824707, lon: 30.439199447631836 }, accuracy: 140.0 } }
            individual mac 'e03f498b051c': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.56190379675357, lon: 37.58271768938091 }, accuracy: 29.908140182495117 } }
            individual mac '000e8fef5ecc': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.561835421024824, lon: 37.582639017138675 }, accuracy: 29.908140182495117 } }
            individual mac '24a65ea9dc80': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.56184692074344, lon: 37.583416876862216 }, accuracy: 29.908140182495117 } }

            detect Yandex outlier: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.86615696937176, lon: 37.44420018587141 }, accuracy: 28.382213592529297 } }
            detect Yandex outlier: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 59.93345832824707, lon: 30.439199447631836 }, accuracy: 140.0 } }
            Estimated location by individual AP: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.56181971998557, lon: 37.58313351112797 }, accuracy: 29.908140182495117 } }
            Error distance, meters: 31.740771479612388
        */
        do_yandex_lbs_requests(wms).await;
    }

    #[tokio::test]
    async fn test_yandex_lbs_request_individual_11() {
        let wms = vec![
            WifiMeasurement {
                bssid: "52:ff:20:71:06:91".to_string(),
                signal_strength: -79.0,
            },
            WifiMeasurement {
                bssid: "2c:05:47:66:82:52".to_string(),
                signal_strength: -74.0,
            },
            WifiMeasurement {
                bssid: "b2:4e:26:e2:f0:a2".to_string(),
                signal_strength: -74.0,
            },
            WifiMeasurement {
                bssid: "dc:f8:b9:e9:c6:47".to_string(),
                signal_strength: -73.0,
            },
        ];
        /*
            Yandex LBS location: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.635350932430846, lon: 37.65717701007903 }, accuracy: 36.774803161621094 } }

            individual mac '52:ff:20:71:06:91': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.635021915041264, lon: 37.65817272838129 }, accuracy: 36.774803161621094 } }
            individual mac 'b2:4e:26:e2:f0:a2': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.63745067024819, lon: 37.65527157671289 }, accuracy: 36.774803161621094 } }
            individual mac 'dc:f8:b9:e9:c6:47': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.61875034882411, lon: 37.65818597701789 }, accuracy: 45.47257995605469 } }
            detect Yandex outlier: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.61875034882411, lon: 37.65818597701789 }, accuracy: 45.47257995605469 } }

            Estimated location by individual AP: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.63623629264473, lon: 37.65672215254709 }, accuracy: 36.774803161621094 } }
            1758957459: distance: 178.731085, coordinates: 55.636236,37.656722, accuracy: 37.000000
            Error distance, meters: 102.5035727797478
        */
        do_yandex_lbs_requests(wms).await;
    }

    #[tokio::test]
    async fn test_yandex_lbs_request_individual_12() {
        let wms = vec![WifiMeasurement {
            bssid: "9c:a2:f4:d8:c4:b9".to_string(),
            signal_strength: -73.0,
        }];
        /*
            Yandex LBS location: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.925853193225976, lon: 37.74814396882311 }, accuracy: 53.40739059448242 } }

            individual mac '9c:a2:f4:d8:c4:b9': YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.925853193225976, lon: 37.74814396882311 }, accuracy: 53.40739059448242 } }

            Estimated location by individual AP: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.92585319322598, lon: 37.74814396882311 }, accuracy: 53.40739059448242 } }
            Error distance, meters: 0.0000000007900885651219831
        */
        do_yandex_lbs_requests(wms).await;
    }
}
