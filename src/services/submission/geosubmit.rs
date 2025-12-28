use actix_web::{
    HttpRequest, HttpResponse, Responder,
    error::ErrorInternalServerError,
    http::{StatusCode, header::USER_AGENT},
    post, web,
};
use anyhow::Context;
use chrono::{DateTime, Utc};
use log::error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::GenericClient;

use crate::{
    lbs::model,
    services::helper::custom_deserialize::{
        date_time_utc_from_str, default_timestamp, mac_address,
    },
};

// only the bare minimum is parsed here: it is assumed that certain data issues
// may be due to device manufacturer software, making it difficult for
// developers to handle per device.
//
// - https://github.com/mjaakko/NeoStumbler/issues/88

/// Serde representation of a submission
#[derive(Deserialize)]
pub struct Submission {
    pub items: Vec<Report>,
}

/// Serde representation of a report item
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Report {
    #[serde(deserialize_with = "date_time_utc_from_str")]
    #[serde(default = "default_timestamp")]
    pub timestamp: DateTime<Utc>,
    pub device_id: Option<String>,
    pub position: Position,
    // compatibility with NeoStumbler
    pub cell_towers: Option<Vec<Cell>>,
    // compatibility with public request
    pub cell: Option<model::Cell>,
    pub wifi_access_points: Option<Vec<Wifi>>,
    pub bluetooth_beacons: Option<Vec<Bluetooth>>,
    #[serde(flatten)]
    pub extra: Value,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum RadioTypeCell {
    Gsm,
    #[serde(rename = "wcdma")]
    Umts,
    Lte,
    Nr,
}

/// Serde representation to deserialize a cell tower in a report
#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Cell {
    pub radio_type: RadioTypeCell,
    pub mobile_country_code: u16,
    pub mobile_network_code: u16,
    pub location_area_code: Option<u32>,
    pub cell_id: Option<u64>,
    pub age: Option<i32>,
    pub asu: Option<i32>,
    pub arfcn: Option<i32>,
    pub primary_scrambling_code: Option<u16>,
    pub serving: Option<u8>,
    pub signal_strength: Option<f64>,
    pub timing_advance: Option<f64>,
    #[serde(flatten)]
    pub extra: Value,
}

/// Serde representation to deserialize a wifi network in a report
#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Wifi {
    #[serde(deserialize_with = "mac_address")]
    pub mac_address: String,
    pub radio_type: Option<String>,
    pub age: Option<i32>,
    pub channel: Option<u16>,
    pub frequency: Option<f64>,
    pub signal_strength: Option<f64>,
    pub signal_to_noise_ratio: Option<f64>,
    pub ssid: Option<String>,
    #[serde(flatten)]
    pub extra: Value,
}

/// Serde representation to deserialize a bluetooth beacon in a report
#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Bluetooth {
    pub mac_address: String,
    pub name: Option<String>,
    pub age: Option<i32>,
    pub signal_strength: Option<f64>,
    #[serde(flatten)]
    pub extra: Value,
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum PositionSource {
    Gps,
    Manual,
    Fused,
}

/// Serde representation of the position of a report
#[derive(Deserialize, Serialize)]
pub struct Position {
    pub latitude: f64,
    pub longitude: f64,
    pub accuracy: Option<f64>,
    pub altitude: Option<f64>,
    pub altitude_accuracy: Option<f64>,
    pub age: Option<i32>,
    pub speed: Option<f64>,
    pub heading: Option<f64>,
    pub pressure: Option<f64>,
    pub source: Option<PositionSource>,
    #[serde(flatten)]
    pub extra: Value,
}

/// The entrypoint that handles a new submission.
#[post("/api/mls/v2/geosubmit")]
pub async fn service(
    data: web::Json<Submission>,
    pool_tp: web::Data<deadpool_postgres::Pool>,
    req: HttpRequest,
) -> actix_web::Result<impl Responder> {
    let data = data.into_inner();

    let ua = match req
        .headers()
        .get(USER_AGENT)
        .map(|x| x.to_str().map(|hv| hv.to_string()))
    {
        Some(Ok(x)) => Some(x),
        Some(Err(_)) => {
            return Ok(HttpResponse::BadRequest().body("user agent contains invalid characters"));
        }
        None => None,
    };

    if let Err(err) = insert(&pool_tp, ua, data, None)
        .await
        .context("Writing to database failed")
        .map_err(ErrorInternalServerError)
    {
        error!("save report in database: {}", err);
    }

    Ok(HttpResponse::new(StatusCode::OK))
}

/// Inserts a submission into the database.
pub async fn insert(
    pool_tp: &deadpool_postgres::Pool,
    user_agent: Option<String>,
    submission: Submission,
    ts_now: Option<DateTime<Utc>>,
) -> anyhow::Result<()> {
    let mapper = pool_tp.get().await?;
    let client = mapper.client();

    for report in submission.items.iter().filter(|r| {
        // Ignore reports for (-1,-1) to (1, 1)
        !(r.position.latitude.abs() <= 1. && r.position.longitude.abs() <= 1.)
    }) {
        crate::db::insert_report(client, report, user_agent.clone(), ts_now).await?;
    }

    Ok(())
}
