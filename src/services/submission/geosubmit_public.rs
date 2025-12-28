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

use crate::{
    config::CONFIG,
    lbs::model::Cell,
    services::{
        helper::custom_deserialize::{date_time_utc_from_str, default_timestamp_ms, mac_address},
        submission::{
            geosubmit::{Position, PositionSource, Report, Submission, Wifi, insert},
            report::{Position as PositionProcess, Report as ReportProcess, Wifi as WifiProcess},
        },
    },
    tasks::report::MessageSaveReport,
};

#[derive(Deserialize, Clone, Debug)]
pub struct SubmissionPublic {
    pub items: Vec<ReportPublic>,
}

impl Into<Submission> for SubmissionPublic {
    fn into(self) -> Submission {
        Submission {
            items: self.items.into_iter().map(|rp| rp.into()).collect(),
        }
    }
}

/// Serde representation of a single report
#[derive(Deserialize, Serialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct ReportPublic {
    #[serde(deserialize_with = "date_time_utc_from_str")]
    #[serde(default = "default_timestamp_ms")]
    timestamp: DateTime<Utc>,
    device_id: Option<String>,
    gnss: PositionPublic,
    wifi: Option<Vec<WifiPublic>>,
    cell: Option<Cell>,
    #[serde(flatten)]
    extra: Value,
}

impl Into<Report> for ReportPublic {
    fn into(self) -> Report {
        let wap = self.wifi.map(|vec_wp| {
            vec_wp
                .into_iter()
                .map(|wp| wp.into())
                .collect::<Vec<Wifi>>()
        });
        Report {
            timestamp: self.timestamp,
            device_id: self.device_id,
            position: self.gnss.into(),
            wifi_access_points: wap,
            cell: self.cell,
            cell_towers: None,
            bluetooth_beacons: None,
            extra: Value::Null,
        }
    }
}

impl Into<ReportProcess> for ReportPublic {
    fn into(self) -> ReportProcess {
        let wap = self.wifi.map(|vec_wp| {
            vec_wp
                .into_iter()
                .map(|wp| wp.into())
                .collect::<Vec<WifiProcess>>()
        });
        ReportProcess {
            timestamp: self.timestamp.timestamp(),
            device_id: self.device_id,
            position: self.gnss.into(),
            wifi_access_points: wap,
            cell: self.cell,
            cell_towers: None,
            bluetooth_beacons: None,
        }
    }
}

/// Serde representation to deserialize a wifi network in a report
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
struct WifiPublic {
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
    bandwidth: Option<f64>,
    #[serde(flatten)]
    extra: Value,
}

impl Into<Wifi> for WifiPublic {
    fn into(self) -> Wifi {
        Wifi {
            mac_address: self.mac,
            radio_type: self.radio_type,
            age: self.age,
            channel: self.channel,
            frequency: self.frequency,
            signal_strength: self.rssi,
            signal_to_noise_ratio: self.snr,
            ssid: self.ssid,
            extra: Value::Null,
        }
    }
}

impl Into<WifiProcess> for WifiPublic {
    fn into(self) -> WifiProcess {
        WifiProcess {
            mac_address: self.mac,
            age: self.age,
            signal_strength: self.rssi,
            ssid: self.ssid,
        }
    }
}

/// Serde representation of the position of a report
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct PositionPublic {
    pub longitude: f64,
    pub latitude: f64,
    pub accuracy: Option<f64>,
    pub altitude: Option<f64>,
    pub altitude_accuracy: Option<f64>,
    pub age: Option<i32>,
    pub speed: Option<f64>,
    pub bearing: Option<f64>,
    pub pressure: Option<f64>,
    pub source: Option<PositionSource>,
    #[serde(flatten)]
    pub extra: Value,
}

impl Into<Position> for PositionPublic {
    fn into(self) -> Position {
        Position {
            latitude: self.latitude,
            longitude: self.longitude,
            accuracy: self.accuracy,
            altitude: self.altitude,
            altitude_accuracy: self.altitude_accuracy,
            age: self.age,
            speed: self.speed,
            heading: self.bearing,
            pressure: self.pressure,
            source: self.source,
            extra: Value::Null,
        }
    }
}

impl Into<PositionProcess> for PositionPublic {
    fn into(self) -> PositionProcess {
        PositionProcess {
            latitude: self.latitude,
            longitude: self.longitude,
            accuracy: self.accuracy,
            age: self.age,
            speed: self.speed,
            heading: self.bearing,
        }
    }
}

#[post("/report")]
pub async fn service(
    data: web::Json<SubmissionPublic>,
    pool_tp: web::Data<deadpool_postgres::Pool>,
    tx_report_web: web::Data<Option<flume::Sender<MessageSaveReport>>>,
    req: HttpRequest,
) -> actix_web::Result<impl Responder> {
    let sp = data.into_inner();

    let ua = match req
        .headers()
        .get(USER_AGENT)
        .map(|x| x.to_str().map(|hv| hv.to_string()))
    {
        Some(Ok(x)) => Some(x),
        Some(Err(_)) => Some("".to_string()),
        None => Some("".to_string()),
    };

    if CONFIG.locator.process_report_online {
        let m = MessageSaveReport {
            sp: sp.clone(),
            ua: ua.clone(),
        };
        let tx_report = (*tx_report_web.into_inner()).clone().unwrap();
        if let Err(_e) = tx_report.send_async(m).await {
            error!("unsuccessful submission of report for processing");
        }
        let ts_now = default_timestamp_ms();
        if let Err(err) = insert(&pool_tp, ua, sp.into(), Some(ts_now))
            .await
            .context("Writing to database failed")
            .map_err(ErrorInternalServerError)
        {
            error!("save report in database: {}", err);
        }
    } else {
        if let Err(err) = insert(&pool_tp, ua, sp.into(), None)
            .await
            .context("Writing to database failed")
            .map_err(ErrorInternalServerError)
        {
            error!("save report in database: {}", err);
        }
    }

    Ok(HttpResponse::new(StatusCode::OK))
}
