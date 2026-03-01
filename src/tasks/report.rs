#![allow(unused)]

use std::collections::{BTreeMap, HashMap};

use geo::{Destination, Point, Rhumb};
use log::{error, info};
use tokio::{sync::oneshot, task::JoinHandle};
use tokio_schedule::Job;

use crate::{
    CONFIG,
    constants::{BASE_RSSI, DEFAULT_RSSI, SIGNAL_DROP_COEFFICIENT},
    db::{
        model::Transmitter,
        pg::{
            RangeId,
            bulk_insert::{DataReport, bulk_insert_data},
            get_range_id_for_report,
            transmitter::TransmitterLocation,
        },
    },
    lbs::http_client::HttpClient,
    services::{
        helper::{
            custom_deserialize::{default_timestamp, default_timestamp_ms},
            pool_task,
        },
        rate_limiter::RateLimitersApp,
        submission::{
            self,
            geosubmit::{Report, insert},
            geosubmit_public::SubmissionPublic,
            report::{Report as ReportProcess, extract_from_report},
        },
    },
    tasks::{
        blobasaur::BAConnectionManageMessage, t38::T38ConnectionManageMessage,
        yandex::YandexApiMessage,
    },
};

pub fn process_reports_task(
    pool_tp: deadpool_postgres::Pool,
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
    yandex_client: HttpClient,
    tx_yandex_api: flume::Sender<YandexApiMessage>,
    rl_app: RateLimitersApp,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        tokio_schedule::every(CONFIG.database.report_processing_frequency)
            .seconds()
            .perform(|| async {
                let geo_fence = None;
                if let Err(err) = submission::process::run(
                    pool_tp.clone(),
                    tx_t38_conn.clone(),
                    tx_ba_conn.clone(),
                    yandex_client.clone(),
                    tx_yandex_api.clone(),
                    rl_app.clone(),
                    geo_fence,
                )
                .await
                {
                    error!("process reports: {}", err);
                }
            })
            .await;
    })
}

pub fn online_process_report_task(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
    yandex_client: HttpClient,
    tx_yandex_api: flume::Sender<YandexApiMessage>,
    rl_app: RateLimitersApp,
    rx_report: flume::Receiver<MessageSaveReport>,
    tx_save_report: flume::Sender<MessageSaveReport>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Ok(message) = rx_report.recv_async().await {
            for rp in &message.sp.items {
                let report: ReportProcess = rp.clone().into();
                let ts = report.timestamp;

                match extract_from_report(
                    report,
                    tx_t38_conn.clone(),
                    tx_ba_conn.clone(),
                    yandex_client.clone(),
                    tx_yandex_api.clone(),
                    rl_app.clone(),
                )
                .await
                {
                    Ok((pos, transmitters)) => {
                        let mut modified: BTreeMap<String, (Transmitter, TransmitterLocation)> =
                            BTreeMap::new();

                        for transmitter in transmitters {
                            let rssi = transmitter.signal_strength().unwrap_or(DEFAULT_RSSI);

                            let distance_since_scan;
                            let lat;
                            let lon;
                            if let Some(speed) = pos.speed
                                && let Some(wifi_age) = transmitter.age()
                                && let Some(pos_age) = pos.age
                            {
                                distance_since_scan =
                                    speed * (wifi_age as f64 - pos_age as f64) / 1000.0;

                                if let Some(heading) = pos.heading {
                                    let transmitter_scan_pos = Rhumb::destination(
                                        Point::new(pos.latitude, pos.longitude),
                                        heading,
                                        -distance_since_scan,
                                    );
                                    (lat, lon) = transmitter_scan_pos.x_y();
                                } else {
                                    lat = pos.latitude;
                                    lon = pos.longitude;
                                }
                            } else {
                                distance_since_scan = 0.0;
                                lat = pos.latitude;
                                lon = pos.longitude;
                            };

                            let distance_from_transmitter =
                                10_f64.powf((BASE_RSSI - rssi) / (10.0 * SIGNAL_DROP_COEFFICIENT));

                            let signal_weight =
                                10_f64.powf(rssi / (10.0 * SIGNAL_DROP_COEFFICIENT));

                            let age_weight = 10_f64.powf(-distance_since_scan.abs() / 25.0);

                            let gnss_accuracy_weight =
                                10_f64.powf(-pos.accuracy.unwrap_or(10.0) / 10.0);

                            let weight = signal_weight * age_weight * gnss_accuracy_weight;

                            let accuracy =
                                distance_from_transmitter + pos.accuracy.unwrap_or_default();

                            let transmitter_name = transmitter.to_string();

                            if let Some((_, tl)) = modified.get_mut(&transmitter_name) {
                                tl.update(lat, lon, accuracy, weight, rssi);
                            } else if let Some(mut tl) =
                                match transmitter.lookup(tx_t38_conn.clone()).await {
                                    Err(_) => {
                                        // skip the cell and ble
                                        continue;
                                    }
                                    Ok(tl) => tl,
                                }
                            {
                                tl.update(lat, lon, accuracy, weight, rssi);
                                modified.insert(transmitter.to_string(), (transmitter, tl));
                            } else {
                                modified.insert(
                                    transmitter.to_string(),
                                    (
                                        transmitter,
                                        TransmitterLocation::new(
                                            &transmitter_name,
                                            lat,
                                            lon,
                                            accuracy,
                                            weight,
                                            rssi,
                                        ),
                                    ),
                                );
                            }
                        }

                        for (_tr_name, (transmitter, mut tl)) in modified {
                            match transmitter {
                                Transmitter::Cell { .. } => {}
                                Transmitter::Bluetooth { .. } => {}
                                Transmitter::Wifi {
                                    mac: _,
                                    signal_strength: _,
                                    age: _,
                                } => {
                                    tl.measurements = None;
                                    let collection = crate::constants::Collection::Wifi.as_ref();
                                    if let Err(e) = crate::db::t38::set_wifi_one(
                                        tx_t38_conn.clone(),
                                        collection,
                                        &tl,
                                    )
                                    .await
                                    {
                                        error!("process report task: {}", e);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed process report with timestamp '{}': {e}", ts);
                    }
                };
            }

            /*
                if let Err(_e) = tx_save_report.send_async(message).await {
                    error!("send save report");
                }
            */
        }
    })
}

#[derive(Debug, Clone)]
pub struct MessageSaveReport {
    pub ua: Option<String>,
    pub sp: SubmissionPublic,
}

pub fn save_report_task(
    pool_tp: deadpool_postgres::Pool,
    rx_save_report: flume::Receiver<MessageSaveReport>,
    pool_tokio_task: pool_task::Pool,
) -> JoinHandle<()> {
    let batch_size = 500;
    let timeout_insert = 60; // seconds
    let mut buffer = Vec::with_capacity(batch_size);

    tokio::spawn(async move {
        loop {
            // save the buffer of messages to PostgreSQL on timeout or when the threshold value is reached
            match tokio::time::timeout(
                tokio::time::Duration::from_secs(timeout_insert),
                rx_save_report.recv_async(),
            )
            .await
            {
                Ok(result) => {
                    if let Ok(message) = result {
                        if buffer.len() < batch_size {
                            for r in message.sp.items {
                                let report_public: Report = r.into();
                                match serde_json::to_vec(&report_public) {
                                    Err(e) => {
                                        error!("serialize report public: {}", e);
                                    }
                                    Ok(raw) => {
                                        let data_report = DataReport {
                                            report: report_public,
                                            user_agent: message.ua.clone().unwrap_or_default(),
                                            processed_at: default_timestamp(),
                                            raw,
                                        };
                                        buffer.push(data_report);
                                    }
                                }
                            }
                        } else {
                            match bulk_insert_data(&pool_tp, &buffer, 500).await {
                                Err(e) => {
                                    error!("bulk insert: {}", e);
                                }
                                Ok(result) => {}
                            }
                            buffer.clear();
                        }
                    }
                }
                // timeout has occurred, we are forcing a write to the PostgreSQL
                Err(_err) => {
                    if !buffer.is_empty() {
                        match bulk_insert_data(&pool_tp, &buffer, 500).await {
                            Err(e) => {
                                error!("bulk insert: {}", e);
                            }
                            Ok(result) => {}
                        }
                        buffer.clear();
                    }
                }
            }
        }
    })
}

// every day at four o'clock
pub fn process_report_partitions_task(pool_tp: deadpool_postgres::Pool) -> JoinHandle<()> {
    tokio::spawn(async move {
        tokio_schedule::every(1)
            .day()
            .at(4, 0, 0)
            .perform(|| async {
                if let Err(err) = crate::db::pg::create_partitions(pool_tp.clone()).await {
                    error!("create 'report' partitions: {}", err);
                }
                if let Err(err) = crate::db::pg::remove_partitions(pool_tp.clone()).await {
                    error!("remove 'report' partitions: {}", err);
                }
                info!("Successful processing of 'report' partitions");
            })
            .await;
    })
}

pub enum MessageServicePartition {
    RangeId {
        partition: String,
        tx: oneshot::Sender<Option<RangeId>>,
    },
    LastId {
        partition: String,
        tx: oneshot::Sender<Option<i64>>,
    },
    SetLastId {
        partition: String,
        id: i64,
    },
}

pub fn service_report_partitions_task(
    pool_tp: deadpool_postgres::Pool,
    rx: flume::Receiver<MessageServicePartition>,
) -> JoinHandle<()> {
    let mut ranges_id: HashMap<String, RangeId> = HashMap::new();
    let mut lasts_id: HashMap<String, i64> = HashMap::new();

    tokio::spawn(async move {
        while let Ok(message) = rx.recv_async().await {
            match message {
                MessageServicePartition::RangeId { partition, tx } => {
                    if let Some(range) = ranges_id.get(&partition) {
                        if let Err(_) = tx.send(Some(*range)) {
                            error!("send RangeId: {:?} for partition {}", range, partition);
                        }
                    } else {
                        match get_range_id_for_report(pool_tp.clone(), &partition).await {
                            Err(e) => {
                                error!("get range id for partition '{}': {}", partition, e);
                                if let Err(_) = tx.send(None) {
                                    error!("send RangeId for partition {}", partition);
                                }
                            }
                            Ok(range) => {
                                if let Some(r) = range.first() {
                                    ranges_id.insert(partition.clone(), *r);
                                    if let Err(_) = tx.send(Some(*r)) {
                                        error!(
                                            "send RangeId: {:?} for partition {}",
                                            range, partition
                                        );
                                    }
                                } else {
                                    if let Err(_) = tx.send(None) {
                                        error!("send RangeId for partition {}", partition);
                                    }
                                }
                            }
                        }
                    }
                }
                MessageServicePartition::LastId { partition, tx } => {
                    if let Some(lid) = lasts_id.get(&partition) {
                        if let Err(_) = tx.send(Some(*lid)) {
                            error!("send last Id {} for partition {}", lid, partition);
                        }
                    } else {
                        match get_range_id_for_report(pool_tp.clone(), &partition).await {
                            Err(e) => {
                                error!("get range id for partition '{}': {}", partition, e);
                                if let Err(_) = tx.send(None) {
                                    error!("send RangeId for partition {}", partition);
                                }
                            }
                            Ok(range) => {
                                if let Some(r) = range.first() {
                                    ranges_id.insert(partition.clone(), *r);
                                    lasts_id.insert(partition, r.min_id);
                                }
                            }
                        }
                    }
                }
                MessageServicePartition::SetLastId { partition, id } => {
                    lasts_id.insert(partition, id);
                }
            }
        }
    })
}
