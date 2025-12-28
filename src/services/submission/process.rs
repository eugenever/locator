//! This module contains functions to process new submissions.
//!
//! `Locator` estimate the position of the beacon using a weighted average
//! algorithm.
//! It also keeps track of the bounding box of the positions where a beacon has
//! been reported, to detect moving beacons and for cell locations, for which
//! the weighted average algorithm is less adapted.
//!
//! `Locator` iterates over all beacons in the reports and checks if it has
//! been reported before.
//! If it can find the beacon in the database it increases the bounding box to
//! include the new reported position if needed, and add the new data to the
//! database by computing he submission weight and incorporating it to the
//! average.
//! Otherwise `Locator` creates a new entry in the database with a zero-sized
//! bounding box around the reported position.
//!
//! Some dead reckoning is done to determine the real beacon location, which can
//! be different from the report location as the GNSS fix on the contributor
//! device isn't synced to the scans.
//!
//! The weight using to compute the average is based on the RSSI (signal level),
//! distance between scan and GNSS fix and GNSS fix accuracy, based on
//! exponential functions in the form 10^(-data/coefficient), with higher values
//! of data being better. The coefficient is used to adjust the behaviour of the
//! curve to match the input data.
//!
//! After processing the data the stats are updated.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use geo::{Destination, Point, Rhumb};
use h3o::LatLng;
use h3o::Resolution;
use log::{error, info};
use tokio_postgres::IsolationLevel;

use super::report::extract;
use crate::{
    CONFIG, constants::BASE_RSSI, constants::SIGNAL_DROP_COEFFICIENT, db::model::Transmitter,
    db::transmitter::TransmitterLocation, services::rate_limiter::RateLimitersApp,
    tasks::t38::T38ConnectionManageMessage, tasks::yandex::YandexApiMessage,
};

const DB_ERROR: &str = "db error";

/// Process new submissions
pub async fn run(
    pool_tp: deadpool_postgres::Pool,
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    yandex_client: reqwest::Client,
    tx_yandex_api: flume::Sender<YandexApiMessage>,
    rl_app: RateLimitersApp,
) -> Result<()> {
    loop {
        let start_processing_reports = std::time::Instant::now();

        let mut mapper = pool_tp.get().await?;
        let transaction = mapper
            .build_transaction()
            .isolation_level(IsolationLevel::Serializable)
            .start()
            .await?;

        let reports = match crate::db::get_reports(&transaction).await {
            Err(e) => {
                transaction.rollback().await?;
                if let Some(pg_err) = e.as_db_error() {
                    // 40001: serialization_failure, 40P01: deadlock_detected
                    let pg_error_code = pg_err.code().code();
                    if pg_error_code == "40001" || pg_error_code == "40P01" {
                        break;
                    }
                }
                // transaction is acquired by another server instance
                if e.to_string().contains(DB_ERROR) {
                    break;
                }
                return Err(anyhow::anyhow!(e));
            }
            Ok(rs) => rs,
        };

        let mut modified: BTreeMap<String, (Transmitter, TransmitterLocation)> = BTreeMap::new();
        let mut h3s = BTreeSet::new();

        let last_report_in_batch = if let Some(report) = reports.last() {
            report.id
        } else {
            info!("Processing reports finishing");
            transaction.commit().await?;
            break;
        };

        for report in reports {
            crate::db::update_report(&transaction, report.id).await?;

            let (pos, transmitters) = match extract(
                &report.raw,
                tx_t38_conn.clone(),
                yandex_client.clone(),
                tx_yandex_api.clone(),
                rl_app.clone(),
            )
            .await
            {
                Ok(x) => x,
                Err(e) => {
                    error!(
                        "Failed process report id {} from '{}': {e}",
                        report.id,
                        report.user_agent.unwrap_or_default()
                    );
                    crate::db::update_report_error(&transaction, report.id, format!("{e}")).await?;
                    continue;
                }
            };

            if transmitters.is_empty() {
                continue;
            };

            for transmitter in transmitters {
                // If we can't get the signal strength, assume a low value
                // to prevent accuracy from being overestimated.
                // It also implies lower weight, so it can quickly be
                // improved by other reports with more data
                let rssi = transmitter.signal_strength().unwrap_or(-90.0);

                let distance_since_scan;
                let lat;
                let lon;
                if let Some(speed) = pos.speed
                    && let Some(wifi_age) = transmitter.age()
                    && let Some(pos_age) = pos.age
                {
                    distance_since_scan = speed * (wifi_age as f64 - pos_age as f64) / 1000.0;

                    // "Reversed dead reckoning": guess where the transmitter was
                    // scanned based on heading and distance since last scan
                    // Neostumbler reduced metadata feature impact this feature
                    // as speed is rounded to 2 m/s and heading to 30° (which
                    // means +/-15° of error, with +/- 7.5° on average)
                    // Here are values for a 80 km/h speed with 1 second age
                    // difference
                    // cos(15°) * 22.22 m = 5.75 m error at most
                    // cos(7.5°) * 22.22 m = 2.90 m on average
                    // This algorithm is still useful with this error as without
                    // it, the data point would be located even further away
                    // (22.22 m in the given example)
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

                // Based on https://codeberg.org/Locator/Locator/issues/31#issuecomment-3098830
                let distance_from_transmitter =
                    10_f64.powf((BASE_RSSI - rssi as f64) / (10.0 * SIGNAL_DROP_COEFFICIENT));
                let signal_weight = 10_f64.powf(rssi as f64 / (10.0 * SIGNAL_DROP_COEFFICIENT));

                // The formula for age was found by quick trial and error. This
                // one seems fine. Let's take an average of 1 second between
                // wifi and pos age.
                // 1 m/s (3.6 km/h, by foot) = 0.91
                // 8.33 m/s (30 km/h, slow car zone in France) = 0.46
                // 13.88 m/s (50 km/h, fast car speed in city) = 0.28
                // 22.22 m/s (80 km/h, rural car speed) = 0.13
                // 30.55 m/s (110 km/h, fast car road) = 0.06
                // 36.11 m/s (130 km/h, fastest car roads) = 0.04
                // When no data is available, this will be computed as if the
                // report was done without moving (giving it an higher than
                // average weight).
                let age_weight = 10_f64.powf(-distance_since_scan.abs() / 25.0);

                // Same, found through trial and error
                // 1m = 0.79
                // 5m = 0.31
                // 10m = 0.1
                // 20m = 0.01
                let gnss_accuracy_weight = 10_f64.powf(-pos.accuracy.unwrap_or(10.0) / 10.0);

                let weight = signal_weight * age_weight * gnss_accuracy_weight;
                let accuracy = distance_from_transmitter + pos.accuracy.unwrap_or_default();

                let transmitter_name = transmitter.to_string();

                if let Some((_, tl)) = modified.get_mut(&transmitter_name) {
                    tl.update(lat, lon, accuracy, weight, rssi);
                } else if let Some(mut tl) = match transmitter.lookup(tx_t38_conn.clone()).await {
                    Err(_) => {
                        // skip the cell and ble
                        continue;
                    }
                    Ok(tl) => tl,
                } {
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

            let pos = LatLng::new(pos.latitude, pos.longitude)?;
            let h3 = pos.to_cell(Resolution::try_from(CONFIG.locator.h3_resolution)?);
            h3s.insert(h3);
        }

        let modified_count = modified.len();

        for (_tr_name, (transmitter, mut tl)) in modified {
            match transmitter {
                Transmitter::Cell {
                    radio,
                    country,
                    network,
                    area,
                    cell,
                    unit,
                    signal_strength: _,
                    age: _,
                } => {
                    crate::db::insert_cell(
                        &transaction,
                        radio as i16,
                        country,
                        network,
                        area,
                        cell,
                        unit,
                        tl,
                    )
                    .await?;
                }

                Transmitter::Wifi {
                    mac: _,
                    signal_strength: _,
                    age: _,
                } => {
                    tl.measurements = None;
                    let collection = crate::constants::Collection::Wifi.as_ref();
                    crate::db::t38::set_wifi_one(tx_t38_conn.clone(), collection, &tl).await?
                }

                Transmitter::Bluetooth {
                    mac,
                    signal_strength: _,
                    age: _,
                } => {
                    crate::db::insert_bluetooth(&transaction, mac, tl).await?;
                }
            }
        }

        for h3 in h3s {
            crate::db::insert_h3(&transaction, h3).await?;
        }

        transaction.commit().await?;

        info!(
            "Processed reports up to #{last_report_in_batch} - {modified_count} transmitters modified, duration: {} sec",
            start_processing_reports.elapsed().as_secs() + 1
        );
    }

    Ok(())
}
