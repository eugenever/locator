use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use geo::{Destination, Point, Rhumb};
use h3o::LatLng;
use h3o::Resolution;
use log::{error, info};
use tokio_postgres::IsolationLevel;

use crate::{
    CONFIG,
    constants::Collection,
    constants::{BASE_RSSI, DEFAULT_RSSI, SIGNAL_DROP_COEFFICIENT},
    db::model::Transmitter,
    db::pg::transmitter::TransmitterLocation,
    lbs::http_client::HttpClient,
    services::{rate_limiter::RateLimitersApp, submission::report::GeoFence},
    tasks::{
        blobasaur::BAConnectionManageMessage, t38::T38ConnectionManageMessage,
        yandex::YandexApiMessage,
    },
};

use super::report::extract;

const DB_ERROR: &str = "db error";

/// Process new submissions
pub async fn run(
    pool_tp: deadpool_postgres::Pool,
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
    yandex_client: HttpClient,
    tx_yandex_api: flume::Sender<YandexApiMessage>,
    rl_app: RateLimitersApp,
    geo_fence: Option<GeoFence>,
) -> Result<()> {
    let start_processing_reports = std::time::Instant::now();

    let mut mapper = pool_tp.get().await?;

    // FOR UPDATE - exclusive lock
    // https://dev.to/markadel/postgresql-isolation-levels-and-locking-summary-9ac
    let transaction = mapper
        .build_transaction()
        .isolation_level(IsolationLevel::Serializable)
        .start()
        .await?;

    let reports = match crate::db::pg::get_reports(&transaction, geo_fence).await {
        Err(e) => {
            transaction.rollback().await?;
            if let Some(pg_err) = e.as_db_error() {
                // 40001: serialization_failure, 40P01: deadlock_detected
                let pg_error_code = pg_err.code().code();
                if pg_error_code == "40001" || pg_error_code == "40P01" {
                    return Ok(());
                }
            }
            // transaction is acquired by another server instance
            if e.to_string().contains(DB_ERROR) {
                return Ok(());
            }
            return Err(anyhow::anyhow!(e));
        }
        Ok(rs) => rs,
    };

    if reports.is_empty() {
        info!("Processing reports finishing");
        transaction.commit().await?;
        return Ok(());
    };

    let mut modified: BTreeMap<String, (Transmitter, TransmitterLocation)> = BTreeMap::new();
    let mut h3s = BTreeSet::new();

    for report in reports {
        let (pos, transmitters) = match extract(
            &report.raw,
            tx_t38_conn.clone(),
            tx_ba_conn.clone(),
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
                // crate::db::pg::update_report_error(&transaction, report.id, e.to_string())
                //     .await?;
                continue;
            }
        };

        crate::db::pg::update_report(&transaction, report.id).await?;

        if transmitters.is_empty() {
            continue;
        };

        for transmitter in transmitters {
            // If we can't get the signal strength, assume a low value
            // to prevent accuracy from being overestimated.
            // It also implies lower weight, so it can quickly be
            // improved by other reports with more data
            let rssi = transmitter.signal_strength().unwrap_or(DEFAULT_RSSI);

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
                10_f64.powf((BASE_RSSI - rssi) / (10.0 * SIGNAL_DROP_COEFFICIENT));
            let signal_weight = 10_f64.powf(rssi / (10.0 * SIGNAL_DROP_COEFFICIENT));

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
                crate::db::pg::insert_cell(
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
                crate::db::t38::set_wifi_one(tx_t38_conn.clone(), collection, &tl).await?;

                if CONFIG.blobasaur.enabled {
                    let namespace = Collection::BaWifi.as_ref();
                    crate::db::blobasaur::set_ba_wifi_one(tx_ba_conn.clone(), namespace, &tl)
                        .await?;
                }
            }

            Transmitter::Bluetooth {
                mac,
                signal_strength: _,
                age: _,
            } => {
                crate::db::pg::insert_bluetooth(&transaction, mac, tl).await?;
            }
        }
    }

    for h3 in h3s {
        crate::db::pg::insert_h3(&transaction, h3).await?;
    }

    transaction.commit().await?;

    info!(
        "Processed reports: {} transmitters modified, duration: {} sec",
        modified_count,
        start_processing_reports.elapsed().as_secs() + 1
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::constants::SIGNAL_DROP_COEFFICIENT;

    #[derive(Debug, Clone)]
    struct YandexPoint {
        lat: f64,
        lon: f64,
    }

    #[test]
    fn test_weighted_yandex_point() {
        let rssi_arr = vec![
            -105.0, -93.0, -65.0, -59.0, -66.0, -65.0, -39.0, -65.0, -65.0,
        ];
        let yandex_points = vec![
            YandexPoint {
                lat: 55.775516510009766,
                lon: 37.603294372558594,
            },
            YandexPoint {
                lat: 55.775516510009766,
                lon: 37.60327911376953,
            },
            YandexPoint {
                lat: 55.77544125744115,
                lon: 37.602891174640945,
            },
            YandexPoint {
                lat: 55.77556991577149,
                lon: 37.6030158996582,
            },
            YandexPoint {
                lat: 55.77538179444947,
                lon: 37.60284215065096,
            },
            YandexPoint {
                lat: 55.77537473258002,
                lon: 37.60290535250138,
            },
            YandexPoint {
                lat: 55.7754096199859,
                lon: 37.60283692401584,
            },
            YandexPoint {
                lat: 55.77536615008746,
                lon: 37.60288520686488,
            },
            YandexPoint {
                lat: 55.7753844364332,
                lon: 37.60289608063719,
            },
        ];

        let mut lat_weighted = 0.0;
        let mut lon_weighted = 0.0;
        let mut w_weight = 0.0;
        let mut c = 0;

        for (i, yp) in yandex_points.into_iter().enumerate() {
            let weight = 10_f64.powf(rssi_arr[i] / (10.0 * SIGNAL_DROP_COEFFICIENT));
            lat_weighted = lat_weighted + yp.lat * weight;
            lon_weighted = lon_weighted + yp.lon * weight;
            w_weight = w_weight + weight;
            c += 1;
        }
        if c > 0 {
            lat_weighted = lat_weighted / w_weight;
            lon_weighted = lon_weighted / w_weight;
        }

        // lat_weighted: 55.77542200750775, lon_weighted: 37.60287907555834
        println!(
            "lat_weighted: {}, lon_weighted: {}",
            lat_weighted, lon_weighted
        );
    }

    #[test]
    fn test_weighted_yandex_point2() {
        let rssi_arr = vec![-67.0, -72.0, -67.0, -70.0, -75.0, -76.0, -80.0];
        let yandex_points = vec![
            YandexPoint {
                lat: 45.068745,
                lon: 38.986023,
            },
            YandexPoint {
                lat: 45.06810624110391,
                lon: 38.98592518382861,
            },
            YandexPoint {
                lat: 45.06862258911133,
                lon: 38.98613357543945,
            },
            YandexPoint {
                lat: 45.068534573295295,
                lon: 38.98616575683926,
            },
            YandexPoint {
                lat: 45.0683479309082,
                lon: 38.986114501953125,
            },
            YandexPoint {
                lat: 45.0688233139698,
                lon: 38.98414177200687,
            },
            YandexPoint {
                lat: 45.068745,
                lon: 38.986023,
            },
        ];

        let mut lat_weighted = 0.0;
        let mut lon_weighted = 0.0;
        let mut w_weight = 0.0;
        let mut c = 0;

        for (i, yp) in yandex_points.into_iter().enumerate() {
            let weight = 10_f64.powf(rssi_arr[i] / (10.0 * SIGNAL_DROP_COEFFICIENT));
            lat_weighted = lat_weighted + yp.lat * weight;
            lon_weighted = lon_weighted + yp.lon * weight;
            w_weight = w_weight + weight;
            c += 1;
        }
        if c > 0 {
            lat_weighted = lat_weighted / w_weight;
            lon_weighted = lon_weighted / w_weight;
        }

        // lat_weighted: 45.06856074946278, lon_weighted: 38.98587238633001
        println!(
            "lat_weighted: {}, lon_weighted: {}",
            lat_weighted, lon_weighted
        );
    }

    #[test]
    fn test_weighted_yandex_point3() {
        let rssi_arr = vec![-88.0, -73.0, -64.0];
        let yandex_points = vec![
            YandexPoint {
                lat: 44.716863,
                lon: 37.778318,
            },
            YandexPoint {
                lat: 44.717360,
                lon: 37.777851,
            },
            YandexPoint {
                lat: 44.716959,
                lon: 37.778316,
            },
        ];

        let mut lat_weighted = 0.0;
        let mut lon_weighted = 0.0;
        let mut w_weight = 0.0;
        let mut c = 0;

        for (i, yp) in yandex_points.into_iter().enumerate() {
            let weight = 10_f64.powf(rssi_arr[i] / (10.0 * SIGNAL_DROP_COEFFICIENT));
            lat_weighted = lat_weighted + yp.lat * weight;
            lon_weighted = lon_weighted + yp.lon * weight;
            w_weight = w_weight + weight;
            c += 1;
        }
        if c > 0 {
            lat_weighted = lat_weighted / w_weight;
            lon_weighted = lon_weighted / w_weight;
        }

        // lat_weighted: 45.06856074946278, lon_weighted: 38.98587238633001
        println!(
            "lat_weighted: {}, lon_weighted: {}",
            lat_weighted, lon_weighted
        );
    }
}
