use std::collections::HashMap;

use clusters::Proximity;
use log::error;

use super::{Algorithm, DBSCAN, Point};
use crate::{
    CONFIG,
    db::transmitter::TransmitterLocation,
    error::ApiError,
    lbs::yandex::{WifiMeasurement, YandexLbsResponse, yandex_lbs_request_by_individual_wifi},
    services::rate_limiter::RateLimitersApp,
    tasks::{t38::T38ConnectionManageMessage, yandex::YandexApiMessage},
};

#[derive(Debug)]
pub struct Outlier<'a> {
    pub mac: &'a str,
    pub id: u32,
}

pub fn check_outlier(outliers_opt: Option<&Vec<Outlier<'_>>>, tl: &TransmitterLocation) -> bool {
    if let Some(outliers) = outliers_opt {
        if outliers.iter().any(|otl| otl.mac == tl.mac) {
            return true;
        }
    }
    false
}

pub async fn detect_outliers(
    tls: &[Option<TransmitterLocation>],
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    yandex_client: reqwest::Client,
    tx_yandex_api: flume::Sender<YandexApiMessage>,
    rl_app: RateLimitersApp,
    cell_opt: Option<HashMap<String, Option<YandexLbsResponse>>>,
) -> Result<Option<Vec<Outlier<'_>>>, ApiError> {
    let tls_filtered = tls
        .iter()
        .filter(|&tl| tl.is_some())
        .map(|t| t.as_ref().unwrap())
        .collect::<Vec<_>>();

    let cap = tls_filtered.len();
    let mut points = Vec::with_capacity(cap);
    let mut macs = Vec::with_capacity(cap);

    // indices of points and MAC addresses in arrays are synchronized
    tls_filtered.iter().enumerate().for_each(|(i, &tl)| {
        points.push(Point {
            id: i as u32,
            lat: tl.lat,
            lon: tl.lon,
        });
        macs.push(tl.mac.as_str());
    });

    let cell = cell_opt.unwrap_or_default();
    let cell_points = create_cell_points(&cell);

    if points.len() == 1 {
        if CONFIG.yandex_lbs.enabled {
            let mut noise = Vec::new();

            // first filter by BS service area
            // for one point this filter can be allocated to a separate block
            if cell_points.len() > 0 {
                let distance_point_cell = points[0].distance(&cell_points[0]);
                if distance_point_cell > CONFIG.locator.max_distance_cell {
                    noise.push(Outlier {
                        id: 0,
                        mac: &tls_filtered[0].mac,
                    });
                    return Ok(Some(noise));
                }
            }

            let mut wms = vec![WifiMeasurement {
                bssid: tls_filtered[0].mac.clone(),
                signal_strength: -70.0, // does not affect the result
            }];

            match yandex_lbs_request_by_individual_wifi(
                tx_t38_conn,
                &CONFIG,
                wms,
                yandex_client.clone(),
                tx_yandex_api,
                rl_app,
            )
            .await
            {
                Err(e) => {
                    error!("Yandex LBS request: {}", e);
                    return Err(e);
                }
                Ok(yandex_lbs_responses) => {
                    if let Some(ylr_opt) = yandex_lbs_responses.get(&tls_filtered[0].mac) {
                        if let Some(ylr) = ylr_opt {
                            let yandex_point = Point {
                                id: 0,
                                lat: ylr.location.point.lat,
                                lon: ylr.location.point.lon,
                            };
                            let distance = yandex_point.distance(&points[0]);
                            // second filter is based on the distance between our point and Yandex's point
                            if distance > 2.0 * CONFIG.locator.max_distance_in_cluster {
                                // can't tell which data has an error: ours or Yandex's
                                // add point to outlier
                                noise.push(Outlier {
                                    id: yandex_point.id,
                                    mac: &tls_filtered[0].mac,
                                });
                            }
                        }
                    }
                    if noise.len() > 0 {
                        return Ok(Some(noise));
                    }
                }
            }
        }
        return Ok(None);
    }

    if points.len() == 2 {
        let d = points[0].distance(&points[1]);
        if d > CONFIG.locator.max_distance_in_cluster && CONFIG.yandex_lbs.enabled {
            let mut noise = Vec::new();

            let mut wms = Vec::with_capacity(points.len());
            tls_filtered.iter().for_each(|&tl| {
                wms.push(WifiMeasurement {
                    bssid: tl.mac.clone(),
                    signal_strength: -70.0, // does not affect the result
                });
            });

            match yandex_lbs_request_by_individual_wifi(
                tx_t38_conn,
                &CONFIG,
                wms,
                yandex_client.clone(),
                tx_yandex_api,
                rl_app,
            )
            .await
            {
                Err(e) => {
                    error!("Yandex LBS request: {}", e);
                    return Err(e);
                }
                Ok(yandex_lbs_responses) => {
                    for (i, (mac, ylr_opt)) in yandex_lbs_responses.into_iter().enumerate() {
                        if let Some(ylr) = ylr_opt {
                            let yandex_point = Point {
                                id: i as u32,
                                lat: ylr.location.point.lat,
                                lon: ylr.location.point.lon,
                            };
                            if let Some(index_mac) = macs.iter().position(|m| m.contains(&mac)) {
                                let distance = yandex_point.distance(&points[index_mac]);
                                let mut distance_point_cell = 0.0;
                                if cell_points.len() > 0 {
                                    distance_point_cell =
                                        points[index_mac].distance(&cell_points[0]);
                                }

                                if distance > 2.0 * CONFIG.locator.max_distance_in_cluster
                                    || distance_point_cell > CONFIG.locator.max_distance_cell
                                {
                                    // can't tell which data has an error: ours or Yandex's
                                    // add point to outlier
                                    noise.push(Outlier {
                                        id: yandex_point.id,
                                        mac: &tls_filtered[index_mac].mac,
                                    });
                                }
                            }
                        }
                    }
                    if noise.len() > 0 {
                        return Ok(Some(noise));
                    } else {
                        // the minimum number of points in a cluster must be greater than 1
                        let min_pts = 1;
                        let algorithm =
                            DBSCAN::new(CONFIG.locator.max_distance_in_cluster, min_pts);
                        let clusters = algorithm.cluster(&points);
                        let actual_clusters = clusters.clusters();
                        let cluster_noise = clusters.noise();
                        if cluster_noise.len() > 0 {
                            let mut outliers = Vec::with_capacity(noise.len());
                            cluster_noise.into_iter().for_each(|p| {
                                if let Some(&tl) = tls_filtered.get(p.id as usize) {
                                    let outlier = Outlier {
                                        id: p.id,
                                        mac: &tl.mac,
                                    };
                                    outliers.push(outlier);
                                }
                            });
                            return Ok(Some(outliers));
                        }
                    }
                }
            }
        }
        return Ok(None);
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

    let algorithm = DBSCAN::new(CONFIG.locator.max_distance_in_cluster, 0);
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
        Ok(None)
    } else {
        let mut outliers = Vec::with_capacity(noise.len());
        noise.into_iter().for_each(|p| {
            if let Some(&tl) = tls_filtered.get(p.id as usize) {
                let outlier = Outlier {
                    id: p.id,
                    mac: &tl.mac,
                };
                outliers.push(outlier);
            }
        });
        Ok(Some(outliers))
    }
}

pub fn create_cell_points(cell: &HashMap<String, Option<YandexLbsResponse>>) -> Vec<Point> {
    let mut cell_points = Vec::with_capacity(cell.len());
    for (cell_code, cell_ylr_opt) in cell {
        if let Some(cell_ylr) = cell_ylr_opt {
            let cell_point = Point {
                id: 0,
                lat: cell_ylr.location.point.lat,
                lon: cell_ylr.location.point.lon,
            };
            cell_points.push(cell_point);
        }
    }
    cell_points
}
