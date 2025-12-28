#![allow(unused)]

use geo::{OutlierDetection, point};

use crate::db::transmitter::TransmitterLocation;

pub const OUTLIER_THRESHOLD: f64 = 2.0;

#[derive(Debug)]
pub struct Outlier<'a> {
    pub mac: &'a str,
    pub lof: f64,
}

pub fn check_outlier(outliers_opt: Option<&Vec<Outlier<'_>>>, tl: &TransmitterLocation) -> bool {
    if let Some(outliers) = outliers_opt {
        if let Some(outlier) = outliers.iter().find(|otl| otl.mac == tl.mac) {
            if outlier.lof.is_nan() {
                return false;
            }
            if outlier.lof.is_infinite() {
                return true;
            }
            if outlier.lof > OUTLIER_THRESHOLD {
                return true;
            }
        }
    }
    false
}

pub fn detect_outliers(tls: &[Option<TransmitterLocation>]) -> Option<Vec<Outlier<'_>>> {
    let mut points = Vec::with_capacity(tls.len());
    let mut k = 0;
    let mut macs: Vec<&str> = Vec::with_capacity(tls.len());
    for (_i, tl_opt) in tls.iter().enumerate() {
        if let Some(tl) = tl_opt {
            points.push(point!(x: tl.lon, y: tl.lat));
            k = k + 1;
            macs.push(&tl.mac);
        }
    }
    if k > 2 {
        let kneighbours = std::cmp::min(2, k - 1);
        // let kneighbours = k - 1;

        let mut result = Vec::with_capacity(macs.len());
        let prepared = points.prepared_detector();
        let lofs = prepared.outliers(kneighbours);
        macs.iter()
            .zip(lofs)
            .for_each(|(m, l)| result.push(Outlier { lof: l, mac: *m }));
        Some(result)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{TransmitterLocation, detect_outliers};

    fn round(x: f64, decimals: u32) -> f64 {
        let y = 10i32.pow(decimals) as f64;
        (x * y).round() / y
    }

    #[test]
    fn test_detect_outliers_failed() {
        let tls = vec![
            Some(TransmitterLocation {
                accuracy: 1.1,
                mac: "1".to_string(),
                lat: 55.711887,
                lon: 37.902874,
                max_lat: 0.0,
                max_lon: 0.0,
                max_strength: 0.0,
                measurements: None,
                min_strength: -10.0,
                min_lat: 0.0,
                min_lon: 0.0,
                total_weight: 0.0,
            }),
            Some(TransmitterLocation {
                accuracy: 1.1,
                mac: "2".to_string(),
                lat: 55.714649,
                lon: 37.899498,
                max_lat: 0.0,
                max_lon: 0.0,
                max_strength: 0.0,
                measurements: None,
                min_strength: -10.0,
                min_lat: 0.0,
                min_lon: 0.0,
                total_weight: 0.0,
            }),
            Some(TransmitterLocation {
                accuracy: 1.1,
                mac: "3".to_string(),
                lat: 55.714781,
                lon: 37.898196,
                max_lat: 0.0,
                max_lon: 0.0,
                max_strength: 0.0,
                measurements: None,
                min_strength: -10.0,
                min_lat: 0.0,
                min_lon: 0.0,
                total_weight: 0.0,
            }),
            Some(TransmitterLocation {
                accuracy: 1.1,
                mac: "3".to_string(),
                lat: 55.713434,
                lon: 37.898742,
                max_lat: 0.0,
                max_lon: 0.0,
                max_strength: 0.0,
                measurements: None,
                min_strength: -10.0,
                min_lat: 0.0,
                min_lon: 0.0,
                total_weight: 0.0,
            }),
        ];

        let outliers = detect_outliers(&tls);
        if let Some(ref result) = outliers {
            println!("{:?}", result);
        } else {
            println!("Impossible to determine outliers for the transmitted data");
        }
        assert_eq!(round(outliers.unwrap()[0].lof, 2), 6.57);
    }

    #[test]
    fn test_detect_outliers_failed2() {
        let tls = vec![
            Some(TransmitterLocation {
                accuracy: 1.1,
                mac: "1".to_string(),
                lat: 44.916786,
                lon: 38.915527,
                max_lat: 0.0,
                max_lon: 0.0,
                max_strength: 0.0,
                measurements: None,
                min_strength: -10.0,
                min_lat: 0.0,
                min_lon: 0.0,
                total_weight: 0.0,
            }),
            Some(TransmitterLocation {
                accuracy: 1.1,
                mac: "2".to_string(),
                lat: 55.714649,
                lon: 37.899498,
                max_lat: 0.0,
                max_lon: 0.0,
                max_strength: 0.0,
                measurements: None,
                min_strength: -10.0,
                min_lat: 0.0,
                min_lon: 0.0,
                total_weight: 0.0,
            }),
            Some(TransmitterLocation {
                accuracy: 1.1,
                mac: "3".to_string(),
                lat: 55.714781,
                lon: 37.898196,
                max_lat: 0.0,
                max_lon: 0.0,
                max_strength: 0.0,
                measurements: None,
                min_strength: -10.0,
                min_lat: 0.0,
                min_lon: 0.0,
                total_weight: 0.0,
            }),
        ];

        let outliers = detect_outliers(&tls);
        if let Some(ref result) = outliers {
            println!("{:?}", result);
        } else {
            println!("Impossible to determine outliers for the transmitted data");
        }
        assert_eq!(round(outliers.unwrap()[0].lof, 3), 34340838.176);
    }

    #[test]
    fn test_detect_outliers_success() {
        let tls = vec![
            Some(TransmitterLocation {
                accuracy: 1.1,
                mac: "2".to_string(),
                lat: 55.714649,
                lon: 37.899498,
                max_lat: 0.0,
                max_lon: 0.0,
                max_strength: 0.0,
                measurements: None,
                min_strength: -10.0,
                min_lat: 0.0,
                min_lon: 0.0,
                total_weight: 0.0,
            }),
            Some(TransmitterLocation {
                accuracy: 1.1,
                mac: "3".to_string(),
                lat: 55.714781,
                lon: 37.898196,
                max_lat: 0.0,
                max_lon: 0.0,
                max_strength: 0.0,
                measurements: None,
                min_strength: -10.0,
                min_lat: 0.0,
                min_lon: 0.0,
                total_weight: 0.0,
            }),
            Some(TransmitterLocation {
                accuracy: 1.1,
                mac: "3".to_string(),
                lat: 55.713434,
                lon: 37.898742,
                max_lat: 0.0,
                max_lon: 0.0,
                max_strength: 0.0,
                measurements: None,
                min_strength: -10.0,
                min_lat: 0.0,
                min_lon: 0.0,
                total_weight: 0.0,
            }),
        ];

        let outliers = detect_outliers(&tls);
        if let Some(ref result) = outliers {
            println!("{:?}", result);
        } else {
            println!("Impossible to determine outliers for the transmitted data");
        }
        assert_eq!(round(outliers.unwrap()[2].lof, 3), 1.098);
    }

    #[test]
    fn test_detect_outliers_failed3() {
        let tls = vec![
            Some(TransmitterLocation {
                accuracy: 1.1,
                mac: "2".to_string(),
                lat: 55.714649,
                lon: 37.899498,
                max_lat: 0.0,
                max_lon: 0.0,
                max_strength: 0.0,
                measurements: None,
                min_strength: -10.0,
                min_lat: 0.0,
                min_lon: 0.0,
                total_weight: 0.0,
            }),
            Some(TransmitterLocation {
                accuracy: 1.1,
                mac: "3".to_string(),
                lat: 55.714781,
                lon: 37.898196,
                max_lat: 0.0,
                max_lon: 0.0,
                max_strength: 0.0,
                measurements: None,
                min_strength: -10.0,
                min_lat: 0.0,
                min_lon: 0.0,
                total_weight: 0.0,
            }),
            Some(TransmitterLocation {
                accuracy: 1.1,
                mac: "3".to_string(),
                lat: 55.713434,
                lon: 37.898742,
                max_lat: 0.0,
                max_lon: 0.0,
                max_strength: 0.0,
                measurements: None,
                min_strength: -10.0,
                min_lat: 0.0,
                min_lon: 0.0,
                total_weight: 0.0,
            }),
            Some(TransmitterLocation {
                accuracy: 1.1,
                mac: "3".to_string(),
                lat: 55.710936,
                lon: 37.900895,
                max_lat: 0.0,
                max_lon: 0.0,
                max_strength: 0.0,
                measurements: None,
                min_strength: -10.0,
                min_lat: 0.0,
                min_lon: 0.0,
                total_weight: 0.0,
            }),
        ];

        let outliers = detect_outliers(&tls);
        if let Some(ref result) = outliers {
            println!("{:?}", result);
        } else {
            println!("Impossible to determine outliers for the transmitted data");
        }
        assert_eq!(round(outliers.unwrap()[3].lof, 3), 5.378);
    }
}
