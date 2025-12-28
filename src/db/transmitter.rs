use geo::{Distance, Haversine, Point};
use log::error;
use redis::{FromRedisValue, ParsingError};
use serde::{Deserialize, Serialize};
use tokio_pg_mapper_derive::PostgresMapper;

use crate::{CONFIG, db::t38::cmd::REDIS_NO_DATA};

macro_rules! not_convertible_error {
    ($v:expr, $det:expr) => {
        ParsingError::from(format!("{:?} (response was {:?})", $det, $v))
    };
}

/// A geospatial bounding box
///
/// This struct represents a geospatial [minimal bounding rectangle](https://en.wikipedia.org/wiki/Minimum_bounding_rectangle).
#[derive(Clone, Copy)]
pub struct Bounds {
    pub min_lat: f64,
    pub min_lon: f64,
    pub max_lat: f64,
    pub max_lon: f64,
}

impl Bounds {
    /// Return the bottom left and the top right point of the rectangle.
    pub fn points(&self) -> (Point, Point) {
        let min = Point::new(self.min_lon, self.min_lat);
        let max = Point::new(self.max_lon, self.max_lat);
        (min, max)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PostgresMapper)]
#[pg_mapper(table = "wifi")]
pub struct TransmitterLocation {
    pub mac: String,
    pub min_lat: f64,
    pub min_lon: f64,
    pub max_lat: f64,
    pub max_lon: f64,

    pub lat: f64,
    pub lon: f64,
    pub accuracy: f64,
    pub total_weight: f64,

    pub min_strength: f64,
    pub max_strength: f64,
    pub measurements: Option<Vec<u8>>,
}

impl FromRedisValue for TransmitterLocation {
    fn from_redis_value(v: redis::Value) -> Result<TransmitterLocation, ParsingError> {
        if let redis::Value::BulkString(ref bulk_string) = v {
            // empty response:
            // crud.go, row 972, return empty bulk-string:
            // return resp.StringValue(""), nil
            // alternative for ID_NOT_FOUND_ERROR
            if bulk_string.len() == 0 {
                return Err(ParsingError::from(REDIS_NO_DATA));
            }

            match serde_json::from_slice::<TransmitterLocation>(&bulk_string) {
                Err(e) => {
                    error!("deserialize TransmitterLocation from BulkString: {}", e);
                    return Err(not_convertible_error!(
                        "",
                        "Expect an arbitrary binary data"
                    ));
                }
                Ok(tl) => {
                    return Ok(tl);
                }
            }
        }

        Err(ParsingError::from(REDIS_NO_DATA))
    }
}

impl TransmitterLocation {
    /// Create a new `TransmitterLocation` struct around a single point.
    pub fn new(mac: &str, lat: f64, lon: f64, accuracy: f64, weight: f64, strength: f64) -> Self {
        Self {
            mac: mac.to_string(),
            min_lat: lat,
            min_lon: lon,
            max_lat: lat,
            max_lon: lon,

            lat: lat,
            lon: lon,
            accuracy: accuracy,
            total_weight: weight,

            min_strength: strength,
            max_strength: strength,
            measurements: None,
        }
    }

    /// Return the bottom left and the top right point of the rectangle.
    pub fn points(&self) -> (Point, Point) {
        let min = Point::new(self.min_lon, self.min_lat);
        let max = Point::new(self.max_lon, self.max_lat);
        (min, max)
    }

    /// Add new data to the weighted average
    pub fn update(&mut self, lat: f64, lon: f64, accuracy: f64, weight: f64, strength: f64) {
        if lat < self.min_lat {
            self.min_lat = lat;
        } else if lat > self.max_lat {
            self.max_lat = lat;
        }

        if lon < self.min_lon {
            self.min_lon = lon;
        } else if lon > self.max_lon {
            self.max_lon = lon;
        }

        self.lat = ((self.lat * self.total_weight) + (lat * weight)) / (self.total_weight + weight);
        self.lon = ((self.lon * self.total_weight) + (lon * weight)) / (self.total_weight + weight);
        self.accuracy = ((self.accuracy * self.total_weight) + (accuracy * weight))
            / (self.total_weight + weight);

        self.total_weight = self.total_weight + weight;

        if strength < self.min_strength {
            self.min_strength = strength;
        } else if strength > self.max_strength {
            self.max_strength = strength;
        }
    }

    /// Check the validity of the new access point coordinates in terms of GPS signal accuracy (spoofing)
    /// Based on old accuracy algorithm (bounding box) as weighted
    /// average "accuracy" data can't detect moving AP
    pub fn valid(&self) -> bool {
        let (min, max) = self.points();
        let center = (min + max) / 2.0;
        let distance = Haversine::distance(min, center);
        if (0.0..=CONFIG.locator.radius_wifi_detection).contains(&distance) {
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transmitter_location_update() {
        // Values were chosen so all floats are rounds, to be easier to test
        let mut location =
            TransmitterLocation::new("11::22::33::44::55::66", 0.0, 0.0, 20.0, 1.0, -72.0);
        location.update(1.8, 0.9, 5.0, 2.0, -56.0);

        assert_eq!(location.max_lat, 1.8);
        assert_eq!(location.max_lon, 0.9);
        assert_eq!(location.min_lat, 0.0);
        assert_eq!(location.min_lon, 0.0);
        assert_eq!(location.lat, 1.2);
        assert_eq!(location.lon, 0.6);
        assert_eq!(location.accuracy, 10.0);
        assert_eq!(location.total_weight, 3.0);

        location.update(-7.2, -4.5, 5.0, 2.0, -76.0);

        assert_eq!(location.max_lat, 1.8);
        assert_eq!(location.max_lon, 0.9);
        assert_eq!(location.min_lat, -7.2);
        assert_eq!(location.min_lon, -4.5);
        assert_eq!(location.lat, -2.16);
        assert_eq!(location.lon, -1.44);
        assert_eq!(location.accuracy, 8.0);
        assert_eq!(location.total_weight, 5.0);
    }
}
