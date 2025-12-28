use serde::Deserialize;

use crate::{db::transmitter::TransmitterLocation, tasks::t38::T38ConnectionManageMessage};

/// A transmitter (cell tower, wifi network or bluetooth beacon)
#[derive(Debug, Clone, PartialEq, PartialOrd, strum_macros::Display)]
pub enum Transmitter {
    /// A cell tower
    #[strum(to_string = "{country}_{network}_{area}_{cell}")]
    Cell {
        radio: CellRadio,
        // all integers are stored as signed in postgres
        country: i16,
        network: i16,
        area: i32,
        cell: i64,
        unit: i16,
        signal_strength: Option<f64>,
        age: Option<i64>,
    },
    /// A wifi network based on its MAC-Address
    #[strum(to_string = "{mac}")]
    Wifi {
        mac: String,
        signal_strength: Option<f64>,
        age: Option<i64>,
    },
    /// A Bluetooth beacon
    #[strum(to_string = "{mac}")]
    Bluetooth {
        mac: String,
        signal_strength: Option<f64>,
        age: Option<i64>,
    },
}

/// Cell radio type
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "lowercase")]
#[repr(i16)]
pub enum CellRadio {
    Gsm = 2,
    Wcdma = 3,
    Lte = 4,
    Nr = 5,
}

impl Transmitter {
    /// Lookup the transmitter location data in the database
    pub async fn lookup(
        &self,
        tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    ) -> Result<Option<TransmitterLocation>, anyhow::Error> {
        let tl = match self {
            Transmitter::Cell { .. } => {
                return Err(anyhow::anyhow!("Cells unsupported"));
            }
            Transmitter::Wifi { mac, .. } => {
                let collection = crate::constants::Collection::Wifi.as_ref();
                crate::db::t38::get_wifi_one(tx_t38_conn, collection, &mac.clone()).await?
            }
            Transmitter::Bluetooth { .. } => {
                return Err(anyhow::anyhow!("Bluetooth unsupported"));
            }
        };

        Ok(tl)
    }

    pub fn signal_strength(&self) -> Option<f64> {
        match *self {
            Transmitter::Cell {
                signal_strength, ..
            } => signal_strength,
            Transmitter::Wifi {
                signal_strength, ..
            } => signal_strength,
            Transmitter::Bluetooth {
                signal_strength, ..
            } => signal_strength,
        }
    }

    pub fn age(&self) -> Option<i64> {
        match *self {
            Transmitter::Cell { age, .. } => age,
            Transmitter::Wifi { age, .. } => age,
            Transmitter::Bluetooth { age, .. } => age,
        }
    }
}
