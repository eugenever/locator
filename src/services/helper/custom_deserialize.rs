use chrono::{DateTime, SubsecRound, Utc};
use serde::{Deserialize, Deserializer};

use crate::error::ApiError;

pub fn default_timestamp() -> DateTime<Utc> {
    chrono::offset::Utc::now().round_subsecs(0)
}

pub fn default_timestamp_ms() -> DateTime<Utc> {
    chrono::offset::Utc::now().round_subsecs(3)
}

// accept timestamps in seconds or milliseconds.
pub fn date_time_utc_from_str<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let threshold: i64 = 1_700_000_000_000;
    let n: i64 = Deserialize::deserialize(deserializer)?;
    if n < threshold {
        let res = DateTime::from_timestamp(n, 0)
            .ok_or(ApiError::TimestampParseFailed(n.to_string()))
            .map_err(serde::de::Error::custom)?;
        return Ok(res);
    }
    let res = DateTime::from_timestamp_millis(n)
        .ok_or(ApiError::TimestampParseFailed(n.to_string()))
        .map_err(serde::de::Error::custom)?;
    Ok(res)
}

// accept mac-address with or without colons:
// "68:72:51:70:35:6b"
// "68725170356b"
pub fn mac_address<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let mut mac: String = Deserialize::deserialize(deserializer)?;
    if !mac.contains(":") {
        mac = mac_formatted(&mac, 2);
    }
    Ok(mac)
}

// BSSID formatted as xx:xx:xx:xx:xx:xx
fn mac_formatted(mac: &str, chunk_size: usize) -> String {
    // format only 12-digit addresses
    if mac.len() != 12 {
        return mac.to_string();
    }

    let mut sections = Vec::with_capacity(6);
    let mut remaining = mac;
    loop {
        // Get the byte offset of the nth character each time so we can split the string
        match remaining.char_indices().nth(chunk_size) {
            Some((offset, _)) => {
                let (a, b) = remaining.split_at(offset);
                sections.push(a);
                remaining = b;
            }
            None => {
                sections.push(remaining);
                return sections.join(":");
            }
        }
    }
}
