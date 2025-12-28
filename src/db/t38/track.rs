use log::error;
use redis::RedisError;
use serde::{Deserialize, Serialize};

use crate::{
    db::t38::{
        ID_NOT_FOUND_ERROR, KEY_NOT_FOUND_ERROR,
        cmd::{exec_cmd, query_cmd},
    },
    tasks::t38::T38ConnectionManageMessage,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WifiTrack {
    pub device_id: String,
    pub records: Vec<WifiTrackRecord>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WifiTrackRecord {
    pub ts: i64,
    pub gnss: Option<Gnss>,
    pub wifi: Vec<Wifi>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Wifi {
    // mac address
    pub m: String,
    // rssi
    pub r: f64,
    // gnss
    pub g: Gnss,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Gnss {
    pub lon: f64,
    pub lat: f64,
}

// collection = "device:whoosh"
pub async fn set_wifi_track_record_one(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    collection: &str,
    wifi_track: WifiTrack,
) -> Result<(), RedisError> {
    let track_bytes = serde_json::to_vec(&wifi_track).unwrap();
    let cmd_arg = redis::cmd("JSET")
        .arg(collection)
        .arg(&wifi_track.device_id)
        .arg("data")
        .arg(track_bytes)
        .to_owned();
    exec_cmd(tx_t38_conn, cmd_arg).await
}

// collection = "device:whoosh"
pub async fn get_wifi_track_one(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    collection: &str,
    device_id: &str,
) -> Result<Option<WifiTrack>, RedisError> {
    let cmd_arg = redis::cmd("JGET")
        .arg(collection)
        .arg(device_id)
        .arg("data")
        .to_owned();
    match query_cmd(tx_t38_conn, cmd_arg).await {
        Err(e) => {
            let e_str = e.to_string();
            if e_str.contains(ID_NOT_FOUND_ERROR) || e_str.contains(KEY_NOT_FOUND_ERROR) {
                return Ok(None);
            }
            error!(
                "device id '{}', get wifi track record data: {}",
                device_id, e
            );
            return Err(e);
        }
        Ok(value) => {
            // crud.go, row 1132, return empty bulk-string:
            // return resp.StringValue(""), nil
            // alternative for ID_NOT_FOUND_ERROR
            if value.len() == 0 {
                return Ok(None);
            }

            let wifi_track_str = String::from_utf8(value).unwrap();
            let mut wifi_track: WifiTrack = serde_json::from_str(&wifi_track_str).unwrap();
            // first element is the oldest, the last one is the newest
            wifi_track.records.sort_by_key(|wtr| wtr.ts);

            Ok(Some(wifi_track))
        }
    }
}
