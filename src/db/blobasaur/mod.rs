#![allow(unused)]

pub mod cmd;

use log::error;
use redis::RedisError;

use crate::{
    db::{
        blobasaur::cmd::{exec_cmd, query_cmd, query_cmd_generic},
        pg::transmitter::TransmitterLocation,
    },
    lbs::yandex::wifi::YandexLbsResponse,
    tasks::blobasaur::BAConnectionManageMessage,
};

// LIMITER by API Key
pub async fn get_ba_limiter(
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
    api_key: &str,
) -> Result<u64, RedisError> {
    let cmd_arg = redis::cmd("LIMITER").arg(api_key).to_owned();
    match query_cmd_generic::<u64>(tx_ba_conn, cmd_arg).await {
        Err(e) => {
            error!("get blobasaur limiter: {}", e);
            Err(e)
        }
        Ok(value) => Ok(value),
    }
}

// namespace (hash) = whoosh_wifi
pub async fn set_ba_wifi_one(
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
    namespace: &str,
    tl: &TransmitterLocation,
) -> Result<(), RedisError> {
    let tl_bytes = serde_json::to_vec(&tl).unwrap();
    let cmd_arg = redis::cmd("HSET")
        .arg(namespace)
        .arg(&tl.mac)
        .arg(tl_bytes)
        .to_owned();
    exec_cmd(tx_ba_conn, cmd_arg).await
}

// namespace (hash) = whoosh_wifi
pub async fn get_ba_wifi_one(
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
    namespace: &str,
    mac: &str,
) -> Result<Option<TransmitterLocation>, RedisError> {
    let cmd_arg = redis::cmd("HGET").arg(namespace).arg(mac).to_owned();
    match query_cmd(tx_ba_conn, cmd_arg).await {
        Err(e) => {
            error!("get blobasaur whoosh wifi data: {}", e);
            Err(e)
        }
        Ok(value) => {
            if value.is_empty() {
                return Ok(None);
            }
            let tl_str = String::from_utf8(value).unwrap();
            let tl: TransmitterLocation = serde_json::from_str(&tl_str).unwrap();
            Ok(Some(tl))
        }
    }
}

// namespace (hash) = "lbs_yandex_wifi"
pub async fn set_ba_lbs_yandex_wifi_one(
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
    namespace: &str,
    yandex_lbs_response: YandexLbsResponse,
    mac: &str,
) -> Result<(), RedisError> {
    let ylr_bytes = serde_json::to_vec(&yandex_lbs_response).unwrap();
    let cmd_arg = redis::cmd("HSET")
        .arg(namespace)
        .arg(mac)
        .arg(ylr_bytes)
        .to_owned();
    exec_cmd(tx_ba_conn, cmd_arg).await
}

// namespace (hash) = "lbs_yandex_wifi"
pub async fn get_ba_lbs_yandex_wifi_one(
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
    namespace: &str,
    mac: &str,
) -> Result<Option<YandexLbsResponse>, RedisError> {
    let cmd_arg = redis::cmd("HGET").arg(namespace).arg(mac).to_owned();
    match query_cmd(tx_ba_conn, cmd_arg).await {
        Err(e) => {
            error!("get blobasaur Yandex LBS wifi: {}", e);
            Err(e)
        }
        Ok(value) => {
            if value.is_empty() {
                return Ok(None);
            }
            let ylr_str = String::from_utf8(value).unwrap();
            let ylr: YandexLbsResponse = serde_json::from_str(&ylr_str).unwrap();
            Ok(Some(ylr))
        }
    }
}

// namespace = "lbs_yandex_cell"
pub async fn set_ba_lbs_yandex_cell_one(
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
    namespace: &str,
    yandex_lbs_response: YandexLbsResponse,
    cell_code: &str,
) -> Result<(), RedisError> {
    let ylr_bytes = serde_json::to_vec(&yandex_lbs_response).unwrap();
    let cmd_arg = redis::cmd("HSET")
        .arg(namespace)
        .arg(cell_code)
        .arg(ylr_bytes)
        .to_owned();
    exec_cmd(tx_ba_conn, cmd_arg).await
}

// namespace = "lbs_yandex_cell"
pub async fn get_ba_lbs_yandex_cell_one(
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
    namespace: &str,
    cell_code: &str,
) -> Result<Option<YandexLbsResponse>, RedisError> {
    let cmd_arg = redis::cmd("HGET").arg(namespace).arg(cell_code).to_owned();
    match query_cmd(tx_ba_conn, cmd_arg).await {
        Err(e) => {
            error!("get blobasaur Yandex LBS cell: {}", e);
            Err(e)
        }
        Ok(value) => {
            if value.is_empty() {
                return Ok(None);
            }
            let ylr_str = String::from_utf8(value).unwrap();
            let ylr: YandexLbsResponse = serde_json::from_str(&ylr_str).unwrap();
            Ok(Some(ylr))
        }
    }
}
