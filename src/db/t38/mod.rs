pub mod cmd;
pub mod scan;
pub mod track;

use std::time::Duration;

use log::error;
use redis::{
    AsyncConnectionConfig, Client, FromRedisValue, RedisError, aio::MultiplexedConnection,
};
use serde::Deserialize;

use crate::{
    db::transmitter::TransmitterLocation, lbs::yandex::YandexLbsResponse,
    tasks::t38::T38ConnectionManageMessage,
};
use cmd::{exec_cmd, query_cmd, query_pipeline};

const TIMEOUT: u64 = 1;
// loading the big AOF file into memory may take a long time
const COUNT_ATTEMPTS_RUN_CMD: u16 = 600; // equivalent to 600 seconds

pub const ID_NOT_FOUND_ERROR: &str = "id not found";
pub const KEY_NOT_FOUND_ERROR: &str = "key not found";

#[derive(Debug, Clone, Deserialize)]
pub struct T38Role {
    // slave or master
    pub role: String,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub state: Option<String>,
    pub offset: Option<i32>,
    pub slaves: Option<Vec<T38Slave>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct T38Slave {
    pub ip: String,
    pub port: String,
    pub offset: String,
}

#[derive(Debug, Clone)]
pub struct T38Node {
    pub role: Option<String>,
    pub host: String,
    pub port: u16,
    pub state: Option<String>,
    pub client: Client,
}

pub fn t38_client(host: &str, port: u16) -> Result<redis::Client, RedisError> {
    let host = format!("redis://{}:{}/", host, port);
    redis::Client::open(host)
}

pub async fn create_connection(
    host: &str,
    port: u16,
) -> Result<redis::aio::MultiplexedConnection, RedisError> {
    let host = format!("redis://{}:{}/", host, port);
    let c = AsyncConnectionConfig::new()
        .set_connection_timeout(Some(Duration::from_secs(5)))
        .set_response_timeout(Some(Duration::from_secs(5)));
    redis::Client::open(host)?
        .get_multiplexed_async_connection_with_config(&c)
        .await
}

pub async fn healthz(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
) -> Result<(), RedisError> {
    let cmd = redis::cmd("HEALTHZ");
    exec_cmd(tx_t38_conn, cmd).await
}

pub async fn aofshrink(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
) -> Result<(), RedisError> {
    let cmd = redis::cmd("AOFSHRINK");
    exec_cmd(tx_t38_conn, cmd).await
}

pub async fn gc(tx_t38_conn: flume::Sender<T38ConnectionManageMessage>) -> Result<(), RedisError> {
    let cmd = redis::cmd("GC");
    exec_cmd(tx_t38_conn, cmd).await
}

pub async fn ping(mut connection: MultiplexedConnection) -> Result<(), RedisError> {
    if let Err(e) = redis::cmd("PING").exec_async(&mut connection).await {
        error!("ping: {}", e);
    };
    Ok(())
}

pub async fn get_role(mut connection: MultiplexedConnection) -> Result<Option<String>, RedisError> {
    let cmd = redis::cmd("ROLE");
    match cmd.query_async::<Vec<redis::Value>>(&mut connection).await {
        Err(e) => {
            return Err(e);
        }
        Ok(values) => {
            /*
                MASTER response:

                "role":"master",
                "offset":5496810,
                "slaves":[
                    {"ip":"127.0.0.1","port":"9852","offset":"5496810"},
                    {"ip":"127.0.0.1","port":"9853","offset":"5496810"}
                ]


                SLAVE response:

                "role":"slave","host":"127.0.0.1","port":9851,"state":"connected","offset":5633527
            */
            Ok(String::from_redis_value(values[0].clone()).ok())
        }
    }
}

pub async fn follow_no_one(mut connection: MultiplexedConnection) -> Result<(), RedisError> {
    redis::cmd("FOLLOW")
        .arg("no")
        .arg("one")
        .exec_async(&mut connection)
        .await?;
    Ok(())
}

pub async fn follow(
    mut connection: MultiplexedConnection,
    host: &str,
    port: u16,
) -> Result<(), RedisError> {
    redis::cmd("FOLLOW")
        .arg(host)
        .arg(port.to_string())
        .exec_async(&mut connection)
        .await?;
    Ok(())
}

// collection = wifi
pub async fn set_wifi_one(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    collection: &str,
    tl: &TransmitterLocation,
) -> Result<(), RedisError> {
    let tl_bytes = serde_json::to_vec(&tl).unwrap();
    let cmd_arg = redis::cmd("SET")
        .arg(collection)
        .arg(&tl.mac)
        .arg("field")
        .arg("mac")
        .arg(&tl.mac)
        .arg("field")
        .arg("data")
        .arg(tl_bytes)
        .arg("POINT")
        .arg(tl.lon) // longitude
        .arg(tl.lat) // latitude
        .to_owned();
    exec_cmd(tx_t38_conn, cmd_arg).await
}

pub async fn fget_wifi_many_from_pipeline<T>(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    collection: &str,
    macs: &[&str],
) -> Result<Vec<Option<T>>, RedisError>
where
    T: serde::de::DeserializeOwned + FromRedisValue,
{
    let mut pipeline = redis::pipe();
    for mac in macs {
        pipeline
            .cmd("FGETPIPE")
            .arg(collection)
            .arg(mac)
            .arg("data");
    }
    match query_pipeline(tx_t38_conn, pipeline).await {
        Err(e) => {
            let e_str = e.to_string();
            if e_str.contains(ID_NOT_FOUND_ERROR) || e_str.contains(KEY_NOT_FOUND_ERROR) {
                return Ok(vec![]);
            }
            error!("fget wifi data from pipeline: {}", e);
            return Err(e);
        }
        Ok(objects) => {
            return Ok(objects);
        }
    }
}

pub async fn get_wifi_many_from_pipeline<T>(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    collection: &str,
    macs: &[&str],
) -> Result<Vec<Option<T>>, RedisError>
where
    T: serde::de::DeserializeOwned + FromRedisValue,
{
    let mut pipeline = redis::pipe();
    for mac in macs {
        pipeline
            .cmd("GETPIPE")
            .arg(collection)
            .arg(mac)
            .arg("WITHFIELDS");
    }
    match query_pipeline(tx_t38_conn, pipeline).await {
        Err(e) => {
            let e_str = e.to_string();
            if e_str.contains(ID_NOT_FOUND_ERROR) || e_str.contains(KEY_NOT_FOUND_ERROR) {
                return Ok(vec![]);
            }
            error!("get wifi data from pipeline: {}", e);
            return Err(e);
        }
        Ok(objects) => {
            return Ok(objects);
        }
    }
}

pub async fn get_wifi_one(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    collection: &str,
    mac: &str,
) -> Result<Option<TransmitterLocation>, RedisError> {
    let cmd_arg = redis::cmd("FGET")
        .arg(collection)
        .arg(mac)
        .arg("data")
        .to_owned();
    match query_cmd(tx_t38_conn, cmd_arg).await {
        Err(e) => {
            let e_str = e.to_string();
            if e_str.contains(ID_NOT_FOUND_ERROR) || e_str.contains(KEY_NOT_FOUND_ERROR) {
                return Ok(None);
            }
            error!("get wifi data: {}", e);
            return Err(e);
        }
        Ok(value) => {
            // crud.go, row 972, return empty bulk-string:
            // return resp.StringValue(""), nil
            // alternative for ID_NOT_FOUND_ERROR
            if value.len() == 0 {
                return Ok(None);
            }

            let tl_str = String::from_utf8(value).unwrap();
            let tl: TransmitterLocation = serde_json::from_str(&tl_str).unwrap();
            Ok(Some(tl))
        }
    }
}

// collection = "lbs:yandex:wifi"
pub async fn set_yandex_lbs_wifi_one(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    collection: &str,
    yandex_lbs_response: YandexLbsResponse,
    mac: &str,
) -> Result<(), RedisError> {
    let ylr_bytes = serde_json::to_vec(&yandex_lbs_response).unwrap();
    let cmd_arg = redis::cmd("SET")
        .arg(collection)
        .arg(mac)
        .arg("field")
        .arg("mac")
        .arg(mac)
        .arg("field")
        .arg("data")
        .arg(ylr_bytes)
        .arg("POINT")
        .arg(yandex_lbs_response.location.point.lon) // longitude
        .arg(yandex_lbs_response.location.point.lat) // latitude
        .to_owned();
    exec_cmd(tx_t38_conn, cmd_arg).await
}

// collection = "lbs:yandex:wifi"
pub async fn get_yandex_lbs_wifi_one(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    collection: &str,
    mac: &str,
) -> Result<Option<YandexLbsResponse>, RedisError> {
    let cmd_arg = redis::cmd("FGET")
        .arg(collection)
        .arg(mac)
        .arg("data")
        .to_owned();
    match query_cmd(tx_t38_conn, cmd_arg).await {
        Err(e) => {
            let e_str = e.to_string();
            if e_str.contains(ID_NOT_FOUND_ERROR) || e_str.contains(KEY_NOT_FOUND_ERROR) {
                return Ok(None);
            }
            error!("get Yandex LBS data: {}", e);
            return Err(e);
        }
        Ok(value) => {
            // crud.go, row 972, return empty bulk-string:
            // return resp.StringValue(""), nil
            // alternative for ID_NOT_FOUND_ERROR
            if value.len() == 0 {
                return Ok(None);
            }

            let ylr_str = String::from_utf8(value).unwrap();
            let ylr: YandexLbsResponse = serde_json::from_str(&ylr_str).unwrap();
            Ok(Some(ylr))
        }
    }
}

// collection = "lbs:yandex:cell"
pub async fn set_yandex_lbs_cell_one(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    collection: &str,
    yandex_lbs_response: YandexLbsResponse,
    cell_code: &str,
) -> Result<(), RedisError> {
    let ylr_bytes = serde_json::to_vec(&yandex_lbs_response).unwrap();
    let cmd_arg = redis::cmd("JSET")
        .arg(collection)
        .arg(cell_code)
        .arg("data")
        .arg(ylr_bytes)
        .to_owned();
    exec_cmd(tx_t38_conn, cmd_arg).await
}

// collection = "lbs:yandex:cell"
pub async fn get_yandex_lbs_cell_one(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    collection: &str,
    cell_code: &str,
) -> Result<Option<YandexLbsResponse>, RedisError> {
    let cmd_arg = redis::cmd("JGET")
        .arg(collection)
        .arg(cell_code)
        .arg("data")
        .to_owned();
    match query_cmd(tx_t38_conn, cmd_arg).await {
        Err(e) => {
            let e_str = e.to_string();
            if e_str.contains(ID_NOT_FOUND_ERROR) || e_str.contains(KEY_NOT_FOUND_ERROR) {
                return Ok(None);
            }
            error!("get Yandex LBS data: {}", e);
            return Err(e);
        }
        Ok(value) => {
            // crud.go, row 972, return empty bulk-string:
            // return resp.StringValue(""), nil
            // alternative for ID_NOT_FOUND_ERROR
            if value.len() == 0 {
                return Ok(None);
            }

            let ylr_str = String::from_utf8(value).unwrap();
            let ylr: YandexLbsResponse = serde_json::from_str(&ylr_str).unwrap();
            Ok(Some(ylr))
        }
    }
}

// very slow responses from Tile38
#[allow(unused)]
pub async fn get_wifi_many(
    mut connection: MultiplexedConnection,
    collection: &str,
    selected_macs: &[&str],
) -> Result<Vec<TransmitterLocation>, RedisError> {
    let mut where_expression = String::new();
    for (i, mac) in selected_macs.iter().enumerate() {
        if i == 0 {
            where_expression = format!(r#"mac == "{}""#, mac);
        } else {
            where_expression.push_str(&format!(r#" || mac == "{}""#, mac));
        }
    }

    let mut scan = redis::cmd("SCAN");
    let cmd_scan = scan.arg(collection).arg("WHERE").arg(where_expression);
    let scan_result: redis::Value = cmd_scan.query_async(&mut connection).await?;

    let mut tls: Vec<TransmitterLocation> = Vec::with_capacity(selected_macs.len());

    match scan_result {
        redis::Value::Array(arr) => {
            for v in arr {
                match v {
                    redis::Value::Array(arr1) => {
                        for v1 in arr1 {
                            match v1 {
                                redis::Value::Array(arr2) => {
                                    for v2 in arr2 {
                                        match v2 {
                                            redis::Value::Array(arr3) => {
                                                for (i3, v3) in arr3.iter().enumerate() {
                                                    match v3 {
                                                        redis::Value::BulkString(data) => {
                                                            // i3 == 0 => field name = "data"
                                                            if i3 == 1 {
                                                                match serde_json::from_slice::<
                                                                    TransmitterLocation,
                                                                >(
                                                                    &data
                                                                ) {
                                                                    Err(err) => {
                                                                        error!(
                                                                            "deserialize tl from Tile38: {}",
                                                                            err
                                                                        );
                                                                    }
                                                                    Ok(tl) => {
                                                                        tls.push(tl);
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        _ => {}
                                                    }
                                                }
                                            }
                                            redis::Value::BulkString(_field) => {
                                                // fields:
                                                // mac String
                                                // geometry Object
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    redis::Value::Int(_i) => {
                        // at first level _i == 0
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }

    Ok(tls)
}
