use std::io;
use std::time::Duration;

use log::error;
use redis::RedisError;

use super::{COUNT_ATTEMPTS_RUN_CMD, ID_NOT_FOUND_ERROR, KEY_NOT_FOUND_ERROR, TIMEOUT};
use crate::tasks::t38::{T38ConnectionManageMessage, get_conection};

pub async fn scan_ids(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    collection: &str,
    limit: u64,
) -> Result<Vec<String>, RedisError> {
    let cmd_arg = redis::cmd("SCAN")
        .arg(collection)
        .arg("LIMIT")
        .arg(limit)
        .arg("IDS")
        .to_owned();

    let mut connection = get_conection(tx_t38_conn.clone(), None).await.unwrap();
    let mut i: u16 = 0;

    loop {
        match cmd_arg
            .query_async::<Vec<redis::Value>>(&mut connection)
            .await
        {
            Err(e) => {
                if e.to_string().contains(ID_NOT_FOUND_ERROR)
                    || e.to_string().contains(KEY_NOT_FOUND_ERROR)
                {
                    // id was not found in the tile38 database.
                    return Err(e);
                }

                if e.category() == "busy loading" {
                    error!("Tile38 is unavailable because it is loading the dataset into memory");
                    return Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        "Unable to connect to the Tile38 store",
                    )
                    .into());
                }

                if i > COUNT_ATTEMPTS_RUN_CMD {
                    return Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        "Unable to connect to the Redis store",
                    )
                    .into());
                }

                tokio::time::sleep(Duration::from_secs(TIMEOUT)).await;

                match get_conection(tx_t38_conn.clone(), Some(e.to_string())).await {
                    Err(e) => {
                        error!("get_connecton: {}", e);
                        continue;
                    }
                    Ok(c) => {
                        connection = c;
                    }
                }
                i = i + 1;
            }
            Ok(values) => {
                for v in values {
                    match v {
                        redis::Value::Array(array) => {
                            let mut ids = Vec::with_capacity(array.len());
                            for id_value in array {
                                match id_value {
                                    redis::Value::BulkString(bulk_string) => {
                                        let id = String::from_utf8(bulk_string).unwrap();
                                        ids.push(id);
                                    }
                                    _ => {}
                                }
                            }
                            return Ok(ids);
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

#[allow(unused)]
mod tests {
    use crate::{
        services::locate::dbscan::{Algorithm, DBSCAN, Point},
        tasks::t38::{T38ConnectionManageMessage, connection_manage_task},
    };
    use clusters::Proximity;

    async fn connection_manager() -> flume::Sender<T38ConnectionManageMessage> {
        let (tx_t38_conn, rx_t38_conn) = flume::unbounded::<T38ConnectionManageMessage>();
        let _connection_manage_t38_handle =
            connection_manage_task(&crate::CONFIG, rx_t38_conn, tx_t38_conn.clone())
                .await
                .unwrap();
        tx_t38_conn
    }

    #[tokio::test]
    async fn test_scan_ids() {
        let tx_t38_conn = connection_manager().await;

        let limit = 3;
        match super::scan_ids(tx_t38_conn.clone(), "lbs:yandex:wifi", limit).await {
            Err(e) => {
                println!("error scan_ids: {}", e);
            }
            Ok(ids) => {
                let p_moscow = Point {
                    id: 0,
                    lat: 55.758608,
                    lon: 37.616500,
                };
                for id in ids {
                    match crate::db::t38::get_yandex_lbs_wifi_one(
                        tx_t38_conn.clone(),
                        "lbs:yandex:wifi",
                        &id,
                    )
                    .await
                    {
                        Err(e) => {
                            println!("error get_yandex_lbs_wifi_one: {}", e);
                        }
                        Ok(ylr_opt) => {
                            if let Some(ylr) = ylr_opt {
                                let p_yandex = Point {
                                    id: 1,
                                    lat: ylr.location.point.lat,
                                    lon: ylr.location.point.lon,
                                };
                                let d = p_yandex.distance(&p_moscow);
                                if d > 120000.0 {
                                    let id_no_colon = id.replace(":", "");
                                    println!(
                                        "MAC: {} / {}, coordinates: ({},{}), d: {}",
                                        id, id_no_colon, p_yandex.lat, p_yandex.lon, d
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
