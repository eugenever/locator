use std::{io, time::Duration};

use log::error;
use redis::{FromRedisValue, RedisError};

use crate::tasks::t38::{T38ConnectionManageMessage, get_conection};

const TIMEOUT: u64 = 1;
// loading the big AOF file into memory may take a long time
const COUNT_ATTEMPTS_RUN_CMD: u16 = 600; // equivalent to 600 seconds

const ID_NOT_FOUND_ERROR: &str = "id not found";
const KEY_NOT_FOUND_ERROR: &str = "key not found";
pub const REDIS_NO_DATA: &str = "no data";

pub async fn exec_cmd(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    cmd: redis::Cmd,
) -> Result<(), RedisError> {
    let mut connection = get_conection(tx_t38_conn.clone(), None).await.unwrap();
    let mut i: u16 = 0;

    while let Err(e) = cmd.exec_async(&mut connection).await {
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
                "Unable to connect to the Tile38 store",
            )
            .into());
        }

        tokio::time::sleep(Duration::from_secs(TIMEOUT)).await;

        match get_conection(tx_t38_conn.clone(), Some(e.to_string())).await {
            Err(e) => {
                return Err(io::Error::new(io::ErrorKind::NotConnected, e).into());
            }
            Ok(c) => {
                connection = c;
            }
        }
        i = i + 1;
    }

    Ok(())
}

pub async fn query_cmd(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    cmd: redis::Cmd,
) -> Result<Vec<u8>, RedisError> {
    let mut connection = get_conection(tx_t38_conn.clone(), None).await.unwrap();
    let mut i: u16 = 0;

    loop {
        match cmd.query_async(&mut connection).await {
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
            Ok(value) => {
                return Ok(value);
            }
        }
    }
}

pub async fn exec_pipeline(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    pipeline: redis::Pipeline,
) -> Result<(), RedisError> {
    let mut connection = get_conection(tx_t38_conn.clone(), None).await.unwrap();
    let mut i: u16 = 0;

    while let Err(e) = pipeline.exec_async(&mut connection).await {
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
                "Unable to connect to the Tile38 store",
            )
            .into());
        }

        tokio::time::sleep(Duration::from_secs(TIMEOUT)).await;

        match get_conection(tx_t38_conn.clone(), Some(e.to_string())).await {
            Err(e) => {
                return Err(io::Error::new(io::ErrorKind::NotConnected, e).into());
            }
            Ok(c) => {
                connection = c;
            }
        }
        i = i + 1;
    }

    Ok(())
}

pub async fn query_pipeline<T>(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    pipeline: redis::Pipeline,
) -> Result<Vec<Option<T>>, RedisError>
where
    T: serde::de::DeserializeOwned + FromRedisValue,
{
    let mut connection = get_conection(tx_t38_conn.clone(), None).await.unwrap();
    let mut i: u16 = 0;

    loop {
        match pipeline
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
                let mut objects = Vec::with_capacity(values.len());
                for v in values {
                    match T::from_redis_value(v) {
                        Ok(o) => {
                            objects.push(Some(o));
                        }
                        Err(e) => {
                            if !e.to_string().contains(REDIS_NO_DATA) {
                                error!("redis parsing: {}", e);
                            }
                            objects.push(None);
                        }
                    }
                }
                return Ok(objects);
            }
        }
    }
}
