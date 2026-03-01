use std::io;
use std::time::Duration;

use log::error;
use redis::{FromRedisValue, RedisError};

use crate::tasks::blobasaur::{BAConnectionManageMessage, get_connection};

const TIMEOUT: u64 = 1;
// loading the big AOF file into memory may take a long time
const COUNT_ATTEMPTS_RUN_CMD: u16 = 5; // equivalent to 5 seconds

pub async fn exec_cmd(
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
    cmd: redis::Cmd,
) -> Result<(), RedisError> {
    if let Ok(mut connection) = get_connection(tx_ba_conn.clone(), None).await {
        let mut i: u16 = 0;
        while let Err(e) = cmd.exec_async(&mut connection).await {
            if i > COUNT_ATTEMPTS_RUN_CMD {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "Unable to connect to the Blobasaur store",
                )
                .into());
            }

            tokio::time::sleep(Duration::from_secs(TIMEOUT)).await;

            match get_connection(tx_ba_conn.clone(), Some(e.to_string())).await {
                Err(e) => {
                    return Err(io::Error::new(io::ErrorKind::NotConnected, e).into());
                }
                Ok(c) => {
                    connection = c;
                }
            }
            i += 1;
        }
    }
    Ok(())
}

pub async fn query_cmd(
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
    cmd: redis::Cmd,
) -> Result<Vec<u8>, RedisError> {
    match get_connection(tx_ba_conn.clone(), None).await {
        Ok(mut connection) => {
            let mut i: u16 = 0;
            loop {
                match cmd.query_async(&mut connection).await {
                    Err(e) => {
                        if i > COUNT_ATTEMPTS_RUN_CMD {
                            return Err(io::Error::new(
                                io::ErrorKind::NotConnected,
                                format!(
                                    "Unable to connect to the Blobasaur store: {}",
                                    e.to_string()
                                ),
                            )
                            .into());
                        }

                        tokio::time::sleep(Duration::from_secs(TIMEOUT)).await;

                        match get_connection(tx_ba_conn.clone(), Some(e.to_string())).await {
                            Err(e) => {
                                error!("get_connecton blobasaur: {}", e);
                                continue;
                            }
                            Ok(c) => {
                                connection = c;
                            }
                        }
                        i += 1;
                    }
                    Ok(value) => {
                        return Ok(value);
                    }
                }
            }
        }
        Err(e) => Err(io::Error::new(io::ErrorKind::NotConnected, e).into()),
    }
}

pub async fn query_cmd_generic<T: FromRedisValue>(
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
    cmd: redis::Cmd,
) -> Result<T, RedisError> {
    match get_connection(tx_ba_conn.clone(), None).await {
        Ok(mut connection) => {
            let mut i: u16 = 0;
            loop {
                match cmd.query_async(&mut connection).await {
                    Err(e) => {
                        if i > COUNT_ATTEMPTS_RUN_CMD {
                            return Err(io::Error::new(
                                io::ErrorKind::NotConnected,
                                format!(
                                    "Unable to connect to the Blobasaur store: {}",
                                    e.to_string()
                                ),
                            )
                            .into());
                        }

                        tokio::time::sleep(Duration::from_secs(TIMEOUT)).await;

                        match get_connection(tx_ba_conn.clone(), Some(e.to_string())).await {
                            Err(e) => {
                                error!("get_connecton blobasaur: {}", e);
                                continue;
                            }
                            Ok(c) => {
                                connection = c;
                            }
                        }
                        i += 1;
                    }
                    Ok(value) => {
                        return Ok(value);
                    }
                }
            }
        }
        Err(e) => Err(io::Error::new(io::ErrorKind::NotConnected, e).into()),
    }
}
