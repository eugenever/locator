use std::time::Duration;

use log::{error, warn};
use redis::{AsyncConnectionConfig, RedisError, aio::MultiplexedConnection};
use tokio::{sync::oneshot, task::JoinHandle};

use crate::config::CONFIG;

const TIMEOUT_CONNECTION: u64 = 5000; // milliseconds

pub enum BAConnectionManageMessage {
    GetConnection {
        tx: oneshot::Sender<Option<MultiplexedConnection>>,
        error: Option<String>,
    },
    RecoverFailedService {
        recover_pool: Vec<MultiplexedConnection>,
    },
}

pub async fn manage_blobasaur(
    rx_ba: flume::Receiver<BAConnectionManageMessage>,
    tx_ba: flume::Sender<BAConnectionManageMessage>,
) -> Result<JoinHandle<()>, anyhow::Error> {
    let mut pool = connection_pool().await?;
    if pool.is_empty() {
        return Err(anyhow::anyhow!("Blobasaur connection pool is empty"));
    }

    let jh = tokio::spawn(async move {
        let mut index_conn = 0;

        while let Ok(message) = rx_ba.recv_async().await {
            match message {
                BAConnectionManageMessage::GetConnection { tx, error } => {
                    if let Some(_e) = error {
                        if let Ok(client) = ba_client(&CONFIG.blobasaur.host, CONFIG.blobasaur.port)
                        {
                            if client
                                .get_connection_with_timeout(Duration::from_millis(
                                    TIMEOUT_CONNECTION,
                                ))
                                .is_err()
                            {
                                if let Err(_) = tx.send(None) {
                                    error!("send Blobasaur connection");
                                }
                                // run the recovery task only once
                                if !pool.is_empty() {
                                    pool.clear();
                                    let _jh = recover_failed_blobasaur_service(tx_ba.clone());
                                }
                                continue;
                            } else {
                                if let Ok(p) = connection_pool_with_client(client).await {
                                    pool = p;
                                } else {
                                    if let Err(_) = tx.send(None) {
                                        error!("send Blobasaur connection");
                                    }
                                    continue;
                                }
                            }
                        }
                    }

                    if index_conn > pool.len() - 1 {
                        index_conn = 0;
                    }
                    if let Some(c) = pool.get(index_conn) {
                        if let Err(_) = tx.send(Some(c.clone())) {
                            error!("send Blobasaur connection");
                        }
                    } else {
                        if let Err(_) = tx.send(None) {
                            error!("send Blobasaur connection");
                        }
                    }
                    index_conn += 1;
                }
                BAConnectionManageMessage::RecoverFailedService { recover_pool } => {
                    pool = recover_pool;
                    warn!("failed instance Blobasaur is recover");
                }
            }
        }
    });

    Ok(jh)
}

fn ba_client(host: &str, port: u16) -> Result<redis::Client, RedisError> {
    let host = format!("redis://{}:{}/", host, port);
    redis::Client::open(host)
}

async fn connection_pool() -> Result<Vec<MultiplexedConnection>, anyhow::Error> {
    let client = ba_client(&CONFIG.blobasaur.host, CONFIG.blobasaur.port)?;
    let config = AsyncConnectionConfig::new()
        .set_connection_timeout(Some(Duration::from_secs(5)))
        .set_response_timeout(Some(Duration::from_secs(5)));
    let mut connections = Vec::with_capacity(CONFIG.blobasaur.pool_size as usize);
    for _ in 0..CONFIG.blobasaur.pool_size {
        let c = client
            .get_multiplexed_async_connection_with_config(&config)
            .await?;
        connections.push(c);
    }
    Ok(connections)
}

async fn connection_pool_with_client(
    client: redis::Client,
) -> Result<Vec<MultiplexedConnection>, anyhow::Error> {
    let config = AsyncConnectionConfig::new()
        .set_connection_timeout(Some(Duration::from_secs(5)))
        .set_response_timeout(Some(Duration::from_secs(5)));
    let mut connections = Vec::with_capacity(CONFIG.blobasaur.pool_size as usize);
    for _ in 0..CONFIG.blobasaur.pool_size {
        let c = client
            .get_multiplexed_async_connection_with_config(&config)
            .await?;
        connections.push(c);
    }
    Ok(connections)
}

pub async fn get_connection(
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
    error: Option<String>,
) -> Result<MultiplexedConnection, anyhow::Error> {
    let (tx, rx) = oneshot::channel();
    tx_ba_conn
        .send_async(BAConnectionManageMessage::GetConnection { tx, error })
        .await?;

    match rx
        .await
        .map_err(|e| anyhow::anyhow!("receive blobasaur connection: {}", e))
    {
        Err(e) => Err(e),
        Ok(v) => {
            if let Some(c) = v {
                Ok(c)
            } else {
                Err(anyhow::anyhow!("blobasaur connection not defined"))
            }
        }
    }
}

fn recover_failed_blobasaur_service(
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
) -> JoinHandle<()> {
    tokio::spawn({
        async move {
            if let Ok(client) = ba_client(&CONFIG.blobasaur.host, CONFIG.blobasaur.port) {
                // waiting indefinitely for the blobasaur instance to be restored
                while client
                    .get_connection_with_timeout(Duration::from_millis(TIMEOUT_CONNECTION))
                    .is_err()
                {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }

                if let Ok(c) = client.get_multiplexed_async_connection().await {
                    if let Ok(mut recover_pool) = connection_pool().await {
                        recover_pool.push(c);
                        if let Err(e) = tx_ba_conn
                            .send_async(BAConnectionManageMessage::RecoverFailedService {
                                recover_pool,
                            })
                            .await
                        {
                            error!("send recovered Tile38 node: {}", e);
                        }
                    }
                }
            }
        }
    })
}
