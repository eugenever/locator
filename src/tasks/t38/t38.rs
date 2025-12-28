use log::error;
use redis::aio::MultiplexedConnection;
use tokio::{sync::oneshot, task::JoinHandle};
use tokio_schedule::Job;

use super::{T38ConnectionManageMessage, manage_master_replica};
use crate::config::Config;

// every aofshrink_frequency day(s) at two o'clock
pub fn aofshrink_task(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    aofshrink_frequency: u32,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        tokio_schedule::every(aofshrink_frequency)
            .day()
            .at(2, 0, 0)
            .perform(|| async {
                if let Err(err) = crate::db::t38::aofshrink(tx_t38_conn.clone()).await {
                    error!("AOFSHRINK Tile38: {}", err);
                }
            })
            .await;
    })
}

pub fn gc_task(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    gc_frequency: u32,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        tokio_schedule::every(gc_frequency)
            .seconds()
            .perform(|| async {
                if let Err(err) = crate::db::t38::gc(tx_t38_conn.clone()).await {
                    error!("GC Tile38: {}", err);
                }
            })
            .await;
    })
}

pub async fn get_conection(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    error: Option<String>,
) -> Result<MultiplexedConnection, anyhow::Error> {
    let (tx, rx) = oneshot::channel();
    tx_t38_conn
        .send_async(T38ConnectionManageMessage::GetConnection { tx, error })
        .await
        .unwrap();

    match rx
        .await
        .map_err(|err| anyhow::anyhow!("error receive tile38 master connection: {}", err))
    {
        Err(e) => Err(e),
        Ok(v) => {
            if let Some(c) = v {
                Ok(c)
            } else {
                Err(anyhow::anyhow!("Tile38 connection not defined"))
            }
        }
    }
}

pub fn healthz_task(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    healthz_frequency: u32,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        tokio_schedule::every(healthz_frequency)
            .seconds()
            .perform(|| async {
                if let Err(err) = crate::db::t38::healthz(tx_t38_conn.clone()).await {
                    error!("Healthz Tile38: {}", err);
                }
            })
            .await;
    })
}

pub async fn connection_manage_task(
    config: &Config,
    rx: flume::Receiver<T38ConnectionManageMessage>,
    tx: flume::Sender<T38ConnectionManageMessage>,
) -> Result<JoinHandle<()>, anyhow::Error> {
    if config.t38.instances.is_some() {
        let res_jh = manage_master_replica(config, rx, tx).await;
        return res_jh;
    } else if config.t38.sentinel.is_some() {
        let jh = tokio::spawn(async {});
        return Ok(jh);
    }
    Err(anyhow::anyhow!("connection management task is not running"))
}
