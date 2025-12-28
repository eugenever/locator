use std::env;

use crate::config::Config;
use deadpool_postgres::{Manager, Pool, PoolError};
use tokio_postgres::Config as ConfigTP;

pub fn create_pool_tp(config: &Config) -> Result<deadpool_postgres::Pool, anyhow::Error> {
    dotenvy::dotenv()?;

    let mut cfg = ConfigTP::new();
    cfg.host("127.0.0.1");
    cfg.user(&env::var("POSTGRES_USER").unwrap());
    cfg.password(&env::var("POSTGRES_PASSWORD").unwrap());
    cfg.dbname(&env::var("POSTGRES_DBNAME").unwrap());

    let port = &env::var("POSTGRES_DBPORT").unwrap();
    cfg.port(port.parse::<u16>().unwrap());

    let manager = Manager::new(cfg, tokio_postgres::NoTls);
    let pool = Pool::builder(manager)
        .max_size(config.database.max_connections_db)
        .build()?;

    return Ok(pool);
}

pub async fn db_client(pool: &Pool) -> Result<deadpool_postgres::Client, PoolError> {
    pool.get().await
}
