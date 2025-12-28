#![allow(unused)]

use chrono::{DateTime, Utc};
use deadpool_postgres::Pool as PgPool;
use futures::future::join_all;
use log::{error, info};
use tokio::time::{Duration, sleep};
use tokio_postgres::Error as PgError;

use super::pool::db_client;
use crate::{
    CONFIG,
    services::{helper::determine_thread_num, submission::geosubmit::Report},
};

const MAX_RETRIES: usize = 3;

pub struct DataReport {
    pub report: Report,
    pub user_agent: String,
    pub processed_at: DateTime<Utc>,
    pub raw: Vec<u8>,
}

fn to_sql_param<T: tokio_postgres::types::ToSql + Sync>(
    value: &T,
) -> &(dyn tokio_postgres::types::ToSql + Sync) {
    value as &(dyn tokio_postgres::types::ToSql + Sync)
}

pub async fn bulk_insert_data(
    pool: &PgPool,
    data: &[DataReport],
    chunk_size: usize,
) -> Result<(), PgError> {
    let columns: &[&str] = if CONFIG.locator.process_report_online {
        &[
            "timestamp",
            "processed_at",
            "latitude",
            "longitude",
            "user_agent",
            "raw",
        ]
    } else {
        &["timestamp", "latitude", "longitude", "user_agent", "raw"]
    };

    let mut client = db_client(pool)
        .await
        .expect("failed to get a client from the pool");

    for chunk in data.chunks(chunk_size) {
        let mut retries = 0;

        let tx = client.transaction().await?;

        // Collect the parameters for each Report
        let insert_data: Vec<Vec<&(dyn tokio_postgres::types::ToSql + Sync)>> = chunk
            .iter()
            .map(|d| {
                if CONFIG.locator.process_report_online {
                    vec![
                        to_sql_param(&d.report.timestamp),
                        to_sql_param(&d.processed_at),
                        to_sql_param(&d.report.position.latitude),
                        to_sql_param(&d.report.position.longitude),
                        to_sql_param(&d.user_agent),
                        to_sql_param(&d.raw),
                    ]
                } else {
                    vec![
                        to_sql_param(&d.report.timestamp),
                        to_sql_param(&d.report.position.latitude),
                        to_sql_param(&d.report.position.longitude),
                        to_sql_param(&d.user_agent),
                        to_sql_param(&d.raw),
                    ]
                }
            })
            .collect();

        let (query, params) = build_pg_bulk_insert_query("report", columns, &insert_data);

        // Execute the transaction and asynchronously wait for the result
        if let Err(e) = tx.execute(&query, &params).await {
            if retries == MAX_RETRIES {
                error!("max retries reached, rolling back transaction: {:?}", e);
                tx.rollback().await?;
                return Err(e);
            }
            if let Some(pg_err) = e.as_db_error() {
                if pg_err.code().code() == "40P01" && retries < MAX_RETRIES {
                    info!("deadlock detected, retrying... Attempt {}", retries + 1,);
                    retries = retries + 1;
                    sleep(Duration::from_millis(1000)).await;
                    continue;
                }
            }
            tx.rollback().await?;
            return Err(e);
        }

        tx.commit().await?;
        sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

pub async fn bulk_insert_async(pool: &PgPool, data: &[DataReport]) -> Result<(), PgError> {
    let thread_num = determine_thread_num();
    let chunk_size = data.len() / thread_num;

    let tasks: Vec<_> = (0..thread_num)
        .map(|i| {
            let start_index = i * chunk_size;
            let end_index = if i == thread_num - 1 {
                data.len()
            } else {
                (i + 1) * chunk_size
            };

            let chunk = &data[start_index..end_index];
            let pool_clone = pool.clone();

            async move { bulk_insert_data(&pool_clone, chunk, chunk_size).await }
        })
        .collect();

    let results = join_all(tasks).await;

    for result in results {
        if let Err(e) = result {
            error!("bulk insert task failed: {:?}", e);
            return Err(e);
        }
    }

    Ok(())
}

pub fn build_pg_bulk_insert_query<'a>(
    table_name: &str,
    columns: &[&str],
    data: &[Vec<&'a (dyn tokio_postgres::types::ToSql + Sync + 'a)>], // Already trait objects
) -> (
    String,
    Vec<&'a (dyn tokio_postgres::types::ToSql + Sync + 'a)>,
) {
    let mut query = format!(
        "INSERT INTO {} ({}) VALUES ",
        table_name,
        columns.join(", ")
    );

    let mut all_params: Vec<&'a (dyn tokio_postgres::types::ToSql + Sync + 'a)> = Vec::new();
    let mut placeholders = Vec::new();

    let row_len = data[0].len(); // Assuming all rows have same length

    for (i, row) in data.iter().enumerate() {
        let mut row_placeholders: Vec<String> = Vec::new();
        // For regular columns
        for j in 0..row_len {
            row_placeholders.push(format!("${}", i * row_len + j + 1));
        }
        placeholders.push(format!("({})", row_placeholders.join(", ")));
        all_params.extend(row.iter());
    }

    query.push_str(&placeholders.join(", "));
    query.push_str(" ON CONFLICT DO NOTHING;");

    (query, all_params)
}
