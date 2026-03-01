#![allow(unused)]

pub mod bulk_insert;
pub mod pool;
pub mod transmitter;

use anyhow::anyhow;
use chrono::{DateTime, Datelike, Duration, TimeZone, Utc};
use h3o::CellIndex;
use log::{error, warn};
use serde::{Deserialize, Serialize};
use tokio_pg_mapper::FromTokioPostgresRow;
use tokio_pg_mapper_derive::PostgresMapper;
use tokio_postgres::{Client, GenericClient, Row, Transaction};

use crate::{
    CONFIG,
    constants::PRESAVED_PARTITIONS_COUNT,
    db::pg::transmitter::TransmitterLocation,
    services::submission::{geosubmit::Report as SubReport, report::GeoFence},
};

#[derive(Serialize, Deserialize, PostgresMapper)]
#[pg_mapper(table = "report")]
pub struct Report {
    pub id: i64,
    pub raw: Vec<u8>,
    pub user_agent: Option<String>,
}

/*
    1) To transfer partitions to a slow HDD, first create a tablespace:

    https://stackoverflow.com/questions/49027330/postgresql-cannot-create-tablespace-due-to-permissions
        chown postgres:postgres folder_for_tablespace/
        chmod 770 folder_for_tablespace/

    CREATE TABLESPACE slow_storage LOCATION '/mnt/data/Work/pg_slow_storage'
        WITH (seq_page_cost = 5.0, random_page_cost = 10.0);

    2) detach a partition from the parent table and move it to the desired tablespace:

    ALTER TABLE report DETACH PARTITION report_2025_10_18;
    ALTER TABLE report_2025_10_18 SET TABLESPACE slow_storage;

    3) Optional remove tablespace:

    DROP TABLESPACE IF EXISTS slow_storage;
*/

#[derive(Serialize, Deserialize, PostgresMapper)]
#[pg_mapper(table = "pg_stat_user_tables")]
pub struct AttachedPartition {
    pub parent_schema: String,
    pub parent: String,
    pub child_schema: String,
    pub child: String,
}

impl AttachedPartition {
    pub fn date(&self) -> Result<DateTime<Utc>, anyhow::Error> {
        let elements: Vec<&str> = self.child.split("_").collect();
        if elements.len() != 4 {
            Err(anyhow!("parse child AttachedPartition: {}", self.child))
        } else {
            let mut date_components = elements.into_iter().skip(1);
            if let (Some(year), Some(month), Some(day)) = (
                date_components.next().and_then(|y| y.parse::<i32>().ok()),
                date_components.next().and_then(|m| m.parse::<u32>().ok()),
                date_components.next().and_then(|d| d.parse::<u32>().ok()),
            ) {
                Ok(Utc.with_ymd_and_hms(year, month, day, 0, 0, 0).unwrap())
            } else {
                Err(anyhow!(
                    "parse date components of Partition: {:?}",
                    date_components
                ))
            }
        }
    }
}

#[derive(Serialize, Deserialize, PostgresMapper)]
#[pg_mapper(table = "pg_stat_user_tables")]
pub struct Partition {
    #[serde(rename = "schemaname")]
    pub schema_name: String,
    #[serde(rename = "relname")]
    pub rel_name: String,
    #[serde(rename = "n_live_tup")]
    pub rows_number: i64,
}

impl Partition {
    pub fn date(&self) -> Result<DateTime<Utc>, anyhow::Error> {
        let elements: Vec<&str> = self.rel_name.split("_").collect();
        if elements.len() != 4 {
            Err(anyhow!("parse child AttachedPartition: {}", self.rel_name))
        } else {
            let mut date_components = elements.into_iter().skip(1);
            if let (Some(year), Some(month), Some(day)) = (
                date_components.next().and_then(|y| y.parse::<i32>().ok()),
                date_components.next().and_then(|m| m.parse::<u32>().ok()),
                date_components.next().and_then(|d| d.parse::<u32>().ok()),
            ) {
                Ok(Utc.with_ymd_and_hms(year, month, day, 0, 0, 0).unwrap())
            } else {
                Err(anyhow!(
                    "parse date components of Partition: {:?}",
                    date_components
                ))
            }
        }
    }

    pub fn parent_table(&self) -> Result<String, anyhow::Error> {
        let elements: Vec<&str> = self.rel_name.split("_").collect();
        if elements.len() != 4 {
            Err(anyhow!("parse relname Partition: {}", self.rel_name))
        } else {
            let table = elements.first().map(|&t| t.to_string());
            if let Some(t) = table {
                Ok(t)
            } else {
                Err(anyhow!(
                    "parent table of Partition is undefined: {:?}",
                    elements
                ))
            }
        }
    }
}

/*
    return all partitions, including those detached from the parent table:

    SELECT
        schema_name,
        rel_name,
        n_live_tup
    FROM pg_stat_user_tables
    WHERE relname like 'report_%'
    ORDER BY relname DESC;


    return partitions attached to the parent table:

    SELECT
        nmsp_parent.nspname AS parent_schema,
        parent.relname      AS parent,
        nmsp_child.nspname  AS child_schema,
        child.relname       AS child
    FROM pg_inherits
        JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
        JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
        JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
    WHERE parent.relname='report';

*/
pub async fn get_report_partitions(
    transaction: &Transaction<'_>,
) -> Result<Vec<Partition>, tokio_postgres::Error> {
    let query = "
        SELECT
            schemaname,
            relname,
            n_live_tup
        FROM pg_stat_user_tables
        WHERE relname like 'report_%'
        ORDER BY relname DESC;
    ";
    let statement = transaction.prepare(&query).await?;

    let partitions = transaction
        .query(&statement, &[])
        .await?
        .iter()
        .map(|row| Partition::from_row_ref(row).unwrap())
        .collect::<Vec<Partition>>();

    Ok(partitions)
}

pub async fn get_report_attached_partitions(
    transaction: &Transaction<'_>,
) -> Result<Vec<AttachedPartition>, tokio_postgres::Error> {
    let query = "
        SELECT
            nmsp_parent.nspname AS parent_schema,
            parent.relname      AS parent,
            nmsp_child.nspname  AS child_schema,
            child.relname       AS child
        FROM pg_inherits
            JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
            JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
            JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
        WHERE parent.relname='report';
    ";
    let statement = transaction.prepare(&query).await?;

    let attached_partitions = transaction
        .query(&statement, &[])
        .await?
        .iter()
        .map(|row| AttachedPartition::from_row_ref(row).unwrap())
        .collect::<Vec<AttachedPartition>>();

    Ok(attached_partitions)
}

pub async fn create_partitions(pool_tp: deadpool_postgres::Pool) -> Result<(), anyhow::Error> {
    let date_now = Utc::now().format("%Y-%m-%d");
    let query = format!(
        "SELECT create_daily_partitions('report', date '{}', 35);",
        date_now
    );
    let manager = pool_tp.get().await?;
    let client = manager.client();
    let statement = client.prepare(&query).await?;
    client.execute(&statement, &[]).await?;
    Ok(())
}

pub async fn remove_partitions(pool_tp: deadpool_postgres::Pool) -> Result<(), anyhow::Error> {
    let query = format!(
        "SELECT drop_old_partitions('report', {}, false);",
        CONFIG.database.report_keep_days,
    );
    let manager = pool_tp.get().await?;
    let client = manager.client();
    let statement = client.prepare(&query).await?;
    client.execute(&statement, &[]).await?;
    Ok(())
}

pub async fn detach_partition(
    transaction: &Transaction<'_>,
    partition: &str,
) -> Result<(), anyhow::Error> {
    let query = format!("ALTER TABLE report DETACH PARTITION {};", partition);
    let statement = transaction.prepare(&query).await?;
    transaction.execute(&statement, &[]).await?;
    Ok(())
}

pub async fn get_reports_by_range_id(
    client: &Client,
    ids: &[u64],
) -> Result<Vec<Report>, tokio_postgres::Error> {
    if ids.len() != 2 {
        error!("ids array length must be 2");
        return Ok(vec![]);
    }
    if ids[0] > ids[1] {
        error!("min ID report must be less than max ID");
        return Ok(vec![]);
    }
    let query = format!(
        "SELECT
            id,
            raw,
            user_agent
        FROM report
        WHERE
            id >= {} AND id <= {}
        ",
        ids[0], ids[1]
    );
    let statement = client.prepare(&query).await?;

    let reports = client
        .query(&statement, &[])
        .await?
        .iter()
        .map(|row| Report::from_row_ref(row).unwrap())
        .collect::<Vec<Report>>();

    Ok(reports)
}

pub async fn get_required_reports(
    client: &Client,
    ids: &[u64],
) -> Result<Vec<Report>, tokio_postgres::Error> {
    let array_ids = ids.iter().map(ToString::to_string).collect::<Vec<String>>();
    let query = format!(
        "SELECT
            id,
            raw,
            user_agent
        FROM report
        WHERE
            id = ANY ('{{{}}}'::integer[])
        ",
        array_ids.join(","),
    );
    let statement = client.prepare(&query).await?;

    let reports = client
        .query(&statement, &[])
        .await?
        .iter()
        .map(|row| Report::from_row_ref(row).unwrap())
        .collect::<Vec<Report>>();

    Ok(reports)
}

pub async fn get_reports(
    transaction: &Transaction<'_>,
    geo_fence: Option<GeoFence>,
) -> Result<Vec<Report>, tokio_postgres::Error> {
    // FOR UPDATE - exclusive lock
    // https://dev.to/markadel/postgresql-isolation-levels-and-locking-summary-9ac

    if CONFIG.database.report_processing_by_partitions {
        match get_report_attached_partitions(transaction).await {
            Err(e) => {
                return Err(e);
            }
            Ok(attached_partitions) => {
                let now = Utc::now();
                let psp = presaved_partitions(PRESAVED_PARTITIONS_COUNT, &now);

                for ap in attached_partitions {
                    // future partitions cannot be processed
                    if let Ok(date) = ap.date() {
                        if date > now {
                            continue;
                        }
                    } else {
                        continue;
                    }

                    let query = query_reports_by_partition(&ap.child, geo_fence.as_ref());
                    let statement = transaction.prepare(&query).await?;

                    let reports = transaction
                        .query(&statement, &[])
                        .await?
                        .iter()
                        .map(|row| Report::from_row_ref(row).unwrap())
                        .collect::<Vec<Report>>();

                    if !reports.is_empty() {
                        // DEBUG
                        // TODO: remove after tests by Whoosh
                        warn!(
                            "processing partition: {}, geofence: {}",
                            ap.child,
                            geo_fence.unwrap_or_default()
                        );

                        return Ok(reports);
                    } else {
                        // detach a child partition only if it has no unprocessed reports and no geofence is specified
                        if !psp.contains(&ap.child) && geo_fence.is_none() {
                            if let Ok(date) = ap.date() {
                                // future partitions cannot be detached
                                if date < now {
                                    if let Err(e) = detach_partition(transaction, &ap.child).await {
                                        error!("detach partition {}: {}", ap.child, e);
                                    } else {
                                        warn!("success detach partition: {}", ap.child);
                                    }
                                }
                            }
                        }
                    }
                }
                // fallback
                return Ok(vec![]);
            }
        }
    }

    let query = query_reports(geo_fence.as_ref());
    let statement = transaction.prepare(&query).await?;

    let reports = transaction
        .query(&statement, &[])
        .await?
        .iter()
        .map(|row| Report::from_row_ref(row).unwrap())
        .collect::<Vec<Report>>();

    Ok(reports)
}

fn presaved_partitions(n: u16, now: &DateTime<Utc>) -> Vec<String> {
    let current_year = now.year();
    let current_month = now.month();
    let current_day = now.day();

    let mut sp = Vec::with_capacity(n as usize + 1);
    sp.push(format!(
        "report_{}_{:02}_{:02}",
        current_year, current_month, current_day
    ));

    if n > 0 {
        (0..n).for_each(|c| {
            let prev_day = *now - Duration::days((c + 1) as i64);
            let prev_year = prev_day.year();
            let prev_month = prev_day.month();
            let prev_day = prev_day.day();
            sp.push(format!(
                "report_{}_{:02}_{:02}",
                prev_year, prev_month, prev_day
            ));
        });
    }

    sp
}

fn query_reports(geo_fence: Option<&GeoFence>) -> String {
    let interval = if CONFIG.database.report_number_days_search <= 1 {
        "1 day".to_string()
    } else {
        format!("{} days", CONFIG.database.report_number_days_search)
    };

    if let Some(gf) = geo_fence
        && gf.validate()
    {
        format!(
            "SELECT
                id,
                raw,
                user_agent
            FROM report
            WHERE
                submitted_at >= NOW() - interval '{}' AND
                submitted_at <= NOW() AND
                latitude >= {} AND
                latitude <= {} AND
                longitude >= {} AND
                longitude <= {} AND
                processed_at IS NULL
            LIMIT {}
            FOR UPDATE",
            interval,
            gf.lat_min,
            gf.lat_max,
            gf.lon_min,
            gf.lon_max,
            CONFIG.database.number_processed_reports,
        )
    } else {
        format!(
            "SELECT
                id,
                raw,
                user_agent
            FROM report
            WHERE
                submitted_at >= NOW() - interval '{}' AND
                submitted_at <= NOW() AND
                processed_at IS NULL
            LIMIT {}
            FOR UPDATE",
            interval, CONFIG.database.number_processed_reports,
        )
    }
}

fn query_reports_by_partition(partition: &str, geo_fence: Option<&GeoFence>) -> String {
    if let Some(gf) = geo_fence
        && gf.validate()
    {
        format!(
            "SELECT
                id,
                raw,
                user_agent
            FROM {}
            WHERE
                latitude >= {} AND
                latitude <= {} AND
                longitude >= {} AND
                longitude <= {} AND
                processed_at IS NULL
            LIMIT {}
            FOR UPDATE",
            partition,
            gf.lat_min,
            gf.lat_max,
            gf.lon_min,
            gf.lon_max,
            CONFIG.database.number_processed_reports,
        )
    } else {
        format!(
            "SELECT
                id,
                raw,
                user_agent
            FROM {}
            WHERE
                processed_at IS NULL
            LIMIT {}
            FOR UPDATE",
            partition, CONFIG.database.number_processed_reports,
        )
    }
}

fn query_reports_by_partition_and_range_id(
    partition: &str,
    geo_fence: Option<&GeoFence>,
    range_id: RangeId,
) -> String {
    if let Some(gf) = geo_fence
        && gf.validate()
    {
        format!(
            "SELECT
                id,
                raw,
                user_agent
            FROM {}
            WHERE
                id >= {} AND
                id <= {} AND
                latitude >= {} AND
                latitude <= {} AND
                longitude >= {} AND
                longitude <= {} AND
                processed_at IS NULL
            LIMIT {}
            FOR UPDATE",
            partition,
            range_id.min_id,
            range_id.max_id,
            gf.lat_min,
            gf.lat_max,
            gf.lon_min,
            gf.lon_max,
            CONFIG.database.number_processed_reports,
        )
    } else {
        format!(
            "SELECT
                id,
                raw,
                user_agent
            FROM {}
            WHERE
                id >= {} AND
                id <= {} AND
                processed_at IS NULL
            LIMIT {}
            FOR UPDATE",
            partition, range_id.min_id, range_id.max_id, CONFIG.database.number_processed_reports,
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RangeId {
    pub min_id: i64,
    pub max_id: i64,
}

impl From<Row> for RangeId {
    fn from(row: Row) -> Self {
        Self {
            min_id: row.get("min_id"),
            max_id: row.get("max_id"),
        }
    }
}

pub async fn get_range_id_for_report(
    pool_tp: deadpool_postgres::Pool,
    partition: &str,
) -> Result<Vec<RangeId>, anyhow::Error> {
    let query = format!(
        "
        SELECT
            MIN(id) AS min_id,
            MAX(id) AS max_id
        FROM {}
        ",
        partition,
    );

    let manager = pool_tp.get().await?;
    let client = manager.client();
    let statement = client.prepare(&query).await?;
    let range = client
        .query(&statement, &[])
        .await?
        .into_iter()
        .map(|row| RangeId::from(row))
        .collect::<Vec<RangeId>>();

    Ok(range)
}

pub async fn update_report(
    transaction: &Transaction<'_>,
    report_id: i64,
) -> Result<bool, anyhow::Error> {
    let statement = transaction
        .prepare(
            "
                UPDATE report
                SET processed_at = now()
                WHERE id = $1",
        )
        .await?;

    let result = transaction.execute(&statement, &[&report_id]).await?;
    match result {
        1 => Ok(true),
        _ => Ok(false),
    }
}

pub async fn update_report_error(
    transaction: &Transaction<'_>,
    report_id: i64,
    err: String,
) -> Result<bool, anyhow::Error> {
    let statement = transaction
        .prepare(
            "
                UPDATE report
                SET processing_error = $1
                WHERE id = $2",
        )
        .await?;

    let result = transaction
        .execute(&statement, &[&report_id, &err.to_string()])
        .await?;

    match result {
        1 => Ok(true),
        _ => Ok(false),
    }
}

pub async fn insert_cell(
    transaction: &Transaction<'_>,
    radio: i16,
    country: i16,
    network: i16,
    area: i32,
    cell: i64,
    unit: i16,
    tl: TransmitterLocation,
) -> Result<bool, anyhow::Error> {
    let statement = transaction
        .prepare("
            INSERT INTO cell (radio, country, network, area, cell, unit, min_lat, min_lon, max_lat, max_lon, lat, lon, accuracy, total_weight, min_strength, max_strength)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (radio, country, network, area, cell, unit)
            DO UPDATE SET min_lat = EXCLUDED.min_lat, min_lon = EXCLUDED.min_lon, max_lat = EXCLUDED.max_lat, max_lon = EXCLUDED.max_lon, 
            lat = EXCLUDED.lat, lon = EXCLUDED.lon, accuracy = EXCLUDED.accuracy, total_weight = EXCLUDED.total_weight, min_strength = EXCLUDED.min_strength, max_strength = EXCLUDED.max_strength
            ",
        )
        .await?;

    let result = transaction
        .execute(
            &statement,
            &[
                &radio,
                &country,
                &network,
                &area,
                &cell,
                &unit,
                &tl.min_lat,
                &tl.min_lon,
                &tl.max_lat,
                &tl.max_lon,
                &tl.lat,
                &tl.lon,
                &tl.accuracy,
                &tl.total_weight,
                &tl.min_strength,
                &tl.max_strength,
            ],
        )
        .await?;

    match result {
        1 => Ok(true),
        _ => Ok(false),
    }
}

pub async fn insert_bluetooth(
    transaction: &Transaction<'_>,
    mac: String,
    tl: TransmitterLocation,
) -> Result<bool, anyhow::Error> {
    let statement = transaction
        .prepare("
            INSERT INTO bluetooth (mac, min_lat, min_lon, max_lat, max_lon, lat, lon, accuracy, total_weight, min_strength, max_strength)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (mac)
            DO UPDATE SET min_lat = EXCLUDED.min_lat, min_lon = EXCLUDED.min_lon, max_lat = EXCLUDED.max_lat, max_lon = EXCLUDED.max_lon,
            lat = EXCLUDED.lat, lon = EXCLUDED.lon, accuracy = EXCLUDED.accuracy, total_weight = EXCLUDED.total_weight, min_strength = EXCLUDED.min_strength, max_strength = EXCLUDED.max_strength",
        )
        .await?;

    let result = transaction
        .execute(
            &statement,
            &[
                &mac.to_string(),
                &tl.min_lat,
                &tl.min_lon,
                &tl.max_lat,
                &tl.max_lon,
                &tl.lat,
                &tl.lon,
                &tl.accuracy,
                &tl.total_weight,
                &tl.min_strength,
                &tl.max_strength,
            ],
        )
        .await?;

    match result {
        1 => Ok(true),
        _ => Ok(false),
    }
}

pub async fn insert_h3(
    transaction: &Transaction<'_>,
    h3: CellIndex,
) -> Result<bool, anyhow::Error> {
    let statement = transaction
        .prepare("INSERT INTO map (h3) values ($1) ON CONFLICT (h3) DO NOTHING")
        .await?;

    let h3_binary = u64::from(h3).to_be_bytes();
    let h3_vec = h3_binary.to_vec();
    let result = transaction.execute(&statement, &[&h3_vec]).await?;

    match result {
        1 => Ok(true),
        _ => Ok(false),
    }
}

pub async fn insert_report(
    client: &Client,
    report: &SubReport,
    user_agent: Option<String>,
    ts_now: Option<DateTime<Utc>>,
) -> Result<bool, anyhow::Error> {
    let extra = serde_json::to_vec(&report)?;

    let result = if let Some(processed_at) = ts_now {
        let statement = client
            .prepare(
                "
            INSERT INTO report (timestamp, processed_at, latitude, longitude, user_agent, raw)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT DO NOTHING",
            )
            .await?;

        client
            .execute(
                &statement,
                &[
                    &report.timestamp,
                    &processed_at,
                    &report.position.latitude,
                    &report.position.longitude,
                    &user_agent,
                    &extra,
                ],
            )
            .await?
    } else {
        let statement = client
            .prepare(
                "
            INSERT INTO report (timestamp, latitude, longitude, user_agent, raw)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT DO NOTHING",
            )
            .await?;

        client
            .execute(
                &statement,
                &[
                    &report.timestamp,
                    &report.position.latitude,
                    &report.position.longitude,
                    &user_agent,
                    &extra,
                ],
            )
            .await?
    };

    match result {
        1 => Ok(true),
        _ => Ok(false),
    }
}
