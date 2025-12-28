#![allow(dead_code)]

pub mod bulk_insert;
pub mod model;
pub mod pool;
pub mod t38;
pub mod transmitter;

use chrono::{DateTime, Utc};
use geojson::Geometry;
use h3o::{CellIndex, geom::dissolve};
use serde::{Deserialize, Serialize};
use tokio_pg_mapper::FromTokioPostgresRow;
use tokio_pg_mapper_derive::PostgresMapper;
use tokio_postgres::{Client, GenericClient, Transaction};

use crate::{
    CONFIG, db::transmitter::TransmitterLocation,
    services::submission::geosubmit::Report as SubReport,
};

#[derive(Serialize, Deserialize, PostgresMapper)]
#[pg_mapper(table = "report")]
pub struct Report {
    pub id: i64,
    pub raw: Vec<u8>,
    pub user_agent: Option<String>,
}

pub async fn create_partitions(pool_tp: deadpool_postgres::Pool) -> Result<(), anyhow::Error> {
    let date_now = Utc::now().format("%Y-%m-%d");
    let query = format!(
        "SELECT create_daily_partitions('report', date '{}', 35);",
        date_now.to_string()
    );

    let mapper = pool_tp.get().await?;
    let client = mapper.client();

    let statement = client.prepare(&query).await?;
    client.execute(&statement, &[]).await?;
    Ok(())
}

pub async fn remove_partitions(pool_tp: deadpool_postgres::Pool) -> Result<(), anyhow::Error> {
    let query = format!(
        "select drop_old_partitions('report', {}, false);",
        CONFIG.database.report_keep_days,
    );

    let mapper = pool_tp.get().await?;
    let client = mapper.client();

    let statement = client.prepare(&query).await?;
    client.execute(&statement, &[]).await?;
    Ok(())
}

pub async fn get_reports(
    transaction: &Transaction<'_>,
) -> Result<Vec<Report>, tokio_postgres::Error> {
    // FOR UPDATE - exclusive lock
    // https://dev.to/markadel/postgresql-isolation-levels-and-locking-summary-9ac

    let partition_query = if CONFIG.database.report_number_days_search <= 1 {
        "SELECT
            id,
            raw,
            user_agent
        FROM report
        WHERE
            submitted_at >= NOW() - interval '1 day' AND
            processed_at IS NULL
        ORDER BY id LIMIT 10000
        FOR UPDATE"
            .to_string()
    } else {
        format!(
            "SELECT
                id,
                raw,
                user_agent
            FROM report
            WHERE
                submitted_at >= NOW() - interval '{} days' AND
                processed_at IS NULL
            ORDER BY id LIMIT 10000
            FOR UPDATE",
            CONFIG.database.report_number_days_search,
        )
    };
    let statement = transaction.prepare(&partition_query).await?;

    let reports = transaction
        .query(&statement, &[])
        .await?
        .iter()
        .map(|row| Report::from_row_ref(row).unwrap())
        .collect::<Vec<Report>>();

    Ok(reports)
}

pub async fn get_reports_all(client: &Client) -> Result<Vec<Report>, anyhow::Error> {
    let statement = client
        .prepare("SELECT id, user_agent, raw FROM report")
        .await?;

    let reports = client
        .query(&statement, &[])
        .await?
        .iter()
        .map(|row| Report::from_row_ref(row).unwrap())
        .collect::<Vec<Report>>();

    Ok(reports)
}

pub async fn get_report_one(client: &Client, id: i64) -> Result<Vec<Report>, anyhow::Error> {
    let statement = client
        .prepare("SELECT id, user_agent, raw FROM report WHERE id = $1")
        .await?;

    let reports = client
        .query(&statement, &[&id])
        .await?
        .iter()
        .map(|row| Report::from_row_ref(row).unwrap())
        .collect::<Vec<Report>>();

    Ok(reports)
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
        updated if updated == 1 => Ok(true),
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
        .execute(&statement, &[&report_id, &format!("{err}")])
        .await?;

    match result {
        updated if updated == 1 => Ok(true),
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
        updated if updated == 1 => Ok(true),
        _ => Ok(false),
    }
}

pub async fn insert_wifi(
    client: &Client,
    mac: String,
    tl: TransmitterLocation,
    ms: Vec<u8>,
) -> Result<bool, anyhow::Error> {
    let statement = client
        .prepare("
            INSERT INTO wifi (mac, min_lat, min_lon, max_lat, max_lon, lat, lon, accuracy, total_weight, min_strength, max_strength, measurements)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (mac)
            DO UPDATE SET min_lat = EXCLUDED.min_lat, min_lon = EXCLUDED.min_lon, max_lat = EXCLUDED.max_lat, max_lon = EXCLUDED.max_lon,
            lat = EXCLUDED.lat, lon = EXCLUDED.lon, accuracy = EXCLUDED.accuracy, total_weight = EXCLUDED.total_weight, min_strength = EXCLUDED.min_strength, max_strength = EXCLUDED.max_strength, measurements = EXCLUDED.measurements
            ",
        )
        .await?;

    let result = client
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
                &ms,
            ],
        )
        .await?;

    match result {
        updated if updated == 1 => Ok(true),
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
        updated if updated == 1 => Ok(true),
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
        updated if updated == 1 => Ok(true),
        _ => Ok(false),
    }
}

#[allow(unused)]
pub async fn get_cell(
    client: &Client,
    radio: i16,
    country: i16,
    network: i16,
    area: i32,
    cell: i64,
    unit: i16,
) -> Option<TransmitterLocation> {
    let statement = client
        .prepare(
            "
                SELECT
                    min_lat,
                    min_lon,
                    max_lat,
                    max_lon,
                    lat,
                    lon,
                    accuracy,
                    total_weight,
                    min_strength,
                    max_strength,
                    measurements
                FROM cell
                WHERE radio = $1 AND
                    country = $2 AND
                    network = $3 AND
                    area = $4 AND
                    cell = $5 AND
                    unit = $6",
        )
        .await
        .unwrap();

    let cell = client
        .query_opt(
            &statement,
            &[&radio, &country, &network, &area, &cell, &unit],
        )
        .await
        .unwrap()
        .map(|row| TransmitterLocation::from_row_ref(&row).unwrap());

    return cell;
}

pub async fn get_wifi_one(client: &Client, mac: String) -> Option<TransmitterLocation> {
    let statement = client
        .prepare(
            "
            SELECT
                mac,
                min_lat,
                min_lon,
                max_lat,
                max_lon,
                lat,
                lon,
                accuracy,
                total_weight,
                min_strength,
                max_strength,
                measurements
            FROM wifi
            WHERE mac = $1",
        )
        .await
        .unwrap();

    let wifi = client
        .query_opt(&statement, &[&mac])
        .await
        .unwrap()
        .map(|row| TransmitterLocation::from_row_ref(&row).unwrap());

    return wifi;
}

pub async fn get_wifi_many(
    client: &Client,
    macs: Vec<&str>,
) -> Result<Vec<Option<TransmitterLocation>>, anyhow::Error> {
    let mac_list = macs
        .iter()
        .map(|m| format!(r#""{m}""#))
        .collect::<Vec<String>>()
        .join(",");

    let query = format!(
        "SELECT
            mac,
            min_lat,
            min_lon,
            max_lat,
            max_lon,
            lat,
            lon,
            accuracy,
            total_weight,
            min_strength,
            max_strength,
            measurements
        FROM wifi
        WHERE mac = ANY('{{{}}}')",
        mac_list,
    );

    let statement = client.prepare(&query).await.unwrap();

    let wifis = client
        .query(&statement, &[])
        .await?
        .iter()
        .map(|row| TransmitterLocation::from_row_ref(row).ok())
        .collect::<Vec<Option<TransmitterLocation>>>();

    return Ok(wifis);
}

pub async fn get_bluetooth(client: &Client, mac: String) -> Option<TransmitterLocation> {
    let statement = client
        .prepare(
            "
            SELECT
                mac,
                min_lat,
                min_lon,
                max_lat,
                max_lon,
                lat,
                lon,
                accuracy,
                total_weight,
                min_strength,
                max_strength,
                measurements
            FROM bluetooth
            WHERE mac = $1",
        )
        .await
        .unwrap();

    let bluetooth = client
        .query_opt(&statement, &[&mac])
        .await
        .unwrap()
        .map(|row| TransmitterLocation::from_row_ref(&row).unwrap());

    return bluetooth;
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
        updated if updated == 1 => Ok(true),
        _ => Ok(false),
    }
}

pub async fn get_map(client: &Client) -> Result<(), anyhow::Error> {
    let statement = client.prepare("SELECT h3 FROM map").await?;
    let h3s = client
        .query(&statement, &[])
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect::<Vec<Vec<u8>>>();

    let mut cells = Vec::with_capacity(h3s.len());
    for h3_binary in h3s {
        assert_eq!(h3_binary.len(), 8);
        let x: [u8; 8] = h3_binary.try_into().unwrap();
        let x = u64::from_be_bytes(x);
        let x = CellIndex::try_from(x)?;
        cells.push(x);
    }

    let poly = dissolve(cells)?;
    let geom = Geometry::new((&poly).into());
    println!("{}", geom.to_string());

    Ok(())
}
