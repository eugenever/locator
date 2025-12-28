//! Models and functionality to work with the config file.
#![allow(dead_code)]

use std::{fs, path::Path};

use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use serde::Deserialize;

pub static CONFIG: Lazy<Config> = Lazy::new(|| {
    let path = Path::new("config.toml");
    let config = load_config(path);
    match config {
        Ok(c) => c,
        Err(err) => panic!("{:?}", err),
    }
});

/// Rust representation of the configuration
#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    /// Http server settings
    pub server: Server,
    /// PostgreSQL database settings
    pub database: Database,
    /// ThreadPool settings
    pub threadpool: Threadpool,
    /// Locator service settings
    pub locator: Locator,
    /// Yandex LBS
    #[serde(rename = "yandex-lbs")]
    pub yandex_lbs: YandexLBS,
    /// AlterGeo LBS
    #[serde(rename = "altergeo-lbs")]
    pub altergeo_lbs: AlterGeoLBS,
    /// Tile38 settings
    pub t38: T38,
    /// GraphHopper settings
    pub graphhopper: GraphHopper,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Locator {
    /// queue size for saving reports to the database
    pub report_queue_size: usize,
    /// number of tasks processing reports
    pub tasks_processing_reports_count: usize,
    /// real-time report processing
    pub process_report_online: bool,
    /// resolution of the h3 hexagons in the data map preview
    pub h3_resolution: u8,
    /// maximum Wi-Fi range
    pub radius_wifi_detection: f64,
    /// maximum distance between points in a cluster
    pub max_distance_in_cluster: f64,
    /// maximum distance to the cell
    pub max_distance_cell: f64,
    /// filter Locally Administered Addresses
    pub laa_filter: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Database {
    // maximum number of connections to the database
    pub max_connections_db: usize,
    /// frequency of processing reports from the database
    pub report_processing_frequency: u32,
    /// number of days to search for reports
    pub report_number_days_search: u16,
    /// How many days should reports be kept
    pub report_keep_days: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Threadpool {
    /// number of threads that stay alive for the entire lifetime of the ThreadPool
    pub core_size: usize,
    /// numbers represents the maximum number of threads that may be alive at the same time within this pool
    pub max_size: usize,
    /// duration for which additional threads outside the core pool remain alive while not receiving
    /// any work before giving up and terminating
    pub keep_alive: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Server {
    /// Port on which Locator listens
    pub http_port: u16,
    // Number of http workers
    pub num_http_workers: usize,
    /// maximum payload size
    pub max_payload_mb: usize,
    /// application logging level
    pub log_level: String,
    /// secret token for HTTP requests
    pub creditional_tokens: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct YandexLBS {
    pub enabled: bool,
    pub url: String,
    pub api_keys: Vec<String>,
    pub rate_limit: usize,
    pub max_distance_in_cluster: f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AlterGeoLBS {
    pub enabled: bool,
    pub url: String,
    pub apikey: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct T38 {
    pub instances: Option<Vec<T38Instance>>,
    pub sentinel: Option<Vec<T38Sentinel>>,
    pub gc_frequency: Option<u32>,
    pub aofshrink_frequency: Option<u32>,
    pub healthz_frequency: Option<u32>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct T38Instance {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct T38Sentinel {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GraphHopper {
    pub host: String,
    pub port: u16,
    pub rate_limit_matching: usize,
    pub admin: GraphHopperAdmin,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GraphHopperAdmin {
    pub host: String,
    pub port: u16,
    pub gc_frequency: Option<u32>,
}

pub fn load_config(path: &Path) -> Result<Config> {
    let data = fs::read_to_string(path).context("Failed to read config")?;
    let config = toml::from_str(&data).context("Failed to parse config")?;
    Ok(config)
}
