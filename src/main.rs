mod config;
mod constants;
mod db;
mod error;
mod lbs;
mod services;
mod tasks;
mod threadpool;

use std::path::PathBuf;
use std::process::exit;

use actix_web::{App, HttpServer, dev::ServerHandle, middleware::Logger, web};
use actix_web_httpauth::middleware::HttpAuthentication;
use anyhow::Result;
use clap::{Parser, Subcommand};
use log::{error, info};
use tokio::task::JoinHandle;

use crate::{
    config::CONFIG,
    services::{crate_rate_limiters_app, validator},
    tasks::{
        report::MessageSaveReport,
        t38::{self, T38ConnectionManageMessage},
        yandex::YandexApiMessage,
    },
};

/// Command line interface parser.
#[derive(Debug, Parser)]
struct Cli {
    #[arg(short, long)]
    config: Option<PathBuf>,

    #[clap(subcommand)]
    command: Command,
}

/// Subcommands of the cli parser
#[derive(Debug, Subcommand)]
enum Command {
    /// Serve the Locator geolocate service
    Serve,
    /// Process newly submitted reports
    Process,
    /// Export a map of all data as h3 hexagons
    Map,
    /// Archive reports out of the database
    Archive {
        #[clap(subcommand)]
        command: services::archive::ArchiveCommand,
    },
    /// Reformat data to the MLS format
    FormatMls,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    env_logger::init_from_env(env_logger::Env::new().default_filter_or(&CONFIG.server.log_level));

    if CONFIG.t38.instances.is_some() && CONFIG.t38.sentinel.is_some() {
        error!(
            "Tile38 config: manual INSTANCES detection in conjunction with SENTINEL is not allowed"
        );
        exit(1);
    }
    if CONFIG.t38.instances.is_none() && CONFIG.t38.sentinel.is_none() {
        error!("Tile38 config: necessary to determine the INSTANCES or SENTINEL");
        exit(1);
    }

    let pool_tp = db::pool::create_pool_tp(&CONFIG)?;
    let _pool_tokio_task = services::helper::pool_task::Pool::bounded(16);

    let thread_pool_name = "locator".to_string();
    let thread_pool = threadpool::create_threadpool(
        CONFIG.threadpool.core_size,
        CONFIG.threadpool.max_size,
        thread_pool_name,
        CONFIG.threadpool.keep_alive,
    )?;

    let yandex_client = reqwest::Client::new();
    let gh_client = reqwest::Client::new();

    let (tx_yandex_api, rx_yandex_api) = flume::unbounded::<YandexApiMessage>();
    let _yandex_api_handle =
        tasks::yandex::yandex_api_task(rx_yandex_api, tx_yandex_api.clone(), yandex_client.clone());

    let rl_app = crate_rate_limiters_app();

    match cli.command {
        Command::Serve => {
            let (tx_t38_conn, rx_t38_conn) = flume::unbounded::<T38ConnectionManageMessage>();
            let _connection_manage_t38_handle =
                t38::connection_manage_task(&CONFIG, rx_t38_conn, tx_t38_conn.clone()).await?;

            db::create_partitions(pool_tp.clone()).await?;

            let mut tx_report_opt = None;
            let save_report_handle_opt = None;

            if !CONFIG.locator.process_report_online {
                let _process_reports_handle = tasks::report::process_reports_task(
                    pool_tp.clone(),
                    tx_t38_conn.clone(),
                    yandex_client.clone(),
                    tx_yandex_api.clone(),
                    rl_app.clone(),
                );
            } else {
                let (tx_report, rx_report) =
                    flume::bounded::<MessageSaveReport>(CONFIG.locator.report_queue_size);
                let (tx_save_report, _rx_save_report) =
                    flume::bounded::<MessageSaveReport>(CONFIG.locator.report_queue_size);
                let mut _online_process_report_handles =
                    Vec::with_capacity(CONFIG.locator.tasks_processing_reports_count);
                for _ in 0..CONFIG.locator.tasks_processing_reports_count {
                    let online_process_report_handle = tasks::report::online_process_report_task(
                        tx_t38_conn.clone(),
                        yandex_client.clone(),
                        tx_yandex_api.clone(),
                        rl_app.clone(),
                        rx_report.clone(),
                        tx_save_report.clone(),
                    );
                    _online_process_report_handles.push(online_process_report_handle);
                }
                tx_report_opt = Some(tx_report);

                /*
                    let save_report_handle = tasks::report::save_report_task(
                        pool_tp.clone(),
                        _rx_save_report,
                        pool_tokio_task,
                    );
                    save_report_handle_opt = Some(save_report_handle);
                */
            }

            let _process_reports_partitions_handle =
                tasks::report::process_report_partitions_task(pool_tp.clone());

            let mut _gc_t38_handle: Option<JoinHandle<()>> = None;
            if let Some(gc_frequency) = CONFIG.t38.gc_frequency {
                _gc_t38_handle = Some(tasks::t38::gc_task(tx_t38_conn.clone(), gc_frequency));
            }

            let mut _aofshrink_handle: Option<JoinHandle<()>> = None;
            if let Some(aofshrink_frequency) = CONFIG.t38.aofshrink_frequency {
                _aofshrink_handle = Some(tasks::t38::aofshrink_task(
                    tx_t38_conn.clone(),
                    aofshrink_frequency,
                ));
            }

            let mut _healthz_handle: Option<JoinHandle<()>> = None;
            if let Some(healthz_frequency) = CONFIG.t38.healthz_frequency {
                _healthz_handle = Some(tasks::t38::healthz_task(
                    tx_t38_conn.clone(),
                    healthz_frequency,
                ));
            }

            let mut _gc_gh_handle: Option<JoinHandle<()>> = None;
            if let Some(gc_frequency) = CONFIG.graphhopper.admin.gc_frequency {
                _gc_gh_handle = Some(tasks::graphhopper::gc_task(gc_frequency, gh_client));
            }

            info!(
                "Locator server started at 127.0.0.1:{}",
                CONFIG.server.http_port
            );

            let pool_tp_clone = pool_tp.clone();
            let server = HttpServer::new(move || {
                let logger = Logger::new("%a %{User-Agent}i")
                    .exclude("/api/v1/locate")
                    .exclude("/api/v1/report")
                    .exclude("/api/v1/country")
                    .exclude("/api/v1/match")
                    .exclude("/api/v1/cell");

                App::new()
                    .app_data(web::Data::new(pool_tp_clone.clone()))
                    .app_data(web::Data::new(tx_t38_conn.clone()))
                    .app_data(web::Data::new(rl_app.clone()))
                    .app_data(web::Data::new(thread_pool.clone()))
                    .app_data(web::Data::new(yandex_client.clone()))
                    .app_data(web::Data::new(tx_yandex_api.clone()))
                    .app_data(web::Data::new(tx_report_opt.clone()))
                    .app_data(
                        web::JsonConfig::default()
                            .limit(CONFIG.server.max_payload_mb * 1024 * 1024),
                    )
                    // public services
                    .service(
                        web::scope("/api/v1")
                            .service(services::geolocate_public::service)
                            .service(services::submission::geosubmit_public::service)
                            .service(services::submission::cell::service)
                            .service(services::health::service)
                            .service(services::routing::matching::service)
                            .wrap(HttpAuthentication::bearer(validator))
                            .wrap(logger),
                    )
                    // services for internal use
                    .service(services::geolocate::service)
                    .service(services::submission::geosubmit::service) // for compatibility with NeoStumbler
            })
            .bind(("0.0.0.0", CONFIG.server.http_port))?
            .workers(CONFIG.server.num_http_workers)
            .disable_signals()
            .shutdown_timeout(30)
            .run();

            let handle = server.handle();
            tokio::spawn(graceful_shutdown(handle, save_report_handle_opt));
            server.await?;
        }
        Command::Process => {
            let (tx_t38_conn, rx_t38_conn) = flume::unbounded::<T38ConnectionManageMessage>();
            let _ = t38::connection_manage_task(&CONFIG, rx_t38_conn, tx_t38_conn.clone()).await?;
            services::submission::process::run(
                pool_tp,
                tx_t38_conn,
                yandex_client,
                tx_yandex_api.clone(),
                rl_app.clone(),
            )
            .await?;
        }
        Command::Map => services::map::run(pool_tp).await?,
        Command::Archive { command } => services::archive::run(pool_tp, command).await?,
        Command::FormatMls => services::mls::format()?,
    };

    Ok(())
}

async fn graceful_shutdown(handle: ServerHandle, save_report_handle: Option<JoinHandle<()>>) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigquit = signal(SignalKind::quit()).unwrap();
        let mut sigterm = signal(SignalKind::terminate()).unwrap();
        let mut sigint = signal(SignalKind::interrupt()).unwrap();

        tokio::select! {
            _ = sigquit.recv() => info!("SIGQUIT received"),
            _ = sigterm.recv() => info!("SIGTERM received"),
            _ = sigint.recv() => info!("SIGINT received"),
        }
    }

    #[cfg(not(unix))]
    {
        use tokio::signal::windows::*;

        let mut sigbreak = ctrl_break().unwrap();
        let mut sigint = ctrl_c().unwrap();
        let mut sigquit = ctrl_close().unwrap();
        let mut sigterm = ctrl_shutdown().unwrap();

        tokio::select! {
            _ = sigbreak.recv() => info!("ctrl-break received"),
            _ = sigquit.recv() => info!("ctrl-c received"),
            _ = sigterm.recv() => info!("ctrl-close received"),
            _ = sigint.recv() => info!("ctrl-shutdown received"),
        }
    }

    if let Some(h) = save_report_handle {
        h.abort();
    }

    info!("Locator server stopped");
    handle.stop(true).await;
}
