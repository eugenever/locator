//! Archive all the reports submitted by users.
//!
//! This module handles the archive command.
//! Currently the only subcommand is `export` which exports all submitted data in JSON-format.
//! The export can be triggered manually to remove processed reports from the database
//! to decrease its size and improve speed.

use anyhow::Result;
use clap::Subcommand;
use tokio_postgres::GenericClient;

/// Enum of possible archive commands
#[derive(Debug, Subcommand)]
pub enum ArchiveCommand {
    /// Export processed reports into a JSON-file
    Export,
}

/// Main entry point of the archive command
pub async fn run(pool_tp: deadpool_postgres::Pool, command: ArchiveCommand) -> Result<()> {
    match command {
        ArchiveCommand::Export => {
            let mapper = pool_tp.get().await?;
            let client = mapper.client();
            let reports = crate::db::get_reports_all(client).await?;
            for report in reports {
                println!("{}", serde_json::to_string(&report)?);
            }
        }
    }

    Ok(())
}
