use std::io;

use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/*
    Save intermediate configs similar to Sentinel

    {
        "master": {
            "host": "127.0.0.1",
            "port": 9851
        },
        "slaves": [
            {
                "host": "127.0.0.1",
                "port": 9852
            }
        ]
    }
*/

#[derive(Serialize, Deserialize, Debug)]
pub struct T38Config {
    pub master: IpAddress,
    pub slaves: Vec<IpAddress>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct IpAddress {
    pub host: String,
    pub port: u16,
}

pub async fn read_t38_config_from_file(file_path: &str) -> io::Result<T38Config> {
    let mut file = File::open(file_path).await?;
    let mut contents = String::new();
    file.read_to_string(&mut contents).await?;
    let t38_config: T38Config = serde_json::from_str(&contents)?;
    Ok(t38_config)
}

pub async fn write_t38_config_to_file(t38_config: T38Config, file_path: &str) -> io::Result<()> {
    let mut file = File::create(file_path).await?;
    let contents = serde_json::to_string_pretty(&t38_config)?;
    file.write_all(contents.as_bytes()).await?;
    Ok(())
}
