use std::collections::HashMap;
use std::time::Duration;

use log::{error, info};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::interval;

use crate::CONFIG;
use crate::lbs::yandex::{WifiMeasurement, YandexLbsRequest};

#[derive(Debug)]
pub struct ApiKey {
    pub key: String,
    pub i: u8,
}

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct InvalidApiKey {
    pub error: Option<String>,
    pub key: String,
    pub i: u8,
}

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct ValidApiKey {
    pub key: String,
    pub i: u8,
}

#[derive(Debug)]
pub enum YandexApiMessage {
    GetApiKey { tx: oneshot::Sender<Option<ApiKey>> },
    InvalidApiKey { invalid_api_key: InvalidApiKey },
    ValidApipKey { valid_api_key: ValidApiKey },
}

pub fn yandex_api_task(
    rx: flume::Receiver<YandexApiMessage>,
    tx: flume::Sender<YandexApiMessage>,
    yandex_client: reqwest::Client,
) -> JoinHandle<()> {
    let mut active_api_keys: HashMap<u8, String> = HashMap::new();
    let mut invalid_api_keys: HashMap<u8, InvalidApiKey> = HashMap::new();

    CONFIG
        .yandex_lbs
        .api_keys
        .iter()
        .enumerate()
        .for_each(|(i, api_key)| {
            active_api_keys.insert(i as u8, api_key.clone());
        });

    let (tx_inv_key, rx_inv_key) = flume::unbounded::<InvalidApiKey>();

    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(1 * 60 * 60)); // 1 hour

        let wm = WifiMeasurement {
            bssid: "52:91:e3:2a:c0:ce".to_string(),
            signal_strength: -70.0,
        };
        // any arbitrary request to check the response code
        let yandex_lbs_request = YandexLbsRequest { wifi: vec![wm] };

        loop {
            tokio::select! {
                message = rx_inv_key.recv_async() => {
                    match message {
                        Err(e) => {
                            error!("receive invalid Yandex Api key: {}", e);
                            // tx_inv_key channel was closed abnormally, exiting the loop
                            break;
                        }
                        Ok(invalid_api_key) => {
                            invalid_api_keys.insert(invalid_api_key.i, invalid_api_key);
                        }
                    }
                }
                _ = interval.tick() => {
                    if invalid_api_keys.len() > 0 {
                        // to remove active keys from the original HashMap 'invalid_api_keys'
                        let temp_keys = invalid_api_keys.clone();

                        for (i, invalid_api_key) in temp_keys {
                            let yandex_lbs_url = format!("{}{}", CONFIG.yandex_lbs.url, &invalid_api_key.key);
                            if let Ok(response) = yandex_client.post(&yandex_lbs_url).json(&yandex_lbs_request).send().await {
                                let status = response.status().as_u16();
                                if status == 200 {
                                    if let Some(valid_key) = invalid_api_keys.remove(&i) {
                                        let m = YandexApiMessage::ValidApipKey { valid_api_key: ValidApiKey {key: valid_key.key, i: i} };
                                        if let Err(e) = tx.send_async(m).await {
                                            error!("send valid Yandex Api key: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    });

    tokio::spawn(async move {
        let mut index_key = 0;
        let mut count_keys = active_api_keys.len();

        while let Ok(message) = rx.recv_async().await {
            match message {
                YandexApiMessage::GetApiKey { tx } => {
                    if count_keys == 0 {
                        if let Err(_e) = tx.send(None) {
                            error!("send None as Yandex api key");
                        }
                        continue;
                    }

                    // simple balancing based on the order of keys
                    let index = {
                        if index_key > (count_keys as u8 - 1) {
                            index_key = 1;
                            0
                        } else {
                            index_key += 1;
                            index_key - 1
                        }
                    };

                    if let Some(k) = active_api_keys.get(&index) {
                        if let Err(_e) = tx.send(Some(ApiKey {
                            key: k.clone(),
                            i: index as u8,
                        })) {
                            error!("send Yandex api key");
                        }
                    } else {
                        // fallback
                        for (i, k) in active_api_keys.iter() {
                            if let Err(_e) = tx.send(Some(ApiKey {
                                key: k.clone(),
                                i: *i,
                            })) {
                                error!("send Yandex api key");
                            }
                            // perform only one iteration
                            break;
                        }
                    }
                }
                YandexApiMessage::InvalidApiKey { invalid_api_key } => {
                    // deactivate the key blocked by Yandex
                    if let Some(api_key) = active_api_keys.remove(&invalid_api_key.i) {
                        count_keys = active_api_keys.len();
                        info!("Yandex Api key '{}' is blocked", api_key);
                    }
                    if let Err(e) = tx_inv_key.send_async(invalid_api_key).await {
                        error!("send invalid Yandex Api key: {}", e);
                    }
                }
                YandexApiMessage::ValidApipKey { valid_api_key } => {
                    info!("Yandex Api key '{}' is active", &valid_api_key.key);
                    // activate the key unlocked by Yandex
                    active_api_keys.insert(valid_api_key.i, valid_api_key.key);
                    count_keys = active_api_keys.len();
                }
            }
        }
    })
}
