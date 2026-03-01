use std::collections::HashMap;
use std::time::Duration;

use chrono::{Datelike, Utc};
use log::{error, info, warn};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::interval;

use crate::{
    CONFIG,
    config::YandexApiKey,
    lbs::{
        http_client::HttpClient,
        yandex::wifi::{WifiMeasurement, YandexLbsRequest},
    },
};

pub struct ApiKey {
    pub key: YandexApiKey,
    pub i: u8,
}

#[derive(Clone)]
pub struct InvalidApiKey {
    pub _error: Option<String>,
    pub key: YandexApiKey,
    pub created_date: chrono::NaiveDate,
    pub i: u8,
}

#[derive(Clone)]
pub struct ValidApiKey {
    pub key: YandexApiKey,
    pub i: u8,
}

pub enum YandexApiMessage {
    GetApiKey { tx: oneshot::Sender<Option<ApiKey>> },
    InvalidApiKey { invalid_api_key: InvalidApiKey },
    ValidApiKey { valid_api_key: ValidApiKey },
}

pub fn yandex_api_task(
    rx: flume::Receiver<YandexApiMessage>,
    tx: flume::Sender<YandexApiMessage>,
    yandex_client: HttpClient,
) -> JoinHandle<()> {
    let mut active_api_keys: HashMap<u8, YandexApiKey> = HashMap::new();
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
        let mut interval = interval(Duration::from_secs(120)); // 2 minutes
        let tz_moscow = chrono_tz::Europe::Moscow;

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
                    if !invalid_api_keys.is_empty() {
                        let now_date = chrono::Utc::now().with_timezone(&tz_moscow).date_naive();
                        // to remove active keys from the original HashMap 'invalid_api_keys'
                        let temp_keys = invalid_api_keys.clone();

                        for (i, invalid_api_key) in temp_keys {
                            // strictly more
                            if now_date > invalid_api_key.created_date {
                                let yandex_lbs_url = format!("{}{}", CONFIG.yandex_lbs.url, &invalid_api_key.key.key);
                                if let Ok(status) = yandex_client.post_for_status(&yandex_lbs_url, &yandex_lbs_request).await {
                                    if status == 200 && let Some(valid_key) = invalid_api_keys.remove(&i) {
                                        let m = YandexApiMessage::ValidApiKey { valid_api_key: ValidApiKey {key: valid_key.key, i} };
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
        let mut start_day = Utc::now().day();
        let mut count_day_requests = 0u64;

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
                            i: index,
                        })) {
                            error!("send Yandex api key");
                        }
                    } else {
                        // fallback
                        if let Some((i, k)) = active_api_keys.iter().next() {
                            if tx
                                .send(Some(ApiKey {
                                    key: k.clone(),
                                    i: *i,
                                }))
                                .is_err()
                            {
                                error!("send Yandex api key");
                            }
                        }
                    }

                    // DEBUG
                    // TODO: remove after tests by Whoosh
                    {
                        let now = Utc::now().with_timezone(&chrono_tz::Europe::Moscow);
                        let now_day = now.day();
                        if now_day == start_day {
                            count_day_requests += 1;
                        } else {
                            start_day = now_day;
                            count_day_requests = 1;
                        }

                        warn!(
                            "{}, requests in YL: {}",
                            now.format("%d-%m-%Y"),
                            count_day_requests
                        );
                    }
                }
                YandexApiMessage::InvalidApiKey { invalid_api_key } => {
                    // deactivate the key blocked by Yandex
                    if let Some(api_key) = active_api_keys.remove(&invalid_api_key.i) {
                        count_keys = active_api_keys.len();
                        info!("Yandex Api key '{}' is blocked", api_key.key);
                    }
                    if let Err(e) = tx_inv_key.send_async(invalid_api_key).await {
                        error!("send invalid Yandex Api key: {}", e);
                    }
                }
                YandexApiMessage::ValidApiKey { valid_api_key } => {
                    info!("Yandex Api key '{}' is active", &valid_api_key.key.key);
                    // activate the key unlocked by Yandex
                    active_api_keys.insert(valid_api_key.i, valid_api_key.key);
                    count_keys = active_api_keys.len();
                }
            }
        }
    })
}
