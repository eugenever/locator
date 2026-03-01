#![allow(unused)]

use std::collections::HashMap;
use std::time::Duration;

use log::{error, info};
use serde::Serialize;

use crate::{
    config::CONFIG,
    constants::{Collection, HC},
    db::{
        blobasaur::{set_ba_lbs_yandex_cell_one, set_ba_lbs_yandex_wifi_one},
        t38::{set_yandex_lbs_cell_one, set_yandex_lbs_wifi_missing_one, set_yandex_lbs_wifi_one},
    },
    error::ApiError,
    lbs::yandex::{
        COUNT_ATTEMPTS, TIMEOUT,
        wifi::{YandexLbsResponse, YandexWifiMissing},
    },
    tasks::{
        blobasaur::BAConnectionManageMessage,
        t38::T38ConnectionManageMessage,
        yandex::{ApiKey, InvalidApiKey, YandexApiMessage},
    },
};

#[derive(Clone)]
pub enum HttpClient {
    Surf { client: surf::Client },
    Reqwest { client: reqwest::Client },
}

impl HttpClient {
    pub fn new(hc: HC) -> Self {
        match hc {
            HC::Surf => Self::Surf {
                client: surf::Client::new(),
            },
            HC::Reqwest => Self::Reqwest {
                client: reqwest::Client::new(),
            },
        }
    }

    pub async fn post_for_status<T: Serialize>(
        &self,
        url: &str,
        body: &T,
    ) -> Result<u16, ApiError> {
        match self {
            HttpClient::Surf { client } => {
                if let Ok(builder) = client.post(url).body_json(body)
                    && let Ok(response) = builder.await
                {
                    let status: u16 = response.status().into();
                    return Ok(status);
                }
            }
            HttpClient::Reqwest { client } => {
                if let Ok(response) = client.post(url).json(body).send().await {
                    let status = response.status().as_u16();
                    return Ok(status);
                }
            }
        }
        Err(ApiError::LbsRequestError())
    }

    pub async fn post_for_data<T: Serialize>(
        &self,
        key: &str,
        url: &str,
        body: &T,
        tx_yandex_api: flume::Sender<YandexApiMessage>,
        api_key: ApiKey,
        lbs_responses: &mut HashMap<String, Option<YandexLbsResponse>>,
        tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
        tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
        collection: &str,
    ) -> Result<(), ApiError> {
        match self {
            HttpClient::Surf { client } => {
                let mut attempt = 0;
                while attempt <= COUNT_ATTEMPTS {
                    attempt += 1;

                    match client.post(url).body_json(body).unwrap().await {
                        Err(e) => {
                            // undefined request error
                            error!("Yandex LBS request for key '{}': {}", key, e);
                            return Err(ApiError::LbsRequestError());
                        }
                        Ok(mut response) => {
                            // 400, 403, 429, 500, 504
                            let status = response.status().into();
                            if status != 200 {
                                // repeat the request in Yandex LBS
                                if status == 429 || status == 500 || status == 504 {
                                    if attempt == COUNT_ATTEMPTS {
                                        return Err(ApiError::LbsError(status));
                                    }
                                    info!("key: {}, status: {}, attempt: {}", key, status, attempt);
                                    tokio::time::sleep(Duration::from_millis(TIMEOUT)).await;
                                    continue;
                                }
                                if status == 403 {
                                    info!("Yandex: number of requests has been exceeded");
                                    let created_date = chrono::Utc::now()
                                        .with_timezone(&chrono_tz::Europe::Moscow)
                                        .date_naive();
                                    let yandex_api_message_invalid_key =
                                        YandexApiMessage::InvalidApiKey {
                                            invalid_api_key: InvalidApiKey {
                                                _error: Some(format!("StatusCode {}", status)),
                                                i: api_key.i,
                                                key: api_key.key,
                                                created_date,
                                            },
                                        };
                                    if let Err(e) = tx_yandex_api
                                        .send_async(yandex_api_message_invalid_key)
                                        .await
                                    {
                                        error!("send yandex api invalid key message: {}", e);
                                    }
                                }
                                info!("key: {}, Yandex response status: {}", key, status);
                                return Err(ApiError::LbsError(status));
                            }

                            match response.body_json::<serde_json::Value>().await {
                                Err(e) => {
                                    error!("deserialize Yandex LBS response: {}", e);
                                    lbs_responses.insert(key.to_string(), None);
                                }
                                Ok(value) => {
                                    // successful response
                                    if let serde_json::Value::Object(ref object) = value
                                        && object.is_empty()
                                    {
                                        // no data available for the requested access point
                                        lbs_responses.insert(key.to_string(), None);

                                        if collection == Collection::LbsYandexWifi.as_ref() {
                                            let now = chrono::Utc::now();
                                            let ywm = YandexWifiMissing {
                                                mac: key.to_string(),
                                                ts: now.format("%d-%m-%Y %H:%M").to_string(),
                                            };
                                            if let Err(e) = set_yandex_lbs_wifi_missing_one(
                                                tx_t38_conn.clone(),
                                                Collection::LbsYandexWifiMissing.as_ref(),
                                                ywm,
                                            )
                                            .await
                                            {
                                                error!("set Yandex WiFi as missing: {}", e);
                                            }
                                        }

                                        // exit from while loop
                                        break;
                                    }

                                    if let Ok(yandex_lbs_response) =
                                        serde_json::from_value::<YandexLbsResponse>(value)
                                    {
                                        let ylr = yandex_lbs_response.clone();
                                        lbs_responses.insert(key.to_string(), Some(ylr));

                                        if collection == Collection::LbsYandexCell.as_ref() {
                                            // save Yandex LBS response in our database
                                            if let Err(e) = set_yandex_lbs_cell_one(
                                                tx_t38_conn.clone(),
                                                collection,
                                                &yandex_lbs_response,
                                                key,
                                            )
                                            .await
                                            {
                                                error!("save Yandex LBS response: {}", e);
                                            }

                                            // save yandex response in blobasaur
                                            if CONFIG.blobasaur.enabled {
                                                let namespace =
                                                    Collection::BaLbsYandexCell.as_ref();
                                                if let Err(e) = set_ba_lbs_yandex_cell_one(
                                                    tx_ba_conn.clone(),
                                                    namespace,
                                                    yandex_lbs_response,
                                                    key,
                                                )
                                                .await
                                                {
                                                    error!(
                                                        "save Yandex LBS response in blobasaur: {}",
                                                        e
                                                    );
                                                }
                                            }
                                        } else if collection == Collection::LbsYandexWifi.as_ref() {
                                            if let Err(e) = set_yandex_lbs_wifi_one(
                                                tx_t38_conn.clone(),
                                                collection,
                                                &yandex_lbs_response,
                                                key,
                                            )
                                            .await
                                            {
                                                error!("save Yandex LBS response: {}", e);
                                            }

                                            // save yandex response in blobasaur
                                            if CONFIG.blobasaur.enabled {
                                                let namespace =
                                                    Collection::BaLbsYandexWifi.as_ref();
                                                if let Err(e) = set_ba_lbs_yandex_wifi_one(
                                                    tx_ba_conn.clone(),
                                                    namespace,
                                                    yandex_lbs_response,
                                                    key,
                                                )
                                                .await
                                                {
                                                    error!(
                                                        "save Yandex LBS response in blobasaur: {}",
                                                        e
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            // exit from while loop
                            break;
                        }
                    }
                }
            }
            HttpClient::Reqwest { client } => {
                let mut attempt = 0;
                while attempt <= COUNT_ATTEMPTS {
                    attempt += 1;

                    match client.post(url).json(body).send().await {
                        Err(e) => {
                            // undefined request error
                            error!("Yandex LBS request for key '{}': {}", key, e);
                            return Err(ApiError::LbsRequestError());
                        }
                        Ok(response) => {
                            // 400, 403, 429, 500, 504
                            let status = response.status().as_u16();
                            if status != 200 {
                                // repeat the request in Yandex LBS
                                if status == 429 || status == 500 || status == 504 {
                                    if attempt == COUNT_ATTEMPTS {
                                        return Err(ApiError::LbsError(status));
                                    }
                                    info!("key: {}, status: {}, attempt: {}", key, status, attempt);
                                    tokio::time::sleep(Duration::from_millis(TIMEOUT)).await;
                                    continue;
                                }
                                if status == 403 {
                                    info!("Yandex: number of requests has been exceeded");
                                    let created_date = chrono::Utc::now()
                                        .with_timezone(&chrono_tz::Europe::Moscow)
                                        .date_naive();
                                    let yandex_api_message_invalid_key =
                                        YandexApiMessage::InvalidApiKey {
                                            invalid_api_key: InvalidApiKey {
                                                _error: Some(format!("StatusCode {}", status)),
                                                i: api_key.i,
                                                key: api_key.key,
                                                created_date,
                                            },
                                        };
                                    if let Err(e) = tx_yandex_api
                                        .send_async(yandex_api_message_invalid_key)
                                        .await
                                    {
                                        error!("send yandex api invalid key message: {}", e);
                                    }
                                }
                                info!("key: {}, Yandex response status: {}", key, status);
                                return Err(ApiError::LbsError(status));
                            }

                            match response.json::<serde_json::Value>().await {
                                Err(e) => {
                                    error!("deserialize Yandex LBS response: {}", e);
                                    lbs_responses.insert(key.to_string(), None);
                                }
                                Ok(value) => {
                                    // successful response
                                    if let serde_json::Value::Object(ref object) = value
                                        && object.is_empty()
                                    {
                                        // no data available for the requested access point
                                        lbs_responses.insert(key.to_string(), None);

                                        if collection == Collection::LbsYandexWifi.as_ref() {
                                            let now = chrono::Utc::now();
                                            let ywm = YandexWifiMissing {
                                                mac: key.to_string(),
                                                ts: now.format("%d-%m-%Y %H:%M").to_string(),
                                            };
                                            if let Err(e) = set_yandex_lbs_wifi_missing_one(
                                                tx_t38_conn.clone(),
                                                Collection::LbsYandexWifiMissing.as_ref(),
                                                ywm,
                                            )
                                            .await
                                            {
                                                error!("set Yandex WiFi as missing: {}", e);
                                            }
                                        }

                                        // exit from while loop
                                        break;
                                    }

                                    if let Ok(yandex_lbs_response) =
                                        serde_json::from_value::<YandexLbsResponse>(value)
                                    {
                                        let ylr = yandex_lbs_response.clone();
                                        lbs_responses.insert(key.to_string(), Some(ylr));

                                        if collection == Collection::LbsYandexCell.as_ref() {
                                            // save Yandex LBS response in our database
                                            if let Err(e) = set_yandex_lbs_cell_one(
                                                tx_t38_conn.clone(),
                                                collection,
                                                &yandex_lbs_response,
                                                key,
                                            )
                                            .await
                                            {
                                                error!("save Yandex LBS response: {}", e);
                                            }

                                            // save yandex response in blobasaur
                                            if CONFIG.blobasaur.enabled {
                                                let namespace =
                                                    Collection::BaLbsYandexCell.as_ref();
                                                if let Err(e) = set_ba_lbs_yandex_cell_one(
                                                    tx_ba_conn.clone(),
                                                    namespace,
                                                    yandex_lbs_response,
                                                    key,
                                                )
                                                .await
                                                {
                                                    error!(
                                                        "save Yandex LBS response in blobasaur: {}",
                                                        e
                                                    );
                                                }
                                            }
                                        } else if collection == Collection::LbsYandexWifi.as_ref() {
                                            if let Err(e) = set_yandex_lbs_wifi_one(
                                                tx_t38_conn.clone(),
                                                collection,
                                                &yandex_lbs_response,
                                                key,
                                            )
                                            .await
                                            {
                                                error!("save Yandex LBS response: {}", e);
                                            }

                                            // save yandex response in blobasaur
                                            if CONFIG.blobasaur.enabled {
                                                let namespace =
                                                    Collection::BaLbsYandexWifi.as_ref();
                                                if let Err(e) = set_ba_lbs_yandex_wifi_one(
                                                    tx_ba_conn.clone(),
                                                    namespace,
                                                    yandex_lbs_response,
                                                    key,
                                                )
                                                .await
                                                {
                                                    error!(
                                                        "save Yandex LBS response in blobasaur: {}",
                                                        e
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            // exit from while loop
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
