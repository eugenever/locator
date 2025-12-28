use std::collections::HashMap;
use std::time::Duration;

use log::{error, info};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use super::model::{Cell, CellMeasurement};
use crate::{
    config::CONFIG,
    constants::{Collection, RadioType},
    db::t38::{get_yandex_lbs_cell_one, set_yandex_lbs_cell_one},
    error::ApiError,
    lbs::{
        model::{self, create_cell_measurement},
        yandex::YandexLbsResponse,
    },
    services::rate_limiter::RateLimitersApp,
    tasks::{
        t38::T38ConnectionManageMessage,
        yandex::{InvalidApiKey, YandexApiMessage},
    },
};

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YandexLbsRequestCell {
    pub cell: Vec<YandexCell>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellLte {
    pub mcc: u16,
    pub mnc: u16,
    pub tac: u64,
    pub ci: u64,
    pub signal_strength: f64,
}

impl From<model::Lte> for CellLte {
    fn from(value: model::Lte) -> Self {
        CellLte {
            mcc: value.mcc,
            mnc: value.mnc,
            tac: value.tac,
            ci: value.eci,
            signal_strength: value.rsrp,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellWcdma {
    pub mcc: u16,
    pub mnc: u16,
    pub lac: u64,
    pub cid: u64,
    pub signal_strength: f64,
}

impl From<model::Wcdma> for CellWcdma {
    fn from(value: model::Wcdma) -> Self {
        CellWcdma {
            mcc: value.mcc,
            mnc: value.mnc,
            lac: value.lac,
            cid: value.ci,
            signal_strength: value.rscp,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellGsm {
    pub mcc: u16,
    pub mnc: u16,
    pub lac: u64,
    pub cid: u64,
    pub signal_strength: f64,
}

impl From<model::Gsm> for CellGsm {
    fn from(value: model::Gsm) -> Self {
        CellGsm {
            mcc: value.mcc,
            mnc: value.mnc,
            lac: value.lac,
            cid: value.ci,
            signal_strength: value.rxlev,
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YandexCell {
    pub lte: Option<CellLte>,
    pub wcdma: Option<CellWcdma>,
    pub gsm: Option<CellGsm>,
}

pub async fn get_cell(
    cell_opt: Option<Cell>,
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    yandex_client: reqwest::Client,
    tx_yandex_api: flume::Sender<YandexApiMessage>,
    rl_app: RateLimitersApp,
) -> Result<Option<HashMap<String, Option<YandexLbsResponse>>>, ApiError> {
    if let Some(cell) = cell_opt {
        let cms = create_cell_measurement(&cell);

        match yandex_lbs_request_by_individual_cell(
            tx_t38_conn,
            cms,
            yandex_client,
            tx_yandex_api,
            rl_app,
        )
        .await
        {
            Err(e) => {
                error!("Yandex LBS request by individual cells: {}", e);
                return Err(e);
            }
            Ok(ylrs) => {
                return Ok(Some(ylrs));
            }
        };
    } else {
        Ok(None)
    }
}

pub async fn yandex_lbs_request_by_individual_cell(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    cms: Vec<CellMeasurement>,
    yandex_client: reqwest::Client,
    tx_yandex_api: flume::Sender<YandexApiMessage>,
    rl_app: RateLimitersApp,
) -> Result<HashMap<String, Option<YandexLbsResponse>>, ApiError> {
    let mut lbs_responses: HashMap<String, Option<YandexLbsResponse>> =
        HashMap::with_capacity(cms.len());

    let collection = Collection::LbsYandexCell.as_ref();

    for cm in cms {
        let mcc = cm.mcc;
        let mnc = cm.mnc;
        let lac = cm.lac;
        let cid = cm.cid;

        if mcc == 0 && mnc == 0 && lac == 0 && cid == 0 {
            info!("MCC, MNC, LAC, CID is undefined");
            continue;
        }

        let cell_code = format!("{}:{}:{}:{}:{}", &cm.radio_type, mcc, mnc, lac, cid);

        // check whether the specified access point is in the database
        match get_yandex_lbs_cell_one(tx_t38_conn.clone(), collection, &cell_code).await {
            Err(_e) => {
                // don`t repeat the request in Yandex LBS
                lbs_responses.insert(cell_code, None);
                continue;
            }
            Ok(ylr_opt) => {
                if ylr_opt.is_some() {
                    // retrieve a previously saved Yandex response from the database
                    lbs_responses.insert(cell_code, ylr_opt);
                    continue;
                }
                // in case of None make a request to Yandex LBS
            }
        }

        let yandex_cell = match create_yandex_cell(cm) {
            None => continue,
            Some(c) => c,
        };

        let yandex_lbs_request = YandexLbsRequestCell {
            cell: vec![yandex_cell],
        };

        let (tx, rx) = tokio::sync::oneshot::channel();
        let yandex_api_message = YandexApiMessage::GetApiKey { tx };

        if let Err(e) = tx_yandex_api.send_async(yandex_api_message).await {
            error!("send yandex api message: {}", e);
            lbs_responses.insert(cell_code, None);
            continue;
        }

        if let Ok(Some(api_key)) = rx.await {
            let yandex_lbs_url = format!("{}{}", CONFIG.yandex_lbs.url, &api_key.key);

            // Acquire permit before processing request
            // Permit released automatically when the handler is completed
            let _permit = rl_app.yandex_lbs.acquire().await;

            let count_attempts = 5;
            let mut attempt = 0;
            while attempt <= count_attempts {
                attempt = attempt + 1;

                match yandex_client
                    .post(&yandex_lbs_url)
                    .json(&yandex_lbs_request)
                    .send()
                    .await
                {
                    Err(e) => {
                        // undefined request error
                        error!("Yandex LBS request for cell code '{}': {}", &cell_code, e);
                        return Err(ApiError::LbsRequestError());
                    }
                    Ok(response) => {
                        // 400, 403, 429, 500, 504
                        let status = response.status().as_u16();
                        if status != 200 {
                            // repeat the request in Yandex LBS
                            if status == 429 || status == 500 || status == 504 {
                                if attempt == count_attempts {
                                    return Err(ApiError::LbsError(status));
                                }
                                info!(
                                    "cell code: {}, status: {}, attempt: {}",
                                    &cell_code, status, attempt
                                );
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                continue;
                            }
                            if status == 403 {
                                info!("Yandex: number of requests has been exceeded");
                                let yandex_api_message_invalid_key =
                                    YandexApiMessage::InvalidApiKey {
                                        invalid_api_key: InvalidApiKey {
                                            error: Some(format!("StatusCode {}", status)),
                                            i: api_key.i,
                                            key: api_key.key,
                                        },
                                    };
                                if let Err(e) = tx_yandex_api
                                    .send_async(yandex_api_message_invalid_key)
                                    .await
                                {
                                    error!("send yandex api invalid key message: {}", e);
                                }
                            }
                            info!(
                                "cell code: {}, Yandex response status: {}",
                                &cell_code, status
                            );
                            return Err(ApiError::LbsError(status));
                        }

                        match response.json::<serde_json::Value>().await {
                            Err(e) => {
                                error!("deserialize Yandex LBS response: {}", e);
                                lbs_responses.insert(cell_code.clone(), None);
                            }
                            Ok(value) => {
                                // successful response
                                if let serde_json::Value::Object(ref object) = value {
                                    if object.is_empty() {
                                        // no data available for the requested access point
                                        lbs_responses.insert(cell_code.clone(), None);
                                        // exit from while loop
                                        break;
                                    }
                                }

                                if let Ok(yandex_lbs_response) =
                                    serde_json::from_value::<YandexLbsResponse>(value)
                                {
                                    let ylr = yandex_lbs_response.clone();
                                    lbs_responses.insert(cell_code.clone(), Some(ylr));

                                    // save Yandex LBS response in our database
                                    if let Err(e) = set_yandex_lbs_cell_one(
                                        tx_t38_conn.clone(),
                                        collection,
                                        yandex_lbs_response,
                                        &cell_code,
                                    )
                                    .await
                                    {
                                        error!("save Yandex LBS response: {}", e);
                                    }
                                }
                            }
                        }
                        // exit from while loop
                        break;
                    }
                }
            }
        } else {
            return Err(ApiError::LbsError(403));
        }
    }

    Ok(lbs_responses)
}

fn create_yandex_cell(cm: CellMeasurement) -> Option<YandexCell> {
    if cm.radio_type == RadioType::Lte.as_ref() {
        Some(YandexCell {
            lte: Some(CellLte {
                mcc: cm.mcc,
                mnc: cm.mnc,
                tac: cm.lac,
                ci: cm.cid,
                signal_strength: cm.signal_strength,
            }),
            wcdma: None,
            gsm: None,
        })
    } else if cm.radio_type == RadioType::Wcdma.as_ref() {
        Some(YandexCell {
            lte: None,
            wcdma: Some(CellWcdma {
                mcc: cm.mcc,
                mnc: cm.mnc,
                lac: cm.lac,
                cid: cm.cid,
                signal_strength: cm.signal_strength,
            }),
            gsm: None,
        })
    } else if cm.radio_type == RadioType::Gsm.as_ref() {
        Some(YandexCell {
            lte: None,
            wcdma: None,
            gsm: Some(CellGsm {
                mcc: cm.mcc,
                mnc: cm.mnc,
                lac: cm.lac,
                cid: cm.cid,
                signal_strength: cm.signal_strength,
            }),
        })
    } else {
        None
    }
}

#[allow(unused)]
mod tests {
    use super::{YandexLbsRequestCell, YandexLbsResponse};
    use crate::{
        CONFIG,
        lbs::{model::CellMeasurement, yandex_cell::create_yandex_cell},
    };

    /*
        YandexLbsResponse: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.920188903808594, lon: 37.83098983764648 }, accuracy: 250.0 } }
        YandexLbsResponse: YandexLbsResponse { location: YandexLocation { point: YandexPoint { lat: 55.7723388671875, lon: 37.496734619140625 }, accuracy: 1285.8140869140625 } }
    */
    #[tokio::test]
    async fn test_yandex_lbs_request() {
        let api_key = "a765ec4f-7902-4719-aa32-b48888126980";
        let cms = vec![
            CellMeasurement {
                mcc: 250,
                mnc: 1,
                lac: 15016,
                cid: 576267,
                signal_strength: -53.0,
                radio_type: "lte".to_string(),
            },
            CellMeasurement {
                mcc: 250,
                mnc: 2,
                lac: 9742,
                cid: 2878,
                signal_strength: 80.0,
                radio_type: "gsm".to_string(),
            },
        ];

        let client = reqwest::Client::new();
        let yandex_lbs_url = format!("{}{}", CONFIG.yandex_lbs.url, api_key);

        for cm in cms {
            if let Some(yandex_cell) = create_yandex_cell(cm.clone()) {
                let yandex_lbs_request = YandexLbsRequestCell {
                    cell: vec![yandex_cell],
                };
                let cell_code = format!(
                    "{}:{}:{}:{}:{}",
                    &cm.radio_type, cm.mcc, cm.mnc, cm.lac, cm.cid
                );

                match client
                    .post(&yandex_lbs_url)
                    .json(&yandex_lbs_request)
                    .send()
                    .await
                {
                    Err(e) => {
                        // undefined request error
                        println!("Yandex LBS request for cell code '{}': {}", cell_code, e);
                    }
                    Ok(response) => {
                        let status = response.status().as_u16();
                        println!("response status: {}", status);

                        match response.json::<serde_json::Value>().await {
                            Err(e) => {
                                // TODO: disable error message after tests
                                println!("deserialize Yandex LBS response: {}", e);
                            }
                            Ok(value) => {
                                // successful response
                                if let serde_json::Value::Object(ref object) = value {
                                    if object.is_empty() {
                                        // no data available for the requested access point
                                        println!("empty object");
                                    }
                                }

                                if let Ok(yandex_lbs_response) =
                                    serde_json::from_value::<YandexLbsResponse>(value)
                                {
                                    println!("YandexLbsResponse: {:?}", yandex_lbs_response);
                                }
                            }
                        }
                    }
                }
            };
        }
    }
}
