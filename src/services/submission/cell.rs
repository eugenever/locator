use actix_web::{HttpRequest, HttpResponse, Responder, http::StatusCode, post, web};
use log::error;

use crate::{
    error::create_error_response,
    lbs::{
        model::{self, create_cell_measurement},
        yandex_cell::yandex_lbs_request_by_individual_cell,
    },
    services::rate_limiter::RateLimitersApp,
    tasks::{t38::T38ConnectionManageMessage, yandex::YandexApiMessage},
};

/*
Yandex scheme:

"cell": [
    {
        "lte": {
            "age": 452,
            "earfcn": 1602,
            "ci": 268435455,
            "mcc": 0,
            "tac": 65535,
            "mnc": 0,
            "pci": 124,
            "signal_strength": -67
        }
    },
    {
        "wcdma": {
            "age": 452,
            "uarfcn": 2850,
            "cid": 248700418,
            "mcc": 250,
            "lac": 7702,
            "mnc": 2,
            "psc": 261,
            "signal_strength": -84
        }
    },
    {
        "gsm": {
            "mcc": 250,
            "mnc": 99,
            "lac": 65534,
            "cid": 129430793,
            "signal_strength": -38
        }
    }
]


Locator scheme:

"cell": {
    "gsm": [
        {
            "mcc": 250,
            "mnc": 2,
            "lac": 9742,
            "ci": 2878,
            "rxlev": 80
        }
    ],
    "lte": [
        {
            "mcc": 250,
            "mnc": 1,
            "tac": 15016,
            "eci": 576267,
            "rsrp": -53
        }
    ]
}

*/

#[post("/cell")]
pub async fn service(
    data: web::Json<model::Cell>,
    tx_t38_conn: web::Data<flume::Sender<T38ConnectionManageMessage>>,
    rl_app_web: web::Data<RateLimitersApp>,
    yandex_client_web: web::Data<reqwest::Client>,
    tx_yandex_api_web: web::Data<flume::Sender<YandexApiMessage>>,
    _req: HttpRequest,
) -> actix_web::Result<impl Responder> {
    let data = data.into_inner();
    let tx_t38c = (*tx_t38_conn.into_inner()).clone();
    let yandex_client = (*yandex_client_web.into_inner()).clone();
    let tx_yandex_api = (*tx_yandex_api_web.into_inner()).clone();
    let rl_app = (*rl_app_web.into_inner()).clone();

    let cms = create_cell_measurement(&data);
    match yandex_lbs_request_by_individual_cell(tx_t38c, cms, yandex_client, tx_yandex_api, rl_app)
        .await
    {
        Err(e) => {
            error!("Yandex LBS request by individual cells: {}", e);
            return Ok(create_error_response(e, "cell"));
        }
        Ok(_ylr) => {}
    }

    Ok(HttpResponse::new(StatusCode::OK))
}
