use serde::{Deserialize, Serialize};

use crate::{CONFIG, lbs::yandex::WifiMeasurement};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AlterGeoLbsResponse {
    pub iamhere: Option<IamHere>,
    pub error: Option<AgError>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IamHere {
    pub latitude: f64,
    pub longitude: f64,
    pub precision: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AgError {
    pub code: u8,
    pub message: String,
}

pub async fn altergeo_lbs_request(
    wms: Vec<WifiMeasurement>,
    client: reqwest::Client,
) -> Result<AlterGeoLbsResponse, anyhow::Error> {
    let ag_lbs_url = format!("{}", CONFIG.altergeo_lbs.url);
    let mut params: Vec<(&str, &str)> = Vec::new();
    params.push(("version", "3.0"));
    params.push(("apikey", &CONFIG.altergeo_lbs.apikey));
    params.push(("doctype", "json"));

    let mut wifi = String::new();
    wms.into_iter().for_each(|w| {
        let mac = w.bssid.replace(":", "");
        let rssi = w.signal_strength as i64;
        let element = format!("{},{};", rssi, mac);
        wifi.push_str(&element);
    });
    let wifi_param = wifi.strip_suffix(";").unwrap_or(&wifi);
    params.push(("wifi", wifi_param));

    let ag_lbs_response: AlterGeoLbsResponse = client
        .post(ag_lbs_url)
        .form(&params)
        .send()
        .await?
        .json()
        .await?;
    Ok(ag_lbs_response)
}

#[allow(unused)]
mod tests {
    use super::{WifiMeasurement, altergeo_lbs_request};

    #[tokio::test]
    async fn test_ag_lbs_request() {
        let wms = vec![
            WifiMeasurement {
                bssid: "ae:84:c6:a9:45:d2".to_string(),
                signal_strength: -62.0,
            },
            WifiMeasurement {
                bssid: "00:a2:b0:8c:90:e5".to_string(),
                signal_strength: -57.0,
            },
        ];
        let client = reqwest::Client::new();
        let ag_lbs_response = altergeo_lbs_request(wms, client).await;
        match ag_lbs_response {
            Err(err) => {
                println!("Error AlterGeo LBS request: {}", err)
            }
            Ok(resp) => {
                println!("AlterGeo LBS location: {:?}", resp)
            }
        }
    }
}
