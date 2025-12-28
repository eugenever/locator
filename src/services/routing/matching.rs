use actix_web::{HttpResponse, post, web};
use futures::StreamExt;
use log::{error, warn};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{CONFIG, services::rate_limiter::RateLimitersApp};

/*

curl -X POST \
-H "Content-Type: application/gpx+xml" \
-H "Authorization: Bearer SECRET-TOKEN" \
-d @test.gpx \
"http://localhost:8080/api/v1/match?profile=bike&type=json&instructions=false&gps_accuracy=50&points_encoded=true"


*/

#[post("/match")]
pub async fn service(
    rl_app: web::Data<RateLimitersApp>,
    mut payload: actix_web::web::Payload,
    params: web::Query<Params>,
) -> actix_web::Result<HttpResponse> {
    // Acquire permit before processing request
    // Permit released automatically when the handler is completed
    let _permit = rl_app.gh_matching.acquire().await;

    let mut body = actix_web::web::BytesMut::with_capacity(1024 * 256);
    while let Some(chunk) = payload.next().await {
        let chunk = chunk?;
        body.extend_from_slice(&chunk);
    }

    match gpx::read(body.as_ref()) {
        Ok(gpx_data) => {
            let mut buffer: Vec<u8> = Vec::new();
            match gpx::write(&gpx_data, &mut buffer) {
                Err(err) => {
                    error!("write data to gpx: {}", err.to_string());
                    return Ok(HttpResponse::InternalServerError().json(json!(
                        {
                            "error": {
                                "domain": "match",
                                "reason": "Error write data to gpx",
                                "message": err.to_string(),
                                "code": 500,
                            }
                        }
                    )));
                }
                Ok(_) => {
                    let gh_response = match gh_match_request(buffer, params.into_inner()).await {
                        Err(err) => {
                            return Ok(HttpResponse::InternalServerError().json(json!(
                                {
                                    "error": {
                                        "domain": "match",
                                        "reason": "Matching request error",
                                        "message": err.to_string(),
                                        "code": 500,
                                    }
                                }
                            )));
                        }
                        Ok(response) => response,
                    };

                    let paths_len = gh_response.paths.len();
                    if paths_len > 1 {
                        warn!("graphhopper paths len = {}", paths_len);
                    }

                    match gh_response.paths.first() {
                        None => {
                            return Ok(HttpResponse::NotFound().json(json!(
                                {
                                    "error": {
                                        "domain": "match",
                                        "reason": "Path not found",
                                        "message": "Path not found",
                                        "code": 404,
                                    }
                                }
                            )));
                        }
                        Some(path) => match &path.points {
                            // encoded geometry
                            serde_json::Value::String(s) => match polyline::decode_polyline(s, 5) {
                                Err(err) => {
                                    return Ok(HttpResponse::InternalServerError().json(json!(
                                        {
                                            "error": {
                                                "domain": "match",
                                                "reason": "Error decode polyline",
                                                "message": err.to_string(),
                                                "code": 500,
                                            }
                                        }
                                    )));
                                }
                                Ok(_line) => {
                                    return Ok(HttpResponse::Ok().json(json!(
                                        {
                                            "geometry_encoded": path.points,
                                            "bbox": path.bbox,
                                            "distance": path.distance
                                        }
                                    )));
                                }
                            },
                            // geometry as LineString
                            serde_json::Value::Object(_) => {
                                let g: Geometry = match serde_json::from_value(path.points.clone())
                                {
                                    Err(err) => {
                                        return Ok(HttpResponse::InternalServerError().json(
                                            json!(
                                                {
                                                    "error": {
                                                        "domain": "match",
                                                        "reason": "Error deserialize object geometry",
                                                        "message": err.to_string(),
                                                        "code": 500,
                                                    }
                                                }
                                            ),
                                        ));
                                    }
                                    Ok(geometry) => geometry,
                                };
                                return Ok(HttpResponse::Ok().json(json!(
                                    {
                                        "geometry": g,
                                        "bbox": path.bbox,
                                        "distance": path.distance
                                    }
                                )));
                            }
                            _ => {
                                error!("Unsupported type geometry");
                                return Ok(HttpResponse::InternalServerError().json(json!(
                                    {
                                        "error": {
                                            "domain": "match",
                                            "reason": "Unsupported type geometry",
                                            "message": "Unsupported type geometry",
                                            "code": 500,
                                        }
                                    }
                                )));
                            }
                        },
                    }
                }
            }
        }
        Err(err) => {
            error!("Can`t convert request body into gpx {:?}", err);
            return Ok(HttpResponse::BadRequest().json(json!(
                {
                    "error": {
                        "domain": "match",
                        "reason": "Invalid gpx data",
                        "message": err.to_string(),
                        "code": 400,
                    }
                }
            )));
        }
    }
}

#[derive(Debug, Deserialize)]
struct Params {
    profile: Option<String>,
    #[serde(rename = "type")]
    response_type: Option<String>,
    instructions: Option<bool>,
    gps_accuracy: Option<f64>,
    points_encoded: Option<bool>,
    #[serde(rename = "ch.disable")]
    ch_disable: Option<bool>,
    #[serde(rename = "lm.disable")]
    lm_disable: Option<bool>,
}

#[derive(Deserialize, Debug)]
struct GhMatchingResponse {
    paths: Vec<MatchingPath>,
    // #[serde(flatten)]
    // extra: serde_json::Value,
}

#[derive(Deserialize, Debug)]
struct MatchingPath {
    bbox: [f64; 4],
    distance: f64,
    points: serde_json::Value,
    // #[serde(flatten)]
    // extra: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
struct Geometry {
    coordinates: Vec<[f64; 2]>,
    #[serde(rename = "type")]
    geometry_type: String,
}

async fn gh_match_request(
    data: Vec<u8>,
    mut params: Params,
) -> Result<GhMatchingResponse, anyhow::Error> {
    let profile = params.profile.take().unwrap_or("bike".to_string());
    let response_type = params.response_type.take().unwrap_or("json".to_string());
    let instructions = params.instructions.take().unwrap_or(false);
    let gps_accuracy = params.gps_accuracy.take().unwrap_or(20.0);
    let points_encoded = params.points_encoded.take().unwrap_or(true);

    let mut gh_url = format!(
        "http://{}:{}/match?profile={}&type={}&instructions={}&gps_accuracy={}&points_encoded={}",
        CONFIG.graphhopper.host,
        CONFIG.graphhopper.port,
        profile,
        response_type,
        instructions,
        gps_accuracy,
        points_encoded,
    );
    if let Some(ch_disable) = params.ch_disable {
        let chd = format!("&ch.disable={}", ch_disable);
        gh_url.push_str(&chd);
    }
    if let Some(lm_disable) = params.lm_disable {
        let lmd = format!("&lm.disable={}", lm_disable);
        gh_url.push_str(&lmd);
    }

    let gh_response: GhMatchingResponse = reqwest::Client::new()
        .post(gh_url)
        .body(data)
        .header("Content-Type", "application/gpx+xml")
        .send()
        .await?
        .json()
        .await?;

    Ok(gh_response)
}
