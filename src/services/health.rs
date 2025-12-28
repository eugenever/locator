use actix_web::{HttpRequest, HttpResponse, get};
use serde_json::json;

#[get("/health")]
pub async fn service(_req: HttpRequest) -> actix_web::Result<HttpResponse> {
    Ok(HttpResponse::Ok().json(json!(
        {
            "message": "OK"
        }
    )))
}
