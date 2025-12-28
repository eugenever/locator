use actix_web::{HttpResponse, ResponseError, http::StatusCode};
use deadpool_postgres::PoolError;
use redis::ParsingError;
use serde::Serialize;
use serde_json::json;
use thiserror::Error;

#[derive(Serialize)]
pub struct ErrorResponse<'a> {
    pub code: u16,
    pub error: &'a str,
    pub message: &'a str,
}

#[allow(unused)]
#[derive(Error, Debug)]
pub enum ApiError {
    #[error("Artificial error")]
    Artificial,
    #[error("Unknown error")]
    Unknown,
    #[error("Invalid Session info")]
    InvalidSession,
    #[error("Server error")]
    ServerError,
    #[error("Document Not found")]
    DocumentNotFound,
    #[error("Collection with id {0} not found")]
    CollectionNotFound(String),
    #[error("Malformed Url")]
    MalformedUrl,
    #[error("Json error")]
    JsonProcessingError,
    #[error("Invalid Bearer")]
    InvalidBearer,
    #[error("Query error")]
    Query(#[from] actix_web::error::QueryPayloadError),
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Blocking error")]
    BlockingError(#[from] actix_web::error::BlockingError),
    #[error("Login Required")]
    LoginRequiredForFeature(String),
    #[error("Not implemented")]
    NotImplemented,
    #[error("Forbidden")]
    Forbidden,
    #[error("Database error")]
    DatabaseError(String),
    #[error("Tile38 error: {0}")]
    Tile38Error(String),
    #[error("Rate limiter error")]
    RateLimiter(anyhow::Error),
    #[error("Couldn't parse timestamp {0}")]
    TimestampParseFailed(String),
    #[error("Couldn't parse DateTime {0}")]
    DateTimeParseFailed(String),
    #[error("GPX data error {0}")]
    GpxDataError(String),
    #[error("Redis parsing error {0}")]
    RedisParsingError(String),
    #[error("Redis no data")]
    RedisNoData,
    #[error("LBS error code: {0}")]
    LbsError(u16),
    #[error("LBS request error")]
    LbsRequestError(),
}

impl ApiError {
    pub fn name(&self) -> &str {
        match self {
            Self::Artificial => "Artificial",
            Self::Unknown => "Unknown",
            Self::InvalidSession => "Invalid Session",
            Self::ServerError => "Server error",
            Self::DocumentNotFound => "Document not found",
            Self::InvalidBearer => "Invalid bearer info",
            Self::MalformedUrl => "Malformed URL",
            Self::JsonProcessingError => "Error processing JSON document",
            Self::Query(_) => "Query error",
            Self::Unauthorized => "Unauthorized",
            Self::BlockingError(_) => "Blocking error",
            Self::CollectionNotFound(_) => "Collection not found",
            Self::LoginRequiredForFeature(_) => "Login Required",
            Self::NotImplemented => "Not implemented",
            Self::Forbidden => "Forbidden",
            Self::DatabaseError(_) => "Database error",
            Self::Tile38Error(_) => "Tile38 error",
            Self::RateLimiter(_) => "Rate limiter error",
            Self::TimestampParseFailed(_) => "Parse timestamp error",
            Self::DateTimeParseFailed(_) => "Parse DateTime error",
            Self::GpxDataError(_) => "Gpx data error",
            Self::RedisParsingError(_) => "Redis parsing error",
            Self::RedisNoData => "Redis no data",
            Self::LbsError(_) => "LBS error",
            Self::LbsRequestError() => "LBS request error",
        }
    }
}

impl ResponseError for ApiError {
    fn status_code(&self) -> StatusCode {
        match *self {
            Self::InvalidSession => StatusCode::BAD_REQUEST,
            Self::DocumentNotFound => StatusCode::NOT_FOUND,
            Self::InvalidBearer => StatusCode::FORBIDDEN,
            Self::MalformedUrl => StatusCode::BAD_REQUEST,
            Self::Query(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::CollectionNotFound(_) => StatusCode::BAD_REQUEST,
            Self::LoginRequiredForFeature(_) => StatusCode::UNAUTHORIZED,
            Self::NotImplemented => StatusCode::NOT_IMPLEMENTED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Tile38Error(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::TimestampParseFailed(_) => StatusCode::BAD_REQUEST,
            Self::GpxDataError(_) => StatusCode::BAD_REQUEST,
            Self::LbsError(code) => StatusCode::from_u16(code).unwrap(),
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let status_code = self.status_code();
        let mut builder = HttpResponse::build(status_code);
        match self {
            ApiError::CollectionNotFound(id) => builder.json(ErrorResponse {
                code: status_code.as_u16(),
                message: format!("Collection with id {} not found", id).as_str(),
                error: self.name(),
            }),
            ApiError::LoginRequiredForFeature(feature) => builder.json(ErrorResponse {
                code: status_code.as_u16(),
                message: format!("Please login to use feature: {0}", feature).as_str(),
                error: self.name(),
            }),
            _ if status_code == StatusCode::INTERNAL_SERVER_ERROR => builder.json(ErrorResponse {
                code: status_code.as_u16(),
                message: "internal server error",
                error: self.name(),
            }),
            _ => builder.json(ErrorResponse {
                code: status_code.as_u16(),
                message: &self.to_string(),
                error: self.name(),
            }),
        }
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(_: serde_json::Error) -> Self {
        ApiError::JsonProcessingError
    }
}

impl From<PoolError> for ApiError {
    fn from(error: PoolError) -> Self {
        ApiError::DatabaseError(error.to_string())
    }
}

impl From<ParsingError> for ApiError {
    fn from(error: ParsingError) -> Self {
        ApiError::RedisParsingError(error.to_string())
    }
}

pub fn create_error_response(e: ApiError, domain: &str) -> HttpResponse {
    match e {
        ApiError::LbsError(code) => {
            let error_resp = error_response(code);
            error_resp
        }
        _ => {
            let error_resp = HttpResponse::InternalServerError().json(json!(
                {
                    "error": {
                        "domain": domain,
                        "reason": "internal server error",
                        "message": "",
                        "code": 500,
                    }
                }
            ));
            error_resp
        }
    }
}

pub fn error_response(code: u16) -> HttpResponse {
    match code {
        400 => HttpResponse::BadRequest().json(json!(
            {
                "error": {
                    "domain": "locate",
                    "reason": "LBS error",
                    "message": "bad request",
                    "code": 400,
                }
            }
        )),
        403 => HttpResponse::Forbidden().json(json!(
            {
                "error": {
                    "domain": "locate",
                    "reason": "LBS error",
                    "message": "invalid api key",
                    "code": 403,
                }
            }
        )),
        429 => HttpResponse::TooManyRequests().json(json!(
            {
                "error": {
                    "domain": "locate",
                    "reason": "LBS error",
                    "message": "the number of requests has been exceeded",
                    "code": 429,
                }
            }
        )),
        _ => HttpResponse::InternalServerError().json(json!(
            {
                "error": {
                    "domain": "locate",
                    "reason": "LBS error",
                    "message": "internal server error",
                    "code": code,
                }
            }
        )),
    }
}
