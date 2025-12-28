use actix_web::Error;
use actix_web::dev::ServiceRequest;
use actix_web_httpauth::extractors::bearer::BearerAuth;

use crate::{CONFIG, error::ApiError};

pub async fn validator(
    req: ServiceRequest,
    credentials: BearerAuth,
) -> Result<ServiceRequest, (Error, ServiceRequest)> {
    if CONFIG
        .server
        .creditional_tokens
        .iter()
        .any(|t| t == credentials.token())
    {
        Ok(req)
    } else {
        Err((Error::from(ApiError::InvalidBearer), req))
    }
}
