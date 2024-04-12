
use axum::http::{StatusCode};
use axum::{async_trait, Json, RequestPartsExt, response::Html, Router, routing::{get}};
use std::env;
use tower::make::Shared;
use tower::ServiceExt;
use axum::extract::{FromRequest, FromRequestParts};
use axum::response::{IntoResponse, Response};
use axum_extra::TypedHeader;
use headers::{
    authorization::{Bearer},
    Authorization,
};
use http::request::Parts;
use serde_json::json;
use tonic::{Request, Status};
use tonic::metadata::MetadataValue;


#[derive(Debug)]
pub enum AuthError {
    WrongCredentials,
    MissingCredentials,
    TokenCreation,
    InvalidToken,
}

pub struct AuthBearer(pub String);

#[async_trait]
impl<S> FromRequestParts<S> for AuthBearer
    where
        S: Send + Sync,
{
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Extract the token from the authorization header
        let bearer = parts
            .headers
            .get("Authorization");
        if bearer.is_none() {
            return Err(AuthError::MissingCredentials);
        }

        let bearer = bearer.unwrap().to_str().unwrap();
        let bearer = bearer.split("Bearer ");
        let bearer = bearer.last().unwrap();

        Ok(AuthBearer(bearer.to_string()))
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            AuthError::WrongCredentials => (StatusCode::UNAUTHORIZED, "Wrong credentials"),
            AuthError::MissingCredentials => (StatusCode::BAD_REQUEST, "Missing credentials"),
            AuthError::TokenCreation => (StatusCode::INTERNAL_SERVER_ERROR, "Token creation error"),
            AuthError::InvalidToken => (StatusCode::BAD_REQUEST, "Invalid token"),
        };
        let body = Json(json!({
            "error": error_message,
        }));
        (status, body).into_response()
    }
}

pub fn check_auth_grpc(req: Request<()>) -> Result<Request<()>, Status> {
    let grpc_token = env::var("GRPC_AUTH_TOKEN").expect("GRPC_AUTH_TOKEN is required");
    let token: MetadataValue<_> = format!("Bearer {}", grpc_token).parse().unwrap();

    match req.metadata().get("authorization") {
        Some(t) if token == t => Ok(req),
        _ => Err(Status::unauthenticated("No valid auth token")),
    }
}