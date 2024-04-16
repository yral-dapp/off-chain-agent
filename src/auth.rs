use std::env;

use axum::extract::FromRequestParts;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{async_trait, Json};
use http::request::Parts;
use serde_json::json;
use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

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
        let bearer = parts.headers.get("Authorization");
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
    let token = format!("Bearer {}", grpc_token);

    // test auth
    // length of grpc-token
    println!("grpc_token len: {}", grpc_token.len());
    println!("grpc_token start: {}", &grpc_token[..10]);
    println!("grpc_token end: {}", &grpc_token[grpc_token.len() - 10..]);

    let req_token = req
        .metadata()
        .get("authorization")
        .map(|t| t.to_str().unwrap());
    if let Some(t) = req_token {
        println!("req_token len: {}", t.len());
        println!("req_token start: {}", &t[..10]);
        println!("req_token end: {}", &t[t.len() - 10..]);
    }

    Ok(req)

    // match req
    //     .metadata()
    //     .get("authorization")
    //     .map(|t| t.to_str().unwrap())
    // {
    //     Some(t) if token == t => Ok(req),
    //     _ => Err(Status::unauthenticated("No valid auth token")),
    // }
}
