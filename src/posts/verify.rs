use std::sync::Arc;

use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
    Json,
};
use candid::Principal;
use ic_agent::{identity::DelegatedIdentity, Identity};
use serde::{Deserialize, Serialize};

use crate::app_state::AppState;

use super::PostRequest;

#[derive(Clone, Serialize, Deserialize)]
pub struct VerifiedPostRequest<T> {
    pub user_principal: Principal,
    pub user_canister: Principal,
    pub request: PostRequest<T>,
}

pub async fn verify_post_request<T>(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode>
where
    T: for<'de> Deserialize<'de> + Serialize + Clone + Send + Sync + 'static,
{
    log::info!("verify_post_request");

    // Extract the JSON body
    let (parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(_) => return Err(StatusCode::BAD_REQUEST),
    };

    // Parse the JSON
    let post_request: PostRequest<T> = match serde_json::from_slice(&bytes) {
        Ok(req) => req,
        Err(_) => return Err(StatusCode::BAD_REQUEST),
    };

    // Convert delegated identity wire to actual identity
    let identity: DelegatedIdentity =
        match DelegatedIdentity::try_from(post_request.delegated_identity_wire.clone()) {
            Ok(identity) => identity,
            Err(_) => return Err(StatusCode::UNAUTHORIZED),
        };

    // Get the user principal from the identity
    let user_principal = match identity.sender() {
        Ok(principal) => principal,
        Err(_) => return Err(StatusCode::UNAUTHORIZED),
    };

    // Get the user's canister ID from metadata
    let user_canister = match state
        .get_individual_canister_by_user_principal(user_principal)
        .await
    {
        Ok(canister_id) => canister_id,
        Err(_) => return Err(StatusCode::UNAUTHORIZED),
    };

    // Create a verified request with all the necessary context
    let verified_request = VerifiedPostRequest {
        user_principal,
        user_canister,
        request: post_request,
    };

    // let mut request = Request::from_parts(parts, axum::body::Body::from(bytes));
    // request.extensions_mut().insert(verified_request);
    // convert verified_request to request body
    let request_body = serde_json::to_string(&verified_request).unwrap();
    let request = Request::from_parts(parts, axum::body::Body::from(request_body));
    // request.extensions_mut().insert(verified_request);

    log::info!("verify_post_request complete");

    // Pass the request to the next handler
    Ok(next.run(request).await)
}
