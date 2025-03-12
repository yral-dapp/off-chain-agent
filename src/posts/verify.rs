use std::{error::Error, sync::Arc};

use axum::{
    extract::{FromRequestParts, Request, State},
    http::{request::Parts, StatusCode},
    middleware::Next,
    response::Response,
    Json,
};
use candid::Principal;
use ic_agent::{identity::DelegatedIdentity, Agent, Identity};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use yral_canisters_client::individual_user_template::IndividualUserTemplate;

use crate::{app_state::AppState, canister::upload_user_video::DelegatedIdentityWire};

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

    let mut request = Request::from_parts(parts, axum::body::Body::from(bytes));
    request.extensions_mut().insert(verified_request);

    // Pass the request to the next handler
    Ok(next.run(request).await)
}
