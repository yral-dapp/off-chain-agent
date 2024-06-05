use crate::consts::SHARED_SECRET;
use crate::{app_state::AppState, error::AppError};
use anyhow::{anyhow, Context, Result};
use axum::{debug_handler, response::IntoResponse};
use axum::{extract::State, http::HeaderMap, response::Response};
use http::StatusCode;
use redis::AsyncCommands;

use candid::Principal;
// use ic_agent::agent::http_transport::reqwest_transport::reqwest::Request;
use serde::Deserialize;
use signature::{verify_signature, WebhookSignature};
use std::sync::Arc;

mod signature;

#[derive(Deserialize, Debug)]
struct WebhookPayload {
    uid: String,
    status: Status,
    meta: Meta,
}

#[derive(Deserialize, Debug)]
struct Status {
    state: String,
}

#[derive(Deserialize, Debug)]
struct Meta {
    creator: String,
    #[serde(rename = "fileName")]
    file_name: String,
    post_id: String,
}

#[debug_handler]
pub async fn cf_stream_webhook_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: String,
) -> Result<Response, AppError> {
    // verify the webhook first.
    let secret: String = SHARED_SECRET.clone();

    // Get the Webhook-Signature header
    let signature_header = headers
        .get("Webhook-Signature")
        .context("no header Webhook-Signature")?;

    // let signature_header = header_value;
    // Extract the value from the HeaderValue
    let signature = signature_header
        .to_str()
        .map_err(|_err| anyhow!("Invalid Webhook-Signature header"))?;

    let webhook_signature: WebhookSignature = signature.parse().unwrap();

    let verified = verify_signature(secret.as_str(), &webhook_signature, body.as_str());

    if !verified {
        return Err(anyhow!("Unauthorized"))?;
    }

    // if webhook is verified, continue
    let yral_metadata_client = state.yral_metadata_client.clone();
    let mut redis_conn = state.redis.get().await?.clone();

    let payload: WebhookPayload = serde_json::from_str(&body).unwrap();

    // set the entry to true - indicating the webhook was received.
    redis_conn.set(payload.uid, true).await?;

    let post_id: u64 = payload
        .meta
        .post_id
        .parse()
        .context("Failed to get user_metadata from yral_metadata_client")?;

    if let Ok(user_principal) = payload.meta.creator.parse::<Principal>() {
        let meta = yral_metadata_client
            .get_user_metadata(user_principal)
            .await
            .context("yral_metadata - could not connect to client")?
            .context("yral_metadata has value None")?;

        let user_canister_id = state
            .get_individual_canister_by_user_principal(meta.user_canister_id)
            .await
            .context("Failed to get user_canister_id")?;

        let user = state.individual_user(user_canister_id);
        let _ = user
            .update_post_as_ready_to_view(post_id)
            .await
            .context("Failed to update post status")?;
        println!("payload {meta:?}");
    }

    Ok((StatusCode::OK).into_response())
}
