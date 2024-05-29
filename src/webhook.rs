

// use axum::debug_handler;
use anyhow::{ Context, Result};
use axum::{extract::Json, extract::State};
use serde_json::Value;
use serde::Deserialize;
use std::sync::Arc;
use candid::Principal;

use crate::error::AppError;
use crate::app_state::AppState;


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

// #[debug_handler]
pub async fn cf_stream_webhook_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<Value>,
) -> Result<(), AppError> {
    let yral_metadata_client = state.yral_metadata_client.clone();

    let payload: WebhookPayload = serde_json::from_value(payload).unwrap();

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

    Ok(())
}
