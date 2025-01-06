use std::sync::Arc;

use crate::{
    app_state::AppState,
    canister::mlfeed_cache::off_chain::{Empty, UpdateMlFeedCacheRequest},
    consts::CLOUDFLARE_ML_FEED_CACHE_WORKER_URL,
    AppError,
};
use axum::extract::State;
use candid::Principal;
use google_cloud_bigquery::http::{job::query::QueryRequest, tabledata::list::Value};
use http::StatusCode;
use off_chain::{off_chain_canister_server::OffChainCanister, MlFeedCacheItem};
use serde::{Deserialize, Serialize};
use yral_canisters_client::individual_user_template::Result25;

pub mod off_chain {
    tonic::include_proto!("offchain_canister");
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("offchain_canister_descriptor");
}

pub struct OffChainCanisterService {
    pub shared_state: Arc<AppState>,
}

#[tonic::async_trait]
impl OffChainCanister for OffChainCanisterService {
    async fn update_ml_feed_cache(
        &self,
        request: tonic::Request<UpdateMlFeedCacheRequest>,
    ) -> core::result::Result<tonic::Response<Empty>, tonic::Status> {
        let request = request.into_inner();

        let state = self.shared_state.clone();

        let canister_principal = Principal::from_text(request.user_canister_id)
            .map_err(|_| tonic::Status::invalid_argument("Invalid canister principal"))?;
        let user_canister = state.individual_user(canister_principal);

        let arg0 = request.items.into_iter().map(|x| x.into()).collect();

        let res = user_canister
            .update_ml_feed_cache(arg0)
            .await
            .map_err(|e| {
                tonic::Status::internal(format!("Error updating ml feed cache: {:?}", e))
            })?;

        if let Result25::Err(err) = res {
            log::error!("Error updating ml feed cache: {:?}", err);
            return Err(tonic::Status::internal(format!(
                "Error updating ml feed cache: {:?}",
                err
            )));
        }

        Ok(tonic::Response::new(Empty {}))
    }
}

impl From<MlFeedCacheItem> for yral_canisters_client::individual_user_template::MlFeedCacheItem {
    fn from(item: MlFeedCacheItem) -> Self {
        yral_canisters_client::individual_user_template::MlFeedCacheItem {
            post_id: item.post_id,
            canister_id: Principal::from_text(item.canister_id).unwrap(),
            video_id: item.video_id,
            creator_principal_id: if item.creator_principal_id.is_empty() {
                None
            } else {
                Some(Principal::from_text(item.creator_principal_id).unwrap())
            },
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CustomMlFeedCacheItem {
    post_id: u64,
    canister_id: String,
    video_id: String,
    creator_principal_id: String,
}

#[cfg(not(feature = "local-bin"))]
pub async fn update_ml_feed_cache(State(state): State<Arc<AppState>>) -> Result<(), AppError> {
    let bigquery_client = state.bigquery_client.clone();
    let request = QueryRequest {
        query: "SELECT uri, (SELECT value FROM UNNEST(metadata) WHERE name = 'timestamp') AS timestamp, (SELECT value FROM UNNEST(metadata) WHERE name = 'canister_id') AS canister_id, (SELECT value FROM UNNEST(metadata) WHERE name = 'post_id') AS post_id, is_nsfw FROM `hot-or-not-feed-intelligence.yral_ds.video_embeddings` WHERE is_nsfw = false GROUP BY 1, 2, 3, 4, 5 ORDER BY timestamp DESC LIMIT 50".to_string(),
        ..Default::default()
    };

    let rs = bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await?;

    let mut offchain_items = Vec::new();
    for row in rs.rows.unwrap_or_default() {
        let mut canister_id_val = "".to_string();
        if let Value::String(canister_id) = &row.f[2].v {
            canister_id_val = canister_id.clone();
        }

        let mut post_id_val = "".to_string();
        if let Value::String(post_id) = &row.f[3].v {
            post_id_val = post_id.clone();
        }

        offchain_items.push(CustomMlFeedCacheItem {
            post_id: post_id_val.parse().unwrap(),
            canister_id: canister_id_val,
            video_id: "".to_string(),
            creator_principal_id: "".to_string(),
        });
    }

    let cf_worker_url = CLOUDFLARE_ML_FEED_CACHE_WORKER_URL;

    // call POST /feed-cache/<CANISTER_ID>
    let url = format!("{}/feed-cache/{}", cf_worker_url, "global-feed");
    let client = reqwest::Client::new();
    let response = client.post(url).json(&offchain_items).send().await;

    match response {
        Ok(_) => (),
        Err(e) => println!("Failed to get update_ml_feed_cache response: {}", e),
    }

    Ok(())
}

#[cfg(feature = "local-bin")]
pub async fn update_ml_feed_cache(State(state): State<Arc<AppState>>) -> Result<(), AppError> {
    Ok(())
}

#[cfg(not(feature = "local-bin"))]
pub async fn update_ml_feed_cache_nsfw(State(state): State<Arc<AppState>>) -> Result<(), AppError> {
    let bigquery_client = state.bigquery_client.clone();
    let request = QueryRequest {
        query: "SELECT uri, (SELECT value FROM UNNEST(metadata) WHERE name = 'timestamp') AS timestamp, (SELECT value FROM UNNEST(metadata) WHERE name = 'canister_id') AS canister_id, (SELECT value FROM UNNEST(metadata) WHERE name = 'post_id') AS post_id, is_nsfw FROM `hot-or-not-feed-intelligence.yral_ds.video_embeddings` WHERE is_nsfw = true GROUP BY 1, 2, 3, 4, 5 ORDER BY timestamp DESC LIMIT 50".to_string(),
        ..Default::default()
    };

    let rs = bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await?;

    let mut offchain_items = Vec::new();
    for row in rs.rows.unwrap_or_default() {
        let mut canister_id_val = "".to_string();
        if let Value::String(canister_id) = &row.f[2].v {
            canister_id_val = canister_id.clone();
        }

        let mut post_id_val = "".to_string();
        if let Value::String(post_id) = &row.f[3].v {
            post_id_val = post_id.clone();
        }

        offchain_items.push(CustomMlFeedCacheItem {
            post_id: post_id_val.parse().unwrap(),
            canister_id: canister_id_val,
            video_id: "".to_string(),
            creator_principal_id: "".to_string(),
        });
    }

    let cf_worker_url = CLOUDFLARE_ML_FEED_CACHE_WORKER_URL;

    // call POST /feed-cache/<CANISTER_ID>
    let url = format!("{}/feed-cache/{}", cf_worker_url, "global-feed-nsfw");
    let client = reqwest::Client::new();
    let response = client.post(url).json(&offchain_items).send().await;

    match response {
        Ok(_) => (),
        Err(e) => println!("Failed to get update_ml_feed_cache response: {}", e),
    }

    Ok(())
}

#[cfg(feature = "local-bin")]
pub async fn update_ml_feed_cache_nsfw(State(state): State<Arc<AppState>>) -> Result<(), AppError> {
    Ok(())
}
