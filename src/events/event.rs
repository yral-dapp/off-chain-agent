use std::{collections::HashMap, env, sync::Arc, time::UNIX_EPOCH};

use crate::{
    app_state::AppState,
    consts::{BIGQUERY_INGESTION_URL, CLOUDFLARE_ACCOUNT_ID},
    events::{queries::get_icpump_insert_query_created_at, warehouse_events::WarehouseEvent},
    utils::cf_images::upload_base64_image,
    AppError,
};
use axum::extract::State;
use candid::Principal;
use chrono::{DateTime, Utc};
use firestore::{errors::FirestoreError, struct_path::path, FirestoreQueryDirection};
use futures::{stream::BoxStream, StreamExt};
use google_cloud_bigquery::http::{
    job::query::QueryRequest,
    tabledata::insert_all::{InsertAllRequest, Row},
};
use http::HeaderMap;
use log::{error, info};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;
use yral_canisters_client::individual_user_template::{
    SuccessHistoryItemV1, SystemTime, WatchHistoryItem,
};

use super::queries::get_icpump_insert_query;

#[derive(Debug, Clone, Deserialize, Serialize)]
struct TokenListItem {
    user_id: String,
    name: String,
    token_name: String,
    token_symbol: String,
    logo: String,
    description: String,
    #[serde(with = "firestore::serialize_as_timestamp")]
    created_at: DateTime<Utc>,
    link: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ICPumpTokenMetadata {
    pub canister_id: String,
    pub description: String,
    pub host: String,
    pub link: String,
    pub logo: String,
    pub token_name: String,
    pub token_symbol: String,
    pub user_id: String,
    pub created_at: String,
}

pub struct Event {
    pub event: WarehouseEvent,
}

impl Event {
    pub fn new(event: WarehouseEvent) -> Self {
        Self { event }
    }

    pub fn stream_to_bigquery(&self, app_state: &AppState) {
        let event_str = self.event.event.clone();
        let params_str = self.event.params.clone();
        let app_state = app_state.clone();

        tokio::spawn(async move {
            let timestamp = chrono::Utc::now().to_rfc3339();

            let data = serde_json::json!({
                "kind": "bigquery#tableDataInsertAllRequest",
                "rows": [
                    {
                        "json": {
                            "event": event_str,
                            "params": params_str,
                            "timestamp": timestamp,
                        }
                    }
                ]
            });

            let res = stream_to_bigquery(&app_state, data).await;
            if res.is_err() {
                error!("Error sending data to BigQuery: {}", res.err().unwrap());
            }
        });
    }

    pub fn stream_to_bigquery_token_metadata(&self, app_state: &AppState) {
        if self.event.event == "token_creation_completed" {
            let params: Value = serde_json::from_str(&self.event.params).expect("Invalid JSON");
            let app_state = app_state.clone();

            tokio::spawn(async move {
                let timestamp = chrono::Utc::now().to_rfc3339();

                let data = ICPumpTokenMetadata {
                    canister_id: params["canister_id"].as_str().unwrap().to_string(),
                    description: params["description"].as_str().unwrap().to_string(),
                    host: params["host"].as_str().unwrap().to_string(),
                    link: params["link"].as_str().unwrap().to_string(),
                    logo: params["logo"].as_str().unwrap().to_string(),
                    token_name: params["token_name"].as_str().unwrap().to_string(),
                    token_symbol: params["token_symbol"].as_str().unwrap().to_string(),
                    user_id: params["user_id"].as_str().unwrap().to_string(),
                    created_at: timestamp,
                };

                let res = stream_to_bigquery_token_metadata_impl_v2(&app_state, data).await;
                if res.is_err() {
                    error!(
                        "stream_to_bigquery_token_metadata: Error sending data to BigQuery: {}",
                        res.err().unwrap()
                    );
                }
            });
        }
    }

    pub fn upload_to_gcs(&self) {
        if self.event.event == "video_upload_successful" {
            let params: Value = serde_json::from_str(&self.event.params).expect("Invalid JSON");

            tokio::spawn(async move {
                let timestamp = chrono::Utc::now().to_rfc3339();
                tokio::time::sleep(Duration::from_secs(60 * 10)).await;

                let uid = params["video_id"].as_str().unwrap();
                let canister_id = params["canister_id"].as_str().unwrap();
                let post_id = params["post_id"].as_u64().unwrap();

                let res = upload_gcs(uid, canister_id, post_id, timestamp).await;
                if res.is_err() {
                    log::error!("Error uploading video to GCS: {:?}", res.err());
                }
            });
        }
    }

    pub fn update_watch_history(&self, app_state: &AppState) {
        if self.event.event == "video_duration_watched" {
            let params: Value = serde_json::from_str(&self.event.params).expect("Invalid JSON");
            let app_state = app_state.clone();

            tokio::spawn(async move {
                let percent_watched = params["percentage_watched"].as_f64().unwrap();

                if percent_watched >= 90.0 {
                    let user_canister_id = params["canister_id"].as_str().unwrap();
                    let user_canister_id_principal =
                        Principal::from_text(user_canister_id).unwrap();
                    let user_canister = app_state.individual_user(user_canister_id_principal);

                    let publisher_canister_id = params["publisher_canister_id"].as_str().unwrap();
                    let publisher_canister_id_principal =
                        Principal::from_text(publisher_canister_id).unwrap();

                    let watch_history_item = WatchHistoryItem {
                        post_id: params["post_id"].as_u64().unwrap(),
                        viewed_at: system_time_to_custom(std::time::SystemTime::now()),
                        percentage_watched: percent_watched as f32,
                        publisher_canister_id: publisher_canister_id_principal,
                        cf_video_id: params["video_id"].as_str().unwrap().to_string(),
                    };

                    let res = user_canister.update_watch_history(watch_history_item).await;
                    if res.is_err() {
                        log::error!("Error updating watch history: {:?}", res.err());
                    }

                    // // test if the watch history is updated
                    // let watch_history = match user_canister.get_watch_history().await {
                    //     Ok(watch_history) => match watch_history {
                    //         Result12::Ok(watch_history) => watch_history,
                    //         Result12::Err(e) => {
                    //             log::error!("1.Error getting watch history: {:?}", e);
                    //             return;
                    //         }
                    //     },
                    //     Err(e) => {
                    //         log::error!("2. Error getting watch history: {:?}", e);
                    //         return;
                    //     }
                    // };

                    // for item in watch_history {
                    //     log::info!(
                    //         "Watch history item: {:?} {:?} {:?}",
                    //         item.cf_video_id,
                    //         item.post_id,
                    //         item.publisher_canister_id
                    //     );
                    // }
                }
            });
        }
    }

    pub fn update_success_history(&self, app_state: &AppState) {
        let params: Value = serde_json::from_str(&self.event.params).expect("Invalid JSON");
        let app_state = app_state.clone();

        let mut percent_watched = 0.0;

        if self.event.event != "video_duration_watched" && self.event.event != "like_video" {
            return;
        }
        if self.event.event == "video_duration_watched" {
            percent_watched = params["percentage_watched"].as_f64().unwrap();
            if percent_watched < 95.0 {
                return;
            }
        }

        let item_type = self.event.event.clone();

        tokio::spawn(async move {
            let user_canister_id = params["canister_id"].as_str().unwrap();
            let user_canister_id_principal = Principal::from_text(user_canister_id).unwrap();
            let user_canister = app_state.individual_user(user_canister_id_principal);

            let publisher_canister_id = params["publisher_canister_id"].as_str().unwrap();
            let publisher_canister_id_principal =
                Principal::from_text(publisher_canister_id).unwrap();

            let success_history_item = SuccessHistoryItemV1 {
                post_id: params["post_id"].as_u64().unwrap(),
                interacted_at: system_time_to_custom(std::time::SystemTime::now()),
                publisher_canister_id: publisher_canister_id_principal,
                cf_video_id: params["video_id"].as_str().unwrap().to_string(),
                percentage_watched: percent_watched as f32,
                item_type,
            };

            let res = user_canister
                .update_success_history(success_history_item)
                .await;
            if res.is_err() {
                log::error!("Error updating success history: {:?}", res.err());
            }

            // test if the success history is updated
            // let success_history = match user_canister.get_success_history().await {
            //     Ok(success_history) => match success_history {
            //         Result10::Ok(success_history) => success_history,
            //         Result10::Err(e) => {
            //             log::error!("1.Error getting success history: {:?}", e);
            //             return;
            //         }
            //     },
            //     Err(e) => {
            //         log::error!("2. Error getting success history: {:?}", e);
            //         return;
            //     }
            // };

            // for item in success_history {
            //     log::info!(
            //         "Success history item: {:?} {:?} {:?}",
            //         item.cf_video_id,
            //         item.post_id,
            //         item.publisher_canister_id
            //     );
            // }
        });
    }

    pub fn stream_to_firestore(&self, app_state: &AppState) {
        if self.event.event == "token_creation_completed" {
            let app_state = app_state.clone();
            let params: Value = serde_json::from_str(&self.event.params).expect("Invalid JSON");

            tokio::spawn(async move {
                let data = TokenListItem {
                    user_id: params["user_id"].as_str().unwrap().to_string(),
                    name: params["name"].as_str().unwrap().to_string(),
                    token_name: params["token_name"].as_str().unwrap().to_string(),
                    token_symbol: params["token_symbol"].as_str().unwrap().to_string(),
                    logo: params["logo"].as_str().unwrap().to_string(),
                    description: params["description"].as_str().unwrap().to_string(),
                    created_at: Utc::now(),
                    link: params["link"].as_str().unwrap().to_string(),
                };

                // link is in the format token/info/NEW_ID/USER_PRICIPAL
                let parts: Vec<&str> = data.link.split('/').collect();
                let document_id = parts[2]; // Get the NEW_ID part

                let db = app_state.firestoredb.clone();

                let res: Result<TokenListItem, FirestoreError> = db
                    .fluent()
                    .insert()
                    .into("tokens-list")
                    .document_id(document_id)
                    .object(&data)
                    .execute()
                    .await;
                if res.is_err() {
                    log::error!("Error uploading to Firestore : {:?}", res.err());
                }
            });
        }
    }
}

pub async fn upload_gcs(
    uid: &str,
    canister_id: &str,
    post_id: u64,
    timestamp_str: String,
) -> Result<(), anyhow::Error> {
    let url = format!(
        "https://customer-2p3jflss4r4hmpnz.cloudflarestream.com/{}/downloads/default.mp4",
        uid
    );
    let name = format!("{}.mp4", uid);

    let file = reqwest::Client::new()
        .get(&url)
        .send()
        .await?
        .bytes_stream();

    // write to GCS
    let gcs_client = cloud_storage::Client::default();
    let mut res_obj = gcs_client
        .object()
        .create_streamed("yral-videos", file, None, &name, "video/mp4")
        .await?;

    let mut hashmap = HashMap::new();
    hashmap.insert("canister_id".to_string(), canister_id.to_string());
    hashmap.insert("post_id".to_string(), post_id.to_string());
    hashmap.insert("timestamp".to_string(), timestamp_str);
    res_obj.metadata = Some(hashmap);

    // update
    let _ = gcs_client.object().update(&res_obj).await?;

    Ok(())
}

async fn stream_to_bigquery(
    app_state: &AppState,
    data: Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let token = app_state
        .get_access_token(&["https://www.googleapis.com/auth/bigquery.insertdata"])
        .await;
    let client = Client::new();
    let request_url = BIGQUERY_INGESTION_URL.to_string();
    let response = client
        .post(request_url)
        .bearer_auth(token)
        .json(&data)
        .send()
        .await?;

    match response.status().is_success() {
        true => Ok(()),
        false => Err(format!("Failed to stream data - {:?}", response.text().await?).into()),
    }
}

fn system_time_to_custom(time: std::time::SystemTime) -> SystemTime {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");

    SystemTime {
        nanos_since_epoch: duration.subsec_nanos(),
        secs_since_epoch: duration.as_secs(),
    }
}

pub async fn stream_to_bigquery_token_metadata_impl_v2(
    app_state: &AppState,
    data: ICPumpTokenMetadata,
) -> Result<(), anyhow::Error> {
    // Upload image to Cloudflare Images and get link

    let link = data.link.clone();
    let key_id = link.split("/").collect::<Vec<&str>>();
    let root_id = key_id[3];

    let base64_image_str = data.logo.clone();
    let base64_image_without_prefix = base64_image_str.replace("data:image/png;base64,", "");

    let cf_images_api_token = env::var("CF_IMAGES_API_TOKEN")?;

    let upload_res = upload_base64_image(
        CLOUDFLARE_ACCOUNT_ID,
        cf_images_api_token.as_str(),
        base64_image_without_prefix.as_str(),
        root_id,
    )
    .await?;

    let logo_link = upload_res.result.variants[0].clone();

    // Stream to BigQuery

    let bq_client = app_state.bigquery_client.clone();

    let query_str = get_icpump_insert_query(
        data.canister_id.clone(),
        data.description.clone(),
        data.host.clone(),
        data.link.clone(),
        logo_link,
        data.token_name.clone(),
        data.token_symbol.clone(),
        data.user_id.clone(),
    );

    let request = QueryRequest {
        query: query_str.to_string(),
        ..Default::default()
    };

    match bq_client
        .query::<google_cloud_bigquery::query::row::Row>("hot-or-not-feed-intelligence", request)
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => {
            log::error!("Error streaming to BigQuery: {:?}", e);
            Err(anyhow::anyhow!("Error streaming to BigQuery"))
        }
    }
}
