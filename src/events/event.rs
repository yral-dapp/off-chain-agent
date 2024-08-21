use std::{collections::HashMap, env, time::UNIX_EPOCH};

use crate::{
    app_state::AppState,
    canister::individual_user_template::{
        Result10, Result12, SuccessHistoryItemV1, SystemTime, WatchHistoryItem,
    },
    consts::BIGQUERY_INGESTION_URL,
    events::warehouse_events::{Empty, WarehouseEvent},
};
use candid::Principal;
use ic_agent::Agent;
use log::{error, info};
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;

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

    pub fn upload_to_gcs(&self) {
        if self.event.event == "video_upload_successful" {
            let params: Value = serde_json::from_str(&self.event.params).expect("Invalid JSON");

            tokio::spawn(async move {
                let timestamp = chrono::Utc::now().to_rfc3339();
                tokio::time::sleep(Duration::from_secs(60)).await;

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
                        return;
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

        let mut item_type = self.event.event.clone();

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
                return;
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
