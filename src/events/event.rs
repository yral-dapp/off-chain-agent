use std::{collections::HashMap, env};

use crate::{
    consts::BIGQUERY_INGESTION_URL,
    events::warehouse_events::{Empty, WarehouseEvent},
};
use log::{error, info};
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;
use yup_oauth2::ServiceAccountAuthenticator;

pub struct Event {
    pub event: WarehouseEvent,
}

impl Event {
    pub fn new(event: WarehouseEvent) -> Self {
        Self { event }
    }

    pub fn stream_to_bigquery(&self) {
        let event_str = self.event.event.clone();
        let params_str = self.event.params.clone();

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

            let res = stream_to_bigquery(data).await;
            if res.is_err() {
                error!("Error sending data to BigQuery: {}", res.err().unwrap());
            }
        });
    }

    pub fn upload_to_gcs(&self) {
        if self.event.event == "video_upload_successful" {
            let params: Value = serde_json::from_str(&self.event.params).expect("Invalid JSON");

            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(30)).await;

                let uid = params["video_id"].as_str().unwrap();
                let canister_id = params["canister_id"].as_str().unwrap();
                let post_id = params["post_id"].as_u64().unwrap();

                let res = upload_gcs(uid, canister_id, post_id).await;
                if res.is_err() {
                    log::error!("Error uploading video to GCS: {:?}", res.err());
                }
            });
        }
    }

    // pub fn update_watch_history(&self) {
    //     todo!()
    // }

    // pub fn update_success_history(&self) {
    //     todo!()
    // }
}

pub async fn upload_gcs(uid: &str, canister_id: &str, post_id: u64) -> Result<(), anyhow::Error> {
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
    res_obj.metadata = Some(hashmap);

    // update
    let _ = gcs_client.object().update(&res_obj).await?;

    Ok(())
}

pub async fn get_access_token() -> String {
    let sa_key_file = env::var("GOOGLE_SA_KEY").expect("GOOGLE_SA_KEY is required");

    // Load your service account key
    let sa_key = yup_oauth2::parse_service_account_key(sa_key_file).expect("GOOGLE_SA_KEY.json");

    let auth = ServiceAccountAuthenticator::builder(sa_key)
        .build()
        .await
        .unwrap();

    let scopes = &["https://www.googleapis.com/auth/bigquery.insertdata"];
    let token = auth.token(scopes).await.unwrap();

    match token.token() {
        Some(t) => t.to_string(),
        _ => panic!("No access token found"),
    }
}

async fn stream_to_bigquery(data: Value) -> Result<(), Box<dyn std::error::Error>> {
    let token = get_access_token().await;
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
