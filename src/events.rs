use std::collections::{HashMap, HashSet};
use std::env;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use candid::Deserialize;
use log::{error, info};
use reqwest::Client;
use serde::Serialize;
use serde_json::Value;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Request;
use yup_oauth2::ServiceAccountAuthenticator;

use warehouse_events::warehouse_events_server::WarehouseEvents;

use crate::consts::{
    BIGQUERY_INGESTION_URL, CLOUDFLARE_ACCOUNT_ID, ML_SERVER_URL, UPSTASH_VECTOR_REST_URL,
};
use crate::events::warehouse_events::{Empty, WarehouseEvent};
use crate::{AppError, AppState};

pub mod warehouse_events {
    tonic::include_proto!("warehouse_events");
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("warehouse_events_descriptor");
}

pub struct WarehouseEventsService {
    pub shared_state: Arc<AppState>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Event {
    event: String,
    params: Value,
    timestamp: String,
}

#[tonic::async_trait]
impl WarehouseEvents for WarehouseEventsService {
    async fn send_event(
        &self,
        request: tonic::Request<WarehouseEvent>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        // let shared_state = self.shared_state.clone();
        // let access_token = shared_state.google_sa_key_access_token.clone();

        let request = request.into_inner();

        let timestamp = chrono::Utc::now().to_rfc3339();

        let data = serde_json::json!({
            "kind": "bigquery#tableDataInsertAllRequest",
            "rows": [
                {
                    "json": {
                        "event": request.event,
                        "params": request.params,
                        "timestamp": timestamp,
                    }
                }
            ]
        });

        let res = stream_to_bigquery(data).await;
        if res.is_err() {
            error!("Error sending data to BigQuery: {}", res.err().unwrap());
            return Err(tonic::Status::new(
                tonic::Code::Unknown,
                "Error sending data to BigQuery",
            ));
        }

        if request.event == "video_upload_successful" {
            tokio::spawn(async move {
                let params: Value = serde_json::from_str(&request.params).expect("Invalid JSON");

                let res = process_upload_event(Event {
                    event: request.event,
                    params,
                    timestamp,
                })
                .await;
                if res.is_err() {
                    log::error!("Error processing upload event: {:?}", res.err());
                }
            });
        }
        Ok(tonic::Response::new(Empty {}))
    }
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

#[derive(Debug, Serialize, Deserialize)]
struct EmbeddingResponse {
    result: Vec<Vec<f64>>,
}

async fn process_upload_event(event: Event) -> Result<(), AppError> {
    let uid = event.params["video_id"].as_str().unwrap();
    let mut off_chain_agent_grpc_auth_token = env::var("ML_SERVER_JWT_TOKEN")?;
    // removing whitespaces and new lines for proper parsing
    off_chain_agent_grpc_auth_token.retain(|c| !c.is_whitespace());

    let op = || async {
        let channel = tonic::transport::Channel::from_static(ML_SERVER_URL)
            .connect()
            .await
            .expect("Failed to connect to ML server");

        let token: MetadataValue<_> = format!("Bearer {}", off_chain_agent_grpc_auth_token)
            .parse()
            .unwrap();
        let mut client = ml_server::ml_server_client::MlServerClient::with_interceptor(
            channel,
            move |mut req: Request<()>| {
                req.metadata_mut().insert("authorization", token.clone());
                Ok(req)
            },
        );
        let request = tonic::Request::new(ml_server::VideoEmbedRequest {
            video_id: uid.into(),
        });
        client.predict(request).await
    };

    // Retry mechanism
    let retries = 4;
    let min = Duration::from_secs(60);
    let max = Duration::from_secs(600);
    let backoff = exponential_backoff::Backoff::new(retries, min, max);

    let mut response: ml_server::VideoEmbedResponse = Default::default();
    for duration in &backoff {
        log::info!("Retrying after {:?}", duration);
        match op().await {
            Ok(s) => {
                response = s.into_inner();
                break;
            }
            Err(_) => tokio::time::sleep(duration).await,
        }
    }

    if response.result.is_empty() {
        return Err(anyhow::anyhow!("Failed to get response from ml_server").into());
    }

    let upstash_token = env::var("UPSTASH_VECTOR_READ_WRITE_TOKEN")?;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/upsert", UPSTASH_VECTOR_REST_URL))
        .header("Authorization", format!("Bearer {}", upstash_token))
        .json(&serde_json::json!({
            "id": uid,
            "vector": response.result
        }))
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(anyhow::anyhow!("Failed to upsert vector to upstash").into());
    }

    Ok(())
}

pub mod ml_server {
    tonic::include_proto!("ml_server");
}

pub async fn ml_server_predict(uid: &String) {
    let mut off_chain_agent_grpc_auth_token = env::var("ML_SERVER_JWT_TOKEN").unwrap();
    // removing whitespaces and new lines for proper parsing
    off_chain_agent_grpc_auth_token.retain(|c| !c.is_whitespace());

    let op = || async {
        let channel = match tonic::transport::Channel::from_static(ML_SERVER_URL)
            .connect()
            .await
        {
            Ok(channel) => channel,
            Err(e) => {
                return Err(tonic::Status::internal("Failed to connect to ML server"));
            }
        };

        let token: MetadataValue<_> = format!("Bearer {}", off_chain_agent_grpc_auth_token)
            .parse()
            .unwrap();
        let mut client = ml_server::ml_server_client::MlServerClient::with_interceptor(
            channel,
            move |mut req: Request<()>| {
                req.metadata_mut().insert("authorization", token.clone());
                Ok(req)
            },
        );
        let request = tonic::Request::new(ml_server::VideoEmbedRequest {
            video_id: uid.into(),
        });
        client.predict(request).await
    };

    // Retry mechanism
    let retries = 4;
    let min = Duration::from_secs(60);
    let max = Duration::from_secs(600);
    let backoff = exponential_backoff::Backoff::new(retries, min, max);

    let mut response: ml_server::VideoEmbedResponse = Default::default();
    for duration in &backoff {
        // log::info!("Retrying after p {:?} for {:?}", duration, uid);
        match op().await {
            Ok(s) => {
                response = s.into_inner();
                break;
            }
            Err(e) => {
                log::error!(
                    "Error while fetching response from ml_server for {:?}: {:?}",
                    uid,
                    e
                );
                tokio::time::sleep(duration).await
            }
        }
    }
    if response.result.is_empty() {
        log::error!("Failed to get response from ml_server for {:?}", uid);
        return;
    }

    let upstash_token = env::var("UPSTASH_VECTOR_READ_WRITE_TOKEN").unwrap();
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/upsert", UPSTASH_VECTOR_REST_URL))
        .header("Authorization", format!("Bearer {}", upstash_token))
        .json(&serde_json::json!({
            "id": uid,
            "vector": response.result
        }))
        .send()
        .await;
    if response.is_err() {
        log::error!("Failed to upsert vector to upstash");
    }
    if !response.unwrap().status().is_success() {
        log::error!("Failed to upsert vector to upstash");
    }
    // else {
    //     log::info!("Successfully upserted vector {:?}", uid);
    // }
}

pub async fn call_predict_v2(
    Query(params): Query<HashMap<String, String>>,
) -> Result<(), AppError> {
    let uid = params.get("uid").unwrap().clone();

    tokio::spawn(async move {
        ml_server_predict(&uid).await;
    });

    Ok(())
}

pub async fn call_predict() -> Result<(), AppError> {
    tokio::spawn(async move {
        ml_server_predict(&"ee1201fc2a6e45d9a981a3e484a7da0a".to_string()).await;
    });

    Ok(())
}

pub async fn test_uv() -> Result<(), AppError> {
    let upstash_token = env::var("UPSTASH_VECTOR_READ_WRITE_TOKEN")?;
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/range", UPSTASH_VECTOR_REST_URL))
        .header("Authorization", format!("Bearer {}", upstash_token))
        .json(&serde_json::json!({
            "cursor": "0",
            "limit": 30,
            "includeMetadata": true
        }))
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!("Failed to get range from upstash").into());
    }

    log::info!("RESPONSE={:?}", response.text().await?);

    Ok(())
}

pub async fn test_uv_info() -> Result<(), AppError> {
    let upstash_token = env::var("UPSTASH_VECTOR_READ_WRITE_TOKEN")?;
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/info", UPSTASH_VECTOR_REST_URL))
        .header("Authorization", format!("Bearer {}", upstash_token))
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!("Failed to get range from upstash").into());
    }

    log::info!("RESPONSE={:?}", response.text().await?);

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct CFStreamResult {
    result: Vec<CFStream>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct CFStream {
    uid: String,
    created: String,
}

pub async fn test_cloudflare(
    Query(params): Query<HashMap<String, String>>,
) -> Result<(), AppError> {
    // Get Request to https://api.cloudflare.com/client/v4/accounts/{account_id}/stream
    // Query param start 2021-05-03T00:00:00Z
    let startdate = params.get("startdate").unwrap().clone();
    let thresh = params.get("thresh").unwrap().parse::<usize>().unwrap();

    let url = format!(
        "https://api.cloudflare.com/client/v4/accounts/{}/stream",
        CLOUDFLARE_ACCOUNT_ID
    );
    let bearer_token = env::var("CLOUDFLARE_STREAM_READ_AND_LIST_ACCESS_TOKEN")?;

    let client = reqwest::Client::new();
    let mut num_vids = 0;
    let mut start_time = startdate;
    let mut cnt = 0;
    let mut hashset: HashSet<String> = HashSet::new();

    loop {
        let response = client
            .get(&url)
            .bearer_auth(&bearer_token)
            .query(&[("asc", "true"), ("start", &start_time)])
            .send()
            .await?;
        // log::info!("Response: {:?}", response);
        if response.status() != 200 {
            log::error!(
                "Failed to get response from Cloudflare: {:?}",
                response.text().await?
            );
            return Err(anyhow::anyhow!("Failed to get response from Cloudflare").into());
        }

        let body = response.text().await?;
        let result: CFStreamResult = serde_json::from_str(&body)?;
        let mut result_vec = result.result.clone();

        // add uids to hashset
        for r in &result_vec {
            hashset.insert(r.uid.clone());

            if hashset.len() >= thresh {
                break;
            }
        }

        if cnt > 0 {
            result_vec.remove(0);
        }

        num_vids += result_vec.len();
        if result_vec.len() == 0 {
            break;
        }
        let last = &result.result[result.result.len() - 1];
        start_time = last.created.clone();
        cnt += 1;

        if cnt > 10000 {
            log::info!("Breaking after 10000 iterations");
            break;
        }

        if hashset.len() >= thresh {
            // hashset retain only 100 elements
            log::error!("Last: {:?}", last);
            break;
        }
    }

    log::info!("Total number of videos: {}", num_vids);
    log::info!("Total number of videos in hashset: {}", hashset.len());
    // log::info!("Hashset: {:?}", hashset);

    // hit the endpoint for all uids of hashset
    // GET https://icp-off-chain-agent.fly.dev/call_predict_v2?uid=ee1201fc2a6e45d9a981a3e484a7da0a

    let mut cnt = 0;
    for uid in hashset {
        let response = match client
            .get(format!(
                "https://icp-off-chain-agent.fly.dev/call_predict_v2?uid={}",
                uid
            ))
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                log::error!("Failed to get response from off_chain_agent: {:?}", e);
                continue;
            }
        };
        if response.status() != 200 {
            log::error!(
                "Failed to get response from off_chain_agent: {:?}",
                response.text().await
            );
        }

        // let body = response.text().await?;
        // log::info!("Response: {:?}", body);
        cnt += 1;
    }

    Ok(())
}

pub async fn test_cloudflare_v2(
    Query(params): Query<HashMap<String, String>>,
) -> Result<(), AppError> {
    // Get Request to https://api.cloudflare.com/client/v4/accounts/{account_id}/stream
    // Query param start 2021-05-03T00:00:00Z
    let startdate = params.get("startdate").unwrap().clone();
    let thresh = params.get("thresh").unwrap().parse::<usize>().unwrap();

    let url = format!(
        "https://api.cloudflare.com/client/v4/accounts/{}/stream",
        CLOUDFLARE_ACCOUNT_ID
    );
    let bearer_token = env::var("CLOUDFLARE_STREAM_READ_AND_LIST_ACCESS_TOKEN")?;

    let client = reqwest::Client::new();
    let mut num_vids = 0;
    let mut start_time = startdate;
    let mut cnt = 0;
    let mut hashset: HashSet<String> = HashSet::new();

    loop {
        let response = client
            .get(&url)
            .bearer_auth(&bearer_token)
            .query(&[("asc", "true"), ("start", &start_time)])
            .send()
            .await?;
        // log::info!("Response: {:?}", response);
        if response.status() != 200 {
            log::error!(
                "Failed to get response from Cloudflare: {:?}",
                response.text().await?
            );
            return Err(anyhow::anyhow!("Failed to get response from Cloudflare").into());
        }

        let body = response.text().await?;
        let result: CFStreamResult = serde_json::from_str(&body)?;
        let mut result_vec = result.result.clone();

        // add uids to hashset
        for r in &result_vec {
            hashset.insert(r.uid.clone());

            if hashset.len() >= thresh {
                log::error!("Last above: {:?}", r);
                break;
            }
        }

        if cnt > 0 {
            result_vec.remove(0);
        }

        num_vids += result_vec.len();
        if result_vec.len() == 0 {
            break;
        }
        let last = &result.result[result.result.len() - 1];
        start_time = last.created.clone();
        cnt += 1;

        if cnt > 10000 {
            log::info!("Breaking after 10000 iterations");
            break;
        }

        if hashset.len() >= thresh {
            // hashset retain only 100 elements
            log::error!("Last: {:?}", last);
            break;
        }
    }

    log::info!("Total number of videos: {}", num_vids);
    log::info!("Total number of videos in hashset: {}", hashset.len());
    // log::info!("Hashset: {:?}", hashset);

    Ok(())
}

pub async fn get_cf_info(Query(params): Query<HashMap<String, String>>) -> Result<(), AppError> {
    let uid = params.get("uid").unwrap().clone();
    let bearer_token = env::var("CLOUDFLARE_STREAM_READ_AND_LIST_ACCESS_TOKEN")?;

    // CALL GET https://api.cloudflare.com/client/v4/accounts/{account_id}/stream/{identifier}
    let url = format!(
        "https://api.cloudflare.com/client/v4/accounts/{}/stream/{}",
        CLOUDFLARE_ACCOUNT_ID, uid
    );

    let client = reqwest::Client::new();
    let response = client.get(&url).bearer_auth(&bearer_token).send().await?;

    if response.status() != 200 {
        log::error!(
            "Failed to get response from Cloudflare: {:?}",
            response.text().await?
        );
        return Err(anyhow::anyhow!("Failed to get response from Cloudflare").into());
    }

    let body = response.text().await?;
    log::info!("Response: {:?}", body);

    Ok(())
}
