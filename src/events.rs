use std::collections::HashSet;
use std::env;
use std::sync::Arc;

use axum::response::Html;
use candid::Deserialize;
use log::{error, info};
use reqwest::Client;
use s3::creds::time::serde::timestamp;
use serde::Serialize;
use serde_json::Value;
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::Request;
use yup_oauth2::ServiceAccountAuthenticator;

use warehouse_events::warehouse_events_server::WarehouseEvents;

use crate::auth::AuthBearer;
use crate::consts::{
    BIGQUERY_INGESTION_URL, CLOUDFLARE_ACCOUNT_ID, ML_SERVER_URL, UPSTASH_VECTOR_REST_TOKEN,
    UPSTASH_VECTOR_REST_URL,
};
use crate::events::warehouse_events::{Empty, WarehouseEvent};
use crate::{AppError, AppState};

pub mod warehouse_events {
    tonic::include_proto!("warehouse_events");
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("warehouse_events_descriptor");
}

pub struct WarehouseEventsService {}

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

        // if request.event == "video_upload_successful" {
        //     tokio::spawn(async move {
        //         let params: Value = serde_json::from_str(&request.params).expect("Invalid JSON");

        //         process_event(Event {
        //             event: request.event,
        //             params,
        //             timestamp,
        //         })
        //         .await;
        //     });
        // }
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

async fn process_event(event: Event) {
    let uid = event.params["video_id"].as_str().unwrap();

    // call ML_SERVER_URL/api/v1/predict_bulk using reqwest
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/api/v1/predict", ML_SERVER_URL))
        .json(&serde_json::json!({
            "video_id": uid
        }))
        .send()
        .await
        .expect(&format!(
            "Failed to send request to {} for event {:?}",
            ML_SERVER_URL, event
        ));
    let body = response.text().await.expect(&format!(
        "Failed to get response body from {} for event {:?}",
        ML_SERVER_URL, event
    ));
    let result: EmbeddingResponse = serde_json::from_str(&body).expect(&format!(
        "Failed to parse response body: {} for event {:?}",
        body, event
    ));

    let response = client
        .post(format!("{}/upsert", UPSTASH_VECTOR_REST_URL))
        .header(
            "Authorization",
            format!("Bearer {}", UPSTASH_VECTOR_REST_TOKEN),
        )
        .json(&serde_json::json!({
            "id": uid,
            "vector": result.result[0]
        }))
        .send()
        .await
        .expect(&format!(
            "Failed to send request to {} for event {:?}",
            UPSTASH_VECTOR_REST_URL, event
        ));
}

pub mod ml_server {
    tonic::include_proto!("ml_server");
}

pub async fn call_predict() -> Result<(), AppError> {
    let channel = Channel::from_static("https://yral-gpu-compute-tasks.fly.dev:443")
        .connect()
        .await?;

    let mut off_chain_agent_grpc_auth_token = env::var("ML_SERVER_JWT_TOKEN")?;
    // removing whitespaces and new lines for proper parsing
    off_chain_agent_grpc_auth_token.retain(|c| !c.is_whitespace());

    let token: MetadataValue<_> = format!("Bearer {}", off_chain_agent_grpc_auth_token).parse()?;

    let mut client = ml_server::ml_server_client::MlServerClient::with_interceptor(
        channel,
        move |mut req: Request<()>| {
            req.metadata_mut().insert("authorization", token.clone());
            Ok(req)
        },
    );

    let request = tonic::Request::new(ml_server::VideoEmbedRequest {
        video_id: "ee1201fc2a6e45d9a981a3e484a7da0a".into(),
    });

    let response: ml_server::VideoEmbedResponse = client.predict(request).await?.into_inner();

    println!("RESPONSE={:?}", response);
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

pub async fn test_cloudflare() -> Result<(), AppError> {
    // Get Request to https://api.cloudflare.com/client/v4/accounts/{account_id}/stream
    // Query param start 2021-05-03T00:00:00Z

    let url = format!(
        "https://api.cloudflare.com/client/v4/accounts/{}/stream",
        CLOUDFLARE_ACCOUNT_ID
    );
    let bearer_token = env::var("CLOUDFLARE_STREAM_READ_AND_LIST_ACCESS_TOKEN")?;

    let client = reqwest::Client::new();
    let mut num_vids = 0;
    let mut start_time = "2021-05-03T00:00:00Z".to_string();
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
    }

    log::info!("Total number of videos: {}", num_vids);
    log::info!("Total number of videos in hashset: {}", hashset.len());

    Ok(())
}
