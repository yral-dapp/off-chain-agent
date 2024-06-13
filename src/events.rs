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

pub async fn call_predict() -> Html<&'static str> {
    // let pem = match std::fs::read_to_string(
    //     "/Users/komalsai/Library/Application Support/mkcert/rootCA.pem",
    // ) {
    //     Ok(pem) => pem,
    //     Err(e) => {
    //         error!("Failed to read rootCA.pem: {}", e);
    //         return Html("Failed to read rootCA.pem");
    //     }
    // };
    // let ca = Certificate::from_pem(pem);

    // let tls = ClientTlsConfig::new().ca_certificate(ca);

    let channel = match Channel::from_static("https://yral-ml-server-test.fly.dev:443")
        // .tls_config(tls)
        // .unwrap()
        .connect()
        .await
    {
        Ok(channel) => channel,
        Err(e) => {
            error!("Failed to connect to ML Server: {}", e);
            return Html("Failed to connect to ML Server");
        }
    };

    let mut off_chain_agent_grpc_auth_token = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ5cmFsLW1sLXNlcnZlciIsImNvbXBhbnkiOiJnb2JhenppbmdhIn0.ObgL1-x_e2yEheD3Xj-nyh6NS2881f2qFyTMA-8ai0XW3bJjMkfqMpZBokubJv2RKhsUGoRdfwukZNobm2xdCA".to_string();
    // removing whitespaces and new lines for proper parsing
    off_chain_agent_grpc_auth_token.retain(|c| !c.is_whitespace());

    let token: MetadataValue<_> =
        match format!("Bearer {}", off_chain_agent_grpc_auth_token).parse() {
            Ok(token) => token,
            Err(e) => {
                error!("Failed to parse token: {}", e);
                return Html("Failed to parse token");
            }
        };

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

    let response: ml_server::VideoEmbedResponse = match client.predict(request).await {
        Ok(response) => response.into_inner(),
        Err(e) => {
            error!("Failed to get response from ML Server: {}", e);
            return Html("Failed to get response from ML Server");
        }
    };

    println!("RESPONSE={:?}", response);
    Html("Hello, World!")
}

pub async fn test_cloudflare() -> Result<(), AppError> {
    // Get Request to https://api.cloudflare.com/client/v4/accounts/{account_id}/stream

    let url = format!(
        "https://api.cloudflare.com/client/v4/accounts/{}/stream",
        CLOUDFLARE_ACCOUNT_ID
    );
    let bearer_token = env::var("CLOUDFLARE_STREAM_READ_AND_LIST_ACCESS_TOKEN")?;

    let client = reqwest::Client::new();
    let response = client.get(url).bearer_auth(bearer_token).send().await?;
    log::info!("Response: {:?}", response);
    if response.status() != 200 {
        log::error!(
            "Failed to get response from Cloudflare: {:?}",
            response.text().await?
        );
        return Err(anyhow::anyhow!("Failed to get response from Cloudflare").into());
    } else {
        log::error!("Response: {:?}", response.text().await?);
    }

    Ok(())
}
