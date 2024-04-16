use std::env;

use axum::response::Html;
use candid::Deserialize;
use reqwest::Client;
use serde::Serialize;
use serde_json::Value;
use yup_oauth2::{ExternalAccountAuthenticator, ServiceAccountAuthenticator};

use warehouse_events::warehouse_events_server::WarehouseEvents;

use crate::auth::AuthBearer;
use crate::consts::BIGQUERY_INGESTION_URL;
use crate::events::warehouse_events::{Empty, WarehouseEvent};

pub mod warehouse_events {
    tonic::include_proto!("warehouse_events");
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("warehouse_events_descriptor");
}

pub struct WarehouseEventsService {}

#[tonic::async_trait]
impl WarehouseEvents for WarehouseEventsService {
    async fn send_event(
        &self,
        request: tonic::Request<WarehouseEvent>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let request = request.into_inner();

        let data = serde_json::json!({
            "kind": "bigquery#tableDataInsertAllRequest",
            "rows": [
                {
                    "json": {
                        "event": request.event,
                        "params": request.params,
                    }
                }
            ]
        });

        let res = stream_to_bigquery(data).await;

        if res.is_err() {
            println!("Error sending data to BigQuery: {}", res.err().unwrap());
            return Err(tonic::Status::new(
                tonic::Code::Unknown,
                "Error sending data to BigQuery",
            ));
        }

        Ok(tonic::Response::new(Empty {}))
    }
}


// async fn get_access_token() -> String {
//     let sa_key_file = env::var("GOOGLE_SA_KEY").expect("GOOGLE_SA_KEY is required");
//
//     // Load your service account key
//     let sa_key = yup_oauth2::parse_service_account_key(sa_key_file).expect("GOOGLE_SA_KEY.json");
//
//     let auth = ServiceAccountAuthenticator::builder(sa_key)
//         .build()
//         .await
//         .unwrap();
//
//     let scopes = &["https://www.googleapis.com/auth/bigquery.insertdata"];
//     let token = auth.token(scopes).await.unwrap();
//
//     match token.token() {
//         Some(t) => t.to_string(),
//         _ => panic!("No access token found"),
//     }
// }

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExternalAccountSecret {
    /// audience
    pub audience: String,
    /// subject_token_type
    pub subject_token_type: String,
    /// service_account_impersonation_url
    pub service_account_impersonation_url: Option<String>,
    /// token_url
    pub token_url: String,
    // TODO: support service_account_impersonation.
    /// credential_source
    pub credential_source: CredentialSource,
    #[serde(rename = "type")]
    /// key_type
    pub key_type: String,
}


async fn get_access_token() -> String {
    let google_creds_file = env::var("GOOGLE_WL_CREDS").expect("GOOGLE_WL_CREDS is required");

    let creds = yup_oauth2::read_external_account_secret(google_creds).await.expect("GOOGLE_WL_CREDS json file");

    let auth = ExternalAccountAuthenticator::builder(creds.into())
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

