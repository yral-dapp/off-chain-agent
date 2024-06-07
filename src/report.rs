use std::{env, fmt::format};

use crate::AppError;
use anyhow::{Context, Result};
use axum::Json;
use axum_extra::TypedHeader;
use headers::{
    authorization::{Basic, Bearer},
    Authorization,
};
use http::HeaderMap;
use reqwest::Client;
use serde_json::{json, Value};
use yup_oauth2::{
    ApplicationDefaultCredentialsFlowOpts, AuthorizedUserAuthenticator, DeviceFlowAuthenticator,
    ServiceAccountAuthenticator,
};

use crate::report::off_chain::{Empty, ReportPostRequest};
use off_chain::off_chain_server::OffChain;

pub mod off_chain {
    tonic::include_proto!("off_chain");
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("off_chain_descriptor");
}

pub struct OffChainService {}

#[tonic::async_trait]
impl OffChain for OffChainService {
    async fn report_post(
        &self,
        request: tonic::Request<ReportPostRequest>,
    ) -> core::result::Result<tonic::Response<Empty>, tonic::Status> {
        let request = request.into_inner();
        let request_url = "https://chat.googleapis.com/v1/spaces/AAAA1yDLYO4/messages";

        let token = get_chat_access_token().await;
        let client = Client::new();

        let text_str = format!(
            "reporter_id: {} \n publisher_id: {} \n publisher_canister_id: {} \n post_id: {} \n video_id: {} \n reason: {} \n video_url: {}",
            request.reporter_id, request.publisher_id, request.publisher_canister_id, request.post_id, request.video_id, request.reason, request.video_url
        );

        let data = json!({
            "cardsV2": [
            {
                "cardId": "unique-card-id",
                "card": {
                    "sections": [
                    {
                        "header": "Report Post",
                        "widgets": [
                        {
                            "textParagraph": {
                                "text": text_str
                            }
                        },
                        {
                            "buttonList": {
                                "buttons": [
                                    {
                                    "text": "Ban Post",
                                    "onClick": {
                                        "action": {
                                        "function": "goToView",
                                        "parameters": [
                                            {
                                            "key": "viewType",
                                            "value": format!("{} {}", request.publisher_canister_id, request.post_id),
                                            }
                                        ]
                                        }
                                    }
                                    }
                                ]
                            }
                        }
                        ]
                    }
                    ]
                }
            }
            ]
        });

        let response = client
            .post(request_url)
            .bearer_auth(token)
            .header("Content-Type", "application/json")
            .json(&data)
            .send()
            .await;

        if response.is_err() {
            log::error!("Error sending data to Google Chat: {:?}", response);
            return Err(tonic::Status::new(
                tonic::Code::Unknown,
                "Error sending data to Google Chat",
            ));
        }

        let body = response.unwrap().text().await.unwrap();
        log::info!("Response from Google Chat: {:?}", body);

        Ok(tonic::Response::new(Empty {}))
    }
}

pub async fn get_chat_access_token() -> String {
    let sa_key_file = env::var("GOOGLE_SA_KEY").expect("GOOGLE_SA_KEY is required");

    // Load your service account key
    let sa_key = yup_oauth2::parse_service_account_key(sa_key_file).expect("GOOGLE_SA_KEY.json");

    let auth = ServiceAccountAuthenticator::builder(sa_key)
        .build()
        .await
        .unwrap();

    let scopes = &["https://www.googleapis.com/auth/chat.bot"];
    let token = auth.token(scopes).await.unwrap();

    match token.token() {
        Some(t) => t.to_string(),
        _ => panic!("No access token found"),
    }
}

pub async fn report_approved_handler(
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<(), AppError> {
    log::error!("report_approved_handler: headers: {:?}", headers);
    log::error!("report_approved_handler: body: {:?}", body);

    let bearer = headers
        .get("Authorization")
        .context("Missing Authorization header")?;
    let bearer_str = bearer
        .to_str()
        .context("Failed to parse Authorization header")?;
    let auth_token = bearer_str
        .split("Bearer ")
        .last()
        .context("Failed to parse Bearer token")?;

    // call GET https://oauth2.googleapis.com/tokeninfo?id_token={BEARER_TOKEN} using reqwest

    let client = reqwest::Client::new();
    let res = client
        .get("https://oauth2.googleapis.com/tokeninfo")
        .query(&[("id_token", auth_token)])
        .send()
        .await?;
    log::info!("report_approved_handler: tokeninfo response: {:?}", res);
    let res_body = res.text().await?;
    log::info!(
        "report_approved_handler: tokeninfo response body: {:?}",
        res_body
    );

    Ok(())
}
