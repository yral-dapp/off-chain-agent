use std::{collections::HashMap, env, sync::Arc};

use crate::{app_state::AppState, consts::GOOGLE_CHAT_REPORT_SPACE_URL, AppError};
use anyhow::{Context, Result};
use axum::extract::State;
use candid::Principal;
use http::HeaderMap;
use jsonwebtoken::DecodingKey;
use reqwest::Client;
use serde_json::{json, Value};
use yral_canisters_client::individual_user_template::PostStatus;
use yup_oauth2::ServiceAccountAuthenticator;

use crate::offchain_service::off_chain::{Empty, ReportPostRequest};
use off_chain::off_chain_server::OffChain;

pub mod off_chain {
    tonic::include_proto!("off_chain");
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("off_chain_descriptor");
}

pub struct OffChainService {
    pub shared_state: Arc<AppState>,
}

#[tonic::async_trait]
impl OffChain for OffChainService {
    async fn report_post(
        &self,
        request: tonic::Request<ReportPostRequest>,
    ) -> core::result::Result<tonic::Response<Empty>, tonic::Status> {
        let shared_state = self.shared_state.clone();
        let request = request.into_inner();

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
                                    "text": "View video",
                                    "onClick": {
                                        "openLink": {
                                        "url": request.video_url
                                        }
                                    }
                                    },
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

        let res = send_message_gchat(GOOGLE_CHAT_REPORT_SPACE_URL, data).await;
        if res.is_err() {
            log::error!("Error sending data to Google Chat: {:?}", res);
            return Err(tonic::Status::new(
                tonic::Code::Unknown,
                "Error sending data to Google Chat",
            ));
        }

        let user_principal = Principal::from_text(&request.reporter_id)
            .map_err(|_| tonic::Status::new(tonic::Code::Unknown, "Invalid reporter id"))?;
        let user_canister_id = shared_state
            .get_individual_canister_by_user_principal(user_principal)
            .await
            .map_err(|_| {
                tonic::Status::new(tonic::Code::Unknown, "Failed to get user canister id")
            })?;
        let publisher_canister_id =
            Principal::from_text(&request.publisher_canister_id).map_err(|_| {
                tonic::Status::new(tonic::Code::Unknown, "Invalid publisher canister id")
            })?;
        let post_report_request = crate::posts::ReportPostRequest {
            canister_id: publisher_canister_id,
            post_id: request
                .post_id
                .parse::<u64>()
                .map_err(|_| tonic::Status::new(tonic::Code::Unknown, "Invalid post id"))?,
            video_id: request.video_id,
            user_canister_id,
            user_principal,
            reason: request.reason,
        };

        let qstash_client = shared_state.qstash_client.clone();
        qstash_client
            .publish_report_post(post_report_request)
            .await
            .map_err(|_| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    "Error sending data to qstash. request from leptos ssr",
                )
            })?;

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

pub async fn send_message_gchat(request_url: &str, data: Value) -> Result<()> {
    let token = get_chat_access_token().await;
    let client = Client::new();

    let response = client
        .post(request_url)
        .bearer_auth(token)
        .header("Content-Type", "application/json")
        .json(&data)
        .send()
        .await;

    if response.is_err() {
        log::error!("Error sending data to Google Chat: {:?}", response);
        return Err(anyhow::anyhow!("Error sending data to Google Chat"));
    }

    let body = response.unwrap().text().await.unwrap();

    Ok(())
}

#[derive(Debug, serde::Deserialize)]
struct GoogleJWT {
    aud: String,
    iss: String,
}

#[derive(Debug, serde::Deserialize)]
struct GChatPayload {
    #[serde(rename = "type")]
    payload_type: String,
    #[serde(rename = "eventTime")]
    event_time: String,
    message: serde_json::Value,
    space: serde_json::Value,
    user: serde_json::Value,
    action: GChatPayloadAction,
    common: serde_json::Value,
}

#[derive(Debug, serde::Deserialize)]
struct GChatPayloadAction {
    #[serde(rename = "actionMethodName")]
    action_method_name: String,
    parameters: Vec<GChatPayloadActionParameter>,
}

#[derive(Debug, serde::Deserialize)]
struct GChatPayloadActionParameter {
    key: String,
    value: String,
}

pub async fn report_approved_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: String,
) -> Result<(), AppError> {
    // log::error!("report_approved_handler: headers: {:?}", headers);
    // log::error!("report_approved_handler: body: {:?}", body);

    // authenticate the request

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

    // get PUBLIC_CERTS from GET https://www.googleapis.com/service_accounts/v1/metadata/x509/chat@system.gserviceaccount.com

    let client = reqwest::Client::new();
    let res = client
        .get("https://www.googleapis.com/service_accounts/v1/metadata/x509/chat@system.gserviceaccount.com")
        .send()
        .await?;
    let res_body = res.text().await?;

    let certs: HashMap<String, String> = serde_json::from_str(&res_body)?;

    // verify the JWT using jsonwebtoken crate

    let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256);
    validation.set_issuer(&["chat@system.gserviceaccount.com"]);
    validation.set_audience(&["82502260393"]);

    let mut valid = false;

    for (k, v) in &certs {
        let jwt = jsonwebtoken::decode::<GoogleJWT>(
            auth_token,
            &DecodingKey::from_rsa_pem(v.as_bytes())?,
            &validation,
        );

        if jwt.is_ok() {
            valid = true;
            break;
        }
    }
    if !valid {
        return Err(anyhow::anyhow!("Invalid JWT").into());
    }

    // Get the data from the body
    let payload: GChatPayload = serde_json::from_str(&body)?;
    let view_type = payload.action.parameters[0].value.clone();

    // view_type format : "canister_id post_id(int)"
    let view_type: Vec<&str> = view_type.split(" ").collect();
    let canister_id = view_type[0];
    let canister_principal = Principal::from_text(canister_id)?;
    let post_id = view_type[1].parse::<u64>()?;

    let user = state.individual_user(canister_principal);

    user.update_post_status(post_id, PostStatus::BannedDueToUserReporting)
        .await?;

    // send confirmation to Google Chat
    let confirmation_msg = json!({
        "text": format!("Successfully banned post : {}/{}", canister_id, post_id)
    });
    send_message_gchat(GOOGLE_CHAT_REPORT_SPACE_URL, confirmation_msg).await?;

    Ok(())
}
