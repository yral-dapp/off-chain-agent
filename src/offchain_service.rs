use std::{collections::HashMap, env, fmt::format, sync::Arc};

use crate::{
    app_state::AppState, canister::individual_user_template::{PostStatus, Result5}, consts::GOOGLE_CHAT_REPORT_SPACE_URL, events::push_notifications::subscribe_device_to_topic, AppError
};
use anyhow::{Context, Result};
use axum::{extract::State, Json};
use axum_extra::TypedHeader;
use candid::{types::principal, Principal};
use headers::{
    authorization::{Basic, Bearer},
    Authorization,
};
use http::HeaderMap;
use jsonwebtoken::DecodingKey;
use reqwest::Client;
use serde_json::{json, Value};
use yup_oauth2::{
    ApplicationDefaultCredentialsFlowOpts, AuthorizedUserAuthenticator, DeviceFlowAuthenticator,
    ServiceAccountAuthenticator,
};

use crate::offchain_service::off_chain::{Empty, ReportPostRequest, BindDeviceToPrincipalRequest};
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

        Ok(tonic::Response::new(Empty {}))
    }

    async fn bind_device_to_principal(
        &self,
        request: tonic::Request<BindDeviceToPrincipalRequest>,
    ) -> core::result::Result<tonic::Response<Empty>, tonic::Status> {
        let request = request.into_inner();
        let device_id = request.device_id;
        let principal_id = request.principal_id;

        let result = subscribe_device_to_topic(&device_id, &principal_id, &self.shared_state).await;
        if let Err(e) = result {
            log::error!("Error subscribing principal to topic: {:?}", e);
            return Err(tonic::Status::new(
                tonic::Code::Unknown,
                "Error subscribing principal to topic",
            ));
        }

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
        return Err(anyhow::anyhow!("Error sending data to Google Chat").into());
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

    let res = user
        .update_post_status(post_id, PostStatus::BannedDueToUserReporting)
        .await?;

    // send confirmation to Google Chat
    let confirmation_msg = json!({
        "text": format!("Successfully banned post : {}/{}", canister_id, post_id)
    });
    let _ = send_message_gchat(GOOGLE_CHAT_REPORT_SPACE_URL, confirmation_msg).await?;

    Ok(())
}
