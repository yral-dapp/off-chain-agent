use std::{error::Error, sync::Arc};

use axum::{extract::State, Json};
use ic_agent::{
    identity::{DelegatedIdentity, Secp256k1Identity, SignedDelegation},
    Agent, Identity,
};
use k256::{elliptic_curve::JwkEcKey, SecretKey};
use serde::{Deserialize, Serialize};
use yral_metadata_client::MetadataClient;

use crate::{app_state::AppState, events::VideoUploadSuccessful};

use super::individual_user_template::{IndividualUserTemplate, PostDetailsFromFrontend, Result_};

#[derive(Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    success: bool,
    error: Option<String>,
    data: Option<T>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct UploadUserVideoRequestBody {
    delegated_identity_wire: DelegatedIdentityWire,
    post_details: PostDetails,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PostDetails {
    pub is_nsfw: bool,
    pub hashtags: Vec<String>,
    pub description: String,
    pub video_uid: String,
    pub creator_consent_for_inclusion_in_hot_or_not: bool,
}

impl From<PostDetails> for PostDetailsFromFrontend {
    fn from(value: PostDetails) -> Self {
        Self {
            is_nsfw: value.is_nsfw,
            hashtags: value.hashtags,
            description: value.description,
            video_uid: value.video_uid,
            creator_consent_for_inclusion_in_hot_or_not: value
                .creator_consent_for_inclusion_in_hot_or_not,
        }
    }
}

pub struct UploadUserVideoResData;

#[derive(Serialize, Deserialize, Clone)]
pub struct DelegatedIdentityWire {
    from_key: Vec<u8>,
    to_secret: JwkEcKey,
    delegation_chain: Vec<SignedDelegation>,
}

impl TryFrom<DelegatedIdentityWire> for DelegatedIdentity {
    fn try_from(value: DelegatedIdentityWire) -> Result<Self, Self::Error> {
        let secret_key = SecretKey::from_jwk(&value.to_secret).map_err(|e| e.to_string())?;
        let to_identity = Secp256k1Identity::from_private_key(secret_key);
        Ok(DelegatedIdentity::new(
            value.from_key,
            Box::new(to_identity),
            value.delegation_chain,
        ))
    }

    type Error = String;
}

impl<T> From<Result<T, Box<dyn Error>>> for ApiResponse<T>
where
    T: Sized,
{
    fn from(value: Result<T, Box<dyn Error>>) -> Self {
        match value {
            Ok(res) => ApiResponse {
                success: true,
                error: None,
                data: Some(res),
            },
            Err(e) => ApiResponse {
                success: false,
                error: Some(e.to_string()),
                data: None,
            },
        }
    }
}

pub async fn upload_user_video_handler(
    State(app_state): State<Arc<AppState>>,
    Json(payload): Json<UploadUserVideoRequestBody>,
) -> Json<ApiResponse<()>> {
    let upload_video_result = upload_user_video_impl(app_state.clone(), payload).await;

    Json(ApiResponse::from(upload_video_result))
}

pub async fn upload_user_video_impl(
    app_state: Arc<AppState>,
    payload: UploadUserVideoRequestBody,
) -> Result<(), Box<dyn Error>> {
    let yral_metadata_client = &app_state.yral_metadata_client;
    let identity: DelegatedIdentity = DelegatedIdentity::try_from(payload.delegated_identity_wire)?;
    let user_principal = identity.sender()?;

    let agent = Agent::builder()
        .with_identity(identity)
        .with_url("https://ic0.app")
        .build()?;
    let user_meta_data = yral_metadata_client
        .get_user_metadata(user_principal)
        .await?
        .ok_or("metadata for principal not found")?;
    let individual_user_template = IndividualUserTemplate(user_meta_data.user_canister_id, &agent);

    let upload_video_res = individual_user_template
        .add_post_v_2(PostDetailsFromFrontend::from(payload.post_details.clone()))
        .await?;

    match upload_video_res {
        Result_::Ok(post_id) => {
            let upload_video_event = VideoUploadSuccessful {
                shared_state: app_state.clone(),
            };

            let upload_event_result = upload_video_event
                .send_event(
                    user_principal,
                    user_meta_data.user_canister_id,
                    user_meta_data.user_name,
                    payload.post_details.video_uid,
                    payload.post_details.hashtags.len(),
                    payload.post_details.is_nsfw,
                    payload
                        .post_details
                        .creator_consent_for_inclusion_in_hot_or_not,
                    post_id,
                )
                .await;

            if let Err(e) = upload_event_result {
                println!(
                    "Error in sending event upload_video_successful {}",
                    e.to_string()
                );
            }

            Ok(())
        }
        Result_::Err(e) => Err(e.into()),
    }
}
