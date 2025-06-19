use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::types::DelegatedIdentityWire;

#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct PostRequest<T> {
    pub delegated_identity_wire: DelegatedIdentityWire,
    #[serde(flatten)]
    pub request_body: T,
}

#[derive(Serialize)]
pub struct VideoDeleteRow {
    pub canister_id: String,
    pub post_id: u64,
    pub video_id: String,
    pub gcs_video_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserPost {
    pub canister_id: String,
    pub post_id: u64,
    pub video_id: String,
}
