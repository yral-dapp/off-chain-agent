use serde::Serialize;

#[derive(Serialize)]
pub struct VideoDeleteRow {
    pub canister_id: String,
    pub post_id: u64,
    pub video_id: String,
    pub gcs_video_id: String,
}
