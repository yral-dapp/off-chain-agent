use std::sync::Arc;

use google_cloud_bigquery::http::tabledata::insert_all::{InsertAllRequest, Row};
use http::StatusCode;
use ic_agent::{identity::DelegatedIdentity, Agent};

use crate::{app_state::AppState, canister::upload_user_video::DelegatedIdentityWire};

use super::types::VideoDeleteRow;

pub async fn get_agent_from_delegated_identity_wire(
    identity_wire: &DelegatedIdentityWire,
) -> Result<Agent, anyhow::Error> {
    let identity: DelegatedIdentity = DelegatedIdentity::try_from(identity_wire.clone())
        .map_err(|_| anyhow::anyhow!("Failed to create delegated identity"))?;
    let agent = Agent::builder()
        .with_identity(identity)
        .with_url("https://ic0.app")
        .build()?;

    Ok(agent)
}

pub async fn insert_video_delete_row_to_bigquery(
    state: Arc<AppState>,
    canister_id: String,
    post_id: u64,
    video_id: String,
) -> Result<(), anyhow::Error> {
    let video_delete_row = VideoDeleteRow {
        canister_id,
        post_id,
        video_id: video_id.clone(),
        gcs_video_id: format!("gs://yral-videos/{}.mp4", video_id),
    };

    let bigquery_client = state.bigquery_client.clone();
    let row = Row {
        insert_id: None,
        json: video_delete_row,
    };

    let request = InsertAllRequest {
        rows: vec![row],
        ..Default::default()
    };

    let res = bigquery_client
        .tabledata()
        .insert(
            "hot-or-not-feed-intelligence",
            "yral_ds",
            "video_deleted",
            &request,
        )
        .await?;

    log::info!("video_deleted insert response : {:?}", res);

    Ok(())
}
