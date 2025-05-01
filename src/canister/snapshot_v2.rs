use std::{env, io::Write, process::Stdio, sync::Arc};

use axum::{extract::State, response::IntoResponse, Json};
use candid::Principal;
use chrono::{DateTime, Duration, Utc};
use http::StatusCode;
use ic_agent::Agent;
use serde::{Deserialize, Serialize};
use tokio::{io::AsyncWriteExt, process::Command};
use tracing::instrument;

use yral_canisters_client::{
    ic::PLATFORM_ORCHESTRATOR_ID, individual_user_template::IndividualUserTemplate,
    platform_orchestrator::PlatformOrchestrator, user_index::UserIndex,
};

use crate::{
    app_state::AppState,
    canister::utils::get_user_canisters_list_v2,
    consts::{CANISTER_BACKUPS_BUCKET, STORJ_BACKUP_CANISTER_ACCESS_GRANT},
};

use super::utils::get_subnet_orch_ids;

#[instrument(skip(state))]
pub async fn backup_canisters_job_v2(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let date_str = Utc::now().format("%Y-%m-%d").to_string();
    log::info!(
        "Starting backup canisters job v2 at {} for date {}",
        timestamp,
        date_str
    );

    let agent = state.agent.clone();

    // send user canister jobs to qstash
    log::info!("Sending user canister jobs to qstash");

    let user_canister_list = get_user_canisters_list_v2(&agent).await.map_err(|e| {
        log::error!("Failed to get user canisters list: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })?;

    log::info!(
        "Sending user canister jobs to qstash: {:?}",
        user_canister_list.len()
    );

    let qstash_client = state.qstash_client.clone();
    qstash_client
        .backup_canister_batch(user_canister_list, date_str.clone())
        .await
        .map_err(|e| {
            log::error!("Failed to backup user canisters: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;

    log::info!("Sent user canister jobs to qstash");

    // perform backup of PF orch and subnet orchs
    backup_pf_and_subnet_orchs(&agent, date_str.clone())
        .await
        .map_err(|e| {
            log::error!("Failed to backup PF and subnet orchs: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;

    log::info!("Sent PF and subnet orchs jobs to qstash");

    Ok((StatusCode::OK, "Backup successful".to_string()))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BackupUserCanisterPayload {
    pub canister_id: Principal,
    pub date_str: String,
}

#[instrument(skip(state))]
pub async fn backup_user_canister(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<BackupUserCanisterPayload>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let agent = state.agent.clone();

    let snapshot_bytes = get_user_canister_snapshot(payload.canister_id, &agent)
        .await
        .map_err(|e| {
            log::error!("Failed to get user canister snapshot: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;

    upload_snapshot_to_storj(payload.canister_id, payload.date_str, snapshot_bytes)
        .await
        .map_err(|e| {
            log::error!("Failed to upload user canister snapshot to storj: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;

    Ok((StatusCode::OK, "Backup successful".to_string()))
}

#[instrument(skip(agent))]
pub async fn backup_pf_and_subnet_orchs(
    agent: &Agent,
    date_str: String,
) -> Result<(), anyhow::Error> {
    let pf_orch_snapshot_bytes =
        get_platform_orchestrator_snapshot(PLATFORM_ORCHESTRATOR_ID, agent).await?;

    upload_snapshot_to_storj(
        PLATFORM_ORCHESTRATOR_ID,
        date_str.clone(),
        pf_orch_snapshot_bytes,
    )
    .await?;

    let subnet_orch_ids = get_subnet_orch_ids(agent).await?;

    for subnet_orch_id in subnet_orch_ids {
        let subnet_orch_snapshot_bytes =
            get_subnet_orchestrator_snapshot(subnet_orch_id, agent).await?;

        upload_snapshot_to_storj(subnet_orch_id, date_str.clone(), subnet_orch_snapshot_bytes)
            .await?;
    }

    Ok(())
}

#[instrument(skip(agent))]
pub async fn get_user_canister_snapshot(
    canister_id: Principal,
    agent: &Agent,
) -> Result<Vec<u8>, anyhow::Error> {
    let user_canister = IndividualUserTemplate(canister_id, agent);

    let snapshot_size = user_canister.save_snapshot_json_v_2().await?;

    // Download snapshot

    let mut snapshot_bytes = vec![];
    let chunk_size = 1000 * 1000;
    let num_iters = (snapshot_size as f32 / chunk_size as f32).ceil() as u32;
    for i in 0..num_iters {
        let start = i * chunk_size;
        let mut end = (i + 1) * chunk_size;
        if end > snapshot_size {
            end = snapshot_size;
        }

        let res = user_canister
            .download_snapshot(start as u64, (end - start) as u64)
            .await?;

        snapshot_bytes.extend(res);
    }

    // clear snapshot
    user_canister.clear_snapshot().await?;

    Ok(snapshot_bytes)
}

#[instrument(skip(agent))]
pub async fn get_subnet_orchestrator_snapshot(
    canister_id: Principal,
    agent: &Agent,
) -> Result<Vec<u8>, anyhow::Error> {
    let subnet_orch = UserIndex(canister_id, agent);

    let snapshot_size = subnet_orch.save_snapshot_json().await?;

    // Download snapshot

    let mut snapshot_bytes = vec![];
    let chunk_size = 1000 * 1000;
    let num_iters = (snapshot_size as f32 / chunk_size as f32).ceil() as u32;
    for i in 0..num_iters {
        let start = i * chunk_size;
        let mut end = (i + 1) * chunk_size;
        if end > snapshot_size {
            end = snapshot_size;
        }

        let res = subnet_orch
            .download_snapshot(start as u64, (end - start) as u64)
            .await?;

        snapshot_bytes.extend(res);
    }

    // clear snapshot
    subnet_orch.clear_snapshot().await?;

    Ok(snapshot_bytes)
}

#[instrument(skip(agent))]
pub async fn get_platform_orchestrator_snapshot(
    canister_id: Principal,
    agent: &Agent,
) -> Result<Vec<u8>, anyhow::Error> {
    let platform_orchestrator = PlatformOrchestrator(canister_id, agent);

    let snapshot_size = platform_orchestrator.save_snapshot_json().await?;

    // Download snapshot

    let mut snapshot_bytes = vec![];
    let chunk_size = 1000 * 1000;
    let num_iters = (snapshot_size as f32 / chunk_size as f32).ceil() as u32;
    for i in 0..num_iters {
        let start = i * chunk_size;
        let mut end = (i + 1) * chunk_size;
        if end > snapshot_size {
            end = snapshot_size;
        }

        let res = platform_orchestrator
            .download_snapshot(start as u64, (end - start) as u64)
            .await?;

        snapshot_bytes.extend(res);
    }

    // clear snapshot
    platform_orchestrator.clear_snapshot().await?;

    Ok(snapshot_bytes)
}

#[instrument(skip(snapshot_bytes))]
#[cfg(feature = "use-uplink")]
pub async fn upload_snapshot_to_storj(
    canister_id: Principal,
    object_id: String,
    snapshot_bytes: Vec<u8>,
) -> Result<(), anyhow::Error> {
    use uplink::{access::Grant, project::options::ListObjects, Project};

    let access_grant = Grant::new(&STORJ_BACKUP_CANISTER_ACCESS_GRANT)?;
    let bucket_name = CANISTER_BACKUPS_BUCKET;
    let project = &mut Project::open(&access_grant);
    let (_bucket, _ok) = project.create_bucket(&bucket_name).expect("create bucket");

    let upload = &mut project.upload_object(
        &bucket_name,
        &format!("{}/{}", canister_id, object_id),
        None,
    )?;
    upload.write_all(&snapshot_bytes)?;
    upload.commit()?;

    // list objects and delete any objects older than 30 days
    let mut list_objects_options = ListObjects::with_prefix(&format!("{}/", canister_id))?;
    list_objects_options.recursive = true;
    let obj_list = &mut project.list_objects(&bucket_name, Some(&list_objects_options))?;
    for obj_res in obj_list {
        let obj = obj_res?;
        let obj_key = obj.key;

        let date_str = obj_key.split("/").last().unwrap(); // obj_key is in the format of "canister_id/date"
        let date_str = format!("{}T00:00:00Z", date_str);
        let obj_date = DateTime::parse_from_rfc3339(&date_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse date: {}", e))?;
        let diff = Utc::now().signed_duration_since(obj_date);
        if diff > Duration::days(30) {
            project
                .delete_object(&bucket_name, &obj_key)
                .map_err(|e| anyhow::anyhow!("Failed to delete object: {}", e))?;
        }
    }

    Ok(())
}

#[cfg(not(feature = "use-uplink"))]
pub async fn upload_snapshot_to_storj(
    canister_id: Principal,
    object_id: String,
    snapshot_bytes: Vec<u8>,
) -> Result<(), anyhow::Error> {
    log::warn!("Uplink is not enabled, skipping upload to storj");
    Ok(())
}
