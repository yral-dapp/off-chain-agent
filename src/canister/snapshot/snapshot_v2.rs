use std::{env, io::Write, process::Stdio, sync::Arc};

use axum::{extract::State, response::IntoResponse, Json};
use candid::Principal;
use chrono::{DateTime, Duration, Utc};
use http::StatusCode;
use ic_agent::Agent;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tracing::instrument;

use yral_canisters_client::{
    ic::PLATFORM_ORCHESTRATOR_ID, individual_user_template::IndividualUserTemplate,
    platform_orchestrator::PlatformOrchestrator, user_index::UserIndex,
};

use crate::{
    app_state::AppState,
    canister::utils::{get_subnet_orch_ids, get_user_canisters_list_v2},
    consts::{CANISTER_BACKUPS_BUCKET, STORJ_BACKUP_CANISTER_ACCESS_GRANT},
};

use super::{CanisterData, CanisterType};

#[derive(Debug, Serialize, Deserialize)]
pub struct BackupCanistersJobPayload {
    pub num_canisters: u32,
    pub rate_limit: u32,
    pub parallelism: u32,
}

#[instrument(skip(state))]
pub async fn backup_canisters_job_v2(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<BackupCanistersJobPayload>,
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

    let mut user_canister_list = get_user_canisters_list_v2(&agent).await.map_err(|e| {
        log::error!("Failed to get user canisters list: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })?;

    log::info!(
        "Sending user canister jobs to qstash: {:?}",
        user_canister_list.len()
    );

    if payload.num_canisters > 0 {
        user_canister_list = user_canister_list
            .into_iter()
            .take(payload.num_canisters as usize)
            .collect();
    }

    let qstash_client = state.qstash_client.clone();
    qstash_client
        .backup_canister_batch(
            user_canister_list,
            payload.rate_limit,
            payload.parallelism,
            date_str.clone(),
        )
        .await
        .map_err(|e| {
            log::error!("Failed to backup user canisters: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;

    log::info!("Successfully sent all jobs to backup user canisters");

    // perform backup of PF orch and subnet orchs
    backup_pf_and_subnet_orchs(&agent, date_str.clone())
        .await
        .map_err(|e| {
            log::error!("Failed to backup PF and subnet orchs: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;

    log::info!("Successfully backed up PF and subnet orchs");

    Ok((StatusCode::OK, "Backup successful".to_string()))
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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

    let canister_data = CanisterData {
        canister_id: payload.canister_id,
        canister_type: CanisterType::User,
    };

    backup_canister_impl(&agent, canister_data, payload.date_str)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((StatusCode::OK, "Backup successful".to_string()))
}

#[instrument(skip(agent))]
pub async fn backup_pf_and_subnet_orchs(
    agent: &Agent,
    date_str: String,
) -> Result<(), anyhow::Error> {
    let pf_orch_canister_data = CanisterData {
        canister_id: PLATFORM_ORCHESTRATOR_ID,
        canister_type: CanisterType::PlatformOrch,
    };

    if let Err(e) = backup_canister_impl(agent, pf_orch_canister_data, date_str.clone()).await {
        log::error!("Failed to backup platform orchestrator: {}", e);
    }

    let subnet_orch_ids = get_subnet_orch_ids(agent).await?;

    for subnet_orch_id in subnet_orch_ids {
        let subnet_orch_canister_data = CanisterData {
            canister_id: subnet_orch_id,
            canister_type: CanisterType::SubnetOrch,
        };

        if let Err(e) =
            backup_canister_impl(agent, subnet_orch_canister_data, date_str.clone()).await
        {
            log::error!("Failed to backup subnet orchestrator: {}", e);
        }
    }

    Ok(())
}

#[instrument(skip(agent))]
pub async fn backup_canister_impl(
    agent: &Agent,
    canister_data: CanisterData,
    date_str: String,
) -> Result<(), anyhow::Error> {
    let canister_id = canister_data.canister_id.to_string();

    let snapshot_bytes = get_canister_snapshot(canister_data.clone(), agent)
        .await
        .map_err(|e| {
            log::error!(
                "Failed to get user canister snapshot for canister: {} error: {}",
                canister_id,
                e
            );
            anyhow::anyhow!("get_canister_snapshot error: {}", e)
        })?;

    upload_snapshot_to_storj(canister_data.canister_id, date_str, snapshot_bytes)
        .await
        .map_err(|e| {
            log::error!(
                "Failed to upload user canister snapshot to storj for canister: {} error: {}",
                canister_id,
                e
            );
            anyhow::anyhow!("upload_snapshot_to_storj error: {}", e)
        })?;

    Ok(())
}

#[instrument(skip(agent))]
pub async fn get_canister_snapshot(
    canister_data: CanisterData,
    agent: &Agent,
) -> Result<Vec<u8>, anyhow::Error> {
    match canister_data.canister_type {
        CanisterType::User => get_user_canister_snapshot(canister_data.canister_id, agent).await,
        CanisterType::SubnetOrch => {
            get_subnet_orchestrator_snapshot(canister_data.canister_id, agent).await
        }
        CanisterType::PlatformOrch => {
            get_platform_orchestrator_snapshot(canister_data.canister_id, agent).await
        }
    }
}

#[instrument(skip(agent))]
pub async fn get_user_canister_snapshot(
    canister_id: Principal,
    agent: &Agent,
) -> Result<Vec<u8>, anyhow::Error> {
    let user_canister = IndividualUserTemplate(canister_id, agent);

    let snapshot_size = user_canister.save_snapshot_json_v_2().await.map_err(|e| {
        log::error!("Failed to save user canister snapshot: {}", e);
        anyhow::anyhow!("Failed to save user canister snapshot: {}", e)
    })?;

    // delay 1 second
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

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
            .await
            .map_err(|e| {
                log::error!("Failed to download user canister snapshot: {}", e);
                anyhow::anyhow!("Failed to download user canister snapshot: {}", e)
            })?;

        snapshot_bytes.extend(res);
    }

    // clear snapshot
    user_canister.clear_snapshot().await.map_err(|e| {
        log::error!("Failed to clear user canister snapshot: {}", e);
        anyhow::anyhow!("Failed to clear user canister snapshot: {}", e)
    })?;

    Ok(snapshot_bytes)
}

#[instrument(skip(agent))]
pub async fn get_subnet_orchestrator_snapshot(
    canister_id: Principal,
    agent: &Agent,
) -> Result<Vec<u8>, anyhow::Error> {
    let subnet_orch = UserIndex(canister_id, agent);

    let snapshot_size = subnet_orch.save_snapshot_json().await.map_err(|e| {
        log::error!("Failed to save subnet orchestrator snapshot: {}", e);
        anyhow::anyhow!("Failed to save subnet orchestrator snapshot: {}", e)
    })?;

    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

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
            .await
            .map_err(|e| {
                log::error!("Failed to download subnet orchestrator snapshot: {}", e);
                anyhow::anyhow!("Failed to download subnet orchestrator snapshot: {}", e)
            })?;

        snapshot_bytes.extend(res);
    }

    // clear snapshot
    subnet_orch.clear_snapshot().await.map_err(|e| {
        log::error!("Failed to clear subnet orchestrator snapshot: {}", e);
        anyhow::anyhow!("Failed to clear subnet orchestrator snapshot: {}", e)
    })?;

    Ok(snapshot_bytes)
}

#[instrument(skip(agent))]
pub async fn get_platform_orchestrator_snapshot(
    canister_id: Principal,
    agent: &Agent,
) -> Result<Vec<u8>, anyhow::Error> {
    let platform_orchestrator = PlatformOrchestrator(canister_id, agent);

    let snapshot_size = platform_orchestrator
        .save_snapshot_json()
        .await
        .map_err(|e| {
            log::error!("Failed to save platform orchestrator snapshot: {}", e);
            anyhow::anyhow!("Failed to save platform orchestrator snapshot: {}", e)
        })?;

    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

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
            .await
            .map_err(|e| {
                log::error!("Failed to download platform orchestrator snapshot: {}", e);
                anyhow::anyhow!("Failed to download platform orchestrator snapshot: {}", e)
            })?;

        snapshot_bytes.extend(res);
    }

    // clear snapshot
    platform_orchestrator.clear_snapshot().await.map_err(|e| {
        log::error!("Failed to clear platform orchestrator snapshot: {}", e);
        anyhow::anyhow!("Failed to clear platform orchestrator snapshot: {}", e)
    })?;

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
