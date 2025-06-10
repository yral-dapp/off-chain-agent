use candid::Principal;
use ic_agent::Agent;
use redis::AsyncCommands;
use yral_canisters_client::ic::PLATFORM_ORCHESTRATOR_ID;

use crate::{
    canister::{
        snapshot::CanisterType,
        utils::{get_subnet_orch_ids, get_user_canisters_list_v2},
    },
    types::RedisPool,
};

use super::CanisterData;

pub async fn insert_canister_backup_date_into_redis(
    canister_backup_redis_pool: &RedisPool,
    date_str: String,
    canister_data: CanisterData,
) -> Result<(), anyhow::Error> {
    let redis_key = format!(
        "canister_backup_date:{:?}:{}",
        canister_data.canister_type, date_str
    );
    let mut conn = canister_backup_redis_pool.get().await?;

    conn.rpush::<String, String, ()>(redis_key, canister_data.canister_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to insert into redis: {}", e))?;

    Ok(())
}

pub async fn get_user_canister_list_for_backup(
    agent: &Agent,
    canister_backup_redis_pool: &RedisPool,
    date_str: String,
) -> Result<Vec<Principal>, anyhow::Error> {
    let user_canister_list = get_user_canisters_list_v2(&agent).await?;

    log::info!("User canister list length: {:?}", user_canister_list.len());

    let mut conn = canister_backup_redis_pool.get().await?;
    let canister_backup_date_list = conn
        .lrange::<String, Vec<String>>(
            format!("canister_backup_date:{:?}:{}", CanisterType::User, date_str),
            0,
            -1,
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get canister backup date list: {}", e))?;
    let canister_backup_date_list = canister_backup_date_list
        .iter()
        .map(|canister_id| Principal::from_text(canister_id).unwrap())
        .collect::<Vec<_>>();

    let user_canister_list = user_canister_list
        .into_iter()
        .filter(|canister_id| !canister_backup_date_list.contains(canister_id))
        .collect::<Vec<_>>();

    log::info!(
        "User canister list length after filtering: {:?}",
        user_canister_list.len()
    );

    Ok(user_canister_list)
}

pub async fn get_subnet_orch_ids_list_for_backup(
    agent: &Agent,
    canister_backup_redis_pool: &RedisPool,
    date_str: String,
) -> Result<Vec<Principal>, anyhow::Error> {
    let subnet_orch_ids_list = get_subnet_orch_ids(&agent).await?;

    log::info!(
        "Subnet orch ids list length: {:?}",
        subnet_orch_ids_list.len()
    );

    let mut conn = canister_backup_redis_pool.get().await?;
    let canister_backup_date_list = conn
        .lrange::<String, Vec<String>>(
            format!(
                "canister_backup_date:{:?}:{}",
                CanisterType::SubnetOrch,
                date_str
            ),
            0,
            -1,
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get canister backup date list: {}", e))?;
    let canister_backup_date_list = canister_backup_date_list
        .iter()
        .map(|canister_id| Principal::from_text(canister_id).unwrap())
        .collect::<Vec<_>>();

    let subnet_orch_ids_list = subnet_orch_ids_list
        .into_iter()
        .filter(|canister_id| !canister_backup_date_list.contains(canister_id))
        .collect::<Vec<_>>();

    log::info!(
        "Subnet orch ids list length after filtering: {:?}",
        subnet_orch_ids_list.len()
    );

    Ok(subnet_orch_ids_list)
}

pub async fn get_platform_orch_ids_list_for_backup(
    agent: &Agent,
    canister_backup_redis_pool: &RedisPool,
    date_str: String,
) -> Result<Vec<Principal>, anyhow::Error> {
    let platform_orch_id = PLATFORM_ORCHESTRATOR_ID;

    let mut conn = canister_backup_redis_pool.get().await?;
    let canister_backup_date_list = conn
        .lrange::<String, Vec<String>>(
            format!(
                "canister_backup_date:{:?}:{}",
                CanisterType::PlatformOrch,
                date_str
            ),
            0,
            -1,
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get canister backup date list: {}", e))?;

    if canister_backup_date_list.contains(&platform_orch_id.to_string()) {
        return Ok(vec![]);
    }

    Ok(vec![platform_orch_id])
}
