use std::{env, io::Write, process::Stdio, sync::Arc};

use axum::{extract::State, response::IntoResponse, Json};
use candid::Principal;
use chrono::{DateTime, Duration, Utc};
use futures::StreamExt;
use http::StatusCode;
use ic_agent::Agent;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use tracing::instrument;

use yral_canisters_client::ic::PLATFORM_ORCHESTRATOR_ID;

use crate::{
    app_state::AppState,
    canister::utils::{get_subnet_orch_ids, get_user_canisters_list_v2},
    consts::{CANISTER_BACKUPS_BUCKET, STORJ_BACKUP_CANISTER_ACCESS_GRANT},
};

use super::{snapshot_v2::backup_canister_impl, CanisterData, CanisterType};

// API for the snapshot alert job which calls the impl function

#[axum::debug_handler]
#[instrument(skip(state))]
pub async fn snapshot_alert_job(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let agent = state.agent.clone();
    snapshot_alert_job_impl(&agent)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

#[instrument(skip(agent))]
#[cfg(feature = "use-uplink")]
pub async fn snapshot_alert_job_impl(agent: &Agent) -> Result<(), anyhow::Error> {
    use std::str::FromStr;
    use uplink::{access::Grant, project::options::ListObjects, Project};

    log::info!("Starting snapshot alert job");

    // Calculate backup_dates_map within a separate block to drop non-Send types
    let backup_dates_map: HashMap<Principal, Vec<DateTime<Utc>>> = {
        let access_grant = Grant::new(&STORJ_BACKUP_CANISTER_ACCESS_GRANT)?;
        let bucket_name = CANISTER_BACKUPS_BUCKET;
        let project = &mut Project::open(&access_grant);
        let (_bucket, _ok) = project
            .create_bucket(&bucket_name)
            .map_err(|e| anyhow::anyhow!("Failed to create/open bucket: {}", e))?;

        let mut list_objects_options = <ListObjects as std::default::Default>::default();
        list_objects_options.recursive = true; // Ensure we get all objects, not just top-level prefixes
        let object_list = &mut project.list_objects(&bucket_name, Some(&list_objects_options))?;

        let mut map_in_scope: HashMap<Principal, Vec<DateTime<Utc>>> = HashMap::new();

        log::info!("Processing backup objects from Storj...");
        for obj_res in object_list {
            match obj_res {
                Ok(obj) => {
                    if obj.is_prefix {
                        // Skip prefixes/directories
                        continue;
                    }
                    let parts: Vec<&str> = obj.key.split('/').collect();
                    if parts.len() == 2 {
                        let canister_id_str = parts[0];
                        let date_str = parts[1];

                        match Principal::from_text(canister_id_str) {
                            Ok(canister_id) => {
                                // Append time and timezone for parsing
                                let datetime_str = format!("{}T00:00:00Z", date_str);
                                match DateTime::parse_from_rfc3339(&datetime_str) {
                                    Ok(obj_date) => {
                                        map_in_scope // Use the map inside the scope
                                            .entry(canister_id)
                                            .or_default()
                                            .push(obj_date.with_timezone(&Utc));
                                    }
                                    Err(e) => {
                                        log::warn!(
                                            "Failed to parse date from key '{}': {}",
                                            obj.key,
                                            e
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                log::warn!(
                                    "Failed to parse Principal from key '{}': {}",
                                    obj.key,
                                    e
                                );
                            }
                        }
                    } else {
                        log::warn!("Unexpected object key format: {}", obj.key);
                    }
                }
                Err(e) => {
                    log::error!("Error listing object: {}", e);
                }
            }
        }
        log::info!(
            "Finished processing Storj objects. Found backups for {} canisters.",
            map_in_scope.len() // Use the map inside the scope
        );

        // Sort dates for each canister
        for dates in map_in_scope.values_mut() {
            // Use the map inside the scope
            dates.sort_unstable(); // Use unstable sort as order of equal dates doesn't matter
        }

        map_in_scope
    };

    // Get all expected canisters
    log::info!("Fetching canister lists...");
    let mut all_canisters: HashSet<Principal> = HashSet::new();
    let mut all_subnet_orch_ids: HashSet<Principal> = HashSet::new();

    // Add user canisters
    match get_user_canisters_list_v2(agent).await {
        Ok(user_canisters) => {
            log::info!("Found {} user canisters.", user_canisters.len());
            all_canisters.extend(user_canisters);
        }
        Err(e) => {
            log::error!("Failed to get user canisters list: {}", e);
            return Err(anyhow::anyhow!("Failed to get user canisters list: {}", e));
        }
    }

    // Add subnet orchestrators
    match get_subnet_orch_ids(agent).await {
        Ok(subnet_orch_ids) => {
            log::info!("Found {} subnet orchestrators.", subnet_orch_ids.len());
            all_canisters.extend(subnet_orch_ids.clone());
            all_subnet_orch_ids.extend(subnet_orch_ids);
        }
        Err(e) => {
            log::error!("Failed to get subnet orchestrator IDs: {}", e);
            return Err(anyhow::anyhow!(
                "Failed to get subnet orchestrator IDs: {}",
                e
            ));
        }
    }

    // Add platform orchestrator
    all_canisters.insert(PLATFORM_ORCHESTRATOR_ID);
    log::info!(
        "Total expected canisters (including Platform Orchestrator): {}",
        all_canisters.len()
    );

    // Identify canisters needing alerts
    let mut canisters_backups: Vec<(CanisterData, String)> = Vec::new();
    let now = Utc::now();
    let alert_threshold = Duration::days(4);

    log::info!("Checking backup status for each canister...");
    for canister_id in all_canisters {
        // if canister_id is in all_subnet_orch_ids, then it's a subnet orchestrator
        let canister_type = if all_subnet_orch_ids.contains(&canister_id) {
            CanisterType::SubnetOrch
        } else if canister_id == PLATFORM_ORCHESTRATOR_ID {
            CanisterType::PlatformOrch
        } else {
            CanisterType::User
        };

        let canister_data = CanisterData {
            canister_id,
            canister_type,
        };

        match backup_dates_map.get(&canister_id) {
            Some(dates) => {
                if let Some(latest_backup_date) = dates.last() {
                    let time_since_last_backup = now.signed_duration_since(*latest_backup_date);
                    if time_since_last_backup > alert_threshold {
                        canisters_backups.push((
                            canister_data,
                            latest_backup_date.format("%Y-%m-%d").to_string(),
                        ));
                    }
                } else {
                    canisters_backups.push((canister_data, "missing".to_string()));
                }
            }
            None => {
                canisters_backups.push((canister_data, "missing".to_string()));
            }
        }
    }

    if canisters_backups.len() == 0 {
        log::info!("Snapshot alert job finished: All canisters have recent backups.");
    } else {
        log::warn!(
            "Snapshot alert job finished: {} canisters require attention",
            canisters_backups.len()
        );
    }

    let date_str = Utc::now().format("%Y-%m-%d").to_string();
    let canisters_retry_backup_results =
        retry_backup_canisters(agent, canisters_backups, date_str).await?;

    send_google_chat_alert(canisters_retry_backup_results).await?;

    Ok(())
}

#[cfg(not(feature = "use-uplink"))]
pub async fn snapshot_alert_job_impl(_agent: &Agent) -> Result<(), anyhow::Error> {
    // Add agent param here too
    log::warn!("Uplink is not enabled, skipping snapshot alert job");
    Ok(())
}

pub async fn retry_backup_canisters(
    agent: &Agent,
    canister_list: Vec<(CanisterData, String)>,
    date_str: String,
) -> Result<HashMap<String, Vec<(String, String)>>, anyhow::Error> {
    let mut results = HashMap::new();

    let futures = canister_list
        .into_iter()
        .map(|(canister_data, old_date_str)| {
            let agent = agent.clone();
            let date_str = date_str.clone();
            async move {
                let canister_id = canister_data.canister_id.to_string();
                if let Err(e) = backup_canister_impl(&agent, canister_data, date_str.clone()).await
                {
                    let err_str = e.to_string();
                    Err((err_str, canister_id, old_date_str))
                } else {
                    Ok(())
                }
            }
        });

    let results_vec = futures::stream::iter(futures)
        .buffer_unordered(30)
        .collect::<Vec<_>>()
        .await;

    for res_item in results_vec {
        match res_item {
            Ok(()) => {}
            Err((err_str, canister_id, old_date_str)) => {
                let err_vec = results.entry(err_str).or_insert_with(Vec::new);
                err_vec.push((canister_id, old_date_str));
            }
        }
    }

    Ok(results)
}

async fn send_google_chat_alert(
    canisters_retry_backup_results: HashMap<String, Vec<(String, String)>>,
) -> Result<(), anyhow::Error> {
    let google_webhook_url = env::var("CANISTER_BACKUP_ALERT_GOOGLE_CHAT_WEBHOOK_URL")
        .map_err(|_| anyhow::anyhow!("CANISTER_BACKUP_ALERT_GOOGLE_CHAT_WEBHOOK_URL not set"))?;
    let client = reqwest::Client::new();
    // Calculate total count from the new structure
    let total_attention_count: usize = canisters_retry_backup_results
        .values()
        .map(|v| v.len())
        .sum();

    if total_attention_count == 0 {
        let body = json!({
            "text": "✅ Snapshot Retry Job Finished: All previously failing canisters backed up successfully or no canisters needed retries."
        });
        let res = client.post(&google_webhook_url).json(&body).send().await?;
        if res.status().is_success() {
            log::info!("Snapshot Retry Job Finished: 'All clear' status sent to Google Chat");
        } else {
            log::error!(
                "Snapshot Retry Job Finished: Failed to send 'All clear' status to Google Chat: {}",
                res.status()
            );
        }
        return Ok(());
    }

    let max_chars_per_message = 10000; // Google Chat message character limit
    let mut messages_to_send = Vec::new();
    let mut current_message = format!(
        "🚨 Snapshot Retry Job Finished: *{}* canisters still require attention after retry!\n\n",
        total_attention_count
    );
    let mut current_char_count = current_message.len();

    // Sort errors for consistent output
    let mut sorted_errors: Vec<_> = canisters_retry_backup_results.into_iter().collect();
    sorted_errors.sort_by(|a, b| a.0.cmp(&b.0));

    for (error_str, mut canister_list) in sorted_errors {
        if canister_list.is_empty() {
            continue;
        }

        // Sort canisters within the error group by date string ("missing" will typically be sorted after dates)
        canister_list.sort_by(|a, b| a.1.cmp(&b.1));

        let section_header = format!("*Error: {}*\n", error_str);
        let continuation_header = format!("*(Cont...) Error: {}*\n", error_str);

        // Check if header fits in the current message
        if current_char_count + section_header.len() > max_chars_per_message {
            messages_to_send.push(current_message.clone());
            current_message = String::new(); // Start fresh
            current_char_count = 0;
        }

        // Add header (either to new or existing message)
        if current_message.is_empty() {
            current_message.push_str(&section_header);
            current_char_count = current_message.len();
        } else {
            // Ensure newline separation if not starting fresh
            if !current_message.ends_with('\n') {
                current_message.push('\n');
                current_char_count += 1;
            }
            if !current_message.ends_with("\n\n") {
                current_message.push('\n');
                current_char_count += 1;
            }
            current_message.push_str(&section_header);
            current_char_count += section_header.len();
        }

        for (canister_id, date_str) in canister_list {
            let item_line = format!("- {}    {}\n", canister_id, date_str);
            let chars_to_add = item_line.len();

            if current_char_count + chars_to_add > max_chars_per_message {
                messages_to_send.push(current_message.clone()); // Finalize previous chunk
                current_message = continuation_header.clone(); // Start new chunk with continuation header
                current_char_count = current_message.len();
            }

            // Add the item line
            current_message.push_str(&item_line);
            current_char_count += chars_to_add;
        }
        // Add a newline after the list section for better separation between error groups
        if !current_message.ends_with("\n\n") {
            current_message.push('\n');
            current_char_count += 1;
        }
    }

    // Add the final message chunk if it contains content
    if !current_message.trim().is_empty() {
        messages_to_send.push(current_message);
    }

    // Send all the messages
    log::info!(
        "Sending {} alert message chunk(s) to Google Chat...",
        messages_to_send.len()
    );
    for (i, msg) in messages_to_send.iter().enumerate() {
        // Basic check to avoid sending empty messages or just headers
        if msg.trim().is_empty() || msg.starts_with("*(Cont...) Error:") && msg.lines().count() <= 1
        {
            log::warn!(
                "Skipping empty or header-only message chunk {}/{}",
                i + 1,
                messages_to_send.len()
            );
            continue;
        }

        let body = json!({ "text": msg });
        match client.post(&google_webhook_url).json(&body).send().await {
            Ok(res) => {
                if res.status().is_success() {
                    log::info!(
                        "Successfully sent message chunk {}/{} to Google Chat",
                        i + 1,
                        messages_to_send.len()
                    );
                } else {
                    log::error!(
                        "Failed to send message chunk {}/{} to Google Chat: Status {} Body: {:?}",
                        i + 1,
                        messages_to_send.len(),
                        res.status(),
                        res.text()
                            .await
                            .unwrap_or_else(|_| "<Failed to read body>".to_string())
                    );
                    // Consider adding error handling strategy here (e.g., stop sending?)
                }
            }
            Err(e) => {
                log::error!(
                    "Error sending message chunk {}/{} to Google Chat: {} ; body: {}, msg: {}, google_webhook_url: {}",
                    i + 1,
                    messages_to_send.len(),
                    e,
                    body,
                    msg,
                    google_webhook_url
                );
                // Consider adding error handling strategy here
            }
        }

        // Add a small delay between messages if needed to avoid rate limiting
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    log::info!("Snapshot alert job finished: Alert processing complete.");
    Ok(())
}
