use std::{env, io::Write, process::Stdio, sync::Arc};

use axum::{extract::State, response::IntoResponse, Json};
use candid::Principal;
use chrono::{DateTime, Duration, Utc};
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
            all_canisters.extend(subnet_orch_ids);
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
    let mut canisters_missing_backups: Vec<String> = Vec::new();
    let mut canisters_outdated_backups: Vec<String> = Vec::new();
    let now = Utc::now();
    let alert_threshold = Duration::days(4);

    log::info!("Checking backup status for each canister...");
    for canister_id in all_canisters {
        match backup_dates_map.get(&canister_id) {
            Some(dates) => {
                if let Some(latest_backup_date) = dates.last() {
                    let time_since_last_backup = now.signed_duration_since(*latest_backup_date);
                    if time_since_last_backup > alert_threshold {
                        // Consider adding humantime crate for better duration logging:
                        // humantime::format_duration(time_since_last_backup.to_std().unwrap_or_default())
                        log::warn!(
                            "Canister {} needs alert: Last backup was on {} ({} days ago > {} day threshold)",
                            canister_id.to_string(),
                            latest_backup_date.format("%Y-%m-%d"),
                            time_since_last_backup.num_days(), // Simple day count
                            alert_threshold.num_days()
                        );
                        canisters_outdated_backups.push(canister_id.to_string());
                    }
                } else {
                    // This case should ideally not happen if canister_id is in the map,
                    // but handles empty Vec just in case.
                    log::warn!(
                        "Canister {} needs alert: Backup entry exists but date list is empty.",
                        canister_id.to_string()
                    );
                    canisters_missing_backups.push(canister_id.to_string());
                }
            }
            None => {
                log::warn!(
                    "Canister {} needs alert: No backup found in Storj.",
                    canister_id.to_string()
                );
                canisters_missing_backups.push(canister_id.to_string());
            }
        }
    }

    if canisters_missing_backups.is_empty() && canisters_outdated_backups.is_empty() {
        log::info!("Snapshot alert job finished: All canisters have recent backups.");
    } else {
        log::warn!(
            "Snapshot alert job finished: {} canisters require attention (missing: {:?} \n outdated: {:?})",
            canisters_missing_backups.len() + canisters_outdated_backups.len(),
            canisters_missing_backups, canisters_outdated_backups
        );
    }

    send_google_chat_alert(canisters_missing_backups, canisters_outdated_backups).await?;

    Ok(())
}

#[cfg(not(feature = "use-uplink"))]
pub async fn snapshot_alert_job_impl(_agent: &Agent) -> Result<(), anyhow::Error> {
    // Add agent param here too
    log::warn!("Uplink is not enabled, skipping snapshot alert job");
    Ok(())
}

// splits messages to chunks of 10K characters and sends them to Google Chat
async fn send_google_chat_alert(
    canisters_missing_backups: Vec<String>,
    canisters_outdated_backups: Vec<String>,
) -> Result<(), anyhow::Error> {
    let google_webhook_url = env::var("CANISTER_BACKUP_ALERT_GOOGLE_CHAT_WEBHOOK_URL")
        .map_err(|_| anyhow::anyhow!("CANISTER_BACKUP_ALERT_GOOGLE_CHAT_WEBHOOK_URL not set"))?;
    let client = reqwest::Client::new();
    let total_attention_count = canisters_missing_backups.len() + canisters_outdated_backups.len();

    if total_attention_count == 0 {
        let body = json!({
            "text": "Snapshot alert job finished: All canisters have recent backups. ✅"
        });
        let res = client.post(&google_webhook_url).json(&body).send().await?;
        if res.status().is_success() {
            log::info!("Snapshot alert job finished: 'All clear' status sent to Google Chat");
        } else {
            log::error!(
                "Snapshot alert job finished: Failed to send 'All clear' status to Google Chat: {}",
                res.status()
            );
        }
        return Ok(());
    }

    let max_chars_per_message = 10000; // Reduced Google Chat message character limit is 12K.
    let mut messages_to_send = Vec::new();
    let mut current_message = format!(
        "🚨 Snapshot alert job finished: *{}* canisters require attention!\n\n",
        total_attention_count
    );
    let mut current_char_count = current_message.len();
    let mut first_item_in_section = true; // Track if we need a comma before the next item

    let missing_header = "*Missing backups:* ❓\n";
    let missing_continuation_header = "(Cont...) *Missing backups cont.:* ❓\n";
    let outdated_header = "\n*Outdated backups (older than 4 days):* ⏳\n";
    let outdated_continuation_header = "(Cont...) *Outdated backups cont.:* ⏳\n";

    let mut added_missing_header = false;

    // --- Process missing backups ---
    if !canisters_missing_backups.is_empty() {
        // Add header or continuation header
        let header_to_add = missing_header;
        if current_char_count + header_to_add.len() > max_chars_per_message {
            messages_to_send.push(current_message);
            current_message = String::new(); // Start fresh for the new message
            current_char_count = 0;
        }
        // If starting a new message, use the full header, otherwise just append
        if current_message.is_empty() {
            current_message.push_str(header_to_add); // Start new message with header
            current_char_count = current_message.len();
        } else {
            current_message.push_str(header_to_add); // Append header to existing message
            current_char_count += header_to_add.len();
        }
        added_missing_header = true;
        first_item_in_section = true;

        for canister_id in canisters_missing_backups.iter() {
            let delimiter = if first_item_in_section { "" } else { ", " };
            let chars_to_add = delimiter.len() + canister_id.len();

            if current_char_count + chars_to_add > max_chars_per_message {
                messages_to_send.push(current_message.clone()); // Finalize previous chunk
                current_message = missing_continuation_header.to_string(); // Start new chunk
                current_char_count = current_message.len();
                first_item_in_section = true; // Reset for new chunk
                let delimiter = if first_item_in_section { "" } else { ", " }; // Re-evaluate delimiter for the new line
                current_message.push_str(delimiter);
                current_message.push_str(canister_id);
                current_char_count += delimiter.len() + canister_id.len();
                first_item_in_section = false;
            } else {
                current_message.push_str(delimiter);
                current_message.push_str(canister_id);
                current_char_count += chars_to_add;
                first_item_in_section = false;
            }
        }
        current_message.push('\n'); // Add newline after the list section
        current_char_count += 1;
    }

    // --- Process outdated backups ---
    if !canisters_outdated_backups.is_empty() {
        let header_to_add = if added_missing_header {
            outdated_header
        } else {
            outdated_header.trim_start_matches('\n')
        }; // Handle spacing
        let continuation_header_to_use = outdated_continuation_header;

        // Check if header fits in the current message
        if current_char_count + header_to_add.len() > max_chars_per_message {
            messages_to_send.push(current_message.clone());
            current_message = String::new(); // Start fresh
            current_char_count = 0;
        }

        // Add header (either to new or existing message)
        if current_message.is_empty() {
            current_message.push_str(header_to_add);
            current_char_count = current_message.len();
        } else {
            // Ensure newline separation if needed and not already present
            if !current_message.ends_with("\n\n")
                && !current_message.ends_with(missing_header)
                && added_missing_header
            {
                current_message.push('\n');
                current_char_count += 1;
            }
            current_message.push_str(header_to_add);
            current_char_count += header_to_add.len();
        }
        first_item_in_section = true;

        for canister_id in canisters_outdated_backups.iter() {
            let delimiter = if first_item_in_section { "" } else { ", " };
            let chars_to_add = delimiter.len() + canister_id.len();

            if current_char_count + chars_to_add > max_chars_per_message {
                messages_to_send.push(current_message.clone()); // Finalize previous chunk
                current_message = continuation_header_to_use.to_string(); // Start new chunk
                current_char_count = current_message.len();
                first_item_in_section = true; // Reset for new chunk
                let delimiter = if first_item_in_section { "" } else { ", " }; // Re-evaluate delimiter
                current_message.push_str(delimiter);
                current_message.push_str(canister_id);
                current_char_count += delimiter.len() + canister_id.len();
                first_item_in_section = false;
            } else {
                current_message.push_str(delimiter);
                current_message.push_str(canister_id);
                current_char_count += chars_to_add;
                first_item_in_section = false;
            }
        }
        current_message.push('\n'); // Add newline after the list section
        current_char_count += 1;
    }

    // Add the final message chunk if it contains content
    if !current_message.is_empty() && !messages_to_send.contains(&current_message) {
        // Avoid adding if it's identical to the last message already pushed (can happen if a header exactly fills a message)
        if messages_to_send
            .last()
            .map_or(true, |last| last != &current_message)
        {
            messages_to_send.push(current_message);
        }
    }

    // Send all the messages
    log::info!(
        "Sending {} alert message chunk(s) to Google Chat...",
        messages_to_send.len()
    );
    for (i, msg) in messages_to_send.iter().enumerate() {
        if msg.trim().is_empty()
            || msg == missing_continuation_header
            || msg == outdated_continuation_header
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
                    // Decide if continuing makes sense
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
                // Decide if continuing makes sense
            }
        }

        // Add a small delay between messages if needed
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    log::info!("Snapshot alert job finished: Alert processing complete.");
    Ok(())
}
