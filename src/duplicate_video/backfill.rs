use crate::{
    app_state,
    qstash::duplicate::{VideoHashDuplication, VideoPublisherData},
};
use anyhow::Context;
use axum::{extract::Query, extract::State, Json};
use google_cloud_bigquery::http::job::query::QueryRequest;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::Mutex;

// Enhanced tracking for backfill jobs
static IS_BACKFILL_RUNNING: Mutex<bool> = Mutex::const_new(false);
static PROCESSED_COUNT: Mutex<usize> = Mutex::const_new(0);
static TOTAL_VIDEOS: Mutex<usize> = Mutex::const_new(0);
static FAILED_VIDEOS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
static LAST_PROCESSED_VIDEO: Mutex<String> = Mutex::const_new(String::new());

// Helper function to get the FAILED_VIDEOS mutex
fn failed_videos() -> &'static Mutex<HashSet<String>> {
    FAILED_VIDEOS.get_or_init(|| Mutex::new(HashSet::new()))
}

#[derive(Debug, Deserialize)]
pub struct BackfillQueryParams {
    batch_size: Option<usize>,
    max_batches: Option<usize>,
    resume_from: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct BackfillResponse {
    status: String,
    message: String,
    processed_count: Option<usize>,
    total_videos: Option<usize>,
    failed_count: Option<usize>,
    last_processed: Option<String>,
    estimated_completion: Option<String>,
}

pub async fn videohash_backfill_handler(
    State(state): State<Arc<app_state::AppState>>,
    Query(params): Query<BackfillQueryParams>,
) -> Json<BackfillResponse> {
    // Check if backfill is already running
    let mut is_running = IS_BACKFILL_RUNNING.lock().await;

    if *is_running {
        let processed = *PROCESSED_COUNT.lock().await;
        let total = *TOTAL_VIDEOS.lock().await;
        let failed = failed_videos().lock().await.len();
        let last_processed = LAST_PROCESSED_VIDEO.lock().await.clone();

        // Calculate estimated completion time
        let estimated_completion = if processed > 0 && total > processed {
            let percent_complete = processed as f64 / total as f64;
            let remaining_percent = 1.0 - percent_complete;

            // Assume current rate continues
            let now = chrono::Utc::now();
            let start_time = now - chrono::Duration::minutes((processed as i64 / 10) * 2); // Rough estimate: 2 minutes per 10 videos
            let elapsed = (now - start_time).num_seconds() as f64;
            let estimated_remaining = (elapsed / percent_complete) * remaining_percent;

            let completion_time = now + chrono::Duration::seconds(estimated_remaining as i64);
            Some(completion_time.to_rfc3339())
        } else {
            None
        };

        return Json(BackfillResponse {
            status: "in_progress".to_string(),
            message: "Backfill job is already running".to_string(),
            processed_count: Some(processed),
            total_videos: Some(total),
            failed_count: Some(failed),
            last_processed: Some(last_processed),
            estimated_completion,
        });
    }

    *is_running = true;

    // Reset counters if not resuming
    if params.resume_from.is_none() {
        *PROCESSED_COUNT.lock().await = 0;
        *TOTAL_VIDEOS.lock().await = 0;
        failed_videos().lock().await.clear();
        *LAST_PROCESSED_VIDEO.lock().await = String::new();
    }

    // Get parameters with defaults
    let batch_size = params.batch_size.unwrap_or(10);
    let max_batches = params.max_batches;
    let resume_from = params.resume_from.clone();
    let state_clone = state.clone();

    tokio::spawn(async move {
        if let Err(e) =
            run_videohash_backfill(state_clone, batch_size, max_batches, resume_from).await
        {
            error!("Backfill job failed: {}", e);
        }

        let mut is_running = IS_BACKFILL_RUNNING.lock().await;
        *is_running = false;
    });

    Json(BackfillResponse {
        status: "started".to_string(),
        message: format!(
            "Backfill job started with batch_size={}, max_batches={:?}, resume_from={:?}",
            batch_size, max_batches, params.resume_from
        ), // Use original here
        processed_count: Some(0),
        total_videos: None,
        failed_count: Some(0),
        last_processed: None,
        estimated_completion: None,
    })
}

pub async fn backfill_status_handler() -> Json<BackfillResponse> {
    let is_running = *IS_BACKFILL_RUNNING.lock().await;
    let processed = *PROCESSED_COUNT.lock().await;
    let total = *TOTAL_VIDEOS.lock().await;
    let failed = failed_videos().lock().await.len();
    let last_processed = LAST_PROCESSED_VIDEO.lock().await.clone();

    let estimated_completion = if is_running && processed > 0 && total > processed {
        let percent_complete = processed as f64 / total as f64;
        let remaining_percent = 1.0 - percent_complete;

        let now = chrono::Utc::now();
        let start_time = now - chrono::Duration::minutes((processed as i64 / 10) * 2); // Rough estimate: 2 minutes per 10 videos
        let elapsed = (now - start_time).num_seconds() as f64;
        let estimated_remaining = (elapsed / percent_complete) * remaining_percent;

        let completion_time = now + chrono::Duration::seconds(estimated_remaining as i64);
        Some(completion_time.to_rfc3339())
    } else {
        None
    };

    Json(BackfillResponse {
        status: if is_running { "in_progress" } else { "idle" }.to_string(),
        message: if is_running {
            format!(
                "Backfill job is running - {}% complete",
                if total > 0 {
                    (processed as f64 / total as f64 * 100.0).round() as usize
                } else {
                    0
                }
            )
        } else {
            if processed > 0 {
                format!(
                    "Backfill complete. Processed {} of {} videos with {} failures",
                    processed, total, failed
                )
            } else {
                "No backfill job is currently running".to_string()
            }
        },
        processed_count: Some(processed),
        total_videos: Some(total),
        failed_count: Some(failed),
        last_processed: if !last_processed.is_empty() {
            Some(last_processed)
        } else {
            None
        },
        estimated_completion,
    })
}

pub async fn failed_videos_handler() -> Json<Vec<String>> {
    let failed_videos = failed_videos().lock().await.clone().into_iter().collect();
    Json(failed_videos)
}

async fn run_videohash_backfill(
    state: Arc<app_state::AppState>,
    batch_size: usize,
    max_batches: Option<usize>,
    resume_from: Option<String>,
) -> anyhow::Result<()> {
    info!(
        "Starting videohash backfill job with batch_size={}, resume_from={:?}",
        batch_size, resume_from
    );

    let duplication_handler =
        VideoHashDuplication::new(&state.qstash_client.client, &state.qstash_client.base_url);

    let bigquery_client = app_state::init_bigquery_client().await;
    let mut processed_count = *PROCESSED_COUNT.lock().await;
    let mut batch_count = 0;

    let resume_clause = match &resume_from {
        Some(last_video) => format!(" AND SUBSTR(uri, 18, LENGTH(uri) - 21) > '{}' ", last_video),
        None => "".to_string(),
    };

    let count_query = format!(
        "SELECT COUNT(*) as count FROM `hot-or-not-feed-intelligence`.`yral_ds`.`video_object_table` AS t
         WHERE SUBSTR(uri, 18, LENGTH(uri) - 21) NOT IN (
           SELECT video_id FROM `hot-or-not-feed-intelligence.yral_ds.videohash_original`
         ){}",
        resume_clause
    );

    let count_request = QueryRequest {
        query: count_query,
        ..Default::default()
    };

    let count_response = bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &count_request)
        .await
        .context("Failed to query BigQuery for video count")?;

    if let Some(rows) = count_response.rows {
        if let Some(row) = rows.first() {
            if row.f.len() > 0 {
                // Convert to string using format!() and debug formatting
                let count_str = format!("{:?}", row.f[0].v);
                // Try to extract the number from the debug output
                if let Some(count) = count_str
                    .trim_matches(|c| c == '"' || c == '\\')
                    .parse::<usize>()
                    .ok()
                {
                    let mut total = TOTAL_VIDEOS.lock().await;
                    *total = count;
                    info!("Found {} videos to process", count);
                }
            }
        }
    }

    let mut last_video_id = resume_from.unwrap_or_default();

    loop {
        if let Some(max) = max_batches {
            if batch_count >= max {
                info!(
                    "Reached maximum number of batches ({}). Stopping backfill.",
                    max
                );
                break;
            }
        }

        let resume_clause = if !last_video_id.is_empty() {
            format!(
                " AND SUBSTR(uri, 18, LENGTH(uri) - 21) > '{}' ",
                last_video_id
            )
        } else {
            "".to_string()
        };

        let query = format!(
            "SELECT
              SUBSTR(uri, 18, LENGTH(uri) - 21) as video_id,
              (SELECT value FROM UNNEST(t.metadata) WHERE name = 'canister_id') AS canister_id,
              (SELECT value FROM UNNEST(t.metadata) WHERE name = 'post_id') AS post_id
            FROM
              `hot-or-not-feed-intelligence`.`yral_ds`.`video_object_table` AS t
            WHERE SUBSTR(uri, 18, LENGTH(uri) - 21) NOT IN (
              SELECT video_id FROM `hot-or-not-feed-intelligence.yral_ds.videohash_original`
            ){}
            ORDER BY SUBSTR(uri, 18, LENGTH(uri) - 21) ASC
            LIMIT {}",
            resume_clause, batch_size
        );

        let request = QueryRequest {
            query,
            ..Default::default()
        };

        let response = bigquery_client
            .job()
            .query("hot-or-not-feed-intelligence", &request)
            .await
            .context("Failed to query BigQuery for videos")?;

        let rows = match response.rows {
            Some(rows) => rows,
            None => {
                info!("No more videos to process. Backfill complete.");
                break;
            }
        };

        if rows.is_empty() {
            info!("No more videos to process. Backfill complete.");
            break;
        }

        batch_count += 1;
        info!(
            "Processing batch {} with {} videos",
            batch_count,
            rows.len()
        );

        // Process each video in the batch
        for row in rows {
            if row.f.len() < 3 {
                warn!("Incomplete row data: {:?}", row);
                continue;
            }

            // Extract video metadata using debug formatting and string manipulation
            let video_id_raw = format!("{:?}", row.f[0].v);
            let video_id = video_id_raw
                .trim_matches(|c| c == '"' || c == '\\')
                .to_string();
            if video_id.is_empty() {
                warn!("Empty video ID: {:?}", row.f[0].v);
                continue;
            }

            // Update last processed video for resume functionality
            last_video_id = video_id.clone();
            *LAST_PROCESSED_VIDEO.lock().await = video_id.clone();

            let canister_id_raw = format!("{:?}", row.f[1].v);
            let canister_id = canister_id_raw
                .trim_matches(|c| c == '"' || c == '\\')
                .to_string();
            if canister_id.is_empty() {
                warn!("Empty canister ID for video {}: {:?}", video_id, row.f[1].v);
                continue;
            }

            let post_id_raw = format!("{:?}", row.f[2].v);
            let post_id_str = post_id_raw.trim_matches(|c| c == '"' || c == '\\');
            let post_id = match post_id_str.parse::<u64>() {
                Ok(id) => id,
                Err(e) => {
                    warn!(
                        "Invalid post_id format for video {}: {} - {}",
                        video_id, post_id_str, e
                    );
                    continue;
                }
            };

            let video_url = format!(
                "https://customer-2p3jflss4r4hmpnz.cloudflarestream.com/{}/downloads/default.mp4",
                video_id
            );

            let publisher_data = VideoPublisherData {
                canister_id: canister_id.clone(),
                publisher_principal: "unknown".to_string(),
                post_id,
            };

            info!(
                "Processing video {}/{}: {}",
                processed_count + 1,
                *TOTAL_VIDEOS.lock().await,
                video_id
            );

            // Process the video using the existing functions from duplicate.rs
            // We're reusing the VideoHashDuplication methods for consistency
            match process_single_video(&duplication_handler, &video_id, &video_url, &publisher_data)
                .await
            {
                Ok(_) => {
                    info!("Successfully processed video {}", video_id);
                    processed_count += 1;

                    // Update the global counter
                    let mut counter = PROCESSED_COUNT.lock().await;
                    *counter = processed_count;
                }
                Err(e) => {
                    error!("Error processing video {}: {}", video_id, e);
                    // Add to failed videos list
                    failed_videos().lock().await.insert(video_id.clone());
                }
            }

            // Add small delay between videos to prevent rate limiting
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }

        info!(
            "Completed batch {}. Total videos processed: {}/{}",
            batch_count,
            processed_count,
            *TOTAL_VIDEOS.lock().await
        );

        // Add a small delay between batches to reduce database load
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }

    info!(
        "Videohash backfill job completed. Processed {} videos in {} batches",
        processed_count, batch_count
    );
    Ok(())
}

async fn process_single_video(
    duplication_handler: &VideoHashDuplication<'_>,
    video_id: &str,
    video_url: &str,
    publisher_data: &VideoPublisherData,
) -> anyhow::Result<()> {
    info!("Processing video {} at URL {}", video_id, video_url);

    // Generate video hash with timeout protection
    let start = std::time::Instant::now();
    info!("Generating hash for video: {}", video_id);

    // Add timeout for video hash generation to prevent stuck processes
    let hash_future = crate::duplicate_video::videohash::VideoHash::from_url(video_url);
    let video_hash = match tokio::time::timeout(
        tokio::time::Duration::from_secs(300), // 5 minute timeout
        hash_future,
    )
    .await
    {
        Ok(result) => {
            result.map_err(|e| anyhow::anyhow!("Failed to generate video hash: {}", e))?
        }
        Err(_) => {
            return Err(anyhow::anyhow!(
                "Timeout generating video hash after 5 minutes"
            ))
        }
    };

    info!(
        "Hash generation for video {} completed in {:?}",
        video_id,
        start.elapsed()
    );

    // 2. Store the original hash directly to BigQuery
    let bigquery_client = app_state::init_bigquery_client().await;

    let query = format!(
        "INSERT INTO `hot-or-not-feed-intelligence.yral_ds.videohash_original` 
         (video_id, videohash, created_at) 
         VALUES ('{}', '{}', CURRENT_TIMESTAMP())",
        video_id, video_hash.hash
    );

    let request = QueryRequest {
        query,
        ..Default::default()
    };

    info!(
        "Storing hash in videohash_original for video_id: {}",
        video_id
    );

    bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await?;

    // 3. Submit to the videohash indexer API - similar to what's in process_video_deduplication
    let client = reqwest::Client::new();

    // Add timeout for indexer API call
    let response = tokio::time::timeout(
        tokio::time::Duration::from_secs(60), // 1 minute timeout
        client
            .post("https://videohash-indexer.fly.dev/search")
            .json(&serde_json::json!({
                "video_id": video_id,
                "hash": video_hash.hash,
            }))
            .send(),
    )
    .await??;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!(
            "VideoHash Indexer API failed: {} - {}",
            status,
            error_text
        ));
    }

    // No further processing needed for backfill
    info!("Successfully processed video {}", video_id);
    Ok(())
}
