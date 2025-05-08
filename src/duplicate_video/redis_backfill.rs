use crate::app_state;
use axum::{extract::Query, extract::State, http::HeaderMap, Json};
use google_cloud_bigquery::client::Client;
use google_cloud_bigquery::http::job::query::QueryRequest;
use log::{error, info, warn};
use redis::AsyncCommands;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::{env, sync::Arc};

#[derive(Debug, Deserialize)]
pub struct BackfillQueryParams {
    batch_size: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct BackfillResponse {
    message: String,
    hashes_loaded: usize,
}

/// Endpoint to trigger Redis backfill for videohash data
pub async fn trigger_redis_backfill(
    State(state): State<Arc<app_state::AppState>>,
    headers: HeaderMap,
    Query(params): Query<BackfillQueryParams>,
) -> Result<Json<BackfillResponse>, StatusCode> {
    // Extract Bearer token from headers
    let auth_token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim_start_matches("Bearer ").to_string())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    // Get token from environment variable
    let expected_token = match env::var("VIDEOHASH_BACKFILL_TOKEN") {
        Ok(token) => token,
        Err(_) => {
            error!("VIDEOHASH_BACKFILL_TOKEN environment variable not set");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    // Validate the bearer token
    if auth_token != expected_token {
        warn!("Unauthorized access attempt to redis backfill endpoint");
        return Err(StatusCode::UNAUTHORIZED);
    }

    // Get parameters with defaults
    let batch_size = params.batch_size.unwrap_or(1000);

    info!(
        "Starting Redis videohash backfill job with batch_size={}",
        batch_size
    );

    // Execute the backfill
    let hashes_loaded = execute_redis_backfill(&state, batch_size)
        .await
        .map_err(|e| {
            error!("Redis backfill execution error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(BackfillResponse {
        message: format!("Loaded {} video hashes into Redis", hashes_loaded),
        hashes_loaded,
    }))
}

async fn execute_redis_backfill(
    state: &Arc<app_state::AppState>,
    batch_size: usize,
) -> anyhow::Result<usize> {
    info!("Using existing BigQuery client from app state");
    let bigquery_client = &state.bigquery_client;
    let redis_client = app_state::init_redis_client();

    // Connect to Redis
    let mut redis_conn = redis_client
        .get_async_connection()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to Redis: {}", e))?;

    // Get total count first to track progress
    let count_query =
        "SELECT COUNT(*) as total FROM `hot-or-not-feed-intelligence.yral_ds.video_unique`";

    info!("Counting total records in video_unique table");

    let count_request = QueryRequest {
        query: count_query.to_string(),
        ..Default::default()
    };

    let count_response = bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &count_request)
        .await?;

    let total_count = match count_response.rows {
        Some(rows) if !rows.is_empty() => {
            let cell_value = &rows[0].f[0].v;
            match cell_value {
                google_cloud_bigquery::http::tabledata::list::Value::String(s) => {
                    s.parse::<usize>().unwrap_or(0)
                }
                _ => 0,
            }
        }
        _ => 0,
    };

    info!("Found total of {} video hashes to load", total_count);

    // Now start loading in batches
    let mut offset = 0;
    let mut loaded_count = 0;

    while loaded_count < total_count {
        let query = format!(
            "SELECT video_id, videohash 
             FROM `hot-or-not-feed-intelligence.yral_ds.video_unique`
             ORDER BY video_id 
             LIMIT {} OFFSET {}",
            batch_size, offset
        );

        info!(
            "Executing BigQuery query with LIMIT {} OFFSET {}",
            batch_size, offset
        );

        let request = QueryRequest {
            query,
            timeout_ms: Some(60000),
            ..Default::default()
        };

        let response = bigquery_client
            .job()
            .query("hot-or-not-feed-intelligence", &request)
            .await?;

        let rows = match response.rows {
            Some(rows) => rows,
            None => {
                info!("No more rows to process");
                break;
            }
        };

        if rows.is_empty() {
            info!("No more rows to process");
            break;
        }

        info!("Retrieved {} hashes from BigQuery", rows.len());

        // Process in smaller chunks for Redis pipeline
        const REDIS_CHUNK_SIZE: usize = 100;
        for chunk in rows.chunks(REDIS_CHUNK_SIZE) {
            // Start a Redis pipeline for bulk insert
            let mut pipe = redis::pipe();
            pipe.cmd("MULTI");

            for row in chunk {
                if row.f.len() >= 2 {
                    let video_id = match &row.f[0].v {
                        google_cloud_bigquery::http::tabledata::list::Value::String(s) => s,
                        _ => continue,
                    };

                    let videohash = match &row.f[1].v {
                        google_cloud_bigquery::http::tabledata::list::Value::String(s) => s,
                        _ => continue,
                    };

                    if !video_id.is_empty() && !videohash.is_empty() {
                        // Add each hash to the pipeline
                        pipe.hset("videohashes", videohash, video_id);
                        loaded_count += 1;
                    }
                }
            }

            pipe.cmd("EXEC");

            // Execute the pipeline
            pipe.query_async::<_, ()>(&mut redis_conn)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to store hashes in Redis: {}", e))?;

            info!("Successfully loaded {} hashes into Redis", chunk.len());
        }

        // Update offset for next batch
        offset += rows.len();
        info!(
            "Progress: {}/{} ({:.1}%)",
            loaded_count,
            total_count,
            (loaded_count as f64 / total_count as f64) * 100.0
        );
    }

    info!(
        "Redis backfill completed successfully. Loaded {} video hashes.",
        loaded_count
    );
    Ok(loaded_count)
}
