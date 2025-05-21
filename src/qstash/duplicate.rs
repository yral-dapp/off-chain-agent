use crate::app_state::AppState;
use crate::{app_state, consts::OFF_CHAIN_AGENT_URL, duplicate_video::videohash::VideoHash};
use chrono::{DateTime, Utc};
use google_cloud_bigquery::client::Client;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::http::tabledata::insert_all::{InsertAllRequest, Row};
use http::header::CONTENT_TYPE;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct VideoPublisherData {
    pub canister_id: String,
    pub publisher_principal: String,
    pub post_id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DuplicateVideoEvent {
    pub original_video_id: String,
    pub parent_video_id: String,
    pub similarity_percentage: f64,
    pub exact_duplicate: bool,
    pub publisher_canister_id: String,
    pub publisher_principal: String,
    pub post_id: u64,
    pub timestamp: String,
}

// For videohash_original table
#[derive(Serialize)]
struct VideohashOriginalRow {
    video_id: String,
    videohash: String,
    created_at: DateTime<Utc>,
}

// For video_unique table
#[derive(Serialize)]
struct VideoUniqueRow {
    video_id: String,
    videohash: String,
    created_at: DateTime<Utc>,
}

// The VideoHashDuplication struct will contain the deduplication logic
pub struct VideoHashDuplication<'a> {
    client: &'a reqwest::Client,
    base_url: &'a reqwest::Url,
}

impl<'a> VideoHashDuplication<'a> {
    pub fn new(client: &'a reqwest::Client, base_url: &'a reqwest::Url) -> Self {
        Self { client, base_url }
    }

    pub async fn publish_duplicate_video_event(
        &self,
        duplicate_event: DuplicateVideoEvent,
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL
            .join("qstash/duplicate_video_detected")
            .unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!(duplicate_event);

        log::info!(
            "Publishing duplicate video event: video_id [{}], parent_video_id [{}], score={}",
            duplicate_event.original_video_id,
            duplicate_event.parent_video_id,
            duplicate_event.similarity_percentage
        );

        self.client
            .post(url.clone())
            .json(&req)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .send()
            .await?;

        Ok(())
    }

    pub async fn process_video_deduplication(
        &self,
        video_id: &str,
        video_url: &str,
        publisher_data: VideoPublisherData,
        app_state: &Arc<AppState>,
        publish_video_callback: impl FnOnce(
            &str,
            &str,
            u64,
            String,
            &str,
        )
            -> futures::future::BoxFuture<'a, Result<(), anyhow::Error>>,
    ) -> Result<(), anyhow::Error> {
        log::info!("Calculating videohash for video URL: {}", video_url);
        let video_hash = VideoHash::from_url(video_url)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to generate videohash: {}", e))?;

        let bigquery_client = &app_state.bigquery_client;
        self.store_videohash_original(video_id, &video_hash.hash, bigquery_client)
            .await?;
        log::info!(
            "Storing videohash in videohash_original_testing_duplicate: {}",
            video_hash.hash
        );

        let redis_client = &app_state.redis_client;
        let mut redis_conn = redis_client
            .get_async_connection()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to Redis: {}", e))?;

        let is_duplicate: bool = redis_conn
            .hexists("videohashes", &video_hash.hash)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to query Redis: {}", e))?;

        if !is_duplicate {
            log::info!("Unique video recorded: video_id [{}]", video_id);
            self.store_unique_video(video_id, &video_hash.hash, bigquery_client)
                .await?;

            redis_conn
                .hset("videohashes", &video_hash.hash, video_id)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to store hash in Redis: {}", e))?;
            log::info!("Stored videohash in Redis: {}", video_hash.hash);
        } else {
            log::info!(
                "Duplicate video detected: video_id [{}], not storing in video_unique",
                video_id
            );
        }

        // Always continue with the callback regardless of duplication status
        let timestamp = chrono::Utc::now().to_rfc3339();
        publish_video_callback(
            video_id,
            &publisher_data.canister_id,
            publisher_data.post_id,
            timestamp,
            &publisher_data.publisher_principal,
        )
        .await?;

        Ok(())
    }

    async fn store_videohash_original(
        &self,
        video_id: &str,
        hash: &str,
        bigquery_client: &Client,
    ) -> Result<(), anyhow::Error> {
        let now = Utc::now();

        let row_data = VideohashOriginalRow {
            video_id: video_id.to_string(),
            videohash: hash.to_string(),
            created_at: now,
        };

        let row = Row {
            insert_id: None,
            json: row_data,
        };

        let request = InsertAllRequest {
            rows: vec![row],
            ..Default::default()
        };

        log::info!(
            "Storing hash in videohash_original_testing_duplicate for video_id [{}]",
            video_id
        );

        let res = bigquery_client
            .tabledata()
            .insert(
                "hot-or-not-feed-intelligence",
                "yral_ds",
                "videohash_original_testing_duplicate",
                &request,
            )
            .await?;

        if let Some(errors) = res.insert_errors {
            if !errors.is_empty() {
                log::error!("videohash_original_testing_duplicate insert errors: {:?}", errors);
                return Err(anyhow::anyhow!(
                    "Failed to insert videohash_original_testing_duplicate row to BigQuery"
                ));
            }
        }

        Ok(())
    }

    async fn store_unique_video(
        &self,
        video_id: &str,
        hash: &str,
        bigquery_client: &Client,
    ) -> Result<(), anyhow::Error> {
        let now = Utc::now();

        let row_data = VideoUniqueRow {
            video_id: video_id.to_string(),
            videohash: hash.to_string(),
            created_at: now,
        };

        let row = Row {
            insert_id: None,
            json: row_data,
        };

        let request = InsertAllRequest {
            rows: vec![row],
            ..Default::default()
        };

        log::info!(
            "Storing unique video in video_unique_testing_duplicate for video_id [{}]",
            video_id
        );

        let res = bigquery_client
            .tabledata()
            .insert(
                "hot-or-not-feed-intelligence",
                "yral_ds",
                "video_unique_testing_duplicate",
                &request,
            )
            .await?;

        if let Some(errors) = res.insert_errors {
            if !errors.is_empty() {
                log::error!("video_unique_testing_duplicate insert errors: {:?}", errors);
                return Err(anyhow::anyhow!(
                    "Failed to insert video_unique_testing_duplicate row to BigQuery"
                ));
            }
        }

        Ok(())
    }
}
