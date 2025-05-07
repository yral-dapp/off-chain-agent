use crate::{app_state, consts::OFF_CHAIN_AGENT_URL, duplicate_video::videohash::VideoHash};
use google_cloud_bigquery::client::Client;
use google_cloud_bigquery::http::job::query::QueryRequest;
use http::header::CONTENT_TYPE;
use serde::{Deserialize, Serialize};

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

// Add these structures to support the indexer API response
#[derive(Debug, Deserialize)]
struct VideoHashIndexerResponse {
    match_found: bool,
    match_details: Option<MatchDetails>,
    hash_added: bool,
}

#[derive(Debug, Deserialize)]
struct MatchDetails {
    video_id: String,
    similarity_percentage: f64,
    is_duplicate: bool,
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

        // Get BigQuery client once
        let bigquery_client = app_state::init_bigquery_client().await;

        // Store the original hash
        self.store_videohash_original(video_id, &video_hash.hash, &bigquery_client)
            .await?;

        // Get Redis client and connection
        let redis_client = app_state::init_redis_client();
        let mut redis_conn = redis_client
            .get_async_connection()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to Redis: {}", e))?;

        // Check if hash exists in Redis hash
        let is_duplicate: bool = redis::cmd("HEXISTS")
            .arg("videohashes")
            .arg(&video_hash.hash)
            .query_async(&mut redis_conn)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to query Redis: {}", e))?;

        if is_duplicate {
            // Get the original video ID from the hash
            let parent_video_id: String = redis::cmd("HGET")
                .arg("videohashes")
                .arg(&video_hash.hash)
                .query_async(&mut redis_conn)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to get parent video ID from Redis: {}", e))?;

            log::info!(
                "Duplicate video detected: video_id [{}] is identical to parent_video_id [{}]",
                video_id,
                parent_video_id
            );

            // Store duplicate in BigQuery with 100% match (exact duplicate)
            self.store_duplicate_video_exact(
                video_id,
                &video_hash.hash,
                &parent_video_id,
                &publisher_data,
                &bigquery_client,
            )
            .await?;
        } else {
            // This is a unique video
            log::info!("Unique video recorded: video_id [{}]", video_id);

            // Store as unique in BigQuery
            self.store_unique_video(video_id, &video_hash.hash, &bigquery_client)
                .await?;

            // Store the hash in Redis hash with HSET
            redis::cmd("HSET")
                .arg("videohashes")
                .arg(&video_hash.hash)
                .arg(video_id)
                .query_async::<_, ()>(&mut redis_conn)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to store hash in Redis: {}", e))?;
        }

        // Remaining code...
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
        let query = format!(
            "INSERT INTO `hot-or-not-feed-intelligence.yral_ds.videohash_original` 
             (video_id, videohash, created_at) 
             VALUES ('{}', '{}', CURRENT_TIMESTAMP())",
            video_id, hash
        );

        let request = QueryRequest {
            query,
            ..Default::default()
        };

        log::info!(
            "Storing hash in videohash_original for video_id [{}]",
            video_id
        );

        bigquery_client
            .job()
            .query("hot-or-not-feed-intelligence", &request)
            .await?;

        Ok(())
    }

    async fn store_unique_video(
        &self,
        video_id: &str,
        hash: &str,
        bigquery_client: &Client,
    ) -> Result<(), anyhow::Error> {
        let query = format!(
            "INSERT INTO `hot-or-not-feed-intelligence.yral_ds.video_unique` 
             (video_id, videohash, created_at) 
             VALUES ('{}', '{}', CURRENT_TIMESTAMP())",
            video_id, hash
        );

        let request = QueryRequest {
            query,
            ..Default::default()
        };

        log::info!(
            "Storing unique video in video_unique for video_id [{}]",
            video_id
        );

        bigquery_client
            .job()
            .query("hot-or-not-feed-intelligence", &request)
            .await?;

        Ok(())
    }

    async fn store_duplicate_video_exact(
        &self,
        video_id: &str,
        hash: &str,
        parent_video_id: &str,
        publisher_data: &VideoPublisherData,
        bigquery_client: &Client,
    ) -> Result<(), anyhow::Error> {
        // For Redis-based matching, we always consider it an exact duplicate (100% match)
        let query = format!(
            "INSERT INTO `hot-or-not-feed-intelligence.yral_ds.duplicate_videos` (
                publisher_canister_id, publisher_principal, post_id,
                original_video_id, parent_video_id, parent_canister_id,
                parent_principal, parent_post_id, exact_duplicate,
                duplication_score
            ) VALUES (
                '{}', '{}', {},
                '{}', '{}', NULL,
                NULL, NULL, true,
                100.0
            )",
            publisher_data.canister_id,
            publisher_data.publisher_principal,
            publisher_data.post_id,
            video_id,
            parent_video_id
        );

        let request = QueryRequest {
            query,
            ..Default::default()
        };

        log::info!(
            "Storing exact duplicate video in duplicate_videos: video_id [{}], parent_video_id [{}], score=100.0",
            video_id,
            parent_video_id
        );

        bigquery_client
            .job()
            .query("hot-or-not-feed-intelligence", &request)
            .await?;

        Ok(())
    }
}
