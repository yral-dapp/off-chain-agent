use std::sync::Arc;

use chrono::Timelike;
use http::{
    header::{AUTHORIZATION, CONTENT_TYPE},
    HeaderMap, HeaderValue,
};
use reqwest::{Client, Url};
use yral_canisters_client::individual_user_template::DeployedCdaoCanisters;

use crate::{
    app_state,
    canister::upgrade_user_token_sns_canister::{SnsCanisters, VerifyUpgradeProposalRequest},
    consts::OFF_CHAIN_AGENT_URL,
    duplicate_video::videohash::VideoHash,
    events::event::UploadVideoInfo,
    posts::ReportPostRequest,
};
use cloud_storage::Client as GcsClient;
use google_cloud_bigquery::http::job::query::QueryRequest;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct QStashClient {
    client: Client,
    base_url: Arc<Url>,
}

impl QStashClient {
    pub fn new(auth_token: &str) -> Self {
        let mut bearer: HeaderValue = format!("Bearer {}", auth_token)
            .parse()
            .expect("Invalid QStash auth token");
        bearer.set_sensitive(true);
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, bearer);

        let client = Client::builder()
            .default_headers(headers)
            .build()
            .expect("Failed to create QStash client");
        let base_url = Url::parse("https://qstash.upstash.io/v2/").unwrap();

        Self {
            client,
            base_url: Arc::new(base_url),
        }
    }

    pub async fn publish_video(
        &self,
        video_id: &str,
        canister_id: &str,
        post_id: u64,
        timestamp_str: String,
        publisher_user_id: &str,
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL.join("qstash/upload_video_gcs").unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!({
            "video_id": video_id,
            "canister_id": canister_id,
            "post_id": post_id,
            "timestamp": timestamp_str,
            "publisher_user_id": publisher_user_id
        });

        self.client
            .post(url)
            .json(&req)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .header("upstash-delay", "600s")
            .send()
            .await?;

        Ok(())
    }

    pub async fn publish_video_frames(
        &self,
        video_id: &str,
        video_info: &UploadVideoInfo,
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL
            .join("qstash/enqueue_video_frames")
            .unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!({
            "video_id": video_id,
            "video_info": video_info,
        });

        self.client
            .post(url)
            .json(&req)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .send()
            .await?;

        Ok(())
    }

    pub async fn publish_video_nsfw_detection(
        &self,
        video_id: &str,
        video_info: &UploadVideoInfo,
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL
            .join("qstash/enqueue_video_nsfw_detection")
            .unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!({
            "video_id": video_id,
            "video_info": video_info,
        });

        self.client
            .post(url)
            .json(&req)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .send()
            .await?;

        Ok(())
    }

    pub async fn publish_video_nsfw_detection_v2(
        &self,
        video_id: &str,
        video_info: UploadVideoInfo,
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL
            .join("qstash/enqueue_video_nsfw_detection_v2")
            .unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!({
            "video_id": video_id,
            "video_info": video_info,
        });

        // Calculate delay until next :20 minute of any hour
        let now = chrono::Utc::now();
        let current_minute = now.minute();
        let minutes_until_20 = if current_minute >= 20 {
            60 - current_minute + 20 // Wait for next hour's :20
        } else {
            20 - current_minute // Wait for this hour's :20
        };

        // Convert to seconds and add random jitter between 0-600 seconds
        let jitter = (now.nanosecond() % 601) as u32;
        let delay_seconds = minutes_until_20 * 60 + jitter + 3600; // add 1 hour to the delay to compensate for bigquery buffer time

        self.client
            .post(url)
            .json(&req)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .header("upstash-delay", format!("{}s", delay_seconds))
            .send()
            .await?;

        Ok(())
    }

    pub async fn upgrade_sns_creator_dao_canister(
        &self,
        sns_canister: SnsCanisters,
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL
            .join("qstash/upgrade_sns_creator_dao_canister")
            .unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!(sns_canister);

        self.client
            .post(url)
            .json(&req)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .header("upstash-retries", "0")
            .send()
            .await?;

        Ok(())
    }

    pub async fn verify_sns_canister_upgrade_proposal(
        &self,
        verify_request: VerifyUpgradeProposalRequest,
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL
            .join("qstash/verify_sns_canister_upgrade_proposal")
            .unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!(verify_request);

        self.client
            .post(url)
            .json(&req)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .header("upstash-delay", "5s")
            .header("upstash-retries", "3")
            .send()
            .await?;

        Ok(())
    }

    pub async fn upgrade_all_sns_canisters_for_a_user_canister(
        &self,
        user_canister_id: String,
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL
            .join(&format!(
                "qstash/upgrade_all_sns_canisters_for_a_user_canister/{}",
                user_canister_id
            ))
            .unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;

        self.client
            .post(url)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .header("upstash-retries", "0")
            .send()
            .await?;

        Ok(())
    }

    pub async fn upgrade_user_token_sns_canister_for_entire_network(
        &self,
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL
            .join(&format!(
                "qstash/upgrade_user_token_sns_canister_for_entire_network",
            ))
            .unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;

        self.client
            .post(url)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .header("upstash-retries", "0")
            .send()
            .await?;

        Ok(())
    }

    pub async fn publish_report_post(
        &self,
        report_request: ReportPostRequest,
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL.join("qstash/report_post").unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!(report_request);

        self.client
            .post(url)
            .json(&req)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .send()
            .await?;

        Ok(())
    }

    pub async fn publish_video_hash_indexing(
        &self,
        video_id: &str,
        video_url: &str,
        publisher_data: VideoPublisherData,
    ) -> Result<(), anyhow::Error> {
        log::info!("Calculating videohash for video URL: {}", video_url);
        let video_hash = VideoHash::from_url(video_url)
            .map_err(|e| anyhow::anyhow!("Failed to generate videohash: {}", e))?;

        self.store_videohash_original(video_id, &video_hash.hash)
            .await?;

        log::info!("Calling VideoHash Indexer service for video: {}", video_id);

        let client = reqwest::Client::new();
        let response = client
            .post("https://videohash-indexer.fly.dev/api/index")
            .json(&serde_json::json!({
                "videoUrl": video_url,
                "hash": video_hash.hash,
                "videoId": video_id
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status(); // Store status before consuming response
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "VideoHash Indexer API failed: {} - {}",
                status,
                error_text
            ));
        }

        let indexer_response: VideoHashIndexerResponse = response.json().await?;

        if indexer_response.match_found {
            if let Some(match_details) = indexer_response.match_details {
                self.store_duplicate_video(
                    video_id,
                    &video_hash.hash,
                    &match_details,
                    &publisher_data,
                )
                .await?;

                log::info!(
                    "Duplicate video detected: {} is similar to {} (score: {})",
                    video_id,
                    match_details.video_id,
                    match_details.similarity_percentage
                );
            }
        } else {
            self.store_unique_video(video_id, &video_hash.hash).await?;
            log::info!("Unique video recorded: {}", video_id);
        }

        Ok(())
    }

    async fn store_videohash_original(
        &self,
        video_id: &str,
        hash: &str,
    ) -> Result<(), anyhow::Error> {
        let bigquery_client = app_state::init_bigquery_client().await;

        let query = format!(
            "INSERT INTO `hot-or-not-feed-intelligence.yral_ds.videohash_original` 
             (video_id, videohash, created_at) 
             VALUES ('{}', '{}', TIMESTAMP '{}')",
            video_id,
            hash,
            chrono::Utc::now().to_rfc3339()
        );

        let request = QueryRequest {
            query,
            ..Default::default()
        };

        log::info!(
            "Storing hash in videohash_original for video_id: {}",
            video_id
        );

        bigquery_client
            .job()
            .query("hot-or-not-feed-intelligence", &request)
            .await?;

        Ok(())
    }

    async fn store_unique_video(&self, video_id: &str, hash: &str) -> Result<(), anyhow::Error> {
        let bigquery_client = app_state::init_bigquery_client().await;

        let query = format!(
            "INSERT INTO `hot-or-not-feed-intelligence.yral_ds.video_unique` 
             (video_id, videohash, created_at) 
             VALUES ('{}', '{}', TIMESTAMP '{}')",
            video_id,
            hash,
            chrono::Utc::now().to_rfc3339()
        );

        let request = QueryRequest {
            query,
            ..Default::default()
        };

        log::info!(
            "Storing unique video in video_unique for video_id: {}",
            video_id
        );

        bigquery_client
            .job()
            .query("hot-or-not-feed-intelligence", &request)
            .await?;

        Ok(())
    }

    async fn store_duplicate_video(
        &self,
        video_id: &str,
        hash: &str,
        match_details: &MatchDetails,
        publisher_data: &VideoPublisherData,
    ) -> Result<(), anyhow::Error> {
        let bigquery_client = app_state::init_bigquery_client().await;

        let exact_duplicate = match_details.similarity_percentage > 99.0;

        let query = format!(
            "INSERT INTO `hot-or-not-feed-intelligence.yral_ds.duplicate_video` (
                publisher_canister_id, publisher_principal, post_id,
                original_video_id, parent_video_id, parent_canister_id,
                parent_principal, parent_post_id, exact_duplicate,
                duplication_score, created_at
            ) VALUES (
                '{}', '{}', {},
                '{}', '{}', '{}',
                '{}', {}, {},
                {}, TIMESTAMP '{}'
            )",
            publisher_data.canister_id,
            publisher_data.publisher_principal,
            publisher_data.post_id,
            video_id,
            match_details.video_id,
            publisher_data.canister_id,
            publisher_data.publisher_principal,
            publisher_data.post_id,
            exact_duplicate,
            match_details.similarity_percentage,
            chrono::Utc::now().to_rfc3339()
        );

        let request = QueryRequest {
            query,
            ..Default::default()
        };

        log::info!(
            "Storing duplicate video in duplicate_video: original={}, parent={}, score={}",
            video_id,
            match_details.video_id,
            match_details.similarity_percentage
        );

        bigquery_client
            .job()
            .query("hot-or-not-feed-intelligence", &request)
            .await?;

        Ok(())
    }
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct VideoPublisherData {
    pub canister_id: String,
    pub publisher_principal: String,
    pub post_id: u64,
}
