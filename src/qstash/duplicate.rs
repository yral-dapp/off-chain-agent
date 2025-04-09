use crate::{app_state, consts::OFF_CHAIN_AGENT_URL, duplicate_video::videohash::VideoHash};
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

    pub async fn publish_video_hash_indexing(
        &self,
        video_id: &str,
        video_url: &str,
        publisher_data: VideoPublisherData,
        publish_video_callback: impl Fn(&str, &str, u64, String, &str) -> Result<(), anyhow::Error>,
    ) -> Result<(), anyhow::Error> {
        log::info!("Calculating videohash for video URL: {}", video_url);
        let video_hash = VideoHash::from_url(video_url)
            .map_err(|e| anyhow::anyhow!("Failed to generate videohash: {}", e))?;

        // Store the original hash regardless of duplication status
        self.store_videohash_original(video_id, &video_hash.hash)
            .await?;

        // Call the video hash indexer API to check for duplicates
        let client = reqwest::Client::new();
        let response = client
            .post("https://videohash-indexer.fly.dev/search")
            .json(&serde_json::json!({
                "video_id": video_id,
                "hash": video_hash.hash,
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "VideoHash Indexer API failed: {} - {}",
                status,
                error_text
            ));
        }

        let indexer_response: VideoHashIndexerResponse = response.json().await?;
        log::info!("VideoHash Indexer response: {:?}", indexer_response);

        if indexer_response.match_found {
            // A similar video was found - record as duplicate
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

                let exact_duplicate = match_details.similarity_percentage > 99.0;
                let duplicate_event = DuplicateVideoEvent {
                    original_video_id: video_id.to_string(),
                    parent_video_id: match_details.video_id.clone(),
                    similarity_percentage: match_details.similarity_percentage,
                    exact_duplicate,
                    publisher_canister_id: publisher_data.canister_id.clone(),
                    publisher_principal: publisher_data.publisher_principal.clone(),
                    post_id: publisher_data.post_id,
                    timestamp: chrono::Utc::now().to_rfc3339(),
                };

                // self.publish_duplicate_video_event(duplicate_event).await?;
            }
        } else {
            // For unique videos
            self.store_unique_video(video_id, &video_hash.hash).await?;
            log::info!("Unique video recorded: {}", video_id);

            let timestamp = chrono::Utc::now().to_rfc3339();
            publish_video_callback(
                video_id,
                &publisher_data.canister_id,
                publisher_data.post_id,
                timestamp,
                &publisher_data.publisher_principal,
            )?;
        }

        // Publish "de-duplication_check_done" event regardless of whether it's duplicate or not
        // self.publish_deduplication_completed(
        //     video_id,
        //     &publisher_data.canister_id,
        //     publisher_data.post_id,
        //     &publisher_data.publisher_principal,
        // )
        // .await?;

        Ok(())
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
            "Publishing duplicate video event: original={}, parent={}, score={}",
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

    pub async fn publish_deduplication_completed(
        &self,
        video_id: &str,
        canister_id: &str,
        post_id: u64,
        publisher_user_id: &str,
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL
            .join("qstash/deduplication_completed")
            .unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!({
            "video_id": video_id,
            "canister_id": canister_id,
            "post_id": post_id,
            "publisher_user_id": publisher_user_id,
            "timestamp": chrono::Utc::now().to_rfc3339()
        });

        log::info!(
            "Publishing deduplication completed event for video: {}",
            video_id
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
            .map_err(|e| anyhow::anyhow!("Failed to generate videohash: {}", e))?;

        // Store the original hash regardless of duplication status
        self.store_videohash_original(video_id, &video_hash.hash)
            .await?;

        // Call the video hash indexer API to check for duplicates
        let client = reqwest::Client::new();
        let response = client
            .post("https://videohash-indexer.fly.dev/search")
            .json(&serde_json::json!({
                "video_id": video_id,
                "hash": video_hash.hash,
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "VideoHash Indexer API failed: {} - {}",
                status,
                error_text
            ));
        }

        let indexer_response: VideoHashIndexerResponse = response.json().await?;
        log::info!("VideoHash Indexer response: {:?}", indexer_response);

        if indexer_response.match_found {
            // A similar video was found - record as duplicate
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

                let exact_duplicate = match_details.similarity_percentage > 99.0;
                let duplicate_event = DuplicateVideoEvent {
                    original_video_id: video_id.to_string(),
                    parent_video_id: match_details.video_id.clone(),
                    similarity_percentage: match_details.similarity_percentage,
                    exact_duplicate,
                    publisher_canister_id: publisher_data.canister_id.clone(),
                    publisher_principal: publisher_data.publisher_principal.clone(),
                    post_id: publisher_data.post_id,
                    timestamp: chrono::Utc::now().to_rfc3339(),
                };

                // self.publish_duplicate_video_event(duplicate_event).await?;
            }
        } else {
            // For unique videos
            self.store_unique_video(video_id, &video_hash.hash).await?;
            log::info!("Unique video recorded: {}", video_id);

            // Only proceed with normal video processing for unique videos
            let timestamp = chrono::Utc::now().to_rfc3339();
            publish_video_callback(
                video_id,
                &publisher_data.canister_id,
                publisher_data.post_id,
                timestamp,
                &publisher_data.publisher_principal,
            )
            .await?;
        }

        // Publish "de-duplication_check_done" event regardless of whether it's duplicate or not
        // self.publish_deduplication_completed(
        //     video_id,
        //     &publisher_data.canister_id,
        //     publisher_data.post_id,
        //     &publisher_data.publisher_principal,
        // )
        // .await?;

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
             VALUES ('{}', '{}', CURRENT_TIMESTAMP())",
            video_id, hash
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
             VALUES ('{}', '{}', CURRENT_TIMESTAMP())",
            video_id, hash
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

        // Default parent data (in case we can't find the actual parent data)
        let mut parent_canister_id = "unknown".to_string();
        let mut parent_principal = "unknown".to_string();
        let mut parent_post_id = 0;

        // First, let's try to get the parent video's metadata from BigQuery
        let parent_query = format!(
            "SELECT publisher_canister_id, publisher_principal, post_id 
             FROM `hot-or-not-feed-intelligence.yral_ds.videohash_original` 
             WHERE video_id = '{}' 
             LIMIT 1",
            match_details.video_id
        );

        let parent_request = QueryRequest {
            query: parent_query,
            ..Default::default()
        };

        // if let Ok(response) = bigquery_client
        //     .job()
        //     .query("hot-or-not-feed-intelligence", &parent_request)
        //     .await
        // {
        //     // First check if rows is Some, then access the first element
        //     if let Some(rows) = response.rows {
        //         if let Some(row) = rows.first() {
        //             // Access by position based on the query's field order 
        //             if row.f.len() > 0 {
        //                 // Use to_string_lossy to convert the Value to a string
        //                 if let Some(v) = row.f[0].v.as_str() {
        //                     parent_canister_id = v.to_string();
        //                 } else {
        //                     parent_canister_id = format!("{:?}", row.f[0].v);
        //                 }
        //             }
                    
        //             if row.f.len() > 1 {
        //                 if let Some(v) = row.f[1].v.as_str() {
        //                     parent_principal = v.to_string();
        //                 } else {
        //                     parent_principal = format!("{:?}", row.f[1].v);
        //                 }
        //             }
                    
        //             if row.f.len() > 2 {
        //                 // For the post_id (a number), first try to extract as a string, then parse
        //                 if let Some(v) = row.f[2].v.as_str() {
        //                     if let Ok(id) = v.parse::<u64>() {
        //                         parent_post_id = id;
        //                     }
        //                 } else if let Some(n) = row.f[2].v.as_i64() {
        //                     parent_post_id = n as u64;
        //                 }
        //             }
        //         }
        //     }
        // }

        let exact_duplicate = match_details.similarity_percentage > 99.0;

        // Now use the parent data in our query
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
                {}, CURRENT_TIMESTAMP()
            )",
            publisher_data.canister_id,
            publisher_data.publisher_principal,
            publisher_data.post_id,
            video_id,
            match_details.video_id,
            parent_canister_id,  
            parent_principal,    
            parent_post_id,      
            exact_duplicate,
            match_details.similarity_percentage
        );

        // Rest of the function remains the same
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
