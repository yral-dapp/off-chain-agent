use std::sync::Arc;

use chrono::Timelike;
use http::{
    header::{AUTHORIZATION, CONTENT_TYPE},
    HeaderMap, HeaderValue,
};
use reqwest::{Client, Url};
use tracing::instrument;

use crate::{
    canister::upgrade_user_token_sns_canister::{SnsCanisters, VerifyUpgradeProposalRequest},
    consts::OFF_CHAIN_AGENT_URL,
    events::event::UploadVideoInfo,
    posts::report_post::{ReportPostRequest, ReportPostRequestV2},
    qstash::duplicate::{DuplicateVideoEvent, VideoHashDuplication, VideoPublisherData},
};

#[derive(Clone, Debug)]
pub struct QStashClient {
    pub client: Client,
    pub base_url: Arc<Url>,
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

    #[instrument(skip(self))]
    pub async fn publish_video_hash_indexing(
        &self,
        video_id: &str,
        video_url: &str,
        publisher_data: VideoPublisherData,
    ) -> Result<(), anyhow::Error> {
        let duplication_handler = VideoHashDuplication::new(&self.client, &self.base_url);

        duplication_handler
            .process_video_deduplication(
                video_id,
                video_url,
                publisher_data,
                |vid_id, canister_id, post_id, timestamp, publisher_user_id| {
                    // Clone the string references to own the data
                    let vid_id = vid_id.to_string();
                    let canister_id = canister_id.to_string();
                    let publisher_user_id = publisher_user_id.to_string();

                    // Create a boxed future from an async block
                    Box::pin(async move {
                        self.publish_video(
                            &vid_id,
                            &canister_id,
                            post_id,
                            timestamp,
                            &publisher_user_id,
                        )
                        .await
                    })
                },
            )
            .await
    }

    #[instrument(skip(self))]
    pub async fn duplicate_to_storj(
        &self,
        data: storj_interface::duplicate::Args,
    ) -> anyhow::Result<()> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL.join("qstash/storj_ingest").unwrap();
        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;

        self.client
            .post(url)
            .json(&data)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .header("Upstash-Flow-Control-Key", "STORJ_INGESTION")
            .header("Upstash-Flow-Control-Value", "Rate=20,Parallelism=10")
            .send()
            .await?;

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn publish_duplicate_video_event(
        &self,
        duplicate_event: DuplicateVideoEvent,
    ) -> Result<(), anyhow::Error> {
        let duplication_handler = VideoHashDuplication::new(&self.client, &self.base_url);

        duplication_handler
            .publish_duplicate_video_event(duplicate_event)
            .await
    }

    #[instrument(skip(self))]
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
            .send()
            .await?;

        Ok(())
    }

    #[instrument(skip(self))]
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

    #[instrument(skip(self))]
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

    #[instrument(skip(self))]
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
            60 - current_minute + 20
        } else {
            20 - current_minute
        };

        let jitter = (now.nanosecond() % 601) as u32;
        let delay_seconds = minutes_until_20 * 60 + jitter + 3600;

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

    #[instrument(skip(self))]
    pub async fn publish_report_post(
        &self,
        report_request: ReportPostRequestV2,
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
}
