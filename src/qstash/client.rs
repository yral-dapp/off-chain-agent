use std::sync::Arc;

use chrono::Timelike;
use http::{
    header::{AUTHORIZATION, CONTENT_TYPE},
    HeaderMap, HeaderValue,
};
use reqwest::{Client, Url};
use yral_canisters_client::individual_user_template::DeployedCdaoCanisters;

use crate::{
    canister::upgrade_user_token_sns_canister::{SnsCanisters, VerifyUpgradeProposalRequest},
    consts::OFF_CHAIN_AGENT_URL,
};

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
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL.join("qstash/upload_video_gcs").unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!({
            "video_id": video_id,
            "canister_id": canister_id,
            "post_id": post_id,
            "timestamp": timestamp_str,
        });

        self.client
            .post(url)
            .json(&req)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .header("upstash-delay", format!("600s"))
            .send()
            .await?;

        Ok(())
    }

    pub async fn publish_video_frames(&self, video_id: &str) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL
            .join("qstash/enqueue_video_frames")
            .unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!({
            "video_id": video_id,
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

    pub async fn publish_video_nsfw_detection(&self, video_id: &str) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL
            .join("qstash/enqueue_video_nsfw_detection")
            .unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!({
            "video_id": video_id,
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
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL
            .join("qstash/enqueue_video_nsfw_detection_v2")
            .unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!({
            "video_id": video_id,
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

    pub async fn publish_video_nsfw_detection_v2_backfill(
        &self,
        video_id: &str,
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL
            .join("qstash/enqueue_video_nsfw_detection_v3")
            .unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!({
            "video_id": video_id,
        });

        // add random jitter between 0-120 seconds
        let now = chrono::Utc::now();
        let jitter = (now.nanosecond() % 121) as u32;

        self.client
            .post(url)
            .json(&req)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .header("upstash-delay", format!("{}s", jitter))
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
}
