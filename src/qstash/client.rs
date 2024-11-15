use std::sync::Arc;

use http::{
    header::{AUTHORIZATION, CONTENT_TYPE},
    HeaderMap, HeaderValue,
};
use reqwest::{Client, Url};
use yral_canisters_client::individual_user_template::Ok;

use crate::consts::OFF_CHAIN_AGENT_URL;

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
    ) -> Result<(), Box<dyn std::error::Error>> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL.join("qstash/upload_video_gcs").unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!({
            "canister_id": canister_id,
            "post_id": post_id,
            "timestamp": timestamp_str
        });

        // upstash-delay is 30 secs
        self.client
            .post(url)
            .json(&req)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .header("upstash-delay", format!("30s"))
            .send()
            .await?;

        Ok(())
    }
}
