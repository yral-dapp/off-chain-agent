use once_cell::sync::Lazy;
use reqwest::Url;

#[macro_export]
macro_rules! env_or_default {
    ($env_var:expr, $default:expr) => {{
        std::env::var($env_var)
            .ok()
            .map(|val| val)
            .unwrap_or_else(|| $default.into())
    }};
}

pub const PLATFORM_ORCHESTRATOR_ID: &str = "74zq4-iqaaa-aaaam-ab53a-cai";

pub const DEFAULT_YRAL_METADATA_URL: &str = yral_metadata_client::consts::DEFAULT_API_URL;

pub const RECYCLE_THRESHOLD_SECS: u64 = 60 * 10; // TODO: 15 * 24 * 60 * 60; // 15 days

pub const DEFAULT_SHARED_SECRET: &str = "";

pub static BIGQUERY_INGESTION_URL: Lazy<Url> = Lazy::new(|| {
    Url::parse("https://bigquery.googleapis.com/bigquery/v2/projects/hot-or-not-feed-intelligence/datasets/analytics_335143420/tables/events_analytics/insertAll").unwrap()
});

pub static YRAL_METADATA_URL: Lazy<Url> = Lazy::new(|| {
    env_or_default!("YRAL_METADATA_URL", DEFAULT_YRAL_METADATA_URL)
        .parse()
        .unwrap()
});


pub static SHARED_SECRET: Lazy<String> = Lazy::new(|| {
    env_or_default!("SHARED_SECRET", DEFAULT_SHARED_SECRET)
});