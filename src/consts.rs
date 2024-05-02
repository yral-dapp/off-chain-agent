use once_cell::sync::Lazy;
use reqwest::Url;

pub static BIGQUERY_INGESTION_URL: Lazy<Url> = Lazy::new(|| {
    Url::parse("https://bigquery.googleapis.com/bigquery/v2/projects/hot-or-not-feed-intelligence/datasets/analytics_335143420/tables/events_analytics/insertAll").unwrap()
});

pub const PLATFORM_ORCHESTRATOR_ID: &str = "74zq4-iqaaa-aaaam-ab53a-cai";

pub static YRAL_METADATA_URL: Lazy<Url> = Lazy::new(|| Url::parse("localhost:8080").unwrap()); // TODO: https://yral-metadata.fly.dev

pub const RECYCLE_THRESHOLD_SECS: u64 = 15; // TODO: 15 * 24 * 60 * 60; // 15 days
