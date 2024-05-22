use once_cell::sync::Lazy;
use reqwest::Url;

// TODO: remove test prefix
pub static BIGQUERY_INGESTION_URL: Lazy<Url> = Lazy::new(|| {
    Url::parse("https://bigquery.googleapis.com/bigquery/v2/projects/hot-or-not-feed-intelligence/datasets/analytics_335143420/tables/test_events_analytics/insertAll").unwrap()
});

pub const PLATFORM_ORCHESTRATOR_ID: &str = "74zq4-iqaaa-aaaam-ab53a-cai";

pub static YRAL_METADATA_URL: Lazy<Url> =
    Lazy::new(|| Url::parse("https://yral-metadata.fly.dev").unwrap());

pub const RECYCLE_THRESHOLD_SECS: u64 = 60 * 10; // TODO: 15 * 24 * 60 * 60; // 15 days

pub const GOOGLE_CHAT_REPORT_SPACE_URL: &str =
    "https://chat.googleapis.com/v1/spaces/AAAA1yDLYO4/messages";

pub const ML_SERVER_URL: &str = "http://168.220.93.94:50051";

pub const UPSTASH_VECTOR_REST_URL: &str = "https://communal-flea-37939-us1-vector.upstash.io";
pub const UPSTASH_VECTOR_REST_TOKEN: &str = "ABcFMGNvbW11bmFsLWZsZWEtMzc5MzktdXMxYWRtaW5OV0ZrWkRNNVpUTXROamhrTmkwMFl6Vm1MVGxtT1dJdFptVm1OVEZtWmpJeU4yTmw=";
