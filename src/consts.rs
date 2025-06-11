use once_cell::sync::Lazy;
use reqwest::Url;

pub const STDB_URL: &str = "https://maincloud.spacetimedb.com";

// TODO: switch to using production identity
pub const DEDUP_INDEX_MODULE_IDENTITY: &str =
    "c2007dac899a3a4fc29eefc18fdd99762bd06eb6c69d114024af28d4c16e6b08";

// TODO: switch to using production token stored in env
pub static STDB_ACCESS_TOKEN: Lazy<String> = Lazy::new(|| {
    "eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiIsImtpZCI6ImRlZmF1bHQifQ.eyJhdWQiOiJlMWE2YTdmYi04YTFkLTQyZGMtODdiNC0xM2ZmOTRlY2JlMzQiLCJleHAiOjE3NTAxMzk3MzAsImlhdCI6MTc0OTUzNDkzMCwiaXNzIjoiaHR0cHM6Ly9hdXRoLnlyYWwuY29tIiwic3ViIjoibzNoaHAtdXI2NHQtcmY0MmotcGdieDMtNWFodTcta2JlbWEtemhuaWUtdWVtbnotdHpzd2gtYnBnaGEtYnFlIiwiZXh0X2lzX2Fub255bW91cyI6dHJ1ZX0.HlPygs1EPn2iioCWhQKBhGe_sNNekOFPfAphaM1ALgXv171pBsZujpXhgJaND70l9V95G1Cvh3_nxe7HGLuFvA".into()
});

/// with nsfw detection v2, nsfw probablity greater or equal to this is considered nsfw
pub const NSFW_THRESHOLD: f32 = 0.4;

pub static BIGQUERY_INGESTION_URL: Lazy<Url> = Lazy::new(|| {
    Url::parse("https://bigquery.googleapis.com/bigquery/v2/projects/hot-or-not-feed-intelligence/datasets/analytics_335143420/tables/test_events_analytics/insertAll").unwrap()
});

pub const PLATFORM_ORCHESTRATOR_ID: &str = "74zq4-iqaaa-aaaam-ab53a-cai";

pub static YRAL_METADATA_URL: Lazy<Url> =
    Lazy::new(|| Url::parse("https://yral-metadata.fly.dev").unwrap());

pub const RECYCLE_THRESHOLD_SECS: u64 = 15 * 24 * 60 * 60; // 15 days

pub const GOOGLE_CHAT_REPORT_SPACE_URL: &str =
    "https://chat.googleapis.com/v1/spaces/AAAA1yDLYO4/messages";

pub const CLOUDFLARE_ACCOUNT_ID: &str = "a209c523d2d9646cc56227dbe6ce3ede";

pub const ICP_LEDGER_CANISTER_ID: &str = "ryjl3-tyaaa-aaaaa-aaaba-cai";

pub static OFF_CHAIN_AGENT_URL: Lazy<Url> = Lazy::new(|| {
    let url = std::env::var("OFF_CHAIN_AGENT_URL")
        .unwrap_or_else(|_| "https://icp-off-chain-agent.fly.dev/".into());
    Url::parse(&url).unwrap()
});

pub const NSFW_SERVER_URL: &str = "https://prod-yral-nsfw-classification.fly.dev:443";

pub const CLOUDFLARE_ML_FEED_CACHE_WORKER_URL: &str =
    "https://yral-ml-feed-cache.go-bazzinga.workers.dev";

pub const ML_FEED_SERVER_GRPC_URL: &str = "https://yral-ml-feed-server.fly.dev:443";

pub static STORJ_INTERFACE_URL: Lazy<Url> = Lazy::new(|| {
    Url::parse("https://storjinterw96bdfdvg3-194a96f1998dcf7a.tec-s1.onthetaedgecloud.com/")
        .unwrap()
});

pub static STORJ_INTERFACE_TOKEN: Lazy<String> =
    Lazy::new(|| std::env::var("STORJ_INTERFACE_TOKEN").expect("STORJ_INTERFACE_TOKEN to be set"));

pub static STORJ_BACKUP_CANISTER_ACCESS_GRANT: Lazy<String> = Lazy::new(|| {
    std::env::var("STORJ_BACKUP_CANISTER_ACCESS_GRANT")
        .expect("STORJ_BACKUP_CANISTER_ACCESS_GRANT to be set")
});

pub const CANISTER_BACKUPS_BUCKET: &str = "canister-backups";
