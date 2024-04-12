use once_cell::sync::Lazy;
use reqwest::Url;

pub static BIGQUERY_INGESTION_URL: Lazy<Url> = Lazy::new(|| {
    Url::parse("https://bigquery.googleapis.com/bigquery/v2/projects/hot-or-not-feed-intelligence/datasets/analytics_335143420/tables/events_analytics/insertAll").unwrap()
});
