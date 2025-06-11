use axum::{body::Bytes, extract::State, http::HeaderMap, response::IntoResponse, Json};
use hmac::{Hmac, Mac};
use http::StatusCode;
use k256::sha2::Sha256;
use serde::{Deserialize, Serialize};
use std::{env, sync::Arc};

use crate::app_state::AppState;

#[derive(Debug, Deserialize)]
pub struct SentryWebhookPayload {
    data: Option<SentryData>,
}

#[derive(Debug, Deserialize)]
pub struct SentryData {
    event: Option<SentryEvent>,
}

#[derive(Debug, Deserialize)]
pub struct SentryEvent {
    web_url: Option<String>,
    title: Option<String>,
    user: Option<SentryUser>,
    level: Option<String>,
    platform: Option<String>,
    timestamp: Option<f64>,
    project: Option<u32>,
    logger: Option<String>,
    release: Option<String>,
    culprit: Option<String>,
    tags: Option<Vec<[String; 2]>>,
}

#[derive(Debug, Deserialize)]
pub struct SentryUser {
    id: Option<String>,
}

#[derive(Debug, Serialize)]
struct GoogleChatMessage {
    text: String,
}

async fn verify_sentry_signature(headers: &HeaderMap, body: &[u8]) -> Result<(), StatusCode> {
    // Get the signature from headers
    let expected_signature = headers
        .get("sentry-hook-signature")
        .and_then(|value| value.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    // Get the client secret from environment
    let client_secret =
        env::var("SENTRY_CLIENT_SECRET").map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Create HMAC-SHA256
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(client_secret.as_bytes())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    mac.update(body);
    let digest = mac.finalize();
    let computed_signature = hex::encode(digest.into_bytes());

    // Compare signatures using constant-time comparison
    if computed_signature != expected_signature {
        log::warn!("Sentry webhook signature verification failed");
        return Err(StatusCode::UNAUTHORIZED);
    }

    Ok(())
}

pub async fn sentry_webhook_handler(
    State(_state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Verify signature
    if let Err(status) = verify_sentry_signature(&headers, &body).await {
        return Err((status, "Signature verification failed".to_string()));
    }

    // Parse the JSON payload
    let payload: SentryWebhookPayload = serde_json::from_slice(&body)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid JSON: {}", e)))?;

    let web_url = payload
        .data
        .as_ref()
        .and_then(|data| data.event.as_ref())
        .and_then(|event| event.web_url.as_ref())
        .map(|url| url.as_str())
        .unwrap_or("N/A");

    let title = payload
        .data
        .as_ref()
        .and_then(|data| data.event.as_ref())
        .and_then(|event| event.title.as_ref())
        .map(|title| title.as_str())
        .unwrap_or("N/A");

    let user_id = payload
        .data
        .as_ref()
        .and_then(|data| data.event.as_ref())
        .and_then(|event| event.user.as_ref())
        .and_then(|user| user.id.as_ref())
        .map(|id| id.as_str())
        .unwrap_or("N/A");

    let level = payload
        .data
        .as_ref()
        .and_then(|data| data.event.as_ref())
        .and_then(|event| event.level.as_ref())
        .map(|l| l.as_str())
        .unwrap_or("unknown");

    let platform = payload
        .data
        .as_ref()
        .and_then(|data| data.event.as_ref())
        .and_then(|event| event.platform.as_ref())
        .map(|p| p.as_str())
        .unwrap_or("unknown");

    let project = payload
        .data
        .as_ref()
        .and_then(|data| data.event.as_ref())
        .and_then(|event| event.project)
        .map(|p| p.to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let release = payload
        .data
        .as_ref()
        .and_then(|data| data.event.as_ref())
        .and_then(|event| event.release.as_ref())
        .map(|r| r.as_str())
        .unwrap_or("unknown");

    // Extract environment from tags
    let environment = payload
        .data
        .as_ref()
        .and_then(|data| data.event.as_ref())
        .and_then(|event| event.tags.as_ref())
        .and_then(|tags| {
            tags.iter()
                .find(|tag| tag[0] == "environment")
                .map(|tag| tag[1].as_str())
        })
        .unwrap_or("unknown");

    log::info!(
        "Sentry event - Title: {}, Level: {}, Platform: {}, Environment: {}, User ID: {}",
        title,
        level,
        platform,
        environment,
        user_id
    );

    // Send to Google Chat if webhook URL is configured
    if let Ok(webhook_url) = env::var("SENTRY_GOOGLE_CHAT_WEBHOOK_URL") {
        let severity_emoji = match level {
            "error" => "🔴",
            "warning" => "🟡",
            "info" => "🔵",
            "debug" => "⚪",
            "fatal" => "💥",
            _ => "⚠️",
        };

        let message = GoogleChatMessage {
            text: format!(
                "{} *Sentry Alert*\n\n*Title:* {}\n*Level:* {}\n*Platform:* {}\n*Environment:* {}\n*Project:* {}\n*Release:* {}\n*User ID:* {}\n*URL:* {}",
                severity_emoji, title, level, platform, environment, project, release, user_id, web_url
            ),
        };

        let client = reqwest::Client::new();
        match client.post(&webhook_url).json(&message).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    log::info!("Successfully sent message to Google Chat");
                } else {
                    log::error!(
                        "Failed to send message to Google Chat: {}",
                        response.status()
                    );
                }
            }
            Err(e) => {
                log::error!("Error sending message to Google Chat: {}", e);
            }
        }
    } else {
        log::debug!("GOOGLE_CHAT_WEBHOOK_URL not configured, skipping Google Chat notification");
    }

    Ok(StatusCode::OK)
}
