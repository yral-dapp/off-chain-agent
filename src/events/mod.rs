use axum::extract::State;
use axum::response::IntoResponse;
use axum::Json;
use candid::Principal;
use event::Event;
use http::{header, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::error::Error;
use std::sync::Arc;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use warehouse_events::warehouse_events_server::WarehouseEvents;

use crate::auth::check_auth_events;
use crate::events::push_notifications::dispatch_notif;
use crate::events::warehouse_events::{Empty, WarehouseEvent};
use crate::AppState;
use serde_json::Value;

pub mod warehouse_events {
    tonic::include_proto!("warehouse_events");
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("warehouse_events_descriptor");
}

pub mod event;
pub mod nsfw;
pub mod push_notifications;
pub mod queries;

pub struct WarehouseEventsService {
    pub shared_state: Arc<AppState>,
}

#[tonic::async_trait]
impl WarehouseEvents for WarehouseEventsService {
    async fn send_event(
        &self,
        request: tonic::Request<WarehouseEvent>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let shared_state = self.shared_state.clone();

        let request = request.into_inner();
        let event = event::Event::new(request);

        process_event_impl(event, shared_state).await.map_err(|e| {
            log::error!("Failed to process event grpc: {}", e);
            tonic::Status::internal("Failed to process event")
        })?;

        Ok(tonic::Response::new(Empty {}))
    }
}

pub struct VideoUploadSuccessful {
    pub shared_state: Arc<AppState>,
}

impl VideoUploadSuccessful {
    pub async fn send_event(
        &self,
        user_principal: Principal,
        user_canister_id: Principal,
        username: String,
        video_uid: String,
        hashtags_len: usize,
        is_nsfw: bool,
        enable_hot_or_not: bool,
        post_id: u64,
    ) -> Result<(), Box<dyn Error>> {
        // video_upload_successful - analytics
        let event_name = "video_upload_successful";

        let ware_house_events_service = WarehouseEventsService {
            shared_state: self.shared_state.clone(),
        };

        let params = &json!({
            "user_id": user_principal,
            "publisher_user_id": user_principal,
            "display_name": username,
            "canister_id": user_canister_id,
            "creator_category": "NA",
            "hashtag_count": hashtags_len,
            "is_NSFW": is_nsfw,
            "is_hotorNot": enable_hot_or_not,
            "is_filter_used": false,
            "video_id": video_uid,
            "post_id": post_id,
        });

        let warehouse_event = WarehouseEvent {
            event: event_name.into(),
            params: params.to_string(),
        };

        let request = tonic::Request::new(warehouse_event);

        ware_house_events_service.send_event(request).await?;

        Ok(())
    }
}

pub fn events_router(state: Arc<AppState>) -> OpenApiRouter {
    OpenApiRouter::new()
        .routes(routes!(post_event))
        .with_state(state)
}

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
pub struct EventRequest {
    event: String,
    params: String,
}

#[utoipa::path(
    post,
    path = "",
    request_body = EventRequest,
    tag = "events",
    responses(
        (status = 200, description = "Event sent successfully"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error"),
    )
)]
async fn post_event(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(payload): Json<EventRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    log::info!("Received event: {:?}", payload);

    let auth_token = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim_start_matches("Bearer ").to_string());

    match auth_token.clone() {
        Some(token) => log::info!("Auth token length: {:?}", token.len()),
        None => log::info!("No auth token"),
    };

    check_auth_events(auth_token).map_err(|e| (StatusCode::UNAUTHORIZED, e.to_string()))?;

    log::info!("valid auth");

    let warehouse_event = WarehouseEvent {
        event: payload.event,
        params: payload.params,
    };

    let event = Event::new(warehouse_event);

    log::info!("before process_event_impl");

    process_event_impl(event, state.clone())
        .await
        .map_err(|e| {
            log::error!("Failed to process event rest: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to process event".to_string(),
            )
        })?;

    log::info!("after process_event_impl success");

    Ok((StatusCode::OK, "Event processed".to_string()))
}

async fn process_event_impl(
    event: Event,
    shared_state: Arc<AppState>,
) -> Result<(), anyhow::Error> {
    let params: Value = serde_json::from_str(&event.event.params).map_err(|e| {
        log::error!("Failed to parse params: {}", e);
        anyhow::anyhow!("Failed to parse params: {}", e)
    })?;

    log::info!("params parsed success: {:?}", params);

    let event_type: &str = &event.event.event;

    #[cfg(not(feature = "local-bin"))]
    event.stream_to_bigquery(&shared_state.clone());

    event.upload_to_gcs(&shared_state.clone());

    event.update_watch_history(&shared_state.clone());

    event.update_success_history(&shared_state.clone());

    #[cfg(not(feature = "local-bin"))]
    event.stream_to_firestore(&shared_state.clone());

    #[cfg(not(feature = "local-bin"))]
    event.stream_to_bigquery_token_metadata(&shared_state.clone());

    let _ = dispatch_notif(event_type, params, &shared_state.clone()).await;

    Ok(())
}
