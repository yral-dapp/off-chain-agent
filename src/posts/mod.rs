use std::sync::Arc;

use axum::{extract::State, http::StatusCode, middleware, response::IntoResponse, Json};
use candid::Principal;
use serde::{Deserialize, Serialize};
use tonic::transport::{Channel, ClientTlsConfig};
use types::PostRequest;
use utils::{get_agent_from_delegated_identity_wire, insert_video_delete_row_to_bigquery};
use utoipa::ToSchema;
use utoipa_axum::{
    router::{OpenApiRouter, UtoipaMethodRouterExt},
    routes,
};
use verify::{verify_post_request, VerifiedPostRequest};
use yral_canisters_client::individual_user_template::{IndividualUserTemplate, Result1};

use crate::{
    app_state::AppState,
    consts::ML_FEED_SERVER_GRPC_URL,
    utils::grpc_clients::ml_feed::{ml_feed_client::MlFeedClient, VideoReportRequest},
};

mod types;
mod utils;
mod verify;

/// Macro to create a route with verification middleware
macro_rules! verified_route {
    ($router:expr, $handler:path, $request_type:ty, $state:expr) => {
        $router.routes(routes!($handler).layer(middleware::from_fn_with_state(
            $state.clone(),
            verify_post_request::<$request_type>,
        )))
    };
}

pub fn posts_router(state: Arc<AppState>) -> OpenApiRouter {
    let mut router = OpenApiRouter::new();

    router = verified_route!(router, handle_delete_post, DeletePostRequest, state);
    router = verified_route!(router, handle_report_post, ReportPostRequest, state);

    router.with_state(state)
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct DeletePostRequest {
    #[schema(value_type = String)]
    canister_id: Principal,
    post_id: u64,
    video_id: String,
}

#[utoipa::path(
    delete,
    path = "",
    request_body = PostRequest<DeletePostRequest>,
    tag = "posts",
    responses(
        (status = 200, description = "Delete post success"),
        (status = 400, description = "Delete post failed"),
        (status = 500, description = "Internal server error"),
        (status = 403, description = "Forbidden"),
    )
)]
async fn handle_delete_post(
    State(state): State<Arc<AppState>>,
    Json(verified_request): Json<VerifiedPostRequest<DeletePostRequest>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Verify that the canister ID matches the user's canister
    if verified_request.request.request_body.canister_id != verified_request.user_canister {
        return Err((StatusCode::FORBIDDEN, "Forbidden".to_string()));
    }

    let request_body = verified_request.request.request_body;

    let canister_id = request_body.canister_id.to_string();
    let post_id = request_body.post_id;
    let video_id = request_body.video_id;

    let agent =
        get_agent_from_delegated_identity_wire(&verified_request.request.delegated_identity_wire)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let individual_user_template = IndividualUserTemplate(verified_request.user_canister, &agent);

    // Call the canister to delete the post
    let delete_res = individual_user_template.delete_post(post_id).await;
    match delete_res {
        Ok(Result1::Ok) => (),
        Ok(Result1::Err(_)) => {
            return Err((
                StatusCode::BAD_REQUEST,
                "Delete post failed - either the post doesn't exist or already deleted".to_string(),
            ))
        }
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Internal server error: {}", e),
            ))
        }
    }

    insert_video_delete_row_to_bigquery(state, canister_id, post_id, video_id)
        .await
        .map_err(|e| {
            log::error!("Failed to insert video delete row to bigquery: {}", e);

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to insert video to bigquery: {}", e),
            )
        })?;

    Ok((StatusCode::OK, "Post deleted".to_string()))
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct ReportPostRequest {
    #[schema(value_type = String)]
    pub canister_id: Principal,
    pub post_id: u64,
    pub video_id: String,
    #[schema(value_type = String)]
    pub user_canister_id: Principal,
    #[schema(value_type = String)]
    pub user_principal: Principal,
    pub reason: String,
}

#[utoipa::path(
    post,
    path = "/report",
    request_body = PostRequest<ReportPostRequest>,
    tag = "posts",
    responses(
        (status = 200, description = "Report post success"),
        (status = 500, description = "Internal server error"),
    )
)]
async fn handle_report_post(
    State(state): State<Arc<AppState>>,
    Json(verified_request): Json<VerifiedPostRequest<ReportPostRequest>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let request_body = verified_request.request.request_body;

    let qstash_client = state.qstash_client.clone();
    qstash_client
        .publish_report_post(request_body)
        .await
        .map_err(|e| {
            log::error!("Failed to publish report post: {}", e);

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to publish report post: {}", e),
            )
        })?;

    Ok((StatusCode::OK, "Post reported".to_string()))
}

pub async fn qstash_report_post(
    State(_state): State<Arc<AppState>>,
    Json(payload): Json<ReportPostRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tls_config = ClientTlsConfig::new().with_webpki_roots();

    let channel = Channel::from_static(ML_FEED_SERVER_GRPC_URL)
        .tls_config(tls_config)
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to create channel: {}", e),
            )
        })?
        .connect()
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to connect to ML feed server: {}", e),
            )
        })?;

    let mut client = MlFeedClient::new(channel);

    let request = VideoReportRequest {
        reportee_user_id: payload.user_principal.to_string(),
        reportee_canister_id: payload.user_canister_id.to_string(),
        video_canister_id: payload.canister_id.to_string(),
        video_post_id: payload.post_id as u32,
        video_id: payload.video_id,
        reason: payload.reason,
    };

    client.report_video(request).await.map_err(|e| {
        log::error!("Failed to report video: {}", e);

        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to report video: {}", e),
        )
    })?;

    Ok((StatusCode::OK, "Report post success".to_string()))
}
