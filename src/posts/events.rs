use std::sync::Arc;

use axum::response::IntoResponse;
use axum::{extract::State, Json};
use candid::Principal;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use yral_canisters_client::individual_user_template::SuccessHistoryItemV1;
use yral_canisters_client::individual_user_template::WatchHistoryItem;

use crate::app_state::AppState;
use crate::utils::time::system_time_to_custom;

use super::types::PostRequest;
use super::verify::VerifiedPostRequest;

#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct VideoDurationWatchedRequest {
    #[schema(value_type = String)]
    pub user_canister_id: Principal,
    #[schema(value_type = String)]
    pub user_principal: Principal,
    #[schema(value_type = String)]
    pub publisher_canister_id: Principal,
    pub post_id: u64,
    pub video_id: String,
    pub percentage_watched: f64,
}

#[utoipa::path(
    post,
    path = "/events/video_duration_watched",
    request_body = PostRequest<VideoDurationWatchedRequest>,
    tag = "posts/events",
    responses(
        (status = 200, description = "SUCCESS"),
        (status = 500, description = "Internal server error"),
        (status = 403, description = "Forbidden"),
    )
)]
pub async fn handle_video_duration_watched(
    State(state): State<Arc<AppState>>,
    Json(verified_request): Json<VerifiedPostRequest<VideoDurationWatchedRequest>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if verified_request.request.request_body.user_canister_id != verified_request.user_canister {
        return Err((StatusCode::FORBIDDEN, "Forbidden".to_string()));
    }

    let request_body = verified_request.request.request_body;

    let user_canister = state.individual_user(verified_request.user_canister);

    if request_body.percentage_watched >= 90.0 {
        let watch_history_item = WatchHistoryItem {
            post_id: request_body.post_id,
            viewed_at: system_time_to_custom(std::time::SystemTime::now()),
            percentage_watched: request_body.percentage_watched as f32,
            publisher_canister_id: request_body.publisher_canister_id,
            cf_video_id: request_body.video_id.clone(),
        };

        let res = user_canister.update_watch_history(watch_history_item).await;
        match res {
            Ok(_) => (),
            Err(e) => {
                log::error!("Error updating watch history: {:?}", e);
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Error updating watch history: {:?}", e),
                ));
            }
        }
    }

    if request_body.percentage_watched >= 95.0 {
        let success_history_item = SuccessHistoryItemV1 {
            post_id: request_body.post_id,
            interacted_at: system_time_to_custom(std::time::SystemTime::now()),
            publisher_canister_id: request_body.publisher_canister_id,
            cf_video_id: request_body.video_id,
            percentage_watched: request_body.percentage_watched as f32,
            item_type: "video_duration_watched".to_string(),
        };

        let res = user_canister
            .update_success_history(success_history_item)
            .await;
        match res {
            Ok(_) => (),
            Err(e) => {
                log::error!("Error updating success history: {:?}", e);
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Error updating success history: {:?}", e),
                ));
            }
        }
    }

    Ok((StatusCode::OK, "SUCCESS".to_string()))
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct LikeVideoRequest {
    #[schema(value_type = String)]
    pub user_canister_id: Principal,
    #[schema(value_type = String)]
    pub user_principal: Principal,
    #[schema(value_type = String)]
    pub publisher_canister_id: Principal,
    pub post_id: u64,
    pub video_id: String,
}

#[utoipa::path(
    post,
    path = "/events/like_video",
    request_body = PostRequest<LikeVideoRequest>,
    tag = "posts/events",
    responses(
        (status = 200, description = "SUCCESS"),
        (status = 500, description = "Internal server error"),
        (status = 403, description = "Forbidden"),
    )
)]
pub async fn handle_like_video(
    State(state): State<Arc<AppState>>,
    Json(verified_request): Json<VerifiedPostRequest<LikeVideoRequest>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if verified_request.request.request_body.user_canister_id != verified_request.user_canister {
        return Err((StatusCode::FORBIDDEN, "Forbidden".to_string()));
    }

    let request_body = verified_request.request.request_body;

    let user_canister = state.individual_user(verified_request.user_canister);

    let success_history_item = SuccessHistoryItemV1 {
        post_id: request_body.post_id,
        interacted_at: system_time_to_custom(std::time::SystemTime::now()),
        publisher_canister_id: request_body.publisher_canister_id,
        cf_video_id: request_body.video_id,
        percentage_watched: 0.0,
        item_type: "like_video".to_string(),
    };

    let res = user_canister
        .update_success_history(success_history_item)
        .await;
    match res {
        Ok(_) => (),
        Err(e) => {
            log::error!("Error updating success history: {:?}", e);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error updating success history: {:?}", e),
            ));
        }
    }

    Ok((StatusCode::OK, "SUCCESS".to_string()))
}
