use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use candid::Principal;
use futures::stream::StreamExt;
use google_cloud_bigquery::{
    client::Client,
    http::tabledata::insert_all::{InsertAllRequest, Row},
};
use serde::{Deserialize, Serialize};
use tracing::instrument;
use utoipa::ToSchema;
use yral_canisters_client::individual_user_template::Result6;

use crate::{app_state::AppState, posts::types::VideoDeleteRow};

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DeleteUserRequest {
    pub id_token: String,
    pub canister_id: String,
    pub user_principal: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserPost {
    pub canister_id: String,
    pub post_id: u64,
    pub video_id: String,
}

#[utoipa::path(
    delete,
    path = "/user",
    request_body = DeleteUserRequest,
    tag = "user",
    responses(
        (status = 200, description = "User deleted successfully"),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error"),
    )
)]
#[instrument(skip(state))]
pub async fn handle_delete_user(
    State(state): State<Arc<AppState>>,
    Json(request): Json<DeleteUserRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Verify the ID token
    // let claims = state
    //     .yral_auth_jwt
    //     .verify_token(&request.id_token)
    //     .map_err(|e| (StatusCode::UNAUTHORIZED, format!("Invalid token: {}", e)))?;

    // let user_principal = Principal::from_text(&claims.sub)
    //     .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid principal: {}", e)))?;

    // // Get user's canister ID
    // let user_canister = state
    //     .get_individual_canister_by_user_principal(user_principal)
    //     .await
    //     .map_err(|e| {
    //         (
    //             StatusCode::NOT_FOUND,
    //             format!("User canister not found: {}", e),
    //         )
    //     })?;

    let user_principal = Principal::from_text(&request.user_principal)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid principal: {}", e)))?;

    let user_canister = Principal::from_text(&request.canister_id)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid canister: {}", e)))?;

    // 1. Get all posts for the user from canister
    let posts = get_user_posts(&state, user_canister).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to get user posts: {}", e),
        )
    })?;

    // 2. Bulk insert into video_deleted table
    if !posts.is_empty() {
        bulk_insert_video_delete_rows(&state.bigquery_client, posts.clone())
            .await
            .map_err(|e| {
                log::error!("Failed to bulk insert video delete rows: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to bulk delete posts: {}", e),
                )
            })?;
    }

    // 3. Delete posts from canister (spawn as background task with concurrency)
    let state_clone = state.clone();
    let posts_for_deletion = posts.clone();
    tokio::spawn(async move {
        delete_posts_from_canister(state_clone, posts_for_deletion).await;
    });

    // 4. Handle duplicate posts cleanup (spawn as background task with concurrency)
    let bigquery_client = state.bigquery_client.clone();
    let video_ids: Vec<String> = posts.iter().map(|p| p.video_id.clone()).collect();
    tokio::spawn(async move {
        handle_duplicate_posts_cleanup(bigquery_client, video_ids).await;
    });

    // 5. Delete from Redis caches
    let ml_feed_cache = state.ml_feed_cache.clone();
    ml_feed_cache
        .delete_user_caches(&user_principal.to_string())
        .await
        .map_err(|e| {
            log::error!("Failed to delete user caches: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to delete user caches: {}", e),
            )
        })?;

    // 7. Add deleted canister to SpaceTimeDB [TODO: place this below after testing]
    #[cfg(not(feature = "local-bin"))]
    {
        if let Err(e) = state
            .canisters_ctx
            .add_deleted_canister(user_canister, user_principal)
            .await
        {
            log::error!("Failed to add deleted canister to SpaceTimeDB: {}", e);
            // Don't fail the request if SpaceTimeDB call fails
        }
    }

    // 6. Delete user metadata using yral_metadata_client
    state
        .yral_metadata_client
        .delete_metadata_bulk(vec![user_principal])
        .await
        .map_err(|e| {
            log::error!("Failed to delete user metadata: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to delete user metadata: {}", e),
            )
        })?;

    Ok((StatusCode::OK, "User deleted successfully".to_string()))
}

async fn get_user_posts(
    state: &AppState,
    canister_id: Principal,
) -> Result<Vec<UserPost>, anyhow::Error> {
    let mut all_posts = Vec::new();
    let mut start = 0u64;
    let batch_size = 100u64; // Get 100 posts at a time

    loop {
        let end = start + batch_size;

        let result = state
            .individual_user(canister_id)
            .get_posts_of_this_user_profile_with_pagination_cursor(start, end)
            .await?;

        match result {
            Result6::Ok(posts) => {
                let result_len = posts.len();
                all_posts.extend(posts.into_iter().map(|p| UserPost {
                    canister_id: canister_id.to_string(),
                    post_id: p.id,
                    video_id: p.video_uid,
                }));
                if result_len < batch_size as usize {
                    break;
                }
            }
            Result6::Err(_) => {
                break;
            }
        }

        start = end;
    }

    Ok(all_posts)
}

async fn delete_posts_from_canister(state: Arc<AppState>, posts: Vec<UserPost>) {
    let futures: Vec<_> = posts
        .into_iter()
        .map(|post| {
            let state_inner = state.clone();
            async move {
                let canister_principal = Principal::from_text(&post.canister_id)
                    .map_err(|e| format!("Invalid canister principal: {}", e))?;

                match state_inner
                    .individual_user(canister_principal)
                    .delete_post(post.post_id)
                    .await
                {
                    Ok(_) => {
                        log::info!(
                            "Successfully deleted post {} from canister {}",
                            post.post_id,
                            post.canister_id
                        );
                        Ok(())
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to delete post {} from canister {}: {}",
                            post.post_id,
                            post.canister_id,
                            e
                        );
                        Err(format!("Failed to delete post: {}", e))
                    }
                }
            }
        })
        .collect();

    let mut buffered = futures::stream::iter(futures).buffer_unordered(10);
    while let Some(result) = buffered.next().await {
        if let Err(e) = result {
            log::error!("Post deletion error: {}", e);
        }
    }
}

async fn handle_duplicate_posts_cleanup(bigquery_client: Client, video_ids: Vec<String>) {
    let futures: Vec<_> = video_ids
        .into_iter()
        .map(|video_id| {
            let client = bigquery_client.clone();
            async move {
                match crate::posts::delete_post::handle_duplicate_post_on_delete(
                    client,
                    video_id.clone(),
                )
                .await
                {
                    Ok(_) => {
                        log::info!(
                            "Successfully handled duplicate post cleanup for video: {}",
                            video_id
                        );
                        Ok(())
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to handle duplicate post on delete for video {}: {}",
                            video_id,
                            e
                        );
                        Err(format!("Failed to handle duplicate post: {}", e))
                    }
                }
            }
        })
        .collect();

    let mut buffered = futures::stream::iter(futures).buffer_unordered(10);
    while let Some(result) = buffered.next().await {
        if let Err(e) = result {
            log::error!("Duplicate post cleanup error: {}", e);
        }
    }
}

async fn bulk_insert_video_delete_rows(
    bq_client: &Client,
    posts: Vec<UserPost>,
) -> Result<(), anyhow::Error> {
    // Process posts in batches of 500
    for chunk in posts.chunks(500) {
        let rows: Vec<Row<VideoDeleteRow>> = chunk
            .iter()
            .map(|post| {
                let video_delete_row = VideoDeleteRow {
                    canister_id: post.canister_id.clone(),
                    post_id: post.post_id,
                    video_id: post.video_id.clone(),
                    gcs_video_id: format!("gs://yral-videos/{}.mp4", post.video_id),
                };
                Row::<VideoDeleteRow> {
                    insert_id: None,
                    json: video_delete_row,
                }
            })
            .collect();

        let request = InsertAllRequest {
            rows,
            ..Default::default()
        };

        let res = bq_client
            .tabledata()
            .insert(
                "hot-or-not-feed-intelligence",
                "yral_ds",
                "video_deleted",
                &request,
            )
            .await?;

        if let Some(errors) = res.insert_errors {
            if !errors.is_empty() {
                log::error!("video_deleted bulk insert errors: {:?}", errors);
                return Err(anyhow::anyhow!(
                    "Failed to bulk insert video deleted rows to bigquery"
                ));
            }
        }
    }

    Ok(())
}
