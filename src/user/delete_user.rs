use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use candid::Principal;
use google_cloud_bigquery::{
    client::Client,
    http::{
        job::query::QueryRequest,
        tabledata::insert_all::{InsertAllRequest, Row},
    },
    query::row::Row as QueryRow,
};
use serde::{Deserialize, Serialize};
use tracing::instrument;
use utoipa::ToSchema;

use crate::{
    app_state::AppState,
    posts::{delete_post::insert_video_delete_row_to_bigquery, types::VideoDeleteRow},
    utils::yral_auth_jwt::YralAuthJwt,
};

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DeleteUserRequest {
    pub id_token: String,
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
    let yral_auth_jwt = YralAuthJwt::init(
        std::env::var("YRAL_AUTH_V2_SIGNING_PUBLIC_KEY").map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Missing auth config".to_string(),
            )
        })?,
    )
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Auth init error: {}", e),
        )
    })?;

    let claims = yral_auth_jwt
        .verify_token(&request.id_token)
        .map_err(|e| (StatusCode::UNAUTHORIZED, format!("Invalid token: {}", e)))?;

    let user_principal = Principal::from_text(&claims.sub)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid principal: {}", e)))?;

    // Get user's canister ID
    let user_canister = state
        .get_individual_canister_by_user_principal(user_principal)
        .await
        .map_err(|e| {
            (
                StatusCode::NOT_FOUND,
                format!("User canister not found: {}", e),
            )
        })?;

    let canister_id = user_canister.to_string();

    // 1. Get all posts for the user from BigQuery
    #[cfg(not(feature = "local-bin"))]
    let posts = get_user_posts(&state.bigquery_client, &canister_id)
        .await
        .map_err(|e| {
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
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to bulk delete posts: {}", e),
                )
            })?;
    }

    // 3. Handle duplicate posts cleanup (spawn as background task)
    let bigquery_client = state.bigquery_client.clone();
    let video_ids: Vec<String> = posts.iter().map(|p| p.video_id.clone()).collect();
    tokio::spawn(async move {
        for video_id in video_ids {
            if let Err(e) = crate::posts::delete_post::handle_duplicate_post_on_delete(
                bigquery_client.clone(),
                video_id,
            )
            .await
            {
                log::error!("Failed to handle duplicate post on delete: {}", e);
            }
        }
    });

    // 4. Delete from Redis caches
    #[cfg(not(feature = "local-bin"))]
    {
        // Delete from ML feed cache - implementation would depend on ML feed cache structure
        // For now, we'll log that this needs to be implemented
        log::info!(
            "TODO: Implement ML feed cache deletion for user: {}",
            user_principal
        );

        // Delete from canister backup cache if needed
        // This would need to be implemented based on your Redis structure
    }

    // 5. Call yral-metadata delete API
    let metadata_url = format!(
        "https://yral-metadata.fly.dev/delete_user_metadata?user_principal={}",
        user_principal
    );

    let client = reqwest::Client::new();
    let metadata_response = client
        .delete(&metadata_url)
        .header(
            "Authorization",
            format!(
                "Bearer {}",
                std::env::var("YRAL_METADATA_TOKEN").unwrap_or_default()
            ),
        )
        .send()
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to call metadata API: {}", e),
            )
        })?;

    if !metadata_response.status().is_success() {
        log::error!(
            "Metadata API returned non-success status: {}",
            metadata_response.status()
        );
    }

    Ok((StatusCode::OK, "User deleted successfully".to_string()))
}

async fn get_user_posts(
    bq_client: &Client,
    canister_id: &str,
) -> Result<Vec<UserPost>, anyhow::Error> {
    let query = format!(
        r#"
        SELECT
            v.publisher_canister_id as canister_id,
            CAST(v.post_id AS INT64) as post_id,
            v.video_id
        FROM `hot-or-not-feed-intelligence.yral_ds.video_transcoding_queue_replica` v
        LEFT JOIN `hot-or-not-feed-intelligence.yral_ds.video_deleted` d
            ON v.video_id = d.video_id
        WHERE v.publisher_canister_id = '{}'
            AND d.video_id IS NULL
        "#,
        canister_id
    );

    let request = QueryRequest {
        query,
        ..Default::default()
    };

    let mut response = bq_client
        .query::<QueryRow>("hot-or-not-feed-intelligence", request)
        .await?;

    let mut posts = Vec::new();
    while let Some(row) = response.next().await? {
        let canister_id: String = row.column(0)?;
        let post_id: i64 = row.column(1)?;
        let video_id: String = row.column(2)?;

        posts.push(UserPost {
            canister_id,
            post_id: post_id as u64,
            video_id,
        });
    }

    Ok(posts)
}

async fn bulk_insert_video_delete_rows(
    bq_client: &Client,
    posts: Vec<UserPost>,
) -> Result<(), anyhow::Error> {
    let rows: Vec<Row<VideoDeleteRow>> = posts
        .into_iter()
        .map(|post| {
            let video_delete_row = VideoDeleteRow {
                canister_id: post.canister_id,
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

    // Insert in batches of 500 rows
    for chunk in rows.chunks(500) {
        let request = InsertAllRequest {
            rows: chunk.to_vec(),
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
