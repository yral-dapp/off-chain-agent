use std::{error::Error, sync::Arc};

// use aide::axum::routing::post_with;
// use aide::axum::ApiRouter;
// use aide::{axum::routing::delete_with, transform::TransformOperation};
use axum::{
    extract::State,
    http::StatusCode,
    middleware,
    routing::{delete, get, post},
    Json,
};
use candid::Principal;
use http::Response;
use ic_agent::{identity::DelegatedIdentity, Agent};
// use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use types::VideoDeleteRow;
use utils::{get_agent_from_delegated_identity_wire, insert_video_delete_row_to_bigquery};
use utoipa::{openapi::request_body, ToSchema};
use utoipa_axum::{router::OpenApiRouter, routes};
use verify::{verify_post_request, VerifiedPostRequest};
use yral_canisters_client::individual_user_template::{IndividualUserTemplate, Result1};

use crate::{app_state::AppState, types::DelegatedIdentityWire};

mod events;
mod types;
mod utils;
mod verify;

// #[derive(Serialize, Deserialize, Clone, JsonSchema)]
// pub struct CreatePostRequest {
//     canister_id: String,
//     post_id: u64,
// }

// pub fn todo_routes(state: Arc<AppState>) -> ApiRouter {
//     ApiRouter::new()
//         .api_route("/", post_with(handle_create_post, create_post_docs))
//         .with_state(state)
// }

// #[axum::debug_handler]
// async fn handle_create_post(
//     State(state): State<Arc<AppState>>,
//     Json(verified_request): Json<CreatePostRequest>,
// ) -> Result<Response<String>, StatusCode> {
//     Ok(Response::builder()
//         .status(StatusCode::OK)
//         .body("Post created".into())
//         .unwrap())
// }

// fn create_post_docs(op: TransformOperation) -> TransformOperation {
//     op.description("Create a new incomplete Todo item.")
//         .response::<201, String>()
// }

pub fn posts_router(state: Arc<AppState>) -> OpenApiRouter {
    let delete_router = OpenApiRouter::new()
        .routes(routes!(handle_delete_post))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            verify_post_request::<DeletePostRequest>,
        ));

    let report_router = OpenApiRouter::new()
        .routes(routes!(handle_report_post))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            verify_post_request::<ReportPostRequest>,
        ));

    OpenApiRouter::new()
        .merge(delete_router)
        .merge(report_router)
        .with_state(state)
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct PostRequest<T> {
    delegated_identity_wire: DelegatedIdentityWire,
    #[serde(flatten)]
    request_body: T,
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct DeletePostRequest {
    #[schema(value_type = String)]
    canister_id: Principal,
    post_id: u64,
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct ReportPostRequest {
    #[schema(value_type = String)]
    canister_id: Principal,
    post_id: u64,
}

#[utoipa::path(
    delete,
    path = "",
    tag = "posts",
    responses(
        (status = 200, description = "Delete post success", body = [PostRequest<DeletePostRequest>]),
        (status = 400, description = "Delete post failed"),
        (status = 500, description = "Internal server error"),
        (status = 403, description = "Forbidden"),
    )
)]
async fn handle_delete_post(
    State(state): State<Arc<AppState>>,
    Json(verified_request): Json<VerifiedPostRequest<DeletePostRequest>>,
) -> Result<Response<String>, StatusCode> {
    // Verify that the canister ID matches the user's canister
    if verified_request.request.request_body.canister_id != verified_request.user_canister {
        return Err(StatusCode::FORBIDDEN);
    }

    let request_body = verified_request.request.request_body;

    let canister_id = request_body.canister_id.to_string();
    let post_id = request_body.post_id;

    let agent =
        get_agent_from_delegated_identity_wire(&verified_request.request.delegated_identity_wire)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let individual_user_template = IndividualUserTemplate(verified_request.user_canister, &agent);

    let post_details = match individual_user_template
        .get_individual_post_details_by_id(post_id)
        .await
    {
        Ok(post_details) => post_details,
        Err(_) => return Err(StatusCode::BAD_REQUEST),
    };
    let video_id = post_details.video_uid;

    // Call the canister to delete the post
    let delete_res = individual_user_template.delete_post(post_id).await;
    match delete_res {
        Ok(Result1::Ok) => (),
        Ok(Result1::Err(_)) => return Err(StatusCode::BAD_REQUEST),
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    }

    insert_video_delete_row_to_bigquery(state, canister_id, post_id, video_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Response::builder()
        .status(StatusCode::OK)
        .body("Post deleted".into())
        .unwrap())
}

#[utoipa::path(
    post,
    path = "/report",
    tag = "posts",
    responses(
        (status = 200, description = "Report post success", body = [PostRequest<ReportPostRequest>])
    )
)]
async fn handle_report_post(
    State(state): State<Arc<AppState>>,
    Json(verified_request): Json<VerifiedPostRequest<ReportPostRequest>>,
) -> Result<Response<String>, StatusCode> {
    let post_id = verified_request.request.request_body.post_id;

    // Call the canister to report the post
    // This is a placeholder - you would implement the actual reporting logic

    // Example:
    // let result = verified_request.individual_user.report_post(post_id).await;
    // if let Err(_) = result {
    //     return Err(StatusCode::INTERNAL_SERVER_ERROR);
    // }

    Ok(Response::builder()
        .status(StatusCode::OK)
        .body("Post reported".into())
        .unwrap())
}
