use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use auth::check_auth_grpc_offchain_mlfeed;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{routing::get, Router};
use canister::mlfeed_cache::off_chain::off_chain_canister_server::OffChainCanisterServer;
use canister::mlfeed_cache::{
    update_ml_feed_cache, update_ml_feed_cache_nsfw, OffChainCanisterService,
};
use canister::upgrade_user_token_sns_canister::{
    upgrade_user_token_sns_canister_for_entire_network, upgrade_user_token_sns_canister_handler,
};
use canister::upload_user_video::upload_user_video_handler;
use config::AppConfig;
use env_logger::{Builder, Target};
use events::nsfw::extract_frames_and_upload;
use http::header::CONTENT_TYPE;
use log::LevelFilter;
use offchain_service::report_approved_handler;
use qstash::qstash_router;
use tower::make::Shared;
use tower::steer::Steer;
use tower_http::cors::CorsLayer;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_swagger_ui::SwaggerUi;

use crate::auth::check_auth_grpc;
use crate::canister::canisters_list_handler;
use crate::canister::reclaim_canisters::reclaim_canisters_handler;
use crate::canister::snapshot::{backup_job_handler, backup_job_handler_without_auth};
use crate::events::warehouse_events::warehouse_events_server::WarehouseEventsServer;
use crate::events::{warehouse_events, WarehouseEventsService};
use crate::offchain_service::off_chain::off_chain_server::OffChainServer;
use crate::offchain_service::{off_chain, OffChainService};
use error::*;

mod app_state;
mod auth;
pub mod canister;
mod config;
mod consts;
mod error;
mod events;
mod offchain_service;
mod posts;
mod qstash;
mod types;
pub mod utils;

use app_state::AppState;

#[tokio::main]
async fn main() -> Result<()> {
    #[derive(OpenApi)]
    #[openapi(
        tags(
            (name = "OFF_CHAIN", description = "Off Chain Agent API")
        )
    )]
    struct ApiDoc;

    let conf = AppConfig::load()?;

    Builder::new()
        .filter_level(LevelFilter::Info)
        .target(Target::Stdout)
        .init();

    let shared_state = Arc::new(AppState::new(conf.clone()).await);

    let (router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .nest("/api/v1/posts", posts::posts_router(shared_state.clone()))
        .nest(
            "/api/v1/events",
            events::events_router(shared_state.clone()),
        )
        .split_for_parts();

    let router =
        router.merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", api.clone()));

    // build our application with a route
    let qstash_routes = qstash_router(shared_state.clone());
    let http = Router::new()
        .route("/healthz", get(health_handler))
        .route("/start_backup", get(backup_job_handler))
        .route(
            "/start_backup_without_auth",
            get(backup_job_handler_without_auth),
        )
        .route("/canisters_list", get(canisters_list_handler))
        .route("/reclaim_canisters", get(reclaim_canisters_handler))
        .route("/report-approved", post(report_approved_handler))
        .route("/import-video", post(upload_user_video_handler))
        .route(
            "/upgrade_user_token_sns_canister/:individual_user_canister_id",
            post(upgrade_user_token_sns_canister_handler),
        )
        .route(
            "/upgrade_user_token_sns_canister_for_entire_network",
            post(upgrade_user_token_sns_canister_for_entire_network),
        )
        .route(
            "/get-snapshot",
            get(canister::snapshot::get_snapshot_canister),
        )
        .route("/extract-frames", post(extract_frames_and_upload))
        .route("/update-global-ml-feed-cache", get(update_ml_feed_cache))
        .route(
            "/update-global-ml-feed-cache-nsfw",
            get(update_ml_feed_cache_nsfw),
        )
        .nest("/qstash", qstash_routes)
        .nest_service("/", router)
        .layer(CorsLayer::permissive())
        .with_state(shared_state.clone());

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(warehouse_events::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(off_chain::FILE_DESCRIPTOR_SET)
        .build_v1()
        .unwrap();

    let mut grpc = tonic::service::Routes::builder();
    grpc.add_service(tonic_web::enable(WarehouseEventsServer::with_interceptor(
        WarehouseEventsService {
            shared_state: shared_state.clone(),
        },
        check_auth_grpc,
    )))
    .add_service(tonic_web::enable(OffChainServer::with_interceptor(
        OffChainService {
            shared_state: shared_state.clone(),
        },
        check_auth_grpc,
    )))
    .add_service(tonic_web::enable(OffChainCanisterServer::with_interceptor(
        OffChainCanisterService {
            shared_state: shared_state.clone(),
        },
        check_auth_grpc_offchain_mlfeed,
    )))
    .add_service(reflection_service);
    let grpc_axum = grpc.routes().into_axum_router();

    let http_grpc = Steer::new(
        vec![http, grpc_axum],
        |req: &axum::extract::Request, _svcs: &[_]| {
            if req.headers().get(CONTENT_TYPE).map(|v| v.as_bytes()) != Some(b"application/grpc") {
                0
            } else {
                1
            }
        },
    );

    // run it
    let addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 50051));
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();

    log::info!("listening on {}", addr);

    axum::serve(listener, Shared::new(http_grpc)).await.unwrap();

    Ok(())
}

async fn health_handler() -> (StatusCode, &'static str) {
    log::info!("Health check");
    log::warn!("Health check");
    log::error!("Health check");

    (StatusCode::OK, "OK")
}
