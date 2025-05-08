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
use consts::STORJ_INTERFACE_TOKEN;
use env_logger::{Builder, Target};
use events::event::storj::enqueue_storj_backfill_item;
use events::nsfw::extract_frames_and_upload;
use http::header::CONTENT_TYPE;
use log::LevelFilter;
use offchain_service::report_approved_handler;
use once_cell::sync::Lazy;
use qstash::qstash_router;
use sentry_tower::{NewSentryLayer, SentryHttpLayer};
use tonic::service::Routes;
use tonic::transport::Server;
use tower::make::Shared;
use tower::steer::Steer;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::instrument;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_swagger_ui::SwaggerUi;

use crate::auth::check_auth_grpc;
use crate::canister::canisters_list_handler;
use crate::canister::reclaim_canisters::reclaim_canisters_handler;
use crate::canister::snapshot::{backup_job_handler, backup_job_handler_without_auth};
use crate::duplicate_video::backfill::trigger_videohash_backfill;
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
mod duplicate_video;
mod error;
mod events;
pub mod metrics;
mod offchain_service;
mod posts;
mod qstash;
mod types;
pub mod utils;

use app_state::AppState;

async fn main_impl() -> Result<()> {
    #[derive(OpenApi)]
    #[openapi(
        tags(
            (name = "OFF_CHAIN", description = "Off Chain Agent API")
        )
    )]
    struct ApiDoc;

    let conf = AppConfig::load()?;

    Lazy::force(&STORJ_INTERFACE_TOKEN);

    let shared_state = Arc::new(AppState::new(conf.clone()).await);

    let sentry_tower_layer = ServiceBuilder::new()
        .layer(NewSentryLayer::new_from_top())
        .layer(SentryHttpLayer::with_transaction());

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

    let admin_routes = Router::new()
        .route("/backfill/videohash", post(trigger_videohash_backfill))
        .with_state(shared_state.clone());

    let http = Router::new()
        .route("/healthz", get(health_handler))
        .route("/start_backup", get(backup_job_handler))
        .route("/canisters_list", get(canisters_list_handler))
        .route("/report-approved", post(report_approved_handler))
        .route("/import-video", post(upload_user_video_handler))
        .route(
            "/upgrade_user_token_sns_canister/{individual_user_canister_id}",
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
        .route(
            "/enqueue_storj_backfill_item",
            post(enqueue_storj_backfill_item),
        )
        .route(
            "/api/redis_backfill",
            post(duplicate_video::redis_backfill::trigger_redis_backfill),
        )
        .nest("/admin", admin_routes)
        .nest("/qstash", qstash_routes)
        .fallback_service(router)
        .layer(CorsLayer::permissive())
        .layer(sentry_tower_layer)
        .with_state(shared_state.clone());

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(warehouse_events::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(off_chain::FILE_DESCRIPTOR_SET)
        .build_v1()
        .unwrap();

    let grpc_axum = Routes::builder()
        .routes()
        .add_service(WarehouseEventsServer::with_interceptor(
            WarehouseEventsService {
                shared_state: shared_state.clone(),
            },
            check_auth_grpc,
        ))
        .add_service(OffChainServer::with_interceptor(
            OffChainService {
                shared_state: shared_state.clone(),
            },
            check_auth_grpc,
        ))
        .add_service(OffChainCanisterServer::with_interceptor(
            OffChainCanisterService {
                shared_state: shared_state.clone(),
            },
            check_auth_grpc_offchain_mlfeed,
        ))
        .add_service(reflection_service)
        .into_axum_router()
        .layer(NewSentryLayer::new_from_top());

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

fn main() {
    let _guard = sentry::init((
        "https://9a2d5e94760b78c84361380a30eae9ef@sentry.yral.com/2",
        sentry::ClientOptions {
            release: sentry::release_name!(),
            // debug: true, // use when debugging sentry issues
            traces_sample_rate: 0.3,
            ..Default::default()
        },
    ));

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                // axum logs rejections from built-in extractors with the `axum::rejection`
                // target, at `TRACE` level. `axum::rejection=trace` enables showing those events
                format!(
                    "{}=debug,tower_http=debug,axum::rejection=trace",
                    env!("CARGO_CRATE_NAME")
                )
                .into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .with(sentry_tracing::layer())
        .init();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            main_impl().await.unwrap();
        });
}

#[instrument]
async fn health_handler() -> (StatusCode, &'static str) {
    log::info!("Health check");
    log::warn!("Health check");
    log::error!("Health check");

    (StatusCode::OK, "OK")
}
