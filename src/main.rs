use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use auth::{check_auth_grpc_offchain_mlfeed, check_auth_grpc_test};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{response::Html, routing::get, Router};
use canister::mlfeed_cache::off_chain::off_chain_canister_server::OffChainCanisterServer;
use canister::mlfeed_cache::OffChainCanisterService;
use canister::upload_user_video::upload_user_video_handler;
use config::AppConfig;
use env_logger::{Builder, Target};
use http::header::CONTENT_TYPE;
use log::LevelFilter;
use offchain_service::report_approved_handler;
use reqwest::Url;
use tower::make::Shared;
use tower::steer::Steer;
use tower::ServiceExt;
use yral_metadata_client::consts::DEFAULT_API_URL;
use yral_metadata_client::MetadataClient;
use yup_oauth2::ServiceAccountAuthenticator;

use crate::auth::{check_auth_grpc, AuthBearer};
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
mod types;

use app_state::AppState;
// struct AppState {
//     yral_metadata_client: MetadataClient<true>,
//     google_sa_key_access_token: String,
// }

#[tokio::main]
async fn main() -> Result<()> {
    let conf = AppConfig::load()?;

    Builder::new()
        .filter_level(LevelFilter::Info)
        .target(Target::Stdout)
        .init();

    let shared_state = Arc::new(AppState::new(conf.clone()).await);

    // build our application with a route
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
            "/get-snapshot",
            get(canister::snapshot::get_snapshot_canister),
        )
        .with_state(shared_state.clone())
        .map_err(axum::BoxError::from)
        .boxed_clone();

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(warehouse_events::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(off_chain::FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    let grpc = tonic::transport::Server::builder()
        .accept_http1(true)
        .add_service(tonic_web::enable(WarehouseEventsServer::with_interceptor(
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
        .add_service(reflection_service)
        .into_service()
        .map_response(|r| r.map(axum::body::boxed))
        .boxed_clone();

    let http_grpc = Steer::new(
        vec![http, grpc],
        |req: &http::Request<hyper::Body>, _svcs: &[_]| {
            if req.headers().get(CONTENT_TYPE).map(|v| v.as_bytes()) != Some(b"application/grpc") {
                0
            } else {
                1
            }
        },
    );

    // run it
    let addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 50051));

    log::info!("listening on {}", addr);

    axum::Server::bind(&addr)
        .serve(Shared::new(http_grpc))
        .await
        .unwrap();

    Ok(())
}

async fn health_handler() -> (StatusCode, &'static str) {
    log::info!("Health check");
    log::warn!("Health check");
    log::error!("Health check");

    (StatusCode::OK, "OK")
}
