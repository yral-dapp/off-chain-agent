use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
// use axum::handler::Handler;
use axum::{
    http::StatusCode, response::Html, routing::get, routing::post,
    Router,
};
// use candid::Principal;
// use crate::canister::{authenticated_canisters, Canisters};

use config::AppConfig;
use env_logger::{Builder, Target};
use http::header::CONTENT_TYPE;
use log::LevelFilter;
// use reqwest::Url;
use tower::make::Shared;
use tower::steer::Steer;
use tower::ServiceExt;
use webhook::cf_stream_webhook_handler;

use crate::auth::{check_auth_grpc, AuthBearer};
use crate::canister::canisters_list_handler;
// use crate::canister::individual_user_template::IndividualUserTemplate;
// use crate::canister::reclaim_canisters::reclaim_canisters_handler;
use crate::canister::snapshot::backup_job_handler;
use crate::events::warehouse_events::warehouse_events_server::WarehouseEventsServer;
use crate::events::{warehouse_events, WarehouseEventsService};
// use error::*;

use app_state::AppState ; 

mod auth;
pub mod canister;
mod config;
mod consts;
mod error;
mod events;
mod types;
mod webhook;
mod app_state;


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
        .route("/", get(hello_work_handler))
        .route("/healthz", get(health_handler))
        .route("/start_backup", get(backup_job_handler))
        .route("/canisters_list", get(canisters_list_handler))
        .route("/cf_webhook", post(cf_stream_webhook_handler))
        // .route_layer(axum::middleware::from_fn(verify_sig_webhook))
        // .route("/reclaim_canisters", get(reclaim_canisters_handler))
        .with_state(shared_state)
        .map_err(axum::BoxError::from)
        .boxed_clone();

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(warehouse_events::FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    let grpc = tonic::transport::Server::builder()
        .accept_http1(true)
        .add_service(tonic_web::enable(WarehouseEventsServer::with_interceptor(
            WarehouseEventsService {},
            check_auth_grpc,
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

async fn hello_work_handler(AuthBearer(token): AuthBearer) -> Html<&'static str> {
    if token
        != env::var("CF_WORKER_ACCESS_OFF_CHAIN_AGENT_KEY")
            .expect("$CF_WORKER_ACCESS_OFF_CHAIN_AGENT_KEY is not set")
    {
        return Html("Unauthorized");
    }
    log::info!("Hello, World!");

    Html("Hello, World!")
}

async fn health_handler() -> (StatusCode, &'static str) {
    log::info!("Health check");
    log::warn!("Health check");
    log::error!("Health check");

    (StatusCode::OK, "OK")
}
