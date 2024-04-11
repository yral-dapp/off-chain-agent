use axum::http::{StatusCode};
use axum::{async_trait, response::Html, Router, routing::{get}};
use std::env;
use std::net::SocketAddr;
use axum::error_handling::HandleErrorLayer;
use axum::middleware::from_fn;
use http::header::CONTENT_TYPE;
use tonic::transport::Server;
use tower::make::Shared;
use tower::ServiceExt;
use tower::steer::Steer;
use axum::{debug_handler};
use axum::extract::FromRequest;
use crate::auth::AuthBearer;

pub mod canister;
mod events;
mod auth;

use crate::canister::canisters_list_handler;
use crate::canister::snapshot::backup_job_handler;
use crate::events::warehouse_events::warehouse_events_server::WarehouseEventsServer;
use crate::events::WarehouseEventsService;

#[tokio::main]
async fn main() {
    // build our application with a route
    let http = Router::new()
        .route("/", get(hello_work_handler))
        .route("/healthz", get(health_handler))
        .route("/start_backup", get(backup_job_handler))
        .route("/canisters_list", get(canisters_list_handler))
        .map_err(axum::BoxError::from)
        .boxed_clone();

    let grpc = tonic::transport::Server::builder()
        .add_service(WarehouseEventsServer::new(WarehouseEventsService{}))
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
    let addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 8080));

    println!("listening on {}", addr);

    axum::Server::bind(&addr)
        .serve(Shared::new(http_grpc))
        .await
        .unwrap();
}

#[debug_handler]
async fn hello_work_handler(AuthBearer(token): AuthBearer) -> Html<&'static str> {
    println!("token: {}", token);
    if token
        != env::var("CF_WORKER_ACCESS_OFF_CHAIN_AGENT_KEY")
            .expect("$CF_WORKER_ACCESS_OFF_CHAIN_AGENT_KEY is not set")
    {
        return Html("Unauthorized");
    }

    Html("Hello, World!")
}

async fn health_handler() -> (StatusCode, &'static str) {
    (StatusCode::OK, "OK")
}


