use std::env;
use std::net::SocketAddr;

use axum::http::StatusCode;
use axum::{response::Html, routing::get, Router};
use http::header::CONTENT_TYPE;
use tower::make::Shared;
use tower::steer::Steer;
use tower::ServiceExt;

use crate::auth::{check_auth_grpc, AuthBearer};
use crate::canister::canisters_list_handler;
use crate::canister::reclaim_canisters::reclaim_canisters_handler;
use crate::canister::snapshot::backup_job_handler;
use crate::events::warehouse_events::warehouse_events_server::WarehouseEventsServer;
use crate::events::{warehouse_events, WarehouseEventsService};

mod auth;
pub mod canister;
mod consts;
mod events;

#[tokio::main]
async fn main() {
    // build our application with a route
    let http = Router::new()
        .route("/", get(hello_work_handler))
        .route("/healthz", get(health_handler))
        .route("/start_backup", get(backup_job_handler))
        .route("/canisters_list", get(canisters_list_handler))
        // .route("/reclaim_canisters", get(reclaim_canisters_handler))
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

    println!("listening on {}", addr);

    axum::Server::bind(&addr)
        .serve(Shared::new(http_grpc))
        .await
        .unwrap();
}

async fn hello_work_handler(AuthBearer(token): AuthBearer) -> Html<&'static str> {
    if token != "test_token"
    // env::var("CF_WORKER_ACCESS_OFF_CHAIN_AGENT_KEY")
    // .expect("$CF_WORKER_ACCESS_OFF_CHAIN_AGENT_KEY is not set")
    {
        return Html("Unauthorized");
    }
    println!("Hello, World!");

    Html("Hello, World!")
}

async fn health_handler() -> (StatusCode, &'static str) {
    println!("Health check");

    (StatusCode::OK, "OK")
}
