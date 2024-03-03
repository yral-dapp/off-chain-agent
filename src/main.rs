use axum::http::StatusCode;
use axum::{response::Html, routing::get, Router};
use axum_auth::AuthBearer;
use std::env;
use std::net::SocketAddr;

use crate::canister::canisters_list_handler;
use crate::canister::snapshot::backup_job_handler;

pub mod canister;

#[tokio::main]
async fn main() {
    // build our application with a route
    let app = Router::new()
        .route("/", get(hello_work_handler))
        .route("/healthz", get(health_handler))
        .route("/start_backup", get(backup_job_handler))
        .route("/canisters_list", get(canisters_list_handler));

    // run it
    // let addr: SocketAddrV6 = "[::]:8080".parse().unwrap();
    let addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 8080));

    println!("listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// async fn canister_list_handler() -> Html<String> {
//     let start = Instant::now();
//     let creds = Credentials::from_env().unwrap();
//     let region = Region::UsEast1;
//     let bucket = Bucket::new("ic-reclaim-backup", region, creds).unwrap();
//     let mut result = String::new();
//     for (key, _size, _last_modified) in bucket.list("").await.unwrap() {
//         result.push_str(&key);
//         result.push_str("\n");
//     }
//     let duration = start.elapsed();
//     println!("Time elapsed in canister_list_handler() is: {:?}", duration);
//     Html(result)
// }

async fn hello_work_handler(AuthBearer(token): AuthBearer) -> Html<&'static str> {
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
