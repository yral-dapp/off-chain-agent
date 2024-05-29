use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
// use axum::debug_handler;
// use axum::handler::Handler;
use axum::{
    extract::Json, extract::State, http::StatusCode, response::Html, routing::get, routing::post,
    Router,
};
use candid::Principal;
// use crate::canister::{authenticated_canisters, Canisters};

use config::AppConfig;
use consts::YRAL_METADATA_URL;
use env_logger::{Builder, Target};
use http::header::CONTENT_TYPE;
use log::LevelFilter;
// use reqwest::Url;
use serde_json::Value;
use tower::make::Shared;
use tower::steer::Steer;
use tower::ServiceExt;
use yral_metadata_client::MetadataClient;

use crate::auth::{check_auth_grpc, AuthBearer};
use crate::canister::canisters_list_handler;
use crate::canister::individual_user_template::IndividualUserTemplate;
// use crate::canister::reclaim_canisters::reclaim_canisters_handler;
use crate::canister::snapshot::backup_job_handler;
use crate::events::warehouse_events::warehouse_events_server::WarehouseEventsServer;
use crate::events::{warehouse_events, WarehouseEventsService};
// use error::*;
use error::AppError;

mod auth;
pub mod canister;
mod config;
mod consts;
mod error;
mod events;
mod types;

use ic_agent::Agent;

#[derive(Clone)]
struct AppState {
    agent: ic_agent::Agent,
    yral_metadata_client: MetadataClient<true>,
}

pub fn init_yral_metadata_client(conf: &AppConfig) -> MetadataClient<true> {
    MetadataClient::with_base_url(YRAL_METADATA_URL.clone())
        .with_jwt_token(conf.yral_metadata_token.clone())
}

pub fn init_agent() -> Agent {
    let pk = env::var("RECLAIM_CANISTER_PEM").expect("$RECLAIM_CANISTER_PEM is not set");

    let identity = match ic_agent::identity::BasicIdentity::from_pem(
        stringreader::StringReader::new(pk.as_str()),
    ) {
        Ok(identity) => identity,
        Err(err) => {
            panic!("Unable to create identity, error: {:?}", err);
        }
    };

    let agent = match Agent::builder()
        .with_url("https://a4gq6-oaaaa-aaaab-qaa4q-cai.raw.ic0.app") // https://a4gq6-oaaaa-aaaab-qaa4q-cai.raw.ic0.app/
        .with_identity(identity)
        .build()
    {
        Ok(agent) => agent,
        Err(err) => {
            panic!("Unable to create agent, error: {:?}", err);
        }
    };

    agent
}

impl AppState {
    pub async fn get_individual_canister_by_user_principal(
        &self,
        user_principal: Principal,
    ) -> Result<Principal> {
        let meta = self
            .yral_metadata_client
            .get_user_metadata(user_principal)
            .await
            .context("Failed to get user_metadata from yral_metadata_client")?;

        match meta {
            Some(meta) => Ok(meta.user_canister_id),
            None => Err(anyhow!(
                "user metadata does not exist in yral_metadata_service"
            )),
        }
    }

    pub fn individual_user(&self, user_canister: Principal) -> IndividualUserTemplate<'_> {
        IndividualUserTemplate(user_canister, &self.agent)
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    let conf = AppConfig::load()?;

    Builder::new()
        .filter_level(LevelFilter::Info)
        .target(Target::Stdout)
        .init();

    let shared_state = Arc::new(AppState {
        yral_metadata_client: init_yral_metadata_client(&conf),
        agent: init_agent(),
    });

    // build our application with a route
    let http = Router::new()
        .route("/", get(hello_work_handler))
        .route("/healthz", get(health_handler))
        .route("/start_backup", get(backup_job_handler))
        .route("/canisters_list", get(canisters_list_handler))
        .route("/cf_webhook", post(cf_stream_webhook_handler))
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

use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct WebhookPayload {
    uid: String,
    status: Status,
    meta: Meta,
}

#[derive(Deserialize, Debug)]
struct Status {
    state: String,
}

#[derive(Deserialize, Debug)]
struct Meta {
    creator: String,
    #[serde(rename = "fileName")]
    file_name: String,
    post_id: String,
}

// #[debug_handler]
async fn cf_stream_webhook_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<Value>,
) -> Result<(), AppError> {
    let yral_metadata_client = state.yral_metadata_client.clone();

    let payload: WebhookPayload = serde_json::from_value(payload).unwrap();

    let post_id: u64 = payload
        .meta
        .post_id
        .parse()
        .context("Failed to get user_metadata from yral_metadata_client")?;

    if let Ok(user_principal) = payload.meta.creator.parse::<Principal>() {
        let meta = yral_metadata_client
            .get_user_metadata(user_principal)
            .await
            .context("yral_metadata - could not connect to client")?
            .context("yral_metadata has value None")?;

        let user_canister_id = state
            .get_individual_canister_by_user_principal(meta.user_canister_id)
            .await
            .context("Failed to get user_canister_id")?;

        let user = state.individual_user(user_canister_id);
        let _ = user
            .update_post_as_ready_to_view(post_id)
            .await
            .context("Failed to update post status")?;
        println!("payload {meta:?}");
    }

    Ok(())
}
