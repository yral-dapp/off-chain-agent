use std::{collections::HashMap, env};

use crate::auth::AuthBearer;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use ic_agent::Agent;
use serde::Serialize;

use self::utils::get_canisters_list_all;
#[allow(clippy::all)]
mod generated;

pub mod reclaim_canisters;
pub mod snapshot;
pub mod utils;
pub use generated::*;
// pub mod canisters;

#[derive(Serialize)]
pub struct CanisterListResponse {
    targets: Vec<String>,
    labels: HashMap<String, String>,
}

pub async fn canisters_list_handler(AuthBearer(token): AuthBearer) -> Response {
    if token
    != *"Pm0SgTL2RGVomuwyAq6e6ieBEHxhXYyMviZthjfpbRImSKE7bYQZviaijwWlP3SlF2zJMaBXs1MeVgQg7cT5opqqsCKUDqg0GJsjOvJnCXg9zFIMFfFnxv2ZCuS8ospf"
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let pk = env::var("RECLAIM_CANISTER_PEM").expect("$RECLAIM_CANISTER_PEM is not set");

    let identity = match ic_agent::identity::BasicIdentity::from_pem(
        stringreader::StringReader::new(pk.as_str()),
    ) {
        Ok(identity) => identity,
        Err(err) => {
            println!("Unable to create identity, error: {:?}", err);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let agent = match Agent::builder()
        .with_url("https://a4gq6-oaaaa-aaaab-qaa4q-cai.raw.ic0.app") // https://a4gq6-oaaaa-aaaab-qaa4q-cai.raw.ic0.app/
        .with_identity(identity)
        .build()
    {
        Ok(agent) => agent,
        Err(err) => {
            println!("Unable to create agent, error: {:?}", err);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    // ‼️‼️comment below line in mainnet‼️‼️
    // agent.fetch_root_key().await.unwrap();

    let canister_ids_list = match get_canisters_list_all(&agent).await {
        Ok(canister_ids_list) => canister_ids_list,
        Err(err) => {
            println!("Unable to get canister list, error: {:?}", err);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let canisters_list = canister_ids_list
        .iter()
        .map(|x| format!("{}.raw.icp0.io", x))
        .collect::<Vec<String>>();

    // let canisters_list = vec![
    //     "bd3sg-teaaa-aaaaa-qaaba-cai.localhost:4943".to_string(),
    //     "avqkn-guaaa-aaaaa-qaaea-cai.localhost:4943".to_string(),
    //     "asrmz-lmaaa-aaaaa-qaaeq-cai.localhost:4943".to_string(),
    // ];

    Json(vec![CanisterListResponse {
        targets: canisters_list,
        labels: HashMap::new(),
    }])
    .into_response()
}
