use crate::AppState;
use axum::{extract::State, Json};
use candid::{decode_args, encode_args, Principal};
use http::StatusCode;
use ic_agent::Agent;
use serde::{Deserialize, Serialize};
use std::{env, str::FromStr, sync::Arc};
use yral_identity::Signature;
use yral_metadata_types::UserMetadata;

pub async fn transfer_hotornot_token_and_post_to_yral(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<MigrateRequest>,
) -> (StatusCode, &'static str) {
    let from_account = match Principal::from_str(&payload.from_account) {
        Ok(principal) => principal,
        Err(error) => {
            log::error!("Invalid from_account: {:?}", error);
            return (StatusCode::BAD_REQUEST, "Invalid from_account");
        }
    };
    let to_account = match Principal::from_str(&payload.to_account) {
        Ok(principal) => principal,
        Err(error) => {
            log::error!("Invalid to_account: {:?}", error);
            return (StatusCode::BAD_REQUEST, "Invalid to_account");
        }
    };
    let user_metadata = UserMetadata {
        user_canister_id: from_account,
        user_name: "".to_owned(),
    };
    match payload
        .signature
        .verify_identity(from_account, user_metadata.into())
    {
        Ok(_) => {}
        Err(error) => {
            log::error!("Unauthorized: {:?}", error);
            return (StatusCode::UNAUTHORIZED, "Unauthorized");
        }
    }

    let (from_account_canister_id, to_account_canister_id) = tokio::join!(
        get_canister_id_from_metadata(state.clone(), from_account),
        get_canister_id_from_metadata(state.clone(), to_account)
    );

    if let Err(error) = from_account_canister_id {
        log::error!("canister id not found: {:?}", error);
        return (StatusCode::BAD_REQUEST, "canister id not found");
    }
    if let Err(error) = to_account_canister_id {
        log::error!("canister id not found: {:?}", error);
        return (StatusCode::BAD_REQUEST, "canister id not found");
    }

    let pk = env::var("RECLAIM_CANISTER_PEM").expect("$RECLAIM_CANISTER_PEM is not set");

    let identity = match ic_agent::identity::BasicIdentity::from_pem(
        stringreader::StringReader::new(pk.as_str()),
    ) {
        Ok(identity) => identity,
        Err(err) => {
            println!("Unable to create identity, error: {:?}", err);
            return (StatusCode::BAD_REQUEST, "Unable to create identity");
        }
    };
    let agent = match Agent::builder()
        .with_url("https://a4gq6-oaaaa-aaaab-qaa4q-cai.raw.ic0.app/") // local : http://127.0.0.1:4943
        .with_identity(identity)
        .build()
    {
        Ok(agent) => agent,
        Err(err) => {
            println!("Unable to create agent, error: {:?}", err);
            return (StatusCode::BAD_REQUEST, "Unable to create agent");
        }
    };
    // ‼️‼️comment below line in mainnet‼️‼️
    // agent.fetch_root_key().await.unwrap();

    match transfer_tokens_and_posts(
        &agent,
        from_account_canister_id.unwrap(),
        to_account,
        to_account_canister_id.unwrap(),
    )
    .await
    {
        Ok(_) => (StatusCode::OK, "Hot Or Not transfer - OK"),
        Err(error) => {
            log::error!("{:?}", error);
            (StatusCode::BAD_REQUEST, "Hot Or Not transfer - Failed")
        }
    }
}

async fn get_canister_id_from_metadata(
    state: Arc<AppState>,
    user_principal_id: Principal,
) -> Result<Principal, String> {
    let yral_metadata_client = state.yral_metadata_client.clone();
    match yral_metadata_client
        .get_user_metadata(user_principal_id)
        .await
    {
        Ok(Some(to_account_metadata)) => Ok(to_account_metadata.user_canister_id),
        Ok(None) => {
            log::error!("Error calling get_user_metadata, error: No canister id found");
            Err("Error calling the method get_user_metadata".to_owned())
        }
        Err(err) => {
            log::error!("Error calling get_user_metadata, error: {:?}", err);
            Err("Error calling the method get_user_metadata".to_owned())
        }
    }
}

async fn transfer_tokens_and_posts(
    agent: &Agent,
    from_account_canister_id: Principal,
    to_account: Principal,
    to_account_canister_id: Principal,
) -> Result<(String,), String> {
    let result = match agent
        .update(&from_account_canister_id, "transfer_tokens_and_posts")
        .with_arg(encode_args((to_account, to_account_canister_id)).unwrap())
        .call_and_wait()
        .await
    {
        Ok(response) => {
            decode_args::<(String,)>(response.as_slice()).map_err(|error| format!("{:?}", error))
        }
        Err(err) => {
            log::error!("Unable to call the method, error: {:?}", err);
            return Err(format!("{:?}", err));
        }
    };
    result
}

#[derive(Serialize, Deserialize)]
pub struct MigrateRequest {
    signature: Signature,
    from_account: String,
    to_account: String,
}
