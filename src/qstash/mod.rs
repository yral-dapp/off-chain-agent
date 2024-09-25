mod verify;

use std::{sync::Arc, time::Duration};

use axum::{
    extract::State,
    middleware::{self},
    response::Response,
    routing::post,
    Json, Router,
};
use candid::Nat;
use http::StatusCode;
use ic_agent::{identity::DelegatedIdentity, Identity};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use tower::ServiceBuilder;
use verify::verify_qstash_message;
use yral_canisters_client::{
    sns_governance::{
        Account, Amount, Command, Command1, Disburse, DissolveState, ListNeurons, ManageNeuron,
    },
    sns_ledger::{Account as LedgerAccount, TransferArg, TransferResult},
    sns_root::ListSnsCanistersArg,
};
use yral_qstash_types::ClaimTokensRequest;

use crate::app_state::AppState;

#[derive(Clone)]
pub struct QStashState {
    decoding_key: Arc<DecodingKey>,
    validation: Arc<Validation>,
}

impl QStashState {
    pub fn init(verification_key: String) -> Self {
        let decoding_key = DecodingKey::from_secret(verification_key.as_bytes());
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_issuer(&["Upstash"]);
        validation.set_audience(&[""]);
        Self {
            decoding_key: Arc::new(decoding_key),
            validation: Arc::new(validation),
        }
    }
}

async fn claim_tokens_from_first_neuron(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ClaimTokensRequest>,
) -> Result<Response, StatusCode> {
    let root_canister = state.sns_root(req.token_root);
    let sns_cans = root_canister
        .list_sns_canisters(ListSnsCanistersArg {})
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let Some(governance_principal) = sns_cans.governance else {
        return Err(StatusCode::BAD_REQUEST);
    };
    let Some(ledger_principal) = sns_cans.ledger else {
        return Err(StatusCode::BAD_REQUEST);
    };
    let identity: DelegatedIdentity = req
        .identity
        .try_into()
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let user_principal = identity
        .sender()
        .expect("Delegated identity without principal?!");

    let governance = state.sns_governance(governance_principal);
    let neurons = governance
        .list_neurons(ListNeurons {
            of_principal: Some(user_principal),
            limit: 10,
            start_page_at: None,
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .neurons;

    if neurons.len() < 2 || neurons[1].cached_neuron_stake_e8s == 0 {
        let res = Response::builder()
            .status(StatusCode::OK)
            .body("Claiming not required".into())
            .unwrap();
        return Ok(res);
    }
    let ix = if matches!(
        neurons[1].dissolve_state.as_ref(),
        Some(DissolveState::DissolveDelaySeconds(0))
    ) {
        1
    } else {
        0
    };

    let amount = neurons[ix].cached_neuron_stake_e8s;
    if amount == 0 {
        let res = Response::builder()
            .status(StatusCode::OK)
            .body("Claiming not required".into())
            .unwrap();
        return Ok(res);
    }
    let neuron_id = &neurons[ix].id.as_ref().ok_or(StatusCode::BAD_REQUEST)?.id;

    let mut tries = 0;
    loop {
        if tries > 10 {
            return Err(StatusCode::LOOP_DETECTED);
        }
        tries += 1;

        let manage_neuron_arg = ManageNeuron {
            subaccount: neuron_id.clone(),
            command: Some(Command::Disburse(Disburse {
                to_account: Some(Account {
                    owner: Some(user_principal),
                    subaccount: None,
                }),
                amount: Some(Amount { e8s: amount }),
            })),
        };
        let manage_neuron = governance
            .manage_neuron(manage_neuron_arg)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        match manage_neuron.command {
            Some(Command1::Disburse(_)) => break,
            Some(Command1::Error(e)) => {
                if e.error_message.contains("PreInitializationSwap") {
                    log::debug!("Governance {governance_principal} is not ready. Retrying...");
                    tokio::time::sleep(Duration::from_secs(8)).await;
                    continue;
                }
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
            _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
        }
    }

    // Transfer to canister
    let user_canister = req.user_canister;
    let ledger = state.sns_ledger(ledger_principal);
    // User has 50% of the overall amount
    // 20% of this 50% is 10% of the overall amount
    // 10% of the overall amount is reserveed for the canister
    let distribution_amt = Nat::from(amount) * 20u32 / 100u32;
    let transfer_resp = ledger
        .icrc_1_transfer(TransferArg {
            to: LedgerAccount {
                owner: user_canister,
                subaccount: None,
            },
            fee: None,
            memo: None,
            from_subaccount: None,
            amount: distribution_amt,
            created_at_time: None,
        })
        .await;

    match transfer_resp {
        Ok(TransferResult::Err(e)) => {
            log::error!("Token is in invalid state, user_canister: {user_canister}, governance: {governance_principal}, irrecoverable {e:?}");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
        Err(e) => {
            log::error!("Token is in invalid state, user_canister: {user_canister}, governance: {governance_principal}, irrecoverable {e}");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
        _ => (),
    }

    let res = Response::builder()
        .status(StatusCode::OK)
        .body("Tokens claimed".into())
        .unwrap();

    Ok(res)
}

pub fn qstash_router<S>(app_state: Arc<AppState>) -> Router<S> {
    Router::new()
        .route("/claim_tokens", post(claim_tokens_from_first_neuron))
        .layer(ServiceBuilder::new().layer(middleware::from_fn_with_state(
            app_state.qstash.clone(),
            verify_qstash_message,
        )))
        .with_state(app_state)
}
