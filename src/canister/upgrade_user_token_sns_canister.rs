use axum::{
    extract::{Path, State},
    Json,
};
use candid::Principal;
use futures::{stream::FuturesUnordered, TryStreamExt};
use google_cloud_bigquery::storage::array::Array;
use hex::ToHex;
use ic_agent::Agent;
use k256::elliptic_curve::rand_core::le;
use serde::{Deserialize, Serialize};
use std::{error::Error, sync::Arc, vec};
use yral_canisters_client::{
    individual_user_template::{DeployedCdaoCanisters, IndividualUserTemplate},
    platform_orchestrator::{self, PlatformOrchestrator},
    sns_governance::{
        self, Action, Command1, Configure, Follow, GetProposal, GetRunningSnsVersionArg,
        IncreaseDissolveDelay, ListNeurons, ManageNeuron, NeuronId, Operation, Proposal,
        ProposalId, SnsGovernance,
    },
    user_index::UserIndex,
};

use crate::{consts::PLATFORM_ORCHESTRATOR_ID, qstash::client::QStashClient};

use crate::app_state::AppState;
use crate::utils::api_response::ApiResponse;

pub const SNS_TOKEN_GOVERNANCE_MODULE_HASH: &'static str =
    "bc91fd7bc4d6c01ea814b12510a1ff8f4f74fcac9ab16248ad4af7cb98d9c69d";
pub const SNS_TOKEN_LEDGER_MODULE_HASH: &'static str =
    "3d808fa63a3d8ebd4510c0400aa078e99a31afaa0515f0b68778f929ce4b2a46";
pub const SNS_TOKEN_ROOT_MODULE_HASH: &'static str =
    "431cb333feb3f762f742b0dea58745633a2a2ca41075e9933183d850b4ddb259";
pub const SNS_TOKEN_SWAP_MODULE_HASH: &'static str =
    "8313ac22d2ef0a0c1290a85b47f235cfa24ca2c96d095b8dbed5502483b9cd18";
pub const SNS_TOKEN_INDEX_MODULE_HASH: &'static str =
    "67b5f0bf128e801adf4a959ea26c3c9ca0cd399940e169a26a2eb237899a94dd";
pub const SNS_TOKEN_ARCHIVE_MODULE_HASH: &'static str =
    "317771544f0e828a60ad6efc97694c425c169c4d75d911ba592546912dba3116";

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub struct VerifyUpgradeProposalRequest {
    pub sns_canisters: SnsCanisters,
    pub proposal_id: u64,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub struct SnsCanisters {
    pub governance: Principal,
    pub index: Principal,
    pub swap: Principal,
    pub root: Principal,
    pub ledger: Principal,
}

impl From<DeployedCdaoCanisters> for SnsCanisters {
    fn from(value: DeployedCdaoCanisters) -> Self {
        Self {
            governance: value.governance,
            index: value.index,
            swap: value.swap,
            root: value.root,
            ledger: value.ledger,
        }
    }
}

pub async fn upgrade_user_token_sns_canister_for_entire_network(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<()>> {
    Json(ApiResponse::from(
        upgrade_user_token_sns_canister_for_entire_network_impl(&state.agent, &state.qstash_client)
            .await,
    ))
}

async fn upgrade_user_token_sns_canister_for_entire_network_impl(
    agent: &Agent,
    qstash_client: &QStashClient,
) -> Result<(), Box<dyn Error>> {
    let platform_orchestrator = Principal::from_text(PLATFORM_ORCHESTRATOR_ID).unwrap();
    let mut individual_canister_ids: Vec<Principal> = vec![];

    let platform_orchestrator = PlatformOrchestrator(platform_orchestrator, agent);

    let subnet_orchestrators = platform_orchestrator.get_all_subnet_orchestrators().await?;

    for subnet_orchestrator in subnet_orchestrators {
        let subnet_orchestrator = UserIndex(subnet_orchestrator, agent);
        individual_canister_ids.extend(subnet_orchestrator.get_user_canister_list().await?);
    }

    for individual_canister in individual_canister_ids {
        qstash_client
            .upgrade_all_sns_canisters_for_a_user_canister(individual_canister.to_text())
            .await?
    }

    Ok(())
}

pub async fn upgrade_user_token_sns_canister_handler(
    Path(user_canister_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<()>> {
    let setup_for_upgrade_result = setup_sns_canisters_of_a_user_canister_for_upgrade(
        &state.agent,
        &state.qstash_client,
        user_canister_id,
    )
    .await;

    Json(ApiResponse::from(setup_for_upgrade_result))
}

pub async fn setup_sns_canisters_of_a_user_canister_for_upgrade(
    agent: &Agent,
    qstash_client: &QStashClient,
    individual_canister_id: String,
) -> Result<(), Box<dyn Error>> {
    let individual_canister_principal =
        Principal::from_text(individual_canister_id).map_err(|e| e.to_string())?;

    let individual_user_template = IndividualUserTemplate(individual_canister_principal, agent);

    let deployed_canisters = individual_user_template
        .deployed_cdao_canisters()
        .await
        .map_err(|e| e.to_string())?;

    let sns_canisters: Vec<SnsCanisters> = deployed_canisters
        .into_iter()
        .map(|d| SnsCanisters::from(d))
        .collect();

    sns_canisters
        .into_iter()
        .map(|sns_canisters| async move {
            recharge_canisters(agent, sns_canisters).await?;
            setup_neurons_for_admin_principal(agent, sns_canisters.governance).await?;
            qstash_client
                .upgrade_sns_creator_dao_canister(sns_canisters)
                .await
                .map_err(|e| <anyhow::Error as Into<Box<dyn Error>>>::into(e))?;

            Ok::<(), Box<dyn Error>>(())
        })
        .collect::<FuturesUnordered<_>>()
        .try_collect::<()>()
        .await?;

    Ok(())
}

async fn setup_neurons_for_admin_principal(
    agent: &Agent,
    governance_canister_id: Principal,
) -> Result<(), Box<dyn Error>> {
    let sns_governance = SnsGovernance(governance_canister_id, agent);

    let neuron_list = sns_governance
        .list_neurons(ListNeurons {
            of_principal: Some(agent.get_principal().unwrap()),
            limit: 10,
            start_page_at: None,
        })
        .await
        .map_err(|e| e.to_string())?
        .neurons;

    let first_neuron = neuron_list
        .get(0)
        .ok_or("first neuron not found")?
        .id
        .as_ref()
        .ok_or("first neuronId not found")?;

    let second_neuron = neuron_list
        .get(1)
        .ok_or("second neuron not found")?
        .id
        .as_ref()
        .ok_or("second neuronId not found")?;

    let _set_dissolve_delay = sns_governance
        .manage_neuron(ManageNeuron {
            subaccount: first_neuron.id.clone(),
            command: Some(sns_governance::Command::Configure(Configure {
                operation: Some(Operation::IncreaseDissolveDelay(IncreaseDissolveDelay {
                    additional_dissolve_delay_seconds: 172800,
                })),
            })),
        })
        .await
        .map_err(|e| format!("{:?}", e))?;

    let _set_dissolve_delay = sns_governance
        .manage_neuron(ManageNeuron {
            subaccount: second_neuron.id.clone(),
            command: Some(sns_governance::Command::Configure(Configure {
                operation: Some(Operation::IncreaseDissolveDelay(IncreaseDissolveDelay {
                    additional_dissolve_delay_seconds: 172800,
                })),
            })),
        })
        .await
        .map_err(|e| format!("{:?}", e))?;

    let function_id_for_upgrading_sns_to_next_version = sns_governance
        .list_nervous_system_functions()
        .await
        .map_err(|e| e.to_string())?
        .functions
        .iter()
        .find(|function| function.name.contains("Upgrade SNS to next version"))
        .map(|function| function.id)
        .ok_or("function id for upgrade sns to next version not found")?;

    let _second_neuron_follow_first_neuron_result = sns_governance
        .manage_neuron(ManageNeuron {
            subaccount: second_neuron.id.clone(),
            command: Some(sns_governance::Command::Follow(Follow {
                function_id: function_id_for_upgrading_sns_to_next_version,
                followees: vec![NeuronId {
                    id: first_neuron.id.clone(),
                }],
            })),
        })
        .await
        .map_err(|e| e.to_string())?;

    Ok(())
}

async fn recharge_canister_using_platform_orchestrator(
    platform_orchestrator: &PlatformOrchestrator<'_>,
    canister_id: Principal,
) -> Result<(), Box<dyn Error>> {
    const RECHARGE_AMOUNT: u128 = 100_000_000_000; //0.1T cycles
    platform_orchestrator
        .deposit_cycles_to_canister(
            canister_id,
            candid::Nat::from(10_000_000_000_u128), //10B
        )
        .await
        .map_err(|e| e.to_string())?;

    Ok(())
}

pub async fn is_upgrade_required(
    sns_governance: &SnsGovernance<'_>,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    let deployed_version = sns_governance
        .get_running_sns_version(GetRunningSnsVersionArg {})
        .await?;

    let deployed_version = deployed_version
        .deployed_version
        .ok_or("deployed version not found")?;

    let governance_hash = deployed_version
        .governance_wasm_hash
        .to_vec()
        .encode_hex::<String>();

    let index_hash = deployed_version
        .index_wasm_hash
        .to_vec()
        .encode_hex::<String>();

    let swap_hash = deployed_version
        .swap_wasm_hash
        .to_vec()
        .encode_hex::<String>();

    let ledger_hash = deployed_version
        .ledger_wasm_hash
        .to_vec()
        .encode_hex::<String>();

    let root_hash = deployed_version
        .root_wasm_hash
        .to_vec()
        .encode_hex::<String>();

    let archive_hash = deployed_version
        .archive_wasm_hash
        .to_vec()
        .encode_hex::<String>();

    let hashes = vec![
        governance_hash,
        index_hash,
        swap_hash,
        ledger_hash,
        root_hash,
        archive_hash,
    ];

    let final_hashes = vec![
        SNS_TOKEN_ARCHIVE_MODULE_HASH.to_owned(),
        SNS_TOKEN_GOVERNANCE_MODULE_HASH.to_owned(),
        SNS_TOKEN_INDEX_MODULE_HASH.to_owned(),
        SNS_TOKEN_LEDGER_MODULE_HASH.to_owned(),
        SNS_TOKEN_ROOT_MODULE_HASH.to_owned(),
        SNS_TOKEN_SWAP_MODULE_HASH.to_owned(),
    ];

    let result = hashes.iter().all(|val| final_hashes.contains(val));

    Ok(result)
}

pub async fn check_if_the_proposal_executed_successfully(
    sns_governance: &SnsGovernance<'_>,
    proposal_id: u64,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    let proposal_result = sns_governance
        .get_proposal(GetProposal {
            proposal_id: Some(ProposalId { id: proposal_id }),
        })
        .await
        .map_err(|e| e.to_string())?;

    if let Some(proposal_result) = proposal_result.result {
        match proposal_result {
            sns_governance::Result1::Proposal(res) => Ok(res.executed_timestamp_seconds != 0),
            sns_governance::Result1::Error(e) => Err(e.error_message.into()),
        }
    } else {
        return Err("Proposal not found".to_owned().into());
    }
}

pub async fn recharge_canisters(
    agent: &Agent,
    deployed_canisters: SnsCanisters,
) -> Result<(), Box<dyn Error>> {
    let platform_orchestrator_canister_principal =
        Principal::from_text(PLATFORM_ORCHESTRATOR_ID).unwrap();

    let platform_orchestrator =
        PlatformOrchestrator(platform_orchestrator_canister_principal, agent);

    let mut recharge_canister_tasks = vec![];

    recharge_canister_tasks.push(recharge_canister_using_platform_orchestrator(
        &platform_orchestrator,
        deployed_canisters.governance,
    ));

    recharge_canister_tasks.push(recharge_canister_using_platform_orchestrator(
        &platform_orchestrator,
        deployed_canisters.index,
    ));
    recharge_canister_tasks.push(recharge_canister_using_platform_orchestrator(
        &platform_orchestrator,
        deployed_canisters.ledger,
    ));
    recharge_canister_tasks.push(recharge_canister_using_platform_orchestrator(
        &platform_orchestrator,
        deployed_canisters.root,
    ));
    recharge_canister_tasks.push(recharge_canister_using_platform_orchestrator(
        &platform_orchestrator,
        deployed_canisters.swap,
    ));

    recharge_canister_tasks
        .into_iter()
        .collect::<FuturesUnordered<_>>()
        .try_collect::<()>()
        .await?;

    Ok(())
}

pub async fn upgrade_user_token_sns_canister_impl(
    agent: &Agent,
    governance_canister_id: String,
) -> Result<u64, Box<dyn Error + Send + Sync>> {
    let governance_canister_id_principal = Principal::from_text(governance_canister_id)?;

    let sns_governance = SnsGovernance(governance_canister_id_principal, agent);

    let neuron_list = sns_governance
        .list_neurons(ListNeurons {
            of_principal: Some(agent.get_principal().unwrap()),
            limit: 10,
            start_page_at: None,
        })
        .await
        .map_err(|e| e.to_string())?
        .neurons;

    let first_neuron = neuron_list
        .get(0)
        .ok_or("first neuron not found")?
        .id
        .as_ref()
        .ok_or("first neuronId not found")?;

    let proposal_id = sns_governance
        .manage_neuron(ManageNeuron {
            subaccount: first_neuron.id.clone(),
            command: Some(sns_governance::Command::MakeProposal(Proposal {
                url: "yral.com".to_owned(),
                title: "Upgrade SNS for token".into(),
                action: Some(Action::UpgradeSnsToNextVersion {}),
                summary: "Upgrading canisters".to_owned(),
            })),
        })
        .await?
        .command
        .unwrap();

    if let Command1::MakeProposal(proposal_id) = proposal_id {
        let proposal_id_u64 = proposal_id.proposal_id.ok_or("proposal id not found")?.id;

        Ok(proposal_id_u64)
    } else {
        Err(format!("{:?}", proposal_id).into())
    }
}
