use axum::{
    extract::{Path, State},
    Json,
};
use candid::Principal;
use futures::{stream::FuturesUnordered, TryStreamExt};
use ic_agent::Agent;
use serde::{Deserialize, Serialize};
use std::{error::Error, sync::Arc, time::Duration, vec};
use yral_canisters_client::{
    individual_user_template::{DeployedCdaoCanisters, IndividualUserTemplate, Ok},
    platform_orchestrator::PlatformOrchestrator,
    sns_governance::{
        self, Action, Command1, Configure, Follow, GetProposal, IncreaseDissolveDelay, ListNeurons,
        ManageNeuron, NeuronId, Operation, Proposal, ProposalId, SnsGovernance,
    },
};

use crate::{consts::PLATFORM_ORCHESTRATOR_ID, qstash::client::QStashClient};

use crate::app_state::AppState;
use crate::utils::api_response::ApiResponse;

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

async fn setup_sns_canisters_of_a_user_canister_for_upgrade(
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

async fn check_if_the_proposal_executed_successfully_with_retries(
    sns_governance: &SnsGovernance<'_>,
    proposal_id: u64,
    max_retries: u64,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut retry_attempt = 0_u64;

    while retry_attempt < max_retries {
        let res = check_if_the_proposal_executed_successfully(sns_governance, proposal_id).await;
        if let Err(e) = res {
            return Err(e);
        } else if let Ok(executed) = res {
            if executed {
                return Ok(());
            } else {
                retry_attempt += 1;
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }

    Err(format!("failed after retrying for {} times", max_retries).into())
}

async fn check_if_the_proposal_executed_successfully(
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

        check_if_the_proposal_executed_successfully_with_retries(
            &sns_governance,
            proposal_id_u64,
            3,
        )
        .await?;

        Ok(proposal_id_u64)
    } else {
        Err(format!("{:?}", proposal_id).into())
    }
}
