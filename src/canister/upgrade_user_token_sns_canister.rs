use axum::{
    extract::{Path, State},
    Json,
};
use candid::Principal;
use ic_agent::Agent;
use serde::{Deserialize, Serialize};
use std::{error::Error, sync::Arc};
use yral_canisters_client::sns_governance::{
    self, Action, Command1, ListNeurons, ManageNeuron, Proposal, SnsGovernance,
};

use crate::app_state::AppState;
use crate::utils::api_response::ApiResponse;

#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct ProposalId {
    id: u64,
}

impl From<Command1> for ApiResponse<ProposalId> {
    fn from(value: Command1) -> Self {
        ApiResponse {
            success: false,
            error: Some(format!("{:?}", value)),
            data: None,
        }
    }
}

pub async fn upgrade_user_token_sns_canister_handler(
    Path(governance_canister_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<ProposalId>> {
    let result = upgrade_user_token_sns_canister_impl(&state.agent, governance_canister_id).await;

    Json(ApiResponse::from(result))
}

async fn upgrade_user_token_sns_canister_impl(
    agent: &Agent,
    governance_canister_id: String,
) -> Result<ProposalId, Box<dyn Error>> {
    let governance_canister_id_princpal = Principal::from_text(governance_canister_id)?;

    let sns_governance = SnsGovernance(governance_canister_id_princpal, &agent);

    let neurons_list = sns_governance
        .list_neurons(ListNeurons {
            of_principal: Some(agent.get_principal().unwrap()),
            limit: 10,
            start_page_at: None,
        })
        .await?;

    let neuron_id = &neurons_list.neurons[0]
        .id
        .as_ref()
        .ok_or(String::from("neuron id not found"))?
        .id;
    let proposal_id = sns_governance
        .manage_neuron(ManageNeuron {
            subaccount: neuron_id.clone(),
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
        let id = proposal_id
            .proposal_id
            .ok_or(String::from("Proposal id not found"))?
            .id;

        Ok(ProposalId { id })
    } else {
        ApiResponse::from(proposal_id)
    }
}
