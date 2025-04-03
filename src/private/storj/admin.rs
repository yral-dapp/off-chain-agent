use crate::consts::PLATFORM_ORCHESTRATOR_ID;
use ic_agent::{export::Principal, Agent};
use yral_canisters_client::{
    self, individual_user_template::IndividualUserTemplate,
    platform_orchestrator::PlatformOrchestrator, user_index::UserIndex,
};

#[derive(Clone)]
pub struct AdminCanisters {
    agent: Agent,
}

impl AdminCanisters {
    pub fn new(agent: Agent) -> Self {
        Self { agent }
    }

    pub async fn platform_orchestrator(&self) -> PlatformOrchestrator<'_> {
        PlatformOrchestrator(
            PLATFORM_ORCHESTRATOR_ID
                .parse()
                .expect("to be valid principal"),
            self.get_agent().await,
        )
    }

    #[inline]
    async fn get_agent(&self) -> &ic_agent::Agent {
        &self.agent
    }

    pub async fn user_index_with(&self, idx_principal: Principal) -> UserIndex<'_> {
        UserIndex(idx_principal, self.get_agent().await)
    }

    pub async fn individual_user_for(
        &self,
        user_canister: Principal,
    ) -> IndividualUserTemplate<'_> {
        IndividualUserTemplate(user_canister, self.get_agent().await)
    }
}
