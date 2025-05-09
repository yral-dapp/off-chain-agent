use ic_agent::{identity::DelegatedIdentity, Agent};

use crate::types::DelegatedIdentityWire;

pub async fn get_agent_from_delegated_identity_wire(
    identity_wire: &DelegatedIdentityWire,
) -> Result<Agent, anyhow::Error> {
    let identity: DelegatedIdentity = DelegatedIdentity::try_from(identity_wire.clone())
        .map_err(|_| anyhow::anyhow!("Failed to create delegated identity"))?;
    let agent = Agent::builder()
        .with_identity(identity)
        .with_url("https://ic0.app")
        .build()?;

    Ok(agent)
}
