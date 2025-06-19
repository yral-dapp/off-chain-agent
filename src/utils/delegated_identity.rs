use candid::Principal;
use ic_agent::{identity::DelegatedIdentity, Identity};

use crate::{app_state::AppState, types::DelegatedIdentityWire};

pub struct UserInfo {
    pub user_principal: Principal,
    pub user_canister: Principal,
}

pub async fn get_user_info_from_delegated_identity_wire(
    state: &AppState,
    delegated_identity_wire: DelegatedIdentityWire,
) -> Result<UserInfo, anyhow::Error> {
    let identity = DelegatedIdentity::try_from(delegated_identity_wire)
        .map_err(|e| anyhow::anyhow!("Failed to parse delegated identity wire: {}", e))?;
    let user_principal = identity
        .sender()
        .map_err(|e| anyhow::anyhow!("Failed to get user principal: {}", e))?;
    let user_canister = state
        .get_individual_canister_by_user_principal(user_principal)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get user canister: {}", e))?;

    Ok(UserInfo {
        user_principal,
        user_canister,
    })
}
