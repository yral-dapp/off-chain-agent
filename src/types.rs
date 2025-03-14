use candid::CandidType;
use ic_agent::identity::SignedDelegation;
use k256::elliptic_curve::JwkEcKey;
use serde::{Deserialize, Serialize};
use utoipa::{schema, ToSchema};

#[derive(Serialize, Deserialize, Clone, Copy, CandidType, Debug, PartialEq)]
pub enum SessionType {
    AnonymousSession,
    RegisteredSession,
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
#[schema(examples(json!({
    "from_key": [0, 1, 2, 3, 4, 5],
    "to_secret": {
        "kty": "EC",
        "crv": "secp256k1",
        "x": "example_x_coordinate_base64",
        "y": "example_y_coordinate_base64",
        "d": "example_private_key_base64"
    },
    "delegation_chain": [
        {
            "delegation": {
                "pubkey": [0, 1, 2, 3, 4, 5],
                "expiration": 1234567890,
                "targets": null
            },
            "signature": [0, 1, 2, 3, 4, 5]
        }
    ]
})))]
pub struct DelegatedIdentityWire {
    pub from_key: Vec<u8>,
    #[schema(value_type = Object)]
    pub to_secret: JwkEcKey,
    #[schema(value_type = Object)]
    pub delegation_chain: Vec<SignedDelegation>,
}
