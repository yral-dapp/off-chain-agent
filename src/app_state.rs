use crate::consts::YRAL_METADATA_URL;
use crate::{canister::individual_user_template::IndividualUserTemplate, config::AppConfig};
use anyhow::{anyhow, Context, Result};
use candid::Principal;
use ic_agent::Agent;
use std::env;
use yral_metadata_client::MetadataClient;

pub type RedisPool = bb8::Pool<bb8_redis::RedisConnectionManager>;

#[derive(Clone)]
pub struct AppState {
    pub agent: ic_agent::Agent,
    pub redis: RedisPool,
    pub yral_metadata_client: MetadataClient<true>,
}

impl AppState {
    pub async fn new(app_config: AppConfig) -> Self {
        AppState {
            yral_metadata_client: init_yral_metadata_client(&app_config),
            agent: init_agent(),
            redis: init_redis(&app_config).await,
        }
    }

    pub async fn get_individual_canister_by_user_principal(
        &self,
        user_principal: Principal,
    ) -> Result<Principal> {
        let meta = self
            .yral_metadata_client
            .get_user_metadata(user_principal)
            .await
            .context("Failed to get user_metadata from yral_metadata_client")?;

        match meta {
            Some(meta) => Ok(meta.user_canister_id),
            None => Err(anyhow!(
                "user metadata does not exist in yral_metadata_service"
            )),
        }
    }

    pub fn individual_user(&self, user_canister: Principal) -> IndividualUserTemplate<'_> {
        IndividualUserTemplate(user_canister, &self.agent)
    }
}

pub async fn init_redis(conf: &AppConfig) -> RedisPool {
    let manager = bb8_redis::RedisConnectionManager::new(conf.redis_url.clone())
        .expect("failed to open connection to redis");
    RedisPool::builder().build(manager).await.unwrap()
}

pub fn init_yral_metadata_client(conf: &AppConfig) -> MetadataClient<true> {
    MetadataClient::with_base_url(YRAL_METADATA_URL.clone())
        .with_jwt_token(conf.yral_metadata_token.clone())
}

pub fn init_agent() -> Agent {
    let pk = env::var("RECLAIM_CANISTER_PEM").expect("$RECLAIM_CANISTER_PEM is not set");

    let identity = match ic_agent::identity::BasicIdentity::from_pem(
        stringreader::StringReader::new(pk.as_str()),
    ) {
        Ok(identity) => identity,
        Err(err) => {
            panic!("Unable to create identity, error: {:?}", err);
        }
    };

    let agent = match Agent::builder()
        .with_url("https://a4gq6-oaaaa-aaaab-qaa4q-cai.raw.ic0.app") // https://a4gq6-oaaaa-aaaab-qaa4q-cai.raw.ic0.app/
        .with_identity(identity)
        .build()
    {
        Ok(agent) => agent,
        Err(err) => {
            panic!("Unable to create agent, error: {:?}", err);
        }
    };

    agent
}
