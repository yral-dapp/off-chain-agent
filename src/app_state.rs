use crate::config::AppConfig;
use crate::consts::{NSFW_SERVER_URL, YRAL_METADATA_URL};
use crate::metrics::{init_metrics, CfMetricTx};
use crate::qstash::client::QStashClient;
use crate::qstash::QStashState;
use anyhow::{anyhow, Context, Result};
use candid::Principal;
use firestore::{FirestoreDb, FirestoreDbOptions};
use google_cloud_bigquery::client::{Client, ClientConfig};
use hyper_util::client::legacy::connect::HttpConnector;
use ic_agent::Agent;
use std::env;
use std::sync::Arc;
use tonic::transport::{Channel, ClientTlsConfig};
use yral_canisters_client::individual_user_template::IndividualUserTemplate;
use yral_metadata_client::MetadataClient;
use yral_ml_feed_cache::MLFeedCacheState;
use yup_oauth2::hyper_rustls::HttpsConnector;
use yup_oauth2::{authenticator::Authenticator, ServiceAccountAuthenticator};

#[derive(Clone)]
pub struct AppState {
    pub agent: ic_agent::Agent,
    pub yral_metadata_client: MetadataClient<true>,
    #[cfg(not(feature = "local-bin"))]
    pub auth: Authenticator<HttpsConnector<HttpConnector>>,
    #[cfg(not(feature = "local-bin"))]
    pub firestoredb: FirestoreDb,
    pub qstash: QStashState,
    #[cfg(not(feature = "local-bin"))]
    pub bigquery_client: Client,
    pub nsfw_detect_channel: Channel,
    pub qstash_client: QStashClient,
    #[cfg(not(feature = "local-bin"))]
    pub gcs_client: Arc<cloud_storage::Client>,
    #[cfg(not(feature = "local-bin"))]
    pub ml_feed_cache: MLFeedCacheState,
    pub metrics: CfMetricTx,
}

impl AppState {
    pub async fn new(app_config: AppConfig) -> Self {
        AppState {
            yral_metadata_client: init_yral_metadata_client(&app_config),
            agent: init_agent().await,
            #[cfg(not(feature = "local-bin"))]
            auth: init_auth().await,
            // ml_server_grpc_channel: init_ml_server_grpc_channel().await,
            #[cfg(not(feature = "local-bin"))]
            firestoredb: init_firestoredb().await,
            qstash: init_qstash(),
            #[cfg(not(feature = "local-bin"))]
            bigquery_client: init_bigquery_client().await,
            nsfw_detect_channel: init_nsfw_detect_channel().await,
            qstash_client: init_qstash_client().await,
            #[cfg(not(feature = "local-bin"))]
            gcs_client: Arc::new(cloud_storage::Client::default()),
            #[cfg(not(feature = "local-bin"))]
            ml_feed_cache: MLFeedCacheState::new().await,
            metrics: init_metrics(),
        }
    }

    pub async fn get_access_token(&self, scopes: &[&str]) -> String {
        #[cfg(feature = "local-bin")]
        {
            "localtoken".into()
        }

        #[cfg(not(feature = "local-bin"))]
        {
            let auth = &self.auth;
            let token = auth.token(scopes).await.unwrap();

            match token.token() {
                Some(t) => t.to_string(),
                _ => panic!("No access token found"),
            }
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

pub fn init_yral_metadata_client(conf: &AppConfig) -> MetadataClient<true> {
    MetadataClient::with_base_url(YRAL_METADATA_URL.clone())
        .with_jwt_token(conf.yral_metadata_token.clone())
}

pub async fn init_agent() -> Agent {
    #[cfg(not(any(feature = "local-bin", feature = "use-local-agent")))]
    {
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
            .with_url("https://a4gq6-oaaaa-aaaab-qaa4q-cai.raw.ic0.app/") // https://a4gq6-oaaaa-aaaab-qaa4q-cai.raw.ic0.app/
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

    #[cfg(any(feature = "local-bin", feature = "use-local-agent"))]
    {
        let agent = Agent::builder()
            .with_url("https://ic0.app")
            .build()
            .unwrap();

        // agent.fetch_root_key().await.unwrap();

        agent
    }
}

pub async fn init_auth() -> Authenticator<HttpsConnector<HttpConnector>> {
    let sa_key_file = env::var("GOOGLE_SA_KEY").expect("GOOGLE_SA_KEY is required");

    // Load your service account key
    let sa_key = yup_oauth2::parse_service_account_key(sa_key_file).expect("GOOGLE_SA_KEY.json");

    ServiceAccountAuthenticator::builder(sa_key)
        .build()
        .await
        .unwrap()
}

pub async fn init_firestoredb() -> FirestoreDb {
    let options = FirestoreDbOptions::new("hot-or-not-feed-intelligence".to_string())
        .with_database_id("ic-pump-fun".to_string());

    FirestoreDb::with_options(options)
        .await
        .expect("failed to create firestore db")
}

pub fn init_qstash() -> QStashState {
    let qstash_key =
        env::var("QSTASH_CURRENT_SIGNING_KEY").expect("QSTASH_CURRENT_SIGNING_KEY is required");

    QStashState::init(qstash_key)
}

pub async fn init_bigquery_client() -> Client {
    let (config, _) = ClientConfig::new_with_auth().await.unwrap();
    Client::new(config).await.unwrap()
}

pub async fn init_nsfw_detect_channel() -> Channel {
    let tls_config = ClientTlsConfig::new().with_webpki_roots();
    Channel::from_static(NSFW_SERVER_URL)
        .tls_config(tls_config)
        .expect("Couldn't update TLS config for nsfw agent")
        .connect()
        .await
        .expect("Couldn't connect to nsfw agent")
}

pub async fn init_qstash_client() -> QStashClient {
    let auth_token = env::var("QSTASH_AUTH_TOKEN").expect("QSTASH_AUTH_TOKEN is required");
    QStashClient::new(auth_token.as_str())
}
