use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use serde_with::serde_as;

#[serde_as]
#[derive(Deserialize, Clone)]
pub struct AppConfig {
    pub yral_metadata_token: String,
    pub google_sa_key: String,
    pub ml_server_auth_token: String,
    pub upstash_vector_rest_token: String,
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        let conf = Config::builder()
            .add_source(File::with_name("config.toml").required(false))
            .add_source(File::with_name(".env").required(false))
            .add_source(Environment::default())
            .build()?;

        conf.try_deserialize()
    }
}
