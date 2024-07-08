use std::env;

use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use serde_with::serde_as;

#[serde_as]
#[derive(Deserialize, Clone)]
pub struct AppConfig {
    pub yral_metadata_token: String,
    pub google_sa_key: String,
    pub ml_server_jwt_token: String,
    pub upstash_vector_read_write_token: String,
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        // set env var
        let sa_key_file = env::var("GOOGLE_SA_KEY").expect("GOOGLE_SA_KEY");
        env::set_var("SERVICE_ACCOUNT_JSON", sa_key_file);

        let conf = Config::builder()
            .add_source(File::with_name("config.toml").required(false))
            .add_source(File::with_name(".env").required(false))
            .add_source(Environment::default())
            .build()?;

        conf.try_deserialize()
    }
}
