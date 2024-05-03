use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("failed to load config {0}")]
    Config(#[from] config::ConfigError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
