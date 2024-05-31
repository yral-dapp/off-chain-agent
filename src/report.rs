use std::env;

use crate::AppError;
use anyhow::{Context, Result};
use serde_json::json;
use yup_oauth2::{
    ApplicationDefaultCredentialsFlowOpts, AuthorizedUserAuthenticator, DeviceFlowAuthenticator,
    ServiceAccountAuthenticator,
};

pub async fn report_handler() -> Result<(), AppError> {
    let url = "https://chat.googleapis.com/v1/spaces/AAAA1yDLYO4/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=JO4bcid5fqKb0Of4rHMcTwmAHeqS9_OfrJVK7BfktW4";

    let client = reqwest::Client::new();
    let res = client
        .post(url)
        .header("Content-Type", "application/json")
        .body(r#"{"text": "Hello, World!"}"#)
        .send()
        .await?;

    Ok(())
}
