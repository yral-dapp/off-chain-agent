use std::sync::Arc;

use axum::{extract::State, response::IntoResponse};
use google_cloud_bigquery::{
    client::Client,
    http::{
        job::query::QueryRequest,
        tabledata::insert_all::{InsertAllRequest, Row},
    },
    query::row::Row as QueryRow,
};
use http::StatusCode;
use ic_agent::Agent;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
    app_state::{self, AppState},
    canister::utils::get_user_principal_canister_list_v2,
};

#[derive(Serialize, Deserialize, Clone)]
pub struct UserCanisterPrincipal {
    pub canister_id: String,
    pub user_principal_id: String,
}

#[instrument(skip(bq_client))]
pub async fn handle_login_successful(
    bq_client: Client,
    canister_id: &str,
    user_id: &str,
) -> Result<(), anyhow::Error> {
    // check if its unique
    let request = QueryRequest {
        query: format!(
            "SELECT * FROM `hot-or-not-feed-intelligence.yral_ds.canister_user_principal` WHERE canister_id = '{}' AND user_principal_id = '{}'",
            canister_id,
            user_id
        ),
        ..Default::default()
    };
    let mut response = bq_client
        .query::<QueryRow>("hot-or-not-feed-intelligence", request)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to query canister_user_principal: {}", e))?;
    let mut res_list = Vec::new();
    while let Some(row) = response.next().await? {
        res_list.push(row);
    }
    // if already exists, return
    if !res_list.is_empty() {
        return Ok(());
    }

    let login_successful_request = UserCanisterPrincipal {
        canister_id: canister_id.to_string(),
        user_principal_id: user_id.to_string(),
    };

    // insert into bigquery
    let request = InsertAllRequest {
        rows: vec![Row {
            insert_id: None,
            json: login_successful_request,
        }],
        ..Default::default()
    };

    let res = bq_client
        .tabledata()
        .insert(
            "hot-or-not-feed-intelligence",
            "yral_ds",
            "canister_user_principal",
            &request,
        )
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to insert canister_user_principal row to bigquery: {}",
                e
            )
        })?;

    if let Some(errors) = res.insert_errors {
        if errors.len() > 0 {
            log::error!("canister_user_principal insert response : {:?}", errors);
            return Err(anyhow::anyhow!(
                "Failed to insert canister_user_principal row to bigquery"
            ));
        }
    }

    Ok(())
}

#[instrument(skip(app_state))]
pub async fn bulk_insert_canister_user_principal(
    State(app_state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let bq_client = app_state.bigquery_client.clone();
    let agent = app_state.agent.clone();

    let user_principal_canisters_list = get_user_principal_canister_list_v2(&agent)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let user_canister_principal_list = user_principal_canisters_list
        .iter()
        .map(|(user_principal, canister_id)| Row {
            insert_id: None,
            json: UserCanisterPrincipal {
                canister_id: canister_id.to_string(),
                user_principal_id: user_principal.to_string(),
            },
        })
        .collect::<Vec<_>>();

    // insert into bigquery in chunks of 1000
    for chunk in user_canister_principal_list.chunks(50000) {
        let request = InsertAllRequest {
            rows: chunk.to_vec(),
            ..Default::default()
        };

        let res = bq_client
            .tabledata()
            .insert(
                "hot-or-not-feed-intelligence",
                "yral_ds",
                "canister_user_principal",
                &request,
            )
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!(
                        "Failed to insert canister_user_principal row to bigquery: {}",
                        e
                    ),
                )
            })?;

        if let Some(errors) = res.insert_errors {
            if errors.len() > 0 {
                log::error!("canister_user_principal insert response : {:?}", errors);
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to insert canister_user_principal row to bigquery".to_string(),
                ));
            }
        }
    }

    Ok(())
}
