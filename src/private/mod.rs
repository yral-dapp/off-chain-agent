use std::{collections::HashMap, sync::Arc};

use anyhow::anyhow;
use axum::{
    extract::{Query, State},
    Json,
};
use google_cloud_bigquery::{
    http::{
        job::query::QueryRequest,
        types::{QueryParameter, QueryParameterType, QueryParameterValue},
    },
    query::row::Row,
};
use serde::Deserialize;
use storj::fetch;

use crate::{app_state::AppState, AppError};

const NSFW_PROBABILITY_QUERY: &str = "SELECT probability, video_id FROM `hot-or-not-feed-intelligence.yral_ds.video_nsfw_agg` WHERE video_id IN UNNEST(@ids);
";

mod storj;

pub async fn kickstart_stage_one(State(app_state): State<Arc<AppState>>) {
    let agent = app_state.agent.clone();

    tokio::spawn(async move {
        log::info!("Starting storj backfill stage one");
        let result = fetch(agent).await;
        match result {
            Ok(value) => log::info!(
                "completed creating work queue!\n{}",
                serde_json::to_string_pretty(&value).unwrap()
            ),
            Err(err) => log::error!("storj backfil state one failed: {err}"),
        }
    });
}

pub async fn get_nsfw_probability(
    State(app_state): State<Arc<AppState>>,
    Json(ids): Json<Vec<String>>,
) -> Result<Json<Vec<(String, f64)>>, AppError> {
    let mut res = Vec::with_capacity(ids.len());

    let params = QueryParameter {
        name: Some("ids".into()),
        parameter_type: QueryParameterType {
            parameter_type: "ARRAY".into(),
            array_type: Some(Box::new(QueryParameterType {
                parameter_type: "STRING".into(),
                ..Default::default()
            })),
            ..Default::default()
        },
        parameter_value: QueryParameterValue {
            array_values: Some(
                ids.into_iter()
                    .map(|id| QueryParameterValue {
                        value: Some(id),
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        },
    };

    let query = QueryRequest {
        query: NSFW_PROBABILITY_QUERY.into(),
        parameter_mode: Some("NAMED".into()),
        query_parameters: vec![params],
        ..Default::default()
    };

    let mut result = app_state
        .bigquery_client
        .query::<Row>("hot-or-not-feed-intelligence", query)
        .await?;

    while let Some(row) = result.next().await? {
        let prob = row.column(0)?;
        let video_id = row.column(1)?;

        res.push((video_id, prob));
    }

    Ok(Json(res))
}

#[derive(Deserialize)]
struct ObjectDataQuery {
    id: String,
}

pub async fn get_object_metadata(
    Query(ObjectDataQuery { id }): Query<ObjectDataQuery>,
) -> Result<Json<HashMap<String, String>>, AppError> {
    let client = cloud_storage::Client::default();

    let object = client
        .object()
        .read("yral-videos", &format!("{id}.m4"))
        .await?;

    let metadata = object.metadata.ok_or(anyhow!("There's no metadata"))?;

    Ok(Json(metadata))
}
