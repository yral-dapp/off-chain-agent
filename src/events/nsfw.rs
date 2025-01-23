use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

use crate::consts::NSFW_SERVER_URL;
use anyhow::Error;
use axum::{extract::State, Json};
use google_cloud_bigquery::http::{
    job::query::QueryRequest,
    tabledata::insert_all::{InsertAllRequest, Row},
};
use serde::{Deserialize, Serialize};
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::{metadata::MetadataValue, Request};

use crate::{app_state::AppState, AppError};

pub mod nsfw_detector {
    tonic::include_proto!("nsfw_detector");
}

fn create_output_directory(video_id: &str) -> Result<PathBuf, Error> {
    let video_name = Path::new(video_id)
        .file_stem()
        .ok_or(anyhow::anyhow!("Failed to get file stem"))?
        .to_str()
        .ok_or(anyhow::anyhow!("Failed to convert file stem to string"))?;
    let output_dir = Path::new(".").join(video_name);

    if !output_dir.exists() {
        fs::create_dir(&output_dir)?;
    }

    Ok(output_dir)
}

pub fn extract_frames(video_path: &str, output_dir: PathBuf) -> Result<Vec<Vec<u8>>, Error> {
    let output_pattern = output_dir.join("output-%04d.jpg");

    let status = Command::new("ffmpeg")
        .arg("-loglevel")
        .arg("error")
        .arg("-i")
        .arg(video_path)
        .arg("-vf")
        .arg("fps=1")
        .arg("-pix_fmt")
        .arg("rgb24")
        .arg(output_pattern.clone())
        .status()?;

    if !status.success() {
        return Err(anyhow::anyhow!("Failed to extract frames"));
    }

    let mut frames = Vec::new();
    for entry in fs::read_dir(output_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let frame = fs::read(&path)?;
            frames.push(frame);
        }
    }

    Ok(frames)
}

pub async fn upload_frames_to_gcs(
    gcs_client: &cloud_storage::Client,
    frames: Vec<Vec<u8>>,
    video_id: &str,
) -> Result<(), Error> {
    let bucket_name = "yral-video-frames";

    // Create a vector of futures for concurrent uploads
    let upload_futures = frames.into_iter().enumerate().map(|(i, frame)| {
        let frame_path = format!("{}/frame-{}.jpg", video_id, i);
        let bucket_name = bucket_name.to_string();

        async move {
            gcs_client
                .object()
                .create(&bucket_name, frame, &frame_path, "image/jpeg")
                .await
        }
    });

    // Execute all futures concurrently and collect results
    let results = futures::future::join_all(upload_futures).await;

    // Check if any upload failed
    for result in results {
        result?;
    }

    Ok(())
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VideoRequest {
    video_id: String,
}

// extract_frames_and_upload API handler which takes video_id as queryparam in axum
pub async fn extract_frames_and_upload(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<VideoRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let video_id = payload.video_id;
    let video_path = format!(
        "https://customer-2p3jflss4r4hmpnz.cloudflarestream.com/{}/downloads/default.mp4",
        video_id
    );
    let output_dir = create_output_directory(&video_id)?;
    let frames = extract_frames(&video_path, output_dir.clone())?;
    #[cfg(not(feature = "local-bin"))]
    upload_frames_to_gcs(&state.gcs_client, frames, &video_id).await?;
    // delete output directory
    fs::remove_dir_all(output_dir)?;

    // enqueue qstash job to detect nsfw
    let qstash_client = state.qstash_client.clone();
    qstash_client
        .publish_video_nsfw_detection(&video_id)
        .await?;

    Ok(Json(
        serde_json::json!({ "message": "Frames extracted and uploaded to GCS" }),
    ))
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct NSFWInfo {
    pub is_nsfw: bool,
    pub nsfw_ec: String,
    pub nsfw_gore: String,
    pub csam_detected: bool,
}

pub async fn get_video_nsfw_info(video_id: String) -> Result<NSFWInfo, Error> {
    // create a new connection everytime and depend on fly proxy to load balance
    let tls_config = ClientTlsConfig::new().with_webpki_roots();
    let channel = Channel::from_static(NSFW_SERVER_URL)
        .tls_config(tls_config)
        .expect("Couldn't update TLS config for nsfw agent")
        .connect()
        .await
        .expect("Couldn't connect to nsfw agent");

    let nsfw_grpc_auth_token = env::var("NSFW_GRPC_TOKEN").expect("NSFW_GRPC_TOKEN");
    let token: MetadataValue<_> = format!("Bearer {}", nsfw_grpc_auth_token).parse()?;

    let mut client = nsfw_detector::nsfw_detector_client::NsfwDetectorClient::with_interceptor(
        channel,
        move |mut req: Request<()>| {
            req.metadata_mut().insert("authorization", token.clone());
            Ok(req)
        },
    );

    let req = tonic::Request::new(nsfw_detector::NsfwDetectorRequestVideoId {
        video_id: video_id.clone(),
    });
    let res = client.detect_nsfw_video_id(req).await?;

    let mut nsfw_info = NSFWInfo::from(res.into_inner());

    Ok(nsfw_info)
}

#[derive(Serialize)]
struct VideoNSFWData {
    video_id: String,
    gcs_video_id: String,
    is_nsfw: bool,
    nsfw_ec: String,
    nsfw_gore: String,
}

#[cfg(feature = "local-bin")]
pub async fn nsfw_job(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<VideoRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    Err(anyhow::anyhow!("not implemented for local binary").into())
}

#[cfg(not(feature = "local-bin"))]
pub async fn nsfw_job(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<VideoRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let video_id = payload.video_id;

    let nsfw_info = get_video_nsfw_info(video_id.clone()).await?;

    // push nsfw info to bigquery table using google-cloud-bigquery
    let bigquery_client = state.bigquery_client.clone();
    push_nsfw_data_bigquery(bigquery_client, nsfw_info, video_id.clone()).await?;

    // enqueue qstash job to detect nsfw v2
    let qstash_client = state.qstash_client.clone();
    qstash_client
        .publish_video_nsfw_detection_v2(&video_id)
        .await?;

    Ok(Json(serde_json::json!({ "message": "NSFW job completed" })))
}

pub async fn push_nsfw_data_bigquery(
    bigquery_client: google_cloud_bigquery::client::Client,
    nsfw_info: NSFWInfo,
    video_id: String,
) -> Result<(), Error> {
    let row_data = VideoNSFWData {
        video_id: video_id.clone(),
        gcs_video_id: format!("gs://yral-videos/{}.mp4", video_id),
        is_nsfw: nsfw_info.is_nsfw,
        nsfw_ec: nsfw_info.nsfw_ec,
        nsfw_gore: nsfw_info.nsfw_gore,
    };

    let row = Row {
        insert_id: None,
        json: row_data,
    };

    let request = InsertAllRequest {
        rows: vec![row],
        ..Default::default()
    };

    bigquery_client
        .tabledata()
        .insert(
            "hot-or-not-feed-intelligence",
            "yral_ds",
            "video_nsfw",
            &request,
        )
        .await?;

    Ok(())
}

impl From<nsfw_detector::NsfwDetectorResponse> for NSFWInfo {
    fn from(item: nsfw_detector::NsfwDetectorResponse) -> Self {
        let is_nsfw = item.csam_detected
            || matches!(
                item.nsfw_gore.as_str(),
                "POSSIBLE" | "LIKELY" | "VERY_LIKELY"
            )
            || matches!(item.nsfw_ec.as_str(), "nudity" | "provocative" | "explicit");

        Self {
            is_nsfw,
            nsfw_ec: item.nsfw_ec,
            nsfw_gore: item.nsfw_gore,
            csam_detected: item.csam_detected,
        }
    }
}

#[cfg(feature = "local-bin")]
pub async fn nsfw_job_v2(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<VideoRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    Err(anyhow::anyhow!("not implemented for local binary").into())
}

#[cfg(not(feature = "local-bin"))]
pub async fn nsfw_job_v2(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<VideoRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let video_id = payload.video_id;

    let nsfw_prob = get_video_nsfw_info_v2(video_id.clone()).await?;

    // push nsfw info to bigquery table using google-cloud-bigquery
    let bigquery_client = state.bigquery_client.clone();
    push_nsfw_data_bigquery_v2(bigquery_client, nsfw_prob, video_id.clone()).await?;

    Ok(Json(
        serde_json::json!({ "message": "NSFW v2 job completed" }),
    ))
}

pub async fn get_video_nsfw_info_v2(video_id: String) -> Result<f32, Error> {
    // create a new connection everytime and depend on fly proxy to load balance
    let tls_config = ClientTlsConfig::new().with_webpki_roots();
    let channel = Channel::from_static(NSFW_SERVER_URL)
        .tls_config(tls_config)
        .expect("Couldn't update TLS config for nsfw agent")
        .connect()
        .await
        .expect("Couldn't connect to nsfw agent");

    let nsfw_grpc_auth_token = env::var("NSFW_GRPC_TOKEN").expect("NSFW_GRPC_TOKEN");
    let token: MetadataValue<_> = format!("Bearer {}", nsfw_grpc_auth_token).parse()?;

    let mut client = nsfw_detector::nsfw_detector_client::NsfwDetectorClient::with_interceptor(
        channel,
        move |mut req: Request<()>| {
            req.metadata_mut().insert("authorization", token.clone());
            Ok(req)
        },
    );

    // get embedding nsfw
    let embedding_req = tonic::Request::new(nsfw_detector::EmbeddingNsfwDetectorRequest {
        video_id: video_id.clone(),
    });
    let embedding_res = client.detect_nsfw_embedding(embedding_req).await?;

    Ok(embedding_res.into_inner().probability)
}

#[derive(Serialize)]
struct VideoNSFWDataV2 {
    video_id: String,
    gcs_video_id: String,
    is_nsfw: bool,
    nsfw_ec: String,
    nsfw_gore: String,
    probability: f32,
}

pub async fn push_nsfw_data_bigquery_v2(
    bigquery_client: google_cloud_bigquery::client::Client,
    nsfw_prob: f32,
    video_id: String,
) -> Result<(), Error> {
    // First query to get existing NSFW data
    let query = format!(
        "SELECT video_id, gcs_video_id, is_nsfw, nsfw_ec, nsfw_gore 
         FROM `hot-or-not-feed-intelligence.yral_ds.video_nsfw`
         WHERE video_id = '{}'",
        video_id
    );

    let request = QueryRequest {
        query,
        ..Default::default()
    };

    let result = bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await?;

    // Get the first row
    let row = result
        .rows
        .and_then(|mut rows| rows.pop())
        .ok_or(anyhow::anyhow!("No data found for video_id"))?;

    // Extract values from row
    let gcs_video_id = match &row.f[1].v {
        google_cloud_bigquery::http::tabledata::list::Value::String(s) => s.clone(),
        _ => return Err(anyhow::anyhow!("Invalid gcs_video_id")),
    };

    let is_nsfw = match &row.f[2].v {
        google_cloud_bigquery::http::tabledata::list::Value::String(b) => b == "true",
        _ => return Err(anyhow::anyhow!("Invalid is_nsfw")),
    };

    let nsfw_ec = match &row.f[3].v {
        google_cloud_bigquery::http::tabledata::list::Value::String(s) => s.clone(),
        _ => return Err(anyhow::anyhow!("Invalid nsfw_ec")),
    };

    let nsfw_gore = match &row.f[4].v {
        google_cloud_bigquery::http::tabledata::list::Value::String(s) => s.clone(),
        _ => return Err(anyhow::anyhow!("Invalid nsfw_gore")),
    };

    // Create row data for aggregated table
    let row_data = VideoNSFWDataV2 {
        video_id: video_id.clone(),
        gcs_video_id,
        is_nsfw,
        nsfw_ec,
        nsfw_gore,
        probability: nsfw_prob,
    };

    let row = Row {
        insert_id: None,
        json: row_data,
    };

    let request = InsertAllRequest {
        rows: vec![row],
        ..Default::default()
    };

    // Insert into aggregated table
    bigquery_client
        .tabledata()
        .insert(
            "hot-or-not-feed-intelligence",
            "yral_ds",
            "video_nsfw_agg",
            &request,
        )
        .await?;

    Ok(())
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BackfillRequest {
    num_videos: u64,
    max_jitter: u32,
}

pub async fn backfill_nsfw_job(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<BackfillRequest>,
) -> Result<Json<String>, AppError> {
    let bigquery_client = state.bigquery_client.clone();
    let qstash_client = state.qstash_client.clone();

    let query = format!(
        "SELECT video_id FROM `hot-or-not-feed-intelligence`.`yral_ds`.`video_nsfw` WHERE video_id NOT IN ( SELECT video_id FROM `hot-or-not-feed-intelligence`.`yral_ds`.`video_nsfw_agg` ) ORDER BY video_id LIMIT {}",
        payload.num_videos
    );

    let request = QueryRequest {
        query,
        ..Default::default()
    };

    let result = bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await?;

    let mut cnt = 0;
    let mut total = 0;
    for urow in result.rows {
        for row in urow {
            let video_id = match &row.f[0].v {
                google_cloud_bigquery::http::tabledata::list::Value::String(s) => s.clone(),
                _ => continue,
            };
            let res = qstash_client
                .publish_video_nsfw_detection_v2_backfill(&video_id, payload.max_jitter)
                .await;
            if res.is_ok() {
                cnt += 1;
            }
            total += 1;
        }
    }

    Ok(Json(format!(
        "NSFW backfill job completed for {}/{} videos",
        cnt, total
    )))
}

#[cfg(feature = "local-bin")]
pub async fn nsfw_job_v3(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<VideoRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    Err(anyhow::anyhow!("not implemented for local binary").into())
}

#[cfg(not(feature = "local-bin"))]
pub async fn nsfw_job_v3(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<VideoRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let video_id = payload.video_id;

    let nsfw_prob = get_video_nsfw_info_v2(video_id.clone()).await?;

    // push nsfw info to bigquery table using google-cloud-bigquery
    let bigquery_client = state.bigquery_client.clone();
    push_nsfw_data_bigquery_v3(bigquery_client, nsfw_prob, video_id.clone()).await?;

    Ok(Json(
        serde_json::json!({ "message": "NSFW v3 job completed" }),
    ))
}

pub async fn push_nsfw_data_bigquery_v3(
    bigquery_client: google_cloud_bigquery::client::Client,
    nsfw_prob: f32,
    video_id: String,
) -> Result<(), Error> {
    // Create row data for aggregated table
    let row_data = VideoNSFWDataV2 {
        video_id: video_id.clone(),
        gcs_video_id: format!("gs://yral-videos/{}.mp4", video_id),
        is_nsfw: false,
        nsfw_ec: "".to_string(),
        nsfw_gore: "".to_string(),
        probability: nsfw_prob,
    };

    let row = Row {
        insert_id: None,
        json: row_data,
    };

    let request = InsertAllRequest {
        rows: vec![row],
        ..Default::default()
    };

    // Insert into aggregated table
    bigquery_client
        .tabledata()
        .insert(
            "hot-or-not-feed-intelligence",
            "yral_ds",
            "video_nsfw_agg",
            &request,
        )
        .await?;

    Ok(())
}
