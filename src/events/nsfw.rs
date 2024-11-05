use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::Error;
use axum::{extract::Query, Json};
use http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::AppError;

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

pub async fn upload_frames_to_gcs(frames: Vec<Vec<u8>>, video_id: &str) -> Result<(), Error> {
    let bucket_name = "yral-video-frames-test";

    // Create a vector of futures for concurrent uploads
    let upload_futures = frames.into_iter().enumerate().map(|(i, frame)| {
        let frame_path = format!("{}/frame-{}.jpg", video_id, i);
        let bucket_name = bucket_name.to_string();

        async move {
            let gcs_client = cloud_storage::Client::default();
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

#[derive(Serialize, Deserialize)]
pub struct ExtractFrameRequest {
    video_id: String,
}

// extract_frames_and_upload API handler which takes video_id as queryparam in axum
pub async fn extract_frames_and_upload(
    Json(payload): Json<ExtractFrameRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let video_id = payload.video_id;
    let video_path = format!(
        "https://customer-2p3jflss4r4hmpnz.cloudflarestream.com/{}/downloads/default.mp4",
        video_id
    );
    let output_dir = create_output_directory(&video_id)?;
    let frames = extract_frames(&video_path, output_dir.clone())?;
    upload_frames_to_gcs(frames, &video_id).await?;
    // delete output directory
    fs::remove_dir_all(output_dir)?;
    Ok(Json(
        serde_json::json!({ "message": "Frames extracted and uploaded to GCS" }),
    ))
}
