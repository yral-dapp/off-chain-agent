use std::error::Error;
use std::path::PathBuf;
use tokio::process::Command;

pub struct FrameExtractor {
    frame_count: usize,
}

impl FrameExtractor {
    pub fn new(frame_count: usize) -> Self {
        Self { frame_count }
    }

    pub async fn extract_frames(&self, video_path: &str, temp_dir: &PathBuf) -> Result<Vec<PathBuf>, Box<dyn Error>> {
        let duration = self.get_video_duration(video_path).await?;
        let frame_times: Vec<f64> = (0..self.frame_count)
            .map(|i| (duration * i as f64) / (self.frame_count - 1) as f64)
            .collect();

        let mut frame_paths = Vec::with_capacity(self.frame_count);
        
        for time in frame_times {
            let frame_path = temp_dir.join(format!("frame_{}.jpg", time));
            self.extract_frame(video_path, &frame_path, time).await?;
            frame_paths.push(frame_path);
        }

        Ok(frame_paths)
    }

    async fn extract_frame(&self, video_path: &str, frame_path: &PathBuf, timestamp: f64) -> Result<(), Box<dyn Error>> {
        let status = Command::new("ffmpeg")
            .args([
                "-ss", &timestamp.to_string(),
                "-i", video_path,
                "-vframes", "1",
                "-q:v", "2",
                "-y",
                frame_path.to_str().unwrap(),
            ])
            .status()
            .await?;

        if !status.success() {
            return Err("Frame extraction failed".into());
        }
        Ok(())
    }

    async fn get_video_duration(&self, video_path: &str) -> Result<f64, Box<dyn Error>> {
        let output = Command::new("ffprobe")
            .args([
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                video_path
            ])
            .output()
            .await?;

        let duration_str = String::from_utf8(output.stdout)?;
        Ok(duration_str.trim().parse::<f64>()?)
    }
}