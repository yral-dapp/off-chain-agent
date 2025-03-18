use image::imageops::FilterType;
use image::DynamicImage;
use log;
use rayon::prelude::*;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use uuid::Uuid;

/// Frame size for video processing
pub const FRAME_SIZE: u32 = 144;
/// Grid size for hash generation (8x8)
pub const GRID_SIZE: u32 = 8;
/// Default sample rate in seconds between frames
pub const SAMPLE_RATE: f32 = 1.0;
/// Maximum number of frames to process
pub const MAX_FRAMES: usize = 60;
/// Size of the generated hash in bits
pub const HASH_SIZE: usize = 64;

fn create_ram_temp_dir(prefix: &str) -> Result<PathBuf, Box<dyn Error + Send + Sync>> {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let count = COUNTER.fetch_add(1, Ordering::SeqCst);

    let unique_id = format!("{}_{:x}_{}", prefix, Uuid::new_v4().as_simple(), count);

    let base_dir = if cfg!(target_os = "linux") {
        if Path::new("/dev/shm").exists() {
            PathBuf::from("/dev/shm")
        } else if Path::new("/run/user").exists() {
            match std::env::var("UID") {
                Ok(uid) => PathBuf::from(format!("/run/user/{}", uid)),
                Err(_) => std::env::temp_dir(),
            }
        } else {
            std::env::temp_dir()
        }
    } else if cfg!(target_os = "macos") {
        if Path::new("/private/var/vm").exists()
            && fs::metadata("/private/var/vm")
                .map(|m| m.is_dir())
                .unwrap_or(false)
        {
            PathBuf::from("/private/var/vm")
        } else {
            std::env::temp_dir()
        }
    } else {
        std::env::temp_dir()
    };

    let dir_path = base_dir.join(unique_id);
    fs::create_dir_all(&dir_path)?;

    log::debug!("Created RAM-based temp directory at: {:?}", dir_path);
    Ok(dir_path)
}

/// VideoHash represents a perceptual hash of a video
#[derive(Debug, Clone)]
pub struct VideoHash {
    /// The binary hash string (64 characters of '0' and '1')
    pub hash: String,
}

impl VideoHash {
    /// Create a new VideoHash from a video file path
    pub fn new(video_path: &Path) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let start = Instant::now();
        let hash = Self::fast_hash(video_path)?;
        log::info!("Total processing time: {:?}", start.elapsed());
        Ok(Self { hash })
    }

    pub fn fast_hash(video_path: &Path) -> Result<String, Box<dyn Error + Send + Sync>> {
        let start = Instant::now();

        let temp_dir = create_ram_temp_dir("videohash")?;
        log::debug!("Using temp directory: {:?}", temp_dir);

        let output_pattern = temp_dir
            .join("frame_%04d.jpg")
            .to_string_lossy()
            .to_string();

        let duration_output = Command::new("ffprobe")
            .args([
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                video_path.to_str().unwrap(),
            ])
            .output()?;

        let duration: f32 = String::from_utf8_lossy(&duration_output.stdout)
            .trim()
            .parse()
            .unwrap_or(0.0);

        let file_size = fs::metadata(video_path).map(|m| m.len()).unwrap_or(0);
        let is_small_file = file_size < 10_000_000;

        let sample_rate = if is_small_file {
            2.0
        } else if duration > MAX_FRAMES as f32 * 2.0 {
            duration / (MAX_FRAMES as f32)
        } else {
            SAMPLE_RATE
        };

        let threads_param = "-threads 0";

        let extra_opts = if cfg!(target_os = "linux") {
            "-preset ultrafast -tune fastdecode"
        } else {
            "-preset ultrafast"
        };

        let ffmpeg_args = format!(
            "-i \"{}\" {} {} -vf \"fps=1/{},scale=-1:{}\" -q:v 2 {}",
            video_path.to_str().unwrap(),
            threads_param,
            extra_opts,
            sample_rate,
            FRAME_SIZE,
            output_pattern
        );

        log::debug!("Running FFmpeg with args: {}", ffmpeg_args);

        let output = Command::new("sh")
            .args(["-c", &format!("ffmpeg {}", ffmpeg_args)])
            .stderr(Stdio::null())
            .stdout(Stdio::null())
            .status()?;

        if !output.success() {
            let _ = fs::remove_dir_all(&temp_dir);
            return Err("Failed to extract frames with ffmpeg".into());
        }

        let mut frame_paths = Vec::new();
        for entry in fs::read_dir(&temp_dir)? {
            match entry {
                Ok(entry) => {
                    let path = entry.path();
                    if path.extension().unwrap_or_default() == "jpg" {
                        frame_paths.push(path);
                    }
                }
                Err(_) => continue,
            }
        }
        frame_paths.sort();

        if frame_paths.is_empty() {
            let _ = fs::remove_dir_all(&temp_dir);
            return Err("No frames could be extracted".into());
        }

        let selected_frames: Vec<_> = if frame_paths.len() > MAX_FRAMES {
            let step = frame_paths.len() / MAX_FRAMES;
            frame_paths
                .iter()
                .enumerate()
                .filter(|(i, _)| i % step == 0)
                .map(|(_, path)| path.clone())
                .take(MAX_FRAMES)
                .collect()
        } else {
            frame_paths.clone()
        };

        log::info!(
            "Extracting {} frames took {:?}",
            selected_frames.len(),
            start.elapsed()
        );
        let hash_start = Instant::now();

        let frames: Vec<_> = selected_frames
            .par_iter()
            .filter_map(|path| image::open(path).ok())
            .collect();

        if frames.is_empty() {
            let _ = fs::remove_dir_all(&temp_dir);
            return Err("Failed to load any frames".into());
        }

        let (wavelet_hash, color_hash) = rayon::join(
            || Self::calculate_wavelet_hash(&frames),
            || Self::calculate_color_hash(&frames),
        );

        let final_hash = Self::xor_hashes(wavelet_hash?, color_hash?);
        log::info!("Hash calculation took {:?}", hash_start.elapsed());

        log::debug!("Cleaning up temporary files in: {:?}", temp_dir);
        let _ = fs::remove_dir_all(&temp_dir);

        Ok(final_hash)
    }

    pub fn calculate_wavelet_hash(
        frames: &[DynamicImage],
    ) -> Result<Vec<bool>, Box<dyn Error + Send + Sync>> {
        let num_frames = frames.len();

        if num_frames == 1 {
            let gray = frames[0]
                .resize_exact(GRID_SIZE, GRID_SIZE, FilterType::Triangle)
                .grayscale()
                .to_luma8();
            let mut pixels: Vec<_> = gray.pixels().map(|p| p[0]).collect();
            pixels.sort_unstable();
            let median = pixels[pixels.len() / 2];
            return Ok(gray.pixels().map(|p| p[0] >= median).collect());
        }

        let grid_side = (num_frames as f64).sqrt().ceil() as u32;
        let mut collage = image::RgbaImage::new(grid_side * FRAME_SIZE, grid_side * FRAME_SIZE);

        let resized_frames: Vec<_> = frames
            .par_iter()
            .map(|frame| {
                frame
                    .resize_exact(FRAME_SIZE, FRAME_SIZE, FilterType::Triangle)
                    .to_rgba8()
            })
            .collect();

        for (i, resized) in resized_frames.iter().enumerate() {
            let x = (i as u32 % grid_side) * FRAME_SIZE;
            let y = (i as u32 / grid_side) * FRAME_SIZE;
            image::imageops::replace(&mut collage, resized, x as i64, y as i64);
        }

        let small = DynamicImage::ImageRgba8(collage)
            .grayscale()
            .resize_exact(GRID_SIZE, GRID_SIZE, FilterType::Triangle)
            .to_luma8();

        let mut pixels: Vec<_> = small.pixels().map(|p| p[0]).collect();
        pixels.sort_unstable_by_key(|k| *k);
        let median = pixels[pixels.len() / 2];

        Ok(small.pixels().map(|p| p[0] >= median).collect())
    }

    pub fn calculate_color_hash(
        frames: &[DynamicImage],
    ) -> Result<Vec<bool>, Box<dyn Error + Send + Sync>> {
        if frames.len() == 1 {
            return Self::calculate_single_frame_color_hash(&frames[0]);
        }

        let total_width = frames
            .par_iter()
            .map(|frame| {
                let aspect_ratio = frame.width() as f32 / frame.height() as f32;
                (FRAME_SIZE as f32 * aspect_ratio).round() as u32
            })
            .sum();

        let mut stitch = image::RgbaImage::new(total_width, FRAME_SIZE);
        let mut x_offset = 0;

        for frame in frames {
            let aspect_ratio = frame.width() as f32 / frame.height() as f32;
            let new_width = (FRAME_SIZE as f32 * aspect_ratio).round() as u32;
            let resized = frame.resize_exact(new_width, FRAME_SIZE, FilterType::Triangle);

            image::imageops::replace(&mut stitch, &resized.to_rgba8(), x_offset, 0);
            x_offset += new_width as i64;
        }

        let chunk_width = stitch.width() / GRID_SIZE;
        let chunk_height = stitch.height();

        let mut hash_bits = Vec::with_capacity(64);

        for _y in 0..8 {
            for x in 0..8 {
                let x_start = x * chunk_width;

                let mut r_sum = 0u64;
                let mut g_sum = 0u64;
                let mut b_sum = 0u64;
                let mut pixel_count = 0;

                let sample_rate = if chunk_width * chunk_height > 10000 {
                    4
                } else {
                    1
                };

                for y_pos in (0..chunk_height).step_by(sample_rate) {
                    for x_pos in (x_start..std::cmp::min(x_start + chunk_width, stitch.width()))
                        .step_by(sample_rate)
                    {
                        let pixel = stitch.get_pixel(x_pos, y_pos);
                        r_sum += pixel[0] as u64;
                        g_sum += pixel[1] as u64;
                        b_sum += pixel[2] as u64;
                        pixel_count += 1;
                    }
                }

                if pixel_count > 0 {
                    let avg_r = (r_sum / pixel_count) as u8;
                    let avg_g = (g_sum / pixel_count) as u8;
                    let avg_b = (b_sum / pixel_count) as u8;

                    let r_dominance = avg_r > avg_g && avg_r > avg_b;
                    let g_dominance = avg_g > avg_r && avg_g > avg_b;
                    let b_dominance = avg_b > avg_r && avg_b > avg_g;

                    if r_dominance {
                        hash_bits.push(avg_r > 128);
                    } else if g_dominance {
                        hash_bits.push(avg_g > 128);
                    } else if b_dominance {
                        hash_bits.push(avg_b > 128);
                    } else {
                        let brightness = (avg_r as u32 + avg_g as u32 + avg_b as u32) / 3;
                        hash_bits.push(brightness > 128);
                    }
                } else {
                    hash_bits.push(false);
                }
            }
        }

        Ok(hash_bits)
    }

    pub fn calculate_single_frame_color_hash(
        frame: &DynamicImage,
    ) -> Result<Vec<bool>, Box<dyn Error + Send + Sync>> {
        let small = frame
            .resize_exact(GRID_SIZE, GRID_SIZE, FilterType::Triangle)
            .to_rgba8();
        let mut hash_bits = Vec::with_capacity(64);

        for y in 0..GRID_SIZE {
            for x in 0..GRID_SIZE {
                let pixel = small.get_pixel(x, y);
                let r = pixel[0] as u32;
                let g = pixel[1] as u32;
                let b = pixel[2] as u32;

                let r_dominance = r > g && r > b;
                let g_dominance = g > r && g > b;
                let b_dominance = b > r && b > g;

                if r_dominance {
                    hash_bits.push(r > 128);
                } else if g_dominance {
                    hash_bits.push(g > 128);
                } else if b_dominance {
                    hash_bits.push(b > 128);
                } else {
                    let brightness = (r + g + b) / 3;
                    hash_bits.push(brightness > 128);
                }
            }
        }

        Ok(hash_bits)
    }

    pub fn xor_hashes(hash1: Vec<bool>, hash2: Vec<bool>) -> String {
        hash1
            .iter()
            .zip(hash2.iter())
            .map(|(bit1, bit2)| if *bit1 ^ *bit2 { '1' } else { '0' })
            .collect()
    }

    /// Calculate the Hamming distance between this hash and another
    pub fn hamming_distance(&self, other: &VideoHash) -> u32 {
        self.hash
            .chars()
            .zip(other.hash.chars())
            .filter(|(a, b)| a != b)
            .count() as u32
    }

    /// Calculate similarity percentage between hashes (100% = identical)
    pub fn similarity(&self, other: &VideoHash) -> f64 {
        let distance = self.hamming_distance(other) as f64;
        let max_distance = self.hash.len() as f64;
        (max_distance - distance) / max_distance * 100.0
    }

    /// Determine if two videos are likely duplicates based on threshold
    pub fn is_duplicate(&self, other: &VideoHash, threshold: Option<f64>) -> bool {
        let threshold = threshold.unwrap_or(85.0);
        self.similarity(other) >= threshold
    }
}
