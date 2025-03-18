use image::imageops::FilterType;
use image::DynamicImage;
use rayon::prelude::*;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Instant;

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

/// VideoHash represents a perceptual hash of a video
/// that can be used for similarity comparison
#[derive(Debug, Clone)]
pub struct VideoHash {
    /// The binary hash string (64 characters of '0' and '1')
    pub hash: String,
}

impl VideoHash {
    /// Create a new VideoHash from a video file path
    ///
    /// # Arguments
    /// * `video_path` - Path to the video file
    ///
    /// # Returns
    /// * `Result<VideoHash, Box<dyn Error + Send + Sync>>` - The computed hash or an error
    pub fn new(video_path: &Path) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let start = Instant::now();
        let hash = Self::fast_hash(video_path)?;
        println!("Total processing time: {:?}", start.elapsed());
        Ok(Self { hash })
    }

    /// Generate a video hash optimized for speed and accuracy
    ///
    /// # Arguments
    /// * `video_path` - Path to the video file
    ///
    /// # Returns
    /// * `Result<String, Box<dyn Error + Send + Sync>>` - The computed hash string or an error
    pub fn fast_hash(video_path: &Path) -> Result<String, Box<dyn Error + Send + Sync>> {
        let start = Instant::now();

        // 1. Set up optimized temporary directory
        let temp_dir = if cfg!(target_os = "linux") && std::path::Path::new("/dev/shm").exists() {
            std::path::PathBuf::from("/dev/shm/videohash_tmp")
        } else {
            std::env::temp_dir().join("videohash_tmp")
        };

        if !temp_dir.exists() {
            fs::create_dir_all(&temp_dir)?;
        }

        let output_pattern = temp_dir
            .join("frame_%04d.jpg")
            .to_string_lossy()
            .to_string();

        // 2. Determine optimal sample rate based on video duration and size
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
        let is_small_file = file_size < 10_000_000; // 10MB threshold

        // Adaptive sampling rate based on video length and file size
        let sample_rate = if is_small_file {
            2.0 // Faster processing for small files
        } else if duration > MAX_FRAMES as f32 * 2.0 {
            duration / (MAX_FRAMES as f32)
        } else {
            SAMPLE_RATE
        };

        // 3. Extract frames with optimized FFmpeg settings
        let threads_param = if cfg!(target_os = "linux") || cfg!(target_os = "macos") {
            "-threads 0" // Use all available threads on Unix-like systems
        } else {
            ""
        };

        // Create the FFmpeg command with optimized parameters
        let ffmpeg_args = format!(
            "-i \"{}\" {} -vf \"fps=1/{},scale=-1:{}\" -q:v 2 -preset ultrafast \"{}\"",
            video_path.to_str().unwrap(),
            threads_param,
            sample_rate,
            FRAME_SIZE,
            output_pattern
        );

        let output = Command::new("sh")
            .args(["-c", &format!("ffmpeg {}", ffmpeg_args)])
            .stderr(Stdio::null())
            .stdout(Stdio::null())
            .status()?;

        if !output.success() {
            return Err("Failed to extract frames with ffmpeg".into());
        }

        // 4. Read frame paths and sort them
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
            return Err("No frames could be extracted".into());
        }

        // 5. Adaptive frame selection for long videos
        let selected_frames: Vec<_> = if frame_paths.len() > MAX_FRAMES {
            // Select evenly distributed frames for long videos
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

        println!(
            "Extracting {} frames took {:?}",
            selected_frames.len(),
            start.elapsed()
        );
        let hash_start = Instant::now();

        // 6. Load frames in parallel with error handling
        let frames: Vec<_> = selected_frames
            .par_iter()
            .filter_map(|path| image::open(path).ok())
            .collect();

        if frames.is_empty() {
            return Err("Failed to load any frames".into());
        }

        // 7. Calculate both hashes in parallel
        let (wavelet_hash, color_hash) = rayon::join(
            || Self::calculate_wavelet_hash(&frames),
            || Self::calculate_color_hash(&frames),
        );

        // 8. XOR the hashes
        let final_hash = Self::xor_hashes(wavelet_hash?, color_hash?);
        println!("Hash calculation took {:?}", hash_start.elapsed());

        // 9. Clean up temporary files
        for path in frame_paths {
            let _ = fs::remove_file(path);
        }
        let _ = fs::remove_dir_all(&temp_dir);

        Ok(final_hash)
    }

    /// Calculate a wavelet hash from a set of video frames
    ///
    /// # Arguments
    /// * `frames` - Vector of frames as DynamicImage
    ///
    /// # Returns
    /// * `Result<Vec<bool>, Box<dyn Error + Send + Sync>>` - Wavelet hash as boolean vector
    pub fn calculate_wavelet_hash(
        frames: &[DynamicImage],
    ) -> Result<Vec<bool>, Box<dyn Error + Send + Sync>> {
        let num_frames = frames.len();

        // Fast path for single frame
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

        // Normal path for multiple frames
        let grid_side = (num_frames as f64).sqrt().ceil() as u32;
        let mut collage = image::RgbaImage::new(grid_side * FRAME_SIZE, grid_side * FRAME_SIZE);

        // Process frames in parallel for collage
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

        // Fast path: directly resize to 8x8 grayscale
        let small = DynamicImage::ImageRgba8(collage)
            .grayscale()
            .resize_exact(GRID_SIZE, GRID_SIZE, FilterType::Triangle)
            .to_luma8();

        // Fast median calculation
        let mut pixels: Vec<_> = small.pixels().map(|p| p[0]).collect();
        pixels.sort_unstable_by_key(|k| *k);
        let median = pixels[pixels.len() / 2];

        Ok(small.pixels().map(|p| p[0] >= median).collect())
    }

    /// Calculate a color hash from a set of video frames
    ///
    /// # Arguments
    /// * `frames` - Vector of frames as DynamicImage
    ///
    /// # Returns
    /// * `Result<Vec<bool>, Box<dyn Error + Send + Sync>>` - Color hash as boolean vector
    pub fn calculate_color_hash(
        frames: &[DynamicImage],
    ) -> Result<Vec<bool>, Box<dyn Error + Send + Sync>> {
        // Fast path for single frame
        if frames.len() == 1 {
            return Self::calculate_single_frame_color_hash(&frames[0]);
        }

        // Calculate total width for the stitch
        let total_width = frames
            .par_iter()
            .map(|frame| {
                let aspect_ratio = frame.width() as f32 / frame.height() as f32;
                (FRAME_SIZE as f32 * aspect_ratio).round() as u32
            })
            .sum();

        // Create the horizontal stitch
        let mut stitch = image::RgbaImage::new(total_width, FRAME_SIZE);
        let mut x_offset = 0;

        // Add frames to stitch
        for frame in frames {
            let aspect_ratio = frame.width() as f32 / frame.height() as f32;
            let new_width = (FRAME_SIZE as f32 * aspect_ratio).round() as u32;
            let resized = frame.resize_exact(new_width, FRAME_SIZE, FilterType::Triangle);

            image::imageops::replace(&mut stitch, &resized.to_rgba8(), x_offset, 0);
            x_offset += new_width as i64;
        }

        // Divide into 64 equal parts
        let chunk_width = stitch.width() / GRID_SIZE;
        let chunk_height = stitch.height();

        let mut hash_bits = Vec::with_capacity(64);

        // Process all 64 regions (8x8 grid)
        for _y in 0..8 {
            for x in 0..8 {
                let x_start = x * chunk_width;

                let mut r_sum = 0u64;
                let mut g_sum = 0u64;
                let mut b_sum = 0u64;
                let mut pixel_count = 0;

                // Sample pixels for large regions instead of processing every pixel
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

                // Calculate average color
                if pixel_count > 0 {
                    let avg_r = (r_sum / pixel_count) as u8;
                    let avg_g = (g_sum / pixel_count) as u8;
                    let avg_b = (b_sum / pixel_count) as u8;
                    let brightness = (avg_r as u32 + avg_g as u32 + avg_b as u32) / 3;
                    hash_bits.push(brightness > 128);
                } else {
                    hash_bits.push(false);
                }
            }
        }

        Ok(hash_bits)
    }

    /// Calculate a color hash from a single frame
    ///
    /// # Arguments
    /// * `frame` - Single video frame as DynamicImage
    ///
    /// # Returns
    /// * `Result<Vec<bool>, Box<dyn Error + Send + Sync>>` - Color hash as boolean vector
    pub fn calculate_single_frame_color_hash(
        frame: &DynamicImage,
    ) -> Result<Vec<bool>, Box<dyn Error + Send + Sync>> {
        // Resize to 8x8
        let small = frame
            .resize_exact(GRID_SIZE, GRID_SIZE, FilterType::Triangle)
            .to_rgba8();
        let mut hash_bits = Vec::with_capacity(64);

        for y in 0..GRID_SIZE {
            for x in 0..GRID_SIZE {
                let pixel = small.get_pixel(x, y);
                let brightness = (pixel[0] as u32 + pixel[1] as u32 + pixel[2] as u32) / 3;
                hash_bits.push(brightness > 128);
            }
        }

        Ok(hash_bits)
    }

    /// Combine two hashes using XOR operation
    ///
    /// # Arguments
    /// * `hash1` - First hash as boolean vector
    /// * `hash2` - Second hash as boolean vector
    ///
    /// # Returns
    /// * `String` - Combined hash as string of '0' and '1'
    pub fn xor_hashes(hash1: Vec<bool>, hash2: Vec<bool>) -> String {
        hash1
            .iter()
            .zip(hash2.iter())
            .map(|(bit1, bit2)| if *bit1 ^ *bit2 { '1' } else { '0' })
            .collect()
    }
}
