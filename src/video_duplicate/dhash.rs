use image::{imageops::FilterType, DynamicImage, GenericImageView, ImageBuffer, Luma};
use std::error::Error;
use tokio::process::Command;

pub async fn calculate_video_dhash(video_path: &str) -> Result<Vec<u64>, Box<dyn Error>> {
    let temp_dir = tempfile::tempdir()?;
    let output_pattern = temp_dir.path().join("frame_%d.jpg");

    let status = Command::new("ffmpeg")
        .args([
            "-i", video_path,
            "-vf", "select='not(mod(n,${total_frames}/5))',setpts=N/FRAME_RATE/TB",
            "-frames:v", "5",
            "-y",
            output_pattern.to_str().unwrap(),
        ])
        .status()
        .await?;

    if !status.success() {
        return Err("Failed to extract frames".into());
    }

    let mut hashes = Vec::new();
    for i in 1..=5 {
        let frame_path = temp_dir.path().join(format!("frame_{}.jpg", i));
        let img = image::open(&frame_path)?;
        let dhash = calculate_dhash(&img);
        hashes.push(dhash);
    }

    Ok(hashes)
}

fn calculate_dhash(img: &DynamicImage) -> u64 {
    let resized = img.resize_exact(9, 8, FilterType::Lanczos3);
    let gray = resized.to_luma8();

    let mut hash = 0u64;
    let mut bit_pos = 0;
    
    for y in 0..8 {
        for x in 0..8 {
            let left = gray.get_pixel(x, y)[0] as u32;
            let right = gray.get_pixel(x + 1, y)[0] as u32;
            
            if left > right {
                hash |= 1 << bit_pos;
            }
            bit_pos += 1;
        }
    }
    
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::{ImageBuffer, Luma};

    #[test]
    fn test_dhash_calculation() {
        let mut img = ImageBuffer::new(9, 8);
        
        for y in 0..8 {
            for x in 0..9 {
                let value = if (x + y) % 2 == 0 { 255 } else { 0 };
                img.put_pixel(x, y, Luma([value]));
            }
        }
        
        let dynamic_img = DynamicImage::ImageLuma8(img);
        let hash = calculate_dhash(&dynamic_img);
        
        assert_ne!(hash, 0);
    }
}