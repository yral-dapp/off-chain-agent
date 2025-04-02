use crate::duplicate_video::videohash::VideoHash;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::Command;

fn create_test_video(
    path: &str,
    duration: u32,
    color: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir_path = Path::new(path).parent().unwrap();
    fs::create_dir_all(dir_path)?;

    let status = Command::new("ffmpeg")
        .args(&[
            "-f",
            "lavfi",
            "-i",
            &format!("color=c={}:s=320x240:d={}", color, duration),
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-y",
            path,
        ])
        .status()?;

    if !status.success() {
        return Err("Failed to create test video".into());
    }

    Ok(())
}

#[test]
fn test_hash_generation() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_dir = "target/test_videos";
    fs::create_dir_all(test_dir)?;

    let test_video = format!("{}/red_3sec.mp4", test_dir);
    create_test_video(&test_video, 3, "red")?;

    let video_path = Path::new(&test_video);
    let hash = VideoHash::new(video_path)?;

    assert_eq!(hash.hash.len(), 64);
    assert!(hash.hash.chars().all(|c| c == '0' || c == '1'));

    fs::remove_file(video_path)?;

    Ok(())
}

#[test]
fn test_identical_videos_have_identical_hashes(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_dir = "target/test_videos";
    fs::create_dir_all(test_dir)?;

    let video1_path = format!("{}/identical1.mp4", test_dir);
    let video2_path = format!("{}/identical2.mp4", test_dir);

    create_test_video(&video1_path, 3, "blue")?;
    fs::copy(&video1_path, &video2_path)?;

    let hash1 = VideoHash::new(Path::new(&video1_path))?;
    let hash2 = VideoHash::new(Path::new(&video2_path))?;

    assert_eq!(hash1.hash, hash2.hash);

    fs::remove_file(&video1_path)?;
    fs::remove_file(&video2_path)?;

    Ok(())
}

#[test]
fn test_similar_videos_have_similar_hashes() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    let test_dir = "target/test_videos";
    fs::create_dir_all(test_dir)?;

    let base_video_path = format!("{}/base_video.mp4", test_dir);
    create_test_video(&base_video_path, 5, "blue")?;

    let concat_list_path = format!("{}/concat_list.txt", test_dir);
    let similar_video_path = format!("{}/similar_video.mp4", test_dir);
    let short_diff_path = format!("{}/short_diff.mp4", test_dir);

    create_test_video(&short_diff_path, 1, "red")?;

    let base_video_abs_path = std::fs::canonicalize(&base_video_path)?;
    let short_diff_abs_path = std::fs::canonicalize(&short_diff_path)?;

    let mut file = fs::File::create(&concat_list_path)?;
    writeln!(file, "file '{}'", base_video_abs_path.to_string_lossy())?;
    writeln!(file, "file '{}'", short_diff_abs_path.to_string_lossy())?;

    let status = Command::new("ffmpeg")
        .args(&[
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            &concat_list_path,
            "-c",
            "copy",
            "-y",
            &similar_video_path,
        ])
        .status()?;

    if !status.success() {
        return Err("Failed to concatenate videos".into());
    }

    let hash1 = VideoHash::new(Path::new(&base_video_path))?;
    let hash2 = VideoHash::new(Path::new(&similar_video_path))?;

    let hamming_distance = hash1
        .hash
        .chars()
        .zip(hash2.hash.chars())
        .filter(|(c1, c2)| c1 != c2)
        .count();

    assert!(
        hamming_distance < 32,
        "Hamming distance was {}, expected less than 32",
        hamming_distance
    );

    fs::remove_file(&base_video_path)?;
    fs::remove_file(&similar_video_path)?;
    fs::remove_file(&short_diff_path)?;
    fs::remove_file(&concat_list_path)?;

    Ok(())
}

#[test]
fn test_hash_consistency() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_dir = "target/test_videos";
    fs::create_dir_all(test_dir)?;

    let test_video = format!("{}/consistency_test.mp4", test_dir);
    create_test_video(&test_video, 3, "green")?;

    let path = Path::new(&test_video);
    let hash1 = VideoHash::new(path)?;
    let hash2 = VideoHash::new(path)?;
    let hash3 = VideoHash::new(path)?;

    assert_eq!(hash1.hash, hash2.hash);
    assert_eq!(hash2.hash, hash3.hash);

    fs::remove_file(path)?;

    Ok(())
}

#[test]
fn test_hamming_distance_and_similarity() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let hash1 = VideoHash {
        hash: "0".repeat(64),
    };
    let hash2 = VideoHash {
        hash: "1".repeat(64),
    };
    let hash3 = VideoHash {
        hash: "0".repeat(32) + &"1".repeat(32),
    };

    assert_eq!(hash1.hamming_distance(&hash1), 0);
    assert_eq!(hash1.hamming_distance(&hash2), 64);
    assert_eq!(hash1.hamming_distance(&hash3), 32);

    assert_eq!(hash1.similarity(&hash1), 100.0);
    assert_eq!(hash1.similarity(&hash2), 0.0);
    assert_eq!(hash1.similarity(&hash3), 50.0);

    Ok(())
}

#[test]
fn test_error_handling_invalid_video() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let test_dir = "target/test_videos";
    fs::create_dir_all(test_dir)?;
    let invalid_path = format!("{}/not_a_video.mp4", test_dir);

    let mut file = fs::File::create(&invalid_path)?;
    file.write_all(b"This is not a video file")?;

    let result = VideoHash::new(Path::new(&invalid_path));
    assert!(result.is_err());

    fs::remove_file(&invalid_path)?;

    Ok(())
}
