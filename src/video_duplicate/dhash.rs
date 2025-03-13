use image::{imageops::FilterType, DynamicImage, GenericImageView};

const HASH_SIZE: u32 = 8;
const WIDTH: u32 = HASH_SIZE + 1;
const HEIGHT: u32 = HASH_SIZE;

pub struct DHash;

impl DHash {
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
}
