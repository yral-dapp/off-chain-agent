use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, RwLock};

use mih_rs::Index;
use uuid::Uuid;

use super::videohash::VideoHash;

// Helper function to convert binary string to u64
fn binary_string_to_u64(binary_str: &str) -> Result<u64, Box<dyn Error + Send + Sync>> {
    if binary_str.len() != 64 {
        return Err(format!("Binary string must be 64 bits, got {}", binary_str.len()).into());
    }
    
    let mut result = 0u64;
    for (i, ch) in binary_str.chars().enumerate() {
        match ch {
            '1' => result |= 1 << (63 - i),
            '0' => {},
            _ => return Err(format!("Invalid character in binary string: {}", ch).into()),
        }
    }
    
    Ok(result)
}

/// VideoHashIndex provides efficient nearest neighbor search functionality
/// for video hashes using Multi-Index Hashing (MIH).
pub struct VideoHashIndex {
    /// Mapping from UUID to u64 hash value
    hashes: RwLock<HashMap<Uuid, u64>>,
    
    /// The MIH index for fast nearest neighbor lookup (rebuilt on changes)
    index: RwLock<Option<Index<u64>>>,
}

impl VideoHashIndex {
    /// Create a new, empty VideoHashIndex
    pub fn new() -> Self {
        Self {
            hashes: RwLock::new(HashMap::new()),
            index: RwLock::new(None),
        }
    }
    
    /// Add a video hash to the index
    pub fn add(&self, uuid: Uuid, hash: &VideoHash) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Convert the hash string to u64
        let hash_value = binary_string_to_u64(&hash.hash)?;
        
        // Add or update the hash in our collection
        let mut hashes = self.hashes.write().unwrap();
        hashes.insert(uuid, hash_value);
        
        // Mark the index as needing rebuild
        let mut index = self.index.write().unwrap();
        *index = None;
        
        Ok(())
    }
    
    /// Ensure the index is built
    fn ensure_index_built(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut index_lock = self.index.write().unwrap();
        
        if index_lock.is_none() {
            let hashes = self.hashes.read().unwrap();
            if hashes.is_empty() {
                *index_lock = None;
                return Ok(());
            }
            
            // Extract hash values for the index
            let codes: Vec<u64> = hashes.values().cloned().collect();
            *index_lock = Some(Index::new(codes)?);
        }
        
        Ok(())
    }
    
    /// Find the top-1 nearest neighbor for a given hash
    pub fn find_nearest_neighbor(&self, hash: &VideoHash) -> Result<Option<(Uuid, u32)>, Box<dyn Error + Send + Sync>> {
        // Convert the hash string to u64
        let hash_value = binary_string_to_u64(&hash.hash)?;
        
        // Ensure the index is built
        self.ensure_index_built()?;
        
        let index_lock = self.index.read().unwrap();
        if index_lock.is_none() {
            return Ok(None);
        }
        
        let index = index_lock.as_ref().unwrap();
        let hashes = self.hashes.read().unwrap();
        
        // Get list of UUIDs in same order as the index was built
        let uuids: Vec<Uuid> = hashes.keys().cloned().collect();
        
        // Use top-1 search
        let mut searcher = index.topk_searcher();
        let answers = searcher.run(hash_value, 1);
        
        if answers.is_empty() {
            return Ok(None);
        }
        
        // Convert index to usize for array access
        let idx = answers[0] as usize;
        if idx >= uuids.len() {
            return Err("Index inconsistency: invalid vector index".into());
        }
        
        let uuid = uuids[idx];
        let stored_hash = *hashes.get(&uuid).unwrap();
        let hamming_dist = (hash_value ^ stored_hash).count_ones();
        
        Ok(Some((uuid, hamming_dist)))
    }
    
    /// Find all video hashes within a given Hamming distance threshold
    pub fn find_within_distance(
        &self, 
        hash: &VideoHash, 
        max_distance: u32
    ) -> Result<Vec<(Uuid, u32)>, Box<dyn Error + Send + Sync>> {
        // Convert the hash string to u64
        let hash_value = binary_string_to_u64(&hash.hash)?;
        
        // Ensure the index is built
        self.ensure_index_built()?;
        
        let index_lock = self.index.read().unwrap();
        if index_lock.is_none() {
            return Ok(Vec::new());
        }
        
        let index = index_lock.as_ref().unwrap();
        let hashes = self.hashes.read().unwrap();
        
        // Get list of UUIDs in same order as the index was built
        let uuids: Vec<Uuid> = hashes.keys().cloned().collect();
        
        // Use range search
        let mut searcher = index.range_searcher();
        let answers = searcher.run(hash_value, max_distance as usize);
        
        let mut neighbors = Vec::new();
        for idx in answers {
            // Convert idx to usize for array access
            let idx_usize = *idx as usize;
            if idx_usize < uuids.len() {
                let uuid = uuids[idx_usize];
                let stored_hash = *hashes.get(&uuid).unwrap();
                let hamming_dist = (hash_value ^ stored_hash).count_ones();
                neighbors.push((uuid, hamming_dist));
            }
        }
        
        // Sort by distance (closest first)
        neighbors.sort_by_key(|&(_, dist)| dist);
        
        Ok(neighbors)
    }
    
    /// Remove a video hash from the index
    pub fn remove(&self, uuid: &Uuid) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let mut hashes = self.hashes.write().unwrap();
        let removed = hashes.remove(uuid).is_some();
        
        if removed {
            // Mark index as needing rebuild
            let mut index = self.index.write().unwrap();
            *index = None;
        }
        
        Ok(removed)
    }
    
    /// Get the number of hashes in the index
    pub fn len(&self) -> usize {
        self.hashes.read().unwrap().len()
    }
    
    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    
    /// Batch add multiple video hashes to the index
    pub fn batch_add(
        &self, 
        entries: &[(Uuid, VideoHash)]
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Convert all hashes to u64
        let mut hash_values = Vec::with_capacity(entries.len());
        for (uuid, hash) in entries {
            let hash_value = binary_string_to_u64(&hash.hash)?;
            hash_values.push((*uuid, hash_value));
        }
        
        // Add all hashes at once
        let mut hashes = self.hashes.write().unwrap();
        for (uuid, hash_value) in hash_values {
            hashes.insert(uuid, hash_value);
        }
        
        // Mark index as needing rebuild
        let mut index = self.index.write().unwrap();
        *index = None;
        
        Ok(())
    }
    
    /// Find potential duplicates for a batch of videos
    pub fn find_duplicates(
        &self, 
        hashes: &[(Uuid, VideoHash)], 
        threshold: f64
    ) -> Result<HashMap<Uuid, Vec<(Uuid, f64)>>, Box<dyn Error + Send + Sync>> {
        let mut results = HashMap::new();
        
        // Convert similarity threshold to Hamming distance threshold
        let max_hamming_distance = ((1.0 - (threshold / 100.0)) * 64.0) as u32;
        
        for (uuid, hash) in hashes {
            let similar_videos = self.find_within_distance(hash, max_hamming_distance)?;
            
            // Convert results to similarity percentage and filter out the query video itself
            let similar_with_similarity: Vec<(Uuid, f64)> = similar_videos
                .into_iter()
                .filter(|(id, _)| id != uuid)
                .map(|(id, distance)| {
                    let similarity = 100.0 * (64.0 - distance as f64) / 64.0;
                    (id, similarity)
                })
                .collect();
            
            if !similar_with_similarity.is_empty() {
                results.insert(*uuid, similar_with_similarity);
            }
        }
        
        Ok(results)
    }
    
    /// Clear the index
    pub fn clear(&self) {
        let mut hashes = self.hashes.write().unwrap();
        hashes.clear();
        
        let mut index = self.index.write().unwrap();
        *index = None;
    }
}

/// Create a shared, thread-safe instance of VideoHashIndex
pub fn create_shared_index() -> Arc<VideoHashIndex> {
    Arc::new(VideoHashIndex::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_binary_string_to_u64() {
        // All ones
        let all_ones = "1".repeat(64);
        assert_eq!(binary_string_to_u64(&all_ones).unwrap(), u64::MAX);
        
        // All zeros
        let all_zeros = "0".repeat(64);
        assert_eq!(binary_string_to_u64(&all_zeros).unwrap(), 0);
        
        // Mixed pattern
        let mixed = "1010".repeat(16);
        let expected = 0xAAAAAAAAAAAAAAAAu64;
        assert_eq!(binary_string_to_u64(&mixed).unwrap(), expected);
    }
    
    #[test]
    fn test_add_and_find() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let index = VideoHashIndex::new();
        
        // Add some test hashes
        let hash1 = VideoHash { hash: "0".repeat(64) };
        let hash2 = VideoHash { hash: "1".repeat(64) };
        let hash3 = VideoHash { hash: "0".repeat(32) + &"1".repeat(32) };
        
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let uuid3 = Uuid::new_v4();
        
        index.add(uuid1, &hash1)?;
        index.add(uuid2, &hash2)?;
        index.add(uuid3, &hash3)?;
        
        // Find nearest to a hash (should be itself)
        let result = index.find_nearest_neighbor(&hash1)?;
        assert!(result.is_some());
        let (found_uuid, distance) = result.unwrap();
        assert_eq!(found_uuid, uuid1);
        assert_eq!(distance, 0);
        
        // Find nearest to a similar hash
        let query = VideoHash { hash: "0".repeat(60) + &"1".repeat(4) };
        let result = index.find_nearest_neighbor(&query)?;
        assert!(result.is_some());
        let (found_uuid, distance) = result.unwrap();
        assert_eq!(found_uuid, uuid1);
        assert_eq!(distance, 4);
        
        Ok(())
    }
}



// Example API service implementation
// async fn find_similar_videos(video_path: PathBuf, shared_index: Arc<VideoHashIndex>) -> Result<Vec<VideoMatch>, Error> {
//     // Step 1: Generate hash for the uploaded video
//     let query_hash = VideoHash::new(&video_path)?;
    
//     // Step 2: Find similar videos (with 85% similarity threshold)
//     let max_distance = ((1.0 - (85.0 / 100.0)) * 64.0) as u32;
//     let similar_videos = shared_index.find_within_distance(&query_hash, max_distance)?;
    
//     // Step 3: Convert to response format
//     let matches = similar_videos
//         .into_iter()
//         .map(|(uuid, distance)| {
//             let similarity = 100.0 * (64.0 - distance as f64) / 64.0;
//             VideoMatch {
//                 uuid,
//                 similarity_percentage: similarity,
//                 is_duplicate: similarity >= 85.0,
//             }
//         })
//         .collect();
    
//     Ok(matches)
// }