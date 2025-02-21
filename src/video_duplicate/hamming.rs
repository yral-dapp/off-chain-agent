#[derive(Debug, Clone, PartialEq)]
pub struct VideoSignature {
    frame_hashes: Vec<u64>,
}

impl VideoSignature {
    pub fn new(hashes: Vec<u64>) -> Self {
        Self { frame_hashes: hashes }
    }

    pub fn similarity_score(&self, other: &VideoSignature) -> u32 {
        self.frame_hashes.iter()
            .zip(other.frame_hashes.iter())
            .map(|(&h1, &h2)| Self::hamming_distance(h1, h2))
            .min()
            .unwrap_or(u32::MAX)
    }

    pub fn is_similar(&self, other: &VideoSignature, threshold: u32) -> bool {
        self.similarity_score(other) <= threshold
    }

    #[inline]
    fn hamming_distance(hash1: u64, hash2: u64) -> u32 {
        (hash1 ^ hash2).count_ones()
    }
}