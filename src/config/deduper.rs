use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeduperConfig {
    pub enabled: bool,
    pub header: String,
    pub require_key: bool,
    pub bloom: BloomDeduperConfig,
    pub redis: RedisDeduperConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomDeduperConfig {
    pub enabled: bool,
    pub capacity: u64,
    pub false_positive_rate: f64,
    pub buckets: usize,
    pub bucket_ttl_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisDeduperConfig {
    pub enabled: bool,
    pub key_prefix: String,
    pub ttl_secs: u64,
    pub inflight_ttl_secs: u64,
}
