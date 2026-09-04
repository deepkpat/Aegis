use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeduperConfig {
    pub enabled: bool,
    pub header: String,
    pub require_key: bool,
    pub bloom: DeduperBloomConfig,
    pub redis: DeduperRedisConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeduperBloomConfig {
    pub enabled: bool,
    pub capacity: u64,
    pub false_positive_rate: f64,
    pub buckets: usize,
    pub bucket_ttl_secs: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeduperRedisConfig {
    pub enabled: bool,
    pub key_prefix: String,
    pub ttl_secs: u64,
}
