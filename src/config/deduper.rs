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
    /// Lifetime of the in-flight lock acquired by `SET NX` (`note.md` §1.1).
    /// Must comfortably exceed the worst-case request latency
    /// (`server.request_timeout_secs` plus a safety margin) but be orders of
    /// magnitude shorter than `ttl_secs`, so a crashed holder stops
    /// wedging retries with `409` after seconds, not a full replay window.
    pub inflight_ttl_secs: u64,
}
