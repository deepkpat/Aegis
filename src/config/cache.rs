use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CacheConfig {
    pub enabled: bool,
    pub moka: MokaConfig,
    pub redis: RedisCacheConfig,
    pub invalidation: InvalidationConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InvalidationConfig {
    pub enabled: bool,
    pub channel: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MokaConfig {
    pub enabled: bool,
    pub max_capacity: u64,
    pub time_to_live_secs: u64,
    pub time_to_idle_secs: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RedisCacheConfig {
    pub enabled: bool,
    pub ttl_secs: u64,
    pub key_prefix: String,
}
