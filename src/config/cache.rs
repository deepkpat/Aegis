use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    pub enabled: bool,
    pub moka: MokaCacheConfig,
    pub redis: RedisCacheConfig,
    pub invalidation: InvalidationConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvalidationConfig {
    pub enabled: bool,
    pub channel: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MokaCacheConfig {
    pub enabled: bool,
    pub max_capacity: u64,
    pub time_to_live_secs: u64,
    pub time_to_idle_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisCacheConfig {
    pub enabled: bool,
    pub ttl_secs: u64,
    pub key_prefix: String,
}
