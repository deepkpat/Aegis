use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct CacheConfig {
    #[serde(default)]
    pub moka: MokaConfig,
    #[serde(default)]
    pub redis: RedisCacheConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MokaConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_moka_capacity")]
    pub max_capacity: u64,
    #[serde(default = "default_moka_ttl")]
    pub time_to_live_secs: u64,
    #[serde(default = "default_moka_tti")]
    pub time_to_idle_secs: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RedisCacheConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_redis_ttl")]
    pub ttl_secs: u64,
    #[serde(default = "default_key_prefix")]
    pub key_prefix: String,
}

fn default_true() -> bool {
    true
}

fn default_moka_capacity() -> u64 {
    10_000
}

fn default_moka_ttl() -> u64 {
    300
}

fn default_moka_tti() -> u64 {
    60
}

fn default_redis_ttl() -> u64 {
    600
}

fn default_key_prefix() -> String {
    "aegis:records:".into()
}

impl Default for MokaConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_capacity: default_moka_capacity(),
            time_to_live_secs: default_moka_ttl(),
            time_to_idle_secs: default_moka_tti(),
        }
    }
}

impl Default for RedisCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ttl_secs: default_redis_ttl(),
            key_prefix: default_key_prefix(),
        }
    }
}
