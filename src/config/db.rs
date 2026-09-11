use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    pub url: String,
    pub max_connections: u32,
    pub acquire_timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisConfig {
    // durable instance: idempotency, rate-limits, pub/sub. use noeviction.
    pub url: String,
    #[serde(default)]
    // evictable cache. falls back to url when absent.
    pub cache_url: Option<String>,
}
