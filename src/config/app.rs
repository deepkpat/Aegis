use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::config::CacheConfig;

use super::{
    CoalescerConfig, DeduperConfig, PostgresConfig, RedisConfig, ServerConfig, SlindowConfig,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub postgres: PostgresConfig,
    pub redis: RedisConfig,
    pub slindow: SlindowConfig,
    pub deduper: DeduperConfig,
    pub coalescer: CoalescerConfig,
    pub cache: CacheConfig,
}

impl AppConfig {
    #[must_use]
    pub fn from_file(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        serde_yaml::from_str(&content)
            .with_context(|| format!("failed to parse config {}", path.display()))
    }
}
