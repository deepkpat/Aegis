use anyhow::Context;
use serde::{Deserialize, Serialize};

use super::cache::CacheConfig;
use super::database::PostgresConfig;
use super::redis::RedisConfig;
use super::server::ServerConfig;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub postgres: PostgresConfig,
    pub redis: RedisConfig,
    #[serde(default)]
    pub cache: CacheConfig,
}

impl AppConfig {
    pub fn from_file(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        serde_yaml::from_str(&content)
            .with_context(|| format!("failed to parse config {}", path.display()))
    }
}
