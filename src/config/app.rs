use anyhow::Context;
use serde::{Deserialize, Serialize};

use super::database::DatabaseConfig;
use super::server::ServerConfig;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
}

impl AppConfig {
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config {path}"))?;
        serde_yaml::from_str(&content).with_context(|| format!("failed to parse config {path}"))
    }
}
