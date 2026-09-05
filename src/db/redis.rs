use redis::{Client, aio::ConnectionManager};

use crate::config::RedisConfig;

pub async fn create_connection_manager(cfg: &RedisConfig) -> anyhow::Result<ConnectionManager> {
    connect(cfg.url.as_str()).await
}

/// Cache L2 connection, falls back to the shared url in single-instance mode.
pub async fn create_cache_connection_manager(
    cfg: &RedisConfig,
) -> anyhow::Result<ConnectionManager> {
    let url = cfg.cache_url.as_deref().unwrap_or(cfg.url.as_str());
    connect(url).await
}

async fn connect(url: &str) -> anyhow::Result<ConnectionManager> {
    let client = Client::open(url)?;
    Ok(ConnectionManager::new(client).await?)
}
