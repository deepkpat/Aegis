use redis::{Client, aio::ConnectionManager};

use crate::config::RedisConfig;

pub async fn create_connection_manager(cfg: &RedisConfig) -> anyhow::Result<ConnectionManager> {
    let client = Client::open(cfg.url.as_str())?;
    Ok(ConnectionManager::new(client).await?)
}
