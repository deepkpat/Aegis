use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{PgPool, postgres::PgPoolOptions};

use crate::config::PostgresConfig;

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct Record {
    pub id: String,
    pub payload: serde_json::Value,
    pub version: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub async fn create_pool(cfg: &PostgresConfig) -> anyhow::Result<PgPool> {
    Ok(PgPoolOptions::new()
        .max_connections(cfg.max_connections)
        .acquire_timeout(Duration::from_secs(cfg.acquire_timeout_secs))
        .connect(&cfg.url)
        .await?)
}
