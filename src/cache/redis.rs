use redis::{AsyncCommands, aio::ConnectionManager};

use crate::config::RedisCacheConfig;
use crate::db::Record;

#[derive(Debug, Clone)]
pub struct RedisCache {
    conn: ConnectionManager,
    key_prefix: String,
    ttl_secs: u64,
}

impl RedisCache {
    pub fn new(cfg: &RedisCacheConfig, conn: ConnectionManager) -> Option<Self> {
        if !cfg.enabled {
            return None;
        }
        Some(Self {
            conn,
            key_prefix: cfg.key_prefix.clone(),
            ttl_secs: cfg.ttl_secs,
        })
    }

    fn key(&self, id: &str) -> String {
        format!("{}{}", self.key_prefix, id)
    }

    pub async fn get(&self, id: &str) -> Option<Record> {
        let key = self.key(id);
        let mut conn = self.conn.clone();
        let raw: Option<String> = conn.get(key).await.ok()?;
        let s = raw?;
        serde_json::from_str(&s).ok()
    }

    pub async fn set(&self, record: &Record) {
        let key = self.key(&record.id);
        let Ok(value) = serde_json::to_string(record) else {
            return;
        };
        let mut conn = self.conn.clone();
        let res: redis::RedisResult<()> = conn.set_ex(key, value, self.ttl_secs).await;
        if let Err(e) = res {
            tracing::warn!(error = %e, "redis cache set failed");
        }
    }

    pub async fn invalidate(&self, id: &str) {
        let key = self.key(id);
        let mut conn = self.conn.clone();
        let res: redis::RedisResult<()> = conn.del(key).await;
        if let Err(e) = res {
            tracing::warn!(error = %e, "redis cache invalidate failed");
        }
    }
}
