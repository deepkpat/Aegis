use std::time::Duration;

use moka::future::Cache;

use crate::config::MokaConfig;
use crate::db::Record;

#[derive(Debug, Clone)]
pub struct MokaCache {
    inner: Cache<String, Record>,
}

impl MokaCache {
    pub fn new(cfg: &MokaConfig) -> Option<Self> {
        if !cfg.enabled {
            return None;
        }
        let inner = Cache::builder()
            .max_capacity(cfg.max_capacity)
            .time_to_live(Duration::from_secs(cfg.time_to_live_secs))
            .time_to_idle(Duration::from_secs(cfg.time_to_idle_secs))
            .build();
        Some(Self { inner })
    }

    pub async fn get(&self, id: &str) -> Option<Record> {
        self.inner.get(id).await
    }

    pub async fn insert(&self, record: &Record) {
        self.inner.insert(record.id.clone(), record.clone()).await;
    }

    pub async fn invalidate(&self, id: &str) {
        self.inner.invalidate(id).await;
    }
}
