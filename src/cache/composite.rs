use redis::aio::ConnectionManager;

use crate::config::CacheConfig;
use crate::db::Record;

use super::moka::MokaCache;
use super::redis::RedisCache;

#[derive(Debug, Clone)]
pub struct CompositeCache {
    moka: Option<MokaCache>,
    redis: Option<RedisCache>,
}

impl CompositeCache {
    #[must_use]
    pub fn new(cfg: &CacheConfig, conn: ConnectionManager) -> Option<Self> {
        if !cfg.enabled {
            return None;
        }
        let moka = MokaCache::new(&cfg.moka);
        let redis = RedisCache::new(&cfg.redis, conn);
        if moka.is_none() && redis.is_none() {
            return None;
        }
        Some(Self { moka, redis })
    }

    pub async fn get(&self, id: &str) -> Option<Record> {
        if let Some(moka) = &self.moka
            && let Some(rec) = moka.get(id).await
        {
            return Some(rec);
        }
        if let Some(redis) = &self.redis
            && let Some(rec) = redis.get(id).await
        {
            if let Some(moka) = &self.moka {
                moka.insert(&rec).await;
            }
            return Some(rec);
        }
        None
    }

    pub async fn insert(&self, record: &Record) {
        if let Some(moka) = &self.moka {
            moka.insert(record).await;
        }
        if let Some(redis) = &self.redis {
            redis.set(record).await;
        }
    }

    pub async fn invalidate(&self, id: &str) {
        if let Some(moka) = &self.moka {
            moka.invalidate(id).await;
        }
        if let Some(redis) = &self.redis {
            redis.invalidate(id).await;
        }
    }
}
