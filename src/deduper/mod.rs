use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::http::HeaderName;
use fastbloom::BloomFilter;
use redis::{AsyncCommands, aio::ConnectionManager};
use tokio::sync::RwLock;

use crate::config::{DeduperBloomConfig, DeduperConfig, DeduperRedisConfig};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "lowercase")]
enum Stored {
    Inflight,
    Complete {
        status: u16,
        body: String,
        content_type: String,
    },
}

#[derive(Debug, Clone)]
pub struct DeduperRedis {
    conn: ConnectionManager,
    key_prefix: String,
    ttl_secs: u64,
    inflight_ttl_secs: u64,
}

pub enum RedisClaim {
    Fresh {
        redis_key: String,
    },
    Replay {
        status: u16,
        body: String,
        content_type: String,
    },
    InFlight,
}

impl DeduperRedis {
    fn new(cfg: &DeduperRedisConfig, conn: ConnectionManager) -> Self {
        Self {
            conn,
            key_prefix: cfg.key_prefix.clone(),
            ttl_secs: cfg.ttl_secs,
            // Short lock TTL so a crashed holder stops blocking retries quickly.
            inflight_ttl_secs: cfg.inflight_ttl_secs.clamp(1, cfg.ttl_secs.max(1)),
        }
    }

    #[must_use]
    pub fn redis_key(&self, method: &str, path: &str, client_key: &str) -> String {
        format!("{}{method}:{path}:{client_key}", self.key_prefix)
    }

    async fn get(&self, redis_key: &str) -> Option<Stored> {
        let mut conn = self.conn.clone();
        let raw: Option<String> = match conn.get(redis_key).await {
            Ok(value) => value,
            Err(e) => {
                tracing::warn!(error = %e, "deduper redis get failed, fail-open");
                return None;
            }
        };
        serde_json::from_str(&raw?).ok()
    }

    async fn set_inflight(&self, redis_key: &str) -> redis::RedisResult<Option<String>> {
        let inflight = serde_json::to_string(&Stored::Inflight).unwrap_or_default();
        let mut conn = self.conn.clone();
        redis::cmd("SET")
            .arg(redis_key)
            .arg(inflight)
            .arg("NX")
            .arg("EX")
            .arg(self.inflight_ttl_secs)
            .query_async(&mut conn)
            .await
    }

    async fn resolve_race(&self, redis_key: &str) -> RedisClaim {
        match self.get(redis_key).await {
            Some(Stored::Complete {
                status,
                body,
                content_type,
            }) => RedisClaim::Replay {
                status,
                body,
                content_type,
            },
            _ => RedisClaim::InFlight,
        }
    }

    pub async fn claim(&self, method: &str, path: &str, client_key: &str) -> Option<RedisClaim> {
        let redis_key = self.redis_key(method, path, client_key);
        if let Some(stored) = self.get(&redis_key).await {
            return Some(match stored {
                Stored::Complete {
                    status,
                    body,
                    content_type,
                } => RedisClaim::Replay {
                    status,
                    body,
                    content_type,
                },
                Stored::Inflight => RedisClaim::InFlight,
            });
        }
        self.try_acquire(&redis_key).await
    }

    // Bloom miss means no completed record exists, so skip the GET.
    pub async fn claim_fresh(
        &self,
        method: &str,
        path: &str,
        client_key: &str,
    ) -> Option<RedisClaim> {
        let redis_key = self.redis_key(method, path, client_key);
        self.try_acquire(&redis_key).await
    }

    async fn try_acquire(&self, redis_key: &str) -> Option<RedisClaim> {
        match self.set_inflight(redis_key).await {
            Ok(Some(_)) => Some(RedisClaim::Fresh {
                redis_key: redis_key.to_owned(),
            }),
            Ok(None) => Some(self.resolve_race(redis_key).await),
            Err(e) => {
                tracing::warn!(error = %e, "deduper redis claim failed, fail-open");
                None
            }
        }
    }

    pub async fn complete(&self, redis_key: &str, status: u16, body: &str, content_type: &str) {
        let entry = Stored::Complete {
            status,
            body: body.to_owned(),
            content_type: content_type.to_owned(),
        };
        let Ok(raw) = serde_json::to_string(&entry) else {
            return;
        };
        let mut conn = self.conn.clone();
        let res: redis::RedisResult<()> = conn.set_ex(redis_key, raw, self.ttl_secs).await;
        if let Err(e) = res {
            tracing::warn!(error = %e, "deduper redis store failed");
        }
    }

    pub async fn release(&self, redis_key: &str) {
        let mut conn = self.conn.clone();
        let res: redis::RedisResult<()> = conn.del(redis_key).await;
        if let Err(e) = res {
            tracing::warn!(error = %e, "deduper redis release failed");
        }
    }
}

struct BloomRing {
    buckets: Vec<BloomFilter>,
    current: usize,
    last_rotation: Instant,
}

// Rotating Bloom filter with zero false negatives inside the retention window.
pub struct DeduperBloom {
    inner: Arc<RwLock<BloomRing>>,
    capacity: usize,
    false_positive_rate: f64,
    bucket_ttl: Duration,
}

impl fmt::Debug for DeduperBloom {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeduperBloom")
            .field("capacity", &self.capacity)
            .field("false_positive_rate", &self.false_positive_rate)
            .field("bucket_ttl", &self.bucket_ttl)
            .finish_non_exhaustive()
    }
}

impl DeduperBloom {
    fn new(cfg: &DeduperBloomConfig) -> Self {
        let num_buckets = cfg.buckets.max(1);
        let capacity = usize::try_from(cfg.capacity.max(1)).unwrap_or(usize::MAX);
        // Lower bound keeps with_false_pos from panicking on zero.
        let false_positive_rate = cfg.false_positive_rate.clamp(f64::MIN_POSITIVE, 0.999_999);
        let buckets = (0..num_buckets)
            .map(|_| Self::build_bucket(capacity, false_positive_rate))
            .collect();
        Self {
            inner: Arc::new(RwLock::new(BloomRing {
                buckets,
                current: 0,
                last_rotation: Instant::now(),
            })),
            capacity,
            false_positive_rate,
            bucket_ttl: Duration::from_secs(cfg.bucket_ttl_secs),
        }
    }

    fn build_bucket(capacity: usize, false_positive_rate: f64) -> BloomFilter {
        BloomFilter::with_false_pos(false_positive_rate).expected_items(capacity)
    }

    fn rotate_if_due(&self, ring: &mut BloomRing) {
        if ring.last_rotation.elapsed() >= self.bucket_ttl {
            ring.current = (ring.current + 1) % ring.buckets.len();
            ring.buckets[ring.current] =
                Self::build_bucket(self.capacity, self.false_positive_rate);
            ring.last_rotation = Instant::now();
        }
    }

    // False is definitive inside the retention window.
    pub async fn maybe_contains(&self, key: &str) -> bool {
        let mut ring = self.inner.write().await;
        self.rotate_if_due(&mut ring);
        ring.buckets.iter().any(|b| b.contains(key))
    }

    pub async fn insert(&self, key: &str) {
        let mut ring = self.inner.write().await;
        self.rotate_if_due(&mut ring);
        let current = ring.current;
        ring.buckets[current].insert(key);
    }
}

#[derive(Debug, Clone)]
pub struct Deduper {
    pub bloom: Option<Arc<DeduperBloom>>,
    pub redis: Option<Arc<DeduperRedis>>,
    pub header_name: HeaderName,
    pub require_key: bool,
}

pub enum Claim {
    Fresh {
        redis_key: Option<String>,
        bloom_key: Option<String>,
    },
    Replay {
        status: u16,
        body: String,
        content_type: String,
    },
    InFlight,
}

impl Claim {
    fn from_redis(claim: RedisClaim, bloom_key: Option<String>) -> Self {
        match claim {
            RedisClaim::Fresh { redis_key } => Self::Fresh {
                redis_key: Some(redis_key),
                bloom_key,
            },
            RedisClaim::Replay {
                status,
                body,
                content_type,
            } => Self::Replay {
                status,
                body,
                content_type,
            },
            RedisClaim::InFlight => Self::InFlight,
        }
    }
}

impl Deduper {
    #[must_use]
    pub fn new(cfg: &DeduperConfig, conn: ConnectionManager) -> Option<Self> {
        if !cfg.enabled {
            return None;
        }
        let header_name = cfg
            .header
            .parse()
            .unwrap_or(HeaderName::from_static("idempotency-key"));
        let bloom = cfg
            .bloom
            .enabled
            .then(|| Arc::new(DeduperBloom::new(&cfg.bloom)));
        let redis = cfg
            .redis
            .enabled
            .then(|| Arc::new(DeduperRedis::new(&cfg.redis, conn)));
        if bloom.is_none() && redis.is_none() {
            return None;
        }
        Some(Self {
            bloom,
            redis,
            header_name,
            require_key: cfg.require_key,
        })
    }

    #[must_use]
    pub fn validate_key(key: &str) -> bool {
        !key.trim().is_empty() && key.len() <= 255
    }

    pub async fn claim(&self, method: &str, path: &str, client_key: &str) -> Option<Claim> {
        // Bloom miss skips the Redis read and goes straight to lock acquisition.
        if let Some(bloom) = &self.bloom
            && !bloom.maybe_contains(client_key).await
        {
            tracing::debug!(%client_key, "bloom miss, skipping redis read");
            if let Some(redis) = &self.redis {
                let redis_claim = redis.claim_fresh(method, path, client_key).await?;
                return Some(Claim::from_redis(redis_claim, Some(client_key.to_owned())));
            }
            return Some(Claim::Fresh {
                redis_key: None,
                bloom_key: Some(client_key.to_owned()),
            });
        }

        let redis = self.redis.as_ref()?;
        let redis_claim = redis.claim(method, path, client_key).await?;
        Some(Claim::from_redis(
            redis_claim,
            self.bloom.as_ref().map(|_| client_key.to_owned()),
        ))
    }

    pub async fn complete(
        &self,
        redis_key: Option<&str>,
        bloom_key: Option<&str>,
        status: u16,
        body: &str,
        content_type: &str,
    ) {
        if let (Some(bloom), Some(key)) = (&self.bloom, bloom_key) {
            bloom.insert(key).await;
        }
        if let (Some(redis), Some(key)) = (&self.redis, redis_key) {
            redis.complete(key, status, body, content_type).await;
        }
    }

    pub async fn release(&self, redis_key: Option<&str>) {
        if let (Some(redis), Some(key)) = (&self.redis, redis_key) {
            redis.release(key).await;
        }
    }
}
