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

// ---------------------------------------------------------------------------
// Redis-backed claim / replay store
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct DeduperRedis {
    conn: ConnectionManager,
    key_prefix: String,
    ttl_secs: u64,
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
        }
    }

    #[must_use]
    pub fn redis_key(&self, method: &str, path: &str, client_key: &str) -> String {
        format!("{}{method}:{path}:{client_key}", self.key_prefix)
    }

    async fn get(&self, redis_key: &str) -> Option<Stored> {
        let mut conn = self.conn.clone();
        let raw: Option<String> = match conn.get(redis_key).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "deduper redis get failed, fail-open");
                return None;
            }
        };
        serde_json::from_str(&raw?).ok()
    }

    pub async fn claim(&self, method: &str, path: &str, client_key: &str) -> Option<RedisClaim> {
        let redis_key = self.redis_key(method, path, client_key);
        if let Some(stored) = self.get(&redis_key).await {
            match stored {
                Stored::Complete {
                    status,
                    body,
                    content_type,
                } => {
                    return Some(RedisClaim::Replay {
                        status,
                        body,
                        content_type,
                    });
                }
                Stored::Inflight => return Some(RedisClaim::InFlight),
            }
        }
        let inflight = serde_json::to_string(&Stored::Inflight).unwrap_or_default();
        let mut conn = self.conn.clone();
        let set: redis::RedisResult<Option<String>> = redis::cmd("SET")
            .arg(&redis_key)
            .arg(inflight)
            .arg("NX")
            .arg("EX")
            .arg(self.ttl_secs)
            .query_async(&mut conn)
            .await;
        match set {
            Ok(Some(_)) => Some(RedisClaim::Fresh { redis_key }),
            Ok(None) => match self.get(&redis_key).await {
                Some(Stored::Complete {
                    status,
                    body,
                    content_type,
                }) => Some(RedisClaim::Replay {
                    status,
                    body,
                    content_type,
                }),
                _ => Some(RedisClaim::InFlight),
            },
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

// ---------------------------------------------------------------------------
// In-memory rotating Bloom filter gate
// ---------------------------------------------------------------------------

struct BloomRing {
    buckets: Vec<BloomFilter>,
    current: usize,
    last_rotation: Instant,
}

/// Multi-bucket rotating Bloom filter. A key inserted at time `t` is
/// retained for `buckets * bucket_ttl_secs`, which must be >= the Redis
/// idempotency TTL, guaranteeing zero false negatives within the window.
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
        // `with_false_pos` panics on fp == 0, so clamp to a strict positive
        // lower bound; values near 1 are meaningless, clamp those too.
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

    /// `BloomFilter::with_false_pos(fp).expected_items(n)` — the builder is
    /// consumed by `expected_items`, which also optimizes the hash count for
    /// the target false-positive rate.
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

    /// Returns `true` if the key may have been seen. A `false` answer is
    /// definitive (zero false negatives within the retention window).
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

// ---------------------------------------------------------------------------
// Composite deduper
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Deduper {
    pub bloom: Option<Arc<DeduperBloom>>,
    pub redis: Option<Arc<DeduperRedis>>,
    pub header_name: HeaderName,
    pub require_key: bool,
}

pub enum Claim {
    /// First execution. `redis_key` is `Some` when a Redis lock was
    /// acquired and must be completed/released; `bloom_key` is `Some`
    /// when the key must be recorded in the Bloom filter on success.
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
        // Bloom gate: a definitive miss means the key cannot be a replay of
        // a completed request, so we skip the Redis read and go straight to
        // lock acquisition. Concurrent in-flight duplicates are still caught
        // by the SET NX below.
        if let Some(bloom) = &self.bloom
            && !bloom.maybe_contains(client_key).await
        {
            tracing::debug!(%client_key, "bloom miss, skipping redis read");
            if let Some(redis) = &self.redis {
                return match redis.claim(method, path, client_key).await? {
                    RedisClaim::Fresh { redis_key } => Some(Claim::Fresh {
                        redis_key: Some(redis_key),
                        bloom_key: Some(client_key.to_owned()),
                    }),
                    RedisClaim::Replay {
                        status,
                        body,
                        content_type,
                    } => Some(Claim::Replay {
                        status,
                        body,
                        content_type,
                    }),
                    RedisClaim::InFlight => Some(Claim::InFlight),
                };
            }
            return Some(Claim::Fresh {
                redis_key: None,
                bloom_key: Some(client_key.to_owned()),
            });
        }

        let redis = self.redis.as_ref()?;
        match redis.claim(method, path, client_key).await? {
            RedisClaim::Fresh { redis_key } => Some(Claim::Fresh {
                redis_key: Some(redis_key),
                bloom_key: self.bloom.as_ref().map(|_| client_key.to_owned()),
            }),
            RedisClaim::Replay {
                status,
                body,
                content_type,
            } => Some(Claim::Replay {
                status,
                body,
                content_type,
            }),
            RedisClaim::InFlight => Some(Claim::InFlight),
        }
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
