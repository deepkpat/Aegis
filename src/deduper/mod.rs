pub mod bloom;
pub mod redis;

pub use bloom::BloomDeduper;
pub use redis::{RedisClaim, RedisDeduper};

use std::sync::Arc;

use ::redis::aio::ConnectionManager;
use axum::http::HeaderName;
use sha2::{Digest, Sha256};

use crate::config::DeduperConfig;

/// Computes a deterministic SHA-256 hash of tuple string slices.
/// Length-prefixed encoding guards against cross-field ambiguity attacks.
pub fn hash_key(parts: &[&str]) -> String {
    let mut h = Sha256::new();
    for p in parts {
        h.update((p.len() as u64).to_le_bytes());
        h.update(p.as_bytes());
    }
    format!("{:x}", h.finalize())
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[derive(Debug, Clone)]
pub struct Deduper {
    pub bloom: Option<Arc<BloomDeduper>>,
    pub redis: Option<Arc<RedisDeduper>>,
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
        body: Option<Vec<u8>>,
        content_type: String,
        req_hash: String,
    },
    InFlight {
        retry_after_secs: u64,
    },
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
                req_hash,
            } => Self::Replay {
                status,
                body,
                content_type,
                req_hash,
            },
            RedisClaim::InFlight { retry_after_secs } => Self::InFlight { retry_after_secs },
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
            .unwrap_or_else(|_| HeaderName::from_static("idempotency-key"));

        let bloom = BloomDeduper::new(&cfg.bloom).map(Arc::new);
        let redis = RedisDeduper::new(&cfg.redis, conn).map(Arc::new);

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

    /// Validates idempotency key format: non-empty, max 255 bytes, visible ASCII only.
    #[must_use]
    pub fn validate_key(key: &str) -> bool {
        !key.is_empty() && key.len() <= 255 && key.bytes().all(|b| (0x21..=0x7E).contains(&b))
    }

    /// Claims an idempotency lock for the given request context.
    ///
    /// The deduplication key is scoped to `(method, path_and_query, client_key)`
    /// before checking the Bloom filter or Redis backend.
    pub async fn claim(
        &self,
        method: &str,
        path_and_query: &str,
        client_key: &str,
    ) -> Option<Claim> {
        // derive canonical scoped hash once for both layers
        let scoped_key = hash_key(&[method, path_and_query, client_key]);

        // check in-memory negative cache (bloom filter)
        if let Some(bloom) = &self.bloom {
            if !bloom.maybe_contains(&scoped_key) {
                tracing::debug!(%client_key, %scoped_key, "bloom miss");

                if let Some(redis) = &self.redis {
                    let redis_claim = redis.claim(&scoped_key).await?;
                    return Some(Claim::from_redis(redis_claim, Some(scoped_key)));
                }

                return Some(Claim::Fresh {
                    redis_key: None,
                    bloom_key: Some(scoped_key),
                });
            }
        }

        // bloom hit or bloom disabled: query authoritative redis backend
        if let Some(redis) = &self.redis {
            let redis_claim = redis.claim(&scoped_key).await?;
            Some(Claim::from_redis(
                redis_claim,
                self.bloom.as_ref().map(|_| scoped_key),
            ))
        } else {
            // bloom hit without redis: bloom filters record presence, not payloads.
            // fail open to avoid dropping incoming traffic.
            tracing::warn!(
                %client_key,
                "bloom filter hit without active redis backend; failing open to fresh claim"
            );
            Some(Claim::Fresh {
                redis_key: None,
                bloom_key: Some(scoped_key),
            })
        }
    }

    pub async fn complete(
        &self,
        redis_key: Option<&str>,
        bloom_key: Option<&str>,
        status: u16,
        body: Option<&[u8]>,
        content_type: &str,
        req_hash: &str,
    ) {
        if let (Some(bloom), Some(key)) = (&self.bloom, bloom_key) {
            bloom.insert(key);
        }
        if let (Some(redis), Some(key)) = (&self.redis, redis_key) {
            redis
                .complete(key, status, body, content_type, req_hash)
                .await;
        }
    }

    pub async fn release(&self, redis_key: Option<&str>) {
        if let (Some(redis), Some(key)) = (&self.redis, redis_key) {
            redis.release(key).await;
        }
    }
}
