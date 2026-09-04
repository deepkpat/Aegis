use axum::http::HeaderName;
use redis::{AsyncCommands, aio::ConnectionManager};

use crate::config::DeduperConfig;

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
pub struct Deduper {
    conn: ConnectionManager,
    key_prefix: String,
    ttl_secs: u64,
    pub header_name: HeaderName,
    pub require_key: bool,
}

pub enum Claim {
    Fresh { redis_key: String },
    Replay { status: u16, body: String },
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
        Some(Self {
            conn,
            key_prefix: cfg.key_prefix.clone(),
            ttl_secs: cfg.ttl_secs,
            header_name,
            require_key: cfg.require_key,
        })
    }

    pub fn redis_key(&self, method: &str, path: &str, client_key: &str) -> String {
        format!("{}{method}:{path}:{client_key}", self.key_prefix)
    }

    pub fn validate_key(key: &str) -> bool {
        !key.trim().is_empty() && key.len() <= 255
    }

    async fn get(&self, redis_key: &str) -> Option<Stored> {
        let mut conn = self.conn.clone();
        let raw: Option<String> = match conn.get(redis_key).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "deduper get failed, fail-open");
                return None;
            }
        };
        serde_json::from_str(&raw?).ok()
    }

    pub async fn claim(&self, method: &str, path: &str, client_key: &str) -> Option<Claim> {
        let redis_key = self.redis_key(method, path, client_key);
        if let Some(stored) = self.get(&redis_key).await {
            match stored {
                Stored::Complete { status, body, .. } => {
                    return Some(Claim::Replay { status, body });
                }
                Stored::Inflight => return Some(Claim::InFlight),
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
            Ok(Some(_)) => Some(Claim::Fresh { redis_key }),
            Ok(None) => match self.get(&redis_key).await {
                Some(Stored::Complete { status, body, .. }) => Some(Claim::Replay { status, body }),
                _ => Some(Claim::InFlight),
            },
            Err(e) => {
                tracing::warn!(error = %e, "deduper claim failed, fail-open");
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
            tracing::warn!(error = %e, "deduper store failed");
        }
    }

    pub async fn release(&self, redis_key: &str) {
        let mut conn = self.conn.clone();
        let res: redis::RedisResult<()> = conn.del(redis_key).await;
        if let Err(e) = res {
            tracing::warn!(error = %e, "deduper release failed");
        }
    }
}
