use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use redis::{AsyncCommands, Script, aio::ConnectionManager};
use serde::{Deserialize, Serialize};

use crate::config::deduper::RedisDeduperConfig;

pub const MAX_STORED_BODY_BYTES: usize = 64 * 1024;
static INFLIGHT_JSON: &str = r#"{"state":"inflight"}"#;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Stored {
    Inflight,
    Complete {
        status: u16,
        content_type: String,
        req_hash: String,
        body_b64: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct RedisDeduper {
    conn: ConnectionManager,
    key_prefix: String,
    ttl_secs: u64,
    inflight_ttl_secs: u64,
    claim_script: Script,
}

#[derive(Debug, Clone)]
pub enum RedisClaim {
    Fresh {
        redis_key: String,
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

impl RedisDeduper {
    pub fn new(cfg: &RedisDeduperConfig, conn: ConnectionManager) -> Option<Self> {
        if !cfg.enabled {
            return None;
        }

        let claim_script = Script::new(
            r#"
            local res = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2])
            if res then
                return {'FRESH'}
            else
                local val = redis.call('GET', KEYS[1])
                local pttl = redis.call('PTTL', KEYS[1])
                return {'EXISTS', val, pttl}
            end
            "#,
        );

        Some(Self {
            conn,
            key_prefix: cfg.key_prefix.clone(),
            ttl_secs: cfg.ttl_secs,
            inflight_ttl_secs: cfg.inflight_ttl_secs.clamp(1, cfg.ttl_secs.max(1)),
            claim_script,
        })
    }

    pub fn redis_key(&self, scoped_key: &str) -> String {
        format!("{}{}", self.key_prefix, scoped_key)
    }

    pub async fn claim(&self, scoped_key: &str) -> Option<RedisClaim> {
        let redis_key = self.redis_key(scoped_key);
        let mut conn = self.conn.clone();

        // single rtt atomic operation via lua script
        let eval_res: redis::RedisResult<(String, Option<String>, Option<i64>)> = self
            .claim_script
            .key(&redis_key)
            .arg(INFLIGHT_JSON)
            .arg(self.inflight_ttl_secs)
            .invoke_async(&mut conn)
            .await;

        let (tag, raw_val, pttl) = match eval_res {
            Ok(res) => res,
            Err(e) => {
                tracing::warn!(error = %e, "deduper redis claim failed, failing open");
                return None;
            }
        };

        if tag == "FRESH" {
            return Some(RedisClaim::Fresh { redis_key });
        }

        // lock already existed, parse the retrieved record
        match raw_val
            .as_deref()
            .and_then(|s| serde_json::from_str::<Stored>(s).ok())
        {
            Some(Stored::Complete {
                status,
                body_b64,
                content_type,
                req_hash,
            }) => Some(RedisClaim::Replay {
                status,
                body: body_b64.and_then(|b| BASE64.decode(b).ok()),
                content_type,
                req_hash,
            }),
            Some(Stored::Inflight) => {
                let pttl_ms = pttl.unwrap_or(-1);
                let retry_after_secs = if pttl_ms > 0 {
                    (pttl_ms as u64).div_ceil(1000).max(1)
                } else {
                    self.inflight_ttl_secs.max(1)
                };
                Some(RedisClaim::InFlight { retry_after_secs })
            }
            None => {
                // record corrupt or expired between SET and GET inside lua (rare).
                // do not issue an unconditional DEL here to avoid race conditions against third-party lock holders.
                tracing::warn!(key = %redis_key, "deduper record missing or corrupt on conflict");
                None
            }
        }
    }

    pub async fn complete(
        &self,
        redis_key: &str,
        status: u16,
        body: Option<&[u8]>,
        content_type: &str,
        req_hash: &str,
    ) {
        let body_b64 = body
            .filter(|b| b.len() <= MAX_STORED_BODY_BYTES)
            .map(|b| BASE64.encode(b));

        let entry = Stored::Complete {
            status,
            body_b64,
            content_type: content_type.to_owned(),
            req_hash: req_hash.to_owned(),
        };

        let Ok(raw) = serde_json::to_string(&entry) else {
            return;
        };

        let mut conn = self.conn.clone();
        if let Err(e) = conn.set_ex::<_, _, ()>(redis_key, raw, self.ttl_secs).await {
            tracing::warn!(error = %e, "deduper redis store failed");
        }
    }

    pub async fn release(&self, redis_key: &str) {
        let mut conn = self.conn.clone();
        if let Err(e) = conn.del::<_, ()>(redis_key).await {
            tracing::warn!(error = %e, "deduper redis release failed");
        }
    }
}
