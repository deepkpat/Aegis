use std::sync::atomic::AtomicU64;

use redis::{Script, aio::ConnectionManager};

use crate::config::{
    SlindowConfig,
    limiters::{TrustedNet, parse_trusted_proxies},
};

const SLINDOW_SCRIPT: &str = include_str!("../../scripts/slindow.lua");

static MEMBER_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub struct SlindowLimiter {
    conn: ConnectionManager,
    key_prefix: String,
    limit: u64,
    window_ms: u64,
    script: Script,
    node_id: String,
    trusted_proxies: Vec<TrustedNet>,
}

#[derive(Debug, Clone, Copy)]
pub struct SlindowDecision {
    pub allowed: bool,
    pub count: u64,
    pub limit: u64,
    pub retry_after_secs: u64,
}

impl SlindowLimiter {
    #[must_use]
    pub fn new(cfg: &SlindowConfig, conn: ConnectionManager) -> Option<Self> {
        if !cfg.enabled {
            return None;
        }

        let script = std::fs::read_to_string(&cfg.script_path).unwrap_or_else(|e| {
            tracing::warn!(path = %cfg.script_path.display(), error = %e, "using embedded slindow script");
            SLINDOW_SCRIPT.to_owned()
        });

        let trusted_proxies = parse_trusted_proxies(&cfg.trusted_proxies).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "ignoring invalid slindow.trusted_proxies");
            Vec::new()
        });

        Some(Self {
            conn,
            key_prefix: cfg.key_prefix.clone(),
            limit: cfg.requests_per_window,
            window_ms: cfg.window_secs * 1000,
            script: Script::new(&script),
            node_id: uuid::Uuid::new_v4().as_simple().to_string(),
            trusted_proxies,
        })
    }

    pub fn trusted_proxies(&self) -> &[TrustedNet] {
        &self.trusted_proxies
    }

    fn key(&self, ip: &str) -> String {
        format!("{}{ip}", self.key_prefix)
    }

    fn fail_open(&self) -> SlindowDecision {
        SlindowDecision {
            allowed: true,
            count: 0,
            limit: self.limit,
            retry_after_secs: 0,
        }
    }

    pub async fn check(&self, ip: &str) -> SlindowDecision {
        let seq = MEMBER_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let member = format!("{}:{seq}", self.node_id);
        let mut conn = self.conn.clone();

        let res: redis::RedisResult<(i64, i64, i64)> = self
            .script
            .key(self.key(ip))
            .arg(self.window_ms)
            .arg(self.limit)
            .arg(member)
            .invoke_async(&mut conn)
            .await;

        match res {
            Ok((allowed, count, ttl_ms)) => SlindowDecision {
                allowed: allowed == 1,
                count: count.cast_unsigned(),
                limit: self.limit,
                retry_after_secs: ttl_ms.max(0).cast_unsigned().div_ceil(1000),
            },
            Err(e) => {
                tracing::warn!(error = %e, "sliding-window check failed, fail-open");
                self.fail_open()
            }
        }
    }
}
