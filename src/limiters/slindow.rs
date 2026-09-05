use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use redis::{Script, aio::ConnectionManager};

use crate::config::SlindowConfig;

const SLINDOW_SCRIPT: &str = include_str!("../../scripts/slindow.lua");

static MEMBER_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub struct SlindowLimiter {
    conn: ConnectionManager,
    key_prefix: String,
    limit: u32,
    window_ms: u64,
    script: String,
}

#[derive(Debug, Clone, Copy)]
pub struct SlindowDecision {
    pub allowed: bool,
    pub count: u32,
    pub limit: u32,
    pub retry_after_secs: u64,
}

impl SlindowLimiter {
    #[must_use]
    pub fn new(cfg: &SlindowConfig, conn: ConnectionManager) -> Option<Self> {
        if !cfg.enabled {
            return None;
        }
        // Fall back to the embedded script when the configured path is unreadable.
        let script = std::fs::read_to_string(&cfg.script_path).unwrap_or_else(|e| {
            tracing::warn!(path = %cfg.script_path.display(), error = %e, "using embedded slindow script");
            SLINDOW_SCRIPT.to_owned()
        });
        Some(Self {
            conn,
            key_prefix: cfg.key_prefix.clone(),
            limit: cfg.requests_per_window,
            window_ms: cfg.window_secs * 1000,
            script,
        })
    }

    fn key(&self, ip: &str) -> String {
        format!("{}{ip}", self.key_prefix)
    }

    pub async fn check(&self, ip: &str) -> SlindowDecision {
        let now_ms = now_millis();
        let seq = MEMBER_SEQ.fetch_add(1, Ordering::Relaxed);
        let member = format!("{now_ms}:{seq}");
        let mut conn = self.conn.clone();
        let res: redis::RedisResult<(i32, i32, i64)> = Script::new(&self.script)
            .key(self.key(ip))
            .arg(now_ms)
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

    fn fail_open(&self) -> SlindowDecision {
        SlindowDecision {
            allowed: true,
            count: 0,
            limit: self.limit,
            retry_after_secs: 0,
        }
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}
