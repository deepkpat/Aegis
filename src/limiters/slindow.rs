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
        Some(Self {
            conn,
            key_prefix: cfg.key_prefix.clone(),
            limit: cfg.requests_per_window,
            window_ms: cfg.window_secs * 1000,
        })
    }

    fn key(&self, ip: &str) -> String {
        format!("{}{ip}", self.key_prefix)
    }

    pub async fn check(&self, ip: &str) -> SlindowDecision {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let seq = MEMBER_SEQ.fetch_add(1, Ordering::Relaxed);
        let member = format!("{now_ms}:{seq}");
        let mut conn = self.conn.clone();
        let res: redis::RedisResult<(i32, i32, i64)> = Script::new(SLINDOW_SCRIPT)
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
                count: count as u32,
                limit: self.limit,
                retry_after_secs: (ttl_ms.max(0) as u64).div_ceil(1000),
            },
            Err(e) => {
                tracing::warn!(error = %e, "sliding-window check failed, fail-open");
                SlindowDecision {
                    allowed: true,
                    count: 0,
                    limit: self.limit,
                    retry_after_secs: 0,
                }
            }
        }
    }
}
