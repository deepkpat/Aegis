use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RedisConfig {
    /// Durable instance: idempotency claims, rate-limit windows, pub/sub.
    /// Run with `noeviction` so memory pressure surfaces as a loud write
    /// error (which the code handles fail-open) instead of silently evicting
    /// correctness-critical keys (note.md §1.4).
    pub url: String,
    /// Evictable instance for cache L2 only (`allkeys-lru`). Optional for
    /// backward compatibility: when absent, cache L2 shares `url` — the old
    /// single-instance setup with the §1.4 eviction gap intact.
    #[serde(default)]
    pub cache_url: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_config_without_cache_url_still_parses() {
        let cfg: RedisConfig = serde_yaml::from_str(r#"url: "redis://localhost:6379""#).unwrap();
        assert_eq!(cfg.url, "redis://localhost:6379");
        assert_eq!(cfg.cache_url, None);
    }
}
