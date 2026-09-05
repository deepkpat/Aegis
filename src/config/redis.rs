use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RedisConfig {
    // Durable instance: idempotency, rate limits, pub/sub. Use noeviction.
    pub url: String,
    // Evictable cache L2. Falls back to url when absent.
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
