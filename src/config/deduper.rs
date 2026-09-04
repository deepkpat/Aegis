use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeduperConfig {
    pub enabled: bool,
    pub key_prefix: String,
    pub ttl_secs: u64,
    pub header: String,
    pub require_key: bool,
}
