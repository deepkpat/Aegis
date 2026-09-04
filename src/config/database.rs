use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PostgresConfig {
    pub url: String,
    pub max_connections: u32,
    pub acquire_timeout_secs: u64,
}
