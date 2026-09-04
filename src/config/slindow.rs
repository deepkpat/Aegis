use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SlindowConfig {
    pub enabled: bool,
    pub requests_per_window: u32,
    pub window_secs: u64,
    pub key_prefix: String,
    pub script_path: std::path::PathBuf,
}
