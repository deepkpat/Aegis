use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CoalescerConfig {
    pub enabled: bool,
}
