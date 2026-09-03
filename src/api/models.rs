use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct CreateRecordRequest {
    pub id: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct PatchRecordRequest {
    pub payload: serde_json::Value,
    pub expected_version: i32,
}
