use axum::{Json, http::StatusCode, response::IntoResponse};
use serde_json::json;

pub enum ApiError {
    NotFound(String),
    Conflict(String),
    VersionMismatch(String),
    BadRequest(String),
    Internal(String),
}

impl From<sqlx::Error> for ApiError {
    fn from(e: sqlx::Error) -> Self {
        Self::Internal(e.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, code, message) = match self {
            Self::NotFound(m) => (StatusCode::NOT_FOUND, "NOT_FOUND", m),
            Self::Conflict(m) => (StatusCode::CONFLICT, "CONFLICT", m),
            Self::VersionMismatch(m) => (StatusCode::PRECONDITION_FAILED, "VERSION_MISMATCH", m),
            Self::BadRequest(m) => (StatusCode::BAD_REQUEST, "BAD_REQUEST", m),
            Self::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", m),
        };
        (status, Json(json!({"error": code, "message": message}))).into_response()
    }
}
