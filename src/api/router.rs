use axum::{Router, routing::get};

use super::health::health;

pub fn app_router() -> Router {
    Router::new().route("/health", get(health))
}
