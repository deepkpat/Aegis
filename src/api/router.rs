use axum::{Router, routing::get};
use sqlx::PgPool;

use super::health::{health, ready};

pub fn app_router(pool: PgPool) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .with_state(pool)
}
