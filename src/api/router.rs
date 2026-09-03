use axum::{
    Router,
    routing::{get, post},
};
use sqlx::PgPool;

use super::health::{health, ready};
use super::records::{create_record, delete_record, get_record, patch_record};

pub fn app_router(pool: PgPool) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/v1/records", post(create_record))
        .route(
            "/v1/records/{id}",
            get(get_record).patch(patch_record).delete(delete_record),
        )
        .with_state(pool)
}
