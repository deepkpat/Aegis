use axum::{
    Router,
    extract::FromRef,
    routing::{get, post},
};
use redis::aio::ConnectionManager;
use sqlx::PgPool;

use super::health::{health, ready};
use super::records::{create_record, delete_record, get_record, patch_record};

#[derive(Clone)]
pub struct AppState {
    pub pg: PgPool,
    pub redis: ConnectionManager,
}

impl FromRef<AppState> for PgPool {
    fn from_ref(state: &AppState) -> Self {
        state.pg.clone()
    }
}

impl FromRef<AppState> for ConnectionManager {
    fn from_ref(state: &AppState) -> Self {
        state.redis.clone()
    }
}

pub fn app_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/v1/records", post(create_record))
        .route(
            "/v1/records/{id}",
            get(get_record).patch(patch_record).delete(delete_record),
        )
        .with_state(state)
}
