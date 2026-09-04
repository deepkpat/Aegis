use axum::{
    Router,
    extract::FromRef,
    routing::{get, post},
};
use redis::aio::ConnectionManager;
use sqlx::PgPool;

use super::health::{health, ready};
use super::records::{create_record, delete_record, get_record, patch_record, put_record};
use super::slindow::slindow_middleware;
use crate::cache::{MokaCache, RedisCache};
use crate::coalescer::Coalescer;
use crate::db::Record;
use crate::limiters::SlindowLimiter;

use super::errors::ApiError;

#[derive(Clone)]
pub struct AppState {
    pub pg: PgPool,
    pub redis: ConnectionManager,
    pub moka: Option<MokaCache>,
    pub redis_cache: Option<RedisCache>,
    pub limiter: Option<SlindowLimiter>,
    pub coalescer: Option<Coalescer<Record, ApiError>>,
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

impl FromRef<AppState> for Option<MokaCache> {
    fn from_ref(state: &AppState) -> Self {
        state.moka.clone()
    }
}

impl FromRef<AppState> for Option<RedisCache> {
    fn from_ref(state: &AppState) -> Self {
        state.redis_cache.clone()
    }
}

impl FromRef<AppState> for Option<SlindowLimiter> {
    fn from_ref(state: &AppState) -> Self {
        state.limiter.clone()
    }
}

impl FromRef<AppState> for Option<Coalescer<Record, ApiError>> {
    fn from_ref(state: &AppState) -> Self {
        state.coalescer.clone()
    }
}

pub fn app_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/v1/records", post(create_record))
        .route(
            "/v1/records/{id}",
            get(get_record)
                .put(put_record)
                .patch(patch_record)
                .delete(delete_record),
        )
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            slindow_middleware,
        ))
        .with_state(state)
}
