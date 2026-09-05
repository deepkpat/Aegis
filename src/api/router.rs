use axum::{
    Router,
    extract::FromRef,
    routing::{get, post},
};
use redis::aio::ConnectionManager;
use sqlx::PgPool;

use super::deduper::deduper_middleware;
use super::health::{health, ready};
use super::records::{create_record, delete_record, get_record, patch_record, put_record};
use super::slindow::slindow_middleware;
use crate::cache::CompositeCache;
use crate::coalescer::Coalescer;
use crate::db::Record;
use crate::deduper::Deduper;
use crate::limiters::SlindowLimiter;

use super::errors::ApiError;

#[derive(Clone)]
pub struct AppState {
    pub pg: PgPool,
    pub redis: ConnectionManager,
    pub cache: Option<CompositeCache>,
    pub limiter: Option<SlindowLimiter>,
    pub coalescer: Option<Coalescer<Record, ApiError>>,
    pub deduper: Option<Deduper>,
    pub invalidation_channel: Option<String>,
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

impl FromRef<AppState> for Option<CompositeCache> {
    fn from_ref(state: &AppState) -> Self {
        state.cache.clone()
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

impl FromRef<AppState> for Option<Deduper> {
    fn from_ref(state: &AppState) -> Self {
        state.deduper.clone()
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
            deduper_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            slindow_middleware,
        ))
        .with_state(state)
}
