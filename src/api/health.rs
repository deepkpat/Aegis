use axum::{Json, extract::State, http::StatusCode};
use redis::{AsyncCommands, aio::ConnectionManager};
use serde_json::{Value, json};
use sqlx::PgPool;

use super::router::CacheRedis;

pub async fn health() -> Json<Value> {
    Json(json!({"status": "healthy"}))
}

pub async fn ready(
    State(pg): State<PgPool>,
    State(mut redis): State<ConnectionManager>,
    State(CacheRedis(mut redis_cache)): State<CacheRedis>,
) -> (StatusCode, Json<Value>) {
    // in single-instance mode both handles point at the same server.
    let (pg_res, redis_res, cache_res) = tokio::join!(
        sqlx::query("SELECT 1").execute(&pg),
        async { redis.ping::<String>().await },
        async { redis_cache.ping::<String>().await },
    );

    let mut failures = serde_json::Map::new();
    if let Err(e) = pg_res {
        failures.insert("postgres".to_owned(), json!(e.to_string()));
    }
    if let Err(e) = redis_res {
        failures.insert("redis".to_owned(), json!(e.to_string()));
    }
    if let Err(e) = cache_res {
        failures.insert("redis_cache".to_owned(), json!(e.to_string()));
    }

    if failures.is_empty() {
        (StatusCode::OK, Json(json!({"status": "ready"})))
    } else {
        let mut body = serde_json::Map::from_iter([("status".to_owned(), json!("not-ready"))]);
        body.extend(failures);
        (StatusCode::SERVICE_UNAVAILABLE, Json(Value::Object(body)))
    }
}
