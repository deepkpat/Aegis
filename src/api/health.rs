use axum::{Json, extract::State, http::StatusCode};
use redis::{AsyncCommands, aio::ConnectionManager};
use serde_json::{Value, json};
use sqlx::PgPool;

pub async fn health() -> Json<Value> {
    Json(json!({"status": "healthy"}))
}

pub async fn ready(
    State(pg): State<PgPool>,
    State(mut redis): State<ConnectionManager>,
) -> (StatusCode, Json<Value>) {
    let (pg_ok, redis_ok) = tokio::join!(sqlx::query("SELECT 1").execute(&pg), async {
        redis.ping::<String>().await
    });

    match (pg_ok, redis_ok) {
        (Ok(_), Ok(_)) => (StatusCode::OK, Json(json!({"status": "ready"}))),
        (Err(pg_e), Err(redis_e)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(
                json!({"status": "not-ready", "postgres": pg_e.to_string(), "redis": redis_e.to_string()}),
            ),
        ),
        (Err(e), _) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "not-ready", "postgres": e.to_string()})),
        ),
        (_, Err(e)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "not-ready", "redis": e.to_string()})),
        ),
    }
}
