use axum::{
    Json,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde_json::json;

use crate::deduper::{Claim, Deduper};

fn is_mutating(req: &Request<Body>) -> bool {
    let m = req.method().as_str();
    (m == "POST" || m == "PUT" || m == "PATCH") && req.uri().path().starts_with("/v1/")
}

pub async fn deduper_middleware(
    State(deduper): State<Option<Deduper>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let Some(deduper) = deduper else {
        return next.run(req).await;
    };
    if !is_mutating(&req) {
        return next.run(req).await;
    }
    let raw_key = req
        .headers()
        .get(&deduper.header_name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .unwrap_or_default()
        .to_owned();
    if raw_key.is_empty() {
        if !deduper.require_key {
            return next.run(req).await;
        }
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "IDEMPOTENCY_KEY_MISSING", "message": "Idempotency-Key header is required"})),
        )
            .into_response();
    }
    if !Deduper::validate_key(&raw_key) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "IDEMPOTENCY_KEY_INVALID", "message": "Idempotency-Key must be 1-255 chars"})),
        )
            .into_response();
    }
    let method = req.method().as_str().to_owned();
    let path = req.uri().path().to_owned();
    match deduper.claim(&method, &path, &raw_key).await {
        None => next.run(req).await,
        Some(Claim::Replay { status, body }) => {
            let code = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
            (
                code,
                [("idempotent-replayed", "true")],
                Json(serde_json::from_str::<serde_json::Value>(&body).unwrap_or(json!({}))),
            )
                .into_response()
        }
        Some(Claim::InFlight) => (
            StatusCode::CONFLICT,
            Json(
                json!({"error": "IDEMPOTENT_IN_FLIGHT", "message": "duplicate request in flight"}),
            ),
        )
            .into_response(),
        Some(Claim::Fresh { redis_key }) => {
            let res = next.run(req).await;
            let (mut parts, body) = res.into_parts();
            let Ok(bytes) = to_bytes(body, usize::MAX).await else {
                deduper.release(&redis_key).await;
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "INTERNAL", "message": "body read failed"})),
                )
                    .into_response();
            };
            if parts.status.is_success() {
                let text = String::from_utf8_lossy(&bytes).into_owned();
                let ct = parts
                    .headers
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("application/json")
                    .to_owned();
                deduper
                    .complete(&redis_key, parts.status.as_u16(), &text, &ct)
                    .await;
            } else {
                deduper.release(&redis_key).await;
            }
            parts.headers.insert(
                "idempotent-replayed",
                axum::http::HeaderValue::from_static("false"),
            );
            Response::from_parts(parts, Body::from(bytes))
        }
    }
}
