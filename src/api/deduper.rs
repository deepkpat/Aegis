use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::api::router::AppState;
use crate::deduper::{Claim, Deduper};

// responses above this are executed but not stored for replay.
const REPLAY_BODY_LIMIT: usize = 1 << 20;
// upper bound for buffering a response in memory.
const BUFFER_BODY_LIMIT: usize = 16 << 20;

pub async fn deduper_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let Some(deduper) = state.deduper.clone() else {
        return next.run(request).await;
    };
    if !is_mutating(&request) || is_health(&request) {
        return next.run(request).await;
    }

    let method = request.method().as_str().to_owned();
    let path = request.uri().path().to_owned();
    let Some(client_key) = extract_key(&request, &deduper) else {
        if deduper.require_key {
            return (
                StatusCode::BAD_REQUEST,
                format!("missing or invalid `{}` header", deduper.header_name),
            )
                .into_response();
        }
        return next.run(request).await;
    };

    // claim failure means a subsystem error, so fail open.
    let Some(claim) = deduper.claim(&method, &path, &client_key).await else {
        return next.run(request).await;
    };

    match claim {
        Claim::Replay {
            status,
            body,
            content_type,
            ..
        } => {
            tracing::debug!(%client_key, "idempotent replay");
            replay(status, body, content_type)
        }
        Claim::InFlight { retry_after_secs } => (
            StatusCode::CONFLICT,
            [("retry-after", retry_after_secs.to_string())],
            "a request with this idempotency key is still in flight",
        )
            .into_response(),
        Claim::Fresh {
            redis_key,
            bloom_key,
        } => {
            let response = next.run(request).await;
            if response.status().is_server_error() {
                deduper.release(redis_key.as_deref()).await;
                return response;
            }
            store_response(&deduper, response, &client_key, redis_key, bloom_key).await
        }
    }
}

fn is_health(request: &Request) -> bool {
    matches!(request.uri().path(), "/health" | "/ready")
}

fn is_mutating(request: &Request) -> bool {
    matches!(request.method().as_str(), "POST" | "PUT" | "PATCH")
}

fn extract_key(request: &Request, deduper: &Deduper) -> Option<String> {
    request
        .headers()
        .get(&deduper.header_name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|k| Deduper::validate_key(k))
        .map(str::to_owned)
}

fn replay(status: u16, body: Option<Vec<u8>>, content_type: String) -> Response {
    let status = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
    let mut resp = (status, body.unwrap_or_default()).into_response();
    if let Ok(value) = content_type.parse() {
        resp.headers_mut().insert(CONTENT_TYPE, value);
    }
    resp.headers_mut()
        .insert("idempotent-replayed", HeaderValue::from_static("true"));
    resp
}

async fn store_response(
    deduper: &Deduper,
    response: Response,
    client_key: &str,
    redis_key: Option<String>,
    bloom_key: Option<String>,
) -> Response {
    let (parts, body) = response.into_parts();
    let Ok(bytes) = to_bytes(body, BUFFER_BODY_LIMIT).await else {
        deduper.release(redis_key.as_deref()).await;
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to buffer response for idempotency",
        )
            .into_response();
    };

    // oversized bodies still return normally, just without a replay record.
    if bytes.len() > REPLAY_BODY_LIMIT {
        tracing::warn!(%client_key, bytes = bytes.len(), "response exceeds replay limit");
        deduper.release(redis_key.as_deref()).await;
        let mut resp = Response::from_parts(parts, Body::from(bytes));
        resp.headers_mut()
            .insert("idempotency-stored", HeaderValue::from_static("none"));
        return resp;
    }

    let status = parts.status.as_u16();
    let content_type = parts
        .headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_owned();
    deduper
        .complete(
            redis_key.as_deref(),
            bloom_key.as_deref(),
            status,
            Some(bytes.as_ref()),
            &content_type,
            "",
        )
        .await;
    Response::from_parts(parts, Body::from(bytes))
}
