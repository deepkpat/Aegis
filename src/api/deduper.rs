//! Idempotency middleware wiring the `Deduper` into the Axum request path.
//!
//! Semantics:
//! - Fresh key    → execute handler, buffer response, record for replay.
//! - Replay       → return the stored response without executing.
//! - In-flight    → 409 Conflict (duplicate while original still running).
//! - Deduper down → fail-open, execute normally.

use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::api::router::AppState;
use crate::deduper::{Claim, Deduper};

/// Cap on response bodies eligible for replay storage. Responses larger
/// than this still execute, but are not recorded (client retries would
/// re-execute). Tune for your payload sizes.
const REPLAY_BODY_LIMIT: usize = 1 << 20; // 1 MiB

/// Hard cap for buffering a response body in memory so the original bytes
/// can be returned even when they exceed `REPLAY_BODY_LIMIT` (note.md §1.5).
/// Only reached by absurdly large record payloads; beyond this we give up
/// with a 500 rather than risk unbounded memory growth.
const BUFFER_BODY_LIMIT: usize = 16 << 20; // 16 MiB

pub async fn deduper_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let Some(deduper) = state.deduper.clone() else {
        return next.run(request).await;
    };

    let path = request.uri().path().to_owned();
    if path == "/health" || path == "/ready" {
        return next.run(request).await;
    }
    let method_str = request.method().as_str().to_owned();
    if !matches!(method_str.as_str(), "POST" | "PUT" | "PATCH") {
        return next.run(request).await;
    }

    // 1. Extract and validate the client-supplied idempotency key.
    let Some(client_key) = request
        .headers()
        .get(&deduper.header_name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|k| Deduper::validate_key(k))
        .map(str::to_owned)
    else {
        if deduper.require_key {
            return (
                StatusCode::BAD_REQUEST,
                format!("missing or invalid `{}` header", deduper.header_name),
            )
                .into_response();
        }
        return next.run(request).await;
    };

    // 2. Claim. `None` means a deduper subsystem error → fail-open.
    let Some(claim) = deduper.claim(&method_str, &path, &client_key).await else {
        return next.run(request).await;
    };

    match claim {
        Claim::Replay {
            status,
            body,
            content_type,
        } => {
            tracing::debug!(%client_key, "idempotent replay");
            let status = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
            let mut resp = (status, body).into_response();
            if let Ok(val) = content_type.parse() {
                resp.headers_mut().insert(CONTENT_TYPE, val);
            }
            resp.headers_mut()
                .insert("idempotent-replayed", HeaderValue::from_static("true"));
            resp
        }
        Claim::InFlight => (
            StatusCode::CONFLICT,
            "a request with this idempotency key is still in flight",
        )
            .into_response(),
        Claim::Fresh {
            redis_key,
            bloom_key,
        } => {
            let response = next.run(request).await;

            // Never record server errors: the client is expected to retry.
            if response.status().is_server_error() {
                deduper.release(redis_key.as_deref()).await;
                return response;
            }

            // Buffer the body so duplicates can be served the stored copy.
            // Buffer up to a generous hard cap so that a response larger
            // than REPLAY_BODY_LIMIT can still be returned intact below.
            let (parts, body) = response.into_parts();
            let Ok(bytes) = to_bytes(body, BUFFER_BODY_LIMIT).await else {
                deduper.release(redis_key.as_deref()).await;
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to buffer response for idempotency",
                )
                    .into_response();
            };

            // Bodies too large to store were still executed: release the
            // claim (nothing to replay, so no reason to hold it) and return
            // the real response with an explicit signal that a retry would
            // re-execute (note.md §1.5). Deliberately *not* recorded in the
            // Bloom filter either, keeping "bloom contains key" truthful
            // about "Redis holds a replayable record".
            if bytes.len() > REPLAY_BODY_LIMIT {
                tracing::warn!(
                    %client_key,
                    bytes = bytes.len(),
                    "response exceeds replay limit, returning without idempotency record"
                );
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
            let body_string = String::from_utf8_lossy(&bytes).into_owned();

            deduper
                .complete(
                    redis_key.as_deref(),
                    bloom_key.as_deref(),
                    status,
                    &body_string,
                    &content_type,
                )
                .await;

            Response::from_parts(parts, Body::from(bytes))
        }
    }
}
