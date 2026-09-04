use axum::{
    Json,
    body::Body,
    extract::{ConnectInfo, Request, State},
    http::{HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde_json::json;
use std::net::SocketAddr;

use crate::limiters::SlindowLimiter;

fn client_ip(req: &Request<Body>) -> Option<String> {
    if let Some(xff) = req.headers().get("x-forwarded-for")
        && let Ok(v) = xff.to_str()
        && let Some(first) = v.split(',').next()
    {
        let ip = first.trim();
        if !ip.is_empty() {
            return Some(ip.to_owned());
        }
    }
    req.extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| addr.ip().to_string())
}

pub async fn slindow_middleware(
    State(limiter): State<Option<SlindowLimiter>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    if !req.uri().path().starts_with("/v1/") {
        return next.run(req).await;
    }
    let Some(limiter) = limiter else {
        return next.run(req).await;
    };
    let Some(ip) = client_ip(&req) else {
        return next.run(req).await;
    };
    let d = limiter.check(&ip).await;
    if d.allowed {
        let mut res = next.run(req).await;
        res.headers_mut().insert(
            "x-ratelimit-limit",
            HeaderValue::from_str(&d.limit.to_string())
                .unwrap_or_else(|_| HeaderValue::from_static("0")),
        );
        res.headers_mut().insert(
            "x-ratelimit-remaining",
            HeaderValue::from_str(&d.limit.saturating_sub(d.count).to_string())
                .unwrap_or_else(|_| HeaderValue::from_static("0")),
        );
        return res;
    }
    (
        StatusCode::TOO_MANY_REQUESTS,
        [
            ("retry-after", d.retry_after_secs.to_string()),
            ("x-ratelimit-limit", d.limit.to_string()),
            ("x-ratelimit-remaining", "0".to_owned()),
        ],
        Json(json!({"error": "RATE_LIMITED", "message": "rate limit exceeded"})),
    )
        .into_response()
}
