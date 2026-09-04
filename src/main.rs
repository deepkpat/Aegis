use aegis::api::router::{AppState, app_router};
use aegis::cache::{MokaCache, RedisCache};
use aegis::coalescer::Coalescer;
use aegis::config::AppConfig;
use aegis::db::{create_connection_manager, create_pool};
use aegis::deduper::Deduper;
use aegis::limiters::SlindowLimiter;

use axum::http::StatusCode;
use std::time::Duration;
use tokio::net::TcpListener;
use tower_http::timeout::TimeoutLayer;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = AppConfig::from_file("config.yaml")?;
    let pg = create_pool(&cfg.postgres).await?;
    let redis = create_connection_manager(&cfg.redis).await?;

    let moka = MokaCache::new(&cfg.cache.moka);
    let redis_cache = RedisCache::new(&cfg.cache.redis, redis.clone());
    let limiter = SlindowLimiter::new(&cfg.slindow, redis.clone());
    let coalescer = Coalescer::new(&cfg.coalescer);
    let deduper = Deduper::new(&cfg.deduper, redis.clone());

    let state = AppState {
        pg,
        redis,
        moka,
        redis_cache,
        limiter,
        coalescer,
        deduper,
    };
    let app = app_router(state).layer(TimeoutLayer::with_status_code(
        StatusCode::REQUEST_TIMEOUT,
        Duration::from_secs(cfg.server.request_timeout_secs),
    ));

    let addr = cfg.server.addr();
    info!(addr = %addr, "listening");
    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    info!("server stopped");

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    info!("shutdown signal received");
}
