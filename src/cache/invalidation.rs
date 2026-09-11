use std::time::Duration;

use futures_util::StreamExt;
use tracing::{error, info, warn};

use super::composite::CompositeCache;

const RECONNECT_DELAY: Duration = Duration::from_secs(2);

pub fn spawn_invalidation_listener(redis_url: String, channel_name: String, cache: CompositeCache) {
    tokio::spawn(async move {
        loop {
            info!("connecting to redis pub-sub for l1 cache invalidation");
            serve_once(&redis_url, &channel_name, &cache).await;
            warn!("pub-sub connection lost. flushing entire l1 for safety.");
            cache.invalidate_all_l1();
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    });
}

async fn serve_once(redis_url: &str, channel_name: &str, cache: &CompositeCache) {
    let client = match redis::Client::open(redis_url) {
        Ok(client) => client,
        Err(e) => {
            error!(error = %e, "invalid redis url");
            return;
        }
    };
    let mut pubsub = match client.get_async_pubsub().await {
        Ok(pubsub) => pubsub,
        Err(e) => {
            error!(error = %e, "failed to establish pub-sub connection");
            return;
        }
    };
    if let Err(e) = pubsub.subscribe(channel_name).await {
        error!(error = %e, channel = %channel_name, "failed to subscribe");
        return;
    }
    info!(channel = %channel_name, "subscribed to invalidation channel");
    let mut stream = pubsub.on_message();
    while let Some(msg) = stream.next().await {
        match msg.get_payload::<String>() {
            Ok(key) => {
                cache.invalidate_l1(&key).await;
                tracing::debug!(key = %key, "evicted key from l1");
            }
            Err(e) => warn!(error = %e, "bad invalidation payload"),
        }
    }
}

pub async fn publish_invalidation(conn: &redis::aio::ConnectionManager, channel: &str, id: &str) {
    let mut conn = conn.clone();
    let res: redis::RedisResult<()> = redis::AsyncCommands::publish(&mut conn, channel, id).await;
    if let Err(e) = res {
        tracing::warn!(error = %e, "invalidation publish failed");
    }
}
