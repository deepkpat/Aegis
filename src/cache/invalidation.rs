use std::time::Duration;

use futures_util::StreamExt;
use tracing::{error, info, warn};

use super::composite::CompositeCache;

const RECONNECT_DELAY: Duration = Duration::from_secs(2);

pub fn spawn_invalidation_listener(redis_url: String, channel_name: String, cache: CompositeCache) {
    tokio::spawn(async move {
        loop {
            info!("Connecting to Redis Pub/Sub for L1 cache invalidation...");
            match redis::Client::open(redis_url.as_str()) {
                Ok(client) => match client.get_async_pubsub().await {
                    Ok(mut pubsub) => {
                        if let Err(e) = pubsub.subscribe(&channel_name).await {
                            error!(error = %e, channel = %channel_name, "Failed to subscribe");
                            tokio::time::sleep(RECONNECT_DELAY).await;
                            continue;
                        }
                        info!(channel = %channel_name, "Subscribed to invalidation channel");
                        let mut stream = pubsub.on_message();
                        while let Some(msg) = stream.next().await {
                            match msg.get_payload::<String>() {
                                Ok(key) => {
                                    cache.invalidate_l1(&key).await;
                                    tracing::debug!(key = %key, "Evicted key from L1");
                                }
                                Err(e) => warn!(error = %e, "Bad invalidation payload"),
                            }
                        }
                    }
                    Err(e) => error!(error = %e, "Failed to establish Pub/Sub connection"),
                },
                Err(e) => error!(error = %e, "Invalid Redis URL"),
            }
            warn!("Pub/Sub connection lost. Flushing entire L1 for safety.");
            cache.invalidate_all_l1();
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    });
}

pub async fn publish_invalidation(conn: &redis::aio::ConnectionManager, channel: &str, id: &str) {
    let mut conn = conn.clone();
    let res: redis::RedisResult<()> = redis::AsyncCommands::publish(&mut conn, channel, id).await;
    if let Err(e) = res {
        tracing::warn!(error = %e, "invalidation publish failed");
    }
}
