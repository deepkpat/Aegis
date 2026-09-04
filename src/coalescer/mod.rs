use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use tokio::sync::{Mutex, broadcast};

use crate::config::CoalescerConfig;

type Inflight<T, E> = Arc<Mutex<HashMap<String, broadcast::Sender<Result<T, E>>>>>;

#[derive(Debug)]
pub struct Coalescer<T, E> {
    inflight: Inflight<T, E>,
}

impl<T, E> Clone for Coalescer<T, E> {
    fn clone(&self) -> Self {
        Self {
            inflight: Arc::clone(&self.inflight),
        }
    }
}

impl<T, E> Coalescer<T, E>
where
    T: Clone + Send + Sync + 'static,
    E: Clone + Send + Sync + 'static,
{
    #[must_use]
    pub fn new(cfg: &CoalescerConfig) -> Option<Self> {
        if !cfg.enabled {
            return None;
        }
        Some(Self {
            inflight: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn execute<F, Fut>(&self, key: &str, fut: F) -> Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let rx = {
            let mut guard = self.inflight.lock().await;
            if let Some(tx) = guard.get(key) {
                tx.subscribe()
            } else {
                let (tx, _rx) = broadcast::channel(1);
                guard.insert(key.to_owned(), tx);
                drop(guard);
                let result = fut().await;
                let mut guard = self.inflight.lock().await;
                if let Some(tx) = guard.get(key) {
                    let _ = tx.send(result.clone());
                }
                guard.remove(key);
                return result;
            }
        };
        Self::await_shared(rx, key, fut).await
    }

    async fn await_shared<F, Fut>(
        mut rx: broadcast::Receiver<Result<T, E>>,
        key: &str,
        fut: F,
    ) -> Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        loop {
            match rx.recv().await {
                Ok(v) => return v,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => {
                    tracing::warn!(key = %key, "coalescer leader dropped, fail-open");
                    return fut().await;
                }
            }
        }
    }
}
