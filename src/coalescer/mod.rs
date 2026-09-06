use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{Mutex, broadcast};

use crate::config::CoalescerConfig;

static ENTRY_IDS: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
struct Entry<T, E> {
    id: u64,
    tx: broadcast::Sender<Result<T, E>>,
}

type Inflight<T, E> = Arc<Mutex<HashMap<String, Arc<Entry<T, E>>>>>;

#[derive(Debug)]
pub struct Coalescer<T, E> {
    inflight: Inflight<T, E>,
}

// Manual Clone avoids adding a Clone bound on T and E.
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
        // Fn (not FnOnce): a waiter that re-contends after a dead leader
        // must be able to build the future again.
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        loop {
            let (id, mut rx) = {
                let mut guard = self.inflight.lock().await;
                if let Some(entry) = guard.get(key) {
                    // Subscribe while holding the lock: the leader sends while
                    // holding the same lock, so we can't miss the value.
                    (entry.id, entry.tx.subscribe())
                } else {
                    let (tx, _rx) = broadcast::channel(1);
                    let entry = Arc::new(Entry {
                        id: ENTRY_IDS.fetch_add(1, Ordering::Relaxed),
                        tx,
                    });
                    guard.insert(key.to_owned(), Arc::clone(&entry));
                    // Subscribe before spawning so the leader never misses
                    // its own result even if the worker finishes instantly.
                    let rx = entry.tx.subscribe();
                    let id = entry.id;
                    drop(guard);
                    Self::spawn_leader(
                        Arc::clone(&self.inflight),
                        key.to_owned(),
                        Arc::clone(&entry),
                        fut(),
                    );
                    drop(entry);
                    (id, rx)
                }
            };
            // Only a Receiver is held across this await. If the leader task
            // dies without sending, the channel reports Closed instead of
            // hanging forever.
            loop {
                match rx.recv().await {
                    Ok(value) => return value,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            // Leader died without sending, or we subscribed after a completed
            // send and missed it. Clear our own stale entry (the id check
            // avoids clobbering a replacement leader) and re-contend, so
            // exactly one waiter becomes the new leader.
            {
                let mut guard = self.inflight.lock().await;
                let stale = guard.get(key).is_some_and(|cur| cur.id == id);
                if stale {
                    guard.remove(key);
                    tracing::warn!(key = %key, "coalescer leader dropped, re-electing");
                }
            }
        }
    }

    fn spawn_leader<Fut>(
        inflight: Inflight<T, E>,
        key: String,
        entry: Arc<Entry<T, E>>,
        fut: Fut,
    ) where
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        // Detached task: outlives the leader's HTTP request, so a leader
        // timeout/cancel no longer strands the followers.
        tokio::spawn(async move {
            // Nested task isolates a panic in user work so the cleanup below
            // always runs instead of leaking the map entry.
            let outcome = tokio::spawn(fut).await;
            let mut guard = inflight.lock().await;
            let ours = guard.get(&key).is_some_and(|cur| cur.id == entry.id);
            match outcome {
                Ok(result) => {
                    if ours {
                        // Send while holding the lock so no subscriber can
                        // slip in between the send and the remove.
                        if let Some(cur) = guard.get(&key) {
                            let _ = cur.tx.send(result.clone());
                        }
                        guard.remove(&key);
                    } else {
                        // Entry already replaced/removed; still notify our
                        // own subscribers.
                        let _ = entry.tx.send(result.clone());
                    }
                }
                Err(join_err) => {
                    if ours {
                        guard.remove(&key);
                    }
                    tracing::warn!(key = %key, error = %join_err, "coalescer worker failed, re-electing");
                }
            }
        });
    }
}
