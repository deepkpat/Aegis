use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use dashmap::mapref::entry::Entry as DashEntry;
use futures_util::FutureExt;
use tokio::sync::watch;

use crate::config::CoalescerConfig;

static ENTRY_IDS: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
struct Entry<T, E> {
    id: u64,
    tx: watch::Sender<Option<Result<T, E>>>,
}

type Inflight<T, E> = Arc<DashMap<String, Arc<Entry<T, E>>>>;

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
            inflight: Arc::new(DashMap::new()),
        })
    }

    pub async fn execute<F, Fut>(&self, key: &str, fut: F) -> Result<T, E>
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        let mut failures: u32 = 0;
        loop {
            // atomic check-and-insert scoped strictly to the key's shard.
            let (id, mut rx) = match self.inflight.entry(key.to_string()) {
                DashEntry::Occupied(entry) => {
                    let val = entry.get();
                    (val.id, val.tx.subscribe())
                }
                DashEntry::Vacant(entry) => {
                    let (tx, rx) = watch::channel(None);
                    let id = ENTRY_IDS.fetch_add(1, Ordering::Relaxed);
                    let item = Arc::new(Entry { id, tx });

                    entry.insert(Arc::clone(&item));

                    Self::spawn_leader(
                        Arc::clone(&self.inflight),
                        key.to_string(),
                        Arc::clone(&item),
                        fut(),
                    );

                    (id, rx)
                }
            };

            // await result without holding any dash-map guard or shard lock.
            if rx.wait_for(|val| val.is_some()).await.is_ok() {
                if let Some(result) = rx.borrow().as_ref() {
                    return result.clone();
                }
            }

            // leader dropped without sending. atomic cleanup via predicate:
            // removes key ONLY if the entry ID matches our leader generation.
            let removed = self
                .inflight
                .remove_if(key, |_, cur| cur.id == id)
                .is_some();
            if removed {
                failures = failures.saturating_add(1);
                tracing::warn!(key = %key, failures, "coalescer leader dropped, re-electing");
            } else {
                failures = 0;
            }

            backoff(failures).await;
        }
    }

    fn spawn_leader<Fut>(inflight: Inflight<T, E>, key: String, entry: Arc<Entry<T, E>>, fut: Fut)
    where
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        tokio::spawn(async move {
            let outcome = AssertUnwindSafe(fut).catch_unwind().await;

            match outcome {
                Ok(result) => {
                    let _ = entry.tx.send(Some(result));
                    // remove entry only if it still belongs to this leader task
                    inflight.remove_if(&key, |_, cur| cur.id == entry.id);
                }
                Err(_) => {
                    inflight.remove_if(&key, |_, cur| cur.id == entry.id);
                    tracing::warn!(key = %key, "coalescer leader panicked, cleaning up key");
                }
            }
        });
    }
}

async fn backoff(failures: u32) {
    if failures == 0 {
        return;
    }
    let shift = failures.saturating_sub(1).min(5);
    let base_ms = 50u64.saturating_mul(1 << shift).min(2000);

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    let jitter = nanos % (base_ms / 2 + 1);

    tokio::time::sleep(Duration::from_millis(base_ms / 2 + jitter)).await;
}
