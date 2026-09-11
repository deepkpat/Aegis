use std::{
    hash::Hash,
    sync::Arc,
    time::{Duration, Instant},
};

use fastbloom::BloomFilter;
use parking_lot::RwLock;

use crate::config::BloomDeduperConfig;

#[derive(Debug, Clone)]
struct BloomRing {
    buckets: Vec<BloomFilter>,
    current: usize,
    last_rotation: Instant,
}

#[derive(Debug, Clone)]
pub struct BloomDeduper {
    inner: Arc<RwLock<BloomRing>>,
    capacity: usize,
    false_positive_rate: f64,
    bucket_ttl: Duration,
}

impl BloomDeduper {
    pub fn new(cfg: &BloomDeduperConfig) -> Option<Self> {
        if !cfg.enabled {
            return None;
        }

        let num_buckets = cfg.buckets.max(1);
        let capacity = usize::try_from(cfg.capacity.max(1)).unwrap_or(usize::MAX);
        let false_positive_rate = cfg.false_positive_rate.clamp(f64::MIN_POSITIVE, 0.999_999);

        let buckets = (0..num_buckets)
            .map(|_| Self::build_bucket(capacity, false_positive_rate))
            .collect();

        Some(Self {
            inner: Arc::new(RwLock::new(BloomRing {
                buckets,
                current: 0,
                last_rotation: Instant::now(),
            })),
            capacity,
            false_positive_rate,
            bucket_ttl: Duration::from_secs(cfg.bucket_ttl_secs),
        })
    }

    /// Inserts a key into the active bucket.
    pub fn insert<T: Hash + ?Sized>(&self, key: &T) {
        let mut ring = self.inner.write();
        self.rotate_if_due(&mut ring);
        let current = ring.current;
        ring.buckets[current].insert(key);
    }

    /// Checks if any bucket in the ring contains the key.
    /// Fast read path using Double-Checked Locking pattern.
    pub fn maybe_contains<T: Hash + ?Sized>(&self, key: &T) -> bool {
        // fast-path: optimistic read lock
        {
            let ring = self.inner.read();
            if ring.last_rotation.elapsed() < self.bucket_ttl {
                return ring.buckets.iter().any(|b| b.contains(key));
            }
        }

        // slow-path: acquire write lock only when rotation is overdue
        let mut ring = self.inner.write();
        self.rotate_if_due(&mut ring);
        ring.buckets.iter().any(|b| b.contains(key))
    }

    fn build_bucket(capacity: usize, false_positive_rate: f64) -> BloomFilter {
        BloomFilter::with_false_pos(false_positive_rate).expected_items(capacity)
    }

    fn rotate_if_due(&self, ring: &mut BloomRing) {
        let elapsed = ring.last_rotation.elapsed();
        if elapsed < self.bucket_ttl {
            return;
        }

        let ttl_millis = self.bucket_ttl.as_millis().max(1);
        let steps = (elapsed.as_millis() / ttl_millis) as usize;
        let num_buckets = ring.buckets.len();

        if steps >= num_buckets {
            // entire ring is expired, rebuild all buckets
            for bucket in &mut ring.buckets {
                *bucket = Self::build_bucket(self.capacity, self.false_positive_rate);
            }
            ring.current = 0;
            ring.last_rotation = Instant::now();
        } else {
            // advance window step-by-step
            for _ in 0..steps {
                ring.current = (ring.current + 1) % num_buckets;
                ring.buckets[ring.current] =
                    Self::build_bucket(self.capacity, self.false_positive_rate);
            }
            // prevent timer drift by advancing strictly by step multiples
            ring.last_rotation += self.bucket_ttl * (steps as u32);
        }
    }
}
