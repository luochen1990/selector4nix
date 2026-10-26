//! Counters for the NAR disk cache (spec §8).

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NarCacheMetricSnapshot {
    /// Verified complete partials served to clients from disk.
    pub disk_hits: u64,
    /// Downloads resumed from an existing partial watermark.
    pub partial_hits: u64,
    /// Sum of watermark bytes that did not have to be fetched from upstream again.
    pub bytes_resumed: u64,
}

#[derive(Debug, Default)]
pub struct NarCacheMetric {
    disk_hits: AtomicU64,
    partial_hits: AtomicU64,
    bytes_resumed: AtomicU64,
}

impl NarCacheMetric {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_disk_hit(&self) {
        self.disk_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_partial_hit(&self, bytes_resumed: u64) {
        self.partial_hits.fetch_add(1, Ordering::Relaxed);
        self.bytes_resumed
            .fetch_add(bytes_resumed, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> NarCacheMetricSnapshot {
        NarCacheMetricSnapshot {
            disk_hits: self.disk_hits.load(Ordering::Relaxed),
            partial_hits: self.partial_hits.load(Ordering::Relaxed),
            bytes_resumed: self.bytes_resumed.load(Ordering::Relaxed),
        }
    }
}
