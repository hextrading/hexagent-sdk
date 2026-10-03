//! Advisory replay counters, never economic state or admission authority.
//! Recording performs no account transaction, notification, allocation or I/O.
//! Cold account transactions copy a snapshot into the existing persisted fields.
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub(super) struct ReplayPageMetrics {
    last: AtomicU64,
    maximum: AtomicU64,
    total: AtomicU64,
}

impl ReplayPageMetrics {
    pub(super) fn new(last: u64, maximum: u64, total: u64) -> Self {
        Self {
            last: AtomicU64::new(last),
            maximum: AtomicU64::new(maximum),
            total: AtomicU64::new(total),
        }
    }

    pub(super) fn record(&self, pages: usize) {
        let pages = pages as u64;
        self.maximum.fetch_max(pages, Ordering::Relaxed);
        let _ = self
            .total
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_add(pages))
            });
        self.last.store(pages, Ordering::Release);
    }

    pub(super) fn snapshot(&self) -> (u64, u64, u64) {
        // Observability may include a concurrent in-progress record. Preserve
        // last <= maximum <= total even across those independent atomic reads.
        let last = self.last.load(Ordering::Acquire);
        let maximum = self.maximum.load(Ordering::Relaxed).max(last);
        let total = self.total.load(Ordering::Relaxed).max(maximum);
        (last, maximum, total)
    }
}
