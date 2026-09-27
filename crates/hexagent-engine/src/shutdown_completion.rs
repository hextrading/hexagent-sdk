//! One executor publishes a single immutable finality message to its router.
//! Capacity is one latched unit event; repeated completion is idempotent and
//! cannot fill a queue. The router polls only after entering shutdown. There
//! is no wake lock between co-located FIFO threads of different priorities.
//! A dropped sender without publication is NOT evidence of order finality.
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

pub(crate) struct CompletionSender(Arc<AtomicBool>);
pub(crate) struct CompletionReceiver(Arc<AtomicBool>);

pub(crate) fn completion_lane() -> (CompletionSender, CompletionReceiver) {
    let completed = Arc::new(AtomicBool::new(false));
    (
        CompletionSender(completed.clone()),
        CompletionReceiver(completed),
    )
}

impl CompletionSender {
    /// Called only by the execution owner, after final updates are enqueued.
    pub(crate) fn publish(&self) {
        self.0.store(true, Ordering::Release);
    }
}

impl CompletionReceiver {
    pub(crate) fn is_complete(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn completion_is_latched_idempotent_and_run_isolated() {
        let (tx, rx) = completion_lane();
        let (other_tx, other_rx) = completion_lane();
        assert!(!rx.is_complete());
        // Publication can precede the router's BeginShutdown handling, and
        // repeated shutdown signals cannot block a producer or erase the event.
        for _ in 0..100_000 {
            tx.publish();
        }
        drop(tx);
        assert!(rx.is_complete());
        assert!(rx.is_complete());
        assert!(!other_rx.is_complete());
        drop(other_tx);
        assert!(!other_rx.is_complete(), "disconnect must fail closed");
    }

    #[test]
    fn completion_follows_final_lifecycle_publication() {
        let (tx, rx) = completion_lane();
        let (updates_tx, updates_rx) = hexagent_runtime::poll_channel::bounded(2);
        let marker = Arc::new(AtomicUsize::new(0));
        let writer_marker = marker.clone();
        let writer = std::thread::spawn(move || {
            updates_tx.send((7, 1)).unwrap();
            updates_tx.send((9, 2)).unwrap();
            writer_marker.store(11, Ordering::Relaxed);
            tx.publish();
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while !rx.is_complete() {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert_eq!(marker.load(Ordering::Relaxed), 11);
        assert_eq!(updates_rx.try_recv(), Ok((7, 1)));
        assert_eq!(updates_rx.try_recv(), Ok((9, 2)));
        writer.join().unwrap();
    }
}
