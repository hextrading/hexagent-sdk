//! Bounded startup owner for submitting cold connection work to Tokio.
//!
//! `Handle::spawn_blocking` can synchronously create a thread and lock the
//! blocking pool. Only this SCHED_OTHER background owner calls it. It never
//! waits for a connection; doing so on a maintenance/RPC pool can deadlock
//! when those workers themselves await HTTP. Warm HTTP requests bypass this
//! lane entirely. FIFO capacity 64, nonblocking rejection, no retry/replay.
//! Messages own their closures; the connection oneshot cancels obsolete jobs.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

const CAPACITY: usize = 64;
pub(crate) type Job = Box<dyn FnOnce() + Send + 'static>;

#[derive(Default)]
struct Counters {
    admitted: AtomicU64,
    dequeued: AtomicU64,
    rejected: AtomicU64,
    panics: AtomicU64,
    sampled_high_water: AtomicUsize,
}

struct Submitter {
    tx: crossbeam_channel::Sender<Job>,
    counters: Arc<Counters>,
}

static SUBMITTER: OnceLock<Result<Submitter, String>> = OnceLock::new();

impl Submitter {
    fn start(capacity: usize) -> Result<Self, String> {
        let (tx, rx) = crossbeam_channel::bounded::<Job>(capacity);
        let counters = Arc::new(Counters::default());
        let worker_counters = Arc::clone(&counters);
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("ord-connect-tx".into())
            .spawn(move || {
                crate::os_tune::pin_background("ord-connect-tx");
                let _ = ready_tx.send(());
                while let Ok(job) = rx.recv() {
                    worker_counters.dequeued.fetch_add(1, Ordering::Relaxed);
                    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(job)).is_err() {
                        worker_counters.panics.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
            .map_err(|error| format!("spawn cold connection submit owner: {error}"))?;
        // Startup-only rendezvous: do not publish a sender before affinity and
        // scheduling have been applied. The live caller never waits here.
        ready_rx
            .recv()
            .map_err(|error| format!("cold connection submit owner startup: {error}"))?;
        Ok(Self { tx, counters })
    }

    fn try_submit(&self, job: Job) -> Result<(), &'static str> {
        self.tx.try_send(job).map_err(|error| {
            self.counters.rejected.fetch_add(1, Ordering::Relaxed);
            match error {
                crossbeam_channel::TrySendError::Full(_) => "cold connection submit queue full",
                crossbeam_channel::TrySendError::Disconnected(_) => {
                    "cold connection submit owner disconnected"
                }
            }
        })?;
        self.counters.admitted.fetch_add(1, Ordering::Relaxed);
        self.counters
            .sampled_high_water
            .fetch_max(self.tx.len(), Ordering::Relaxed);
        Ok(())
    }
}

pub fn prewarm() -> Result<(), String> {
    SUBMITTER
        .get_or_init(|| Submitter::start(CAPACITY))
        .as_ref()
        .map(|_| ())
        .map_err(Clone::clone)
}

pub(crate) fn try_submit(job: Job) -> std::io::Result<()> {
    // async_rt::init prewarms before trading. Standalone SDK clients/tests may
    // lazily start this owner, outside a configured production hot path.
    prewarm().map_err(std::io::Error::other)?;
    SUBMITTER
        .get()
        .unwrap()
        .as_ref()
        .unwrap()
        .try_submit(job)
        .map_err(std::io::Error::other)
}

/// Formatting/export is called only by the existing latency dump owner.
pub(crate) fn log_snapshot() {
    if let Some(Ok(owner)) = SUBMITTER.get() {
        let c = &owner.counters;
        log::info!("[cold_connect_submit] capacity={} depth={} sampled_high_water={} admitted={} dequeued={} rejected={} panics={}",
            CAPACITY, owner.tx.len(), c.sampled_high_water.load(Ordering::Relaxed),
            c.admitted.load(Ordering::Relaxed), c.dequeued.load(Ordering::Relaxed),
            c.rejected.load(Ordering::Relaxed), c.panics.load(Ordering::Relaxed));
    }
}

#[cfg(test)]
pub(crate) fn benchmark_snapshot() -> (usize, usize, u64, u64, u64, u64) {
    let owner = SUBMITTER.get().unwrap().as_ref().unwrap();
    let c = &owner.counters;
    (
        owner.tx.len(),
        c.sampled_high_water.load(Ordering::Relaxed),
        c.admitted.load(Ordering::Relaxed),
        c.dequeued.load(Ordering::Relaxed),
        c.rejected.load(Ordering::Relaxed),
        c.panics.load(Ordering::Relaxed),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_fifo_rejects_full_recovers_and_keeps_submitter_off_caller() {
        let owner = Submitter::start(2).unwrap();
        let (started_tx, started_rx) = crossbeam_channel::bounded(1);
        let (release_tx, release_rx) = crossbeam_channel::bounded(1);
        owner
            .try_submit(Box::new(move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }))
            .unwrap();
        started_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        let (done_tx, done_rx) = crossbeam_channel::bounded(3);
        for id in [1, 2] {
            let tx = done_tx.clone();
            owner
                .try_submit(Box::new(move || {
                    tx.send((id, std::thread::current().id())).unwrap();
                }))
                .unwrap();
        }
        assert!(owner
            .try_submit(Box::new(|| panic!("rejected job must not run")))
            .is_err());
        assert_eq!(owner.counters.sampled_high_water.load(Ordering::Relaxed), 2);
        release_tx.send(()).unwrap();
        for id in [1, 2] {
            let (actual, thread) = done_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap();
            assert_eq!(actual, id);
            assert_ne!(thread, std::thread::current().id());
        }
        owner
            .try_submit(Box::new(move || {
                done_tx.send((3, std::thread::current().id())).unwrap();
            }))
            .unwrap();
        assert_eq!(
            done_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap()
                .0,
            3
        );
        assert_eq!(owner.counters.rejected.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn submit_panic_does_not_kill_owner_and_disconnection_is_explicit() {
        let owner = Submitter::start(2).unwrap();
        owner
            .try_submit(Box::new(|| panic!("controlled submission panic")))
            .unwrap();
        let (tx, rx) = crossbeam_channel::bounded(1);
        owner
            .try_submit(Box::new(move || {
                tx.send(()).unwrap();
            }))
            .unwrap();
        rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
        assert_eq!(owner.counters.panics.load(Ordering::Relaxed), 1);
        let (tx, rx) = crossbeam_channel::bounded(1);
        drop(rx);
        let dead = Submitter {
            tx,
            counters: Arc::default(),
        };
        assert_eq!(
            dead.try_submit(Box::new(|| {})),
            Err("cold connection submit owner disconnected")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_queued_connect_drops_owned_future_without_starting_worker() {
        let owner = Arc::new(Submitter::start(2).unwrap());
        let (started_tx, started_rx) = crossbeam_channel::bounded(1);
        let (release_tx, release_rx) = crossbeam_channel::bounded(1);
        owner
            .try_submit(Box::new(move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }))
            .unwrap();
        started_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        struct DropFlag(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = DropFlag(dropped.clone());
        let polled_worker = polled.clone();
        let submit_owner = owner.clone();
        let mut task = tokio::spawn(crate::instrumented_http1::cold_connect_via(
            async move {
                let _flag = flag;
                polled_worker.store(true, Ordering::Release);
            },
            move |job| submit_owner.try_submit(job).map_err(std::io::Error::other),
        ));
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while owner.tx.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut task)
                .await
                .is_err()
        );
        task.abort();
        let _ = task.await;
        release_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !dropped.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!polled.load(Ordering::Acquire));
    }
}
