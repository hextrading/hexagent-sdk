//! Startup-owned cold executor: at most 64 queued+active connection futures.
//! One SCHED_OTHER runtime, nonblocking admission, caller-drop cancellation.
//! Socket registration stays on the original order reactor; no blocking worker
//! is retained for the duration of a handshake.
use futures_util::FutureExt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
const CAPACITY: usize = 64;
pub(crate) type Job = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
#[derive(Default)]
struct Counters {
    admitted: AtomicU64,
    dequeued: AtomicU64,
    rejected: AtomicU64,
    panics: AtomicU64,
    sampled_high_water: AtomicUsize,
    in_flight: AtomicUsize,
    in_flight_high_water: AtomicUsize,
}
struct Permit(Arc<Counters>);
impl Drop for Permit {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}
struct Submitter {
    tx: tokio::sync::mpsc::Sender<Job>,
    counters: Arc<Counters>,
    capacity: usize,
}
static SUBMITTER: OnceLock<Result<Submitter, String>> = OnceLock::new();
impl Submitter {
    fn start(capacity: usize) -> Result<Self, String> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Job>(capacity);
        let counters = Arc::new(Counters::default());
        let worker = Arc::clone(&counters);
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("ord-connect-tx".into())
            .spawn(move || {
                crate::os_tune::pin_cold_connect("ord-connect-tx");
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .max_blocking_threads(2)
                    .thread_name("ord-connect-dns")
                    .on_thread_start(|| crate::os_tune::pin_cold_connect("ord-connect-dns"))
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error.to_string()));
                        return;
                    }
                };
                crate::latency::prepare_scheduler_tail_queue();
                let _ = ready_tx.send(Ok(()));
                runtime.block_on(async move {
                    while let Some(job) = rx.recv().await {
                        worker.dequeued.fetch_add(1, Ordering::Relaxed);
                        tokio::spawn(job);
                    }
                });
            })
            .map_err(|e| e.to_string())?;
        ready_rx.recv().map_err(|e| e.to_string())??;
        Ok(Self {
            tx,
            counters,
            capacity,
        })
    }
    fn depth(&self) -> usize {
        self.capacity - self.tx.capacity()
    }
    fn try_submit(&self, job: Job) -> Result<(), &'static str> {
        let count =
            self.counters
                .in_flight
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                    (n < self.capacity).then_some(n + 1)
                });
        let count = match count {
            Ok(n) => n + 1,
            Err(_) => {
                self.counters.rejected.fetch_add(1, Ordering::Relaxed);
                return Err("cold connection active+queued capacity exhausted");
            }
        };
        self.counters
            .in_flight_high_water
            .fetch_max(count, Ordering::Relaxed);
        let permit = Permit(Arc::clone(&self.counters));
        let counters = Arc::clone(&self.counters);
        let wrapped = Box::pin(async move {
            // Also releases when an unpolled queued job is dropped.
            let _permit = permit;
            if std::panic::AssertUnwindSafe(job)
                .catch_unwind()
                .await
                .is_err()
            {
                counters.panics.fetch_add(1, Ordering::Relaxed);
            }
        });
        self.tx.try_send(wrapped).map_err(|_| {
            self.counters.rejected.fetch_add(1, Ordering::Relaxed);
            "cold connection owner unavailable"
        })?;
        self.counters.admitted.fetch_add(1, Ordering::Relaxed);
        self.counters
            .sampled_high_water
            .fetch_max(self.depth(), Ordering::Relaxed);
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
    prewarm().map_err(std::io::Error::other)?;
    SUBMITTER
        .get()
        .unwrap()
        .as_ref()
        .unwrap()
        .try_submit(job)
        .map_err(std::io::Error::other)
}
pub(crate) fn log_snapshot() {
    if let Some(Ok(owner)) = SUBMITTER.get() {
        let c = &owner.counters;
        log::info!("[cold_connect_submit] capacity={} depth={} sampled_high_water={} admitted={} dequeued={} rejected={} panics={} in_flight={} in_flight_high_water={}",
            CAPACITY, owner.depth(), c.sampled_high_water.load(Ordering::Relaxed),
            c.admitted.load(Ordering::Relaxed), c.dequeued.load(Ordering::Relaxed),
            c.rejected.load(Ordering::Relaxed), c.panics.load(Ordering::Relaxed),
            c.in_flight.load(Ordering::Relaxed), c.in_flight_high_water.load(Ordering::Relaxed));
    }
}
#[cfg(test)]
pub(crate) fn benchmark_snapshot() -> (usize, usize, u64, u64, u64, u64) {
    let o = SUBMITTER.get().unwrap().as_ref().unwrap();
    let c = &o.counters;
    (
        o.depth(),
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
    #[tokio::test]
    async fn bounded_active_connects_reject_overflow_and_recover_after_cancel() {
        let owner = Submitter::start(2).unwrap();
        let (a, b) = tokio::sync::oneshot::channel::<()>();
        let (c, d) = tokio::sync::oneshot::channel::<()>();
        owner
            .try_submit(Box::pin(async {
                let _ = b.await;
            }))
            .unwrap();
        owner
            .try_submit(Box::pin(async {
                let _ = d.await;
            }))
            .unwrap();
        assert!(owner
            .try_submit(Box::pin(async { panic!("rejected job ran") }))
            .is_err());
        assert_eq!(owner.counters.in_flight.load(Ordering::Acquire), 2);
        drop(a);
        drop(c);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while owner.counters.in_flight.load(Ordering::Acquire) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let (done, received) = tokio::sync::oneshot::channel();
        owner
            .try_submit(Box::pin(async move {
                let _ = done.send(std::thread::current().id());
            }))
            .unwrap();
        assert_ne!(received.await.unwrap(), std::thread::current().id());
    }
    #[tokio::test]
    async fn cancelled_caller_drops_handshake_and_releases_capacity() {
        struct DropFlag(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let owner = Arc::new(Submitter::start(1).unwrap());
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = DropFlag(dropped.clone());
        let target = owner.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(crate::instrumented_http1::cold_connect_via(
            async move {
                let _flag = flag;
                let _ = started.send(());
                std::future::pending::<()>().await;
            },
            move |job| target.try_submit(job).map_err(std::io::Error::other),
        ));
        ready.await.unwrap();
        task.abort();
        let _ = task.await;
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !dropped.load(Ordering::Acquire)
                || owner.counters.in_flight.load(Ordering::Acquire) != 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
