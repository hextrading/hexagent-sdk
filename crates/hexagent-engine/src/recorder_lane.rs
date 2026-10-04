//! One router owns the producer; one archive worker owns the consumer/writer.
//! FIFO 10,000 in production, preallocated, no sender clone. Full drops only the
//! archival copy; Exit alone may wait during shutdown. Counters are per lane,
//! never an account authority. Formatting, histograms and files stay on the
//! archive worker. No additional worker or unbounded recovery queue.
use crate::types::MarketEvent;
use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) const ARCHIVE_BATCH: usize = 256;

pub(crate) struct RecordedEvent {
    pub event: Arc<MarketEvent>,
    queued_at: Instant,
}

#[derive(Default)]
#[repr(align(64))]
struct ProducerCounters {
    admitted: AtomicU64,
    dropped: AtomicU64,
    disconnected: AtomicU64,
    sampled_high_water: AtomicUsize,
}

pub(crate) struct RecorderSender {
    tx: Sender<RecordedEvent>,
    counters: Arc<ProducerCounters>,
    admitted: u64,
    dropped: u64,
    disconnected: u64,
    high_water: usize,
    capacity: usize,
}

impl RecorderSender {
    pub fn forward(&mut self, event: Arc<MarketEvent>) {
        let terminal = matches!(event.as_ref(), MarketEvent::Exit);
        let envelope = RecordedEvent {
            event,
            queued_at: Instant::now(),
        };
        if terminal {
            // Shutdown barrier, excluded from market counters. All prior
            // producer stores finish before the consumer receives this Exit.
            let _ = self.tx.send(envelope);
            return;
        }
        match self.tx.try_send(envelope) {
            Ok(()) => {
                self.admitted += 1;
                self.counters
                    .admitted
                    .store(self.admitted, Ordering::Relaxed);
                self.observe_depth(self.tx.len());
            }
            Err(TrySendError::Full(_)) => {
                self.dropped += 1;
                self.counters.dropped.store(self.dropped, Ordering::Relaxed);
                self.observe_depth(self.capacity);
            }
            Err(TrySendError::Disconnected(_)) => {
                self.disconnected += 1;
                self.counters
                    .disconnected
                    .store(self.disconnected, Ordering::Relaxed);
            }
        }
    }

    fn observe_depth(&mut self, depth: usize) {
        if depth > self.high_water {
            self.high_water = depth;
            self.counters
                .sampled_high_water
                .store(depth, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct QueueSnapshot {
    schema_version: u32,
    run_ns: u64,
    timestamp_ns: u64,
    boundary: &'static str,
    final_report: bool,
    capacity: usize,
    depth: usize,
    sampled_high_water: usize,
    pub admitted: u64,
    pub consumed: u64,
    pub dropped: u64,
    pub disconnected: u64,
    pub write_errors: u64,
    queue_wait_max_ns: u64,
}

pub(crate) struct RecorderTelemetry {
    counters: Arc<ProducerCounters>,
    capacity: usize,
    consumed: u64,
    queue_wait_max_ns: u64,
    write_errors: u64,
    run_ns: u64,
    next_report: Instant,
    last_reported_drops: u64,
    manifest: Option<PathBuf>,
}

pub(crate) fn recorder_lane(
    capacity: usize,
) -> (RecorderSender, Receiver<RecordedEvent>, RecorderTelemetry) {
    assert!(capacity > 0);
    let (tx, rx) = bounded(capacity);
    let counters = Arc::new(ProducerCounters::default());
    let sender = RecorderSender {
        tx,
        counters: Arc::clone(&counters),
        admitted: 0,
        dropped: 0,
        disconnected: 0,
        high_water: 0,
        capacity,
    };
    let telemetry = RecorderTelemetry {
        counters,
        capacity,
        consumed: 0,
        queue_wait_max_ns: 0,
        write_errors: 0,
        run_ns: crate::types::now_ns(),
        next_report: Instant::now(),
        last_reported_drops: 0,
        manifest: None,
    };
    (sender, rx, telemetry)
}

impl RecorderTelemetry {
    /// Called only by the cold archive owner after creating the output root.
    pub fn set_output_dir(&mut self, dir: &Path) {
        self.manifest = Some(dir.join(format!("archive-queue-{}.json", self.run_ns)));
    }

    pub fn received(&mut self, message: RecordedEvent) -> Arc<MarketEvent> {
        if !matches!(message.event.as_ref(), MarketEvent::Exit) {
            self.consumed += 1;
            let age = message.queued_at.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            self.queue_wait_max_ns = self.queue_wait_max_ns.max(age);
            crate::latency::record_ns("market.recorder.queue_wait", age);
        }
        message.event
    }

    pub fn write_error(&mut self) {
        self.write_errors += 1;
    }

    pub fn snapshot(&self, depth: usize, final_report: bool) -> QueueSnapshot {
        QueueSnapshot { schema_version: 1, run_ns: self.run_ns, timestamp_ns: crate::types::now_ns(),
            boundary: "router_enqueue_to_archive_dequeue; counters exclude Exit; live snapshots weakly consistent; not a durable Parquet completeness claim",
            final_report, capacity: self.capacity, depth,
            sampled_high_water: self.counters.sampled_high_water.load(Ordering::Relaxed),
            admitted: self.counters.admitted.load(Ordering::Relaxed), consumed: self.consumed,
            dropped: self.counters.dropped.load(Ordering::Relaxed),
            disconnected: self.counters.disconnected.load(Ordering::Relaxed),
            write_errors: self.write_errors, queue_wait_max_ns: self.queue_wait_max_ns }
    }

    pub fn report(&mut self, depth: usize, finished: bool) {
        let now = Instant::now();
        if !finished && now < self.next_report {
            return;
        }
        self.next_report = now + Duration::from_secs(10);
        let snapshot = self.snapshot(depth, finished);
        // The existing async log sink batches these cold-worker records.
        if snapshot.dropped > self.last_reported_drops {
            log::warn!("[Recorder] live queue saturated; dropped_events_total={} action=preserve_strategy_latency", snapshot.dropped);
            self.last_reported_drops = snapshot.dropped;
        }
        match serde_json::to_vec(&snapshot) {
            Ok(bytes) => {
                log::info!("[recorder_queue] {}", String::from_utf8_lossy(&bytes));
                if let Some(path) = self.manifest.as_ref() {
                    let result = (|| -> std::io::Result<()> {
                        use std::io::Write;
                        let tmp = path.with_extension("json.tmp");
                        let mut file = std::fs::File::create(&tmp)?;
                        file.write_all(&bytes)?;
                        file.write_all(b"\n")?;
                        file.sync_all()?;
                        std::fs::rename(tmp, path)?;
                        std::fs::File::open(path.parent().expect("manifest parent"))?.sync_all()
                    })();
                    if let Err(error) = result {
                        self.write_errors += 1;
                        log::error!("[Recorder] queue manifest write failed: {error}");
                    }
                }
            }
            Err(error) => log::error!("[Recorder] queue metrics serialization failed: {error}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Exchange;

    fn event() -> Arc<MarketEvent> {
        Arc::new(MarketEvent::Connected {
            exchange: Exchange::Polymarket,
        })
    }

    #[test]
    fn fifo_duplicate_delivery_overflow_and_lanes_are_independent() {
        let (mut tx, rx, mut metrics) = recorder_lane(2);
        let (mut other, other_rx, other_metrics) = recorder_lane(1);
        let first = event();
        let second = event();
        tx.forward(Arc::clone(&first));
        tx.forward(Arc::clone(&second));
        for _ in 0..7 {
            tx.forward(event());
        }
        other.forward(event());
        assert_eq!(metrics.snapshot(rx.len(), false).dropped, 7);
        assert_eq!(other_metrics.snapshot(other_rx.len(), false).dropped, 0);
        assert!(Arc::ptr_eq(&metrics.received(rx.recv().unwrap()), &first));
        assert!(Arc::ptr_eq(&metrics.received(rx.recv().unwrap()), &second));
        // Repeated events are preserved; this lane never invents deduplication.
        tx.forward(Arc::clone(&first));
        assert!(Arc::ptr_eq(&metrics.received(rx.recv().unwrap()), &first));
        tx.forward(Arc::new(MarketEvent::Exit));
        assert!(matches!(
            metrics.received(rx.recv().unwrap()).as_ref(),
            MarketEvent::Exit
        ));
        let final_state = metrics.snapshot(0, true);
        assert_eq!(
            (
                final_state.admitted,
                final_state.consumed,
                final_state.dropped
            ),
            (3, 3, 7)
        );
        assert_eq!(final_state.sampled_high_water, 2);
    }

    #[test]
    fn disconnected_archive_is_counted_without_blocking_or_cross_lane_leak() {
        let (mut tx, rx, metrics) = recorder_lane(1);
        drop(rx);
        tx.forward(event());
        tx.forward(event());
        assert_eq!(metrics.snapshot(0, false).disconnected, 2);
        assert_eq!(metrics.snapshot(0, false).dropped, 0);
    }

    #[test]
    fn full_lane_exit_waits_only_for_shutdown_and_preserves_order() {
        let (mut tx, rx, mut metrics) = recorder_lane(1);
        let first = event();
        tx.forward(Arc::clone(&first));
        let join = std::thread::spawn(move || tx.forward(Arc::new(MarketEvent::Exit)));
        assert!(Arc::ptr_eq(&metrics.received(rx.recv().unwrap()), &first));
        assert!(matches!(
            metrics.received(rx.recv().unwrap()).as_ref(),
            MarketEvent::Exit
        ));
        join.join().unwrap();
        assert_eq!(metrics.snapshot(0, true).admitted, 1);
    }
    #[test]
    fn final_manifest_captures_trailing_drops_without_another_admitted_event() {
        let dir = std::env::temp_dir().join(format!(
            "archive-lane-{}-{}",
            std::process::id(),
            crate::types::now_ns()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let (mut tx, rx, mut metrics) = recorder_lane(1);
        metrics.set_output_dir(&dir);
        tx.forward(event());
        for _ in 0..9 {
            tx.forward(event());
        }
        metrics.received(rx.recv().unwrap());
        drop(tx);
        metrics.report(0, true);
        let state: serde_json::Value =
            serde_json::from_slice(&std::fs::read(metrics.manifest.as_ref().unwrap()).unwrap())
                .unwrap();
        assert_eq!(state["dropped"], 9);
        assert_eq!(state["admitted"], 1);
        assert_eq!(state["consumed"], 1);
        assert_eq!(state["final_report"], true);
        let (_, _, fresh) = recorder_lane(1);
        assert_eq!(
            fresh.snapshot(0, false).dropped,
            0,
            "restart counters must not leak between runs"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
    /// Synthetic archive CPU demand + three CPU-bound training surrogates.
    /// No sockets, keys, orders, live queues, files, or production-core load.
    /// Compare identical bounded lanes on shared versus independent CPUs.
    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "Linux controlled two-core performance experiment; explicit CPU env required"]
    fn benchmark_recorder_cpu_contention() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let cpu_text =
            std::env::var("HEXBOT_RECORDER_BENCH_CPUS").expect("set two non-trading CPUs");
        let cpus: Vec<usize> = cpu_text.split(',').map(|v| v.parse().unwrap()).collect();
        assert_eq!(cpus.len(), 2);
        assert_ne!(cpus[0], cpus[1]);
        crate::os_tune::pin_current(cpus[1], "archive-bench-producer");
        fn burn(rounds: u64) {
            let mut value = std::hint::black_box(17u64);
            for _ in 0..rounds {
                value =
                    std::hint::black_box(value.wrapping_mul(6364136223846793005).rotate_left(11));
            }
            std::hint::black_box(value);
        }
        let start = Instant::now();
        burn(10_000_000);
        let rounds = (10_000_000u128 * 300_000 / start.elapsed().as_nanos()).max(1) as u64;
        const N: usize = 45_000;
        const PERIOD_NS: u64 = 666_667;
        fn quantiles(values: &mut [u64]) -> serde_json::Value {
            values.sort_unstable();
            let n = values.len();
            serde_json::json!({"n":n,"median":values[(n-1)/2],"p99":values[(n-1)*99/100],
                "p999":values[(n-1)*999/1000],"maximum":values[n-1]})
        }
        let mut losses = Vec::new();
        for (label, consumer_cpu) in [("shared", cpus[0]), ("isolated", cpus[1])] {
            let stop = Arc::new(AtomicBool::new(false));
            let mut trainers = Vec::new();
            for _ in 0..3 {
                let flag = Arc::clone(&stop);
                let cpu = cpus[0];
                trainers.push(std::thread::spawn(move || {
                    crate::os_tune::pin_current(cpu, "archive-bench-training");
                    while !flag.load(Ordering::Relaxed) {
                        burn(50_000);
                    }
                }));
            }
            let (mut tx, rx, mut metrics) = recorder_lane(10_000);
            let consumer = std::thread::spawn(move || {
                crate::os_tune::pin_current(consumer_cpu, "archive-bench-recorder");
                let mut queue_wait = Vec::with_capacity(N);
                while let Ok(message) = rx.recv() {
                    queue_wait.push(message.queued_at.elapsed().as_nanos() as u64);
                    metrics.received(message);
                    burn(rounds);
                }
                (metrics.snapshot(0, true), queue_wait)
            });
            let event = event();
            let mut enqueue = Vec::with_capacity(N);
            let started = Instant::now();
            for i in 0..N {
                let due = started + Duration::from_nanos(i as u64 * PERIOD_NS);
                if let Some(delay) = due.checked_duration_since(Instant::now()) {
                    std::thread::sleep(delay);
                }
                let begin = Instant::now();
                tx.forward(Arc::clone(&event));
                enqueue.push(begin.elapsed().as_nanos() as u64);
            }
            drop(tx);
            // Training continues through the complete archive drain; do not
            // hide queue latency by removing the competing load at input end.
            let (snapshot, mut queue_wait) = consumer.join().unwrap();
            stop.store(true, Ordering::Relaxed);
            for worker in trainers {
                worker.join().unwrap();
            }
            assert_eq!(snapshot.admitted, snapshot.consumed);
            assert_eq!(snapshot.admitted + snapshot.dropped, N as u64);
            println!(
                "RECORDER_CONTENTION {}",
                serde_json::json!({"label":label,
                "producer_cpu":cpus[1],"training_cpu":cpus[0],"recorder_cpu":consumer_cpu,
                "capacity":10000,"input_n":N,"period_ns":PERIOD_NS,"cpu_work_rounds":rounds,
                "queue":snapshot,"enqueue_ns":quantiles(&mut enqueue),"queue_wait_ns":quantiles(&mut queue_wait),
                "elapsed_ms":started.elapsed().as_millis(),
                "boundary":"enqueue = recorder forward only; wait = before try_send to dequeue; three CPU-bound surrogates; no disk/network"})
            );
            losses.push(snapshot.dropped);
        }
        assert!(
            losses[0] > 0,
            "baseline must reproduce bounded-lane overload"
        );
        assert_eq!(
            losses[1], 0,
            "independent recorder CPU must keep up with the identical input"
        );
    }
}
