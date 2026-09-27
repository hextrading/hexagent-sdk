//! Low-overhead latency instrumentation with thread-owned preallocated bins.
//!
//! The recording path never takes a process-global lock after a thread has
//! observed a stage for the first time. Each calling thread owns one fixed
//! telemetry slab. A background dumper reads and resets those atomic bins and
//! performs all percentile calculation and formatting off critical threads.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

pub use quanta::{Clock, Instant};

// Include recovery-only and queue breakdown stages without late registration
// drops. Fixed 2 MiB per recorder, allocated before worker readiness.
const MAX_STAGES: usize = 512;
const SUB_BUCKETS: usize = 8;
const BUCKETS: usize = 64 * SUB_BUCKETS;
static DROPPED_STAGE_REGISTRATIONS: AtomicU64 = AtomicU64::new(0);

/// CPU time of the calling worker, for separating work from scheduling waits.
/// Returns zero where unavailable; callers must not label that wall time as CPU.
pub fn thread_cpu_ns() -> u64 {
    #[cfg(target_os = "linux")]
    unsafe {
        let mut clock: libc::timespec = std::mem::zeroed();
        if libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut clock) == 0 {
            return (clock.tv_sec as u64)
                .saturating_mul(1_000_000_000)
                .saturating_add(clock.tv_nsec as u64);
        }
    }
    0
}

/// One process-wide mapping from static stage names to dense numeric IDs.
/// It is touched only on the first observation of a stage by each thread.
struct StageRegistry {
    state: Mutex<StageRegistryState>,
}

struct StageRegistryState {
    by_name: HashMap<&'static str, usize>,
    names: Vec<&'static str>,
}

impl StageRegistry {
    fn new() -> Self {
        Self {
            state: Mutex::new(StageRegistryState {
                by_name: HashMap::with_capacity(MAX_STAGES),
                names: Vec::with_capacity(MAX_STAGES),
            }),
        }
    }

    fn id(&self, stage: &'static str) -> Option<usize> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(id) = state.by_name.get(stage) {
            return Some(*id);
        }
        let id = state.names.len();
        if id >= MAX_STAGES {
            // Telemetry must never take down a business or maintenance task.
            // The caller caches this disabled registration, so the stage is
            // dropped without repeatedly entering the global registry.
            DROPPED_STAGE_REGISTRATIONS.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        state.names.push(stage);
        state.by_name.insert(stage, id);
        Some(id)
    }

    fn names(&self) -> Vec<&'static str> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .names
            .clone()
    }
}

static STAGES: OnceLock<StageRegistry> = OnceLock::new();

fn stages() -> &'static StageRegistry {
    STAGES.get_or_init(StageRegistry::new)
}

/// A fixed-size slab written by exactly one business thread. Atomic cells let
/// the background dumper snapshot/reset without pausing that owner.
struct ThreadTelemetry {
    bins: Box<[AtomicU64]>,
    maxima: Box<[AtomicU64]>,
    observations: OnceLock<ObservationQueue>,
}

const OBSERVATION_CAPACITY: usize = 65_536;
const OBSERVATION_DRAIN_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

/// One producer thread, existing latency-dump consumer. FIFO, advisory only:
/// saturation drops the new observation and counts it, never blocks a feed or
/// competes with private lifecycle queues. Allocated explicitly at startup.
struct ObservationQueue {
    queue: crate::try_queue::TryQueue<(usize, u64)>,
    owner: String,
    dropped: AtomicU64,
    high_water: AtomicU64,
    /// Consumer-owned interval count. Atomic because test snapshots may also
    /// consume it; never updated by the business producer.
    drained: AtomicU64,
}

impl ObservationQueue {
    fn new(capacity: usize) -> Self {
        Self {
            queue: crate::try_queue::TryQueue::new(capacity),
            owner: std::thread::current().name().unwrap_or("unnamed").into(),
            dropped: AtomicU64::new(0),
            high_water: AtomicU64::new(0),
            drained: AtomicU64::new(0),
        }
    }
    fn publish(&self, stage: usize, ns: u64) -> bool {
        self.high_water.fetch_max(
            self.queue
                .len()
                .saturating_add(1)
                .min(self.queue.capacity()) as u64,
            Ordering::Relaxed,
        );
        if self.queue.try_push((stage, ns)).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        true
    }
}

impl ThreadTelemetry {
    fn new() -> Self {
        let bins = std::iter::repeat_with(|| AtomicU64::new(0))
            .take(MAX_STAGES * BUCKETS)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let maxima = std::iter::repeat_with(|| AtomicU64::new(0))
            .take(MAX_STAGES)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            bins,
            maxima,
            observations: OnceLock::new(),
        }
    }

    #[inline]
    fn record(&self, stage_id: usize, ns: u64) {
        let bucket = latency_bucket(ns);
        self.bins[stage_id * BUCKETS + bucket].fetch_add(1, Ordering::Relaxed);
        self.maxima[stage_id].fetch_max(ns, Ordering::Relaxed);
    }

    /// Only latency-dump consumes this FIFO. Bound one pass to the observed
    /// depth so a continuously active producer cannot starve other threads.
    fn drain_observations(&self) {
        let Some(queue) = self.observations.get() else {
            return;
        };
        let mut drained = 0;
        for _ in 0..queue.queue.len().min(queue.queue.capacity()) {
            let Some((id, ns)) = queue.queue.try_pop() else {
                break;
            };
            if id < MAX_STAGES {
                self.record(id, ns);
            }
            drained += 1;
        }
        queue.drained.fetch_add(drained, Ordering::Relaxed);
    }
}

struct ThreadRecorder {
    telemetry: Arc<ThreadTelemetry>,
    /// Thread-local cache: no global stage-registry access after first use.
    stage_ids: HashMap<&'static str, Option<usize>>,
}

impl ThreadRecorder {
    fn new() -> Self {
        let telemetry = Arc::new(ThreadTelemetry::new());
        recorders()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(Arc::downgrade(&telemetry));
        Self {
            telemetry,
            stage_ids: HashMap::with_capacity(32),
        }
    }

    #[inline]
    fn record(&mut self, stage: &'static str, ns: u64) {
        let stage_id = self.stage_id(stage);
        if let Some(stage_id) = stage_id {
            self.telemetry.record(stage_id, ns);
        }
    }

    #[inline]
    fn stage_id(&mut self, stage: &'static str) -> Option<usize> {
        match self.stage_ids.get(stage) {
            Some(id) => *id,
            None => {
                let id = stages().id(stage);
                self.stage_ids.insert(stage, id);
                id
            }
        }
    }
}

thread_local! {
    static THREAD_RECORDER: RefCell<Option<ThreadRecorder>> = const { RefCell::new(None) };
}

static RECORDERS: OnceLock<Mutex<Vec<Weak<ThreadTelemetry>>>> = OnceLock::new();

fn recorders() -> &'static Mutex<Vec<Weak<ThreadTelemetry>>> {
    RECORDERS.get_or_init(|| Mutex::new(Vec::new()))
}

/// Eight logarithmic sub-buckets per power of two. The mapping is branch-light,
/// allocation-free, monotonic, and covers the complete u64 nanosecond range.
#[inline]
fn latency_bucket(ns: u64) -> usize {
    if ns <= 1 {
        return 0;
    }
    let exponent = 63usize.saturating_sub(ns.leading_zeros() as usize);
    let base = 1u64 << exponent;
    let fraction = (((ns - base) as u128 * SUB_BUCKETS as u128) / base as u128) as usize;
    (exponent * SUB_BUCKETS + fraction.min(SUB_BUCKETS - 1)).min(BUCKETS - 1)
}

#[inline]
fn bucket_upper_ns(bucket: usize) -> u64 {
    let exponent = bucket / SUB_BUCKETS;
    let fraction = bucket % SUB_BUCKETS;
    let base = 1u64 << exponent.min(63);
    let increment = ((base as u128 * (fraction + 1) as u128) / SUB_BUCKETS as u128)
        .min(u64::MAX as u128) as u64;
    base.saturating_add(increment).max(1)
}

/// Record elapsed time under a static stage name.
#[inline]
pub fn record(stage: &'static str, start: Instant) {
    record_ns(
        stage,
        start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
    );
}

/// Record a raw nanosecond duration.
#[inline]
pub fn record_ns(stage: &'static str, ns: u64) {
    THREAD_RECORDER.with(|slot| {
        let mut slot = slot.borrow_mut();
        let recorder = slot.get_or_insert_with(ThreadRecorder::new);
        recorder.record(stage, ns);
    });
}

/// Allocate the calling thread's fixed telemetry slab and resolve all stage
/// IDs before it enters a latency-sensitive loop. Subsequent `record_ns` calls
/// for these stages are lock-free and allocation-free.
pub fn prepare_thread_stages(stages: &[&'static str]) {
    THREAD_RECORDER.with(|slot| {
        let mut slot = slot.borrow_mut();
        let recorder = slot.get_or_insert_with(ThreadRecorder::new);
        for stage in stages {
            let _ = recorder.stage_id(stage);
        }
    });
}

/// Startup-only reservation of stages and the compact observation FIFO.
/// Percentiles/binning are performed by latency-dump, never the producer.
pub fn prepare_observation_stages(stages: &[&'static str]) {
    prepare_thread_stages(stages);
    THREAD_RECORDER.with(|slot| {
        slot.borrow()
            .as_ref()
            .unwrap()
            .telemetry
            .observations
            .get_or_init(|| ObservationQueue::new(OBSERVATION_CAPACITY));
    });
}

/// No implicit initialization: an unprepared thread/stage drops telemetry.
#[inline]
pub fn observe_ns(stage: &'static str, ns: u64) -> bool {
    THREAD_RECORDER.with(|slot| {
        let slot = slot.borrow();
        let Some(recorder) = slot.as_ref() else {
            return false;
        };
        let Some(Some(id)) = recorder.stage_ids.get(stage) else {
            return false;
        };
        recorder
            .telemetry
            .observations
            .get()
            .is_some_and(|queue| queue.publish(*id, ns))
    })
}

/// Fixed queue stages: parser-to-adapter and adapter-to-router use message
/// enqueue timestamps from the same process monotonic clock, never wall time.
pub fn prepare_market_queue_stages() {
    prepare_thread_stages(&[
        "market.adapter_queue.binance",
        "market.adapter_queue.coinbase",
        "market.adapter_queue.polymarket",
        "market.adapter_queue.other",
        "market.root_queue.binance",
        "market.root_queue.coinbase",
        "market.root_queue.polymarket",
        "market.root_queue.other",
    ]);
}

/// Prewarm every currently-declared Polymarket order-dispatch stage on an
/// execution or order-runtime thread. Keep this startup-only manifest next to
/// the recorder so new critical stages have one reviewable registration site.
pub fn prepare_polymarket_order_stages() {
    prepare_thread_stages(&[
        "polymarket.cancel.prep_to_http_dispatch",
        "polymarket.cancel.completion_queue",
        "polymarket.cancel.response_classify",
        "polymarket.cancel.response_handler",
        "polymarket.order.dispatch_to_lifecycle_done",
        "polymarket.order.completion_queue",
        "polymarket.order.prep_to_signed",
        "polymarket.order.quote_to_prep",
        "polymarket.order.request_buffer_pool_exhausted",
        "polymarket.order.reserve_to_http_dispatch",
        "polymarket.order.response_handler",
        "polymarket.order.response_parse",
        "polymarket.order.signed_to_reserve",
        "polymarket.http.body",
        "polymarket.http.connect",
        "polymarket.http.dns",
        "polymarket.http.initial_connect",
        "polymarket.http.reuse",
        "polymarket.http.slot_serialization_wait",
        "polymarket.http.tcp",
        "polymarket.http.tls",
        "polymarket.http.total_segmented",
        "polymarket.http.transparent_reconnect",
        "polymarket.http.ttfb",
    ]);
}

/// Prewarm private-feed parsing/routing/application stages. These execute on
/// the general runtime and account-owner workers, never on quote callbacks.
pub fn prepare_polymarket_private_stages() {
    prepare_thread_stages(&[
        "polymarket.account.lifecycle_apply",
        "polymarket.account.owner_started",
        "polymarket.account.settled_gc",
        "polymarket.gap_replay.http_body",
        "polymarket.gap_replay.json_decode",
        "polymarket.user.account_apply",
        "polymarket.user.account_order_log",
        "polymarket.user.account_resolve_anomaly",
        "polymarket.user.cold_commit_ack_overflow",
        "polymarket.user.cold_committed_skip",
        "polymarket.user.dispatch",
        "polymarket.user.event_parse",
        "polymarket.user.fast_route_to_account_owner",
        "polymarket.user.frame_total",
        "polymarket.user.json_parse",
        "polymarket.user.json_parse_cpu",
        "polymarket.user.json_parse_off_cpu",
        "polymarket.user.apply_enqueue",
        "polymarket.user.health_apply",
        "polymarket.user.terminal_high_water",
        "polymarket.user.trade_replay_anchor_apply",
        "polymarket.user.validate_route",
        "polymarket.user.validate_route_dispatch",
        "polymarket.user.validate_trade_fields",
        "polymarket.update.producer_to_root_router",
    ]);
}

/// Prewarm the dedicated public CLOB reader stages before socket polling.
pub fn prepare_polymarket_clob_stages() {
    prepare_observation_stages(&[
        "polymarket.ws.clob_source_age_at_publish",
        "polymarket.ws.clob_quote_wait_tick",
        "polymarket.ws.clob_quote_wait_deadline",
        "polymarket.ws.clob_bbo_wait_ready",
        "polymarket.ws.clob_bbo_wait_deadline",
        "polymarket.ws.clob_bbo_wait_snapshot",
        "polymarket.ws.clob_deferred_timer_late",
    ]);
    prepare_thread_stages(&[
        "market.root_overflow_drop",
        "polymarket.ws.clob_parse",
        "polymarket.ws.clob_bbo_settle",
        "polymarket.ws.clob_book_apply",
        "polymarket.ws.clob_book_canonicalization",
        "polymarket.ws.clob_event_construction",
        "polymarket.ws.clob_parse_apply",
        "polymarket.ws.clob_parse_apply_cpu",
        "polymarket.ws.clob_parse_apply_preempted",
        "polymarket.ws.clob_price_change_apply",
        "polymarket.ws.clob_quote_canonicalization",
        "polymarket.ws.clob_runtime_scheduler_lag",
        "polymarket.ws.clob_simd_json",
    ]);
}

/// RAII timing guard for functions with multiple exits.
pub struct TimedStage {
    stage: &'static str,
    start: Instant,
}

impl TimedStage {
    #[inline]
    pub fn new(stage: &'static str) -> Self {
        Self {
            stage,
            start: Instant::now(),
        }
    }
}

impl Drop for TimedStage {
    #[inline]
    fn drop(&mut self) {
        record(self.stage, self.start);
    }
}

#[derive(Default)]
struct StageSnapshot {
    bins: Vec<u64>,
    count: u64,
    max: u64,
}

fn live_recorders() -> Vec<Arc<ThreadTelemetry>> {
    let mut registered = recorders()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut live = Vec::with_capacity(registered.len());
    registered.retain(|weak| {
        if let Some(recorder) = weak.upgrade() {
            live.push(recorder);
            true
        } else {
            false
        }
    });
    live
}

fn snapshot_and_reset() -> Vec<(&'static str, StageSnapshot)> {
    let names = stages().names();
    let telemetry = live_recorders();
    let mut snapshots = names
        .iter()
        .map(|_| StageSnapshot {
            bins: vec![0; BUCKETS],
            count: 0,
            max: 0,
        })
        .collect::<Vec<_>>();
    for recorder in telemetry {
        recorder.drain_observations();
        if let Some(queue) = recorder.observations.get() {
            let drained = queue.drained.swap(0, Ordering::Relaxed);
            let high_water = queue.high_water.load(Ordering::Relaxed);
            if high_water != 0 {
                log::info!("[latency_observation_queue] owner={} capacity={} drained={} depth={} high_water={} dropped={}",
                    queue.owner, queue.queue.capacity(), drained, queue.queue.len(), high_water,
                    queue.dropped.load(Ordering::Relaxed));
            }
        }
        for stage_id in 0..names.len() {
            let snapshot = &mut snapshots[stage_id];
            let offset = stage_id * BUCKETS;
            for bucket in 0..BUCKETS {
                let value = recorder.bins[offset + bucket].swap(0, Ordering::AcqRel);
                snapshot.bins[bucket] = snapshot.bins[bucket].saturating_add(value);
                snapshot.count = snapshot.count.saturating_add(value);
            }
            snapshot.max = snapshot
                .max
                .max(recorder.maxima[stage_id].swap(0, Ordering::AcqRel));
        }
    }
    names
        .into_iter()
        .zip(snapshots)
        .filter(|(_, snapshot)| snapshot.count > 0)
        .collect()
}

fn value_at_quantile(snapshot: &StageSnapshot, quantile: f64) -> u64 {
    if snapshot.count == 0 {
        return 0;
    }
    let rank = ((snapshot.count as f64 * quantile.clamp(0.0, 1.0)).ceil() as u64).max(1);
    let mut seen = 0u64;
    for (bucket, count) in snapshot.bins.iter().enumerate() {
        seen = seen.saturating_add(*count);
        if seen >= rank {
            return bucket_upper_ns(bucket).min(snapshot.max.max(1));
        }
    }
    snapshot.max
}

fn format_duration(ns: u64) -> String {
    if ns >= 1_000_000_000 {
        format!("{:.2}s", ns as f64 / 1_000_000_000.0)
    } else if ns >= 1_000_000 {
        format!("{:.2}ms", ns as f64 / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.1}us", ns as f64 / 1_000.0)
    } else {
        format!("{}ns", ns)
    }
}

fn format_line(stage: &str, snapshot: &StageSnapshot) -> String {
    format!(
        "[latency] {:<40} n={:<7} p50={} p85={} p95={} p99={} p99.9={} max={}",
        stage,
        snapshot.count,
        format_duration(value_at_quantile(snapshot, 0.50)),
        format_duration(value_at_quantile(snapshot, 0.85)),
        format_duration(value_at_quantile(snapshot, 0.95)),
        format_duration(value_at_quantile(snapshot, 0.99)),
        format_duration(value_at_quantile(snapshot, 0.999)),
        format_duration(snapshot.max),
    )
}

static PERIODIC_DUMP_STARTED: AtomicBool = AtomicBool::new(false);

/// Periodically aggregates thread-owned slabs and logs percentile summaries.
///
/// The returned handle is joinable and the worker is woken immediately by the
/// run's unified shutdown token; it no longer survives a failed live runtime.
pub fn spawn_periodic_dump(
    interval: std::time::Duration,
    shutdown: crate::shutdown::ShutdownToken,
) -> Option<std::thread::JoinHandle<()>> {
    if PERIODIC_DUMP_STARTED
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return None;
    }
    let shutdown_rx = shutdown.subscribe();
    match std::thread::Builder::new()
        .name("latency-dump".into())
        .spawn(move || {
            crate::os_tune::pin_background("latency-dump");
            // Drain independently of the reporting interval: at 60-second
            // reports a 65,536-item FIFO otherwise overflows above 1,092/s.
            // Binning stays on this existing background worker; publication,
            // queue capacity and the feed's nonblocking overflow rule stay put.
            let interval = interval.max(std::time::Duration::from_millis(1));
            let drain_tick = crossbeam_channel::tick(interval.min(OBSERVATION_DRAIN_INTERVAL));
            let mut next_dump = std::time::Instant::now() + interval;
            loop {
                crossbeam_channel::select! {
                    recv(shutdown_rx) -> _ => break,
                    recv(drain_tick) -> _ => {}
                }
                for recorder in live_recorders() {
                    recorder.drain_observations();
                }
                if std::time::Instant::now() < next_dump {
                    continue;
                }
                let dropped = DROPPED_STAGE_REGISTRATIONS.swap(0, Ordering::AcqRel);
                if dropped > 0 {
                    log::warn!(
                        "[latency] stage_capacity_exhausted capacity={} dropped_registrations={} action=metrics_only_dropped",
                        MAX_STAGES,
                        dropped,
                    );
                }
                for (stage, snapshot) in snapshot_and_reset() {
                    log::info!("{}", format_line(stage, &snapshot));
                }
                next_dump = std::time::Instant::now() + interval;
            }
            PERIODIC_DUMP_STARTED.store(false, Ordering::Release);
        })
    {
        Ok(handle) => Some(handle),
        Err(error) => {
            PERIODIC_DUMP_STARTED.store(false, Ordering::Release);
            panic!("spawn latency-dump thread: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observation_fifo_is_bounded_ordered_and_thread_isolated() {
        let a = ObservationQueue::new(2);
        let b = ObservationQueue::new(2);
        assert!(a.publish(1, 10));
        assert!(a.publish(2, 20));
        assert!(!a.publish(3, 30));
        assert_eq!(a.dropped.load(Ordering::Relaxed), 1);
        assert_eq!(a.high_water.load(Ordering::Relaxed), 2);
        assert!(b.publish(4, 40));
        assert_eq!(a.queue.try_pop().unwrap(), (1, 10));
        assert_eq!(a.queue.try_pop().unwrap(), (2, 20));
        assert!(a.queue.try_pop().is_none());
        assert_eq!(b.queue.try_pop().unwrap(), (4, 40));
        assert_eq!(b.dropped.load(Ordering::Relaxed), 0);
        // Recovery after full: the next observation can enter normally.
        assert!(a.publish(5, 50));
        assert_eq!(a.queue.try_pop().unwrap(), (5, 50));
    }

    #[test]
    #[ignore = "release: compact observation producer cost and following dequeue"]
    fn benchmark_observation_publish() {
        prepare_observation_stages(&["benchmark.observation"]);
        let mut samples = Vec::with_capacity(100_000);
        let mut drained = Vec::with_capacity(100_000);
        for i in 0..100_256 {
            let start = std::time::Instant::now();
            assert!(observe_ns("benchmark.observation", 100));
            let published = start.elapsed().as_nanos() as u64;
            THREAD_RECORDER.with(|slot| {
                slot.borrow()
                    .as_ref()
                    .unwrap()
                    .telemetry
                    .observations
                    .get()
                    .unwrap()
                    .queue
                    .try_pop()
                    .unwrap();
            });
            if i >= 256 {
                samples.push(published);
                drained.push(start.elapsed().as_nanos() as u64);
            }
        }
        for (boundary, values) in [("publish", &mut samples), ("through_dequeue", &mut drained)] {
            values.sort_unstable();
            eprintln!("observation_probe boundary={boundary} N=100000 p50_ns={} p99_ns={} p999_ns={} max_ns={} high_water=1 overflow=0", values[49_999], values[98_999], values[99_899], values[99_999]);
        }
    }

    #[test]
    fn periodic_dump_is_woken_by_unified_shutdown() {
        prepare_observation_stages(&["latency.test.periodic_observation"]);
        assert!(observe_ns("latency.test.periodic_observation", 123));
        let shutdown = crate::shutdown::ShutdownToken::new();
        let handle = spawn_periodic_dump(std::time::Duration::from_secs(3_600), shutdown.clone())
            .expect("latency dumper starts once");
        let depth = || {
            THREAD_RECORDER.with(|slot| {
                slot.borrow()
                    .as_ref()
                    .unwrap()
                    .telemetry
                    .observations
                    .get()
                    .unwrap()
                    .queue
                    .len()
            })
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while depth() != 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert_eq!(
            depth(),
            0,
            "observation draining must not wait for the hourly report"
        );
        shutdown.request();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while !handle.is_finished() && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(
            handle.is_finished(),
            "latency dumper ignored shutdown token"
        );
        handle.join().unwrap();
    }

    #[test]
    fn frequent_observation_drain_preserves_bursts_until_report() {
        let telemetry = ThreadTelemetry::new();
        assert!(telemetry
            .observations
            .set(ObservationQueue::new(OBSERVATION_CAPACITY))
            .is_ok());
        let queue = telemetry.observations.get().unwrap();
        // 2,000 observations/s, 60 seconds: the old report-only drain loses
        // 54,464 samples. Four bounded passes per second retain all 120,000.
        for _ in 0..240 {
            for ns in 1..=500 {
                assert!(queue.publish(0, ns));
            }
            telemetry.drain_observations();
        }
        assert_eq!(queue.queue.len(), 0);
        assert_eq!(queue.high_water.load(Ordering::Relaxed), 500);
        assert_eq!(queue.dropped.load(Ordering::Relaxed), 0);
        assert_eq!(queue.drained.load(Ordering::Relaxed), 120_000);
        assert_eq!(
            telemetry.bins[..BUCKETS]
                .iter()
                .map(|v| v.load(Ordering::Relaxed))
                .sum::<u64>(),
            120_000
        );
        assert_eq!(telemetry.maxima[0].load(Ordering::Relaxed), 500);
        telemetry.drain_observations();
        assert_eq!(
            queue.drained.load(Ordering::Relaxed),
            120_000,
            "empty passes cannot duplicate samples"
        );
    }

    #[test]
    #[ignore = "release: 60-second logical workload, report-only versus 250ms draining"]
    fn benchmark_observation_report_cadence() {
        for frequent in [false, true] {
            let telemetry = ThreadTelemetry::new();
            assert!(telemetry
                .observations
                .set(ObservationQueue::new(OBSERVATION_CAPACITY))
                .is_ok());
            let queue = telemetry.observations.get().unwrap();
            let mut producer_ns = Vec::with_capacity(120_000);
            let mut drain_ns = Vec::with_capacity(240);
            for _ in 0..240 {
                for ns in 1..=500 {
                    let start = std::time::Instant::now();
                    std::hint::black_box(queue.publish(0, ns));
                    producer_ns.push(start.elapsed().as_nanos() as u64);
                }
                if frequent {
                    let start = std::time::Instant::now();
                    telemetry.drain_observations();
                    drain_ns.push(start.elapsed().as_nanos() as u64);
                }
            }
            if !frequent {
                let start = std::time::Instant::now();
                telemetry.drain_observations();
                drain_ns.push(start.elapsed().as_nanos() as u64);
            }
            for (boundary, values) in [
                ("producer_try_publish", &mut producer_ns),
                ("consumer_pass", &mut drain_ns),
            ] {
                values.sort_unstable();
                let at = |q: usize| values[(values.len() * q).div_ceil(1000) - 1];
                eprintln!("observation_cadence frequent={frequent} boundary={boundary} N={} p50_ns={} p99_ns={} p999_ns={} max_ns={} final_depth={} high_water={} overflow={} drained={} logical_input=120000 rate_per_second=2000", values.len(), at(500), at(990), at(999), at(1000), queue.queue.len(), queue.high_water.load(Ordering::Relaxed), queue.dropped.load(Ordering::Relaxed), queue.drained.load(Ordering::Relaxed));
            }
        }
    }

    #[test]
    fn logarithmic_buckets_are_monotonic_and_cover_u64() {
        let values = [
            0,
            1,
            2,
            3,
            10,
            999,
            1_000,
            1_000_000,
            60_000_000_000,
            u64::MAX,
        ];
        let mut previous = 0;
        for value in values {
            let bucket = latency_bucket(value);
            assert!(bucket >= previous);
            assert!(bucket < BUCKETS);
            previous = bucket;
        }
    }

    #[test]
    fn thread_local_recording_produces_tail_percentiles() {
        let stage = "latency.test.thread_local";
        for value in 1..=1_000u64 {
            record_ns(stage, value);
        }
        let snapshots = snapshot_and_reset();
        let snapshot = snapshots
            .iter()
            .find(|(name, _)| *name == stage)
            .map(|(_, snapshot)| snapshot)
            .expect("test stage snapshot");
        assert_eq!(snapshot.count, 1_000);
        assert!(value_at_quantile(snapshot, 0.50) >= 500);
        assert!(value_at_quantile(snapshot, 0.999) >= 900);
        assert_eq!(snapshot.max, 1_000);
    }

    #[test]
    fn stage_capacity_exhaustion_drops_telemetry_without_panicking() {
        let registry = StageRegistry::new();
        for index in 0..MAX_STAGES {
            let stage: &'static str =
                Box::leak(format!("latency.test.capacity.{index}").into_boxed_str());
            assert_eq!(registry.id(stage), Some(index));
        }
        assert_eq!(registry.id("latency.test.capacity.overflow"), None);
    }
}
