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

const MAX_STAGES: usize = 256;
const SUB_BUCKETS: usize = 8;
const BUCKETS: usize = 64 * SUB_BUCKETS;
static DROPPED_STAGE_REGISTRATIONS: AtomicU64 = AtomicU64::new(0);

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
        Self { bins, maxima }
    }

    #[inline]
    fn record(&self, stage_id: usize, ns: u64) {
        let bucket = latency_bucket(ns);
        self.bins[stage_id * BUCKETS + bucket].fetch_add(1, Ordering::Relaxed);
        self.maxima[stage_id].fetch_max(ns, Ordering::Relaxed);
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
    prepare_thread_stages(&[
        "market.root_overflow_drop",
        "polymarket.ws.clob_parse",
        "polymarket.ws.clob_runtime_scheduler_lag",
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

fn snapshot_and_reset() -> Vec<(&'static str, StageSnapshot)> {
    let names = stages().names();
    let telemetry = {
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
    };
    let mut snapshots = names
        .iter()
        .map(|_| StageSnapshot {
            bins: vec![0; BUCKETS],
            count: 0,
            max: 0,
        })
        .collect::<Vec<_>>();
    for recorder in telemetry {
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
            loop {
                crossbeam_channel::select! {
                    recv(shutdown_rx) -> _ => break,
                    recv(crossbeam_channel::after(interval)) -> _ => {}
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
    fn periodic_dump_is_woken_by_unified_shutdown() {
        let shutdown = crate::shutdown::ShutdownToken::new();
        let handle = spawn_periodic_dump(std::time::Duration::from_secs(3_600), shutdown.clone())
            .expect("latency dumper starts once");
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
