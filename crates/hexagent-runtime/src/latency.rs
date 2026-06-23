//! Latency instrumentation: per-stage HDR histograms + periodic dump.
//!
//! Design goals:
//!   1. Cheap to call at event boundaries (~100 ns per record, incl. lock).
//!      Safe to leave on in production.
//!   2. Thread-safe — callers are the async runtime thread, strategy
//!      engine thread, heartbeat, joiners, user-feed etc.
//!   3. Named stages are `&'static str` (no allocation on the hot path).
//!   4. Percentile queries on demand; a background thread periodically
//!      dumps a compact summary line per stage to the log.
//!
//! Usage from a hot path:
//!
//! ```ignore
//! use crate::latency;
//!
//! let t0 = latency::Instant::now();
//! // ... work ...
//! latency::record("polymarket.ws.parse", t0);
//! ```
//!
//! Or for spans that cross function boundaries, stash the start `Instant`
//! in whatever struct owns the work (e.g. `OrderRequest`, `HttpTask`)
//! and call `latency::record(stage, t0)` at the end.
//!
//! Clock source is `quanta::Instant` (TSC-based on x86_64, ~5 ns per
//! reading vs ~25 ns for `std::time::Instant`). We don't expose
//! `quanta::Instant` directly — instead we re-export it as
//! `latency::Instant` so callers don't care about the backend.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock, RwLock};

use hdrhistogram::Histogram;

pub use quanta::Instant;

/// Highest value we expect to ever record (1 minute in nanoseconds).
/// HDR histogram needs an upper bound; values above this are clamped
/// and counted as `high`.
const HISTOGRAM_MAX_NS: u64 = 60_000_000_000;
/// Three significant digits is the standard recommendation — gives ~0.1%
/// bucket accuracy across the full range with ~few KB per histogram.
const HISTOGRAM_SIGFIG: u8 = 3;

struct Registry {
    /// Maps stage name → its histogram. Read-mostly (first record for a
    /// new stage takes the write lock; steady state is all reads).
    stages: RwLock<HashMap<&'static str, Mutex<Histogram<u64>>>>,
}

impl Registry {
    fn new() -> Self {
        Self { stages: RwLock::new(HashMap::new()) }
    }

    fn record(&self, stage: &'static str, ns: u64) {
        let v = ns.min(HISTOGRAM_MAX_NS);
        // Fast path: read lock, find entry, take inner mutex briefly.
        {
            let guard = self.stages.read().expect("stages RwLock poisoned");
            if let Some(h) = guard.get(stage) {
                if let Ok(mut h) = h.lock() {
                    h.record(v).ok();
                }
                return;
            }
        }
        // Slow path: first sample for this stage — install it, then
        // drop the outer write guard before touching the inner mutex
        // so subsequent record() calls on other stages don't block.
        {
            let mut guard = self.stages.write().expect("stages RwLock poisoned");
            guard.entry(stage).or_insert_with(|| {
                Mutex::new(
                    Histogram::<u64>::new_with_bounds(1, HISTOGRAM_MAX_NS, HISTOGRAM_SIGFIG)
                        .expect("histogram bounds are valid"),
                )
            });
        }
        let guard = self.stages.read().expect("stages RwLock poisoned");
        if let Some(h) = guard.get(stage) {
            if let Ok(mut h) = h.lock() {
                h.record(v).ok();
            }
        }
    }

    /// Take a snapshot of every stage and reset them in place. Returns
    /// `(stage_name, histogram_snapshot)` pairs sorted by stage name.
    /// Resetting gives us per-interval (not cumulative) dumps, which
    /// are easier to read for spotting recent regressions.
    fn snapshot_and_reset(&self) -> Vec<(&'static str, Histogram<u64>)> {
        let guard = self.stages.read().expect("stages RwLock poisoned");
        let mut out: Vec<(&'static str, Histogram<u64>)> = Vec::with_capacity(guard.len());
        for (name, mu) in guard.iter() {
            let mut h = match mu.lock() {
                Ok(h) => h,
                Err(_) => continue,
            };
            if h.len() == 0 {
                continue; // skip stages with no new samples this interval
            }
            let snap = h.clone();
            h.reset();
            out.push((*name, snap));
        }
        out.sort_by_key(|(n, _)| *n);
        out
    }
}

static REGISTRY: OnceLock<Registry> = OnceLock::new();

fn registry() -> &'static Registry {
    REGISTRY.get_or_init(Registry::new)
}

/// Record the elapsed time from `start` to now under `stage`.
///
/// `stage` MUST be a `&'static str` — it's used as the registry key
/// without allocation.
#[inline]
pub fn record(stage: &'static str, start: Instant) {
    let elapsed_ns = start.elapsed().as_nanos() as u64;
    registry().record(stage, elapsed_ns);
}

/// Record a raw nanosecond duration (when you've computed it elsewhere
/// and don't have an `Instant`).
#[inline]
#[allow(dead_code)]
pub fn record_ns(stage: &'static str, ns: u64) {
    registry().record(stage, ns);
}

/// RAII guard: captures `Instant::now()` on construction and calls
/// `record(stage, start)` on drop. Handy for instrumenting functions
/// with many early-return paths without peppering the body with
/// `latency::record` calls at every exit.
///
/// ```ignore
/// fn quote_event(...) -> Vec<Signal> {
///     let _t = latency::TimedStage::new("polymarket.strategy.quote");
///     if some_gate_fails { return vec![]; }        // _t drops → recorded
///     // ... real work ...
///     signals                                      // _t drops → recorded
/// }
/// ```
///
/// Overhead is one `quanta::Instant::now()` plus one registry lookup +
/// HDR bucket update on drop — ~100 ns combined.
pub struct TimedStage {
    stage: &'static str,
    start: Instant,
}

impl TimedStage {
    #[inline]
    pub fn new(stage: &'static str) -> Self {
        Self { stage, start: Instant::now() }
    }
}

impl Drop for TimedStage {
    #[inline]
    fn drop(&mut self) {
        record(self.stage, self.start);
    }
}

/// Format a single histogram as a compact summary line.
fn format_line(stage: &str, h: &Histogram<u64>) -> String {
    let fmt = |ns: u64| -> String {
        if ns >= 1_000_000_000 {
            format!("{:.2}s", ns as f64 / 1_000_000_000.0)
        } else if ns >= 1_000_000 {
            format!("{:.2}ms", ns as f64 / 1_000_000.0)
        } else if ns >= 1_000 {
            format!("{:.1}us", ns as f64 / 1_000.0)
        } else {
            format!("{}ns", ns)
        }
    };
    // p85 is the most diagnostic body-of-distribution percentile for
    // Polymarket HTTP RTT: it sits past the fast-network mode (p50)
    // and before the cap-driven tail (p95+). Live14.log analysis
    // showed p85/p50 ≈ 5–10× — a signal that regime-switches into
    // server-stress show up at p85 about 30–60 minutes before they
    // become visible at p50. Emitting p85 lets operators monitor and
    // calibrators (`calibrate_from_log → SidedParams.p85_ms`) anchor
    // the body shape directly instead of interpolating between p50
    // and p95 in the 5-anchor CDF.
    format!(
        "[latency] {:<40} n={:<7} p50={} p85={} p95={} p99={} p99.9={} max={}",
        stage,
        h.len(),
        fmt(h.value_at_quantile(0.50)),
        fmt(h.value_at_quantile(0.85)),
        fmt(h.value_at_quantile(0.95)),
        fmt(h.value_at_quantile(0.99)),
        fmt(h.value_at_quantile(0.999)),
        fmt(h.max()),
    )
}

/// Spawn a background thread that periodically snapshots every stage
/// and logs a one-line summary. Idempotent — safe to call from main.
///
/// `interval` of 30-60s is a reasonable default. Each call resets the
/// histograms so each dump reflects the last interval's samples, not
/// cumulative since process start.
pub fn spawn_periodic_dump(interval: std::time::Duration) {
    static STARTED: OnceLock<()> = OnceLock::new();
    if STARTED.set(()).is_err() {
        return; // already started
    }
    std::thread::Builder::new()
        .name("latency-dump".into())
        .spawn(move || {
            crate::os_tune::pin_background("latency-dump");
            loop {
                std::thread::sleep(interval);
                let snap = registry().snapshot_and_reset();
                if snap.is_empty() {
                    continue;
                }
                for (stage, h) in &snap {
                    log::info!("{}", format_line(stage, h));
                }
            }
        })
        .expect("spawn latency-dump thread");
}
