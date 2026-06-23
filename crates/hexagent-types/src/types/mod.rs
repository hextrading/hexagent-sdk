pub mod event;
pub mod instrument;
pub mod market;
pub mod order;

pub use event::*;
pub use instrument::*;
pub use market::*;
pub use order::*;

use std::sync::atomic::{AtomicU64, Ordering};

/// Global simulated clock for backtest mode (0 = not set / live mode).
static SIM_CLOCK_NS: AtomicU64 = AtomicU64::new(0);

/// Set the simulated clock (call with `local_timestamp_ns` from market events in backtest).
#[inline]
pub fn set_sim_clock(ns: u64) {
    SIM_CLOCK_NS.store(ns, Ordering::Relaxed);
}

/// Get the simulated clock value, or `None` if not set.
#[inline]
pub fn sim_clock_ns() -> Option<u64> {
    match SIM_CLOCK_NS.load(Ordering::Relaxed) {
        0 => None,
        ns => Some(ns),
    }
}

/// Get current timestamp in nanoseconds since UNIX epoch
#[inline]
pub fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

/// Sim-clock-preferring "now": returns the BT sim clock when one is
/// installed (set by the BT engine on every replayed event), otherwise
/// falls back to wall-clock `now_ns`. This is the canonical clock for
/// anything that should be deterministic across BT runs — TTL stamping,
/// rtt_gate timestamps, async-fetch polling, in-flight tracking, etc.
///
/// **Live / Paper**: sim_clock_ns() returns None → always wall-clock →
/// behaviour unchanged from a direct `now_ns()` call.
///
/// **Backtest**: sim_clock_ns() returns the parquet-replay timestamp →
/// all readers see the same value within an event tick → BT becomes
/// fully reproducible across runs (assuming all RNGs are seeded too).
#[inline]
pub fn sim_or_wall_ns() -> u64 {
    sim_clock_ns().unwrap_or_else(now_ns)
}
