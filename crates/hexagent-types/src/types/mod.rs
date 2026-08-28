pub mod event;
pub mod instrument;
pub mod market;
pub mod order;

pub use event::*;
pub use instrument::*;
pub use market::*;
pub use order::*;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

/// Strategy-local simulated clock for backtest mode (0 = not set / live mode).
/// Server-lane replay must never write this clock.
static SIM_CLOCK_NS: AtomicU64 = AtomicU64::new(0);

/// Set the strategy-local simulated clock. Backtest engines call this only at
/// strategy callbacks or private order/trade delivery boundaries.
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

/// Process-local monotonic timestamp for cross-thread latency traces. Values
/// are meaningful only within one process generation and deliberately never
/// participate in exchange/business timestamps or persistence.
#[inline]
pub fn monotonic_now_ns() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    (EPOCH
        .get_or_init(Instant::now)
        .elapsed()
        .as_nanos()
        .min(u64::MAX as u128) as u64)
        .saturating_add(1)
}

/// Sim-clock-preferring "now": returns the BT sim clock when one is
/// installed (set by the BT engine on every replayed event), otherwise
/// falls back to wall-clock `now_ns`. This is the canonical clock for
/// anything that should be deterministic across BT runs — TTL stamping,
/// per-event RTT timestamps, async-fetch polling, in-flight tracking, etc.
///
/// **Live / Paper**: sim_clock_ns() returns None → always wall-clock →
/// behaviour unchanged from a direct `now_ns()` call.
///
/// **Backtest**: sim_clock_ns() returns the monotonic strategy-local replay or
/// private-delivery timestamp. Exchange/server-lane progress is intentionally
/// invisible to strategy code.
#[inline]
pub fn sim_or_wall_ns() -> u64 {
    sim_clock_ns().unwrap_or_else(now_ns)
}
