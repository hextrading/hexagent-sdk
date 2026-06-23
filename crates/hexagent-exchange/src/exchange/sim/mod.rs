//! Shared sim infrastructure retained after the v1 `SimExchange` removal.
//!
//! The v1 execution simulator (`SimExchange`) has been removed — every backtest
//! and paper-trading session now runs on `crate::exchange::sim_v2`. What remains
//! in this module is the latency modelling that sim_v2 builds on top of:
//!
//! - [`latency`]: empirical RTT CDF + AR(1) clustering + GPD tail, auto-calibrated
//!   from live logs (`calibrate_from_logs`).
//! - [`per_event_rtt`]: per-event RTT override tables for `sim_rtt_mode="exact"`.
//! - [`gpd`]: generalised-Pareto tail fitting used by `latency`.

pub mod calib_source;
pub mod gpd;
pub mod latency;
pub mod latency_record_replay;
pub mod per_event_rtt;

/// RTT-simulation mode selector. Both modes draw their source log(s)
/// from the single `sim_latency_calibrate_from` knob; the mode only
/// decides whether the per-event override layer is built on top of the
/// hourly-empirical model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SimRttMode {
    /// **Prediction mode** (default — backtest regression). RTT is
    /// drawn from the hourly-empirical CDF + AR(1) clustering model
    /// auto-calibrated from the log(s). Generalises across days, so
    /// it's the right model when replaying a window the log wasn't
    /// recorded on. No per-event matching.
    #[default]
    Predict,
    /// **Exact event-match mode** (sim test). On top of the hourly
    /// model, the engine builds a per-`event_id` RTT table from the
    /// same log(s) and, at each Polymarket event boundary, overrides
    /// (a) the sampler's place/cancel distribution with that event's
    /// live-observed quantiles and (b) the strategy gate's
    /// `prev_event_p60` (which drives the RTT-N scaling factor) with
    /// the live observation. Reproduces a specific recorded window
    /// faithfully; events absent from the table fall back to the
    /// hourly model.
    Exact,
}

impl SimRttMode {
    /// Parse from a TOML string. Case-insensitive; accepts
    /// `"exact"` / `"exact_match"` for [`SimRttMode::Exact`] and
    /// `"predict"` / `"prediction"` for [`SimRttMode::Predict`].
    /// Unknown / empty → `Predict` (the safe generalising default;
    /// the engine logs the resolved mode so a typo is visible).
    pub fn from_str(s: &str) -> Self {
        match s.to_ascii_lowercase().replace('-', "_").as_str() {
            "exact" | "exact_match" | "per_event" => SimRttMode::Exact,
            _ => SimRttMode::Predict,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            SimRttMode::Predict => "predict",
            SimRttMode::Exact => "exact",
        }
    }
}
