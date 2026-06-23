//! Strategy construction registry.
//!
//! The engine builds the per-instance runtime dependencies (RTT-probe channel,
//! stale-threshold handle, Polymarket SharedState) and then asks the
//! [`StrategyRegistry`] to construct each configured strategy by name. The
//! engine therefore never names a concrete strategy type — the registry is
//! populated by the application (the `hexbot` bin) from the strategy crates.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};

use crate::config::{Config, StrategyConfig};
use crate::strategy::Strategy;

/// Per-instance RTT-probe wiring: (sample receiver, enable flag, active-token handle).
pub type RttProbeHandle = (
    crossbeam_channel::Receiver<f64>,
    Arc<AtomicBool>,
    hexagent_exchange::exchange::polymarket::rtt_probe::ActiveTokenHandle,
);

/// Per-instance Polymarket execution/user-feed state.
pub type PolySharedState = Arc<hexagent_exchange::exchange::polymarket::trade::SharedState>;

/// Everything a [`StrategyFactory`] needs to construct one strategy instance.
///
/// `cfg`/`full`/`bt_start_ns`/`strategy_index` are generic; the remaining
/// fields are optional Polymarket execution handles the engine installs in
/// live mode (empty in backtest/paper → legacy no-op). They are surfaced here
/// rather than generalised so the move stays byte-identical; a later cleanup
/// can hide them behind a capability bag.
pub struct StrategyBuildDeps<'a> {
    /// This strategy's `[[strategies]]` block.
    pub cfg: &'a StrategyConfig,
    /// The full loaded config (general / exchanges / backtest / recording).
    pub full: &'a Config,
    /// Backtest start timestamp (ns); 0 outside backtest. Pre-computed by the
    /// engine (replaces `self.parse_backtest_start_ns()` calls inside arms).
    pub bt_start_ns: u64,
    /// Position of this instance in the build order (legacy default-id naming).
    pub strategy_index: usize,
    /// This instance's RTT-probe channel, if the engine installed one.
    pub rtt_probe: Option<RttProbeHandle>,
    /// Whether the RTT-probe map is non-empty (drives the "missing channel" warn).
    pub rtt_probe_map_nonempty: bool,
    /// This instance's executor stale-threshold handle, if installed.
    pub stale_threshold: Option<Arc<AtomicU64>>,
    /// Whether the stale-threshold map is non-empty (drives the "missing" warn).
    pub stale_threshold_map_nonempty: bool,
    /// This instance's Polymarket SharedState (live only).
    pub poly_state: Option<PolySharedState>,
}

/// Constructs one kind of strategy (by config `name`) from [`StrategyBuildDeps`].
/// Implemented in strategy crates (e.g. `PolymakerFactory` in `polymaker`).
pub trait StrategyFactory: Send + Sync {
    /// The config `name` this factory builds (e.g. `"polymaker"`).
    fn name(&self) -> &'static str;
    /// Build one instance, or `None` to skip it (with a logged reason).
    fn build(&self, deps: StrategyBuildDeps<'_>) -> Option<Box<dyn Strategy>>;
}

/// Name → factory map the engine consults instead of `match cfg.name`.
#[derive(Default)]
pub struct StrategyRegistry {
    factories: HashMap<&'static str, Box<dyn StrategyFactory>>,
}

impl StrategyRegistry {
    pub fn new() -> Self { Self { factories: HashMap::new() } }

    /// Register a factory under its `name()`.
    pub fn register<F: StrategyFactory + 'static>(&mut self, factory: F) {
        self.factories.insert(factory.name(), Box::new(factory));
    }

    /// Build a strategy for `deps.cfg.name`, or warn + return `None` if no
    /// factory is registered for that name (mirrors the old `other =>` arm).
    pub fn build(&self, deps: StrategyBuildDeps<'_>) -> Option<Box<dyn Strategy>> {
        match self.factories.get(deps.cfg.name.as_str()) {
            Some(f) => f.build(deps),
            None => {
                log::warn!("Unknown strategy: {}", deps.cfg.name);
                None
            }
        }
    }
}

/// Parse the `index_exchanges` param (array of `{exchange, weight}` tables or
/// bare exchange-name strings) into `(name, weight)` pairs. Shared by the
/// polymaker and index_price factories. Moved verbatim from the engine.
pub fn parse_index_exchanges(cfg: &StrategyConfig) -> Vec<(String, f64)> {
    cfg.params.get("index_exchanges")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|item| {
            // Try table format: {exchange = "binance", weight = 2.0}
            if let Some(t) = item.as_table() {
                let ex = t.get("exchange")?.as_str()?.to_string();
                let w = t.get("weight")
                    .and_then(|v| v.as_float().or_else(|| v.as_integer().map(|i| i as f64)))
                    .unwrap_or(1.0);
                Some((ex, w))
            } else {
                // Simple string format: "binance"
                item.as_str().map(|s| (s.to_string(), 1.0))
            }
        }).collect())
        .unwrap_or_else(|| vec![
            ("binance".into(), 1.0), ("okx".into(), 1.0),
            ("coinbase".into(), 1.0), ("bybit".into(), 1.0),
        ])
}
