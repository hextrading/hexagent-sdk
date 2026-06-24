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

/// Optional engine subsystems a strategy needs. The engine queries these via
/// the registry INSTEAD of string-matching `cfg.name == "polymaker"`, so it
/// stays strategy-agnostic. A flag must be declared by exactly the strategies
/// the old name-check matched, so the gate yields the identical boolean.
#[derive(Debug, Default, Clone, Copy)]
pub struct StrategyCapabilities {
    /// Engine spawns an RTT-probe channel + stale-threshold handle (+ the BT
    /// per-event probe) for this strategy. (polymaker)
    pub needs_rtt_probe: bool,
    /// Engine pre-loads HAR historical bars (binance) for this strategy. (polymaker)
    pub needs_hist_bars: bool,
    /// Backtest sim seeds a per-instance USDC wallet for this strategy. (polymaker)
    pub needs_sim_wallet: bool,
    /// Engine builds a Polymarket SharedState + user-feed for this strategy. (polymaker)
    pub needs_poly_user_feed: bool,
    /// Engine spawns per-instance Hexmarket execution workers. (hexmaker)
    pub needs_hex_workers: bool,
}

/// Constructs one kind of strategy (by config `name`) from [`StrategyBuildDeps`].
/// Implemented in strategy crates (e.g. `PolymakerFactory` in `polymaker`).
pub trait StrategyFactory: Send + Sync {
    /// The config `name` this factory builds (e.g. `"polymaker"`).
    fn name(&self) -> &'static str;
    /// Build one instance, or `None` to skip it (with a logged reason).
    fn build(&self, deps: StrategyBuildDeps<'_>) -> Option<Box<dyn Strategy>>;
    /// Optional engine subsystems this strategy needs. Default: none.
    fn capabilities(&self) -> StrategyCapabilities {
        StrategyCapabilities::default()
    }
    /// Inject this strategy's required market-data symbols/feeds into the full
    /// config before the engine spawns exchange feeds. Default: no-op. Called
    /// once per enabled strategy instance from `Engine::new`. Implementors may
    /// gate on `full.general.mode`. (Replaces the engine's old
    /// `inject_polymaker_symbols` / `inject_hexmaker_symbols`.)
    fn inject_config(&self, _cfg: &StrategyConfig, _full: &mut Config) {}
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

    /// Capabilities declared by the factory for `name` (all-false if unknown).
    /// The engine queries this instead of `cfg.name == "polymaker"`.
    pub fn capabilities(&self, name: &str) -> StrategyCapabilities {
        self.factories.get(name).map(|f| f.capabilities()).unwrap_or_default()
    }

    /// Run each enabled strategy's `inject_config` against the full config
    /// before exchange feeds spawn (replaces the engine's inject_*_symbols).
    /// Iterates a clone of the strategy list to avoid borrowing `full.strategies`
    /// while mutating `full`.
    pub fn inject_all_config(&self, full: &mut Config) {
        let cfgs = full.strategies.clone();
        for cfg in &cfgs {
            if !cfg.enabled {
                continue;
            }
            if let Some(f) = self.factories.get(cfg.name.as_str()) {
                f.inject_config(cfg, full);
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

/// Inject a spot-MM strategy's price-feed symbols (binance + derived
/// coinbase / kraken / okx / bybit) into the full config so the engine
/// subscribes to them. LIVE-only — backtest/paper use explicitly configured
/// data sources. Used by the polymaker + index_price factories' `inject_config`.
/// Moved verbatim from the engine's old `inject_polymaker_symbols`.
pub fn inject_spot_feed_symbols(cfg: &StrategyConfig, full: &mut Config) {
    if full.general.mode != crate::config::RunMode::Live {
        return;
    }
    let binance_symbol = cfg
        .params
        .get("binance_symbol")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if binance_symbol.is_empty() {
        return;
    }
    let base = binance_symbol
        .strip_suffix("USDT")
        .unwrap_or(&binance_symbol)
        .to_string();
    let coinbase_symbol = format!("{}-USD", base);
    let kraken_symbol = format!("{}/USD", base);
    let okx_symbol = format!("{}-USDT", base);

    // Ensure binance exchange exists in config.
    let has_binance = full.exchanges.iter().any(|e| e.name == "binance");
    if !has_binance {
        full.exchanges.push(crate::config::ExchangeConfig {
            name: "binance".to_string(),
            enabled: true,
            symbols: vec![binance_symbol.clone()],
            api_key: String::new(),
            api_secret: String::new(),
            api_passphrase: String::new(),
            private_key: String::new(),
            mnemonic: String::new(),
            api_url_prefix: String::new(),
            wss_url: String::new(),
            max_connections: 1,
            rate_limit_per_second: 10,
            source: String::new(),
            btc_feed_id: String::new(),
            signature_type: String::new(),
            clob_version: String::new(),
            builder_code: String::new(),
            market_info_v2_path: String::new(),
            use_batch_orders: true,
            http_timeout_ms: 0,
            gap_replay_interval_ms: 2000,
            gap_replay_periodic_rewind_ms: 5000,
            gap_replay_reconnect_rewind_ms: 5000,
            executor_workers: 8,
        });
    } else {
        inject_exchange_symbol(full, "binance", &binance_symbol);
    }

    // Inject symbols for the other spot venues if they are configured.
    inject_exchange_symbol(full, "bybit", &binance_symbol); // bybit uses the binance format
    inject_exchange_symbol(full, "coinbase", &coinbase_symbol);
    inject_exchange_symbol(full, "kraken", &kraken_symbol);
    inject_exchange_symbol(full, "okx", &okx_symbol);
}

fn inject_exchange_symbol(full: &mut Config, exchange_name: &str, symbol: &str) {
    for exchange_cfg in &mut full.exchanges {
        if exchange_cfg.name == exchange_name && exchange_cfg.enabled {
            let sym = symbol.to_string();
            if !exchange_cfg.symbols.contains(&sym) {
                exchange_cfg.symbols.push(sym);
            }
        }
    }
}

/// Inject a hex/poly paired strategy's event slugs into the hexmarket +
/// polymarket exchange configs. All modes (matches the old behaviour). Used by
/// the hexmaker factory's `inject_config`. Moved verbatim from the engine's old
/// `inject_hexmaker_symbols`.
pub fn inject_hex_poly_event_symbols(cfg: &StrategyConfig, full: &mut Config) {
    let events = cfg
        .params
        .get("events")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let mut hex_slugs = Vec::new();
    let mut poly_slugs = Vec::new();
    for item in &events {
        if let Some(table) = item.as_table() {
            if let Some(hex) = table.get("hex").and_then(|v| v.as_str()) {
                hex_slugs.push(hex.to_string());
            }
            if let Some(poly) = table.get("poly").and_then(|v| v.as_str()) {
                poly_slugs.push(poly.to_string());
            }
        }
    }

    for exchange_cfg in &mut full.exchanges {
        match exchange_cfg.name.as_str() {
            "hexmarket" => {
                for slug in &hex_slugs {
                    if !exchange_cfg.symbols.contains(slug) {
                        exchange_cfg.symbols.push(slug.clone());
                    }
                }
            }
            "polymarket" => {
                for slug in &poly_slugs {
                    if !exchange_cfg.symbols.contains(slug) {
                        exchange_cfg.symbols.push(slug.clone());
                    }
                }
            }
            _ => {}
        }
    }

    if !hex_slugs.is_empty() || !poly_slugs.is_empty() {
        log::info!(
            "[Engine] Injected hexmaker symbols: hex={:?}, poly={:?}",
            hex_slugs, poly_slugs
        );
    }
}
