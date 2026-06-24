//! Engine — event loop, strategy dispatch, and thread management.
//!
//! Supports four modes:
//! - Live: exchange feeds → strategy → execution
//! - Record: exchange feeds → Parquet recorder
//! - Backtest: Parquet replay → strategy → sim_v2 DES
//! - Paper: live feeds → strategy → sim_v2 matching core

use anyhow::Result;
use crossbeam_channel::{bounded, Receiver, Sender};
use log::{error, info, warn};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use crate::config::{Config, RunMode};
use crate::exchange::binance::{BinanceMarket, BinanceTrade};
use crate::exchange::hexmarket::{HexmarketMarket, HexmarketTrade};
use crate::exchange::polymarket::{PolymarketMarket, PolymarketTrade};
use crate::exchange::{ExchangeMarket, ExchangeTrade};
use crate::recorder::{MarketRecorder, MarketReplayer};
use crate::strategy::Strategy;
use hexagent_strategy::factory::{StrategyBuildDeps, StrategyRegistry};
use crate::types::*;

const CHANNEL_CAPACITY: usize = 10_000;

pub struct Engine {
    config: Config,
    /// Strategy factories the application registered (the engine never names a
    /// concrete strategy type — see `build_strategies`).
    registry: StrategyRegistry,
}

impl Engine {
    pub fn new(mut config: Config, registry: StrategyRegistry) -> Self {
        crate::account::order_manager::init_global_order_id();
        // Each registered strategy injects its own required market-data symbols
        // (replaces the engine's old per-strategy-name inject_*_symbols).
        registry.inject_all_config(&mut config);
        Self { config, registry }
    }

    /// Backtest start timestamp (ns since epoch); 0 outside backtest mode.
    fn parse_backtest_start_ns(&self) -> u64 {
        if self.config.general.mode != RunMode::Backtest { return 0; }
        chrono::DateTime::parse_from_rfc3339(&self.config.backtest.start_date)
            .or_else(|_| chrono::NaiveDateTime::parse_from_str(&self.config.backtest.start_date, "%Y-%m-%dT%H:%M:%SZ")
                .map(|ndt| ndt.and_utc().fixed_offset()))
            .map(|dt| dt.with_timezone(&chrono::Utc).timestamp_nanos_opt().unwrap_or(0) as u64)
            .unwrap_or(0)
    }

    // ── Mode Execution (called from main.rs) ───────────────────────────

    pub fn run(&self) -> Result<()> {
        match self.config.general.mode {
            RunMode::Live => self.run_live(),
            RunMode::Record => self.run_record(),
            RunMode::Backtest => self.run_backtest(),
            RunMode::Paper => self.run_paper(),
        }
    }

    /// Spawn a market data recorder thread. Returns (sender, join handle).
    fn spawn_recorder_thread(&self) -> Result<(Sender<MarketEvent>, thread::JoinHandle<()>)> {
        self.spawn_recorder_thread_to(&self.config.recording.output_dir)
    }

    fn spawn_recorder_thread_to(&self, dir: &str) -> Result<(Sender<MarketEvent>, thread::JoinHandle<()>)> {
        let output_dir = std::fs::canonicalize(dir)
            .unwrap_or_else(|_| {
                let p = PathBuf::from(dir);
                let _ = std::fs::create_dir_all(&p);
                std::fs::canonicalize(&p).unwrap_or(p)
            })
            .to_string_lossy()
            .to_string();
        let (recorder_tx, recorder_rx) = bounded::<MarketEvent>(CHANNEL_CAPACITY);
        let handle = thread::Builder::new()
            .name("recorder".into())
            .spawn(move || {
                crate::os_tune::pin_background("recorder");
                let mut recorder = match MarketRecorder::new(PathBuf::from(&output_dir)) {
                    Ok(r) => r,
                    Err(e) => { error!("[Recorder] Failed to create: {}", e); return; }
                };
                let mut last_flush = std::time::Instant::now();
                let flush_interval = std::time::Duration::from_secs(60);
                // **Checkpoint cadence** (added 2026-05-20): every 5
                // minutes (clock-aligned to wall time, not elapsed),
                // close + sidecar-rename current parquet buffers so
                // their data becomes readable on disk before the
                // hour's `rotate_buffer` finally closes the canonical
                // path. Without this, hourly files stay un-footered
                // (and unreadable to downstream consumers) for up to
                // 60 minutes. Aligned via `next_checkpoint_unix_secs`
                // so multiple bot restarts in the same hour still
                // produce one checkpoint at each :05 / :10 / … mark.
                const CHECKPOINT_INTERVAL_SECS: u64 = 300;
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let mut next_checkpoint_unix_secs = (
                    (now_secs / CHECKPOINT_INTERVAL_SECS) + 1
                ) * CHECKPOINT_INTERVAL_SECS;
                loop {
                    match recorder_rx.recv_timeout(std::time::Duration::from_secs(5)) {
                        Ok(event) => {
                            if matches!(event, MarketEvent::Exit) { break; }
                            if let Err(e) = recorder.write_event(&event) {
                                error!("[Recorder] Write error: {}", e);
                            }
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                        Err(_) => break,
                    }
                    if last_flush.elapsed() >= flush_interval {
                        recorder.flush_buffers();
                        last_flush = std::time::Instant::now();
                    }
                    // Clock-aligned checkpoint at every :00 / :05 / :10 /…
                    let cur = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    if cur >= next_checkpoint_unix_secs {
                        recorder.checkpoint();
                        // Advance past the now-crossed boundary; if the
                        // bot was paused for > 5 min, skip past the
                        // backlog to avoid a checkpoint flood.
                        next_checkpoint_unix_secs = (
                            (cur / CHECKPOINT_INTERVAL_SECS) + 1
                        ) * CHECKPOINT_INTERVAL_SECS;
                    }
                }
                info!("[Recorder] Flushing {} events...", recorder.event_count());
                if let Err(e) = recorder.flush() {
                    error!("[Recorder] Flush error: {}", e);
                }
                info!("[Recorder] Finished: {} events written", recorder.event_count());
            })?;
        Ok((recorder_tx, handle))
    }

    /// LIVE / PAPER pre-flight: refuse to start when recorded spot warm-up
    /// data is stale. The prediction / apv2 warm-up replays recorded
    /// ORDERBOOK + TRADE parquet (websocket capture only); a gap to `now`
    /// cannot be back-filled from REST, so it would silently train the spot
    /// predictor on stale data — or, under `prediction_wait_for_model`,
    /// block quoting for ~one training window. HAR-RV bars are exempt
    /// (REST-klines self-heal in `load_hist_bars`). Aborts when any warm-up
    /// source's effective gap exceeds `[general] live_max_data_gap_secs`
    /// (`<= 0` disables).
    ///
    /// Mirrors the warm-up's data-dir selection so the gap we report is the
    /// one the warm-up will actually see: live reads ONLY
    /// `backtest.data_dir`; paper/other fall back to `paper_data_dir`, and
    /// `MarketReplayer` picks the FIRST dir with events inside the replay
    /// window. So per source we use the first dir whose newest event is
    /// within the prediction window; only if none qualifies do we report the
    /// freshest (still-stale) dir.
    fn check_warmup_data_freshness(&self) -> Result<()> {
        let mode = self.config.general.mode;
        let max_gap = self.config.general.live_max_data_gap_secs;
        if max_gap <= 0.0 {
            info!("[Engine] {} data-freshness pre-flight DISABLED (live_max_data_gap_secs <= 0)", mode);
            return Ok(());
        }
        // Same sources the prediction / apv2 warm-up replays. Empty ⇒ no
        // warm-up configured ⇒ nothing to gate on.
        let (sources, warmup_hours) = self.prediction_warmup_sources();
        if sources.is_empty() {
            return Ok(());
        }
        // Candidate dirs, in the same order spawn_strategy_thread builds
        // them: live = backtest.data_dir only; others add paper_data_dir
        // when it differs.
        let data_dir = PathBuf::from(&self.config.backtest.data_dir);
        let mut data_dirs = vec![data_dir.clone()];
        if mode != RunMode::Live {
            let paper_dir = PathBuf::from(&self.config.recording.paper_data_dir);
            if paper_dir != data_dir {
                data_dirs.push(paper_dir);
            }
        }
        let now_ns = crate::types::now_ns();
        // A dir whose newest event is within this window is the one
        // MarketReplayer would pick first (it discovers files by window
        // overlap). Use the prediction window — the recency-critical one.
        let window_ns = (warmup_hours * 3600.0 * 1e9) as u64;
        let fmt_ts = |ns: u64| -> String {
            chrono::DateTime::<chrono::Utc>::from_timestamp((ns / 1_000_000_000) as i64, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M UTC").to_string())
                .unwrap_or_else(|| "?".to_string())
        };
        let dirs_label = data_dirs.iter().map(|d| d.display().to_string()).collect::<Vec<_>>().join(", ");
        let mut worst: Option<(String, String, f64)> = None;
        for (exchange, symbol) in &sources {
            let mut effective: Option<u64> = None; // first in-window dir → warm-up uses it
            let mut freshest: Option<u64> = None; // newest across all dirs (diagnostic / fallback)
            for dir in &data_dirs {
                if let Some(latest) = crate::recorder::latest_recorded_ts_ns(dir, exchange, symbol) {
                    if freshest.map(|f| latest > f).unwrap_or(true) {
                        freshest = Some(latest);
                    }
                    if effective.is_none() && now_ns.saturating_sub(latest) <= window_ns {
                        effective = Some(latest);
                    }
                }
            }
            let gap_secs = match effective.or(freshest) {
                Some(latest_ns) => {
                    let gap = now_ns.saturating_sub(latest_ns) as f64 / 1e9;
                    info!(
                        "[Engine] data-freshness {}/{}: latest={} gap={:.1}h",
                        exchange, symbol, fmt_ts(latest_ns), gap / 3600.0
                    );
                    gap
                }
                None => {
                    warn!(
                        "[Engine] data-freshness {}/{}: NO recorded data under [{}]",
                        exchange, symbol, dirs_label
                    );
                    f64::INFINITY
                }
            };
            if worst.as_ref().map(|(_, _, g)| gap_secs > *g).unwrap_or(true) {
                worst = Some((exchange.clone(), symbol.clone(), gap_secs));
            }
        }
        if let Some((ex, sym, gap)) = worst {
            if gap > max_gap {
                let gap_label = if gap.is_finite() {
                    format!("{:.1}h", gap / 3600.0)
                } else {
                    "∞ (no recorded data)".to_string()
                };
                return Err(anyhow::anyhow!(
                    "{} pre-flight ABORT: spot warm-up data for {}/{} is stale by {} \
                     (limit {:.1}h via [general] live_max_data_gap_secs). Orderbook/trade \
                     history can't be back-filled from REST, so the spot predictor & apv2 \
                     baseline would warm up on stale data (or block quoting under \
                     prediction_wait_for_model). Record fresh data up to now \
                     (mode = \"record\") before starting, or raise / disable \
                     live_max_data_gap_secs.",
                    mode, ex, sym, gap_label, max_gap / 3600.0,
                ));
            }
        }
        info!("[Engine] {} data-freshness pre-flight OK (limit {:.1}h)", mode, max_gap / 3600.0);
        Ok(())
    }

    fn run_live(&self) -> Result<()> {
        info!("══════════════════════════════════════");
        info!("  Starting LIVE TRADING mode");
        info!("══════════════════════════════════════");

        // Pre-flight: abort BEFORE spawning recorder / feeds if recorded
        // spot warm-up data is too stale to warm the spot predictor / apv2
        // baseline (orderbook history can't be back-filled from REST). HAR
        // bars are exempt. See `check_warmup_data_freshness`.
        self.check_warmup_data_freshness()?;

        // ── Per-request place/cancel latency recording.
        //
        // Active when EITHER `[general] latency_record_enabled` (log
        // latencies during normal trading) OR `[general] all_probe` (the
        // no-trading probe session, which implies recording). The global
        // recorder is installed here; the actual rows are captured at the
        // SharedState http choke point (real quotes + probe alike).
        //
        // `all_probe` additionally turns the run into a pure
        // latency-measurement session: split/redeem disabled, all events
        // PROBE (no quoting) — wired in build_strategies.
        let all_probe = self.config.general.all_probe;
        let recording = all_probe || self.config.general.latency_record_enabled;
        if recording {
            let start_label = chrono::Local::now().format("%Y%m%d_%H%M%S").to_string();
            crate::latency_record::init(&self.config.general.latency_record, &start_label);
            if all_probe {
                warn!(
                    "[Engine] ALL-PROBE mode ENABLED — NO trading: split/redeem disabled, all \
                     events PROBE, place/cancel latency → {}/<UTC-date>.csv (daily UTC rotation)",
                    self.config.general.latency_record,
                );
            } else {
                info!(
                    "[Engine] latency_record ENABLED — per-request place/cancel latency → {}/<UTC-date>.csv (daily UTC rotation)",
                    self.config.general.latency_record,
                );
            }
        }

        let (market_tx, market_rx) = bounded::<MarketEvent>(CHANNEL_CAPACITY);
        let (signal_tx, signal_rx) = bounded::<Signal>(CHANNEL_CAPACITY);
        let (update_tx, update_rx) = bounded::<OrderUpdate>(CHANNEL_CAPACITY);

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_tx = market_tx.clone();

        // Periodic flush of the per-request latency CSV on each wall-clock
        // 5-min boundary. `maybe_flush` is a no-op until a boundary is
        // crossed (and entirely a no-op when recording is off), so this
        // tiny poll loop is cheap. A dedicated thread keeps flushing
        // independent of probe / trade activity.
        let latency_flush_handle: Option<thread::JoinHandle<()>> = if recording {
            let sd = shutdown.clone();
            thread::Builder::new()
                .name("latency-record-flush".into())
                .spawn(move || {
                    crate::os_tune::pin_background("latency-record-flush");
                    while !sd.load(std::sync::atomic::Ordering::Relaxed) {
                        std::thread::sleep(std::time::Duration::from_secs(2));
                        crate::latency_record::maybe_flush();
                    }
                })
                .ok()
        } else {
            None
        };

        // Spawn recorder for market data persistence
        let (recorder_tx, recorder_handle) = self.spawn_recorder_thread()?;

        // Build the per-instance Polymarket SharedState map. The
        // underlying h2 pool is shared across instances; auth, signer,
        // and order-id registry are per-instance (Phase 2a).
        let poly_states = self.build_poly_shared_states_map();

        let feed_handles = self.spawn_exchange_feeds(market_tx, shutdown.clone())?;

        // Stale-signal threshold handle — shared `Arc<AtomicU64>` between
        // the executor (reads on every signal arrival) and the strategy
        // (writes on each event boundary as part of the per-event RTT-N
        // scaling). Initial value = TOML polymaker.quote_interval_ms × 1.5,
        // matching the legacy startup-only behaviour for the first event.
        // The strategy keeps it in sync afterwards. Other strategies that
        // don't update it will see the static initial value, which is
        // equivalent to the pre-handle behaviour.
        // Phase 2e-4: per-instance stale-threshold map. Each polymaker
        // instance gets its own `Arc<AtomicU64>` (in ms), initialised
        // from that strategy's own `quote_interval_ms × 1.5`. Strategy
        // overwrites at each event boundary via the per-event RTT-N
        // scaling; executor reads at signal arrival using the
        // signal's instance_id.
        let stale_threshold_handles: HashMap<String, std::sync::Arc<std::sync::atomic::AtomicU64>> = {
            let mut m = HashMap::new();
            for sc in &self.config.strategies {
                if !sc.enabled || !self.registry.capabilities(&sc.name).needs_rtt_probe { continue; }
                if sc.instance_id.is_empty() { continue; }
                let iid = sc.instance_id.clone();
                let init_ms: u64 = sc.params.get("quote_interval_ms")
                    .and_then(|v| v.as_integer())
                    .map(|qi| ((qi.max(1) as f64) * 1.5).round() as u64)
                    .unwrap_or(150);
                m.insert(iid, std::sync::Arc::new(std::sync::atomic::AtomicU64::new(init_ms)));
            }
            m
        };

        let exec_handle = self.spawn_execution_thread_with_poly(
            signal_rx, update_tx.clone(), poly_states.clone(),
            stale_threshold_handles.clone(),
        );
        let user_feed_handle = self.spawn_hex_user_feed(update_tx.clone(), shutdown.clone());
        // Phase 2b: spawn one user_feed per polymarket instance.
        let poly_feed_handles = self.spawn_poly_user_feeds(
            update_tx, shutdown.clone(), &poly_states,
        );
        // Phase 2c: one heartbeat per instance.
        let heartbeat_handles = self.spawn_poly_heartbeats(shutdown.clone(), &poly_states);

        // RTT-probe wiring per polymaker instance (Phase 2d). Each
        // SharedState gets its own dedicated probe channel tuple:
        // (sample receiver, enable flag, active-token handle). The
        // engine then spawns one rtt_probe task per shared state and
        // hands the receiver/enable/token to the matching polymaker
        // strategy at `build_strategies` time, keyed by `instance_id`.
        //
        // Each strategy's quote_interval / probe_interval / event token
        // remains its own — two instances never cross-contaminate
        // RTT samples or probe state.
        let mut probe_install_map: HashMap<
            String,
            (
                crossbeam_channel::Receiver<f64>,
                std::sync::Arc<std::sync::atomic::AtomicBool>,
                crate::exchange::polymarket::rtt_probe::ActiveTokenHandle,
            ),
        > = HashMap::new();
        let mut probe_handles: Vec<thread::JoinHandle<()>> = Vec::new();
        {
            // Stable iteration order so log lines match heartbeat/feed
            // order from earlier in run_live.
            let mut keys: Vec<&String> = poly_states.keys().collect();
            keys.sort();
            for id in keys {
                let ps = match poly_states.get(id) { Some(s) => s.clone(), None => continue };
                // Probe interval is per-strategy: each polymaker entry
                // may set its own `rtt_gate_probe_interval_secs`.
                let interval_secs = self.config.strategies.iter()
                    .find(|s| {
                        self.registry.capabilities(&s.name).needs_rtt_probe
                            && s.enabled
                            && s.instance_id == *id
                    })
                    .and_then(|s| s.params.get("adaptive_params_v2_probe_interval_secs").or_else(|| s.params.get("rtt_gate_probe_interval_secs")))
                    .and_then(|v| v.as_float().or_else(|| v.as_integer().map(|i| i as f64)))
                    .unwrap_or(2.0)
                    .max(0.1);
                let (tx, rx) = crossbeam_channel::unbounded::<f64>();
                let enable = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                let active_token: crate::exchange::polymarket::rtt_probe::ActiveTokenHandle =
                    std::sync::Arc::new(std::sync::Mutex::new(None));
                match crate::exchange::polymarket::rtt_probe::spawn_rtt_probe(
                    ps,
                    enable.clone(),
                    tx,
                    active_token.clone(),
                    std::time::Duration::from_secs_f64(interval_secs),
                    shutdown.clone(),
                    all_probe,
                    id.clone(),
                ) {
                    Ok(h) => {
                        info!(
                            "[Engine] rtt_probe started for instance_id={} interval={:.1}s all_probe={}",
                            id, interval_secs, all_probe,
                        );
                        probe_handles.push(h);
                        probe_install_map.insert(id.clone(), (rx, enable, active_token));
                    }
                    Err(e) => {
                        warn!(
                            "[Engine] rtt_probe spawn failed for instance_id={}: {}",
                            id, e,
                        );
                    }
                }
            }
        }

        let strategy_handle = self.spawn_strategy_thread(
            market_rx, signal_tx, update_rx, false, Some(recorder_tx),
            probe_install_map,
            stale_threshold_handles.clone(),
            &poly_states,
        );

        Self::wait_for_shutdown(&shutdown, &shutdown_tx);

        let _ = strategy_handle.join();
        let _ = exec_handle.join();
        if let Some(h) = user_feed_handle { let _ = h.join(); }
        for h in poly_feed_handles { let _ = h.join(); }
        for h in heartbeat_handles { let _ = h.join(); }
        for h in probe_handles { let _ = h.join(); }
        for h in feed_handles { let _ = h.join(); }
        let _ = recorder_handle.join();
        if let Some(h) = latency_flush_handle { let _ = h.join(); }

        // Final flush of any buffered latency rows recorded after the
        // flush thread's last tick. No-op when recording is off.
        crate::latency_record::flush();

        info!("  All threads stopped, exiting");
        Ok(())
    }

    fn run_paper(&self) -> Result<()> {
        // Paper mode uses a fixed one-way latency derived from the
        // configured median RTT (sim_latency_p50_ms / 2). The
        // distribution sampler isn't wired into paper because it's
        // optimised for end-to-end ack timing rather than the fast
        // signal→fill loop the paper executor models.
        let sim_latency_ms = self.config.backtest.sim_latency_p50_ms / 2;
        info!("══════════════════════════════════════");
        info!("  Starting PAPER TRADING mode");
        info!("  sim_v2 matching core for Polymarket orders");
        info!("  sim_latency: {}ms (one-way, = sim_latency_p50_ms/2)", sim_latency_ms);
        info!("══════════════════════════════════════");

        // Pre-flight: abort before spawning feeds if recorded spot warm-up
        // data is too stale (same gate as live; orderbook history can't be
        // back-filled from REST). See `check_warmup_data_freshness`.
        self.check_warmup_data_freshness()?;

        let (market_tx, market_rx) = bounded::<MarketEvent>(CHANNEL_CAPACITY);
        let (sim_feed_tx, sim_feed_rx) = bounded::<MarketEvent>(CHANNEL_CAPACITY);
        let (signal_tx, signal_rx) = bounded::<Signal>(CHANNEL_CAPACITY);
        let (update_tx, update_rx) = bounded::<OrderUpdate>(CHANNEL_CAPACITY);

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_tx = market_tx.clone();

        // Live exchange feeds — Polymarket events also sent to sim_feed_tx for the sim_v2 core
        let feed_handles = self.spawn_exchange_feeds_paper(
            market_tx, Some(sim_feed_tx), shutdown.clone())?;

        // Paper execution: sim_v2 matching core fed by live Polymarket data.
        // `sim_latency_ms` was computed above from sim_latency_p50_ms/2.
        let exec_handle = Self::spawn_paper_execution_thread(
            signal_rx, sim_feed_rx, update_tx.clone(), sim_latency_ms,
            self.config.backtest.clone());

        // Spawn recorder for market data persistence (paper data goes to separate dir)
        let (recorder_tx, recorder_handle) = self.spawn_recorder_thread_to(&self.config.recording.paper_data_dir)?;

        // Strategy thread: same as live, data_dir = backtest.data_dir with paper_data_dir fallback
        // Paper mode: no RTT-probe (no real CLOB to probe). No stale-
        // threshold handle either — paper exec doesn't apply the gate.
        let strategy_handle = self.spawn_strategy_thread(
            market_rx, signal_tx, update_rx, false, Some(recorder_tx),
            HashMap::new(), HashMap::new(),
            // Paper mode has no live PM user feed (fills are sim-driven), so
            // the user-feed-health gates stay inactive (empty map).
            &HashMap::new());

        Self::wait_for_shutdown(&shutdown, &shutdown_tx);

        let _ = strategy_handle.join();
        let _ = exec_handle.join();
        for h in feed_handles { let _ = h.join(); }
        let _ = recorder_handle.join();

        info!("  All threads stopped, exiting");
        Ok(())
    }

    fn run_record(&self) -> Result<()> {
        info!("══════════════════════════════════════");
        info!("  Starting RECORD mode");
        info!("══════════════════════════════════════");

        // RECORD + all_probe: alongside market-data recording, also fire
        // the latency probe (real resting place + cancel) and log each
        // request's latency. `latency_record_enabled` (without all_probe)
        // has nothing to record in RECORD mode — there are no real trades
        // — so probing is gated on `all_probe` specifically.
        let all_probe = self.config.general.all_probe;
        if all_probe {
            let start_label = chrono::Local::now().format("%Y%m%d_%H%M%S").to_string();
            crate::latency_record::init(&self.config.general.latency_record, &start_label);
            warn!(
                "[Engine] RECORD + ALL-PROBE — recording market data AND firing place/cancel \
                 latency probes → {}/<UTC-date>.csv (daily UTC rotation)",
                self.config.general.latency_record,
            );
        }

        let (market_tx, market_rx) = bounded::<MarketEvent>(CHANNEL_CAPACITY);
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_tx = market_tx.clone();

        // Probe target token, shared by all probe tasks (one polymaker
        // series → one current event). Populated from the feed's
        // Instrument events inside the recorder loop (no strategy runs in
        // RECORD mode, so there's nothing else to set it).
        let probe_active_token: crate::exchange::polymarket::rtt_probe::ActiveTokenHandle =
            Arc::new(std::sync::Mutex::new(None));

        let feed_handles = self.spawn_exchange_feeds(market_tx, shutdown.clone())?;

        // Spawn one RTT-probe per configured polymaker instance (all
        // sharing `probe_active_token`). all_probe=true ⇒ fires
        // continuously and ignores the gate enable flag. Per-request
        // latency is recorded at the SharedState http choke point.
        let mut probe_handles: Vec<thread::JoinHandle<()>> = Vec::new();
        if all_probe {
            let poly_states = self.build_poly_shared_states_map();
            if poly_states.is_empty() {
                warn!(
                    "[Engine] RECORD + ALL-PROBE but no polymaker instances configured \
                     (need [[strategies]] instance_id + [poly.<id>] secrets) — no probes will fire",
                );
            }
            let mut keys: Vec<&String> = poly_states.keys().collect();
            keys.sort();
            for id in keys {
                let ps = match poly_states.get(id) { Some(s) => s.clone(), None => continue };
                let interval_secs = self.config.strategies.iter()
                    .find(|s| self.registry.capabilities(&s.name).needs_rtt_probe && s.enabled && s.instance_id == *id)
                    .and_then(|s| s.params.get("adaptive_params_v2_probe_interval_secs").or_else(|| s.params.get("rtt_gate_probe_interval_secs")))
                    .and_then(|v| v.as_float().or_else(|| v.as_integer().map(|i| i as f64)))
                    .unwrap_or(2.0)
                    .max(0.1);
                // Gate channel is unused in RECORD mode (no strategy to
                // drain it) — drop the receiver; all_probe sends are
                // best-effort and ignore the disconnected channel.
                let (tx, _rx) = crossbeam_channel::unbounded::<f64>();
                let enable = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
                match crate::exchange::polymarket::rtt_probe::spawn_rtt_probe(
                    ps, enable, tx, probe_active_token.clone(),
                    std::time::Duration::from_secs_f64(interval_secs),
                    shutdown.clone(), true, id.clone(),
                ) {
                    Ok(h) => {
                        info!("[Engine] rtt_probe started for instance_id={} interval={:.1}s all_probe=true", id, interval_secs);
                        probe_handles.push(h);
                    }
                    Err(e) => warn!("[Engine] rtt_probe spawn failed for instance_id={}: {}", id, e),
                }
            }
        }

        // Periodic 5-min-aligned flush of the latency CSV (no-op until a
        // boundary is crossed / when recording is off).
        let latency_flush_handle: Option<thread::JoinHandle<()>> = if all_probe {
            let sd = shutdown.clone();
            thread::Builder::new()
                .name("latency-record-flush".into())
                .spawn(move || {
                    crate::os_tune::pin_background("latency-record-flush");
                    while !sd.load(std::sync::atomic::Ordering::Relaxed) {
                        std::thread::sleep(std::time::Duration::from_secs(2));
                        crate::latency_record::maybe_flush();
                    }
                })
                .ok()
        } else {
            None
        };
        // Handed to the recorder loop so it can keep the probe's target
        // token fresh from Instrument events.
        let token_for_recorder = if all_probe { Some(probe_active_token.clone()) } else { None };

        // all_probe restricts probing to the FIRST configured polymarket
        // series only. The `[[exchanges]] polymarket` `symbols` list may
        // hold several series (e.g. btc/eth/sol 5m + 15m + hourly), but
        // one resting-order RTT sample per probe interval is all the
        // latency CSV needs — there's no value in cycling the single probe
        // through every series. We gate the probe-target update on the
        // first series' EventStart so the probe locks onto series[0].
        // `None` (no polymarket configured) ⇒ no gating (probe-target
        // logic never runs anyway, since token_for_recorder is None).
        let first_poly_series: Option<String> = if all_probe {
            self.config.exchanges.iter()
                .find(|e| e.name == "polymarket" && e.enabled)
                .and_then(|e| e.symbols.first())
                .cloned()
        } else {
            None
        };
        if let Some(first) = &first_poly_series {
            info!(
                "[Engine] RECORD + ALL-PROBE — probe target locked to first polymarket series '{}' \
                 (other configured series are recorded but not probed)",
                first,
            );
        }

        let output_dir = std::fs::canonicalize(&self.config.recording.output_dir)
            .unwrap_or_else(|_| {
                let p = PathBuf::from(&self.config.recording.output_dir);
                let _ = std::fs::create_dir_all(&p);
                std::fs::canonicalize(&p).unwrap_or(p)
            })
            .to_string_lossy()
            .to_string();

        let recorder_handle = thread::Builder::new()
            .name("recorder".into())
            .spawn(move || {
                crate::os_tune::pin_background("recorder");
                let mut recorder = match MarketRecorder::new(PathBuf::from(&output_dir)) {
                    Ok(r) => r,
                    Err(e) => { error!("[Recorder] Failed to create: {}", e); return; }
                };
                let mut last_flush = std::time::Instant::now();
                let flush_interval = std::time::Duration::from_secs(60);
                // Same 5-min checkpoint cadence as the live recorder
                // loop — see comment there for rationale.
                const CHECKPOINT_INTERVAL_SECS: u64 = 300;
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let mut next_checkpoint_unix_secs = (
                    (now_secs / CHECKPOINT_INTERVAL_SECS) + 1
                ) * CHECKPOINT_INTERVAL_SECS;
                // all_probe: track the current polymarket event's Up/Down
                // tokens + their latest best asks so the probe targets the
                // high-priced side (its deep BUY @ 0.01 then rests far
                // below the book; see rtt_probe::pick_probe_side). The
                // strategy path does this off its orderbook_manager —
                // RECORD has no strategy, so we keep a tiny ask cache here.
                let mut probe_up: Option<String> = None;
                let mut probe_down: Option<String> = None;
                let mut probe_up_ask: Option<f64> = None;
                let mut probe_down_ask: Option<f64> = None;
                // Series of the most recent EventStart. Each series refresh
                // emits EventStart then that series' Instruments as one
                // contiguous FIFO block (see PolymarketMarket::next_event),
                // so this reliably tags the Instruments that follow. Used
                // to gate the probe target onto `first_poly_series` only.
                let mut current_event_series: Option<String> = None;
                loop {
                    match market_rx.recv_timeout(std::time::Duration::from_secs(5)) {
                        Ok(event) => {
                            if matches!(event, MarketEvent::Exit) { break; }
                            // all_probe: keep the probe's target fresh — the
                            // current polymarket event's high-priced side.
                            if let Some(tok) = &token_for_recorder {
                                let mut repick = false;
                                match &event {
                                    // Tag the series of the Instruments that
                                    // follow, so the probe-target gate below
                                    // can restrict to `first_poly_series`.
                                    MarketEvent::EventStart { exchange, symbol, .. }
                                        if *exchange == crate::types::Exchange::Polymarket =>
                                    {
                                        current_event_series = Some(symbol.clone());
                                    }
                                    // Only the FIRST configured series drives
                                    // the probe target. `first_poly_series` =
                                    // None ⇒ undetermined ⇒ don't gate (keep
                                    // prior behaviour of tracking any series).
                                    MarketEvent::Instrument(crate::types::Instrument::BinaryOption(bo))
                                        if bo.exchange == crate::types::Exchange::Polymarket
                                            && first_poly_series.as_deref().is_none_or(|first| {
                                                current_event_series.as_deref() == Some(first)
                                            }) =>
                                    {
                                        let find = |name: &str| bo.clob_token_ids.iter()
                                            .zip(bo.outcomes.iter())
                                            .find(|(_, n)| n.as_str() == name)
                                            .map(|(t, _)| t.clone());
                                        probe_up = find("Up");
                                        probe_down = find("Down");
                                        // New event: asks not yet known →
                                        // bootstrap to Up until books arrive.
                                        probe_up_ask = None;
                                        probe_down_ask = None;
                                        repick = probe_up.is_some();
                                    }
                                    MarketEvent::OrderBook(ob)
                                        if ob.exchange == crate::types::Exchange::Polymarket =>
                                    {
                                        let ask = ob.asks.first().map(|l| l.price);
                                        if Some(&ob.symbol) == probe_up.as_ref() {
                                            probe_up_ask = ask;
                                            repick = true;
                                        } else if Some(&ob.symbol) == probe_down.as_ref() {
                                            probe_down_ask = ask;
                                            repick = true;
                                        }
                                    }
                                    _ => {}
                                }
                                if repick {
                                    if let Some(up) = probe_up.as_deref() {
                                        let down = probe_down.as_deref().unwrap_or(up);
                                        let chosen = crate::exchange::polymarket::rtt_probe::pick_probe_side(
                                            up, probe_up_ask, down, probe_down_ask,
                                        ).to_string();
                                        if let Ok(mut g) = tok.lock() { *g = Some(chosen); }
                                    }
                                }
                            }
                            if let Err(e) = recorder.write_event(&event) {
                                error!("[Recorder] Write error: {}", e);
                            }
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                        Err(_) => break, // channel closed
                    }
                    // Periodic flush: write row groups every 60s to free memory
                    if last_flush.elapsed() >= flush_interval {
                        recorder.flush_buffers();
                        last_flush = std::time::Instant::now();
                    }
                    let cur = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    if cur >= next_checkpoint_unix_secs {
                        recorder.checkpoint();
                        next_checkpoint_unix_secs = (
                            (cur / CHECKPOINT_INTERVAL_SECS) + 1
                        ) * CHECKPOINT_INTERVAL_SECS;
                    }
                }
                info!("[Recorder] Flushing {} events...", recorder.event_count());
                if let Err(e) = recorder.flush() {
                    error!("[Recorder] Flush error: {}", e);
                }
                info!("[Recorder] Finished: {} events written", recorder.event_count());
            })?;

        Self::wait_for_shutdown(&shutdown, &shutdown_tx);
        // Drop our copy of market_tx so channel closes when feeds exit
        drop(shutdown_tx);
        // Wait for feed threads to exit first (releases their market_tx clones)
        for h in feed_handles { let _ = h.join(); }
        // Now recorder sees channel closed or Exit message → flushes and exits
        let _ = recorder_handle.join();
        // All-probe teardown: stop the probes + latency flush, final flush.
        for h in probe_handles { let _ = h.join(); }
        if let Some(h) = latency_flush_handle { let _ = h.join(); }
        crate::latency_record::flush();

        info!("  All threads stopped, exiting");
        Ok(())
    }

    /// Backtest driver — the sim_v2 first-principles DES (feed + clock +
    /// unified-wall-clock scheduler + bidirectional RTT latency + matching).
    ///
    /// Hybrid architecture (see `docs/sim_v2_design.md`): the engine keeps the
    /// strat-lane setup + dispatch (so bars/RTDS/chainlink/multi-instance behave
    /// as in live), while the server-axis DES + order lifecycle + RTT latency
    /// live inside `sim_v2::Simulator`. The driver loop merges the strat lane
    /// (local_ts) against `sim.peek_when()` (unified wall clock: server market
    /// events + my-order arrivals + ack deliveries).
    fn run_backtest(&self) -> Result<()> {
        use std::collections::BinaryHeap;
        use crate::exchange::sim_v2::{SimV2Config, Simulator};

        let bt = &self.config.backtest;

        // Determinism: `Engine::new` seeded the global order-id counter from
        // wall-clock (live collision-avoidance). A backtest must instead be
        // byte-identical across runs — coids are FNV-hashed into the sim's
        // per-order Bernoullis (taker-capture), so a wall-clock coid base
        // reshuffles fills every run (±0.3% edge/vol noise). Re-seed from the
        // sim seed; change `sim_latency_seed` for independent replicates.
        crate::account::order_manager::init_global_order_id_seeded(bt.sim_latency_seed);

        let parse_dt = |s: &str| -> Result<chrono::DateTime<chrono::Utc>> {
            chrono::DateTime::parse_from_rfc3339(s)
                .or_else(|_| {
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%SZ")
                        .map(|ndt| ndt.and_utc().fixed_offset())
                })
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .map_err(|e| anyhow::anyhow!("Invalid date '{}': {}", s, e))
        };
        let unbounded_start = parse_dt("2020-01-01T00:00:00Z").unwrap();
        let unbounded_end = parse_dt("2099-12-31T23:59:59Z").unwrap();
        let start_time = if bt.start_date.is_empty() { unbounded_start } else { parse_dt(&bt.start_date)? };
        let end_time = if bt.end_date.is_empty() {
            if bt.start_date.is_empty() { unbounded_end } else { parse_dt(&bt.start_date)? + chrono::TimeDelta::days(1) }
        } else {
            parse_dt(&bt.end_date)?
        };
        let start_ns = start_time.timestamp_nanos_opt().unwrap_or(0) as u64;
        let end_ns = end_time.timestamp_nanos_opt().unwrap_or(0) as u64;

        let mut replay_sources: Vec<(String, String)> = Vec::new();
        for ex_cfg in &self.config.exchanges {
            if !ex_cfg.enabled { continue; }
            for sym in &ex_cfg.symbols {
                replay_sources.push((ex_cfg.name.clone(), sym.clone()));
            }
        }
        let data_dir = bt.data_dir.clone();
        let data_path = PathBuf::from(&data_dir);
        let start_dt = start_time;
        let end_dt = end_time;

        info!("══════════════════════════════════════");
        info!("  BACKTEST mode (sim_v2)");
        info!("══════════════════════════════════════");

        // ── Strat-lane replayers (local_ts order) — verbatim from v1 ──
        let mut strat_replayers: Vec<MarketReplayer> = Vec::new();
        for (exchange, symbol) in &replay_sources {
            if symbol.starts_with("rtds:") || exchange == "binance_futures"
                || exchange == "chainlink" || exchange == "pyth" { continue; }
            if let Ok(r) = MarketReplayer::new(&data_path, exchange, symbol, start_dt, end_dt) {
                strat_replayers.push(r);
            }
        }
        for (_exchange, symbol) in &replay_sources {
            let rtds_rest = match symbol.strip_prefix("rtds:") { Some(r) => r, None => continue };
            let parts: Vec<&str> = rtds_rest.splitn(2, ':').collect();
            if parts.len() != 2 { continue; }
            let source = parts[0];
            for filter in parts[1].split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
                let sym_lower = filter.to_lowercase().replace('/', "-");
                let rtds_path = format!("{}/{}", source, sym_lower);
                let rtds_end_dt = end_dt + chrono::TimeDelta::seconds(10);
                if let Ok(r) = MarketReplayer::new(&data_path, "rtds", &rtds_path, start_dt, rtds_end_dt) {
                    strat_replayers.push(r);
                }
            }
        }
        for (exchange, symbol) in &replay_sources {
            if exchange != "chainlink" && exchange != "pyth" { continue; }
            let sym_lower = symbol.to_lowercase().replace('/', "-");
            let early = if exchange == "chainlink" { 10 } else { 0 };
            let start = start_dt - chrono::TimeDelta::seconds(early);
            let end = end_dt + chrono::TimeDelta::seconds(10);
            if let Ok(r) = MarketReplayer::new(&data_path, exchange, &sym_lower, start, end) {
                strat_replayers.push(r);
            }
        }
        for (exchange, symbol) in &replay_sources {
            if exchange != "binance_futures" { continue; }
            let base_symbol = symbol.split('@').next().unwrap_or(symbol);
            let sym_lower = if base_symbol.len() > 3 && base_symbol.to_uppercase().ends_with("USD") {
                let base = &base_symbol[..base_symbol.len() - 3];
                format!("{}-usd", base.to_lowercase())
            } else {
                base_symbol.to_lowercase()
            };
            let end = end_dt + chrono::TimeDelta::seconds(10);
            if let Ok(r) = MarketReplayer::new(&data_path, "binance_futures", &sym_lower, start_dt, end) {
                strat_replayers.push(r);
            }
        }

        let mut strat_peeked: Vec<Option<(u64, MarketEvent)>> = Vec::new();
        for r in &mut strat_replayers {
            strat_peeked.push(r.next_event().ok().flatten());
        }

        // ── Hist bars (binance) — verbatim from v1 ──
        let needs_hist_bars = self.config.strategies.iter().any(|s| s.enabled && self.registry.capabilities(&s.name).needs_hist_bars);
        let mut bar_events: Vec<(u64, MarketEvent)> = Vec::new();
        if needs_hist_bars {
            let hist_bar_interval: String = self.config.strategies.iter()
                .find(|s| s.enabled && self.registry.capabilities(&s.name).needs_hist_bars)
                .and_then(|s| s.params.get("hist_bar_interval"))
                .and_then(|v| v.as_str())
                .unwrap_or("1m")
                .to_string();
            let hist_lookback_ns = 30u64 * 24 * 3_600_000_000_000;
            let hist_start_ns = start_ns.saturating_sub(hist_lookback_ns);
            for (exchange, symbol) in &replay_sources {
                if exchange != "binance" { continue; }
                let req = crate::types::HistDataRequest {
                    exchange: crate::types::Exchange::Binance,
                    symbol: symbol.clone(),
                    interval: hist_bar_interval.clone(),
                    start_date_ns: hist_start_ns,
                    end_date_ns: end_ns,
                };
                match crate::recorder::load_hist_bars(&data_path, &req) {
                    Ok(bars) => {
                        for bar in bars {
                            let ts = bar.close_time_ns;
                            bar_events.push((ts, MarketEvent::Bar(bar)));
                        }
                    }
                    Err(e) => {
                        error!("[Replayer v2] CRITICAL: load_hist_bars failed for {}/{} {}: {}",
                            exchange, symbol, hist_bar_interval, e);
                        std::process::exit(2);
                    }
                }
            }
            bar_events.sort_by_key(|e| e.0);
        }
        let mut bar_cursor: usize = 0;

        // ── Synthetic RTT-probe wiring — mirrors v1 (engine.rs ~2797): while
        // the strat gate is in Probe mode it sets `bt_probe_enable`; we feed one
        // place-RTT sample (from the v2 latency sampler) every
        // `bt_probe_interval_ns` of sim clock so the gate recovers Probe→Trade
        // and the strategy quotes. ──
        let (bt_probe_tx, bt_probe_rx) = crossbeam_channel::unbounded::<f64>();
        let bt_probe_enable = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let bt_probe_interval_ns: u64 = {
            let secs = self.config.strategies.iter()
                .find(|s| self.registry.capabilities(&s.name).needs_rtt_probe)
                .and_then(|s| s.params.get("adaptive_params_v2_probe_interval_secs").or_else(|| s.params.get("rtt_gate_probe_interval_secs")))
                .and_then(|v| v.as_float().or_else(|| v.as_integer().map(|i| i as f64)))
                .unwrap_or(2.0)
                .max(0.1);
            (secs * 1e9) as u64
        };
        let mut last_bt_probe_emit_sim_ns: u64 = 0;
        let bt_probe_active_token: crate::exchange::polymarket::rtt_probe::ActiveTokenHandle =
            std::sync::Arc::new(std::sync::Mutex::new(None));
        let bt_probe_map: HashMap<
            String,
            (
                crossbeam_channel::Receiver<f64>,
                std::sync::Arc<std::sync::atomic::AtomicBool>,
                crate::exchange::polymarket::rtt_probe::ActiveTokenHandle,
            ),
        > = self.config.strategies.iter()
            .find(|s| self.registry.capabilities(&s.name).needs_rtt_probe && s.enabled && !s.instance_id.is_empty())
            .map(|s| {
                let mut m = HashMap::new();
                m.insert(s.instance_id.clone(), (bt_probe_rx, bt_probe_enable.clone(), bt_probe_active_token));
                m
            })
            .unwrap_or_else(HashMap::new);

        let mut strategies = self.build_strategies(bt_probe_map, HashMap::new(), &HashMap::new());
        let hist_data_dir = PathBuf::from(&data_dir);
        for s in &mut strategies { s.on_init(); }

        // Split bar_events: pre-backtest bars → on_hist_bar, in-range → loop.
        if !bar_events.is_empty() {
            let hist_bars: Vec<_> = bar_events.iter().filter(|(ts, _)| *ts < start_ns).collect();
            if !hist_bars.is_empty() {
                for (_, event) in &hist_bars {
                    if let MarketEvent::Bar(b) = event {
                        for s in &mut strategies { s.on_hist_bar(b); }
                    }
                }
                // Fresh-bars gate (when on) reads completeness from each
                // strategy's own fit-window-capped resample cache, so BT (a
                // 30-day prefetch) matches live (fit-window load) — out-of-
                // window holes auto-drop and don't false-pause.
                for s in &mut strategies { s.on_hist_data_loaded(start_ns); }
            }
            bar_events.retain(|(ts, _)| *ts >= start_ns);
        }

        // ── Prediction warm-up — verbatim from v1 ──
        {
            let (warmup_sources, warmup_hours) = self.prediction_warmup_sources();
            if !warmup_sources.is_empty() && warmup_hours > 0.0 {
                let hour_ns: u64 = 3600 * 1_000_000_000;
                let warmup_end_ns = (start_ns / hour_ns) * hour_ns;
                let warmup_end_dt = chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(warmup_end_ns as i64);
                let warmup_start_dt = warmup_end_dt - chrono::TimeDelta::seconds((warmup_hours * 3600.0) as i64);
                for s in &mut strategies { s.on_prediction_warmup_start(); }
                for (exchange, symbol) in &warmup_sources {
                    match crate::recorder::MarketReplayer::new(&data_path, exchange, symbol, warmup_start_dt, warmup_end_dt) {
                        Ok(mut replayer) => {
                            while let Ok(Some((_ts, event))) = replayer.next_event() {
                                for strategy in &mut strategies {
                                    match &event {
                                        MarketEvent::OrderBook(ob) => strategy.on_orderbook(ob),
                                        MarketEvent::Trade(t) => strategy.on_trade_tick(t),
                                        _ => {}
                                    }
                                }
                            }
                        }
                        Err(e) => warn!("[Backtest v2] Warm-up: no data for {}/{}: {}", exchange, symbol, e),
                    }
                }
                for s in &mut strategies { s.on_prediction_warmup_end(start_ns); }
            }
        }

        // ── Dedicated chronological apv2 warm-up ──
        // The prediction warm-up above is per-exchange-sequential AND only
        // `prediction_training_period_hours` (≈1 day) long, so apv2 is gated
        // off there. Fill the v2 z-baseline here instead, over
        // `apv2_warmup_days` in TRUE wall-clock (merged k-way) order — exactly
        // what apv2 would see from an early-started replay. Spot sources only;
        // feeds apv2 exclusively (no predictor/index/vol/inventory effects).
        // `apv2_warmup_days = 0` (default) ⇒ skipped ⇒ byte-identical.
        {
            let aw_days = self.apv2_warmup_days();
            let (spot_sources, _) = self.prediction_warmup_sources();
            if aw_days > 0.0 && !spot_sources.is_empty() {
                let aw_end_dt = chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(start_ns as i64);
                let aw_start_dt = aw_end_dt - chrono::TimeDelta::seconds((aw_days * 86400.0) as i64);
                info!("[Backtest v2] apv2 warm-up: {:.1}d chronological spot replay [{} → {}]",
                    aw_days, aw_start_dt.format("%Y-%m-%d %H:%M"), aw_end_dt.format("%Y-%m-%d %H:%M"));
                let mut replayers: Vec<crate::recorder::MarketReplayer> = Vec::new();
                for (exchange, symbol) in &spot_sources {
                    match crate::recorder::MarketReplayer::new(&data_path, exchange, symbol, aw_start_dt, aw_end_dt) {
                        Ok(r) => replayers.push(r),
                        Err(e) => warn!("[Backtest v2] apv2 warm-up: no data for {}/{}: {}", exchange, symbol, e),
                    }
                }
                // One buffered event per replayer; repeatedly emit the global
                // minimum-timestamp event (merge by local_ts, same key the
                // main replay uses) so apv2's wall-clock buckets see venues
                // interleaved chronologically.
                let mut peeked: Vec<Option<(u64, MarketEvent)>> =
                    replayers.iter_mut().map(|r| r.next_event().ok().flatten()).collect();
                let mut fed: u64 = 0;
                loop {
                    let best = peeked.iter().enumerate()
                        .filter_map(|(i, p)| p.as_ref().map(|(ts, _)| (i, *ts)))
                        .min_by_key(|&(_, ts)| ts);
                    let Some((idx, _)) = best else { break; };
                    let (_, event) = peeked[idx].take().unwrap();
                    match &event {
                        MarketEvent::OrderBook(ob) => { for s in &mut strategies { s.on_apv2_warmup_orderbook(ob); } }
                        MarketEvent::Trade(t) => { for s in &mut strategies { s.on_apv2_warmup_trade(t); } }
                        _ => {}
                    }
                    fed += 1;
                    peeked[idx] = replayers[idx].next_event().ok().flatten();
                }
                info!("[Backtest v2] apv2 warm-up complete: {} spot events fed", fed);
            }
        }

        let mut last_quote_ns: Vec<u64> = vec![0; strategies.len()];

        // Per-instance USDC + per-event split shares (mirrors v1's sim wallet
        // seeding). split_amount_usdc → shares of each token credited at event.
        let mut sim_wallet_usdc_by_iid: HashMap<String, f64> = HashMap::new();
        let mut sim_split_by_iid: HashMap<String, f64> = HashMap::new();
        for s in &self.config.strategies {
            if !s.enabled || !self.registry.capabilities(&s.name).needs_sim_wallet || s.instance_id.is_empty() { continue; }
            let bal = s.params.get("init_balance")
                .and_then(|v| v.as_float().or_else(|| v.as_integer().map(|i| i as f64)))
                .unwrap_or(0.0);
            sim_wallet_usdc_by_iid.insert(s.instance_id.clone(), bal);
            // Split seed amount. Preferred key `split_hands` is denominated in
            // hands (× base_qty → USDC); legacy raw `split_amount_usdc` is kept
            // as a fallback for unmigrated configs. Must match the same formula
            // used at the live/maintenance read site (search `split_hands`).
            let pf = |key: &str, default: f64| -> f64 {
                s.params.get(key)
                    .and_then(|v| v.as_float().or_else(|| v.as_integer().map(|i| i as f64)))
                    .unwrap_or(default)
            };
            let split_hands = pf("split_hands", 0.0);
            let split = if split_hands > 0.0 {
                split_hands * pf("base_qty", 10.0)
            } else {
                pf("split_amount_usdc", 0.0)
            };
            sim_split_by_iid.insert(s.instance_id.clone(), split);
        }

        // ── RTT source selection ──
        // `sim_latency_calibrate_from` resolves to EITHER:
        //   * a **directory** → record-replay: draw per-request RTT from the
        //     `latency_record` CSVs in it (LatencyProfile::RecordReplay), or
        //   * one/more **.log files** → analytic calibration (empirical CDF +
        //     AR(1), parsed below), the legacy path.
        // Detection is `is_dir()` on the (single) trimmed path; the comma-list
        // form is only meaningful for the log path, so a directory is taken
        // verbatim. Empty = static knobs (unchanged).
        let calib_from = bt.sim_latency_calibrate_from.trim();
        let is_record_dir = !calib_from.is_empty() && std::path::Path::new(calib_from).is_dir();

        // ── RTT calibration: honor sim_latency_calibrate_from (parse live
        // log[s] for empirical place/cancel anchors + per-UTC-hour buckets +
        // ρ); else static knobs. The full `CalibratedParams` is retained
        // (`calibrated`) so the per-hour `HourlyEmpirical` profile can be
        // built below from `place_hourly` / `cancel_hourly`. ──
        const V2_CLIENT_TIMEOUT_DEFAULT_MS: u64 = 500;
        let dflt_lat = (bt.sim_latency_p50_ms as f64, bt.sim_latency_p95_ms as f64, bt.sim_latency_p99_ms as f64);
        // Calibrate only for the log/archive source (the directory source
        // carries its own per-sample RTT and skips the analytic fit).
        let calibrated: Option<crate::exchange::sim::latency::CalibratedParams> =
            if calib_from.is_empty() || is_record_dir {
                None
            } else {
                let paths: Vec<String> = bt.sim_latency_calibrate_from
                    .split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
                match crate::exchange::sim::latency::calibrate_from_logs(&paths) {
                    Ok(cal) => Some(cal),
                    Err(e) => {
                        warn!("[Backtest v2] RTT calibration failed ({}): using static knobs", e);
                        None
                    }
                }
            };
        let (place_p, cancel_p, lat_rho, lat_cross, client_timeout_ms) = match &calibrated {
            Some(cal) => {
                // Mirror v1: the log-inferred client timeout takes effect only
                // when the TOML knob is still at the 500 ms default (overrides win).
                let ct = if bt.sim_client_timeout_ms == V2_CLIENT_TIMEOUT_DEFAULT_MS
                    && cal.inferred_client_timeout_ms > 0.0
                {
                    cal.inferred_client_timeout_ms.round() as u64
                } else {
                    bt.sim_client_timeout_ms
                };
                info!(
                    "[Backtest v2] RTT calibrated: place p50/p95/p99={:.0}/{:.0}/{:.0}ms  cancel={:.0}/{:.0}/{:.0}ms  ρ_place={:?}  client_timeout={}ms",
                    cal.place.p50_ms, cal.place.p95_ms, cal.place.p99_ms,
                    cal.cancel.p50_ms, cal.cancel.p95_ms, cal.cancel.p99_ms, cal.place.rho_lag1, ct,
                );
                (
                    (cal.place.p50_ms, cal.place.p95_ms, cal.place.p99_ms),
                    (cal.cancel.p50_ms, cal.cancel.p95_ms, cal.cancel.p99_ms),
                    cal.place.rho_lag1.unwrap_or(bt.sim_latency_correlation),
                    cal.cross_corr_log_p99.unwrap_or(bt.sim_latency_cross_correlation),
                    ct,
                )
            }
            None => (dflt_lat, dflt_lat, bt.sim_latency_correlation, bt.sim_latency_cross_correlation, bt.sim_client_timeout_ms),
        };
        let ahead_frac = if bt.sim_v2_ahead_frac >= 0.0 { Some(bt.sim_v2_ahead_frac) } else { None };

        // ── Record-replay profiles (sim_latency_calibrate_from = directory) ──
        // Load the latency-record CSVs into per-side RecordReplay profiles that
        // replay recorded place/cancel RTT by wall-clock / time-of-day (Tier
        // 1/2/3 + the `rtt_sim_fallback` date-aware fallback). On any failure
        // (load error, empty side) we fall back to the analytic empirical-CDF
        // anchors so the run still proceeds.
        //
        // NOTE: this is ONLY for the directory source. A log / parquet-archive
        // `sim_latency_calibrate_from` keeps the analytic empirical-CDF model
        // (`calibrate_from_logs` above) — per-request Submit↔ack samples are too
        // sparse once sliced by day × time-of-day to give stable bucket
        // quantiles (esp. the tail), so the pooled CDF + GPD-tail extrapolation
        // is the more reliable RTT model there.
        use crate::exchange::sim::latency_record_replay as rrl;
        let (place_profile, cancel_profile): (
            Option<crate::exchange::sim::latency::LatencyProfile>,
            Option<crate::exchange::sim::latency::LatencyProfile>,
        ) = if is_record_dir {
            let dir = std::path::Path::new(calib_from);
            let bucket_secs = bt.sim_latency_record_tod_bucket_secs.clamp(1, 86_400) as u32;
            let params = rrl::RecordReplayParams {
                abs_tol_ms: bt.sim_latency_record_abs_tol_ms,
                tod_tol_secs: bt.sim_latency_record_tod_tol_secs.min(u32::MAX as u64) as u32,
                fallback: rrl::RecordReplayFallback::from_str(&bt.rtt_sim_fallback),
            };
            match rrl::RecordReplayData::load_dir(dir, bucket_secs) {
                Ok(data) if data.place.n() > 0 && data.cancel.n() > 0 => {
                    info!(
                        "[Backtest v2] RTT record-replay from {}: {} csv file(s), place n={} (epoch_ms [{}..{}]), cancel n={}; params abs_tol={}ms tod_tol={}s tod_bucket={}s fallback={}, ρ={:.3} ρ_cross={:.3}",
                        dir.display(), data.n_files, data.place.n(),
                        data.place.min_epoch_ms(), data.place.max_epoch_ms(), data.cancel.n(),
                        params.abs_tol_ms, params.tod_tol_secs, bucket_secs, params.fallback.as_str(), lat_rho, lat_cross,
                    );
                    use crate::exchange::sim::latency::LatencyProfile::RecordReplay;
                    (
                        Some(RecordReplay { records: data.place.clone(),  rho: lat_rho, params }),
                        Some(RecordReplay { records: data.cancel.clone(), rho: lat_rho, params }),
                    )
                }
                Ok(data) => {
                    warn!("[Backtest v2] record-replay dir {} has an empty side (place {}, cancel {}) → static knobs",
                        dir.display(), data.place.n(), data.cancel.n());
                    (None, None)
                }
                Err(e) => {
                    warn!("[Backtest v2] record-replay load {} failed ({}) → static knobs", dir.display(), e);
                    (None, None)
                }
            }
        } else if let Some(cal) = calibrated.as_ref() {
            // Log/archive source: per-UTC-hour `HourlyEmpirical` profile so the
            // empirical-CDF model gets intra-day-session awareness (RTT regime
            // varies by hour-of-day). Built per side when ≥ HOURLY_MIN_HOURS
            // buckets are populated (each bucket already requires
            // HOURLY_MIN_SAMPLES samples); sparse sides stay `None` → the
            // simulator falls back to the pooled empirical CDF from the scalar
            // anchors. Per-event override (sim_rtt_mode="exact") still applies
            // on top — it takes priority over the hourly base in the sampler.
            use crate::exchange::sim::latency::{EmpiricalAnchors, HOURLY_MIN_HOURS, LatencyProfile, SidedParams};
            let to_anchors = |s: &SidedParams| EmpiricalAnchors {
                p50_ms: s.p50_ms,
                p85_ms_override: s.p85_ms,
                p95_ms: s.p95_ms,
                p99_ms: s.p99_ms,
                p999_ms_override: s.p999_ms_override,
                gpd_tail: s.gpd_tail,
            };
            let build_hourly = |hourly: &[Option<SidedParams>; 24], pooled: &SidedParams, side: &str|
              -> Option<LatencyProfile> {
                let n_pop = hourly.iter().filter(|h| h.is_some()).count();
                if n_pop < HOURLY_MIN_HOURS {
                    info!("[Backtest v2] {} hourly: only {} populated UTC-hour bucket(s) (<{}) → pooled empirical CDF",
                        side, n_pop, HOURLY_MIN_HOURS);
                    return None;
                }
                let anchors: [Option<EmpiricalAnchors>; 24] =
                    std::array::from_fn(|h| hourly[h].as_ref().map(to_anchors));
                info!("[Backtest v2] {} hourly-empirical RTT: {} populated UTC-hour buckets (ρ={:.3})",
                    side, n_pop, lat_rho);
                Some(LatencyProfile::HourlyEmpirical {
                    hourly: Box::new(anchors),
                    fallback: to_anchors(pooled),
                    rho: lat_rho,
                })
            };
            (
                build_hourly(&cal.place_hourly, &cal.place, "place"),
                build_hourly(&cal.cancel_hourly, &cal.cancel, "cancel"),
            )
        } else {
            (None, None)
        };

        // ── Per-event RTT table (sim_rtt_mode="exact"): per-event live RTT
        // shape (+ intra-event early/late segments) + prev_event p60. None →
        // pooled CDF (predict mode). Used by the sim lane (latency anchors) and
        // the strat lane (gate prev_p override). ──
        let per_event_rtt_table: Option<std::collections::HashMap<
            u64, crate::exchange::sim::per_event_rtt::EventRttOverride>> =
            if crate::exchange::sim::SimRttMode::from_str(&bt.sim_rtt_mode)
                == crate::exchange::sim::SimRttMode::Exact
                && !calib_from.is_empty()
                && !is_record_dir
            {
                let paths: Vec<String> = bt.sim_latency_calibrate_from
                    .split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
                match crate::exchange::sim::per_event_rtt::extract_per_event_rtt(&paths) {
                    Ok(t) if !t.is_empty() => {
                        let n_place = t.values().filter(|e| e.place_p50_ms.is_some()).count();
                        let n_seg = t.values().filter(|e| e.has_segmented_place()).count();
                        info!("[Backtest v2] per-event RTT (sim_rtt_mode=exact): {} events, {} with place quantiles, {} segmented",
                            t.len(), n_place, n_seg);
                        Some(t)
                    }
                    Ok(_) => { warn!("[Backtest v2] per-event RTT table empty → pooled CDF"); None }
                    Err(e) => { warn!("[Backtest v2] per-event RTT extract failed ({}) → pooled CDF", e); None }
                }
            } else {
                None
            };

        // ── Taker matching-overhead: auto-calibrate from the log when the
        // knobs are at their measured defaults (explicit overrides win). ──
        let (tovh_p50, tovh_p95, tovh_p99) = {
            let at_default = (bt.sim_v2_taker_overhead_p50_ms - 267.0).abs() < 1e-6
                && (bt.sim_v2_taker_overhead_p95_ms - 910.0).abs() < 1e-6
                && (bt.sim_v2_taker_overhead_p99_ms - 1612.0).abs() < 1e-6;
            let cfg = (bt.sim_v2_taker_overhead_p50_ms, bt.sim_v2_taker_overhead_p95_ms, bt.sim_v2_taker_overhead_p99_ms);
            if at_default && !calib_from.is_empty() && !is_record_dir {
                let paths: Vec<String> = bt.sim_latency_calibrate_from
                    .split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
                match crate::exchange::sim::per_event_rtt::extract_taker_overhead(&paths) {
                    Ok(Some((a, b, c))) => {
                        info!("[Backtest v2] taker overhead auto-calibrated from log: p50/p95/p99={:.0}/{:.0}/{:.0} ms", a, b, c);
                        (a, b, c)
                    }
                    Ok(None) => { warn!("[Backtest v2] taker overhead: <30 paired samples → config defaults"); cfg }
                    Err(e) => { warn!("[Backtest v2] taker overhead extract failed ({}) → config defaults", e); cfg }
                }
            } else {
                cfg
            }
        };

        // ── Build the v2 Simulator (owns server-axis feed + DES + RTT) ──
        let mut sim = Simulator::new(SimV2Config {
            data_dir: data_dir.clone(),
            start: start_dt,
            end: end_dt,
            sources: replay_sources.clone(),
            place_p50_ms: place_p.0,
            place_p95_ms: place_p.1,
            place_p99_ms: place_p.2,
            cancel_p50_ms: cancel_p.0,
            cancel_p95_ms: cancel_p.1,
            cancel_p99_ms: cancel_p.2,
            rho: lat_rho,
            rho_cross: lat_cross,
            seed: bt.sim_latency_seed,
            client_timeout_ns: client_timeout_ms.saturating_mul(1_000_000),
            wallet_usdc_by_iid: sim_wallet_usdc_by_iid,
            split_by_iid: sim_split_by_iid,
            ahead_frac,
            adverse_sel_rate: bt.sim_v2_adverse_sel_rate,
            adverse_scale_ticks: bt.sim_v2_adverse_scale_ticks,
            book_through_rate: bt.sim_v2_book_through_rate,
            fill_markout_vn: bt.sim_v2_fill_markout_vn,
            fill_markout_horizon_ns: bt.sim_v2_fill_markout_horizon_ms.saturating_mul(1_000_000),
            fill_push_mult: bt.sim_v2_fill_push_mult,
            matched_cant_cancel_window_ns: bt.sim_matched_cant_cancel_window_ms.saturating_mul(1_000_000),
            per_event_rtt: per_event_rtt_table.clone(),
            taker_overhead_p50_ms: tovh_p50,
            taker_overhead_p95_ms: tovh_p95,
            taker_overhead_p99_ms: tovh_p99,
            maker_race_rate: bt.sim_v2_maker_race_rate,
            taker_race_rate: bt.sim_v2_taker_race_rate,
            maker_race_horizon_ns: bt.sim_v2_maker_race_horizon_ms.saturating_mul(1_000_000),
            taker_race_horizon_ns: bt.sim_v2_taker_race_horizon_ms.saturating_mul(1_000_000),
            fold_outcomes: bt.sim_v2_fold_outcomes,
            taker_comp_rate: bt.sim_v2_taker_comp_rate,
            taker_comp_window_ns: bt.sim_v2_taker_comp_window_ms.saturating_mul(1_000_000),
            deep_queue_decay: bt.sim_v2_deep_queue_decay,
            // Mirror the polymarket exchange's batch flag so the sim splits
            // reprice cancels onto the cancel RTT when batching is off (the
            // live config sets use_batch_orders=false).
            use_batch_orders: self.config.exchanges.iter()
                .find(|e| e.name == "polymarket")
                .map(|e| e.use_batch_orders)
                .unwrap_or(true),
            // Record-replay place/cancel profiles (Some only when
            // sim_latency_calibrate_from is a directory); None → scalar CDF.
            place_profile,
            cancel_profile,
        })?;

        info!("[Backtest v2] {} strat replayers, {} bar events", strat_replayers.len(), bar_events.len());

        // ── k-way merge: strat lane (local_ts) + bars + sim (wall clock) ──
        #[derive(Eq, PartialEq)]
        struct HeapEntry { ts: u64, idx: usize }
        impl Ord for HeapEntry {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering { other.ts.cmp(&self.ts) }
        }
        impl PartialOrd for HeapEntry {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
        }
        let mut strat_heap: BinaryHeap<HeapEntry> = BinaryHeap::new();
        for (i, p) in strat_peeked.iter().enumerate() {
            if let Some((ts, _)) = p { strat_heap.push(HeapEntry { ts: *ts, idx: i }); }
        }

        let mut strat_clock_ns: u64 = 0;
        let mut sim_clock_ns: u64 = 0;

        loop {
            let strat_ts = strat_heap.peek().map(|e| e.ts).unwrap_or(u64::MAX);
            let bar_ts = bar_events.get(bar_cursor).map(|(ts, _)| *ts).unwrap_or(u64::MAX);
            let strat_min = strat_ts.min(bar_ts);
            let sim_ts = sim.peek_when().unwrap_or(u64::MAX);
            let min_ts = strat_min.min(sim_ts);
            if min_ts == u64::MAX { break; }

            if min_ts == sim_ts {
                // Server market event OR my-order lifecycle event (unified wall
                // clock). Acks/fills due now come back as `updates`.
                sim_clock_ns = sim_ts;
                set_sim_clock(sim_clock_ns);
                let updates = sim.step();
                for update in updates {
                    strat_clock_ns = strat_clock_ns.max(update.timestamp_ns);
                    set_sim_clock(update.timestamp_ns);
                    for strategy in strategies.iter_mut() {
                        for sig in strategy.on_order_update(&update) {
                            sim.submit(&sig, update.timestamp_ns);
                        }
                    }
                }
            } else {
                // Strategy market event (by local_timestamp) — replayer or bars.
                let (ts, event) = if min_ts == bar_ts && bar_cursor < bar_events.len() {
                    let pair = bar_events[bar_cursor].clone();
                    bar_cursor += 1;
                    pair
                } else {
                    let entry = strat_heap.pop().unwrap();
                    let best_idx = entry.idx;
                    let pair = strat_peeked[best_idx].take().unwrap();
                    strat_peeked[best_idx] = strat_replayers[best_idx].next_event().ok().flatten();
                    if let Some((ts, _)) = &strat_peeked[best_idx] {
                        strat_heap.push(HeapEntry { ts: *ts, idx: best_idx });
                    }
                    pair
                };
                strat_clock_ns = ts;
                set_sim_clock(strat_clock_ns);

                for (i, strategy) in strategies.iter_mut().enumerate() {
                    let signals = match &event {
                        MarketEvent::OrderBook(ob) => { strategy.on_orderbook(ob); Vec::new() }
                        MarketEvent::Trade(t) => { strategy.on_trade_tick(t); Vec::new() }
                        // Quote / SpotPrice update internal state only — the
                        // quote cadence is driven exclusively by OrderBook
                        // events (see the OrderBook trigger block below).
                        MarketEvent::Quote(q) => { strategy.on_quote_tick(q); Vec::new() }
                        MarketEvent::Bar(b) => { strategy.on_bar(b); Vec::new() }
                        MarketEvent::SpotPrice(sp) => { strategy.on_spot_price(sp); Vec::new() }
                        MarketEvent::Instrument(inst) => {
                            // Hist gap-fill BEFORE on_instrument (matches v1).
                            let hist_reqs = strategy.load_hist_data(ts);
                            for req in &hist_reqs {
                                match crate::recorder::load_hist_bars(&hist_data_dir, req) {
                                    Ok(bars) => { for bar in &bars { strategy.on_hist_bar(bar); } }
                                    Err(e) => warn!("[Strategy v2] Failed to load hist bars: {}", e),
                                }
                            }
                            if !hist_reqs.is_empty() { strategy.on_hist_data_loaded(ts); }
                            // Per-event prev_p RTT-gate override (sim_rtt_mode=exact):
                            // forward live's prev_event place-p60 (× overhead factor)
                            // so the gate's N matches live at this event start.
                            if let (Some(ref table), crate::types::instrument::Instrument::BinaryOption(bo)) =
                                (per_event_rtt_table.as_ref(), inst)
                            {
                                let factor = bt.sim_per_event_rtt_overhead_factor.max(0.0);
                                let override_ms = parse_event_start_ts_secs(&bo.event_start_time)
                                    .and_then(|s| table.get(&s))
                                    .and_then(|e| e.prev_event_p_ms)
                                    .map(|v| (v as f64) * factor);
                                strategy.set_per_event_prev_p_override(override_ms);
                            }
                            strategy.on_instrument(inst);
                            Vec::new()
                        }
                        MarketEvent::TickSizeChange(tsc) => { strategy.on_tick_size_change(tsc) }
                        MarketEvent::Connected { exchange } => { strategy.on_connected(*exchange); Vec::new() }
                        MarketEvent::Disconnected { exchange, reason } => { strategy.on_disconnected(*exchange, reason); Vec::new() }
                        _ => Vec::new(),
                    };
                    // OrderBook events are the sole driver of the quote
                    // cadence. Optionally restricted to Binance OBs, and
                    // with a fractional early-trigger tolerance to absorb
                    // local-timestamp jitter on the OB feed.
                    if let MarketEvent::OrderBook(ob) = &event {
                        let venue_ok = !strategy.quote_trigger_binance_ob_only()
                            || ob.exchange == Exchange::Binance;
                        let interval = strategy.quote_interval_ms();
                        // Tick-by-tick fires a quote on every OB, EXCEPT when
                        // the backpressure detector flags congestion (rolling
                        // P(RTT>T) over threshold, decided per-event) — then
                        // fall back to the quote_interval (×N) throttle.
                        let tbt = strategy.quote_tick_by_tick()
                            && !strategy.cadence_rtt_throttle();
                        if venue_ok && (tbt || interval > 0) {
                            let fire = if tbt {
                                true
                            } else {
                                let frac = strategy.quote_interval_tolerance_frac().clamp(0.0, 1.0);
                                let threshold_ns =
                                    ((interval as f64) * 1_000_000.0 * (1.0 - frac)) as u64;
                                ts - last_quote_ns[i] >= threshold_ns
                            };
                            if fire {
                                last_quote_ns[i] = ts;
                                for sig in strategy.on_quote(ts) {
                                    sim.submit(&sig, strat_clock_ns);
                                }
                            }
                        }
                    }
                    for sig in signals {
                        sim.submit(&sig, strat_clock_ns);
                    }
                }
            }

            // Synthetic RTT-probe emit (gate recovery), mirrors v1.
            let now_for_probe = sim_clock_ns.max(strat_clock_ns);
            if bt_probe_enable.load(std::sync::atomic::Ordering::Relaxed)
                && now_for_probe >= last_bt_probe_emit_sim_ns.saturating_add(bt_probe_interval_ns)
            {
                let rtt_ms = sim.sample_probe_rtt_ms(now_for_probe);
                let _ = bt_probe_tx.send(rtt_ms);
                last_bt_probe_emit_sim_ns = now_for_probe;
            }
        }

        for s in &mut strategies {
            s.on_exit();
            s.on_shutdown();
        }

        let (anchored, fallback) = sim.trade_anchor_stats();
        info!("  Sim v2:   trade-ts reconstruction: {} anchored, {} fallback (no prior book)", anchored, fallback);
        let (taker_fills, maker_fills, rejects) = sim.core_stats();
        info!("  Sim v2:   taker_fills={}  maker_fills={}  rejects={}", taker_fills, maker_fills, rejects);
        let (rj_tb, rj_ts, rj_rb, rj_rs, rj_rs_short) = sim.reject_breakdown();
        info!("  Sim v2:   reject reasons: taker_buy={} taker_sell={} rest_buy={} rest_sell={} (rest_sell short Σ={:.0} shares, mean={:.1})",
            rj_tb, rj_ts, rj_rb, rj_rs, rj_rs_short,
            if rj_rs > 0 { rj_rs_short / rj_rs as f64 } else { 0.0 });
        for s in &self.config.strategies {
            if s.enabled && self.registry.capabilities(&s.name).needs_sim_wallet && !s.instance_id.is_empty() {
                if let Some(bal) = sim.wallet_usdc(&s.instance_id) {
                    let seed = s.params.get("init_balance")
                        .and_then(|v| v.as_float().or_else(|| v.as_integer().map(|i| i as f64)))
                        .unwrap_or(0.0);
                    info!("  Sim v2:   gating-wallet USDC [{}]: final={:.2} (seeded={:.2}, net={:.2}) — settlement-aware (split cost debited at seed, payouts credited at retire); net ≈ in-flight seed float, NOT a bleed",
                        s.instance_id, bal, seed, bal - seed);
                }
            }
        }
        let (timeouts, matched_cant_cancel) = sim.timeout_stats();
        info!("  Sim v2:   timeouts={}  matched_cant_cancel={}", timeouts, matched_cant_cancel);
        let (po_rejects, po_seen) = sim.post_only_stats();
        info!("  Sim v2:   post_only_rejects={}/{} ({:.2}% cross at reach)", po_rejects, po_seen,
            if po_seen > 0 { 100.0 * po_rejects as f64 / po_seen as f64 } else { 0.0 });
        let (mean_age_ms, over1s, mean_life_ms) = sim.fill_timing_stats();
        info!("  Sim v2:   maker fill-age mean={:.0}ms  >1s={:.1}%  | removed-order lifetime mean={:.0}ms", mean_age_ms, 100.0 * over1s, mean_life_ms);
        let (race_infl, race_plc, race_ratio, taker_capped, taker_capped_zero) = sim.race_stats();
        if race_plc > 0 {
            info!("  Sim v2:   maker race inflated {}/{} ({:.1}%) placements, mean q_ahead×{:.2}",
                race_infl, race_plc, 100.0 * race_infl as f64 / race_plc as f64, race_ratio);
        }
        // Report taker-race caps independently of maker-race (the two are
        // separate knobs; gating this on maker placements hid taker-race effect).
        if taker_capped > 0 {
            let zero_pct = 100.0 * taker_capped_zero as f64 / taker_capped as f64;
            info!("  Sim v2:   taker race capped {} fills ({} to ~0 = full miss, {:.1}%)",
                taker_capped, taker_capped_zero, zero_pct);
        }
        let adv_adv = sim.adverse_advanced();
        if adv_adv > 0 {
            info!("  Sim v2:   adverse-sel tilt advanced queue on {} resyncs (cancel-attribution ahead_frac→1 on adverse mid moves)", adv_adv);
        }
        let bt_fills = sim.book_through_fills();
        if bt_fills > 0 {
            info!("  Sim v2:   book-through adverse fills: {} (resting orders the contra swept through → picked off)", bt_fills);
        }
        let hc = sim.fill_haircuts();
        if hc > 0 {
            info!("  Sim v2:   forward-markout haircuts: {} favorable maker fills downweighted (markout → live −0.75c)", hc);
        }
        let (mq, tv) = sim.depth_distributions();
        info!("  Sim v2:   maker q_init (shares ahead at placement) n={:.0} mean={:.1} | p10={:.0} p25={:.0} p50={:.0} p75={:.0} p90={:.0} p99={:.0} | zero-queue={:.1}%",
            mq[0], mq[1], mq[2], mq[3], mq[4], mq[5], mq[6], mq[7], 100.0 * mq[8]);
        info!("  Sim v2:   taker avail-vol (fillable within limit at match) n={:.0} mean={:.1} | p10={:.0} p25={:.0} p50={:.0} p75={:.0} p90={:.0} p99={:.0} | zero={:.1}%",
            tv[0], tv[1], tv[2], tv[3], tv[4], tv[5], tv[6], tv[7], 100.0 * tv[8]);
        let pb = sim.placement_buckets();
        let ptot: u64 = pb.iter().map(|b| b[0]).sum::<u64>().max(1);
        let pnames = ["improve(inside/new-best)", "join(==best)", "behind(deeper)", "no-book-this-side"];
        info!("  Sim v2:   maker placement price-vs-BBO (why q_init=0):");
        for (b, nm) in pb.iter().zip(pnames) {
            let q0pct = if b[0] > 0 { 100.0 * b[1] as f64 / b[0] as f64 } else { 0.0 };
            info!("  Sim v2:     {:<26} {:>7} ({:>4.1}% of placements)  q_init=0 in {:>5.1}%",
                nm, b[0], 100.0 * b[0] as f64 / ptot as f64, q0pct);
        }
        let (q0_extra, q0_best) = sim.q0_fallback_split();
        info!("  Sim v2:     q_init=0 resolved by: extrapolation(beyond-window)={} | best-level rule(in-window gap)={}",
            q0_extra, q0_best);
        let (tcc, tccz, tc_mean) = sim.taker_comp_stats();
        if tcc > 0 {
            let zpct = 100.0 * tccz as f64 / tcc as f64;
            info!("  Sim v2:   taker trade-flow competition capped {} fills ({} to ~0 = full miss, {:.1}%) | mean competing vol={:.1}",
                tcc, tccz, zpct, tc_mean);
        }
        info!("══════════════════════════════════════");
        info!("  BACKTEST complete (sim_v2)");
        info!("══════════════════════════════════════");
        let _ = (start_ns, end_ns, sim_clock_ns, strat_clock_ns);
        Ok(())
    }

    // ── Strategy construction (registry-driven) ──
    // Build the per-instance runtime deps (RTT-probe channel, stale-threshold
    // handle, Polymarket SharedState) and dispatch to the `StrategyRegistry` so
    // the engine never names a concrete strategy type. `rtt_probe_install` is
    // keyed by `instance_id`; an empty map ⇒ no probes (paper / BT path). The
    // per-strategy construction logic lives in each strategy crate's
    // `StrategyFactory` (e.g. `polymaker::PolymakerFactory`).
    fn build_strategies(
        &self,
        rtt_probe_install: HashMap<
            String,
            (
                crossbeam_channel::Receiver<f64>,
                std::sync::Arc<std::sync::atomic::AtomicBool>,
                crate::exchange::polymarket::rtt_probe::ActiveTokenHandle,
            ),
        >,
        stale_threshold_handles: HashMap<String, Arc<std::sync::atomic::AtomicU64>>,
        poly_states: &HashMap<String, Arc<crate::exchange::polymarket::trade::SharedState>>,
    ) -> Vec<Box<dyn Strategy>> {
        let mut strategies: Vec<Box<dyn Strategy>> = Vec::new();
        let bt_start_ns = self.parse_backtest_start_ns();
        let rtt_probe_map_nonempty = !rtt_probe_install.is_empty();
        let stale_threshold_map_nonempty = !stale_threshold_handles.is_empty();
        for cfg in &self.config.strategies {
            if !cfg.enabled {
                continue;
            }
            let deps = StrategyBuildDeps {
                cfg,
                full: &self.config,
                bt_start_ns,
                strategy_index: strategies.len(),
                rtt_probe: rtt_probe_install.get(&cfg.instance_id).cloned(),
                rtt_probe_map_nonempty,
                stale_threshold: stale_threshold_handles.get(&cfg.instance_id).cloned(),
                stale_threshold_map_nonempty,
                poly_state: poly_states.get(&cfg.instance_id).cloned(),
            };
            if let Some(s) = self.registry.build(deps) {
                strategies.push(s);
            }
        }
        strategies
    }

    /// Paper execution thread: the sim_v2 matching core (`SimExchangeV2`) fed by
    /// live Polymarket data. Runs at wall-clock with a fixed one-way latency —
    /// the full DES `Simulator` (its replay `ServerFeed` + RTT distribution +
    /// race/markout lookahead) is backtest-only, so paper drives the core
    /// directly, exactly as the old v1 paper executor drove `SimExchange`.
    /// Lookahead-based knobs (race, forward-markout) are inert here (no future
    /// book live); the queue/taker/book-through/fold knobs are mirrored from the
    /// backtest config so paper fills track the calibrated backtest behaviour.
    fn spawn_paper_execution_thread(
        signal_rx: Receiver<Signal>,
        sim_feed_rx: Receiver<MarketEvent>,
        update_tx: Sender<OrderUpdate>,
        sim_latency_ms: u64,
        bt: crate::config::BacktestConfig,
    ) -> thread::JoinHandle<()> {
        thread::Builder::new()
            .name("paper-exec".into())
            .spawn(move || {
                crate::os_tune::pin_background("paper-exec");
                use crate::exchange::sim_v2::exchange::SimExchangeV2;

                // Paper has no real CLOB to time out against; use a nominal
                // client-timeout for the core's matched-can't-cancel window.
                let client_timeout_ns = if bt.sim_client_timeout_ms > 0 {
                    bt.sim_client_timeout_ms.saturating_mul(1_000_000)
                } else {
                    500_000_000
                };
                let mut sim = SimExchangeV2::new(
                    client_timeout_ns,
                    std::collections::HashMap::new(),
                    std::collections::HashMap::new(),
                );
                // Mirror the backtest model knobs that don't need future-book
                // lookahead. Race + forward-markout are omitted: they require
                // peeking the next snapshot, which a live feed can't provide.
                let ahead_frac = if bt.sim_v2_ahead_frac >= 0.0 { Some(bt.sim_v2_ahead_frac) } else { None };
                sim.configure(ahead_frac, bt.sim_matched_cant_cancel_window_ms.saturating_mul(1_000_000));
                sim.configure_adverse_sel(bt.sim_v2_adverse_sel_rate, bt.sim_v2_adverse_scale_ticks);
                sim.configure_book_through(bt.sim_v2_book_through_rate);
                sim.set_fold_outcomes(bt.sim_v2_fold_outcomes);
                sim.set_deep_queue_decay(bt.sim_v2_deep_queue_decay);
                sim.configure_taker_comp(bt.sim_v2_taker_comp_rate, bt.sim_v2_taker_comp_window_ms.saturating_mul(1_000_000));
                let latency = std::time::Duration::from_millis(sim_latency_ms);
                info!("[PaperExec] Started on sim_v2 core (latency={}ms)", sim_latency_ms);

                // Collect updates from sim, apply response latency, then send
                let send_updates = |updates: Vec<OrderUpdate>, tx: &Sender<OrderUpdate>, delay: std::time::Duration| {
                    if updates.is_empty() { return; }
                    if delay.as_millis() > 0 {
                        std::thread::sleep(delay);
                    }
                    for u in updates {
                        let _ = tx.send(u);
                    }
                };

                loop {
                    crossbeam_channel::select! {
                        recv(sim_feed_rx) -> msg => {
                            match msg {
                                Ok(MarketEvent::OrderBook(ref ob)) => {
                                    // v2 `on_orderbook` returns book-through
                                    // adverse fills directly (empty unless
                                    // sim_v2_book_through_rate > 0).
                                    let fills = sim.on_orderbook(ob);
                                    send_updates(fills, &update_tx, latency);
                                }
                                Ok(MarketEvent::Trade(ref t)) => {
                                    let fills = sim.on_trade_tick(t);
                                    send_updates(fills, &update_tx, latency);
                                }
                                Ok(MarketEvent::TickSizeChange(ref tsc)) => {
                                    sim.on_tick_size_change(tsc);
                                }
                                Ok(MarketEvent::Instrument(ref inst)) => {
                                    sim.on_instrument(inst);
                                }
                                Ok(MarketEvent::Exit) => {
                                    info!("[PaperExec] Exit signal received");
                                    break;
                                }
                                Err(_) => break,
                                _ => {}
                            }
                        }
                        recv(signal_rx) -> msg => {
                            // Simulate network latency: signal → exchange
                            if latency.as_millis() > 0 {
                                std::thread::sleep(latency);
                            }
                            let mut updates = Vec::new();
                            // Paper mode runs at wall-clock — pass `now_ns()`
                            // as the sim clock so cancel timestamps and the
                            // matched-cant-cancel age check live on real time.
                            let sim_now = crate::types::now_ns();
                            match msg {
                                Ok(Signal::NewOrder(ref order)) => {
                                    updates.push(sim.submit_order(order, sim_now));
                                }
                                Ok(Signal::CancelOrder { exchange, ref client_order_id, .. }) => {
                                    updates.push(sim.cancel_order(exchange, client_order_id, sim_now));
                                }
                                Ok(Signal::CancelAll { exchange, ref symbol, .. }) => {
                                    updates.extend(sim.cancel_all(exchange, symbol, sim_now));
                                }
                                Ok(Signal::BatchNewOrders { ref orders, .. }) => {
                                    for order in orders {
                                        updates.push(sim.submit_order(order, sim_now));
                                    }
                                }
                                Ok(Signal::BatchCancelOrders { exchange, ref client_order_ids, .. }) => {
                                    for id in client_order_ids {
                                        updates.push(sim.cancel_order(exchange, id, sim_now));
                                    }
                                }
                                Ok(Signal::BatchUpdateOrders { exchange, ref cancel_client_order_ids, ref place_orders, .. }) => {
                                    // Places before cancels — same rationale as the
                                    // BT main-loop branch: gives `submit_order` a
                                    // realistic view of resting orders for queue /
                                    // cascade-cancel / synthetic-balance-error paths.
                                    for order in place_orders {
                                        updates.push(sim.submit_order(order, sim_now));
                                    }
                                    for id in cancel_client_order_ids {
                                        updates.push(sim.cancel_order(exchange, id, sim_now));
                                    }
                                }
                                Ok(Signal::ReconcilePolymarket { .. }) => {
                                    // Paper/sim mode has no externally-observable
                                    // order state to reconcile against — the sim
                                    // delivers deterministic results synchronously.
                                }
                                Ok(Signal::PolymarketCancelAllOrders { ref reason, .. }) => {
                                    warn!("[PaperExec] PolymarketCancelAllOrders: reason={}", reason);
                                    updates.extend(sim.cancel_all(Exchange::Polymarket, "", sim_now));
                                }
                                Ok(Signal::Exit) => {
                                    info!("[PaperExec] Exit signal from strategy");
                                    break;
                                }
                                Err(_) => break,
                            }
                            // Simulate network latency: exchange → strategy.
                            // (v2 core has no balance-error cascade-cancel
                            // side-effects to drain.)
                            send_updates(updates, &update_tx, latency);
                        }
                    }
                }
                info!("[PaperExec] Stopped");
            })
            .expect("Failed to spawn paper-exec thread")
    }

    /// Collect prediction source configs for warm-up.
    fn prediction_warmup_sources(&self) -> (Vec<(String, String)>, f64) {
        let mut sources = Vec::new();
        let mut max_hours = 0.0_f64;
        for cfg in &self.config.strategies {
            if !cfg.enabled { continue; }
            if let Some(arr) = cfg.params.get("prediction_sources").and_then(|v| v.as_array()) {
                for item in arr {
                    if let Some(t) = item.as_table() {
                        let ex = t.get("exchange").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let sym = t.get("symbol").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        if !ex.is_empty() && !sym.is_empty() {
                            sources.push((ex, sym));
                        }
                    }
                }
            }
            let hours = cfg.params.get("prediction_training_period_hours")
                .and_then(|v| v.as_float().or_else(|| v.as_integer().map(|i| i as f64)))
                .unwrap_or(24.0);
            if hours > max_hours { max_hours = hours; }
        }
        sources.dedup();
        (sources, max_hours)
    }

    /// Lookback (days) for the dedicated chronological apv2 warm-up pass,
    /// driven by the per-strategy boolean `apv2_warmup`:
    ///   false (default) ⇒ OFF — pass skipped, byte-identical to legacy cold-start.
    ///   true            ⇒ warm the FULL lookback = `apv2_z_window / 288`
    ///                     (288 = 5-min buckets/day); auto-tracks z_window so it
    ///                     can never silently under-warm.
    /// Returns the max over enabled apv2 strategies (0 ⇒ pass skipped).
    fn apv2_warmup_days(&self) -> f64 {
        // buckets-per-day = 86400 / BUCKET_SECS(300) = 288
        const BUCKETS_PER_DAY: f64 = 288.0;
        let mut max_days = 0.0_f64;
        for cfg in &self.config.strategies {
            if !cfg.enabled { continue; }
            let v2_on = cfg.params.get("adaptive_params_v2_enabled")
                .and_then(|v| v.as_bool()).unwrap_or(false);
            if !v2_on { continue; }
            let on = cfg.params.get("apv2_warmup")
                .and_then(|v| v.as_bool()).unwrap_or(false);
            if !on { continue; }
            // full lookback. Default z_window = 2016 (7d).
            let zwin = cfg.params.get("apv2_z_window")
                .and_then(|v| v.as_integer())
                .map(|i| i.max(2) as f64)
                .unwrap_or(2016.0);
            let days = zwin / BUCKETS_PER_DAY;
            if days > max_days { max_days = days; }
        }
        max_days
    }

    fn spawn_strategy_thread(
        &self,
        market_rx: Receiver<MarketEvent>,
        signal_tx: Sender<Signal>,
        update_rx: Receiver<OrderUpdate>,
        backtest: bool,
        recorder_tx: Option<Sender<MarketEvent>>,
        rtt_probe_install: HashMap<
            String,
            (
                crossbeam_channel::Receiver<f64>,
                std::sync::Arc<std::sync::atomic::AtomicBool>,
                crate::exchange::polymarket::rtt_probe::ActiveTokenHandle,
            ),
        >,
        stale_threshold_handles: HashMap<String, Arc<std::sync::atomic::AtomicU64>>,
        poly_states: &HashMap<String, Arc<crate::exchange::polymarket::trade::SharedState>>,
    ) -> thread::JoinHandle<()> {
        let mut strategies = self.build_strategies(rtt_probe_install, stale_threshold_handles, poly_states);
        let data_dir = PathBuf::from(&self.config.backtest.data_dir);
        // Prediction-warmup data sources.
        //
        // **LIVE mode rule**: read ONLY from `backtest.data_dir` (typically
        // `./data`). The recorder's `recording.output_dir` (e.g. `./live_data`)
        // contains bot-recorded ticks from THIS or PRIOR live runs, and if
        // those runs ever recorded wild prices (feed glitches, predictor
        // misadjustments) the warm-up replay would feed those back into the
        // freshly-trained prediction model — a self-contaminating loop.
        // No fallback to `paper_data_dir` either, for the same reason.
        //
        // **Other modes** (Paper / Backtest / Record): keep the original
        // primary-then-fallback behaviour so paper sessions can use locally
        // cached data when the canonical store is missing.
        //
        // Defensive: also exclude any path that resolves to
        // `recording.output_dir` (in case operator pointed `data_dir` itself
        // at the recorder output by mistake).
        let mut data_dirs = vec![data_dir.clone()];
        match self.config.general.mode {
            RunMode::Live => {
                // Single-source in live: no fallback.
            }
            _ => {
                let paper_dir = PathBuf::from(&self.config.recording.paper_data_dir);
                if paper_dir != data_dir {
                    data_dirs.push(paper_dir);
                }
            }
        }
        // **Live mode — unified-storage detection (2026-05-20)**.
        //
        // Previously this site REMOVED `data_dir` from the warm-up list
        // when it equalled `recording.output_dir`, on the theory that
        // recorded live ticks could contain feed-glitch outliers that
        // would self-contaminate a freshly-trained prediction model.
        //
        // We now intentionally point both at the same canonical store
        // (`./data`) so m_dynamic can see live's just-finished events
        // without a copy step — BT/Live consistency depends on this.
        // The same recordings have always been used in BT replays
        // without issue, so the "self-contamination" risk is the same
        // category as any BT, not a Live-specific hazard.
        //
        // New behaviour: detect the unified case → INFO log + keep the
        // dir. Only flag a WARN when the dirs DIFFER (operator
        // misconfiguration → Live recordings invisible to m_dynamic).
        if self.config.general.mode == RunMode::Live {
            let recorder_out = PathBuf::from(&self.config.recording.output_dir);
            let recorder_out_canon = std::fs::canonicalize(&recorder_out)
                .unwrap_or_else(|_| recorder_out.clone());
            let unified = data_dirs.iter().any(|d| {
                let dc = std::fs::canonicalize(d).unwrap_or_else(|_| d.clone());
                dc == recorder_out_canon
            });
            if unified {
                log::info!(
                    "[Strategy] Live mode: unified storage detected \
                     (backtest.data_dir == recording.output_dir == {}). \
                     Warm-up will use this dir; adaptive_params sees live recordings live.",
                    recorder_out.display(),
                );
            } else {
                log::warn!(
                    "[Strategy] Live mode: STORAGE SPLIT — \
                     backtest.data_dir={} ≠ recording.output_dir={}. \
                     adaptive_params will NOT see this session's recordings; \
                     point both at the same path to enable unified storage.",
                    self.config.backtest.data_dir,
                    self.config.recording.output_dir,
                );
            }
        }

        // ── Prediction warm-up: done BEFORE spawning thread (before exchange feeds start) ──
        {
            let (warmup_sources, warmup_hours) = self.prediction_warmup_sources();
            if !warmup_sources.is_empty() && warmup_hours > 0.0 {
                let end = if backtest && !self.config.backtest.start_date.is_empty() {
                    chrono::DateTime::parse_from_rfc3339(&self.config.backtest.start_date)
                        .or_else(|_| chrono::NaiveDateTime::parse_from_str(&self.config.backtest.start_date, "%Y-%m-%dT%H:%M:%SZ")
                            .map(|ndt| ndt.and_utc().fixed_offset()))
                        .map(|dt| dt.with_timezone(&chrono::Utc))
                        .unwrap_or_else(|_| chrono::Utc::now())
                } else {
                    chrono::Utc::now()
                };
                let start = end - chrono::TimeDelta::seconds((warmup_hours * 3600.0) as i64);
                info!("[Strategy] Prediction warm-up: loading {:.1}h of history for {} sources (end={})",
                    warmup_hours, warmup_sources.len(), end.format("%Y-%m-%d %H:%M"));
                // Tell strategies we're entering warm-up so they can suppress
                // per-hour retrains while samples stream in.
                for s in &mut strategies { s.on_prediction_warmup_start(); }
                for (exchange, symbol) in &warmup_sources {
                    // Try each data dir in order (primary then fallback)
                    let mut loaded = false;
                    for dir in &data_dirs {
                        match crate::recorder::MarketReplayer::new(dir, exchange, symbol, start, end) {
                            Ok(mut replayer) => {
                                let mut count = 0u64;
                                while let Ok(Some((_ts, event))) = replayer.next_event() {
                                    for strategy in &mut strategies {
                                        match &event {
                                            MarketEvent::OrderBook(ob) => strategy.on_orderbook(ob),
                                            MarketEvent::Trade(t) => strategy.on_trade_tick(t),
                                            _ => {}
                                        }
                                    }
                                    count += 1;
                                }
                                if count > 0 {
                                    info!("[Strategy] Warm-up: {} events from {}/{} ({})", count, exchange, symbol, dir.display());
                                    loaded = true;
                                    break;
                                }
                            }
                            Err(_) => {}
                        }
                    }
                    if !loaded {
                        warn!("[Strategy] Warm-up: no data for {}/{}", exchange, symbol);
                    }
                }

                // Drain any live events buffered on `market_rx` during the
                // 24h warm-up replay. WS feeds are spawned BEFORE this
                // function runs, so OBs / Trades / Instruments have been
                // queuing up for however long the replay took (seconds →
                // minutes depending on data volume). If we just called
                // `on_prediction_warmup_end` now, it would set
                // `last_retrain_ns = now_ns()`; when the live strategy
                // thread eventually drains these stale-timestamped events,
                // the per-tick retrain check `ts_ns - last_retrain_ns`
                // underflowed and fired a spurious retrain.
                // `saturating_sub` in prediction.rs already papers over
                // that symptom, but it's cleaner to catch the strategy
                // state up to "now" BEFORE warmup_end runs so
                // `last_retrain_ns` actually matches the timestamps of
                // subsequent live events. warming_up is still true here,
                // so per-tick retrain stays suppressed throughout the
                // drain. We skip `on_quote` / signal emission — those
                // belong to the live thread after warm-up ends.
                let mut drained = 0u64;
                while let Ok(event) = market_rx.try_recv() {
                    if let Some(ref rtx) = recorder_tx {
                        let _ = rtx.send(event.clone());
                    }
                    for strategy in &mut strategies {
                        match &event {
                            MarketEvent::OrderBook(ob) => strategy.on_orderbook(ob),
                            MarketEvent::Trade(t) => strategy.on_trade_tick(t),
                            MarketEvent::Quote(q) => strategy.on_quote_tick(q),
                            MarketEvent::Bar(b) => strategy.on_bar(b),
                            MarketEvent::SpotPrice(sp) => strategy.on_spot_price(sp),
                            MarketEvent::Instrument(inst) => {
                                // Mirror the live thread's Instrument
                                // handler — gap-fill hist bars FIRST so
                                // the strategy's vol_model is populated
                                // before on_instrument's m_dynamic
                                // compute runs (see live-mode comment
                                // around line 2165 for the rationale —
                                // first event after restart was getting
                                // a cold-vol compute_for_event).
                                let ts_event = event.timestamp_ns();
                                let hist_reqs = strategy.load_hist_data(ts_event);
                                for req in &hist_reqs {
                                    for dir in &data_dirs {
                                        match crate::recorder::load_hist_bars(dir, req) {
                                            Ok(bars) if !bars.is_empty() => {
                                                for bar in &bars { strategy.on_hist_bar(bar); }
                                                break;
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                if !hist_reqs.is_empty() {
                                    strategy.on_hist_data_loaded(ts_event);
                                }
                                // vol_model warm → on_instrument can
                                // run m_dynamic compute against
                                // populated bars.
                                strategy.on_instrument(inst);
                            }
                            MarketEvent::Connected { exchange } => strategy.on_connected(*exchange),
                            MarketEvent::Disconnected { exchange, reason } => strategy.on_disconnected(*exchange, reason),
                            MarketEvent::TickSizeChange(tsc) => { let _ = strategy.on_tick_size_change(tsc); }
                            MarketEvent::EventStart { .. } | MarketEvent::Exit => {}
                        }
                    }
                    drained += 1;
                    // Soft cap — prevents a pathological WS flood from
                    // blocking strategy startup indefinitely. One million
                    // events at ~30 bytes each is ~30 MB; if we see this
                    // something is very wrong upstream.
                    if drained >= 1_000_000 { break; }
                }
                if drained > 0 {
                    info!("[Strategy] Drained {} live events buffered during warm-up", drained);
                }

                // Warm-up done — strategies run a single final retrain and
                // resume normal per-hour retrain cadence.
                for s in &mut strategies { s.on_prediction_warmup_end(crate::types::now_ns()); }
                info!("[Strategy] Prediction warm-up complete");
            }
        }

        // ── Dedicated chronological apv2 warm-up (live / paper) ──
        // The prediction warm-up above is per-exchange-sequential AND only
        // `prediction_training_period_hours` long, so apv2 is gated off there
        // (a single wall-clock window can't ingest out-of-order per-exchange
        // replay). Pre-fill the v2 z-baseline here over `apv2_warmup_days` in
        // TRUE wall-clock (merged k-way) order from recorded data, so apv2
        // runs on a full baseline from the first live event instead of the
        // ~1-week cold-start ramp after a restart. Feeds apv2 exclusively
        // (no predictor/index/vol/inventory effects). `apv2_warmup = false`
        // (default) ⇒ skipped. End = backtest start (paper) / now (live).
        {
            let aw_days = self.apv2_warmup_days();
            let (spot_sources, _) = self.prediction_warmup_sources();
            if aw_days > 0.0 && !spot_sources.is_empty() {
                let aw_end = if backtest && !self.config.backtest.start_date.is_empty() {
                    chrono::DateTime::parse_from_rfc3339(&self.config.backtest.start_date)
                        .or_else(|_| chrono::NaiveDateTime::parse_from_str(&self.config.backtest.start_date, "%Y-%m-%dT%H:%M:%SZ")
                            .map(|ndt| ndt.and_utc().fixed_offset()))
                        .map(|dt| dt.with_timezone(&chrono::Utc))
                        .unwrap_or_else(|_| chrono::Utc::now())
                } else {
                    chrono::Utc::now()
                };
                let aw_start = aw_end - chrono::TimeDelta::seconds((aw_days * 86400.0) as i64);
                info!("[Strategy] apv2 warm-up: {:.1}d chronological spot replay [{} → {}]",
                    aw_days, aw_start.format("%Y-%m-%d %H:%M"), aw_end.format("%Y-%m-%d %H:%M"));
                // Per source, pick the first data_dir that actually yields
                // events (mirrors the prediction warm-up's primary→fallback
                // selection), priming one buffered event each.
                let mut replayers: Vec<crate::recorder::MarketReplayer> = Vec::new();
                let mut peeked: Vec<Option<(u64, MarketEvent)>> = Vec::new();
                for (exchange, symbol) in &spot_sources {
                    for dir in &data_dirs {
                        if let Ok(mut r) = crate::recorder::MarketReplayer::new(dir, exchange, symbol, aw_start, aw_end) {
                            let first = r.next_event().ok().flatten();
                            if first.is_some() {
                                replayers.push(r);
                                peeked.push(first);
                                break;
                            }
                        }
                    }
                }
                // k-way merge by local_ts → apv2 sees venues interleaved
                // chronologically, exactly as in the real feed.
                let mut fed: u64 = 0;
                loop {
                    let best = peeked.iter().enumerate()
                        .filter_map(|(i, p)| p.as_ref().map(|(ts, _)| (i, *ts)))
                        .min_by_key(|&(_, ts)| ts);
                    let Some((idx, _)) = best else { break; };
                    let (_, event) = peeked[idx].take().unwrap();
                    match &event {
                        MarketEvent::OrderBook(ob) => { for s in &mut strategies { s.on_apv2_warmup_orderbook(ob); } }
                        MarketEvent::Trade(t) => { for s in &mut strategies { s.on_apv2_warmup_trade(t); } }
                        _ => {}
                    }
                    fed += 1;
                    peeked[idx] = replayers[idx].next_event().ok().flatten();
                }
                info!("[Strategy] apv2 warm-up complete: {} spot events fed", fed);
            }
        }

        for s in &mut strategies { s.on_init(); }

        thread::Builder::new()
            .name("strategy".into())
            .spawn(move || {
                // Pin strategy thread to its dedicated core and raise to
                // SCHED_FIFO so CPU-bound decision work isn't preempted
                // by SCHED_OTHER background tasks. Done inside the closure
                // so the affinity sticks to THIS worker thread, not the
                // spawning thread.
                crate::os_tune::pin_strategy("strategy");

                let mut last_quote_ns: Vec<u64> = vec![0; strategies.len()];

                loop {
                    crossbeam_channel::select! {
                        recv(market_rx) -> msg => {
                            match msg {
                                Ok(MarketEvent::Exit) => {
                                    info!("[Strategy] Exit event received");
                                    for s in &mut strategies {
                                        s.on_exit();
                                        for sig in s.on_shutdown() { let _ = signal_tx.send(sig); }
                                    }
                                    let _ = signal_tx.send(Signal::Exit);
                                    if let Some(ref rtx) = recorder_tx {
                                        let _ = rtx.send(MarketEvent::Exit);
                                    }
                                    return;
                                }
                                Ok(event) => {
                                    // Record market data if recorder is active
                                    if let Some(ref rtx) = recorder_tx {
                                        let _ = rtx.send(event.clone());
                                    }
                                    if backtest {
                                        if !matches!(&event, MarketEvent::Instrument(_) | MarketEvent::Connected { .. } | MarketEvent::Disconnected { .. }) {
                                            set_sim_clock(event.timestamp_ns());
                                        }
                                    }
                                    for (i, strategy) in strategies.iter_mut().enumerate() {
                                        let signals = match &event {
                                            MarketEvent::OrderBook(ob) => { strategy.on_orderbook(ob); Vec::new() }
                                            MarketEvent::Trade(t) => { strategy.on_trade_tick(t); Vec::new() }
                                            // Quote / SpotPrice update internal state
                                            // only — the quote cadence is driven
                                            // exclusively by OrderBook events (see the
                                            // OrderBook trigger block below).
                                            MarketEvent::Quote(q) => { strategy.on_quote_tick(q); Vec::new() }
                                            MarketEvent::Bar(b) => { strategy.on_bar(b); Vec::new() }
                                            MarketEvent::SpotPrice(sp) => { strategy.on_spot_price(sp); Vec::new() }
                                            MarketEvent::Instrument(inst) => {
                                                strategy.on_instrument(inst);
                                                // Load historical bars after instrument setup
                                                let ts_event = event.timestamp_ns();
                                                let hist_reqs = strategy.load_hist_data(ts_event);
                                                for req in &hist_reqs {
                                                    let mut loaded = false;
                                                    for dir in &data_dirs {
                                                        match crate::recorder::load_hist_bars(dir, req) {
                                                            Ok(bars) if !bars.is_empty() => {
                                                                info!("[Strategy] Loaded {} hist bars for {}/{} {} ({})", bars.len(), req.exchange, req.symbol, req.interval, dir.display());
                                                                for bar in &bars {
                                                                    strategy.on_hist_bar(bar);
                                                                }
                                                                loaded = true;
                                                                break;
                                                            }
                                                            _ => {}
                                                        }
                                                    }
                                                    if !loaded {
                                                        warn!("[Strategy] Failed to load hist bars for {}/{}", req.exchange, req.symbol);
                                                    }
                                                }
                                                if !hist_reqs.is_empty() {
                                                    strategy.on_hist_data_loaded(ts_event);
                                                }
                                                Vec::new()
                                            }
                                            MarketEvent::Connected { exchange } => { strategy.on_connected(*exchange); Vec::new() }
                                            MarketEvent::Disconnected { exchange, reason } => { strategy.on_disconnected(*exchange, reason); Vec::new() }
                                            MarketEvent::TickSizeChange(tsc) => { strategy.on_tick_size_change(tsc) }
                                            MarketEvent::EventStart { .. } | MarketEvent::Exit => Vec::new(),
                                        };
                                        // OrderBook events are the sole driver of the
                                        // quote cadence. Optionally restricted to
                                        // Binance OBs, with a fractional early-trigger
                                        // tolerance to absorb local-timestamp jitter.
                                        if let MarketEvent::OrderBook(ob) = &event {
                                            let venue_ok = !strategy.quote_trigger_binance_ob_only()
                                                || ob.exchange == Exchange::Binance;
                                            let interval = strategy.quote_interval_ms();
                                            let tbt = strategy.quote_tick_by_tick()
                                                && !strategy.cadence_rtt_throttle();
                                            if venue_ok && (tbt || interval > 0) {
                                                let ts = event.timestamp_ns();
                                                let fire = if tbt {
                                                    true
                                                } else {
                                                    let frac = strategy.quote_interval_tolerance_frac().clamp(0.0, 1.0);
                                                    let threshold_ns =
                                                        ((interval as f64) * 1_000_000.0 * (1.0 - frac)) as u64;
                                                    ts - last_quote_ns[i] >= threshold_ns
                                                };
                                                if fire {
                                                    last_quote_ns[i] = ts;
                                                    let ob_signals = strategy.on_quote(ts);
                                                    for sig in ob_signals {
                                                        if signal_tx.send(sig).is_err() { return; }
                                                    }
                                                }
                                            }
                                        }
                                        for sig in signals {
                                            if signal_tx.send(sig).is_err() { return; }
                                        }
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                        recv(update_rx) -> msg => {
                            match msg {
                                Ok(update) => {
                                    for s in &mut strategies {
                                        // Strategy may emit signals directly
                                        // from an OrderUpdate (e.g. immediate
                                        // reconcile on timeout) — forward them
                                        // to the executor without waiting for
                                        // the next quote tick.
                                        for sig in s.on_order_update(&update) {
                                            if signal_tx.send(sig).is_err() { return; }
                                        }
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    }
                }
                for s in &mut strategies {
                    for sig in s.on_shutdown() {
                        let _ = signal_tx.send(sig);
                    }
                }
            })
            .unwrap()
    }

    fn wait_for_shutdown(shutdown: &Arc<AtomicBool>, shutdown_tx: &Sender<MarketEvent>) {
        use signal_hook::consts::{SIGINT, SIGTERM};
        use signal_hook::iterator::Signals;

        let start_time = std::time::Instant::now();
        let mut signals = Signals::new(&[SIGINT, SIGTERM])
            .expect("Failed to register signal handlers");
        info!("Press Ctrl-C to stop...");

        // Block until a signal arrives
        if let Some(sig) = signals.forever().next() {
            let sig_name = match sig {
                SIGINT => "SIGINT (Ctrl-C)",
                SIGTERM => "SIGTERM",
                _ => "unknown",
            };
            let uptime = start_time.elapsed();
            let hours = uptime.as_secs() / 3600;
            let mins = (uptime.as_secs() % 3600) / 60;
            let secs = uptime.as_secs() % 60;

            // Read RSS from /proc/self/status (Linux) or use sysctl (macOS)
            let rss_mb = Self::get_rss_mb().unwrap_or(0.0);

            info!(
                "Shutdown: signal={}, uptime={}h{}m{}s, rss={:.1}MB, pid={}",
                sig_name, hours, mins, secs, rss_mb, std::process::id()
            );
        }

        shutdown.store(true, Ordering::Relaxed);
        let _ = shutdown_tx.send(MarketEvent::Exit);
    }

    /// Get current process RSS in MB.
    fn get_rss_mb() -> Option<f64> {
        #[cfg(target_os = "linux")]
        {
            // /proc/self/status has "VmRSS: <kb> kB"
            if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
                for line in status.lines() {
                    if line.starts_with("VmRSS:") {
                        let kb: f64 = line.split_whitespace().nth(1)?.parse().ok()?;
                        return Some(kb / 1024.0);
                    }
                }
            }
            None
        }
        #[cfg(target_os = "macos")]
        {
            // Use mach API via ps as simple fallback
            let output = std::process::Command::new("ps")
                .args(&["-o", "rss=", "-p", &std::process::id().to_string()])
                .output().ok()?;
            let kb: f64 = String::from_utf8_lossy(&output.stdout).trim().parse().ok()?;
            Some(kb / 1024.0)
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            None
        }
    }

    // ── Thread Spawning (called by HbEngine and run methods) ─────────────

    /// Spawn exchange feed threads that produce MarketEvents.
    /// Alias for paper mode: spawn feeds with sim_feed_tx for Polymarket.
    pub fn spawn_exchange_feeds_paper(
        &self,
        market_tx: Sender<MarketEvent>,
        sim_feed_tx: Option<Sender<MarketEvent>>,
        shutdown: Arc<AtomicBool>,
    ) -> Result<Vec<thread::JoinHandle<()>>> {
        self.spawn_exchange_feeds_inner(market_tx, sim_feed_tx, shutdown)
    }

    pub fn spawn_exchange_feeds(
        &self,
        market_tx: Sender<MarketEvent>,
        shutdown: Arc<AtomicBool>,
    ) -> Result<Vec<thread::JoinHandle<()>>> {
        self.spawn_exchange_feeds_inner(market_tx, None, shutdown)
    }

    fn spawn_exchange_feeds_inner(
        &self,
        market_tx: Sender<MarketEvent>,
        sim_feed_tx: Option<Sender<MarketEvent>>,
        shutdown: Arc<AtomicBool>,
    ) -> Result<Vec<thread::JoinHandle<()>>> {
        let mut handles = Vec::new();

        for exchange_cfg in &self.config.exchanges {
            if !exchange_cfg.enabled {
                continue;
            }

            let tx = market_tx.clone();
            let sim_tx = if exchange_cfg.name == "polymarket" { sim_feed_tx.clone() } else { None };
            let cfg = exchange_cfg.clone();
            let shutdown = shutdown.clone();
            // Resolve the spot kline interval BEFORE moving into the
            // feed thread — `self` (or its &refs) can't escape the
            // method into the thread closure's 'static bound.
            let spot_kline_interval: String = self.config.strategies.iter()
                .find(|s| s.enabled && self.registry.capabilities(&s.name).needs_hist_bars)
                .and_then(|s| s.params.get("hist_bar_interval"))
                .and_then(|v| v.as_str())
                .unwrap_or("1m")
                .to_string();
            // Same 'static-borrow constraint applies to `data_dir`:
            // resolve to an owned PathBuf before the move-closure. We
            // pass this into `BinanceMarket::with_data_dir` so the WS
            // task's runtime gap-fill (Phase C+) persists fetched bars
            // into `histdata/` and a subsequent restart finds them
            // locally instead of hitting REST again.
            let spot_data_dir: PathBuf = PathBuf::from(&self.config.backtest.data_dir);

            let handle = thread::Builder::new()
                .name(format!("feed-{}", cfg.name))
                .spawn(move || {
                    crate::os_tune::pin_execution(&format!("feed-{}", cfg.name));
                    let exchange = match cfg.name.as_str() {
                        "binance" => Exchange::Binance,
                        "bybit" => Exchange::Bybit,
                        "binance_futures" => Exchange::Binance, // placeholder; sends SpotPrice events
                        "chainlink" => Exchange::Polymarket, // placeholder; Chainlink sends SpotPrice events
                        "coinbase" => Exchange::Coinbase,
                        "kraken" => Exchange::Kraken,
                        "okx" => Exchange::Okx,
                        "gate" => Exchange::Gate,
                        "bitget" => Exchange::Bitget,
                        "kucoin" => Exchange::Kucoin,
                        "mexc" => Exchange::Mexc,
                        "pyth" => Exchange::Polymarket, // placeholder; Pyth sends SpotPrice events
                        "polymarket" => Exchange::Polymarket,
                        "hexmarket" => Exchange::Hexmarket,
                        other => {
                            error!("Unknown exchange: {}", other);
                            return;
                        }
                    };

                    let mut feed: Box<dyn ExchangeMarket> = match cfg.name.as_str() {
                        // spot_kline_interval was resolved above and
                        // moved into this closure — defaults to "1m"
                        // when polymaker isn't configured (or when its
                        // hist_bar_interval is absent).
                        "binance" => Box::new(
                            BinanceMarket::with_kline_interval(
                                cfg.api_key.clone(), false, spot_kline_interval.clone(),
                            )
                            // Phase C+: persist runtime WS-reconnect
                            // gap-fill bars so subsequent restarts
                            // don't re-fetch the same gap from REST.
                            .with_data_dir(spot_data_dir.clone())
                            // Config-driven WS / REST overrides
                            // (`exchanges[].wss_url` / `api_url_prefix`).
                            // Empty values pass through as "no override"
                            // → compile-time defaults.
                            .with_ws_base(cfg.wss_url.clone())
                            .with_rest_base(cfg.api_url_prefix.clone()),
                        ),
                        "binance_futures" => Box::new(
                            BinanceMarket::new(cfg.api_key.clone(), true)
                                .with_ws_base(cfg.wss_url.clone())
                                .with_rest_base(cfg.api_url_prefix.clone()),
                        ),
                        "bybit" => Box::new(crate::exchange::bybit::BybitMarket::new()),
                        "chainlink" => {
                            match cfg.source.as_str() {
                                "stream" => Box::new(crate::exchange::chainlink::ChainlinkStreamMarket::new(
                                    &cfg.api_key, &cfg.api_secret, &cfg.wss_url,
                                )),
                                _ => Box::new(crate::exchange::chainlink::ChainlinkMarket::new()),
                            }
                        }
                        "coinbase" => Box::new(crate::exchange::coinbase::CoinbaseMarket::new()),
                        "kraken" => Box::new(crate::exchange::kraken::KrakenMarket::new()),
                        "okx" => Box::new(crate::exchange::okx::OkxMarket::new()),
                        "gate" => Box::new(crate::exchange::gate::GateMarket::new()),
                        "bitget" => Box::new(crate::exchange::bitget::BitgetMarket::new()),
                        "kucoin" => Box::new(crate::exchange::kucoin::KucoinMarket::new()),
                        "mexc" => Box::new(crate::exchange::mexc::MexcMarket::new()),
                        "pyth" => Box::new(crate::exchange::pyth::PythHermesMarket::new()),
                        "polymarket" => {
                            let mut pm = PolymarketMarket::new();
                            pm.set_market_tx(tx.clone(), shutdown.clone());
                            Box::new(pm)
                        }
                        "hexmarket" => Box::new(HexmarketMarket::new(&cfg.api_url_prefix, &cfg.wss_url)),
                        _ => return,
                    };

                    if let Err(e) = feed.subscribe(&cfg.symbols) {
                        error!("[{}] Subscribe error: {}", cfg.name, e);
                        return;
                    }

                    let mut backoff = crate::exchange::ReconnectBackoff::new(100, 30_000);

                    loop {
                        if shutdown.load(Ordering::Relaxed) {
                            break;
                        }

                        if let Err(e) = feed.connect() {
                            let delay = backoff.next_delay();
                            warn!("[{}] Connect error: {}, retrying in {:.1}s...", cfg.name, e, delay.as_secs_f64());
                            let _ = tx.send(MarketEvent::Disconnected {
                                exchange,
                                reason: e.to_string(),
                            });
                            std::thread::sleep(delay);
                            continue;
                        }

                        let connected_at = std::time::Instant::now();
                        let _ = tx.send(MarketEvent::Connected { exchange });
                        let mut last_data_at = std::time::Instant::now();
                        // Per-feed stale-data timeout. The default 10 s fits
                        // spot book / trade streams that push multiple times
                        // per second, but is too tight for slower index /
                        // asset-index feeds. Observed on 2026-04-24:
                        // `binance_futures usdtusd@assetIndex` goes silent
                        // for several seconds at a time, triggering a
                        // flap-reconnect every 10 s and wasting 1+ s of
                        // hotfix recovery per cycle. Allow per-exchange
                        // override here.
                        let data_timeout = std::time::Duration::from_secs(match cfg.name.as_str() {
                            "binance_futures" => 60, // assetIndex cadence ~1-5 s, tolerate gaps
                            // chainlink RTDS (ws-live-data) is event-driven: a calm
                            // BTC market legitimately pushes no PRICE for >30 s on a
                            // HEALTHY connection. This engine watchdog only resets on
                            // price events, so 30 s flap-reconnected ~64×/31h. True
                            // liveness is the in-task 60 s read-stall watchdog, which
                            // (with the corrected "ping" → pong keepalive) resets on
                            // pong frames. Raise this to a loose backstop only.
                            "chainlink" => 120,
                            "pyth" => 30,
                            // Polymarket CLOB book diffs are event-driven: a
                            // calm 5m up/down market legitimately goes >10 s
                            // with no update. 10 s flap-reconnected ~27×/session
                            // on healthy connections. The in-task 90 s stall
                            // watchdog still catches true silent-freezes.
                            "polymarket" => 45,
                            _ => 10,
                        });

                        loop {
                            if shutdown.load(Ordering::Relaxed) {
                                break;
                            }
                            match feed.next_event() {
                                Ok(Some(event)) => {
                                    last_data_at = std::time::Instant::now();
                                    // Paper mode: also send Polymarket events to the sim_v2 core
                                    if let Some(ref stx) = sim_tx {
                                        let _ = stx.send(event.clone());
                                    }
                                    if tx.send(event).is_err() {
                                        break;
                                    }
                                }
                                Ok(None) => {
                                    // No data — check for stale connection.
                                    // Suppress the data-timeout watchdog when the feed has no
                                    // active subscription (e.g. Polymarket between events with
                                    // no currently-trading event in the series). Reconnecting
                                    // would not help because there is nothing to subscribe to,
                                    // and the resulting ~5s warn-spam churns the WS for nothing.
                                    if last_data_at.elapsed() > data_timeout
                                        && feed.has_active_subscription()
                                    {
                                        warn!("[{}] No data for {:.0}s, reconnecting...",
                                            cfg.name, last_data_at.elapsed().as_secs_f64());
                                        let _ = tx.send(MarketEvent::Disconnected {
                                            exchange,
                                            reason: "data timeout".to_string(),
                                        });
                                        break;
                                    }
                                    // While the feed is idle (no active subscription),
                                    // keep the watchdog clock fresh so we don't fire the
                                    // moment a subscription is established.
                                    if !feed.has_active_subscription() {
                                        last_data_at = std::time::Instant::now();
                                    }
                                    // `next_event()` is non-blocking — when empty we'd
                                    // otherwise busy-spin. Under SCHED_FIFO that's fatal:
                                    // `execution` / hex worker threads share core 3 at
                                    // the same priority and get zero CPU until our time
                                    // slice (kernel.sched_rr_timeslice_ms, ~100 ms by
                                    // default) expires. A short sleep yields the CPU and
                                    // costs nothing — 100 µs latency is orders of
                                    // magnitude under any WS event cadence.
                                    std::thread::sleep(std::time::Duration::from_micros(100));
                                    continue;
                                }
                                Err(e) => {
                                    warn!("[{}] Feed error: {}", cfg.name, e);
                                    let _ = tx.send(MarketEvent::Disconnected {
                                        exchange,
                                        reason: e.to_string(),
                                    });
                                    break; // break inner loop → reconnect
                                }
                            }
                        }

                        feed.disconnect();

                        // Reset backoff if connection was stable for >30s
                        if connected_at.elapsed().as_secs() > 30 { backoff.reset(); }

                        if shutdown.load(Ordering::Relaxed) {
                            break;
                        }
                        let delay = backoff.next_delay();
                        warn!("[{}] Disconnected, reconnecting in {:.1}s...", cfg.name, delay.as_secs_f64());
                        std::thread::sleep(delay);
                    }

                    feed.disconnect();
                })?;

            handles.push(handle);
        }

        Ok(handles)
    }

    /// Spawn HexMarket user WebSocket feed for real-time fill/cancel notifications.
    pub fn spawn_hex_user_feed(
        &self,
        update_tx: Sender<OrderUpdate>,
        shutdown: Arc<AtomicBool>,
    ) -> Option<thread::JoinHandle<()>> {
        let hex_cfg = self.config.exchanges.iter().find(|e| e.name == "hexmarket" && e.enabled)?;
        let private_key = &hex_cfg.private_key;
        let mnemonic = &hex_cfg.mnemonic;
        let wss_url = &hex_cfg.wss_url;

        if private_key.is_empty() && mnemonic.is_empty() {
            info!("[Engine] No hex wallet configured, skipping user feed");
            return None;
        }

        use crate::exchange::hexmarket::auth::{resolve_auth, wss_url_or_default};
        let wss_url = wss_url_or_default(wss_url).to_string();
        let api_url_prefix = crate::exchange::hexmarket::auth::api_url_prefix_or_default(&hex_cfg.api_url_prefix);

        match resolve_auth(private_key, mnemonic, api_url_prefix) {
            Ok(auth) => {
                match crate::exchange::hexmarket::user_feed::spawn_user_feed(
                    &wss_url,
                    auth.credentials,
                    update_tx,
                    shutdown,
                ) {
                    Ok(handle) => {
                        info!("[Engine] HexMarket user feed started");
                        Some(handle)
                    }
                    Err(e) => {
                        warn!("[Engine] Failed to start hex user feed: {}", e);
                        None
                    }
                }
            }
            Err(e) => {
                warn!("[Engine] Failed to resolve hex auth for user feed: {}", e);
                None
            }
        }
    }

    /// Spawn Polymarket user WebSocket feed for real-time order/trade notifications.
    /// Build a single Polymarket SharedState (auth + signer + order-id
    /// registry + HTTP agent + live position manager) shared by the user
    /// feed thread, the heartbeat thread, and the LiveRouter in the
    /// execution thread. Cloning `Arc<SharedState>` into each consumer
    /// means they all share the process-wide h2 reqwest client — one
    /// multiplexed TLS connection to clob.polymarket.com instead of each
    /// consumer spinning up its own.
    /// **Multi-instance** SharedState builder — Phase 2a of the
    /// multi-strategy refactor. Loads `secrets.toml` and constructs
    /// one `Arc<SharedState>` per polymaker strategy in the config,
    /// keyed by its `instance_id` from `[strategies.params].instance_id`.
    ///
    /// Common Polymarket transport config (`clob_version`,
    /// `api_url_prefix`, `use_batch_orders`, `rate_limit_per_second`,
    /// `http_timeout_*_ms`) still lives in `[[exchanges]] polymarket`
    /// and is shared across all instances (single h2 pool, single
    /// session-timeout table — auth, signer, and order-id registry
    /// are per-instance).
    ///
    /// Returns an empty map when no polymaker strategy is enabled or
    /// the secrets file lacks the matching `[poly.<instance_id>]`
    /// blocks. Live mode treats an empty map as "no real trading"
    /// (same semantic as the pre-multi-instance no-creds path).
    pub fn build_poly_shared_states_map(
        &self,
    ) -> HashMap<String, Arc<crate::exchange::polymarket::trade::SharedState>> {
        use crate::config::SecretsFile;

        let mut out: HashMap<String, Arc<crate::exchange::polymarket::trade::SharedState>> =
            HashMap::new();

        let poly_cfg = match self.config.exchanges.iter().find(|e| e.name == "polymarket" && e.enabled) {
            Some(c) => c,
            None => {
                info!("[Engine] Polymarket exchange disabled; no SharedState built");
                return out;
            }
        };

        // Install global FAST/CANCEL h2 timeout ONCE — shared by all
        // instances since they share the underlying h2 pool.
        crate::async_rt::init_http_timeout(poly_cfg.http_timeout_ms);

        // Resolve and load secrets.toml. Empty (= no file) is fine for
        // non-live paths (CLI / paper / BT that mocks creds); we surface
        // a clear error per instance only when that instance's block is
        // actually needed.
        //
        // Priority: `config.secrets_file` (already absolute after
        // `Config::load` resolved it relative to the main config's
        // directory) → `$HEXBOT_SECRETS` → `./secrets.toml`.
        let secrets_path = if !self.config.general.secrets_file.is_empty() {
            std::path::PathBuf::from(&self.config.general.secrets_file)
        } else {
            std::env::var("HEXBOT_SECRETS")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|_| std::path::PathBuf::from("./secrets.toml"))
        };
        let secrets = match SecretsFile::load(&secrets_path) {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    "[Engine] Failed to load secrets file at {}: {} \
                     — polymaker strategies that reference instance_id will fail to start",
                    secrets_path.display(), e,
                );
                return out;
            }
        };

        // Iterate enabled polymaker strategies and build a SharedState per
        // instance_id. Operator-set duplicate instance_ids (same id on
        // two strategy entries) collapse to the first one's SharedState
        // — second entry logs WARN and shares.
        for sc in &self.config.strategies {
            if !sc.enabled || !self.registry.capabilities(&sc.name).needs_poly_user_feed { continue; }
            let instance_id = if sc.instance_id.is_empty() {
                warn!(
                    "[Engine] Polymaker strategy missing required `instance_id` \
                     on the [[strategies]] block — skipping. Add e.g. \
                     `instance_id = \"makerA\"` to the strategy entry and a \
                     `[poly.makerA]` block in secrets.toml."
                );
                continue;
            } else {
                sc.instance_id.clone()
            };
            if out.contains_key(&instance_id) {
                warn!(
                    "[Engine] Duplicate polymaker instance_id `{}` — ignoring \
                     subsequent strategy entry; first one wins",
                    instance_id,
                );
                continue;
            }
            let creds = match secrets.poly_for(&instance_id) {
                Ok(c) => c,
                Err(e) => {
                    warn!("[Engine] {}", e);
                    continue;
                }
            };
            // Mirror this instance's signer + API creds into the POLY_*
            // env vars. The trade executor below receives them directly,
            // but the live maintenance thread (redeem + split-seed) calls
            // `load_wallet()`, which resolves creds from the ENVIRONMENT —
            // and the bot-run path never invokes the CLI's
            // `resolve_and_apply()` (that's gated to wallet subcommands in
            // main.rs), so without this push the maintenance thread fails
            // with "no wallet credentials resolved from the secrets file".
            // Builder creds (POLY_BUILDER_*) come from `[builder]` via
            // `apply_shared_to_env` at Config::load.
            // NOTE: the env wallet is a single global; with multiple
            // polymaker instances the LAST one built here wins. Correct
            // per-instance maintenance creds would need threading the
            // instance into `spawn_maintenance_thread` (follow-up).
            crate::exchange::polymarket::cli_account::apply_creds_to_env(creds);
            // builder_code is sourced solely from the shared `[builder]`
            // block — one attribution code for all of the operator's
            // wallets (per-instance `[poly.<id>].builder_code` was removed).
            let builder_code = secrets.builder.as_ref()
                .map(|b| b.builder_code.clone())
                .unwrap_or_default();
            let neg_risk = false;
            let sig_type = crate::exchange::polymarket::signer::parse_signature_type(&creds.signature_type);
            let clob_version =
                crate::exchange::polymarket::trade::ClobVersion::parse(&poly_cfg.clob_version);
            match PolymarketTrade::new_with_pool(
                &creds.api_key, &creds.api_secret, &creds.api_passphrase,
                &creds.private_key, neg_risk, poly_cfg.rate_limit_per_second,
                sig_type,
                clob_version,
                &builder_code,
                &poly_cfg.api_url_prefix,
                poly_cfg.use_batch_orders,
                &instance_id,
                &creds.funder,
                crate::exchange::polymarket::trade::GapReplayConfig {
                    interval_ms: poly_cfg.gap_replay_interval_ms,
                    periodic_rewind_ms: poly_cfg.gap_replay_periodic_rewind_ms,
                    reconnect_rewind_ms: poly_cfg.gap_replay_reconnect_rewind_ms,
                },
            ) {
                Ok(trade) => {
                    trade.prewarm_connections();
                    let shared = trade.shared_state();
                    info!(
                        "[Engine] Built Polymarket SharedState for instance_id={} \
                         (sig_type={} builder_code={})",
                        instance_id, creds.signature_type,
                        if builder_code.is_empty() { "<none>" } else { &builder_code },
                    );
                    out.insert(instance_id, shared);
                }
                Err(e) => {
                    warn!(
                        "[Engine] Failed to init Polymarket SharedState for instance_id={}: {}",
                        instance_id, e,
                    );
                }
            }
        }

        info!("[Engine] Built {} Polymarket SharedState(s)", out.len());
        out
    }

    /// **Single-instance shim** — Phase 2a back-compat. Returns the
    /// FIRST SharedState from the multi-instance map. Existing
    /// callsites (user_feed / heartbeat / rtt_probe / executor) still
    /// consume one SharedState until Phase 2b–2e fans them out per
    /// instance. Logs a one-time WARN when more than one instance is
    /// configured (only the first will actually run).
    pub fn build_poly_shared_state(&self) -> Option<Arc<crate::exchange::polymarket::trade::SharedState>> {
        let map = self.build_poly_shared_states_map();
        if map.len() > 1 {
            let ids: Vec<&String> = map.keys().collect();
            warn!(
                "[Engine] {} polymaker instances configured but Phase 2b–2e not yet wired \
                 — only one will receive user_feed / heartbeat / rtt_probe / executor traffic. \
                 Instances: {:?}",
                map.len(), ids,
            );
        }
        // BTreeMap-ish stable pick: sort by key so "first" is deterministic
        // (HashMap iter order is randomised, would surface non-determinism).
        let mut keys: Vec<&String> = map.keys().collect();
        keys.sort();
        keys.first().and_then(|k| map.get(*k).cloned())
    }

    /// Phase 2b: spawn one user_feed thread per polymaker instance.
    /// Each WS reads its own credentials off the per-instance
    /// `SharedState.auth` (set up by `build_poly_shared_states_map`),
    /// so two instances open two independent authenticated streams
    /// without sharing nonces. Returns the list of join handles so
    /// the engine teardown can wait on every one.
    pub fn spawn_poly_user_feeds(
        &self,
        update_tx: Sender<OrderUpdate>,
        shutdown: Arc<AtomicBool>,
        states: &HashMap<String, Arc<crate::exchange::polymarket::trade::SharedState>>,
    ) -> Vec<thread::JoinHandle<()>> {
        let mut handles = Vec::with_capacity(states.len());
        if states.is_empty() {
            info!("[Engine] No Polymarket SharedState(s); skipping user feeds");
            return handles;
        }
        // Deterministic spawn order so log lines / OS thread-name suffix
        // are stable across runs (helps debugging multi-instance flows).
        let mut keys: Vec<&String> = states.keys().collect();
        keys.sort();
        for id in keys {
            let shared = match states.get(id) { Some(s) => s.clone(), None => continue };
            let api_key = shared.auth.api_key.clone();
            let api_secret_b64 = shared.auth.api_secret_b64().to_string();
            let passphrase = shared.auth.passphrase.clone();
            match crate::exchange::polymarket::user_feed::spawn_user_feed(
                &api_key, &api_secret_b64, &passphrase,
                shared, update_tx.clone(), shutdown.clone(),
            ) {
                Ok(h) => {
                    info!("[Engine] Polymarket user feed started for instance_id={}", id);
                    handles.push(h);
                }
                Err(e) => {
                    warn!(
                        "[Engine] Failed to start Polymarket user feed for instance_id={}: {}",
                        id, e,
                    );
                }
            }
        }
        handles
    }

    /// Single-instance back-compat shim — Phase 2b. Builds the map
    /// in-place, picks the lexicographically-first instance, spawns
    /// one feed. Existing callers stay compiling; multi-instance
    /// callers should use `spawn_poly_user_feeds` directly.
    pub fn spawn_poly_user_feed(
        &self,
        update_tx: Sender<OrderUpdate>,
        shutdown: Arc<AtomicBool>,
        shared: Option<Arc<crate::exchange::polymarket::trade::SharedState>>,
    ) -> Option<thread::JoinHandle<()>> {
        let shared = shared?;
        let api_key = shared.auth.api_key.clone();
        let api_secret_b64 = shared.auth.api_secret_b64().to_string();
        let passphrase = shared.auth.passphrase.clone();
        match crate::exchange::polymarket::user_feed::spawn_user_feed(
            &api_key, &api_secret_b64, &passphrase,
            shared, update_tx, shutdown,
        ) {
            Ok(handle) => {
                info!("[Engine] Polymarket user feed started");
                Some(handle)
            }
            Err(e) => {
                warn!("[Engine] Failed to start Polymarket user feed: {}", e);
                None
            }
        }
    }

    /// Phase 2c: spawn one heartbeat thread per polymaker instance.
    /// Each beats its own session keep-alive ping so a connection
    /// drop in one instance doesn't take the others down. Returns
    /// the JoinHandle list so the engine teardown can wait on all.
    pub fn spawn_poly_heartbeats(
        &self,
        shutdown: Arc<AtomicBool>,
        states: &HashMap<String, Arc<crate::exchange::polymarket::trade::SharedState>>,
    ) -> Vec<thread::JoinHandle<()>> {
        let mut handles = Vec::with_capacity(states.len());
        let mut keys: Vec<&String> = states.keys().collect();
        keys.sort();
        for id in keys {
            let shared = match states.get(id) { Some(s) => s.clone(), None => continue };
            let api_key = shared.auth.api_key.clone();
            let trade = PolymarketTrade::from_shared(shared, &api_key);
            handles.push(trade.spawn_heartbeat(shutdown.clone()));
            info!("[Engine] Polymarket heartbeat started for instance_id={}", id);
        }
        handles
    }

    /// Single-instance back-compat shim — Phase 2c. Reads creds from
    /// `shared.auth` instead of `[[exchanges]] polymarket` so the
    /// legacy TOML credential fields stay unused.
    pub fn spawn_poly_heartbeat(
        &self,
        shutdown: Arc<AtomicBool>,
        shared: Option<Arc<crate::exchange::polymarket::trade::SharedState>>,
    ) -> Option<thread::JoinHandle<()>> {
        let shared = shared?;
        let api_key = shared.auth.api_key.clone();
        let trade = PolymarketTrade::from_shared(shared, &api_key);
        Some(trade.spawn_heartbeat(shutdown))
    }

    /// Spawn the execution thread that processes Signal → OrderUpdate.
    pub fn spawn_execution_thread(
        &self,
        signal_rx: Receiver<Signal>,
        update_tx: Sender<OrderUpdate>,
    ) -> thread::JoinHandle<()> {
        // Standalone caller (no live polymaker wiring) — pass an empty
        // per-instance stale-threshold map. The executor's fallback
        // dispatch will use the 150 ms legacy default for every signal.
        self.spawn_execution_thread_with_poly(
            signal_rx, update_tx, HashMap::new(), HashMap::new(),
        )
    }

    /// Same as `spawn_execution_thread` but wires a pre-built Polymarket
    /// `SharedState` into the LiveRouter so the execution thread shares its
    /// HTTP agent / connection pool with the heartbeat and user_feed.
    pub fn spawn_execution_thread_with_poly(
        &self,
        signal_rx: Receiver<Signal>,
        update_tx: Sender<OrderUpdate>,
        poly_states: HashMap<String, Arc<crate::exchange::polymarket::trade::SharedState>>,
        stale_threshold_handles: HashMap<String, Arc<std::sync::atomic::AtomicU64>>,
    ) -> thread::JoinHandle<()> {
        let config = self.config.clone();
        let hex_max_connections = config.exchanges.iter()
            .find(|e| e.name == "hexmarket")
            .map(|e| e.max_connections)
            .unwrap_or(4);

        thread::Builder::new()
            .name("execution".into())
            .spawn(move || {
                crate::os_tune::pin_execution("execution");
                let hex_cfg = config.exchanges.iter().find(|e| e.name == "hexmarket");
                let mut instance_pools: HashMap<String, Vec<Sender<(Signal, Sender<OrderUpdate>)>>> = HashMap::new();

                // NOTE: the sole residual strategy-name check. This runs inside
                // a spawned executor thread that only captured a `config` clone
                // (not `self`/`registry`), so a capability query isn't available
                // here; it gates Hexmarket execution workers (live-only).
                for (idx, strategy_cfg) in config.strategies.iter().enumerate() {
                    if strategy_cfg.name != "hexmaker" || !strategy_cfg.enabled {
                        continue;
                    }
                    let instance_id = if strategy_cfg.instance_id.is_empty() {
                        format!("hexmaker_{}", idx)
                    } else {
                        strategy_cfg.instance_id.clone()
                    };

                    let pk = strategy_cfg.params.get("private_key")
                        .and_then(|v| v.as_str()).map(|s| s.to_string())
                        .or_else(|| hex_cfg.map(|e| e.private_key.clone()))
                        .unwrap_or_default();
                    let mn = strategy_cfg.params.get("mnemonic")
                        .and_then(|v| v.as_str()).map(|s| s.to_string())
                        .or_else(|| hex_cfg.map(|e| e.mnemonic.clone()))
                        .unwrap_or_default();
                    let api = strategy_cfg.params.get("api_url_prefix")
                        .and_then(|v| v.as_str()).map(|s| s.to_string())
                        .or_else(|| hex_cfg.map(|e| e.api_url_prefix.clone()))
                        .unwrap_or_default();

                    let rate_limit = hex_cfg.map(|e| e.rate_limit_per_second).unwrap_or(10);
                    let trade = HexmarketTrade::new(&pk, &mn, &api, rate_limit);
                    info!("[Executor] Instance '{}': creating {} workers", instance_id, hex_max_connections);

                    let pool: Vec<Sender<(Signal, Sender<OrderUpdate>)>> = (0..hex_max_connections)
                        .map(|i| {
                            let mut worker = trade.clone_worker();
                            let inst_id = instance_id.clone();
                            let (tx, rx) = bounded::<(Signal, Sender<OrderUpdate>)>(64);
                            let worker_name = format!("{}-worker-{}", inst_id, i);
                            thread::Builder::new()
                                .name(worker_name.clone())
                                .spawn(move || {
                                    crate::os_tune::pin_execution(&worker_name);
                                    while let Ok((signal, update_tx)) = rx.recv() {
                                        let updates = execute_hex_signal(&mut worker, signal);
                                        for update in updates {
                                            let _ = update_tx.send(update);
                                        }
                                    }
                                })
                                .unwrap();
                            tx
                        })
                        .collect();

                    instance_pools.insert(instance_id, pool);
                }

                // Phase 2e-2: LiveRouter now holds per-instance
                // PolymarketTrade routes. `poly_route_mut(instance_id)`
                // dispatches each signal to the matching SharedState's
                // auth/signer.
                let mut fallback = LiveRouter::new_with_poly_map(&config, &poly_states);

                // Plan A — pipeline Polymarket order dispatch across a pool of
                // worker threads. The strategy enqueues BatchUpdateOrders /
                // place / cancel signals; previously this executor thread ran
                // each one INLINE, blocking on the HTTP drain (~RTT, up to the
                // 2s timeout) before pulling the next signal — so one slow
                // dispatch stalled the whole queue and signals aged past the
                // 150ms stale threshold ("Signal stale" storms under load).
                //
                // Now N workers pull from ONE shared (MPMC) channel: a free
                // worker grabs the next signal, so a busy/slow worker only
                // costs 1/N of throughput (no head-of-line block). Each worker
                // builds its own LiveRouter via `new_with_poly_map`, which
                // shares each instance's `Arc<SharedState>` (via `from_shared`)
                // — so order tracking (open_orders / coid maps) stays
                // consistent across workers, guarded by SharedState's existing
                // mutexes. The HTTP client is shared too → h2 multiplexes the
                // concurrent dispatches. Per-token cancel→place ordering is
                // preserved WITHIN a signal (serial_replace_dispatch); across
                // signals it's intentionally not serialised.
                let poly_worker_n = config.exchanges.iter()
                    .find(|e| e.name == "polymarket")
                    .map(|e| e.executor_workers).unwrap_or(8).max(1);
                let (poly_pool_tx, poly_pool_rx) =
                    bounded::<(Signal, u64, Sender<OrderUpdate>)>(CHANNEL_CAPACITY);
                let mut poly_worker_handles: Vec<thread::JoinHandle<()>> = Vec::new();
                if !poly_states.is_empty() {
                    for i in 0..poly_worker_n {
                        let mut worker = LiveRouter::new_with_poly_map(&config, &poly_states);
                        let rx = poly_pool_rx.clone();
                        let wname = format!("poly-exec-{}", i);
                        let h = thread::Builder::new()
                            .name(wname.clone())
                            .spawn(move || {
                                crate::os_tune::pin_execution(&wname);
                                while let Ok((signal, stale_ms, utx)) = rx.recv() {
                                    for update in execute_fallback_signal(&mut worker, signal, stale_ms) {
                                        if utx.send(update).is_err() { break; }
                                    }
                                }
                            })
                            .unwrap();
                        poly_worker_handles.push(h);
                    }
                    info!("[Executor] Polymarket dispatch pool: {} workers", poly_worker_n);
                }
                drop(poly_pool_rx); // main loop only sends; workers hold their clones
                // Option so Exit can drop the sender + join workers (drain all
                // in-flight dispatches) BEFORE the shutdown cancel-all, so no
                // worker places an order after cancel-all snapshots the book.
                let mut poly_pool_tx = Some(poly_pool_tx);

                // Stale-signal threshold — read from the shared
                // `Arc<AtomicU64>` handle on every signal arrival (Relaxed
                // load: it's a small int that flips at event boundaries,
                // no ordering needed against other state).
                //
                // The handle is owned by both this executor thread AND
                // the strategy. Strategy writes `quote_interval_ms × 1.5`
                // at every on_instrument as part of the per-event RTT-N
                // scaling (quote_interval scales with N, so the stale
                // threshold MUST scale with it — otherwise a slow event's
                // signals get dropped here even though they're emitted
                // on schedule for that event's tempo).
                //
                // Initial value is set engine-side from
                // polymaker.quote_interval_ms × 1.5 (or 150 ms fallback
                // when polymaker isn't enabled).
                let total_workers: usize = instance_pools.values().map(|p| p.len()).sum();
                // Phase 2e-4: per-instance stale-threshold map. Log
                // all polymaker instances so operators can verify each
                // strategy's initial quote_interval × 1.5 wired up.
                let stale_summary: String = if stale_threshold_handles.is_empty() {
                    "<none>".to_string()
                } else {
                    let mut ids: Vec<&String> = stale_threshold_handles.keys().collect();
                    ids.sort();
                    ids.iter()
                        .map(|id| format!("{}={}ms",
                            id,
                            stale_threshold_handles[*id]
                                .load(std::sync::atomic::Ordering::Relaxed)))
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                info!(
                    "[Executor] Started: {} instances, {} total hex workers, stale_threshold per instance: [{}] (initial; tracks live quote_interval × 1.5)",
                    instance_pools.len(), total_workers, stale_summary,
                );

                let mut round_robins: HashMap<String, usize> = HashMap::new();

                while let Ok(signal) = signal_rx.recv() {
                    match &signal {
                        Signal::Exit => {
                            // Stop the dispatch pool first: drop the sender so
                            // workers end their recv loops, then join so any
                            // in-flight / queued dispatch finishes BEFORE the
                            // cancel-all below (otherwise a worker could place
                            // an order after cancel-all snapshots the book).
                            poly_pool_tx = None;
                            for h in std::mem::take(&mut poly_worker_handles) { let _ = h.join(); }
                            // Phase 2e-3: walk every per-instance
                            // PolymarketTrade and wipe its book on
                            // shutdown — each instance has its own
                            // server-side book and orderID registry.
                            info!("[Executor] Exit signal, canceling all Polymarket orders across {} instance(s)...",
                                fallback.poly_routes.len().max(1));
                            // Pull keys first to avoid holding an
                            // immutable borrow while calling &mut self.
                            let ids: Vec<String> = fallback.poly_routes.keys().cloned().collect();
                            if ids.is_empty() {
                                // No instance map populated (paper / BT
                                // shim path) — fall back to the default.
                                fallback.polymarket.cancel_all_orders();
                            } else {
                                for id in &ids {
                                    if let Some(trade) = fallback.poly_routes.get_mut(id) {
                                        info!("[Executor] Exit: cancel_all_orders instance_id={}", id);
                                        trade.cancel_all_orders();
                                    }
                                }
                            }
                            info!("[Executor] Stopping");
                            drop(instance_pools);
                            break;
                        }
                        Signal::BatchUpdateOrders { exchange: Exchange::Hexmarket, .. }
                        | Signal::BatchNewOrders { exchange: Exchange::Hexmarket, .. }
                        | Signal::BatchCancelOrders { exchange: Exchange::Hexmarket, .. }
                        | Signal::CancelAll { exchange: Exchange::Hexmarket, .. }
                        | Signal::CancelOrder { exchange: Exchange::Hexmarket, .. } => {
                            let inst_id = extract_instance_id(&signal);
                            if let Some(pool) = instance_pools.get(&inst_id) {
                                let rr = round_robins.entry(inst_id).or_insert(0);
                                let idx = *rr % pool.len();
                                *rr += 1;
                                let _ = pool[idx].send((signal, update_tx.clone()));
                            } else {
                                warn!("[Executor] Unknown instance '{}', dropping signal", inst_id);
                            }
                        }
                        Signal::NewOrder(order) if order.exchange == Exchange::Hexmarket => {
                            let inst_id = order.instance_id.clone();
                            if let Some(pool) = instance_pools.get(&inst_id) {
                                let rr = round_robins.entry(inst_id).or_insert(0);
                                let idx = *rr % pool.len();
                                *rr += 1;
                                let _ = pool[idx].send((signal, update_tx.clone()));
                            } else {
                                warn!("[Executor] Unknown instance '{}', dropping signal", inst_id);
                            }
                        }
                        _ => {
                            // Phase 2e-4: lookup per-instance stale
                            // threshold. Falls back to 150 ms (legacy
                            // default) for signals whose instance_id
                            // isn't in the map — e.g. non-polymaker
                            // exchanges or paper/BT shims that pass
                            // an empty map.
                            let iid = extract_instance_id(&signal);
                            let stale_threshold_ms = stale_threshold_handles
                                .get(&iid)
                                .map(|h| h.load(std::sync::atomic::Ordering::Relaxed))
                                .unwrap_or(150);
                            // Plan A: Polymarket signals for a known instance go
                            // to the pipelined worker pool; everything else
                            // (binance, unknown iid, or pool disabled) runs
                            // inline on this thread as before.
                            if poly_states.contains_key(&iid) && poly_pool_tx.is_some() {
                                let _ = poly_pool_tx.as_ref().unwrap()
                                    .send((signal, stale_threshold_ms, update_tx.clone()));
                            } else {
                                let updates = execute_fallback_signal(&mut fallback, signal, stale_threshold_ms);
                                for update in updates {
                                    if update_tx.send(update).is_err() { break; }
                                }
                            }
                        }
                    }
                }

                // Drain the dispatch pool (no-op if Exit already did it):
                // dropping the sender ends each worker's recv loop; join so any
                // in-flight dispatch finishes before the executor thread exits.
                drop(poly_pool_tx.take());
                for h in poly_worker_handles { let _ = h.join(); }
                info!("[Executor] Thread stopped");
            })
            .unwrap()
    }
}

// ── Signal Execution Helpers ─────────────────────────────────────────────

/// Parse `BinaryOption.event_start_time` (ISO 8601, e.g. `"2026-03-29T06:10:00Z"`)
/// into unix seconds floored to the 5-min event boundary used by
/// `per_event_rtt::extract_per_event_rtt`. Returns `None` on parse
/// failure so the caller can skip the per-event override push.
fn parse_event_start_ts_secs(iso: &str) -> Option<u64> {
    if iso.is_empty() { return None; }
    let dt = chrono::DateTime::parse_from_rfc3339(iso).ok()?;
    let secs = dt.timestamp();
    if secs < 0 { return None; }
    let secs = secs as u64;
    // Floor to 5-min boundary so callers see the same key the parser
    // builds. Polymarket events ALWAYS start on the boundary, but the
    // floor is cheap insurance against any future drift.
    Some((secs / 300) * 300)
}

fn extract_instance_id(signal: &Signal) -> String {
    match signal {
        Signal::NewOrder(order) => order.instance_id.clone(),
        Signal::CancelOrder { instance_id, .. } => instance_id.clone(),
        Signal::CancelAll { instance_id, .. } => instance_id.clone(),
        Signal::BatchNewOrders { instance_id, orders, .. } => {
            // Prefer the explicit field; fall back to the first order's
            // instance_id for backward-compat with emit sites that pre-
            // dated the explicit-field addition.
            if !instance_id.is_empty() { return instance_id.clone(); }
            orders.first().map(|o| o.instance_id.clone()).unwrap_or_default()
        }
        Signal::BatchCancelOrders { instance_id, .. } => instance_id.clone(),
        Signal::BatchUpdateOrders { instance_id, place_orders, .. } => {
            if !instance_id.is_empty() { return instance_id.clone(); }
            place_orders.first().map(|o| o.instance_id.clone()).unwrap_or_default()
        }
        Signal::ReconcilePolymarket { instance_id, .. } => instance_id.clone(),
        Signal::PolymarketCancelAllOrders { instance_id, .. } => instance_id.clone(),
        _ => String::new(),
    }
}

fn execute_hex_signal(worker: &mut HexmarketTrade, signal: Signal) -> Vec<OrderUpdate> {
    match signal {
        Signal::NewOrder(order) => {
            match worker.submit_order(&order) {
                Ok(update) => vec![update],
                Err(e) => {
                    error!("[Executor] Submit error: {}", e);
                    vec![OrderUpdate {
                        client_order_id: order.client_order_id, exchange: order.exchange,
                        symbol: order.symbol, side: order.side, exchange_order_id: None,
                        status: OrderStatus::Rejected, liquidity: None,
                        filled_quantity: 0.0, remaining_quantity: order.quantity,
                        avg_fill_price: 0.0, timestamp_ns: now_ns(),
                        trade_id: None,
                        error: None,
                    }]
                }
            }
        }
        Signal::CancelOrder { exchange, client_order_id, .. } => {
            match worker.cancel_order(exchange, &client_order_id) {
                Ok(update) => vec![update],
                Err(e) => { error!("[Executor] Cancel error: {}", e); vec![] }
            }
        }
        Signal::CancelAll { exchange, symbol, .. } => {
            worker.cancel_all(exchange, &symbol).unwrap_or_else(|e| {
                error!("[Executor] Cancel-all error: {}", e); vec![]
            })
        }
        Signal::BatchNewOrders { market_id, orders, .. } => {
            worker.batch_submit_orders(&market_id, &orders).unwrap_or_else(|e| {
                error!("[Executor] Batch place error: {}", e); vec![]
            })
        }
        Signal::BatchCancelOrders { exchange, market_id, client_order_ids, .. } => {
            worker.batch_cancel_orders(exchange, &market_id, &client_order_ids).unwrap_or_else(|e| {
                error!("[Executor] Batch cancel error: {}", e); vec![]
            })
        }
        Signal::BatchUpdateOrders { exchange, market_id, cancel_client_order_ids, place_orders, .. } => {
            worker.batch_update_orders(exchange, &market_id, &cancel_client_order_ids, &place_orders).unwrap_or_else(|e| {
                error!("[Executor] Batch update error: {}", e); vec![]
            })
        }
        _ => vec![],
    }
}

fn execute_fallback_signal(executor: &mut LiveRouter, signal: Signal, stale_threshold_ms: u64) -> Vec<OrderUpdate> {
    // Build an ExecutorRejected OrderUpdate for a placement we didn't even send.
    let build_exec_rejected_place = |order: &OrderRequest| -> OrderUpdate {
        OrderUpdate {
            client_order_id: order.client_order_id.clone(),
            exchange: order.exchange,
            symbol: order.symbol.clone(),
            side: order.side,
            exchange_order_id: None,
            status: OrderStatus::ExecutorRejected,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: order.quantity,
            avg_fill_price: 0.0,
            timestamp_ns: now_ns(),
            trade_id: None,
            error: None,
        }
    };
    let build_exec_rejected_cancel = |coid: String, exchange: Exchange| -> OrderUpdate {
        OrderUpdate {
            client_order_id: coid,
            exchange,
            symbol: String::new(),
            side: Side::Buy,
            exchange_order_id: None,
            status: OrderStatus::ExecutorRejected,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: 0.0,
            avg_fill_price: 0.0,
            timestamp_ns: now_ns(),
            trade_id: None,
            error: None,
        }
    };
    let is_stale = |ts: u64| -> bool {
        if ts == 0 || stale_threshold_ms == 0 { return false; }
        let now = now_ns();
        now.saturating_sub(ts) / 1_000_000 > stale_threshold_ms
    };

    // Phase 2e-3: route every polymarket-targeted signal through
    // `poly_route_mut(instance_id)` so each instance hits its own
    // SharedState (auth / signer / orderID registry). Non-polymarket
    // signals keep the legacy trait-based dispatch via `executor.*`
    // (Binance is single-account; hexmaker has its own per-instance
    // worker pool earlier in the dispatch loop).
    let instance_id = extract_instance_id(&signal);

    match signal {
        Signal::NewOrder(order) => {
            if is_stale(order.timestamp_ns) {
                warn!("[Executor] Signal stale ({}ms > {}ms), dropping NewOrder coid={}",
                    (now_ns().saturating_sub(order.timestamp_ns))/1_000_000,
                    stale_threshold_ms, order.client_order_id);
                return vec![build_exec_rejected_place(&order)];
            }
            let result = if order.exchange == Exchange::Polymarket {
                executor.poly_route_mut(&instance_id).submit_order(&order)
            } else {
                executor.submit_order(&order)
            };
            match result {
                Ok(update) => vec![update],
                Err(e) => {
                    error!("[Executor] Submit error: {}", e);
                    vec![OrderUpdate {
                        client_order_id: order.client_order_id, exchange: order.exchange,
                        symbol: order.symbol, side: order.side, exchange_order_id: None,
                        status: OrderStatus::Rejected, liquidity: None,
                        filled_quantity: 0.0, remaining_quantity: order.quantity,
                        avg_fill_price: 0.0, timestamp_ns: now_ns(),
                        trade_id: None,
                        error: None,
                    }]
                }
            }
        }
        Signal::CancelOrder { exchange, client_order_id, timestamp_ns, .. } => {
            if is_stale(timestamp_ns) {
                warn!("[Executor] Signal stale, dropping CancelOrder coid={}", client_order_id);
                return vec![build_exec_rejected_cancel(client_order_id, exchange)];
            }
            let result = if exchange == Exchange::Polymarket {
                executor.poly_route_mut(&instance_id).cancel_order(exchange, &client_order_id)
            } else {
                executor.cancel_order(exchange, &client_order_id)
            };
            match result {
                Ok(update) => vec![update],
                Err(e) => { error!("[Executor] Cancel error: {}", e); vec![] }
            }
        }
        Signal::CancelAll { exchange, symbol, .. } => {
            let result = if exchange == Exchange::Polymarket {
                executor.poly_route_mut(&instance_id).cancel_all(exchange, &symbol)
            } else {
                executor.cancel_all(exchange, &symbol)
            };
            result.unwrap_or_else(|e| {
                error!("[Executor] Cancel-all error: {}", e); vec![]
            })
        }
        Signal::BatchNewOrders { exchange, market_id, orders, .. } => {
            let oldest_ts = orders.iter().map(|o| o.timestamp_ns).min().unwrap_or(0);
            if is_stale(oldest_ts) {
                warn!("[Executor] Signal stale, dropping BatchNewOrders ({} orders)", orders.len());
                return orders.iter().map(build_exec_rejected_place).collect();
            }
            let result = if exchange == Exchange::Polymarket {
                executor.poly_route_mut(&instance_id).batch_submit_orders(&market_id, &orders)
            } else {
                executor.batch_submit_orders(&market_id, &orders)
            };
            result.unwrap_or_else(|e| {
                error!("[Executor] Batch place error: {}", e); vec![]
            })
        }
        Signal::BatchCancelOrders { exchange, market_id, client_order_ids, timestamp_ns, .. } => {
            if is_stale(timestamp_ns) {
                warn!("[Executor] Signal stale, dropping BatchCancelOrders ({} ids)", client_order_ids.len());
                return client_order_ids.into_iter()
                    .map(|coid| build_exec_rejected_cancel(coid, exchange))
                    .collect();
            }
            let result = if exchange == Exchange::Polymarket {
                executor.poly_route_mut(&instance_id)
                    .batch_cancel_orders(exchange, &market_id, &client_order_ids)
            } else {
                executor.batch_cancel_orders(exchange, &market_id, &client_order_ids)
            };
            result.unwrap_or_else(|e| {
                error!("[Executor] Batch cancel error: {}", e); vec![]
            })
        }
        Signal::BatchUpdateOrders { exchange, market_id, cancel_client_order_ids, place_orders, timestamp_ns, .. } => {
            if is_stale(timestamp_ns) {
                warn!(
                    "[Executor] Signal stale, dropping BatchUpdateOrders ({} cancels, {} places)",
                    cancel_client_order_ids.len(), place_orders.len(),
                );
                let mut out: Vec<OrderUpdate> = cancel_client_order_ids.into_iter()
                    .map(|coid| build_exec_rejected_cancel(coid, exchange))
                    .collect();
                out.extend(place_orders.iter().map(build_exec_rejected_place));
                return out;
            }
            let result = if exchange == Exchange::Polymarket {
                executor.poly_route_mut(&instance_id).batch_update_orders(
                    exchange, &market_id, &cancel_client_order_ids, &place_orders,
                )
            } else {
                executor.batch_update_orders(
                    exchange, &market_id, &cancel_client_order_ids, &place_orders,
                )
            };
            result.unwrap_or_else(|e| {
                error!("[Executor] Batch update error: {}", e); vec![]
            })
        }
        Signal::ReconcilePolymarket { pending_places, pending_cancels, .. } => {
            executor.poly_route_mut(&instance_id)
                .reconcile_orphans(&pending_places, &pending_cancels)
        }
        Signal::PolymarketCancelAllOrders { reason, .. } => {
            warn!("[Executor] PolymarketCancelAllOrders (instance_id={}): reason={}", instance_id, reason);
            executor.poly_route_mut(&instance_id).cancel_all_orders();
            vec![]
        }
        _ => vec![],
    }
}

// ── LiveRouter ───────────────────────────────────────────────────────────

/// Routes orders to the correct exchange-specific executor.
struct LiveRouter {
    binance: BinanceTrade,
    /// Per-instance Polymarket trade clients keyed by `instance_id`.
    /// Each wraps the matching `SharedState` from
    /// `Engine::build_poly_shared_states_map`. The map preserves
    /// insertion order via lex-sorted keys at construction; the
    /// "primary" (lex-first) instance is used as the default when a
    /// signal has no instance_id or references an unknown one
    /// (with a WARN at that call site).
    poly_routes: HashMap<String, PolymarketTrade>,
    /// Lex-first instance_id from `poly_routes` — cached so the
    /// default route lookup is O(1) on the hot path. Empty iff
    /// `poly_routes` is empty (only valid for paper / BT paths that
    /// never touch poly).
    poly_default_id: String,
    /// Live-mutable back-compat view: returns the default instance's
    /// `PolymarketTrade` for callers that haven't yet been migrated
    /// to per-instance routing. Kept as a separate clone so methods
    /// taking `&mut self.polymarket` keep compiling.
    polymarket: PolymarketTrade,
    hexmarket: HexmarketTrade,
}

impl LiveRouter {
    /// Phase 2e-2: build a LiveRouter from a multi-instance SharedState
    /// map. Each `instance_id` in `states` becomes a `PolymarketTrade`
    /// inside `poly_routes`; the lex-first becomes `polymarket` (the
    /// back-compat default view).
    ///
    /// Empty map is tolerated (paper / BT paths) — `polymarket` falls
    /// back to a `PolymarketTrade::from_shared(blank_shared, "")` -
    /// shape stub which panics on any actual call, matching the
    /// previous "required for live mode" semantics.
    fn new_with_poly_map(
        config: &Config,
        states: &HashMap<String, Arc<crate::exchange::polymarket::trade::SharedState>>,
    ) -> Self {
        let hex_cfg = config.exchanges.iter().find(|e| e.name == "hexmarket");
        let hex_private_key = hex_cfg.map(|e| e.private_key.as_str()).unwrap_or("");
        let hex_mnemonic = hex_cfg.map(|e| e.mnemonic.as_str()).unwrap_or("");
        let hex_api_host = hex_cfg.map(|e| e.api_url_prefix.as_str()).unwrap_or("");

        let mut poly_routes: HashMap<String, PolymarketTrade> = HashMap::new();
        let mut keys: Vec<&String> = states.keys().collect();
        keys.sort();
        for id in &keys {
            let shared = states.get(*id).cloned().unwrap();
            let owner = shared.auth.api_key.clone();
            poly_routes.insert((*id).clone(), PolymarketTrade::from_shared(shared, &owner));
        }
        let poly_default_id = keys.first().map(|s| (*s).clone()).unwrap_or_default();

        // The legacy `self.polymarket` field still backs all
        // `ExchangeTrade` trait calls that route purely by `Exchange`.
        // Phase 2e-3 migrated the executor's hot path to
        // `poly_route_mut(iid)`, but the trait impl on LiveRouter
        // (`submit_order` / `cancel_order` / ...) still reads
        // `self.polymarket` for non-instance-aware callers — kept as
        // a clone of the lex-first instance.
        //
        // Phase 6: legacy `[[exchanges]] polymarket` credential fields
        // are removed. The only valid source of poly creds is now
        // `secrets.toml`. If the SharedState map is empty here, the
        // operator misconfigured the live mode (no polymaker
        // strategies enabled, or all their `instance_id`s missing
        // from secrets.toml). Fail loud.
        let polymarket = if !poly_default_id.is_empty() {
            let shared = states.get(&poly_default_id).cloned().unwrap();
            let owner = shared.auth.api_key.clone();
            PolymarketTrade::from_shared(shared, &owner)
        } else {
            panic!(
                "LiveRouter: no Polymarket SharedState built. Phase 6 \
                 requires every polymaker strategy's `instance_id` to \
                 match a `[poly.<id>]` block in secrets.toml. Check the \
                 enabled `[[strategies]]` blocks' instance_id and the \
                 secrets file at $HEXBOT_SECRETS / ./secrets.toml."
            )
        };

        Self {
            binance: BinanceTrade::new(),
            poly_routes,
            poly_default_id,
            polymarket,
            hexmarket: HexmarketTrade::new(hex_private_key, hex_mnemonic, hex_api_host,
                hex_cfg.map(|e| e.rate_limit_per_second).unwrap_or(10)),
        }
    }

    /// Look up the `PolymarketTrade` for a given `instance_id`. Falls
    /// back to the default (lex-first) when the id is empty or
    /// unknown, with a one-line WARN so the operator notices a
    /// signal-routing miss.
    #[allow(dead_code)]
    fn poly_route_mut(&mut self, instance_id: &str) -> &mut PolymarketTrade {
        if !instance_id.is_empty() && self.poly_routes.contains_key(instance_id) {
            return self.poly_routes.get_mut(instance_id).expect("contains_key checked");
        }
        if !instance_id.is_empty() {
            warn!(
                "[LiveRouter] Unknown polymarket instance_id `{}`; routing to default `{}`",
                instance_id, self.poly_default_id,
            );
        }
        // Fall back to the default in-place clone. This keeps the
        // hot path simple at the cost of one extra PolymarketTrade
        // allocation at construction; legacy callsites that never
        // populated an instance_id behave exactly as before.
        &mut self.polymarket
    }
}

impl ExchangeTrade for LiveRouter {
    fn submit_order(&mut self, order: &OrderRequest) -> Result<OrderUpdate> {
        match order.exchange {
            Exchange::Binance => self.binance.submit_order(order),
            Exchange::Polymarket => self.polymarket.submit_order(order),
            Exchange::Hexmarket => self.hexmarket.submit_order(order),
            _ => Err(anyhow::anyhow!("Trading not supported on {:?}", order.exchange)),
        }
    }

    fn cancel_order(&mut self, exchange: Exchange, client_order_id: &str) -> Result<OrderUpdate> {
        match exchange {
            Exchange::Binance => self.binance.cancel_order(exchange, client_order_id),
            Exchange::Polymarket => self.polymarket.cancel_order(exchange, client_order_id),
            Exchange::Hexmarket => self.hexmarket.cancel_order(exchange, client_order_id),
            _ => Err(anyhow::anyhow!("Trading not supported on {:?}", exchange)),
        }
    }

    fn cancel_all(&mut self, exchange: Exchange, symbol: &str) -> Result<Vec<OrderUpdate>> {
        match exchange {
            Exchange::Binance => self.binance.cancel_all(exchange, symbol),
            Exchange::Polymarket => self.polymarket.cancel_all(exchange, symbol),
            Exchange::Hexmarket => self.hexmarket.cancel_all(exchange, symbol),
            _ => Err(anyhow::anyhow!("Trading not supported on {:?}", exchange)),
        }
    }

    fn batch_submit_orders(&mut self, market_id: &str, orders: &[OrderRequest]) -> Result<Vec<OrderUpdate>> {
        if let Some(first) = orders.first() {
            match first.exchange {
                Exchange::Hexmarket => self.hexmarket.batch_submit_orders(market_id, orders),
                Exchange::Polymarket => self.polymarket.batch_submit_orders(market_id, orders),
                _ => {
                    let mut updates = Vec::new();
                    for order in orders {
                        updates.push(self.submit_order(order)?);
                    }
                    Ok(updates)
                }
            }
        } else {
            Ok(vec![])
        }
    }

    fn batch_cancel_orders(&mut self, exchange: Exchange, market_id: &str, client_order_ids: &[String]) -> Result<Vec<OrderUpdate>> {
        match exchange {
            Exchange::Hexmarket => self.hexmarket.batch_cancel_orders(exchange, market_id, client_order_ids),
            Exchange::Polymarket => self.polymarket.batch_cancel_orders(exchange, market_id, client_order_ids),
            _ => {
                let mut updates = Vec::new();
                for id in client_order_ids {
                    updates.push(self.cancel_order(exchange, id)?);
                }
                Ok(updates)
            }
        }
    }

    fn batch_update_orders(
        &mut self,
        exchange: Exchange,
        market_id: &str,
        cancel_client_order_ids: &[String],
        place_orders: &[OrderRequest],
    ) -> Result<Vec<OrderUpdate>> {
        match exchange {
            Exchange::Hexmarket => self.hexmarket.batch_update_orders(exchange, market_id, cancel_client_order_ids, place_orders),
            // Polymarket has its own parallel cancel+place via thread::scope
            // (uses DELETE /orders and POST /orders batch endpoints in
            // parallel). Route straight through so we don't fall back to a
            // serial cancel_order → submit_order loop.
            Exchange::Polymarket => self.polymarket.batch_update_orders(
                exchange, market_id, cancel_client_order_ids, place_orders,
            ),
            _ => {
                let mut updates = Vec::new();
                if !cancel_client_order_ids.is_empty() {
                    updates.extend(self.batch_cancel_orders(exchange, market_id, cancel_client_order_ids)?);
                }
                if !place_orders.is_empty() {
                    updates.extend(self.batch_submit_orders(market_id, place_orders)?);
                }
                Ok(updates)
            }
        }
    }

    fn name(&self) -> &str {
        "live"
    }
}
