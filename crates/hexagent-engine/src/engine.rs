//! Engine — event loop, strategy dispatch, and thread management.
//!
//! Supports four modes:
//! - Live: exchange feeds → strategy → execution
//! - Record: exchange feeds → Parquet recorder
//! - Backtest: Parquet replay → strategy → sim_v2 DES
//! - Paper: live feeds → strategy → sim_v2 matching core

use anyhow::Result;
use crossbeam_channel::{bounded, Receiver, Sender};
use log::{debug, error, info, warn};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;

use crate::config::{Config, ExchangeConfig, RunMode};
use crate::exchange::aster::AsterTrade;
use crate::exchange::binance::{BinanceMarket, BinanceTrade};
use crate::exchange::hexmarket::{HexmarketMarket, HexmarketTrade};
use crate::exchange::hyperliquid::HyperliquidTrade;
use crate::exchange::lighter::LighterTrade;
use crate::exchange::polymarket::{
    PolymarketFeedPhase, PolymarketLiveness, PolymarketLivenessSnapshot, PolymarketMarket,
    PolymarketTrade,
};
use crate::exchange::{
    ExchangeMarket, ExchangeTrade, PublicMarketPublisher, PublicMarketReceiver,
};
use crate::recorder::{MarketRecorder, MarketReplayer};
use crate::strategy::Strategy;
use crate::types::*;
use hexagent_strategy::factory::{StrategyBuildDeps, StrategyRegistry};
use hexagent_runtime::shutdown::ShutdownToken;

const CHANNEL_CAPACITY: usize = 10_000;
// Four account updates keep fill/cancel feedback responsive while bounding
// the time an already-enqueued market trigger can sit behind the biased lane.
// At the private-apply target (<=1 ms p95), 32 permitted a 32 ms quote stall.
const PRIVATE_UPDATE_BURST_BUDGET: usize = 4;
const STRATEGY_WORKER_STALL_NS: u64 = 5_000_000_000;
const WATCHDOG_MAX_DEFERRAL: std::time::Duration = std::time::Duration::from_millis(500);
const ACCOUNT_METRIC_POSITION_EPS: f64 = 1e-9;
const FEED_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(250);
const POLY_SUPERVISOR_TICK: std::time::Duration = std::time::Duration::from_millis(250);
const POLY_SUPERVISOR_LOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
const POLY_RAW_FRAME_STALL_NS: u64 = 20_000_000_000;
const POLY_MARKET_DATA_STALL_NS: u64 = 120_000_000_000;
// A live raw transport plus a silent topic can be a non-operational or quiet
// market. One rebuild probes recovery; subsequent full worker replacement is
// rate-limited while raw/feed-loop and rotation watchdogs remain fail-safe.
const POLY_MARKET_DATA_REBUILD_COOLDOWN_NS: u64 = 900_000_000_000;
const POLY_FEED_LOOP_RECONNECT_NS: u64 = 3_000_000_000;
const POLY_FEED_LOOP_FAIL_FAST_NS: u64 = 10_000_000_000;
const POLY_ROTATION_GRACE_NS: u64 = 3_000_000_000;
const POLY_REPLACEMENT_READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const POLY_MAX_CONSECUTIVE_FAILED_REPLACEMENTS: u32 = 3;
const MEMORY_TELEMETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

fn spawn_memory_telemetry(shutdown_token: ShutdownToken) -> Result<thread::JoinHandle<()>> {
    let shutdown_rx = shutdown_token.subscribe();
    Ok(thread::Builder::new()
        .name("memory-telemetry".into())
        .spawn(move || {
            crate::os_tune::pin_background("memory-telemetry");
            loop {
                match hexagent_runtime::memory::process_memory_stats() {
                    Ok(process) => {
                        let allocator = hexagent_runtime::memory::allocator_stats();
                        let recorder = crate::recorder::recorder_stats();
                        let replayer = crate::recorder::replayer_stats();
                        info!(
                            "[Memory] VmRSS={} VmHWM={} VmLck={} VmSize={} Threads={} Private_Dirty={} Locked={} recorder_rows={} recorder_bytes={} recorder_written_rows={} recorder_written_bytes={} recorder_streams={} replayer_rows={} replayer_capacity={} replayer_loaded_rows={} replayer_streams={} mimalloc_reserved={} mimalloc_committed={}",
                            process.vm_rss_bytes,
                            process.vm_hwm_bytes,
                            process.vm_lck_bytes,
                            process.vm_size_bytes,
                            process.threads,
                            process.private_dirty_bytes,
                            process.locked_bytes,
                            recorder.buffered_rows,
                            recorder.buffered_bytes,
                            recorder.written_rows,
                            recorder.written_bytes,
                            recorder.open_streams,
                            replayer.buffered_rows,
                            replayer.buffer_capacity,
                            replayer.loaded_rows,
                            replayer.active_streams,
                            allocator.reserved_bytes,
                            allocator.committed_bytes,
                        );
                    }
                    Err(error) => warn!("[Memory] process telemetry unavailable: {}", error),
                }

                match shutdown_rx.recv_timeout(MEMORY_TELEMETRY_INTERVAL) {
                    Ok(_) | Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                }
            }
        })?)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PolymarketSupervisorAction {
    Reconnect(String),
    MarketDataStall(String),
    FailFast(String),
}

#[derive(Default)]
struct PolymarketMarketDataRebuildGate {
    next_rebuild_ns: u64,
    suppression_logged: bool,
}

impl PolymarketMarketDataRebuildGate {
    fn admit(&mut self, _event_end_ns: u64, now_ns: u64) -> bool {
        if now_ns < self.next_rebuild_ns {
            return false;
        }
        self.next_rebuild_ns = now_ns.saturating_add(POLY_MARKET_DATA_REBUILD_COOLDOWN_NS);
        self.suppression_logged = false;
        true
    }

    fn remaining_ns(&self, now_ns: u64) -> u64 {
        self.next_rebuild_ns.saturating_sub(now_ns)
    }
}

#[derive(Clone)]
struct PolymarketWorkerEpoch {
    generation: u64,
    liveness: Arc<PolymarketLiveness>,
    force_clob_runtime_fallback: bool,
}

struct PolymarketWorkerSlot {
    current: RwLock<PolymarketWorkerEpoch>,
}

impl PolymarketWorkerSlot {
    fn new() -> Self {
        Self {
            current: RwLock::new(PolymarketWorkerEpoch {
                generation: 0,
                liveness: Arc::new(PolymarketLiveness::default()),
                force_clob_runtime_fallback: false,
            }),
        }
    }

    fn current(&self) -> PolymarketWorkerEpoch {
        self.current
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn replace(&self, expected_generation: u64) -> Option<PolymarketWorkerEpoch> {
        let mut current = self
            .current
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if current.generation != expected_generation {
            return None;
        }
        let replacement = PolymarketWorkerEpoch {
            generation: expected_generation.saturating_add(1),
            liveness: Arc::new(PolymarketLiveness::default()),
            force_clob_runtime_fallback: true,
        };
        *current = replacement.clone();
        Some(replacement)
    }

    fn is_current(&self, generation: u64) -> bool {
        self.current
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .generation
            == generation
    }
}

#[derive(Debug)]
struct PolymarketWorkerRebuild {
    generation: u64,
    reason: String,
}

fn pending_polymarket_recovery_stage(
    snapshot: &PolymarketLivenessSnapshot,
) -> Option<&'static str> {
    if !snapshot.connecting_seen {
        Some("Connecting")
    } else if !snapshot.subscribed_seen {
        Some("Subscribed")
    } else if !snapshot.first_raw_frame_seen {
        Some("first raw frame")
    } else if !snapshot.ready_seen {
        Some("READY")
    } else {
        None
    }
}

fn assess_polymarket_replacement_recovery(
    generation: u64,
    snapshot: &PolymarketLivenessSnapshot,
) -> Option<PolymarketSupervisorAction> {
    if generation == 0 || !snapshot.active || snapshot.ready_seen {
        return None;
    }
    let recovery_age = std::time::Duration::from_nanos(snapshot.recovery_age_ns?);
    if recovery_age < POLY_REPLACEMENT_READY_TIMEOUT {
        return None;
    }
    let missing = pending_polymarket_recovery_stage(snapshot).unwrap_or("READY");
    Some(PolymarketSupervisorAction::Reconnect(format!(
        "replacement recovery timeout after {:.1}s missing={} milestones=Connecting:{} Subscribed:{} first_raw_frame:{} READY:{}",
        recovery_age.as_secs_f64(),
        missing,
        snapshot.connecting_seen,
        snapshot.subscribed_seen,
        snapshot.first_raw_frame_seen,
        snapshot.ready_seen,
    )))
}

fn next_failed_replacement_count(
    generation: u64,
    ready_seen: bool,
    consecutive_failed: u32,
) -> u32 {
    if generation == 0 || ready_seen {
        0
    } else {
        consecutive_failed.saturating_add(1)
    }
}

fn assess_polymarket_liveness(
    snapshot: PolymarketLivenessSnapshot,
    wall_now_ns: u64,
) -> Option<PolymarketSupervisorAction> {
    if !snapshot.active {
        return None;
    }
    if snapshot
        .feed_loop_age_ns
        .is_some_and(|age| age >= POLY_FEED_LOOP_FAIL_FAST_NS)
    {
        return Some(PolymarketSupervisorAction::FailFast(format!(
            "feed_loop_age={:.1}s phase={} phase_age={:.1}s",
            snapshot.feed_loop_age_ns.unwrap_or(0) as f64 / 1e9,
            snapshot.phase.as_str(),
            snapshot.phase_age_ns as f64 / 1e9,
        )));
    }
    if snapshot
        .feed_loop_age_ns
        .is_some_and(|age| age >= POLY_FEED_LOOP_RECONNECT_NS)
    {
        return Some(PolymarketSupervisorAction::Reconnect(format!(
            "feed_loop_age={:.1}s phase={}",
            snapshot.feed_loop_age_ns.unwrap_or(0) as f64 / 1e9,
            snapshot.phase.as_str(),
        )));
    }
    if snapshot
        .raw_frame_age_ns
        .is_some_and(|age| age >= POLY_RAW_FRAME_STALL_NS)
    {
        return Some(PolymarketSupervisorAction::Reconnect(format!(
            "raw_frame_age={:.1}s",
            snapshot.raw_frame_age_ns.unwrap_or(0) as f64 / 1e9,
        )));
    }
    if snapshot.current_event_end_ns > 0
        && wall_now_ns
            >= snapshot
                .current_event_end_ns
                .saturating_add(POLY_ROTATION_GRACE_NS)
    {
        return Some(PolymarketSupervisorAction::Reconnect(format!(
            "rotation overdue by {:.1}s phase={}",
            wall_now_ns.saturating_sub(snapshot.current_event_end_ns) as f64 / 1e9,
            snapshot.phase.as_str(),
        )));
    }
    if snapshot
        .market_data_age_ns
        .is_some_and(|age| age >= POLY_MARKET_DATA_STALL_NS)
    {
        return Some(PolymarketSupervisorAction::MarketDataStall(format!(
            "market_data_age={:.1}s (raw_frame_age={:.1}s)",
            snapshot.market_data_age_ns.unwrap_or(0) as f64 / 1e9,
            snapshot.raw_frame_age_ns.unwrap_or(0) as f64 / 1e9,
        )));
    }
    None
}

fn send_feed_event_with_timeout(
    tx: &PublicMarketPublisher,
    event: MarketEvent,
    _timeout: std::time::Duration,
) -> std::result::Result<(), crossbeam_channel::SendError<MarketEvent>> {
    // Compatibility wrapper retained while feed-manager call sites migrate.
    // Public feed threads must never park behind the root router: replaceable
    // observations are best-effort; a full ordered lane returns immediately
    // and the caller reconnects/fails closed.
    crate::exchange::publish_market_event(tx, event)
}

fn request_feed_process_shutdown(reason: &str) {
    error!("[feed_fail_fast] {reason}; requesting coordinated process shutdown");
    eprintln!("feed fail-fast: {reason}");
    #[cfg(unix)]
    {
        if signal_hook::low_level::raise(signal_hook::consts::SIGTERM).is_ok() {
            return;
        }
    }
    std::process::abort();
}

fn spawn_polymarket_supervisor(
    worker_slot: Arc<PolymarketWorkerSlot>,
    rebuild_tx: Sender<PolymarketWorkerRebuild>,
    readiness: Arc<RwLock<HashMap<String, FeedReadiness>>>,
    market_tx: PublicMarketPublisher,
    shutdown: Arc<AtomicBool>,
) -> std::io::Result<thread::JoinHandle<()>> {
    thread::Builder::new()
        .name("polymarket-supervisor".to_string())
        .spawn(move || {
            crate::os_tune::pin_background("polymarket-supervisor");
            let mut last_log = std::time::Instant::now()
                .checked_sub(POLY_SUPERVISOR_LOG_INTERVAL)
                .unwrap_or_else(std::time::Instant::now);
            let mut market_data_rebuild_gate = PolymarketMarketDataRebuildGate::default();
            while !shutdown.load(Ordering::Acquire) {
                let epoch = worker_slot.current();
                let snapshot = epoch.liveness.snapshot();
                if last_log.elapsed() >= POLY_SUPERVISOR_LOG_INTERVAL {
                    info!(
                        "[feed_phase] polymarket generation={} phase={} phase_age_ms={} active={} recovery_age_ms={} raw_age_ms={} market_data_age_ms={} feed_loop_age_ms={} recovery=Connecting:{} Subscribed:{} first_raw_frame:{} READY:{} event_end_ns={}",
                        epoch.generation,
                        snapshot.phase.as_str(),
                        snapshot.phase_age_ns / 1_000_000,
                        snapshot.active,
                        snapshot.recovery_age_ns.unwrap_or(0) / 1_000_000,
                        snapshot.raw_frame_age_ns.unwrap_or(0) / 1_000_000,
                        snapshot.market_data_age_ns.unwrap_or(0) / 1_000_000,
                        snapshot.feed_loop_age_ns.unwrap_or(0) / 1_000_000,
                        snapshot.connecting_seen,
                        snapshot.subscribed_seen,
                        snapshot.first_raw_frame_seen,
                        snapshot.ready_seen,
                        snapshot.current_event_end_ns,
                    );
                    last_log = std::time::Instant::now();
                }

                let current_event_end_ns = snapshot.current_event_end_ns;
                let wall_now_ns = crate::types::now_ns();
                let action = assess_polymarket_replacement_recovery(
                    epoch.generation,
                    &snapshot,
                )
                .or_else(|| assess_polymarket_liveness(snapshot, wall_now_ns));
                let action = match action {
                    Some(PolymarketSupervisorAction::MarketDataStall(reason)) => {
                        if market_data_rebuild_gate.admit(current_event_end_ns, wall_now_ns) {
                            Some(PolymarketSupervisorAction::Reconnect(reason))
                        } else {
                            if !market_data_rebuild_gate.suppression_logged {
                                info!(
                                    "[polymarket_supervisor] suppressing repeated market-data-only worker rebuild for {:.1}s: raw transport remains live; venue/event may be non-operational ({})",
                                    market_data_rebuild_gate.remaining_ns(wall_now_ns) as f64 / 1e9,
                                    reason,
                                );
                                market_data_rebuild_gate.suppression_logged = true;
                            }
                            None
                        }
                    }
                    other => other,
                };
                match action {
                    Some(PolymarketSupervisorAction::Reconnect(reason)) => {
                        if epoch.liveness.request_reconnect(reason.clone()) {
                            set_feed_readiness(
                                &readiness,
                                "polymarket",
                                FeedReadiness::NotReady {
                                    stage: "supervisor".to_string(),
                                    reason: reason.clone(),
                                },
                            );
                            warn!(
                                "[polymarket_supervisor] external worker rebuild requested: generation={} {}",
                                epoch.generation,
                                reason,
                            );
                            if let Err(error) = rebuild_tx.try_send(PolymarketWorkerRebuild {
                                generation: epoch.generation,
                                reason: reason.clone(),
                            }) {
                                error!(
                                    "[polymarket_supervisor] failed to enqueue external worker rebuild generation={}: {}",
                                    epoch.generation,
                                    error,
                                );
                            }
                            let _ = send_feed_event_with_timeout(
                                &market_tx,
                                MarketEvent::Disconnected {
                                    exchange: Exchange::Polymarket,
                                    reason: format!("supervisor: {reason}"),
                                },
                                FEED_SEND_TIMEOUT,
                            );
                        }
                    }
                    Some(PolymarketSupervisorAction::FailFast(reason)) => {
                        set_feed_readiness(
                            &readiness,
                            "polymarket",
                            FeedReadiness::NotReady {
                                stage: "feed_loop".to_string(),
                                reason: reason.clone(),
                            },
                        );
                        let _ = send_feed_event_with_timeout(
                            &market_tx,
                            MarketEvent::Disconnected {
                                exchange: Exchange::Polymarket,
                                reason: format!("feed-loop fail-fast: {reason}"),
                            },
                            FEED_SEND_TIMEOUT,
                        );
                        request_feed_process_shutdown(&format!(
                            "polymarket feed loop did not progress: {reason}"
                        ));
                        return;
                    }
                    Some(PolymarketSupervisorAction::MarketDataStall(_)) => unreachable!(
                        "market-data stalls are admitted or suppressed before supervisor dispatch"
                    ),
                    None => {}
                }
                thread::sleep(POLY_SUPERVISOR_TICK);
            }
        })
}

fn spawn_polymarket_feed_worker(
    cfg: ExchangeConfig,
    tx: PublicMarketPublisher,
    sim_tx: Option<Sender<MarketEvent>>,
    shutdown: Arc<AtomicBool>,
    feed_readiness: Arc<RwLock<HashMap<String, FeedReadiness>>>,
    worker_slot: Arc<PolymarketWorkerSlot>,
    epoch: PolymarketWorkerEpoch,
) -> std::io::Result<thread::JoinHandle<()>> {
    thread::Builder::new()
        .name(format!("feed-polymarket-{}", epoch.generation))
        .spawn(move || {
            crate::os_tune::pin_execution("feed-polymarket");
            let liveness = epoch.liveness;
            let generation = epoch.generation;
            let force_clob_runtime_fallback = epoch.force_clob_runtime_fallback;
            let still_current = || worker_slot.is_current(generation);
            liveness.set_phase(PolymarketFeedPhase::Starting);
            liveness.heartbeat_feed_loop();

            let make_feed = || {
                let mut feed = PolymarketMarket::with_liveness(liveness.clone());
                if force_clob_runtime_fallback {
                    feed.force_clob_runtime_fallback();
                }
                feed.set_market_tx(tx.clone(), shutdown.clone());
                feed
            };
            let mut feed = make_feed();
            let mut subscribe_backoff = crate::exchange::ReconnectBackoff::new(1_000, 30_000);
            let mut subscribe_failures = 0_u64;
            let subscribe_started = std::time::Instant::now();

            loop {
                liveness.set_phase(PolymarketFeedPhase::Subscribe);
                liveness.heartbeat_feed_loop();
                if shutdown.load(Ordering::Relaxed) || !still_current() {
                    feed.disconnect();
                    liveness.set_phase(PolymarketFeedPhase::Stopped);
                    return;
                }
                match feed.subscribe(&cfg.symbols) {
                    Ok(()) => {
                        if subscribe_failures > 0 {
                            info!(
                                "[feed_health] polymarket readiness=SUBSCRIBED recovered_after={:.1}s attempts={} generation={}",
                                subscribe_started.elapsed().as_secs_f64(),
                                subscribe_failures + 1,
                                generation,
                            );
                        }
                        break;
                    }
                    Err(error) => {
                        subscribe_failures = subscribe_failures.saturating_add(1);
                        let delay = subscribe_backoff
                            .next_delay()
                            .min(std::time::Duration::from_secs(30));
                        set_feed_readiness(
                            &feed_readiness,
                            "polymarket",
                            FeedReadiness::NotReady {
                                stage: "subscribe".to_string(),
                                reason: error.to_string(),
                            },
                        );
                        if subscribe_failures == 1 {
                            error!(
                                "[feed_health] polymarket readiness=NOT_READY stage=subscribe error={} retrying_in={:.1}s generation={}",
                                error,
                                delay.as_secs_f64(),
                                generation,
                            );
                        } else {
                            warn!(
                                "[polymarket] Subscribe attempt {} failed: {}; retrying in {:.1}s generation={}",
                                subscribe_failures,
                                error,
                                delay.as_secs_f64(),
                                generation,
                            );
                        }
                        let _ = send_feed_event_with_timeout(
                            &tx,
                            MarketEvent::Disconnected {
                                exchange: Exchange::Polymarket,
                                reason: format!("subscription unavailable: {error}"),
                            },
                            FEED_SEND_TIMEOUT,
                        );
                        feed.disconnect();
                        feed = make_feed();
                        liveness.set_phase(PolymarketFeedPhase::Backoff);
                        liveness.heartbeat_feed_loop();
                        if !sleep_with_shutdown(&shutdown, delay) || !still_current() {
                            feed.disconnect();
                            liveness.set_phase(PolymarketFeedPhase::Stopped);
                            return;
                        }
                    }
                }
            }

            let mut backoff = crate::exchange::ReconnectBackoff::new(100, 30_000);
            'connections: loop {
                if shutdown.load(Ordering::Relaxed) || !still_current() {
                    break;
                }
                liveness.set_phase(PolymarketFeedPhase::Connect);
                liveness.heartbeat_feed_loop();
                if let Err(error) = feed.connect() {
                    let delay = backoff.next_delay();
                    set_feed_readiness(
                        &feed_readiness,
                        "polymarket",
                        FeedReadiness::NotReady {
                            stage: "connect".to_string(),
                            reason: error.to_string(),
                        },
                    );
                    warn!(
                        "[polymarket] Connect error: {}, retrying in {:.1}s generation={}",
                        error,
                        delay.as_secs_f64(),
                        generation,
                    );
                    let _ = send_feed_event_with_timeout(
                        &tx,
                        MarketEvent::Disconnected {
                            exchange: Exchange::Polymarket,
                            reason: error.to_string(),
                        },
                        FEED_SEND_TIMEOUT,
                    );
                    liveness.set_phase(PolymarketFeedPhase::Backoff);
                    liveness.heartbeat_feed_loop();
                    if !sleep_with_shutdown(&shutdown, delay) {
                        break;
                    }
                    continue;
                }

                let connected_at = std::time::Instant::now();
                set_feed_readiness(
                    &feed_readiness,
                    "polymarket",
                    FeedReadiness::NotReady {
                        stage: "subscription".to_string(),
                        reason: "awaiting CLOB subscription confirmation".to_string(),
                    },
                );
                info!(
                    "[feed_health] polymarket readiness=NOT_READY stage=subscription reason=awaiting_clob_subscription generation={}",
                    generation,
                );
                let mut last_data_at = std::time::Instant::now();
                let data_timeout = std::time::Duration::from_secs(45);

                loop {
                    liveness.set_phase(PolymarketFeedPhase::Poll);
                    liveness.heartbeat_feed_loop();
                    if shutdown.load(Ordering::Relaxed) || !still_current() {
                        break 'connections;
                    }
                    let next_event = feed.next_event();
                    liveness.heartbeat_feed_loop();
                    if !still_current() {
                        break 'connections;
                    }
                    match next_event {
                        Ok(Some(event)) => {
                            if !event.has_finite_market_values() {
                                warn!(
                                    "[feed_health] polymarket rejected market event with NaN/Infinity before dispatch"
                                );
                                continue;
                            }
                            last_data_at = std::time::Instant::now();
                            if let Some(state) = polymarket_readiness_transition(&event) {
                                set_feed_readiness(&feed_readiness, "polymarket", state.clone());
                                match state {
                                    FeedReadiness::Ready => {
                                        liveness.mark_ready();
                                        info!(
                                            "[feed_health] polymarket readiness=READY stage=market_stream generation={}",
                                            generation,
                                        );
                                    }
                                    FeedReadiness::NotReady { reason, .. } => {
                                        if is_routine_clob_resubscribe(&reason) {
                                            info!(
                                                "[feed_health] polymarket readiness=NOT_READY stage=data_stream error={} generation={}",
                                                reason,
                                                generation,
                                            );
                                        } else {
                                            warn!(
                                                "[feed_health] polymarket readiness=NOT_READY stage=data_stream error={} generation={}",
                                                reason,
                                                generation,
                                            );
                                        }
                                    }
                                    FeedReadiness::Starting => {}
                                }
                            }
                            if let Some(ref sim_sender) = sim_tx {
                                if let Err(error) = sim_sender.send_timeout(
                                    event.clone(),
                                    FEED_SEND_TIMEOUT,
                                ) {
                                    request_feed_process_shutdown(&format!(
                                        "polymarket sim-feed dispatch blocked for {}ms: {}",
                                        FEED_SEND_TIMEOUT.as_millis(),
                                        error,
                                    ));
                                    return;
                                }
                            }
                            liveness.set_phase(PolymarketFeedPhase::Dispatch);
                            if let Err(error) = send_feed_event_with_timeout(
                                &tx,
                                event,
                                FEED_SEND_TIMEOUT,
                            ) {
                                set_feed_readiness(
                                    &feed_readiness,
                                    "polymarket",
                                    FeedReadiness::NotReady {
                                        stage: "dispatch".to_string(),
                                        reason: error.to_string(),
                                    },
                                );
                                request_feed_process_shutdown(&format!(
                                    "polymarket market dispatch blocked for {}ms: {}",
                                    FEED_SEND_TIMEOUT.as_millis(),
                                    error,
                                ));
                                return;
                            }
                        }
                        Ok(None) => {
                            if last_data_at.elapsed() > data_timeout
                                && feed.has_active_subscription()
                            {
                                set_feed_readiness(
                                    &feed_readiness,
                                    "polymarket",
                                    FeedReadiness::NotReady {
                                        stage: "data_stream".to_string(),
                                        reason: "data timeout".to_string(),
                                    },
                                );
                                warn!(
                                    "[polymarket] No data for {:.0}s, reconnecting generation={}...",
                                    last_data_at.elapsed().as_secs_f64(),
                                    generation,
                                );
                                let _ = send_feed_event_with_timeout(
                                    &tx,
                                    MarketEvent::Disconnected {
                                        exchange: Exchange::Polymarket,
                                        reason: "data timeout".to_string(),
                                    },
                                    FEED_SEND_TIMEOUT,
                                );
                                break;
                            }
                            if !feed.has_active_subscription() {
                                last_data_at = std::time::Instant::now();
                            }
                            std::thread::sleep(std::time::Duration::from_micros(100));
                        }
                        Err(error) => {
                            if !still_current() {
                                break 'connections;
                            }
                            set_feed_readiness(
                                &feed_readiness,
                                "polymarket",
                                FeedReadiness::NotReady {
                                    stage: "data_stream".to_string(),
                                    reason: error.to_string(),
                                },
                            );
                            warn!("[polymarket] Feed error: {}", error);
                            let _ = send_feed_event_with_timeout(
                                &tx,
                                MarketEvent::Disconnected {
                                    exchange: Exchange::Polymarket,
                                    reason: error.to_string(),
                                },
                                FEED_SEND_TIMEOUT,
                            );
                            break;
                        }
                    }
                }

                feed.disconnect();
                if connected_at.elapsed().as_secs() > 30 {
                    backoff.reset();
                }
                if shutdown.load(Ordering::Relaxed) || !still_current() {
                    break;
                }
                let delay = backoff.next_delay();
                warn!(
                    "[polymarket] Disconnected, reconnecting in {:.1}s generation={}...",
                    delay.as_secs_f64(),
                    generation,
                );
                liveness.set_phase(PolymarketFeedPhase::Backoff);
                liveness.heartbeat_feed_loop();
                if !sleep_with_shutdown(&shutdown, delay) {
                    break;
                }
            }

            feed.disconnect();
            liveness.set_phase(PolymarketFeedPhase::Stopped);
            liveness.heartbeat_feed_loop();
            info!(
                "[polymarket_worker] stopped generation={} current={}",
                generation,
                still_current(),
            );
        })
}

fn spawn_polymarket_feed_manager(
    cfg: ExchangeConfig,
    tx: PublicMarketPublisher,
    sim_tx: Option<Sender<MarketEvent>>,
    shutdown: Arc<AtomicBool>,
    feed_readiness: Arc<RwLock<HashMap<String, FeedReadiness>>>,
) -> std::io::Result<Vec<thread::JoinHandle<()>>> {
    let worker_slot = Arc::new(PolymarketWorkerSlot::new());
    let (rebuild_tx, rebuild_rx) = bounded::<PolymarketWorkerRebuild>(1);
    let supervisor = spawn_polymarket_supervisor(
        worker_slot.clone(),
        rebuild_tx,
        feed_readiness.clone(),
        tx.clone(),
        shutdown.clone(),
    )?;
    let manager = thread::Builder::new()
        .name("polymarket-feed-manager".to_string())
        .spawn(move || {
            crate::os_tune::pin_background("polymarket-feed-manager");
            let initial = worker_slot.current();
            let mut workers = Vec::new();
            let mut consecutive_failed_replacements = 0_u32;
            match spawn_polymarket_feed_worker(
                cfg.clone(),
                tx.clone(),
                sim_tx.clone(),
                shutdown.clone(),
                feed_readiness.clone(),
                worker_slot.clone(),
                initial,
            ) {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    request_feed_process_shutdown(&format!(
                        "failed to spawn initial polymarket feed worker: {error}"
                    ));
                    return;
                }
            }

            while !shutdown.load(Ordering::Acquire) {
                match rebuild_rx.recv_timeout(std::time::Duration::from_millis(250)) {
                    Ok(command) => {
                        let current = worker_slot.current();
                        if current.generation != command.generation {
                            info!(
                                "[polymarket_manager] stale rebuild ignored generation={} reason={}",
                                command.generation,
                                command.reason,
                            );
                            continue;
                        }
                        let recovery = current.liveness.snapshot();
                        consecutive_failed_replacements = next_failed_replacement_count(
                            current.generation,
                            recovery.ready_seen,
                            consecutive_failed_replacements,
                        );
                        if consecutive_failed_replacements
                            >= POLY_MAX_CONSECUTIVE_FAILED_REPLACEMENTS
                        {
                            let missing = pending_polymarket_recovery_stage(&recovery)
                                .unwrap_or("READY");
                            let reason = format!(
                                "CLOB replacement circuit breaker tripped after {} consecutive generations without READY; generation={} missing={} milestones=Connecting:{} Subscribed:{} first_raw_frame:{} READY:{} last_reason={}",
                                consecutive_failed_replacements,
                                current.generation,
                                missing,
                                recovery.connecting_seen,
                                recovery.subscribed_seen,
                                recovery.first_raw_frame_seen,
                                recovery.ready_seen,
                                command.reason,
                            );
                            set_feed_readiness(
                                &feed_readiness,
                                "polymarket",
                                FeedReadiness::NotReady {
                                    stage: "replacement_circuit_breaker".to_string(),
                                    reason: reason.clone(),
                                },
                            );
                            let _ = send_feed_event_with_timeout(
                                &tx,
                                MarketEvent::Disconnected {
                                    exchange: Exchange::Polymarket,
                                    reason: reason.clone(),
                                },
                                FEED_SEND_TIMEOUT,
                            );
                            request_feed_process_shutdown(&reason);
                            return;
                        }
                        let Some(replacement) = worker_slot.replace(command.generation) else {
                            info!(
                                "[polymarket_manager] stale rebuild ignored generation={} reason={}",
                                command.generation,
                                command.reason,
                            );
                            continue;
                        };
                        warn!(
                            "[polymarket_manager] rebuilding entire feed worker old_generation={} new_generation={} runtime=general-fallback consecutive_failed_replacements={} reason={}",
                            command.generation,
                            replacement.generation,
                            consecutive_failed_replacements,
                            command.reason,
                        );
                        match spawn_polymarket_feed_worker(
                            cfg.clone(),
                            tx.clone(),
                            sim_tx.clone(),
                            shutdown.clone(),
                            feed_readiness.clone(),
                            worker_slot.clone(),
                            replacement,
                        ) {
                            Ok(worker) => workers.push(worker),
                            Err(error) => {
                                request_feed_process_shutdown(&format!(
                                    "failed to rebuild polymarket feed worker: {error}"
                                ));
                                return;
                            }
                        }
                        let mut idx = 0;
                        while idx < workers.len() {
                            if workers[idx].is_finished() {
                                let worker = workers.swap_remove(idx);
                                let _ = worker.join();
                            } else {
                                idx += 1;
                            }
                        }
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                }
            }
            let current = worker_slot.current();
            current.liveness.request_reconnect("process shutdown");
            let finished = workers.iter().filter(|worker| worker.is_finished()).count();
            info!(
                "[polymarket_manager] stopping generations={} finished={}",
                workers.len(),
                finished,
            );
        })?;
    Ok(vec![supervisor, manager])
}

#[cfg(test)]
mod polymarket_supervisor_tests {
    use super::*;

    fn healthy_snapshot() -> PolymarketLivenessSnapshot {
        PolymarketLivenessSnapshot {
            active: true,
            phase: PolymarketFeedPhase::Poll,
            phase_age_ns: 1_000_000_000,
            connecting_seen: true,
            subscribed_seen: true,
            first_raw_frame_seen: true,
            ready_seen: true,
            recovery_age_ns: Some(1_000_000_000),
            raw_frame_age_ns: Some(1_000_000_000),
            market_data_age_ns: Some(1_000_000_000),
            feed_loop_age_ns: Some(1_000_000),
            current_event_end_ns: 2_000_000_000_000,
        }
    }

    #[test]
    fn fault_injection_raw_transport_stall_forces_reconnect() {
        let mut snapshot = healthy_snapshot();
        snapshot.raw_frame_age_ns = Some(POLY_RAW_FRAME_STALL_NS);
        assert!(matches!(
            assess_polymarket_liveness(snapshot, 1_000_000_000_000),
            Some(PolymarketSupervisorAction::Reconnect(reason))
                if reason.contains("raw_frame_age")
        ));
    }

    #[test]
    fn fault_injection_control_frames_cannot_hide_market_data_stall() {
        let mut snapshot = healthy_snapshot();
        snapshot.raw_frame_age_ns = Some(100_000_000);
        snapshot.market_data_age_ns = Some(POLY_MARKET_DATA_STALL_NS);
        assert!(matches!(
            assess_polymarket_liveness(snapshot, 1_000_000_000_000),
            Some(PolymarketSupervisorAction::MarketDataStall(reason))
                if reason.contains("market_data_age")
        ));
    }

    #[test]
    fn market_data_only_rebuild_is_rate_limited_across_event_rotations() {
        let mut gate = PolymarketMarketDataRebuildGate::default();
        let now = 1_000_000_000_000;
        let event_end = 2_000_000_000_000;

        assert!(gate.admit(event_end, now));
        assert!(!gate.admit(event_end, now + POLY_MARKET_DATA_REBUILD_COOLDOWN_NS - 1));
        assert!(
            !gate.admit(event_end + 300_000_000_000, now + 1),
            "a routine event rotation must not bypass the worker rebuild cooldown"
        );
        assert!(gate.admit(event_end, now + POLY_MARKET_DATA_REBUILD_COOLDOWN_NS));
    }

    #[test]
    fn fault_injection_feed_loop_stall_escalates_from_reconnect_to_fail_fast() {
        let mut snapshot = healthy_snapshot();
        snapshot.phase = PolymarketFeedPhase::Dispatch;
        snapshot.feed_loop_age_ns = Some(POLY_FEED_LOOP_RECONNECT_NS);
        assert!(matches!(
            assess_polymarket_liveness(snapshot, 1_000_000_000_000),
            Some(PolymarketSupervisorAction::Reconnect(_))
        ));

        snapshot.feed_loop_age_ns = Some(POLY_FEED_LOOP_FAIL_FAST_NS);
        assert!(matches!(
            assess_polymarket_liveness(snapshot, 1_000_000_000_000),
            Some(PolymarketSupervisorAction::FailFast(reason))
                if reason.contains("phase=dispatch")
        ));
    }

    #[test]
    fn fault_injection_overdue_rotation_forces_reconnect() {
        let mut snapshot = healthy_snapshot();
        snapshot.current_event_end_ns = 10_000_000_000;
        snapshot.market_data_age_ns = Some(POLY_MARKET_DATA_STALL_NS * 10);
        let now = snapshot.current_event_end_ns + POLY_ROTATION_GRACE_NS;
        assert!(matches!(
            assess_polymarket_liveness(snapshot, now),
            Some(PolymarketSupervisorAction::Reconnect(reason))
                if reason.contains("rotation overdue")
        ));
    }

    #[test]
    fn intentional_disconnect_and_backoff_are_not_supervised_as_stalls() {
        let mut snapshot = healthy_snapshot();
        snapshot.active = false;
        snapshot.phase = PolymarketFeedPhase::Backoff;
        snapshot.raw_frame_age_ns = Some(POLY_RAW_FRAME_STALL_NS * 10);
        snapshot.market_data_age_ns = Some(POLY_MARKET_DATA_STALL_NS * 10);
        snapshot.feed_loop_age_ns = Some(POLY_FEED_LOOP_FAIL_FAST_NS * 10);
        assert_eq!(
            assess_polymarket_liveness(snapshot, u64::MAX),
            None,
            "an intentional disconnected/backoff phase must never fail-fast",
        );
    }

    #[test]
    fn dispatch_publish_is_nonblocking_and_phase_is_observable() {
        let (tx, _rx) = crate::exchange::market_event_channel(1);
        crate::exchange::publish_market_event(&tx, MarketEvent::Exit).unwrap();
        let started = std::time::Instant::now();
        assert!(matches!(
            send_feed_event_with_timeout(
                &tx,
                MarketEvent::Exit,
                std::time::Duration::from_millis(5),
            ),
            Err(crossbeam_channel::SendError(_))
        ));
        assert!(started.elapsed() < std::time::Duration::from_millis(100));

        let liveness = PolymarketLiveness::default();
        liveness.set_phase(PolymarketFeedPhase::Dispatch);
        assert_eq!(liveness.snapshot().phase, PolymarketFeedPhase::Dispatch);
    }

    #[test]
    fn external_rebuild_revokes_stuck_worker_generation_without_its_cooperation() {
        let slot = PolymarketWorkerSlot::new();
        let stuck = slot.current();
        assert!(slot.is_current(stuck.generation));

        let replacement = slot
            .replace(stuck.generation)
            .expect("supervisor replaces current generation");
        assert_eq!(replacement.generation, stuck.generation + 1);
        assert!(!slot.is_current(stuck.generation));
        assert!(slot.is_current(replacement.generation));
        assert!(!Arc::ptr_eq(&stuck.liveness, &replacement.liveness));
        assert!(replacement.force_clob_runtime_fallback);
        assert!(
            slot.replace(stuck.generation).is_none(),
            "a delayed duplicate rebuild cannot replace the fresh worker",
        );
    }

    #[test]
    fn replacement_timeout_reports_the_first_missing_recovery_milestone() {
        let mut snapshot = healthy_snapshot();
        snapshot.ready_seen = false;
        snapshot.first_raw_frame_seen = false;
        snapshot.recovery_age_ns = Some(
            POLY_REPLACEMENT_READY_TIMEOUT
                .as_nanos()
                .min(u64::MAX as u128) as u64,
        );
        let action = assess_polymarket_replacement_recovery(1, &snapshot);
        assert!(matches!(
            action,
            Some(PolymarketSupervisorAction::Reconnect(reason))
                if reason.contains("missing=first raw frame")
                    && reason.contains("READY:false")
        ));

        snapshot.recovery_age_ns = Some(
            (POLY_REPLACEMENT_READY_TIMEOUT - std::time::Duration::from_millis(1))
                .as_nanos()
                .min(u64::MAX as u128) as u64,
        );
        assert_eq!(
            assess_polymarket_replacement_recovery(1, &snapshot),
            None,
            "a replacement gets the full bounded recovery window",
        );
    }

    #[test]
    fn replacement_timeout_is_suspended_while_legitimately_between_events() {
        let mut snapshot = healthy_snapshot();
        snapshot.active = false;
        snapshot.ready_seen = false;
        snapshot.recovery_age_ns = Some(u64::MAX);
        assert_eq!(assess_polymarket_replacement_recovery(1, &snapshot), None);
    }

    #[test]
    fn three_consecutive_non_ready_replacements_trip_and_ready_resets_count() {
        let mut failures = next_failed_replacement_count(0, false, 0);
        assert_eq!(failures, 0, "the initial worker is not a replacement");
        failures = next_failed_replacement_count(1, false, failures);
        failures = next_failed_replacement_count(2, false, failures);
        failures = next_failed_replacement_count(3, false, failures);
        assert_eq!(failures, POLY_MAX_CONSECUTIVE_FAILED_REPLACEMENTS);

        failures = next_failed_replacement_count(4, true, failures);
        assert_eq!(
            failures, 0,
            "a generation that reached READY resets the breaker"
        );
    }
}

fn account_metric_position_summary(positions: &HashMap<String, f64>) -> (usize, f64, usize) {
    positions.values().fold(
        (0usize, 0.0f64, 0usize),
        |(count, total_abs, invalid), quantity| {
            if !quantity.is_finite() {
                (
                    count.saturating_add(1),
                    total_abs,
                    invalid.saturating_add(1),
                )
            } else if quantity.abs() > ACCOUNT_METRIC_POSITION_EPS {
                (count.saturating_add(1), total_abs + quantity.abs(), invalid)
            } else {
                (count, total_abs, invalid)
            }
        },
    )
}

#[cfg(test)]
mod account_metric_tests {
    use super::*;

    #[test]
    fn position_logging_hides_zero_and_float_dust_but_keeps_material_values() {
        let positions = HashMap::from([
            ("zero".to_string(), 0.0),
            ("negative_zero".to_string(), -0.0),
            ("dust".to_string(), 2.84e-14),
            ("negative_dust".to_string(), -1e-12),
            ("epsilon".to_string(), ACCOUNT_METRIC_POSITION_EPS),
            ("long".to_string(), 1e-6),
            ("short".to_string(), -0.25),
            ("invalid".to_string(), f64::NAN),
        ]);

        let (count, total_abs, invalid) = account_metric_position_summary(&positions);

        assert_eq!(count, 3);
        assert!((total_abs - 0.250001).abs() < 1e-12);
        assert_eq!(invalid, 1);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShutdownTrigger {
    Signal(i32),
    CriticalThread(&'static str),
}

fn first_finished_critical_thread(
    critical_threads: &[(&'static str, &thread::JoinHandle<()>)],
) -> Option<&'static str> {
    critical_threads
        .iter()
        .find_map(|(name, handle)| handle.is_finished().then_some(*name))
}

fn account_ledger_display_path(path: &std::path::Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequiredAccountLedgerStatus {
    NotRequired,
    Existing,
    BootstrapMissing,
}

fn validate_required_account_ledger(
    path: Option<&std::path::Path>,
    require_existing: bool,
    bootstrap_allowed: bool,
) -> std::result::Result<RequiredAccountLedgerStatus, String> {
    if !require_existing {
        return Ok(RequiredAccountLedgerStatus::NotRequired);
    }
    let Some(path) = path else {
        return Err(
            "account_ledger_require_existing=true but account_ledger_dir is empty".to_string(),
        );
    };
    match std::fs::metadata(path) {
        Ok(metadata) if !metadata.is_file() => Err(format!(
            "required durable account ledger exists but is not a file: {}",
            account_ledger_display_path(path).display(),
        )),
        Ok(_) if bootstrap_allowed => Err(format!(
            "durable account ledger already exists but the account remains in the one-time account_ledger_bootstrap_accounts allowlist: {}; remove the stale bootstrap entry",
            account_ledger_display_path(path).display(),
        )),
        Ok(_) => Ok(RequiredAccountLedgerStatus::Existing),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && bootstrap_allowed => {
            Ok(RequiredAccountLedgerStatus::BootstrapMissing)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Err(format!(
            "required durable account ledger is missing: {}",
            account_ledger_display_path(path).display(),
        )),
        Err(error) => Err(format!(
            "failed to inspect required durable account ledger {}: {error}",
            account_ledger_display_path(path).display(),
        )),
    }
}

fn verify_bootstrapped_account_ledger(
    path: &std::path::Path,
    account_id: &str,
) -> std::result::Result<u64, String> {
    let bytes = std::fs::read(path).map_err(|error| {
        format!(
            "read bootstrapped account ledger {}: {error}",
            account_ledger_display_path(path).display(),
        )
    })?;
    let persisted: serde_json::Value = serde_json::from_slice(&bytes).map_err(|error| {
        format!(
            "parse bootstrapped account ledger {}: {error}",
            account_ledger_display_path(path).display(),
        )
    })?;
    if persisted.get("account_id").and_then(|value| value.as_str()) != Some(account_id) {
        return Err(format!(
            "bootstrapped account ledger {} does not belong to `{account_id}`",
            account_ledger_display_path(path).display(),
        ));
    }
    if persisted
        .get("version")
        .and_then(|value| value.as_u64())
        .is_none()
        || !persisted
            .get("state")
            .is_some_and(|value| value.is_object())
    {
        return Err(format!(
            "bootstrapped account ledger {} is missing its durable version/state envelope",
            account_ledger_display_path(path).display(),
        ));
    }
    Ok(bytes.len() as u64)
}

fn validate_account_ledger_bootstrap_accounts(
    configured_accounts: &[String],
    require_existing: bool,
    enabled_accounts: &HashSet<String>,
) -> std::result::Result<HashSet<String>, String> {
    let mut validated = HashSet::new();
    for configured in configured_accounts {
        let account_id = configured.trim();
        if !require_existing {
            return Err(
                "account_ledger_bootstrap_accounts requires account_ledger_require_existing=true"
                    .to_string(),
            );
        }
        if account_id.is_empty() || account_id != configured {
            return Err(
                "account_ledger_bootstrap_accounts contains an empty or whitespace-padded account id"
                    .to_string(),
            );
        }
        if !validated.insert(account_id.to_string()) {
            return Err(format!(
                "duplicate one-time ledger bootstrap account `{account_id}`",
            ));
        }
        if !enabled_accounts.contains(account_id) {
            return Err(format!(
                "one-time ledger bootstrap account `{account_id}` has no enabled Polymarket strategy",
            ));
        }
    }
    Ok(validated)
}

fn elapsed_ns(origin: &std::time::Instant) -> u64 {
    origin.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

/// Process-local monotonic timestamp used by the cross-thread market lane.
///
/// `types::now_ns()` is wall time and may jump when NTP or the VM host adjusts
/// the clock.  Subtracting two wall timestamps made a clock correction look
/// like strategy-queue latency (live samples showed repeatable ~40 ms false
/// tails around future-instrument registration).  A single process origin
/// keeps the compact atomic `u64` representation required by latest-value
/// slots while making producer/consumer deltas strictly monotonic.
#[inline]
fn market_queue_monotonic_ns() -> u64 {
    struct MarketQueueClock {
        clock: crate::latency::Clock,
        origin: crate::latency::Instant,
    }
    static CLOCK: std::sync::OnceLock<MarketQueueClock> = std::sync::OnceLock::new();
    let clock = CLOCK.get_or_init(|| {
        let clock = crate::latency::Clock::new();
        let origin = clock.now();
        MarketQueueClock { clock, origin }
    });
    clock
        .clock
        .now()
        .duration_since(clock.origin)
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

fn lifecycle_delta_ms(stage_ns: u64, origin_ns: u64) -> f64 {
    if origin_ns == 0 {
        return -1.0;
    }
    (stage_ns as i128 - origin_ns as i128) as f64 / 1_000_000.0
}

const QUOTE_TRIGGER_SLOW_NS: u64 = 10_000_000;
const QUOTE_EXECUTOR_QUEUE_SLOW_NS: u64 = 5_000_000;
const LIFECYCLE_SLOW_LOG_INTERVAL_NS: u64 = 10_000_000_000;
static QUOTE_TRIGGER_SLOW_LOG_UNTIL_NS: AtomicU64 = AtomicU64::new(0);
static QUOTE_TRIGGER_SLOW_SUPPRESSED: AtomicU64 = AtomicU64::new(0);
static QUOTE_EXECUTOR_SLOW_LOG_UNTIL_NS: AtomicU64 = AtomicU64::new(0);
static QUOTE_EXECUTOR_SLOW_SUPPRESSED: AtomicU64 = AtomicU64::new(0);

fn claim_lifecycle_slow_log(
    silent_until_ns: &AtomicU64,
    suppressed: &AtomicU64,
    now: u64,
) -> Option<u64> {
    loop {
        let current = silent_until_ns.load(Ordering::Relaxed);
        if now < current {
            suppressed.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        match silent_until_ns.compare_exchange_weak(
            current,
            now.saturating_add(LIFECYCLE_SLOW_LOG_INTERVAL_NS),
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return Some(suppressed.swap(0, Ordering::Relaxed)),
            Err(observed) if now < observed => {
                suppressed.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            Err(_) => continue,
        }
    }
}

/// Attach the exact OrderBook event that caused `Strategy::on_quote` to run.
/// The strategy only receives the local timestamp, so this engine boundary is
/// the last place where both exchange and local clocks are still available.
fn stamp_quote_trigger(signals: &mut [Signal], trigger: &OrderBookSnapshot, log_lifecycle: bool) {
    let stage_ns = now_ns();
    let stamp = |order: &mut OrderRequest| {
        order.quote_trigger_exchange_timestamp_ns = trigger.exchange_timestamp_ns;
        order.quote_trigger_local_timestamp_ns = trigger.local_timestamp_ns;
        order.quote_trigger_source = QuoteTriggerSource::OrderBook(trigger.exchange);
        if log_lifecycle {
            let trigger_to_stage_ns = if order.quote_trigger_local_timestamp_ns == 0 {
                0
            } else {
                stage_ns.saturating_sub(order.quote_trigger_local_timestamp_ns)
            };
            hexagent_runtime::latency::record_ns(
                "strategy.trigger_to_quote_signal",
                trigger_to_stage_ns,
            );
            if trigger_to_stage_ns >= QUOTE_TRIGGER_SLOW_NS {
                let Some(suppressed) = claim_lifecycle_slow_log(
                    &QUOTE_TRIGGER_SLOW_LOG_UNTIL_NS,
                    &QUOTE_TRIGGER_SLOW_SUPPRESSED,
                    stage_ns,
                ) else {
                    return;
                };
                warn!(
                    "[order_lifecycle_slow] stage=quote_signal coid={} iid={} event={} symbol={} side={} trigger_source={} trigger_exchange_ns={} trigger_local_ns={} quote_emit_ns={} stage_ns={} trigger_exchange_to_stage_ms={:.3} trigger_local_to_stage_ms={:.3} quote_to_stage_ms={:.3} threshold_ms={:.3} suppressed_since_last={} log_interval_s={}",
                    order.client_order_id,
                    order.instance_id,
                    order.quote_event_id,
                    order.symbol,
                    order.side,
                    order.quote_trigger_source,
                    order.quote_trigger_exchange_timestamp_ns,
                    order.quote_trigger_local_timestamp_ns,
                    order.timestamp_ns,
                    stage_ns,
                    lifecycle_delta_ms(stage_ns, order.quote_trigger_exchange_timestamp_ns),
                    lifecycle_delta_ms(stage_ns, order.quote_trigger_local_timestamp_ns),
                    lifecycle_delta_ms(stage_ns, order.timestamp_ns),
                    QUOTE_TRIGGER_SLOW_NS as f64 / 1_000_000.0,
                    suppressed,
                    LIFECYCLE_SLOW_LOG_INTERVAL_NS / 1_000_000_000,
                );
            } else {
                debug!(
                    "[order_lifecycle] stage=quote_signal coid={} iid={} event={} symbol={} side={} trigger_source={} trigger_local_to_stage_ms={:.3} quote_to_stage_ms={:.3}",
                    order.client_order_id,
                    order.instance_id,
                    order.quote_event_id,
                    order.symbol,
                    order.side,
                    order.quote_trigger_source,
                    lifecycle_delta_ms(stage_ns, order.quote_trigger_local_timestamp_ns),
                    lifecycle_delta_ms(stage_ns, order.timestamp_ns),
                );
            }
        }
    };
    for signal in signals {
        match signal {
            Signal::NewOrder(order) => stamp(order),
            Signal::BatchNewOrders { orders, .. }
            | Signal::BatchUpdateOrders {
                place_orders: orders,
                ..
            }
            | Signal::ReplaceOrder {
                place_orders: orders,
                ..
            } => {
                for order in orders {
                    stamp(order);
                }
            }
            _ => {}
        }
    }
}

fn log_executor_receive(signal: &Signal) {
    let stage_ns = now_ns();
    let log_order = |order: &OrderRequest| {
        let quote_to_stage_ns = if order.timestamp_ns == 0 {
            0
        } else {
            stage_ns.saturating_sub(order.timestamp_ns)
        };
        hexagent_runtime::latency::record_ns(
            "strategy.quote_signal_to_executor",
            quote_to_stage_ns,
        );
        if quote_to_stage_ns >= QUOTE_EXECUTOR_QUEUE_SLOW_NS {
            let Some(suppressed) = claim_lifecycle_slow_log(
                &QUOTE_EXECUTOR_SLOW_LOG_UNTIL_NS,
                &QUOTE_EXECUTOR_SLOW_SUPPRESSED,
                stage_ns,
            ) else {
                return;
            };
            warn!(
                "[order_lifecycle_slow] stage=executor_receive coid={} iid={} event={} symbol={} side={} trigger_source={} trigger_exchange_ns={} trigger_local_ns={} quote_emit_ns={} stage_ns={} trigger_exchange_to_stage_ms={:.3} trigger_local_to_stage_ms={:.3} quote_to_stage_ms={:.3} threshold_ms={:.3} suppressed_since_last={} log_interval_s={}",
                order.client_order_id,
                order.instance_id,
                order.quote_event_id,
                order.symbol,
                order.side,
                order.quote_trigger_source,
                order.quote_trigger_exchange_timestamp_ns,
                order.quote_trigger_local_timestamp_ns,
                order.timestamp_ns,
                stage_ns,
                lifecycle_delta_ms(stage_ns, order.quote_trigger_exchange_timestamp_ns),
                lifecycle_delta_ms(stage_ns, order.quote_trigger_local_timestamp_ns),
                lifecycle_delta_ms(stage_ns, order.timestamp_ns),
                QUOTE_EXECUTOR_QUEUE_SLOW_NS as f64 / 1_000_000.0,
                suppressed,
                LIFECYCLE_SLOW_LOG_INTERVAL_NS / 1_000_000_000,
            );
        } else {
            debug!(
                "[order_lifecycle] stage=executor_receive coid={} iid={} event={} symbol={} side={} trigger_source={} trigger_local_to_stage_ms={:.3} quote_to_stage_ms={:.3}",
                order.client_order_id,
                order.instance_id,
                order.quote_event_id,
                order.symbol,
                order.side,
                order.quote_trigger_source,
                lifecycle_delta_ms(stage_ns, order.quote_trigger_local_timestamp_ns),
                lifecycle_delta_ms(stage_ns, order.timestamp_ns),
            );
        }
    };
    match signal {
        Signal::NewOrder(order) => log_order(order),
        Signal::BatchNewOrders { orders, .. }
        | Signal::BatchUpdateOrders {
            place_orders: orders,
            ..
        }
        | Signal::ReplaceOrder {
            place_orders: orders,
            ..
        } => {
            for order in orders {
                log_order(order);
            }
        }
        _ => {}
    }
}

fn register_polymarket_wallet_identity(
    wallet_accounts: &mut HashMap<String, String>,
    account_id: &str,
    maker_address: &str,
) -> std::result::Result<(), String> {
    let maker = maker_address.trim().to_ascii_lowercase();
    if maker.is_empty() {
        return Err(format!(
            "Polymarket account `{account_id}` resolved an empty maker/funder address"
        ));
    }
    if let Some(existing) = wallet_accounts.get(&maker) {
        if existing != account_id {
            return Err(format!(
                "Polymarket physical wallet `{maker_address}` is configured as both account_id `{existing}` and `{account_id}`; use one shared account_id"
            ));
        }
        return Ok(());
    }
    wallet_accounts.insert(maker, account_id.to_string());
    Ok(())
}

#[derive(Debug, Clone)]
struct QueuedMarketPayload {
    event: Arc<MarketEvent>,
    enqueued_ns: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SymbolId(u64, u64);

impl SymbolId {
    /// Allocation-free, ASCII-case-insensitive stable symbol identity. Feed
    /// schemas use ASCII venue symbols/token IDs; two independent accumulators
    /// make accidental cross-symbol aliasing vanishingly unlikely while the
    /// startup/dynamic registration path still checks duplicate ownership.
    #[inline]
    fn of(symbol: &str) -> Self {
        let mut fnv = 0xcbf29ce484222325u64;
        let mut mixed = 0x9e3779b97f4a7c15u64;
        for byte in symbol.bytes() {
            let byte = byte.to_ascii_lowercase() as u64;
            fnv ^= byte;
            fnv = fnv.wrapping_mul(0x100000001b3);
            mixed ^= byte.wrapping_add(0x9e3779b97f4a7c15);
            mixed = mixed.rotate_left(27).wrapping_mul(0x94d049bb133111eb);
        }
        Self(fnv, mixed)
    }

    /// Hash two ASCII identities without allocating a temporary joined
    /// string. The separator is mixed explicitly so (`ab`, `c`) cannot alias
    /// (`a`, `bc`). Used for source-scoped observations such as SpotPrice.
    #[inline]
    fn of_parts(first: &str, second: &str) -> Self {
        let mut fnv = 0xcbf29ce484222325u64;
        let mut mixed = 0x9e3779b97f4a7c15u64;
        for byte in first
            .bytes()
            .chain(std::iter::once(0xff))
            .chain(second.bytes())
        {
            let byte = byte.to_ascii_lowercase() as u64;
            fnv ^= byte;
            fnv = fnv.wrapping_mul(0x100000001b3);
            mixed ^= byte.wrapping_add(0x9e3779b97f4a7c15);
            mixed = mixed.rotate_left(27).wrapping_mul(0x94d049bb133111eb);
        }
        Self(fnv, mixed)
    }
}

type RouteMask = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct LatestMarketKey {
    kind: u8,
    exchange: Exchange,
    symbol: SymbolId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LatestMarketKeyHandle {
    id: u16,
    generation: u32,
}

/// Router-thread-owned fixed-capacity key registry. IDs are allocated from a
/// startup-preallocated free-list and returned only after every per-instance
/// slot for the retiring generation has been invalidated. Hash-map mutation is
/// confined to the single strategy-router writer; worker threads only consume
/// immutable `(id, generation)` markers through their bounded market lanes.
#[derive(Debug)]
struct LatestMarketKeyRegistry {
    key_to_handle: HashMap<LatestMarketKey, LatestMarketKeyHandle>,
    free_ids: Vec<u16>,
    generations: [u32; MAX_LATEST_MARKET_KEYS],
    capacity_fallbacks: u64,
}

impl Default for LatestMarketKeyRegistry {
    fn default() -> Self {
        let mut free_ids = Vec::with_capacity(MAX_LATEST_MARKET_KEYS);
        free_ids.extend((0..MAX_LATEST_MARKET_KEYS as u16).rev());
        Self {
            key_to_handle: HashMap::with_capacity(MAX_LATEST_MARKET_KEYS),
            free_ids,
            generations: [0; MAX_LATEST_MARKET_KEYS],
            capacity_fallbacks: 0,
        }
    }
}

impl LatestMarketKeyRegistry {
    fn get_or_register(&mut self, key: LatestMarketKey) -> Option<LatestMarketKeyHandle> {
        if let Some(handle) = self.key_to_handle.get(&key) {
            return Some(*handle);
        }
        let Some(id) = self.free_ids.pop() else {
            // Correctness-preserving overflow policy: route the event directly
            // through the bounded lane instead of panicking or aliasing an
            // active key. The periodic router metric reports this condition.
            self.capacity_fallbacks = self.capacity_fallbacks.saturating_add(1);
            return None;
        };
        let generation = self.generations[id as usize].wrapping_add(1).max(1);
        self.generations[id as usize] = generation;
        let handle = LatestMarketKeyHandle { id, generation };
        self.key_to_handle.insert(key, handle);
        Some(handle)
    }

    fn remove(&mut self, key: &LatestMarketKey) -> Option<LatestMarketKeyHandle> {
        self.key_to_handle.remove(key)
    }

    fn release(&mut self, handle: LatestMarketKeyHandle) {
        debug_assert!(!self.free_ids.contains(&handle.id));
        self.free_ids.push(handle.id);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.key_to_handle.len()
    }
}

#[derive(Debug, Clone)]
enum QueuedMarketEvent {
    Direct(QueuedMarketPayload),
    Latest { slot_id: u16, tag: u64 },
}

const MAX_LATEST_MARKET_KEYS: usize = 128;
const LATEST_EPOCH_SLOTS: usize = 4;
const LATEST_SLOT_COUNT: usize = MAX_LATEST_MARKET_KEYS * LATEST_EPOCH_SLOTS;
const LATEST_IDLE: u8 = 0;
const LATEST_WRITING: u8 = 1;
const LATEST_PENDING: u8 = 2;
const LATEST_CONSUMING: u8 = 3;

#[derive(Debug)]
struct LatestMarketSlot {
    state: std::sync::atomic::AtomicU8,
    /// `(key generation << 32) | barrier epoch`. Packing both identities into
    /// the pre-existing word keeps every slot cache-compact while making an
    /// old queued marker distinguishable after ID/physical-slot reuse.
    tag: AtomicU64,
    enqueued_ns: AtomicU64,
    event: std::sync::atomic::AtomicPtr<MarketEvent>,
}

impl Default for LatestMarketSlot {
    fn default() -> Self {
        Self {
            state: std::sync::atomic::AtomicU8::new(LATEST_IDLE),
            tag: AtomicU64::new(0),
            enqueued_ns: AtomicU64::new(0),
            event: std::sync::atomic::AtomicPtr::new(std::ptr::null_mut()),
        }
    }
}

enum LatestPublish {
    MarkerRequired,
    Replaced,
    Direct(Arc<MarketEvent>),
}

impl LatestMarketSlot {
    fn publish(
        &self,
        event: Arc<MarketEvent>,
        tag: u64,
        enqueued_ns: u64,
    ) -> LatestPublish {
        loop {
            let state = self.state.load(Ordering::Acquire);
            match state {
                LATEST_IDLE => {
                    if self
                        .state
                        .compare_exchange(
                            LATEST_IDLE,
                            LATEST_WRITING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    self.tag.store(tag, Ordering::Relaxed);
                    self.enqueued_ns.store(enqueued_ns, Ordering::Relaxed);
                    let raw = Arc::into_raw(event).cast_mut();
                    // The pointer publication is a production side effect; do
                    // not place it inside `debug_assert!`, which is removed in
                    // optimized builds.
                    let previous = self.event.swap(raw, Ordering::AcqRel);
                    debug_assert!(previous.is_null());
                    self.state.store(LATEST_PENDING, Ordering::Release);
                    return LatestPublish::MarkerRequired;
                }
                LATEST_PENDING => {
                    if self.tag.load(Ordering::Relaxed) != tag {
                        return LatestPublish::Direct(event);
                    }
                    if self
                        .state
                        .compare_exchange(
                            LATEST_PENDING,
                            LATEST_WRITING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    let raw = Arc::into_raw(event).cast_mut();
                    let previous = self.event.swap(raw, Ordering::AcqRel);
                    self.enqueued_ns.store(enqueued_ns, Ordering::Relaxed);
                    self.state.store(LATEST_PENDING, Ordering::Release);
                    if !previous.is_null() {
                        // SAFETY: the slot owned exactly one strong reference.
                        unsafe { drop(Arc::from_raw(previous)) };
                    }
                    return LatestPublish::Replaced;
                }
                // A producer never waits behind its consumer. Direct enqueue
                // preserves ordering and keeps the router's latency bounded.
                LATEST_WRITING | LATEST_CONSUMING => return LatestPublish::Direct(event),
                _ => unreachable!("invalid latest-market slot state"),
            }
        }
    }

    fn take(&self, expected_tag: u64) -> Option<QueuedMarketPayload> {
        loop {
            match self.state.compare_exchange(
                LATEST_PENDING,
                LATEST_CONSUMING,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(LATEST_WRITING) => std::hint::spin_loop(),
                Err(_) => return None,
            }
        }
        // Acquiring PENDING synchronizes with the producer's Release store, so
        // the immutable tag can be read Relaxed after the CAS.
        if self.tag.load(Ordering::Relaxed) != expected_tag {
            // The state made an IDLE→PENDING ABA transition while this stale
            // marker was waiting. Restore the new generation's pending state;
            // its own marker remains queued behind this one.
            self.state.store(LATEST_PENDING, Ordering::Release);
            return None;
        }
        let event = self.event.swap(std::ptr::null_mut(), Ordering::AcqRel);
        let enqueued_ns = self.enqueued_ns.load(Ordering::Relaxed);
        self.state.store(LATEST_IDLE, Ordering::Release);
        if event.is_null() {
            return None;
        }
        // SAFETY: publish transferred one Arc strong reference into the slot;
        // this consumer atomically removed that exact pointer.
        Some(QueuedMarketPayload {
            event: unsafe { Arc::from_raw(event) },
            enqueued_ns,
        })
    }

    fn discard_if_pending(&self, expected_tag: u64) {
        if self
            .state
            .compare_exchange(
                LATEST_PENDING,
                LATEST_CONSUMING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return;
        }
        if self.tag.load(Ordering::Relaxed) != expected_tag {
            self.state.store(LATEST_PENDING, Ordering::Release);
            return;
        }
        let event = self.event.swap(std::ptr::null_mut(), Ordering::AcqRel);
        self.state.store(LATEST_IDLE, Ordering::Release);
        if !event.is_null() {
            // SAFETY: same ownership transfer as `take`, discarded on failed
            // marker admission.
            unsafe { drop(Arc::from_raw(event)) };
        }
    }

    fn discard_generation_if_pending(&self, expected_generation: u32) {
        if self
            .state
            .compare_exchange(
                LATEST_PENDING,
                LATEST_CONSUMING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return;
        }
        if latest_slot_generation(self.tag.load(Ordering::Relaxed)) != expected_generation {
            self.state.store(LATEST_PENDING, Ordering::Release);
            return;
        }
        let event = self.event.swap(std::ptr::null_mut(), Ordering::AcqRel);
        self.state.store(LATEST_IDLE, Ordering::Release);
        if !event.is_null() {
            // SAFETY: same ownership transfer as `take`, discarded at an
            // ordered lifecycle retirement boundary.
            unsafe { drop(Arc::from_raw(event)) };
        }
    }
}

impl Drop for LatestMarketSlot {
    fn drop(&mut self) {
        let event = self.event.swap(std::ptr::null_mut(), Ordering::AcqRel);
        if !event.is_null() {
            // SAFETY: shutdown owns the slot exclusively.
            unsafe { drop(Arc::from_raw(event)) };
        }
    }
}

#[derive(Debug)]
struct LatestMarketStore {
    slots: Box<[LatestMarketSlot]>,
    replacements: AtomicU64,
}

impl Default for LatestMarketStore {
    fn default() -> Self {
        Self {
            slots: std::iter::repeat_with(LatestMarketSlot::default)
                .take(LATEST_SLOT_COUNT)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            replacements: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone)]
struct MarketEventLane {
    tx: Sender<QueuedMarketEvent>,
    latest: Arc<LatestMarketStore>,
    /// Non-coverable events advance the epoch so a later book cannot replace
    /// a marker queued before an intervening trade/lifecycle event.
    barrier_epoch: Arc<AtomicU64>,
}

fn latest_market_key(event: &MarketEvent) -> Option<LatestMarketKey> {
    match event {
        MarketEvent::OrderBook(book) => Some(LatestMarketKey {
            kind: 0,
            exchange: book.exchange,
            symbol: SymbolId::of(&book.symbol),
        }),
        MarketEvent::Quote(quote) => Some(LatestMarketKey {
            kind: 1,
            exchange: quote.exchange,
            symbol: SymbolId::of(&quote.symbol),
        }),
        MarketEvent::SpotPrice(spot) => Some(LatestMarketKey {
            kind: 2,
            // SpotPrice carries a source string rather than Exchange. Fold
            // source into the symbol identity so independent oracle/venue
            // observations never replace each other.
            exchange: Exchange::Chainlink,
            symbol: SymbolId::of_parts(&spot.source, &spot.symbol),
        }),
        MarketEvent::AssetCtx(asset) => Some(LatestMarketKey {
            kind: 3,
            exchange: asset.exchange,
            symbol: SymbolId::of(&asset.symbol),
        }),
        _ => None,
    }
}

#[inline]
fn latest_slot_tag(generation: u32, epoch: u64) -> u64 {
    // A queued marker is bounded by the lane capacity and the 5s worker-stall
    // watchdog, so it cannot survive the 2^32 barrier-epoch wrap needed to
    // alias this packed tag. A generation wraps only after 2^32 retirements of
    // the same ID.
    ((generation as u64) << 32) | epoch as u32 as u64
}

#[inline]
fn latest_slot_generation(tag: u64) -> u32 {
    (tag >> 32) as u32
}

fn enqueue_market_event(
    lane: &MarketEventLane,
    event: Arc<MarketEvent>,
    latest_key: Option<LatestMarketKeyHandle>,
) -> bool {
    let enqueued_ns = market_queue_monotonic_ns();
    let epoch = lane.barrier_epoch.load(Ordering::Relaxed);
    let Some(key) = latest_key else {
        lane.barrier_epoch.fetch_add(1, Ordering::Relaxed);
        return lane
            .tx
            .try_send(QueuedMarketEvent::Direct(QueuedMarketPayload {
                event,
                enqueued_ns,
            }))
            .is_ok();
    };
    let slot_id = key.id as usize * LATEST_EPOCH_SLOTS + (epoch as usize % LATEST_EPOCH_SLOTS);
    let tag = latest_slot_tag(key.generation, epoch);
    let Some(slot) = lane.latest.slots.get(slot_id) else {
        return lane
            .tx
            .try_send(QueuedMarketEvent::Direct(QueuedMarketPayload {
                event,
                enqueued_ns,
            }))
            .is_ok();
    };
    match slot.publish(event, tag, enqueued_ns) {
        LatestPublish::Replaced => {
            lane.latest.replacements.fetch_add(1, Ordering::Relaxed);
            true
        }
        LatestPublish::MarkerRequired => {
            if lane
                .tx
                .try_send(QueuedMarketEvent::Latest {
                    slot_id: slot_id as u16,
                    tag,
                })
                .is_ok()
            {
                true
            } else {
                slot.discard_if_pending(tag);
                false
            }
        }
        LatestPublish::Direct(event) => lane
            .tx
            .try_send(QueuedMarketEvent::Direct(QueuedMarketPayload {
                event,
                enqueued_ns,
            }))
            .is_ok(),
    }
}

fn resolve_market_event(
    queued: QueuedMarketEvent,
    latest: &LatestMarketStore,
) -> Option<QueuedMarketPayload> {
    match queued {
        QueuedMarketEvent::Direct(payload) => Some(payload),
        QueuedMarketEvent::Latest { slot_id, tag } => {
            latest.slots.get(slot_id as usize)?.take(tag)
        }
    }
}

fn retire_latest_market_key(
    handle: LatestMarketKeyHandle,
    market_lanes: &[MarketEventLane],
) {
    let slot_base = handle.id as usize * LATEST_EPOCH_SLOTS;
    for lane in market_lanes {
        for offset in 0..LATEST_EPOCH_SLOTS {
            if let Some(slot) = lane.latest.slots.get(slot_base + offset) {
                slot.discard_generation_if_pending(handle.generation);
            }
        }
    }
}

fn private_update_lane_enabled(private_burst: usize, market_waiting: bool) -> bool {
    private_burst < PRIVATE_UPDATE_BURST_BUDGET || !market_waiting
}

fn should_spawn_per_instance_strategy_workers(backtest: bool, strategy_count: usize) -> bool {
    !backtest && strategy_count > 0
}

#[derive(Debug)]
struct QueuedOrderUpdate {
    update: OrderUpdate,
    enqueued_at: std::time::Instant,
}

struct HistoricalLoadJob {
    epoch: u64,
    ts_event: u64,
    requests: Vec<HistDataRequest>,
}

struct HistoricalLoadResult {
    epoch: u64,
    ts_event: u64,
    loaded: Vec<(HistDataRequest, Arc<[BarData]>)>,
}

/// Numeric ownership crosses the strategy/execution boundary with the signal;
/// strategy workers never publish bare process-global commands.
#[derive(Debug)]
pub struct RoutedSignal {
    pub owner: u16,
    pub signal: Signal,
}

/// Execution feedback preserves the numeric owner captured at strategy
/// admission. Normal ACK/reject/completion routing therefore never consults a
/// shared coid map or reparses a string namespace. SYSTEM owner is reserved
/// for account-wide shutdown/recovery updates that still require cold routing.
#[derive(Debug)]
pub struct RoutedOrderUpdate {
    pub owner: u16,
    pub update: OrderUpdate,
}

#[derive(Clone)]
struct ExecutorUpdateSender {
    owner: u16,
    tx: Sender<RoutedOrderUpdate>,
}

const SYSTEM_SIGNAL_OWNER: u16 = u16::MAX;
const INSTANCE_SIGNAL_LANE_CAPACITY: usize = 1024;

#[derive(Clone)]
struct SignalSender {
    owner: u16,
    tx: Sender<RoutedSignal>,
    owner_lanes: Option<Arc<Vec<Sender<RoutedSignal>>>>,
    arbiter_control_tx: Option<Sender<RoutedSignal>>,
}

impl SignalSender {
    fn system(tx: Sender<RoutedSignal>) -> Self {
        Self {
            owner: SYSTEM_SIGNAL_OWNER,
            tx,
            owner_lanes: None,
            arbiter_control_tx: None,
        }
    }

    fn system_with_owner_lanes(
        tx: Sender<RoutedSignal>,
        owner_lanes: Vec<Sender<RoutedSignal>>,
        arbiter_control_tx: Sender<RoutedSignal>,
    ) -> Self {
        Self {
            owner: SYSTEM_SIGNAL_OWNER,
            tx,
            owner_lanes: Some(Arc::new(owner_lanes)),
            arbiter_control_tx: Some(arbiter_control_tx),
        }
    }

    fn with_owner(&self, owner: usize) -> Self {
        let tx = self
            .owner_lanes
            .as_ref()
            .and_then(|lanes| lanes.get(owner))
            .cloned()
            .unwrap_or_else(|| self.tx.clone());
        Self {
            owner: u16::try_from(owner).expect("strategy owner index exceeds u16"),
            tx,
            owner_lanes: None,
            arbiter_control_tx: self.arbiter_control_tx.clone(),
        }
    }

    fn try_send(
        &self,
        signal: Signal,
    ) -> Result<(), crossbeam_channel::TrySendError<RoutedSignal>> {
        let barrier = self.owner == SYSTEM_SIGNAL_OWNER
            && matches!(signal, Signal::BeginShutdown | Signal::Exit);
        let tx = if barrier {
            self.arbiter_control_tx.as_ref().unwrap_or(&self.tx)
        } else {
            &self.tx
        };
        tx.try_send(RoutedSignal {
            owner: self.owner,
            signal,
        })
    }

    /// Exceptional fail-closed path. The arbiter drains already-admitted
    /// owner commands first, then forwards this cancel through its independent
    /// control lane. Calling strategy threads never wait for capacity.
    fn try_send_emergency(
        &self,
        signal: Signal,
    ) -> Result<(), crossbeam_channel::TrySendError<RoutedSignal>> {
        self.arbiter_control_tx
            .as_ref()
            .unwrap_or(&self.tx)
            .try_send(RoutedSignal {
                owner: self.owner,
                signal,
            })
    }

    fn send(&self, signal: Signal) -> Result<(), crossbeam_channel::SendError<RoutedSignal>> {
        let barrier = self.owner == SYSTEM_SIGNAL_OWNER
            && matches!(signal, Signal::BeginShutdown | Signal::Exit);
        let tx = if barrier {
            self.arbiter_control_tx.as_ref().unwrap_or(&self.tx)
        } else {
            &self.tx
        };
        tx.send(RoutedSignal {
            owner: self.owner,
            signal,
        })
    }
}

/// Merge bounded per-instance strategy lanes into the executor ingress. Every
/// lane is lossless and ordered. Quote signals are intentionally not coalesced
/// yet: replacing a signal that already owns a reservation needs an explicit
/// rollback envelope before it can be safe.
fn spawn_strategy_signal_arbiter(
    root_tx: Sender<RoutedSignal>,
    owner_rxs: Vec<Receiver<RoutedSignal>>,
    control_rx: Receiver<RoutedSignal>,
) -> Option<thread::JoinHandle<()>> {
    if owner_rxs.is_empty() {
        return None;
    }
    Some(
        thread::Builder::new()
            .name("strategy-signal-arbiter".into())
            .spawn(move || {
                crate::os_tune::pin_execution("strategy-signal-arbiter");
                // The system sender owns every producer for the arbiter's
                // lifetime, so channels cannot disappear before Exit. Build
                // the selector once at startup: no per-signal registration or
                // temporary Vec allocation on this critical lane.
                let mut select = crossbeam_channel::Select::new();
                for rx in &owner_rxs {
                    select.recv(rx);
                }
                let control_index = select.recv(&control_rx);
                loop {
                    let selected = select.select();
                    if selected.index() == control_index {
                        let Ok(barrier) = selected.recv(&control_rx) else {
                            break;
                        };
                        // Strategy workers stop before shutdown barriers are
                        // emitted, so drain all owner lanes first.
                        for rx in &owner_rxs {
                            while let Ok(routed) = rx.try_recv() {
                                if root_tx.send(routed).is_err() {
                                    return;
                                }
                            }
                        }
                        let terminal = matches!(barrier.signal, Signal::Exit);
                        if root_tx.send(barrier).is_err() || terminal {
                            break;
                        }
                        continue;
                    }
                    let owner = selected.index();
                    match selected.recv(&owner_rxs[owner]) {
                        Ok(routed) => {
                            if root_tx.send(routed).is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            })
            .expect("spawn strategy signal arbiter"),
    )
}

fn quarantine_strategy_worker(
    idx: usize,
    reason: &str,
    instance_ids: &[String],
    quarantined: &[Arc<AtomicBool>],
    signal_tx: &SignalSender,
) -> bool {
    let Some(flag) = quarantined.get(idx) else {
        return false;
    };
    if flag.swap(true, Ordering::AcqRel) {
        return false;
    }
    let instance_id = instance_ids.get(idx).cloned().unwrap_or_default();
    error!(
        "[strategy_supervisor] instance={} quarantined=1 reason={}",
        instance_id, reason
    );
    enqueue_emergency_instance_cancel(idx, &instance_id, reason, signal_tx);
    true
}

fn enqueue_emergency_instance_cancel(
    idx: usize,
    instance_id: &str,
    reason: &str,
    signal_tx: &SignalSender,
) -> bool {
    let cancel = Signal::PolymarketCancelAllOrders {
        reason: format!("strategy instance `{instance_id}` quarantined: {reason}"),
        market: None,
        asset_ids: Vec::new(),
        instance_id: instance_id.to_string(),
    };
    if let Err(error) = signal_tx.with_owner(idx).try_send_emergency(cancel) {
        error!(
            "[strategy_supervisor] failed to enqueue emergency instance cancel for worker index={idx}: {error}"
        );
        return false;
    }
    true
}

fn emergency_cancel_for_signal(signal: &Signal, instance_id: &str, reason: &str) -> Signal {
    let exchange = match signal {
        Signal::NewOrder(order) => Some(order.exchange),
        Signal::CancelOrder { exchange, .. }
        | Signal::CancelAll { exchange, .. }
        | Signal::BatchNewOrders { exchange, .. }
        | Signal::BatchCancelOrders { exchange, .. }
        | Signal::BatchUpdateOrders { exchange, .. }
        | Signal::ReplaceOrder { exchange, .. } => Some(*exchange),
        Signal::ReconcilePolymarket { .. }
        | Signal::PolymarketCancelAllOrders { .. }
        | Signal::RetainPolymarketEventAudit { .. }
        | Signal::RetirePolymarketEventAudit { .. } => Some(Exchange::Polymarket),
        Signal::BeginShutdown | Signal::Exit => None,
    };
    if exchange == Some(Exchange::Polymarket) {
        Signal::PolymarketCancelAllOrders {
            reason: format!("instance `{instance_id}` signal lane failed closed: {reason}"),
            market: None,
            asset_ids: Vec::new(),
            instance_id: instance_id.to_string(),
        }
    } else {
        Signal::CancelAll {
            exchange: exchange.unwrap_or(Exchange::Polymarket),
            symbol: String::new(),
            instance_id: instance_id.to_string(),
            timestamp_ns: crate::types::now_ns(),
        }
    }
}

fn emit_strategy_signal(
    signal: Signal,
    signal_tx: &SignalSender,
    quarantined: &AtomicBool,
    instance_id: &str,
) -> bool {
    if quarantined.load(Ordering::Acquire) {
        return false;
    }
    match signal_tx.try_send(signal) {
        Ok(()) => true,
        Err(crossbeam_channel::TrySendError::Full(routed)) => {
            quarantined.store(true, Ordering::Release);
            error!(
                "[strategy_worker] instance={} signal lane overflow; quarantining fail-closed",
                instance_id,
            );
            let emergency = emergency_cancel_for_signal(
                &routed.signal,
                instance_id,
                "bounded owner lane overflow",
            );
            if let Err(error) = signal_tx.try_send_emergency(emergency) {
                error!(
                    "[strategy_worker] instance={} emergency control lane unavailable: {}",
                    instance_id, error,
                );
            }
            false
        }
        Err(crossbeam_channel::TrySendError::Disconnected(routed)) => {
            quarantined.store(true, Ordering::Release);
            error!(
                "[strategy_worker] instance={} signal lane disconnected while emitting {:?}",
                instance_id, routed.signal,
            );
            false
        }
    }
}

/// Exact identity of one historical-bars input snapshot.
///
/// This deliberately contains only raw-data coordinates. Two strategies with
/// different model parameters may safely reuse the same immutable bars while
/// still building their own independent model state from them.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct HistBarsKey {
    exchange: Exchange,
    symbol: String,
    interval: String,
    start_date_ns: u64,
    end_date_ns: u64,
}

impl From<&HistDataRequest> for HistBarsKey {
    fn from(req: &HistDataRequest) -> Self {
        Self {
            exchange: req.exchange,
            symbol: req.symbol.clone(),
            interval: req.interval.clone(),
            start_date_ns: req.start_date_ns,
            end_date_ns: req.end_date_ns,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct HistPreloadStats {
    requests: usize,
    unique_loads: usize,
    cache_hits: usize,
    failed_loads: usize,
    initialized_strategies: usize,
}

/// Preload historical bars for every strategy against one fixed end timestamp.
///
/// Requests with the same raw-data identity are loaded once and the resulting
/// immutable slice is replayed into every consumer. Model state is intentionally
/// *not* shared here: each strategy still receives its own `on_hist_bar` /
/// `on_hist_data_loaded` callbacks, preserving isolation across model configs.
fn preload_hist_bars_with<F>(
    strategies: &mut [Box<dyn Strategy>],
    hist_end_ns: u64,
    mut loader: F,
) -> HistPreloadStats
where
    F: FnMut(&HistDataRequest) -> Option<Vec<BarData>>,
{
    // Ask every strategy with the same anchor before doing any I/O. Preserve
    // each strategy's requested lookback duration, but force the exact common
    // end so sequential instance startup cannot shift otherwise-identical
    // windows by milliseconds or seconds.
    let request_sets: Vec<Vec<HistDataRequest>> = strategies
        .iter()
        .map(|strategy| {
            let mut requests = strategy.load_hist_data(hist_end_ns);
            for req in &mut requests {
                let lookback_ns = req.end_date_ns.saturating_sub(req.start_date_ns);
                req.end_date_ns = hist_end_ns;
                req.start_date_ns = hist_end_ns.saturating_sub(lookback_ns);
            }
            requests
        })
        .collect();

    let mut stats = HistPreloadStats::default();
    let mut cache: HashMap<HistBarsKey, Option<Arc<[BarData]>>> = HashMap::new();

    for (strategy, requests) in strategies.iter_mut().zip(request_sets) {
        if requests.is_empty() {
            continue;
        }
        stats.initialized_strategies += 1;

        for req in &requests {
            stats.requests += 1;
            let key = HistBarsKey::from(req);
            let bars = match cache.entry(key) {
                std::collections::hash_map::Entry::Occupied(entry) => {
                    stats.cache_hits += 1;
                    entry.get().clone()
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    stats.unique_loads += 1;
                    let loaded = loader(req)
                        .filter(|bars| !bars.is_empty())
                        .map(|bars| Arc::<[BarData]>::from(bars.into_boxed_slice()));
                    if loaded.is_none() {
                        stats.failed_loads += 1;
                    }
                    entry.insert(loaded.clone());
                    loaded
                }
            };

            if let Some(bars) = bars {
                for bar in bars.iter() {
                    strategy.on_hist_bar(bar);
                }
            }
        }

        // Preserve the existing lifecycle: a strategy that requested history
        // is notified once even when the load was empty/failed, so its
        // freshness gate can remain blocked and retry later.
        strategy.on_hist_data_loaded(hist_end_ns);
    }

    stats
}

fn preload_hist_bars(
    strategies: &mut [Box<dyn Strategy>],
    data_dirs: &[PathBuf],
    hist_end_ns: u64,
) -> HistPreloadStats {
    preload_hist_bars_with(strategies, hist_end_ns, |req| {
        for dir in data_dirs {
            match crate::recorder::load_hist_bars(dir, req) {
                Ok(bars) if !bars.is_empty() => {
                    info!(
                        "[Strategy] Loaded {} hist bars once for {}/{} {} [{}..{}) ({})",
                        bars.len(),
                        req.exchange,
                        req.symbol,
                        req.interval,
                        req.start_date_ns,
                        req.end_date_ns,
                        dir.display(),
                    );
                    return Some(bars);
                }
                _ => {}
            }
        }
        warn!(
            "[Strategy] Failed to preload hist bars for {}/{} {} [{}..{})",
            req.exchange, req.symbol, req.interval, req.start_date_ns, req.end_date_ns,
        );
        None
    })
}

/// Current readiness of one configured public market-data feed.
///
/// The state is stored independently from strategy callbacks so operators and
/// embedding applications can distinguish "process is alive" from "all feeds
/// required for quoting are usable".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedReadiness {
    Starting,
    NotReady { stage: String, reason: String },
    Ready,
}

fn set_feed_readiness(
    states: &Arc<RwLock<HashMap<String, FeedReadiness>>>,
    feed: &str,
    state: FeedReadiness,
) {
    states
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(feed.to_string(), state);
}

/// Polymarket readiness is lifecycle-driven by the async CLOB task:
/// `Connected` is emitted only after the first subscription or a valid
/// post-reconnect book, while `Disconnected` is emitted at the transport
/// failure boundary.
fn polymarket_readiness_transition(event: &MarketEvent) -> Option<FeedReadiness> {
    match event {
        MarketEvent::Connected {
            exchange: Exchange::Polymarket,
        } => Some(FeedReadiness::Ready),
        MarketEvent::Disconnected {
            exchange: Exchange::Polymarket,
            reason,
        } => Some(FeedReadiness::NotReady {
            stage: "data_stream".to_string(),
            reason: reason.clone(),
        }),
        _ => None,
    }
}

/// Reasons emitted by the normal five-minute market lifecycle. Keep these
/// exact-match checks narrow: unexpected account-wide cancels and real CLOB
/// disconnects must remain warnings.
fn is_routine_expiry_cancel(reason: &str, market_scoped: bool) -> bool {
    market_scoped && reason == "event_expiry_sweep"
}

fn is_routine_clob_resubscribe(reason: &str) -> bool {
    reason == "CLOB resubscribe requested"
}

fn forward_recorder_shared(
    recorder_tx: Option<&Sender<Arc<MarketEvent>>>,
    event: Arc<MarketEvent>,
) {
    if let Some(tx) = recorder_tx {
        if matches!(event.as_ref(), MarketEvent::Exit) {
            let _ = tx.send(event);
            return;
        }
        let started = std::time::Instant::now();
        match tx.try_send(event) {
            Ok(()) => crate::latency::record_ns(
                "market.recorder.enqueue",
                started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            ),
            Err(crossbeam_channel::TrySendError::Full(_)) => {
                static DROPPED: AtomicU64 = AtomicU64::new(0);
                static LAST_WARN_NS: AtomicU64 = AtomicU64::new(0);
                let dropped = DROPPED.fetch_add(1, Ordering::Relaxed) + 1;
                let now = now_ns();
                let last = LAST_WARN_NS.load(Ordering::Relaxed);
                if now.saturating_sub(last) >= 10_000_000_000
                    && LAST_WARN_NS
                        .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                        .is_ok()
                {
                    warn!(
                        "[Recorder] live queue saturated; dropped_events_total={} action=preserve_strategy_latency",
                        dropped
                    );
                }
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => {}
        }
    }
}

fn forward_recorder_event(recorder_tx: Option<&Sender<Arc<MarketEvent>>>, event: &MarketEvent) {
    forward_recorder_shared(recorder_tx, Arc::new(event.clone()));
}

/// Sleep for a retry delay while keeping shutdown latency bounded. Subscription
/// recovery can remain in its capped backoff state indefinitely, so a plain
/// `thread::sleep` could otherwise delay process shutdown by tens of seconds.
fn sleep_with_shutdown(shutdown: &AtomicBool, delay: std::time::Duration) -> bool {
    let deadline = std::time::Instant::now() + delay;
    while !shutdown.load(Ordering::Relaxed) {
        let now = std::time::Instant::now();
        if now >= deadline {
            return true;
        }
        thread::sleep(
            deadline
                .saturating_duration_since(now)
                .min(std::time::Duration::from_millis(100)),
        );
    }
    false
}

fn is_public_subscription_exchange(name: &str) -> bool {
    matches!(
        name,
        "binance"
            | "binance_futures"
            | "coinbase"
            | "chainlink"
            | "bybit"
            | "kraken"
            | "okx"
            | "gate"
            | "bitget"
            | "kucoin"
            | "mexc"
            | "pyth"
    )
}

fn same_public_feed_connection(a: &ExchangeConfig, b: &ExchangeConfig) -> bool {
    a.name == b.name
        && a.enabled == b.enabled
        && a.source == b.source
        && a.api_url_prefix == b.api_url_prefix
        && a.wss_url == b.wss_url
        && a.api_key == b.api_key
        && a.api_secret == b.api_secret
        && a.api_passphrase == b.api_passphrase
        && a.network == b.network
        && a.feed_ids == b.feed_ids
}

/// Treat `feed_ids` as the single Chainlink Data Streams configuration source
/// in live-like modes. Backtests keep human-readable archive labels in
/// `symbols`, and legacy stream configs without a feed map remain supported.
fn normalize_chainlink_stream_subscriptions(mode: RunMode, exchanges: &mut [ExchangeConfig]) {
    if mode == RunMode::Backtest {
        return;
    }

    for exchange in exchanges {
        if exchange.name != "chainlink"
            || !exchange.source.eq_ignore_ascii_case("stream")
            || exchange.feed_ids.is_empty()
        {
            continue;
        }

        let (subscriptions, invalid_labels) = exchange.chainlink_stream_subscriptions();
        exchange.symbols = subscriptions;
        if exchange.enabled {
            for label in invalid_labels {
                warn!(
                    "[chainlink] invalid or missing feed_ids entry for {}; feed will not be subscribed",
                    label,
                );
            }
        }
    }
}

/// Collapse duplicate public-feed config blocks onto one connection and one
/// unique symbol set. Strategy routing remains one-to-many, so every sibling
/// instance still receives the shared event without opening another socket.
fn coalesce_public_exchange_subscriptions(exchanges: &mut Vec<ExchangeConfig>) {
    let mut merged: Vec<ExchangeConfig> = Vec::with_capacity(exchanges.len());
    for mut candidate in exchanges.drain(..) {
        if is_public_subscription_exchange(&candidate.name) {
            if let Some(existing) = merged
                .iter_mut()
                .find(|existing| same_public_feed_connection(existing, &candidate))
            {
                let before = existing.symbols.len();
                for symbol in candidate.symbols.drain(..) {
                    if !existing
                        .symbols
                        .iter()
                        .any(|current| current.eq_ignore_ascii_case(&symbol))
                    {
                        existing.symbols.push(symbol);
                    }
                }
                log::info!(
                    "[Engine] Coalesced duplicate {} feed block: {} unique symbol(s) (added {})",
                    existing.name,
                    existing.symbols.len(),
                    existing.symbols.len().saturating_sub(before),
                );
                continue;
            }
        }
        let mut seen = std::collections::HashSet::new();
        candidate
            .symbols
            .retain(|symbol| seen.insert(symbol.to_ascii_lowercase()));
        merged.push(candidate);
    }
    *exchanges = merged;
}

#[cfg(test)]
mod public_subscription_coalesce_tests {
    use super::*;

    fn exchange(toml: &str) -> ExchangeConfig {
        toml::from_str(toml).expect("exchange config")
    }

    #[test]
    fn required_account_ledger_fails_closed_when_history_is_absent() {
        let missing = std::env::temp_dir().join(format!(
            "hexagent-required-ledger-missing-{}-{}.json",
            std::process::id(),
            crate::types::now_ns(),
        ));
        assert_eq!(
            validate_required_account_ledger(Some(&missing), false, false).unwrap(),
            RequiredAccountLedgerStatus::NotRequired,
        );
        let error = validate_required_account_ledger(Some(&missing), true, false).unwrap_err();
        assert!(error.contains("required durable account ledger is missing"));
        assert!(validate_required_account_ledger(None, true, false).is_err());
    }

    #[test]
    fn required_account_ledger_bootstrap_is_missing_only_and_one_time() {
        let path = std::env::temp_dir().join(format!(
            "hexagent-required-ledger-bootstrap-{}-{}.json",
            std::process::id(),
            crate::types::now_ns(),
        ));
        assert_eq!(
            validate_required_account_ledger(Some(&path), true, true).unwrap(),
            RequiredAccountLedgerStatus::BootstrapMissing,
        );

        std::fs::write(&path, b"durable-ledger-placeholder").unwrap();
        assert_eq!(
            validate_required_account_ledger(Some(&path), true, false).unwrap(),
            RequiredAccountLedgerStatus::Existing,
        );
        let stale = validate_required_account_ledger(Some(&path), true, true).unwrap_err();
        assert!(stale.contains("remove the stale bootstrap entry"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn bootstrapped_account_ledger_is_fsynced_and_identity_verified() {
        let path = std::env::temp_dir().join(format!(
            "hexagent-bootstrap-ledger-fsync-{}-{}.json",
            std::process::id(),
            crate::types::now_ns(),
        ));
        {
            let account =
                crate::account::shared_account::SharedAccount::new_persistent("new001", &path)
                    .unwrap();
            account.register_instance("btc-new", 1.0);
            account.register_instance("eth-new", 1.0);
            account.register_market_scope("btc-new", "btc-up-or-down-5m");
            account
                .flush_persistence(std::time::Duration::from_secs(5))
                .unwrap();
            assert!(verify_bootstrapped_account_ledger(&path, "new001").unwrap() > 0);
        }

        let stale = validate_required_account_ledger(Some(&path), true, true).unwrap_err();
        assert!(stale.contains("remove the stale bootstrap entry"));
        std::fs::remove_file(&path).unwrap();
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        std::fs::remove_file(PathBuf::from(lock_path)).unwrap();
    }

    #[test]
    fn account_ledger_bootstrap_allowlist_rejects_unsafe_configuration() {
        let enabled = HashSet::from(["new001".to_string()]);
        assert!(
            validate_account_ledger_bootstrap_accounts(&[], true, &enabled)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            validate_account_ledger_bootstrap_accounts(&["new001".to_string()], true, &enabled,)
                .unwrap(),
            enabled.clone(),
        );
        assert!(validate_account_ledger_bootstrap_accounts(
            &["new001".to_string()],
            false,
            &enabled,
        )
        .is_err());
        assert!(validate_account_ledger_bootstrap_accounts(
            &["new001".to_string(), "new001".to_string()],
            true,
            &enabled,
        )
        .is_err());
        assert!(validate_account_ledger_bootstrap_accounts(
            &["unknown".to_string()],
            true,
            &enabled,
        )
        .is_err());
        assert!(validate_account_ledger_bootstrap_accounts(
            &[" new001".to_string()],
            true,
            &enabled,
        )
        .is_err());
    }

    #[test]
    fn identical_public_blocks_share_one_case_insensitive_symbol_set() {
        let mut exchanges = vec![
            exchange("name = 'binance'\nsymbols = ['BTCUSDT', 'ETHUSDT']"),
            exchange("name = 'binance'\nsymbols = ['btcusdt', 'SOLUSDT']"),
        ];
        coalesce_public_exchange_subscriptions(&mut exchanges);
        assert_eq!(exchanges.len(), 1);
        assert_eq!(exchanges[0].symbols, vec!["BTCUSDT", "ETHUSDT", "SOLUSDT"]);
    }

    #[test]
    fn different_connection_settings_remain_separate() {
        let mut exchanges = vec![
            exchange("name = 'chainlink'\nsource = 'rtds'\nsymbols = ['btc/usd']"),
            exchange("name = 'chainlink'\nsource = 'stream'\nsymbols = ['btc/usd']"),
        ];
        coalesce_public_exchange_subscriptions(&mut exchanges);
        assert_eq!(exchanges.len(), 2);
    }

    #[test]
    fn chainlink_stream_uses_sorted_feed_map_subscriptions() {
        let btc = format!("0x{:064x}", 1);
        let twap = format!("0x{:064x}", 2);
        let mut exchanges = vec![exchange(&format!(
            "name = 'chainlink'\nsource = 'stream'\nsymbols = ['stale']\n\
             feed_ids = {{ 'btc/usd/twap/60s' = '{twap}', 'btc/usd' = '{btc}', 'eth/usd' = '' }}"
        ))];

        normalize_chainlink_stream_subscriptions(RunMode::Live, &mut exchanges);

        assert_eq!(
            exchanges[0].symbols,
            vec![format!("{btc}:btc/usd"), format!("{twap}:btc/usd/twap/60s")]
        );
    }

    #[test]
    fn chainlink_backtest_keeps_archive_labels() {
        let btc = format!("0x{:064x}", 1);
        let mut exchanges = vec![exchange(&format!(
            "name = 'chainlink'\nsource = 'stream'\nsymbols = ['btc/usd/twap/60s']\n\
             feed_ids = {{ 'btc/usd' = '{btc}' }}"
        ))];

        normalize_chainlink_stream_subscriptions(RunMode::Backtest, &mut exchanges);

        assert_eq!(exchanges[0].symbols, vec!["btc/usd/twap/60s"]);
    }
}

pub struct Engine {
    config: Config,
    /// Strategy factories the application registered (the engine never names a
    /// concrete strategy type — see `build_strategies`).
    registry: StrategyRegistry,
    feed_readiness: Arc<RwLock<HashMap<String, FeedReadiness>>>,
}

impl Engine {
    pub fn new(mut config: Config, registry: StrategyRegistry) -> Self {
        crate::account::order_manager::init_global_order_id();
        // Each registered strategy injects its own required market-data symbols
        // (replaces the engine's old per-strategy-name inject_*_symbols).
        registry.inject_all_config(&mut config);
        normalize_chainlink_stream_subscriptions(config.general.mode, &mut config.exchanges);
        coalesce_public_exchange_subscriptions(&mut config.exchanges);
        let feed_readiness = config
            .exchanges
            .iter()
            .filter(|cfg| cfg.enabled)
            .map(|cfg| (cfg.name.clone(), FeedReadiness::Starting))
            .collect();
        Self {
            config,
            registry,
            feed_readiness: Arc::new(RwLock::new(feed_readiness)),
        }
    }

    /// Snapshot the readiness of every enabled public market-data feed.
    pub fn feed_readiness(&self) -> HashMap<String, FeedReadiness> {
        self.feed_readiness
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Process-level readiness suitable for a health/readiness endpoint.
    /// Liveness remains separate: a running process with one `NotReady` feed
    /// returns false here and must not be considered ready to quote.
    pub fn feeds_ready(&self) -> bool {
        self.feed_readiness
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .all(|state| matches!(state, FeedReadiness::Ready))
    }

    /// Backtest start timestamp (ns since epoch); 0 outside backtest mode.
    fn parse_backtest_start_ns(&self) -> u64 {
        if self.config.general.mode != RunMode::Backtest {
            return 0;
        }
        chrono::DateTime::parse_from_rfc3339(&self.config.backtest.start_date)
            .or_else(|_| {
                chrono::NaiveDateTime::parse_from_str(
                    &self.config.backtest.start_date,
                    "%Y-%m-%dT%H:%M:%SZ",
                )
                .map(|ndt| ndt.and_utc().fixed_offset())
            })
            .map(|dt| {
                dt.with_timezone(&chrono::Utc)
                    .timestamp_nanos_opt()
                    .unwrap_or(0) as u64
            })
            .unwrap_or(0)
    }

    // ── Mode Execution (called from main.rs) ───────────────────────────

    pub fn run(&self) -> Result<()> {
        self.run_with_shutdown(ShutdownToken::new())
    }

    /// Run with a process-run shutdown token shared by engine-owned and
    /// application-owned background workers (for example latency dumping).
    pub fn run_with_shutdown(&self, shutdown_token: ShutdownToken) -> Result<()> {
        let memory_telemetry_handle = spawn_memory_telemetry(shutdown_token.clone())?;
        let result = match self.config.general.mode {
            RunMode::Live => self.run_live(shutdown_token.clone()),
            RunMode::Record => self.run_record(),
            RunMode::Backtest => self.run_backtest(),
            RunMode::Paper => self.run_paper(),
        };

        // Also cover startup/pre-flight errors and non-live modes. The memory
        // worker shares the same process-run token and must never outlive run().
        shutdown_token.request();
        shutdown_token.finish();
        if memory_telemetry_handle.join().is_err() && result.is_ok() {
            return Err(anyhow::anyhow!("memory telemetry worker panicked"));
        }
        result
    }

    /// Spawn a market data recorder thread. Returns (sender, join handle).
    fn spawn_recorder_thread(&self) -> Result<(Sender<Arc<MarketEvent>>, thread::JoinHandle<()>)> {
        // Live uses the same unfiltered recorder semantics as Record mode:
        // external spot/index and Polymarket events share one output root.
        self.spawn_recorder_thread_to(&self.config.recording.output_dir)
    }

    fn spawn_recorder_thread_to(
        &self,
        dir: &str,
    ) -> Result<(Sender<Arc<MarketEvent>>, thread::JoinHandle<()>)> {
        let output_dir = std::fs::canonicalize(dir)
            .unwrap_or_else(|_| {
                let p = PathBuf::from(dir);
                let _ = std::fs::create_dir_all(&p);
                std::fs::canonicalize(&p).unwrap_or(p)
            })
            .to_string_lossy()
            .to_string();
        let (recorder_tx, recorder_rx) = bounded::<Arc<MarketEvent>>(CHANNEL_CAPACITY);
        let handle = thread::Builder::new()
            .name("recorder".into())
            .spawn(move || {
                crate::os_tune::pin_background("recorder");
                let mut recorder = match MarketRecorder::new(PathBuf::from(&output_dir)) {
                    Ok(r) => r,
                    Err(e) => {
                        error!("[Recorder] Failed to create: {}", e);
                        return;
                    }
                };
                let mut last_flush = std::time::Instant::now();
                let flush_interval = std::time::Duration::from_secs(60);
                // Every 5 minutes, close the current partial fixed-capacity
                // row group as an immutable shard. The Arrow batch is dropped
                // immediately; completed data is never retained or rewritten.
                // Alignment keeps shard boundaries predictable across restarts.
                const CHECKPOINT_INTERVAL_SECS: u64 = 300;
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let mut next_checkpoint_unix_secs =
                    ((now_secs / CHECKPOINT_INTERVAL_SECS) + 1) * CHECKPOINT_INTERVAL_SECS;
                loop {
                    match recorder_rx.recv_timeout(std::time::Duration::from_secs(5)) {
                        Ok(event) => {
                            if matches!(event.as_ref(), MarketEvent::Exit) {
                                break;
                            }
                            if let Err(e) = recorder.write_event(event.as_ref()) {
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
                        next_checkpoint_unix_secs =
                            ((cur / CHECKPOINT_INTERVAL_SECS) + 1) * CHECKPOINT_INTERVAL_SECS;
                    }
                }
                info!("[Recorder] Flushing {} events...", recorder.event_count());
                if let Err(e) = recorder.flush() {
                    error!("[Recorder] Flush error: {}", e);
                }
                info!(
                    "[Recorder] Finished: {} events written",
                    recorder.event_count()
                );
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
            info!(
                "[Engine] {} data-freshness pre-flight DISABLED (live_max_data_gap_secs <= 0)",
                mode
            );
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
        let dirs_label = data_dirs
            .iter()
            .map(|d| d.display().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let mut worst: Option<(String, String, f64)> = None;
        for (exchange, symbol) in &sources {
            let mut effective: Option<u64> = None; // first in-window dir → warm-up uses it
            let mut freshest: Option<u64> = None; // newest across all dirs (diagnostic / fallback)
            for dir in &data_dirs {
                if let Some(latest) = crate::recorder::latest_recorded_ts_ns(dir, exchange, symbol)
                {
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
                        exchange,
                        symbol,
                        fmt_ts(latest_ns),
                        gap / 3600.0
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
            if worst
                .as_ref()
                .map(|(_, _, g)| gap_secs > *g)
                .unwrap_or(true)
            {
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
                    mode,
                    ex,
                    sym,
                    gap_label,
                    max_gap / 3600.0,
                ));
            }
        }
        info!(
            "[Engine] {} data-freshness pre-flight OK (limit {:.1}h)",
            mode,
            max_gap / 3600.0
        );
        Ok(())
    }

    fn run_live(&self, shutdown_token: ShutdownToken) -> Result<()> {
        info!("══════════════════════════════════════");

        // Latency aggregation is a live-runtime worker, not a detached
        // process singleton. It shares the same token and is joined below.
        let latency_dump_handle = crate::latency::spawn_periodic_dump(
            std::time::Duration::from_secs(60),
            shutdown_token.clone(),
        );
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

        let (market_tx, market_rx) =
            crate::exchange::market_event_channel(CHANNEL_CAPACITY);
        // Strategy↔executor control traffic must not stall quote processing or
        // HTTP completion threads. Venue-specific queues below enforce the
        // actual place/cancel/reconcile admission policy.
        let (signal_tx_raw, signal_rx) = bounded::<RoutedSignal>(CHANNEL_CAPACITY);
        let strategy_count = self.config.strategies.iter().filter(|s| s.enabled).count();
        let (owner_signal_txs, owner_signal_rxs): (Vec<_>, Vec<_>) = (0..strategy_count)
            .map(|_| bounded::<RoutedSignal>(INSTANCE_SIGNAL_LANE_CAPACITY))
            .unzip();
        let (signal_control_tx, signal_control_rx) = bounded::<RoutedSignal>(8);
        let signal_arbiter_handle = spawn_strategy_signal_arbiter(
            signal_tx_raw.clone(),
            owner_signal_rxs,
            signal_control_rx,
        );
        let signal_tx = if strategy_count == 0 {
            SignalSender::system(signal_tx_raw)
        } else {
            SignalSender::system_with_owner_lanes(
                signal_tx_raw,
                owner_signal_txs,
                signal_control_tx,
            )
        };
        let (executor_update_tx, executor_update_rx) =
            bounded::<RoutedOrderUpdate>(CHANNEL_CAPACITY);
        // Authenticated user feeds are independent producers and may need
        // recovery routing by venue order id. Keep them separate from exact
        // owner-stamped executor feedback.
        let (private_update_tx, private_update_rx) = bounded::<OrderUpdate>(CHANNEL_CAPACITY);
        let (shutdown_done_tx, shutdown_done_rx) = bounded::<()>(1);

        let shutdown = shutdown_token.requested_flag();
        let shutdown_tx = market_tx.clone();

        // Periodic flush of the per-request latency CSV on each wall-clock
        // 5-min boundary. `maybe_flush` is a no-op until a boundary is
        // crossed (and entirely a no-op when recording is off), so this
        // tiny poll loop is cheap. A dedicated thread keeps flushing
        // independent of probe / trade activity.
        let latency_flush_handle: Option<thread::JoinHandle<()>> = if recording {
            let shutdown_rx = shutdown_token.subscribe();
            thread::Builder::new()
                .name("latency-record-flush".into())
                .spawn(move || {
                    crate::os_tune::pin_background("latency-record-flush");
                    loop {
                        crossbeam_channel::select! {
                            recv(shutdown_rx) -> _ => break,
                            recv(crossbeam_channel::after(std::time::Duration::from_secs(2))) -> _ => {}
                        }
                        crate::latency_record::maybe_flush();
                    }
                })
                .ok()
        } else {
            None
        };

        // Build the per-instance Polymarket SharedState map. The
        // underlying h2 pool is shared across instances; auth, signer,
        // and order-id registry are per-instance (Phase 2a).
        let poly_states = self.build_poly_shared_states_map_with_shutdown(shutdown_token.clone());
        let mut required_poly_instances = Vec::new();
        for strategy in self.config.strategies.iter().filter(|strategy| {
            strategy.enabled
                && self
                    .registry
                    .capabilities(&strategy.name)
                    .needs_poly_user_feed
        }) {
            if strategy.instance_id.trim().is_empty() {
                anyhow::bail!(
                    "live Polymarket strategy `{}` requires a non-empty instance_id",
                    strategy.name,
                );
            }
            required_poly_instances.push(strategy.instance_id.clone());
        }
        required_poly_instances.sort();
        required_poly_instances.dedup();
        let missing_poly_instances: Vec<String> = required_poly_instances
            .iter()
            .filter(|instance_id| !poly_states.contains_key(*instance_id))
            .cloned()
            .collect();
        if !missing_poly_instances.is_empty() {
            anyhow::bail!(
                "refusing LIVE startup: Polymarket execution routes missing for enabled instance(s) [{}]; no feeds or strategies were started",
                missing_poly_instances.join(","),
            );
        }

        // Persist every subscribed live market-data source through one
        // recorder before strategy fan-out. Route admission above must pass
        // first so a zero-executor process cannot look healthy and run feeds.
        let (recorder_tx, recorder_handle) = self.spawn_recorder_thread()?;

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
                if !sc.enabled || !self.registry.capabilities(&sc.name).needs_rtt_probe {
                    continue;
                }
                if sc.instance_id.is_empty() {
                    continue;
                }
                let iid = sc.instance_id.clone();
                let init_ms: u64 = sc
                    .params
                    .get("quote_interval_ms")
                    .and_then(|v| v.as_integer())
                    .map(|qi| ((qi.max(1) as f64) * 1.5).round() as u64)
                    .unwrap_or(150);
                m.insert(
                    iid,
                    std::sync::Arc::new(std::sync::atomic::AtomicU64::new(init_ms)),
                );
            }
            m
        };

        let exec_handle = self.spawn_execution_thread_with_poly_shutdown(
            signal_rx,
            executor_update_tx,
            poly_states.clone(),
            stale_threshold_handles.clone(),
            shutdown_done_tx,
            shutdown_token.clone(),
        );
        let user_feed_handle =
            self.spawn_hex_user_feed(private_update_tx.clone(), shutdown.clone());
        // Phase 2b: spawn one user_feed per polymarket instance.
        let poly_feed_handles =
            self.spawn_poly_user_feeds(private_update_tx, shutdown.clone(), &poly_states);
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
                let ps = match poly_states.get(id) {
                    Some(s) => s.clone(),
                    None => continue,
                };
                // Probe interval is per-strategy: each polymaker entry
                // may set its own `adaptive_params_v2_probe_interval_secs`
                // (legacy alias: `rtt_gate_probe_interval_secs`).
                let interval_secs = self
                    .config
                    .strategies
                    .iter()
                    .find(|s| {
                        self.registry.capabilities(&s.name).needs_rtt_probe
                            && s.enabled
                            && s.instance_id == *id
                    })
                    .and_then(|s| {
                        s.params
                            .get("adaptive_params_v2_probe_interval_secs")
                            .or_else(|| s.params.get("rtt_gate_probe_interval_secs"))
                    })
                    .and_then(|v| v.as_float().or_else(|| v.as_integer().map(|i| i as f64)))
                    .unwrap_or(2.0)
                    .max(0.1);
                // RTT samples are replaceable observations; a bounded lane
                // prevents a paused strategy from accumulating stale probes.
                let (tx, rx) = crossbeam_channel::bounded::<f64>(64);
                let enable = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                let active_token =
                    crate::exchange::polymarket::rtt_probe::ActiveTokenHandle::new(None);
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
            market_rx,
            signal_tx,
            private_update_rx,
            Some(executor_update_rx),
            false,
            Some(recorder_tx),
            probe_install_map,
            stale_threshold_handles.clone(),
            &poly_states,
            Some(shutdown_done_rx),
        );

        // Strategy construction may spend seconds warming predictors while
        // the authenticated private feed is already replaying. Release each
        // account's bounded startup buffer only after the update router and
        // strategy workers exist; recovery timeout accounting starts at this
        // edge rather than at socket authentication.
        for (_, shared) in Self::dedup_states_by_account(&poly_states) {
            shared.user_feed_health.mark_strategy_consumer_ready();
        }

        // Strategy construction performs prediction/APV2 warm-up
        // synchronously. Subscribe only after it returns: otherwise the
        // bounded market queue can fill while no consumer exists, block CLOB
        // socket consumption, and leave the first tradable book stale.
        let feed_handles = self.spawn_exchange_feeds(market_tx, shutdown.clone())?;

        let shutdown_trigger = Self::wait_for_shutdown_or_critical(
            &shutdown,
            Some(&shutdown_token),
            &shutdown_tx,
            &[("strategy", &strategy_handle), ("execution", &exec_handle)],
        );

        let strategy_join = strategy_handle.join();
        let exec_join = exec_handle.join();
        if let Some(h) = user_feed_handle {
            let _ = h.join();
        }
        for h in poly_feed_handles {
            let _ = h.join();
        }
        if let Some(handle) = signal_arbiter_handle {
            let _ = handle.join();
        }
        for h in heartbeat_handles {
            let _ = h.join();
        }
        for h in probe_handles {
            let _ = h.join();
        }
        for h in feed_handles {
            let _ = h.join();
        }
        // Every producer that can enqueue private lifecycle/account work has
        // stopped. Let the lossless account/audit actors drain and exit, then
        // join them before dropping SharedState.
        shutdown_token.finish();
        for (_, shared) in Self::dedup_states_by_account(&poly_states) {
            let _ = shared.join_background_workers();
        }
        let _ = recorder_handle.join();
        if let Some(h) = latency_flush_handle {
            let _ = h.join();
        }
        if let Some(h) = latency_dump_handle {
            let _ = h.join();
        }

        // Final flush of any buffered latency rows recorded after the
        // flush thread's last tick. No-op when recording is off.
        crate::latency_record::flush();

        info!("  All threads stopped, exiting");
        let mut critical_failures = Vec::new();
        if let ShutdownTrigger::CriticalThread(name) = shutdown_trigger {
            critical_failures.push(format!("critical thread `{name}` exited before shutdown"));
        }
        if strategy_join.is_err() {
            critical_failures.push("strategy thread panicked".to_string());
        }
        if exec_join.is_err() {
            critical_failures.push("execution thread panicked".to_string());
        }
        if !critical_failures.is_empty() {
            anyhow::bail!(
                "live runtime stopped after critical-thread failure: {}",
                critical_failures.join("; "),
            );
        }
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
        info!(
            "  sim_latency: {}ms (one-way, = sim_latency_p50_ms/2)",
            sim_latency_ms
        );
        info!("══════════════════════════════════════");

        // Pre-flight: abort before spawning feeds if recorded spot warm-up
        // data is too stale (same gate as live; orderbook history can't be
        // back-filled from REST). See `check_warmup_data_freshness`.
        self.check_warmup_data_freshness()?;

        let (market_tx, market_rx) =
            crate::exchange::market_event_channel(CHANNEL_CAPACITY);
        let (sim_feed_tx, sim_feed_rx) = bounded::<MarketEvent>(CHANNEL_CAPACITY);
        let (signal_tx_raw, signal_rx) = bounded::<RoutedSignal>(CHANNEL_CAPACITY);
        let strategy_count = self.config.strategies.iter().filter(|s| s.enabled).count();
        let (owner_signal_txs, owner_signal_rxs): (Vec<_>, Vec<_>) = (0..strategy_count)
            .map(|_| bounded::<RoutedSignal>(INSTANCE_SIGNAL_LANE_CAPACITY))
            .unzip();
        let (signal_control_tx, signal_control_rx) = bounded::<RoutedSignal>(8);
        let signal_arbiter_handle = spawn_strategy_signal_arbiter(
            signal_tx_raw.clone(),
            owner_signal_rxs,
            signal_control_rx,
        );
        let signal_tx = if strategy_count == 0 {
            SignalSender::system(signal_tx_raw)
        } else {
            SignalSender::system_with_owner_lanes(
                signal_tx_raw,
                owner_signal_txs,
                signal_control_tx,
            )
        };
        let (update_tx, update_rx) = bounded::<OrderUpdate>(CHANNEL_CAPACITY);
        let (shutdown_done_tx, shutdown_done_rx) = bounded::<()>(1);

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_tx = market_tx.clone();

        // Live exchange feeds — Polymarket events also sent to sim_feed_tx for the sim_v2 core
        let feed_handles =
            self.spawn_exchange_feeds_paper(market_tx, Some(sim_feed_tx), shutdown.clone())?;

        // Paper execution: sim_v2 matching core fed by live Polymarket data.
        // `sim_latency_ms` was computed above from sim_latency_p50_ms/2.
        let exec_handle = Self::spawn_paper_execution_thread(
            signal_rx,
            sim_feed_rx,
            update_tx.clone(),
            sim_latency_ms,
            self.config.backtest.clone(),
            shutdown_done_tx,
        );

        // Spawn recorder for market data persistence (paper data goes to separate dir)
        let (recorder_tx, recorder_handle) =
            self.spawn_recorder_thread_to(&self.config.recording.paper_data_dir)?;

        // Strategy thread: same as live, data_dir = backtest.data_dir with paper_data_dir fallback
        // Paper mode: no RTT-probe (no real CLOB to probe). No stale-
        // threshold handle either — paper exec doesn't apply the gate.
        let strategy_handle = self.spawn_strategy_thread(
            market_rx,
            signal_tx,
            update_rx,
            None,
            false,
            Some(recorder_tx),
            HashMap::new(),
            HashMap::new(),
            // Paper mode has no live PM user feed (fills are sim-driven), so
            // the user-feed-health gates stay inactive (empty map).
            &HashMap::new(),
            Some(shutdown_done_rx),
        );

        Self::wait_for_shutdown(&shutdown, &shutdown_tx);

        let _ = strategy_handle.join();
        let _ = exec_handle.join();
        if let Some(handle) = signal_arbiter_handle {
            let _ = handle.join();
        }
        for h in feed_handles {
            let _ = h.join();
        }
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

        let (market_tx, market_rx) =
            crate::exchange::market_event_channel(CHANNEL_CAPACITY);
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_tx = market_tx.clone();

        // Probe target token, shared by all probe tasks (one polymaker
        // series → one current event). Populated from the feed's
        // Instrument events inside the recorder loop (no strategy runs in
        // RECORD mode, so there's nothing else to set it).
        let probe_active_token =
            crate::exchange::polymarket::rtt_probe::ActiveTokenHandle::new(None);

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
                let ps = match poly_states.get(id) {
                    Some(s) => s.clone(),
                    None => continue,
                };
                let interval_secs = self
                    .config
                    .strategies
                    .iter()
                    .find(|s| {
                        self.registry.capabilities(&s.name).needs_rtt_probe
                            && s.enabled
                            && s.instance_id == *id
                    })
                    .and_then(|s| {
                        s.params
                            .get("adaptive_params_v2_probe_interval_secs")
                            .or_else(|| s.params.get("rtt_gate_probe_interval_secs"))
                    })
                    .and_then(|v| v.as_float().or_else(|| v.as_integer().map(|i| i as f64)))
                    .unwrap_or(2.0)
                    .max(0.1);
                // Gate channel is unused in RECORD mode (no strategy to
                // drain it) — drop the receiver; all_probe sends are
                // best-effort and ignore the disconnected channel.
                let (tx, _rx) = crossbeam_channel::bounded::<f64>(64);
                let enable = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
                match crate::exchange::polymarket::rtt_probe::spawn_rtt_probe(
                    ps,
                    enable,
                    tx,
                    probe_active_token.clone(),
                    std::time::Duration::from_secs_f64(interval_secs),
                    shutdown.clone(),
                    true,
                    id.clone(),
                ) {
                    Ok(h) => {
                        info!(
                            "[Engine] rtt_probe started for instance_id={} interval={:.1}s all_probe=true",
                            id, interval_secs
                        );
                        probe_handles.push(h);
                    }
                    Err(e) => warn!(
                        "[Engine] rtt_probe spawn failed for instance_id={}: {}",
                        id, e
                    ),
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
        let token_for_recorder = if all_probe {
            Some(probe_active_token.clone())
        } else {
            None
        };

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
            self.config
                .exchanges
                .iter()
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
                // Same bounded-shard checkpoint cadence as the live recorder.
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
                                        // Polymarket snapshots encode asks worst-to-best.
                                        let ask = ob.best_ask().map(|l| l.price);
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
                                        tok.store(Some(chosen));
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
        for h in feed_handles {
            let _ = h.join();
        }
        // Now recorder sees channel closed or Exit message → flushes and exits
        let _ = recorder_handle.join();
        // All-probe teardown: stop the probes + latency flush, final flush.
        for h in probe_handles {
            let _ = h.join();
        }
        if let Some(h) = latency_flush_handle {
            let _ = h.join();
        }
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
        use crate::exchange::sim_v2::{SimV2Config, Simulator};
        use std::collections::BinaryHeap;

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
        let start_time = if bt.start_date.is_empty() {
            unbounded_start
        } else {
            parse_dt(&bt.start_date)?
        };
        let end_time = if bt.end_date.is_empty() {
            if bt.start_date.is_empty() {
                unbounded_end
            } else {
                parse_dt(&bt.start_date)? + chrono::TimeDelta::days(1)
            }
        } else {
            parse_dt(&bt.end_date)?
        };
        let start_ns = start_time.timestamp_nanos_opt().unwrap_or(0) as u64;
        let end_ns = end_time.timestamp_nanos_opt().unwrap_or(0) as u64;

        let mut replay_sources: Vec<(String, String)> = Vec::new();
        for ex_cfg in &self.config.exchanges {
            if !ex_cfg.enabled {
                continue;
            }
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
            if symbol.starts_with("rtds:")
                || exchange == "binance_futures"
                || exchange == "chainlink"
                || exchange == "pyth"
            {
                continue;
            }
            if let Ok(r) = MarketReplayer::new(&data_path, exchange, symbol, start_dt, end_dt) {
                strat_replayers.push(r);
            }
        }
        if !bt.market_data_health_replay_path.trim().is_empty() {
            let path = PathBuf::from(bt.market_data_health_replay_path.trim());
            let replayer = MarketReplayer::from_market_data_health_csv(
                &path,
                start_dt,
                end_dt,
            )
            .map_err(|error| {
                anyhow::anyhow!(
                    "cannot load market_data_health_replay_path {}: {}",
                    path.display(),
                    error,
                )
            })?;
            strat_replayers.push(replayer);
        }
        for (_exchange, symbol) in &replay_sources {
            let rtds_rest = match symbol.strip_prefix("rtds:") {
                Some(r) => r,
                None => continue,
            };
            let parts: Vec<&str> = rtds_rest.splitn(2, ':').collect();
            if parts.len() != 2 {
                continue;
            }
            let source = parts[0];
            for filter in parts[1]
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
            {
                let sym_lower = filter.to_lowercase().replace('/', "-");
                let rtds_path = format!("{}/{}", source, sym_lower);
                let rtds_end_dt = end_dt + chrono::TimeDelta::seconds(10);
                if let Ok(r) =
                    MarketReplayer::new(&data_path, "rtds", &rtds_path, start_dt, rtds_end_dt)
                {
                    strat_replayers.push(r);
                }
            }
        }
        for (exchange, symbol) in &replay_sources {
            if exchange != "chainlink" && exchange != "pyth" {
                continue;
            }
            let sym_lower = symbol.to_lowercase().replace('/', "-");
            let early = if exchange == "chainlink" { 10 } else { 0 };
            let start = start_dt - chrono::TimeDelta::seconds(early);
            let end = end_dt + chrono::TimeDelta::seconds(10);
            if let Ok(r) = MarketReplayer::new(&data_path, exchange, &sym_lower, start, end) {
                strat_replayers.push(r);
            }
        }
        for (exchange, symbol) in &replay_sources {
            if exchange != "binance_futures" {
                continue;
            }
            let base_symbol = symbol.split('@').next().unwrap_or(symbol);
            let sym_lower = if base_symbol.len() > 3 && base_symbol.to_uppercase().ends_with("USD")
            {
                let base = &base_symbol[..base_symbol.len() - 3];
                format!("{}-usd", base.to_lowercase())
            } else {
                base_symbol.to_lowercase()
            };
            let end = end_dt + chrono::TimeDelta::seconds(10);
            if let Ok(r) =
                MarketReplayer::new(&data_path, "binance_futures", &sym_lower, start_dt, end)
            {
                strat_replayers.push(r);
            }
        }

        let mut strat_peeked: Vec<Option<(u64, MarketEvent)>> = Vec::new();
        for r in &mut strat_replayers {
            strat_peeked.push(r.next_event().ok().flatten());
        }

        // ── Synthetic RTT-probe wiring — carried over from the removed v1 sim engine: while
        // the strat gate is in Probe mode it sets `bt_probe_enable`; we feed one
        // place-RTT sample (from the v2 latency sampler) every
        // `bt_probe_interval_ns` of sim clock so the gate recovers Probe→Trade
        // and the strategy quotes. ──
        let (bt_probe_tx, bt_probe_rx) = crossbeam_channel::bounded::<f64>(64);
        let bt_probe_enable = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let bt_probe_interval_ns: u64 = {
            let secs = self
                .config
                .strategies
                .iter()
                .find(|s| self.registry.capabilities(&s.name).needs_rtt_probe)
                .and_then(|s| {
                    s.params
                        .get("adaptive_params_v2_probe_interval_secs")
                        .or_else(|| s.params.get("rtt_gate_probe_interval_secs"))
                })
                .and_then(|v| v.as_float().or_else(|| v.as_integer().map(|i| i as f64)))
                .unwrap_or(2.0)
                .max(0.1);
            (secs * 1e9) as u64
        };
        let mut last_bt_probe_emit_sim_ns: u64 = 0;
        let bt_probe_active_token =
            crate::exchange::polymarket::rtt_probe::ActiveTokenHandle::new(None);
        let bt_probe_map: HashMap<
            String,
            (
                crossbeam_channel::Receiver<f64>,
                std::sync::Arc<std::sync::atomic::AtomicBool>,
                crate::exchange::polymarket::rtt_probe::ActiveTokenHandle,
            ),
        > = self
            .config
            .strategies
            .iter()
            .find(|s| {
                self.registry.capabilities(&s.name).needs_rtt_probe
                    && s.enabled
                    && !s.instance_id.is_empty()
            })
            .map(|s| {
                let mut m = HashMap::new();
                m.insert(
                    s.instance_id.clone(),
                    (bt_probe_rx, bt_probe_enable.clone(), bt_probe_active_token),
                );
                m
            })
            .unwrap_or_else(HashMap::new);

        let mut strategies = self.build_strategies(bt_probe_map, HashMap::new(), &HashMap::new());
        let hist_data_dir = PathBuf::from(&data_dir);
        for s in &mut strategies {
            s.on_init();
        }

        // ── Hist bars (binance) — the lookback comes from the strategies
        // themselves via `load_hist_data(start_ns)`, the SAME path live uses
        // (live: engine.rs run_live → strategy.load_hist_data(ts_event)), so
        // BT and live train vol models on identically-sized windows.
        // Strategies that return no request fall back to the legacy 30-day
        // window. ──
        let needs_hist_bars = self
            .config
            .strategies
            .iter()
            .any(|s| s.enabled && self.registry.capabilities(&s.name).needs_hist_bars);
        // Compact hist-bar storage (2026-07-26). Formerly every loaded bar
        // became a `(u64, MarketEvent::Bar)` (272 B enum + 2 heap Strings per
        // bar) in one big sorted Vec — a 29d 1s lookback + in-window tail is
        // ~3.1M bars ≈ >1 GB transient. Bars are now STREAMED out of the
        // parquet reader (`load_hist_bars_streamed`): pre-window bars are fed
        // straight to `Strategy::on_hist_bar` without being retained, and
        // only in-window rows are kept — 72 B each, rebuilt into a full
        // `BarData` on emission in the merge loop below.
        //
        // Both bar producers (parquet reader + REST kline fetch) set
        // `is_closed = true` and `exchange/local_timestamp_ns =
        // close_time_ns`, so the row only stores the 9 varying fields;
        // `to_bar` reinstates the invariant ones from the per-source
        // template (exchange/symbol/interval).
        #[derive(Clone, Copy)]
        struct HistBarRow {
            open_time_ns: u64,
            close_time_ns: u64,
            open: f64,
            high: f64,
            low: f64,
            close: f64,
            volume: f64,
            taker_buy_base: f64,
            quote_volume: f64,
        }
        impl HistBarRow {
            fn from_bar(b: &crate::types::BarData) -> Self {
                debug_assert!(
                    b.is_closed
                        && b.exchange_timestamp_ns == b.close_time_ns
                        && b.local_timestamp_ns == b.close_time_ns,
                    "hist bar violates the compact-row invariants"
                );
                Self {
                    open_time_ns: b.open_time_ns,
                    close_time_ns: b.close_time_ns,
                    open: b.open,
                    high: b.high,
                    low: b.low,
                    close: b.close,
                    volume: b.volume,
                    taker_buy_base: b.taker_buy_base,
                    quote_volume: b.quote_volume,
                }
            }
            fn to_bar(&self, template: &crate::types::BarData) -> crate::types::BarData {
                let mut b = template.clone();
                b.open_time_ns = self.open_time_ns;
                b.close_time_ns = self.close_time_ns;
                b.open = self.open;
                b.high = self.high;
                b.low = self.low;
                b.close = self.close;
                b.volume = self.volume;
                b.taker_buy_base = self.taker_buy_base;
                b.quote_volume = self.quote_volume;
                b.is_closed = true;
                b.exchange_timestamp_ns = self.close_time_ns;
                b.local_timestamp_ns = self.close_time_ns;
                b
            }
        }
        let mut bar_templates: Vec<crate::types::BarData> = Vec::new();
        // In-window rows as (source-idx, row), merged by (close_time, source)
        // — exactly the order the former stable `sort_by_key(close_time)`
        // produced (push order was source order).
        let mut bar_rows: Vec<(u32, HistBarRow)> = Vec::new();
        if needs_hist_bars {
            let hist_bar_interval: String = self
                .config
                .strategies
                .iter()
                .find(|s| s.enabled && self.registry.capabilities(&s.name).needs_hist_bars)
                .and_then(|s| s.params.get("hist_bar_interval"))
                .and_then(|v| v.as_str())
                .unwrap_or("1m")
                .to_string();
            let fallback_lookback_ns = 30u64 * 24 * 3_600_000_000_000;
            let strat_start_ns = strategies
                .iter()
                .flat_map(|s| s.load_hist_data(start_ns))
                .map(|r| r.start_date_ns)
                .min();
            let hist_start_ns =
                strat_start_ns.unwrap_or_else(|| start_ns.saturating_sub(fallback_lookback_ns));
            info!(
                "[Replayer v2] hist-bar window: {:.2}d before backtest start ({})",
                (start_ns.saturating_sub(hist_start_ns)) as f64 / 86_400e9,
                if strat_start_ns.is_some() {
                    "strategy-declared"
                } else {
                    "30d fallback"
                }
            );
            let bin_symbols: Vec<String> = replay_sources
                .iter()
                .filter(|(exchange, _)| exchange == "binance")
                .map(|(_, symbol)| symbol.clone())
                .collect();
            // Sole source (the standing config) ⇒ stream order IS merged
            // order: pre-window bars are fed inline with zero retention.
            // Multi-source keeps compact pre-window rows per source and
            // merge-feeds them below so the interleaving matches the former
            // global sort.
            let single_source = bin_symbols.len() == 1;
            let mut pre_rows_by_src: Vec<Vec<HistBarRow>> = Vec::new();
            let mut in_rows_by_src: Vec<Vec<HistBarRow>> = Vec::new();
            let mut fed_hist = false;
            for symbol in &bin_symbols {
                let req = crate::types::HistDataRequest {
                    exchange: crate::types::Exchange::Binance,
                    symbol: symbol.clone(),
                    interval: hist_bar_interval.clone(),
                    start_date_ns: hist_start_ns,
                    end_date_ns: end_ns,
                };
                bar_templates.push(crate::types::BarData {
                    exchange: crate::types::Exchange::Binance,
                    symbol: symbol.clone(),
                    interval: hist_bar_interval.clone(),
                    open_time_ns: 0,
                    close_time_ns: 0,
                    open: 0.0,
                    high: 0.0,
                    low: 0.0,
                    close: 0.0,
                    volume: 0.0,
                    taker_buy_base: 0.0,
                    quote_volume: 0.0,
                    is_closed: true,
                    exchange_timestamp_ns: 0,
                    local_timestamp_ns: 0,
                });
                let mut pre: Vec<HistBarRow> = Vec::new();
                let mut inr: Vec<HistBarRow> = Vec::new();
                let src_idx = in_rows_by_src.len() as u32;
                let res = crate::recorder::load_hist_bars_streamed(&data_path, &req, &mut |bar| {
                    // The event ts (former sort key) is close_time_ns.
                    if bar.close_time_ns < start_ns {
                        if single_source {
                            for s in &mut strategies {
                                s.on_hist_bar(bar);
                            }
                            fed_hist = true;
                        } else {
                            pre.push(HistBarRow::from_bar(bar));
                        }
                    } else if single_source {
                        bar_rows.push((src_idx, HistBarRow::from_bar(bar)));
                    } else {
                        inr.push(HistBarRow::from_bar(bar));
                    }
                });
                if let Err(e) = res {
                    error!(
                        "[Replayer v2] CRITICAL: load_hist_bars failed for binance/{} {}: {}",
                        symbol, hist_bar_interval, e
                    );
                    std::process::exit(2);
                }
                pre_rows_by_src.push(pre);
                in_rows_by_src.push(inr);
            }
            // k-way merge by (close_time, source-idx) — multi-source only;
            // the single-source vectors above are empty.
            let merge = |by_src: Vec<Vec<HistBarRow>>, emit: &mut dyn FnMut(u32, &HistBarRow)| {
                let mut cursors = vec![0usize; by_src.len()];
                loop {
                    let mut best: Option<(u64, usize)> = None;
                    for (i, rows) in by_src.iter().enumerate() {
                        if let Some(r) = rows.get(cursors[i]) {
                            if best.map_or(true, |(bt, _)| r.close_time_ns < bt) {
                                best = Some((r.close_time_ns, i));
                            }
                        }
                    }
                    let Some((_, i)) = best else { break };
                    emit(i as u32, &by_src[i][cursors[i]]);
                    cursors[i] += 1;
                }
            };
            if !single_source {
                let templates = &bar_templates;
                merge(pre_rows_by_src, &mut |src, row| {
                    let bar = row.to_bar(&templates[src as usize]);
                    for s in &mut strategies {
                        s.on_hist_bar(&bar);
                    }
                    fed_hist = true;
                });
                merge(in_rows_by_src, &mut |src, row| {
                    bar_rows.push((src, *row));
                });
            }
            if fed_hist {
                // Fresh-bars gate (when on) reads completeness from each
                // strategy's own fit-window-capped resample cache, so BT (a
                // 30-day prefetch) matches live (fit-window load) — out-of-
                // window holes auto-drop and don't false-pause.
                for s in &mut strategies {
                    s.on_hist_data_loaded(start_ns);
                }
            }
        }
        let mut bar_cursor: usize = 0;

        // ── Prediction warm-up — verbatim from v1 ──
        {
            let (warmup_sources, warmup_hours) = self.prediction_warmup_sources();
            if !warmup_sources.is_empty() && warmup_hours > 0.0 {
                let hour_ns: u64 = 3600 * 1_000_000_000;
                let warmup_end_ns = (start_ns / hour_ns) * hour_ns;
                let warmup_end_dt =
                    chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(warmup_end_ns as i64);
                let warmup_start_dt =
                    warmup_end_dt - chrono::TimeDelta::seconds((warmup_hours * 3600.0) as i64);
                for s in &mut strategies {
                    s.on_prediction_warmup_start();
                }
                for (exchange, symbol) in &warmup_sources {
                    match crate::recorder::MarketReplayer::new(
                        &data_path,
                        exchange,
                        symbol,
                        warmup_start_dt,
                        warmup_end_dt,
                    ) {
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
                        Err(e) => warn!(
                            "[Backtest v2] Warm-up: no data for {}/{}: {}",
                            exchange, symbol, e
                        ),
                    }
                }
                for s in &mut strategies {
                    s.on_prediction_warmup_end(start_ns);
                }
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
                let aw_end_dt =
                    chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(start_ns as i64);
                let aw_start_dt =
                    aw_end_dt - chrono::TimeDelta::seconds((aw_days * 86400.0) as i64);
                // apv2 warm-up cache: strategies import their cached per-bucket
                // baseline and narrow the raw replay to the uncached tail gap.
                // (Live-only by default; a BT strategy returns aw_start ⇒ full
                // replay ⇒ byte-identical baseline.)
                let aw_start_ns = aw_start_dt.timestamp_nanos_opt().unwrap_or(0).max(0) as u64;
                let aw_end_ns = aw_end_dt.timestamp_nanos_opt().unwrap_or(0).max(0) as u64;
                let resume_ns = strategies
                    .iter_mut()
                    .map(|s| s.apv2_warmup_resume_ns(aw_start_ns, aw_end_ns))
                    .min()
                    .unwrap_or(aw_start_ns);
                let replay_start_dt =
                    chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(resume_ns as i64);
                info!(
                    "[Backtest v2] apv2 warm-up: {:.1}d window [{} → {}], raw replay from {}",
                    aw_days,
                    aw_start_dt.format("%Y-%m-%d %H:%M"),
                    aw_end_dt.format("%Y-%m-%d %H:%M"),
                    replay_start_dt.format("%Y-%m-%d %H:%M")
                );
                let mut replayers: Vec<crate::recorder::MarketReplayer> = Vec::new();
                for (exchange, symbol) in &spot_sources {
                    match crate::recorder::MarketReplayer::new(
                        &data_path,
                        exchange,
                        symbol,
                        replay_start_dt,
                        aw_end_dt,
                    ) {
                        Ok(r) => replayers.push(r),
                        Err(e) => warn!(
                            "[Backtest v2] apv2 warm-up: no data for {}/{}: {}",
                            exchange, symbol, e
                        ),
                    }
                }
                // One buffered event per replayer; repeatedly emit the global
                // minimum-timestamp event (merge by local_ts, same key the
                // main replay uses) so apv2's wall-clock buckets see venues
                // interleaved chronologically.
                let mut peeked: Vec<Option<(u64, MarketEvent)>> = replayers
                    .iter_mut()
                    .map(|r| r.next_event().ok().flatten())
                    .collect();
                let mut fed: u64 = 0;
                loop {
                    let best = peeked
                        .iter()
                        .enumerate()
                        .filter_map(|(i, p)| p.as_ref().map(|(ts, _)| (i, *ts)))
                        .min_by_key(|&(_, ts)| ts);
                    let Some((idx, _)) = best else {
                        break;
                    };
                    let (_, event) = peeked[idx].take().unwrap();
                    match &event {
                        MarketEvent::OrderBook(ob) => {
                            for s in &mut strategies {
                                s.on_apv2_warmup_orderbook(ob);
                            }
                        }
                        MarketEvent::Trade(t) => {
                            for s in &mut strategies {
                                s.on_apv2_warmup_trade(t);
                            }
                        }
                        _ => {}
                    }
                    fed += 1;
                    peeked[idx] = replayers[idx].next_event().ok().flatten();
                }
                info!(
                    "[Backtest v2] apv2 warm-up complete: {} spot events fed",
                    fed
                );
                for s in &mut strategies {
                    s.apv2_warmup_finalize_cache();
                }
            }
        }

        let mut last_quote_ns: Vec<u64> = vec![0; strategies.len()];
        let mut quote_signal_batch = Vec::with_capacity(32);

        // Per-instance USDC + per-event split shares (carried over from the removed v1 sim's wallet
        // seeding). split_amount_usdc → shares of each token credited at event.
        let mut sim_wallet_usdc_by_iid: HashMap<String, f64> = HashMap::new();
        let mut sim_split_by_iid: HashMap<String, f64> = HashMap::new();
        for s in &self.config.strategies {
            if !s.enabled
                || !self.registry.capabilities(&s.name).needs_sim_wallet
                || s.instance_id.is_empty()
            {
                continue;
            }
            let bal = s
                .params
                .get("init_balance")
                .and_then(|v| v.as_float().or_else(|| v.as_integer().map(|i| i as f64)))
                .unwrap_or(0.0);
            sim_wallet_usdc_by_iid.insert(s.instance_id.clone(), bal);
            // Split seed amount. Preferred key `split_hands` is denominated in
            // hands (× base_qty → USDC); legacy raw `split_amount_usdc` is kept
            // as a fallback for unmigrated configs. Must match the same formula
            // used at the live/maintenance read site (search `split_hands`).
            let pf = |key: &str, default: f64| -> f64 {
                s.params
                    .get(key)
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
        let dflt_lat = (
            bt.sim_latency_p50_ms as f64,
            bt.sim_latency_p95_ms as f64,
            bt.sim_latency_p99_ms as f64,
        );
        // Calibrate only for the log/archive source (the directory source
        // carries its own per-sample RTT and skips the analytic fit).
        let calibrated: Option<crate::exchange::sim::latency::CalibratedParams> =
            if calib_from.is_empty() || is_record_dir {
                None
            } else {
                let paths: Vec<String> = bt
                    .sim_latency_calibrate_from
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                match crate::exchange::sim::latency::calibrate_from_logs(&paths) {
                    Ok(cal) => Some(cal),
                    Err(e) => {
                        warn!(
                            "[Backtest v2] RTT calibration failed ({}): using static knobs",
                            e
                        );
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
                    cal.place.p50_ms,
                    cal.place.p95_ms,
                    cal.place.p99_ms,
                    cal.cancel.p50_ms,
                    cal.cancel.p95_ms,
                    cal.cancel.p99_ms,
                    cal.place.rho_lag1,
                    ct,
                );
                (
                    (cal.place.p50_ms, cal.place.p95_ms, cal.place.p99_ms),
                    (cal.cancel.p50_ms, cal.cancel.p95_ms, cal.cancel.p99_ms),
                    cal.place.rho_lag1.unwrap_or(bt.sim_latency_correlation),
                    cal.cross_corr_log_p99
                        .unwrap_or(bt.sim_latency_cross_correlation),
                    ct,
                )
            }
            None => (
                dflt_lat,
                dflt_lat,
                bt.sim_latency_correlation,
                bt.sim_latency_cross_correlation,
                bt.sim_client_timeout_ms,
            ),
        };
        let ahead_frac = if bt.sim_v2_ahead_frac >= 0.0 {
            Some(bt.sim_v2_ahead_frac)
        } else {
            None
        };

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
        let mut dynamic_window_rtt_by_event: Option<std::collections::HashMap<u64, f64>> = None;
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
                        dir.display(),
                        data.n_files,
                        data.place.n(),
                        data.place.min_epoch_ms(),
                        data.place.max_epoch_ms(),
                        data.cancel.n(),
                        params.abs_tol_ms,
                        params.tod_tol_secs,
                        bucket_secs,
                        params.fallback.as_str(),
                        lat_rho,
                        lat_cross,
                    );
                    if bt.sim_v2_dynamic_taker_windows {
                        let table = data.place.causal_rolling_event_quantile(
                            300,
                            bt.sim_v2_dynamic_window_rtt_lookback_events.max(1) as usize,
                            bt.sim_v2_dynamic_window_rtt_quantile,
                            bt.sim_v2_dynamic_window_rtt_cap_ms,
                        );
                        let (min_state, max_state) = table
                            .values()
                            .fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), value| {
                                (lo.min(*value), hi.max(*value))
                            });
                        info!(
                            "[Backtest v2] dynamic taker windows: {} causal event states, lookback={} events, event_q={:.2}, cap={:.0}ms, ref={:.3}ms, race_elasticity={:.3}, comp_elasticity={:.3}, multiplier=[{:.2},{:.2}], state_range=[{:.1},{:.1}]ms",
                            table.len(),
                            bt.sim_v2_dynamic_window_rtt_lookback_events,
                            bt.sim_v2_dynamic_window_rtt_quantile,
                            bt.sim_v2_dynamic_window_rtt_cap_ms,
                            bt.sim_v2_dynamic_window_rtt_ref_ms,
                            bt.sim_v2_dynamic_race_rtt_elasticity,
                            bt.sim_v2_dynamic_comp_rtt_elasticity,
                            bt.sim_v2_dynamic_window_min_mult,
                            bt.sim_v2_dynamic_window_max_mult,
                            min_state,
                            max_state,
                        );
                        dynamic_window_rtt_by_event = Some(table);
                    }
                    use crate::exchange::sim::latency::LatencyProfile::RecordReplay;
                    (
                        Some(RecordReplay {
                            records: data.place.clone(),
                            rho: lat_rho,
                            params,
                        }),
                        Some(RecordReplay {
                            records: data.cancel.clone(),
                            rho: lat_rho,
                            params,
                        }),
                    )
                }
                Ok(data) => {
                    warn!(
                        "[Backtest v2] record-replay dir {} has an empty side (place {}, cancel {}) → static knobs",
                        dir.display(),
                        data.place.n(),
                        data.cancel.n()
                    );
                    (None, None)
                }
                Err(e) => {
                    warn!(
                        "[Backtest v2] record-replay load {} failed ({}) → static knobs",
                        dir.display(),
                        e
                    );
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
            use crate::exchange::sim::latency::{
                EmpiricalAnchors, LatencyProfile, SidedParams, HOURLY_MIN_HOURS,
            };
            let to_anchors = |s: &SidedParams| EmpiricalAnchors {
                p50_ms: s.p50_ms,
                p85_ms_override: s.p85_ms,
                p95_ms: s.p95_ms,
                p99_ms: s.p99_ms,
                p999_ms_override: s.p999_ms_override,
                gpd_tail: s.gpd_tail,
            };
            let build_hourly = |hourly: &[Option<SidedParams>; 24],
                                pooled: &SidedParams,
                                side: &str|
             -> Option<LatencyProfile> {
                let n_pop = hourly.iter().filter(|h| h.is_some()).count();
                if n_pop < HOURLY_MIN_HOURS {
                    info!(
                        "[Backtest v2] {} hourly: only {} populated UTC-hour bucket(s) (<{}) → pooled empirical CDF",
                        side, n_pop, HOURLY_MIN_HOURS
                    );
                    return None;
                }
                let anchors: [Option<EmpiricalAnchors>; 24] =
                    std::array::from_fn(|h| hourly[h].as_ref().map(to_anchors));
                info!(
                    "[Backtest v2] {} hourly-empirical RTT: {} populated UTC-hour buckets (ρ={:.3})",
                    side, n_pop, lat_rho
                );
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
        if bt.sim_v2_dynamic_taker_windows && !is_record_dir {
            warn!(
                "[Backtest v2] dynamic taker windows require sim_latency_calibrate_from to be a latency-record directory; using fixed windows"
            );
        }

        // ── Per-event RTT table (sim_rtt_mode="exact"): per-event live RTT
        // shape (+ intra-event early/late segments) + prev_event p60. None →
        // pooled CDF (predict mode). Used by the sim lane (latency anchors) and
        // the strat lane (gate prev_p override). ──
        let per_event_rtt_table: Option<
            std::collections::HashMap<u64, crate::exchange::sim::per_event_rtt::EventRttOverride>,
        > = if crate::exchange::sim::SimRttMode::from_str(&bt.sim_rtt_mode)
            == crate::exchange::sim::SimRttMode::Exact
            && !calib_from.is_empty()
            && !is_record_dir
        {
            let paths: Vec<String> = bt
                .sim_latency_calibrate_from
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            match crate::exchange::sim::per_event_rtt::extract_per_event_rtt(&paths) {
                Ok(t) if !t.is_empty() => {
                    let n_place = t.values().filter(|e| e.place_p50_ms.is_some()).count();
                    let n_seg = t.values().filter(|e| e.has_segmented_place()).count();
                    info!(
                        "[Backtest v2] per-event RTT (sim_rtt_mode=exact): {} events, {} with place quantiles, {} segmented",
                        t.len(),
                        n_place,
                        n_seg
                    );
                    Some(t)
                }
                Ok(_) => {
                    warn!("[Backtest v2] per-event RTT table empty → pooled CDF");
                    None
                }
                Err(e) => {
                    warn!(
                        "[Backtest v2] per-event RTT extract failed ({}) → pooled CDF",
                        e
                    );
                    None
                }
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
            let cfg = (
                bt.sim_v2_taker_overhead_p50_ms,
                bt.sim_v2_taker_overhead_p95_ms,
                bt.sim_v2_taker_overhead_p99_ms,
            );
            if at_default && !calib_from.is_empty() && !is_record_dir {
                let paths: Vec<String> = bt
                    .sim_latency_calibrate_from
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                match crate::exchange::sim::per_event_rtt::extract_taker_overhead(&paths) {
                    Ok(Some((a, b, c))) => {
                        info!(
                            "[Backtest v2] taker overhead auto-calibrated from log: p50/p95/p99={:.0}/{:.0}/{:.0} ms",
                            a, b, c
                        );
                        (a, b, c)
                    }
                    Ok(None) => {
                        warn!("[Backtest v2] taker overhead: <30 paired samples → config defaults");
                        cfg
                    }
                    Err(e) => {
                        warn!(
                            "[Backtest v2] taker overhead extract failed ({}) → config defaults",
                            e
                        );
                        cfg
                    }
                }
            } else {
                cfg
            }
        };

        // ── Causal dynamic taker matching overhead. The live-log source is
        // deliberately separate from the exact HTTP RTT latency directory.
        // Rows for event E contain only completed-event samples before E. ──
        let dynamic_taker_overhead_by_event = if bt.sim_v2_dynamic_taker_overhead {
            let source = bt.sim_v2_taker_overhead_calibrate_from.trim();
            if source.is_empty() {
                warn!(
                    "[Backtest v2] dynamic taker overhead enabled without sim_v2_taker_overhead_calibrate_from; using fixed anchors"
                );
                None
            } else {
                let paths: Vec<String> = source
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                let base = (tovh_p50, tovh_p95, tovh_p99);
                match crate::exchange::sim::per_event_rtt::extract_dynamic_taker_overhead(
                    &paths,
                    bt.sim_v2_taker_overhead_instance_id.trim(),
                    bt.sim_v2_dynamic_taker_overhead_lookback_events.max(1) as usize,
                    bt.sim_v2_dynamic_taker_overhead_min_samples.max(1) as usize,
                    bt.sim_v2_dynamic_taker_overhead_blend,
                    base,
                ) {
                    Ok(t) if !t.is_empty() => {
                        info!(
                            "[Backtest v2] dynamic taker overhead: {} causal events, iid={}, lookback={}, min_samples={}, blend={:.2}",
                            t.len(),
                            bt.sim_v2_taker_overhead_instance_id,
                            bt.sim_v2_dynamic_taker_overhead_lookback_events,
                            bt.sim_v2_dynamic_taker_overhead_min_samples,
                            bt.sim_v2_dynamic_taker_overhead_blend
                        );
                        Some(t)
                    }
                    Ok(_) => {
                        warn!(
                            "[Backtest v2] dynamic taker overhead table empty; using fixed anchors"
                        );
                        None
                    }
                    Err(e) => {
                        warn!(
                            "[Backtest v2] dynamic taker overhead extract failed ({}); using fixed anchors",
                            e
                        );
                        None
                    }
                }
            }
        } else {
            None
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
            dynamic_ahead_frac_strength: bt.sim_v2_dynamic_ahead_frac_strength,
            adverse_sel_rate: bt.sim_v2_adverse_sel_rate,
            adverse_scale_ticks: bt.sim_v2_adverse_scale_ticks,
            book_through_rate: bt.sim_v2_book_through_rate,
            unexplained_depletion_exec_rate: bt.sim_v2_unexplained_depletion_exec_rate,
            inferred_maker_residual_rate: bt.sim_v2_inferred_maker_residual_rate,
            inferred_maker_residual_fraction: bt.sim_v2_inferred_maker_residual_fraction,
            replay_self_depth_rate: bt.sim_v2_replay_self_depth_rate,
            replay_self_taker_depth_rate: bt.sim_v2_replay_self_taker_depth_rate,
            cancel_finality_delay_frac: bt.sim_v2_cancel_finality_delay_frac,
            fill_markout_vn: bt.sim_v2_fill_markout_vn,
            book_fill_markout_vn: bt.sim_v2_book_fill_markout_vn,
            fill_markout_horizon_ns: bt.sim_v2_fill_markout_horizon_ms.saturating_mul(1_000_000),
            dynamic_fill_markout: bt.sim_v2_dynamic_fill_markout,
            dynamic_markout_spot_vol: bt.sim_v2_dynamic_markout_spot_vol,
            dynamic_markout_lookback_ns: bt
                .sim_v2_dynamic_markout_lookback_ms
                .saturating_mul(1_000_000),
            dynamic_markout_vol_ref_ticks: bt.sim_v2_dynamic_markout_vol_ref_ticks,
            dynamic_markout_vol_elasticity: bt.sim_v2_dynamic_markout_vol_elasticity,
            dynamic_markout_min_mult: bt.sim_v2_dynamic_markout_min_mult,
            dynamic_markout_max_mult: bt.sim_v2_dynamic_markout_max_mult,
            fill_push_mult: bt.sim_v2_fill_push_mult,
            private_fill_p50_ms: bt.sim_v2_private_fill_p50_ms,
            private_fill_p95_ms: bt.sim_v2_private_fill_p95_ms,
            private_fill_p99_ms: bt.sim_v2_private_fill_p99_ms,
            matched_cant_cancel_window_ns: bt
                .sim_matched_cant_cancel_window_ms
                .saturating_mul(1_000_000),
            per_event_rtt: per_event_rtt_table.clone(),
            taker_overhead_p50_ms: tovh_p50,
            taker_overhead_p95_ms: tovh_p95,
            taker_overhead_p99_ms: tovh_p99,
            dynamic_taker_overhead_by_event,
            maker_race_rate: bt.sim_v2_maker_race_rate,
            taker_race_rate: bt.sim_v2_taker_race_rate,
            order_queue_position_strength: bt.sim_v2_order_queue_position_strength,
            maker_toxicity_strength: bt.sim_v2_maker_toxicity_strength,
            maker_toxicity_scale_ticks: bt.sim_v2_maker_toxicity_scale_ticks,
            maker_race_horizon_ns: bt.sim_v2_maker_race_horizon_ms.saturating_mul(1_000_000),
            taker_race_horizon_ns: bt.sim_v2_taker_race_horizon_ms.saturating_mul(1_000_000),
            fold_outcomes: bt.sim_v2_fold_outcomes,
            book_stale_after_ns: bt.sim_v2_book_stale_after_ms.saturating_mul(1_000_000),
            causal_matching: bt.sim_v2_causal_matching,
            stale_resting_exchange_only: bt.sim_v2_stale_resting_exchange_only,
            taker_comp_rate: bt.sim_v2_taker_comp_rate,
            taker_comp_window_ns: bt.sim_v2_taker_comp_window_ms.saturating_mul(1_000_000),
            taker_overlap_dedup: bt.sim_v2_taker_overlap_dedup,
            dynamic_window_rtt_by_event,
            dynamic_window_rtt_ref_ms: bt.sim_v2_dynamic_window_rtt_ref_ms,
            dynamic_race_rtt_elasticity: bt.sim_v2_dynamic_race_rtt_elasticity,
            dynamic_comp_rtt_elasticity: bt.sim_v2_dynamic_comp_rtt_elasticity,
            dynamic_window_min_mult: bt.sim_v2_dynamic_window_min_mult,
            dynamic_window_max_mult: bt.sim_v2_dynamic_window_max_mult,
            deep_queue_decay: bt.sim_v2_deep_queue_decay,
            dynamic_deep_queue_strength: bt.sim_v2_dynamic_deep_queue_strength,
            dynamic_deep_queue_min_decay: bt.sim_v2_dynamic_deep_queue_min_decay,
            // Mirror the polymarket exchange's batch flag so the sim splits
            // reprice cancels onto the cancel RTT when batching is off (the
            // live config sets use_batch_orders=false).
            use_batch_orders: self
                .config
                .exchanges
                .iter()
                .find(|e| e.name == "polymarket")
                .map(|e| e.use_batch_orders)
                .unwrap_or(true),
            // Record-replay place/cancel profiles (Some only when
            // sim_latency_calibrate_from is a directory); None → scalar CDF.
            place_profile,
            cancel_profile,
        })?;
        sim.configure_maker_order_audit(bt.sim_v2_fill_audit);

        info!(
            "[Backtest v2] {} strat replayers, {} bar events",
            strat_replayers.len(),
            bar_rows.len()
        );

        // ── k-way merge: strat lane (local_ts) + bars + sim (wall clock) ──
        #[derive(Eq, PartialEq)]
        struct HeapEntry {
            ts: u64,
            idx: usize,
        }
        impl Ord for HeapEntry {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                other.ts.cmp(&self.ts)
            }
        }
        impl PartialOrd for HeapEntry {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        let mut strat_heap: BinaryHeap<HeapEntry> = BinaryHeap::new();
        for (i, p) in strat_peeked.iter().enumerate() {
            if let Some((ts, _)) = p {
                strat_heap.push(HeapEntry { ts: *ts, idx: i });
            }
        }

        let mut strat_clock_ns: u64 = 0;
        let mut sim_clock_ns: u64 = 0;

        loop {
            let strat_ts = strat_heap.peek().map(|e| e.ts).unwrap_or(u64::MAX);
            let bar_ts = bar_rows
                .get(bar_cursor)
                .map(|(_, r)| r.close_time_ns)
                .unwrap_or(u64::MAX);
            let strat_min = strat_ts.min(bar_ts);
            let sim_ts = sim.peek_when().unwrap_or(u64::MAX);
            let min_ts = strat_min.min(sim_ts);
            if min_ts == u64::MAX {
                break;
            }

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
                let (ts, event) = if min_ts == bar_ts && bar_cursor < bar_rows.len() {
                    // Rebuild the full BarData from the compact row (the
                    // former code cloned a pre-built MarketEvent here).
                    let (src, row) = bar_rows[bar_cursor];
                    bar_cursor += 1;
                    (
                        row.close_time_ns,
                        MarketEvent::Bar(row.to_bar(&bar_templates[src as usize])),
                    )
                } else {
                    let entry = strat_heap.pop().unwrap();
                    let best_idx = entry.idx;
                    let pair = strat_peeked[best_idx].take().unwrap();
                    strat_peeked[best_idx] = strat_replayers[best_idx].next_event().ok().flatten();
                    if let Some((ts, _)) = &strat_peeked[best_idx] {
                        strat_heap.push(HeapEntry {
                            ts: *ts,
                            idx: best_idx,
                        });
                    }
                    pair
                };
                strat_clock_ns = ts;
                set_sim_clock(strat_clock_ns);

                if let MarketEvent::OrderBook(ob) = &event {
                    sim.observe_local_orderbook(ob, ts);
                    sim.observe_dynamic_markout_spot_book(ob, ts);
                }

                for (i, strategy) in strategies.iter_mut().enumerate() {
                    let signals = match &event {
                        MarketEvent::OrderBook(ob) => {
                            strategy.on_orderbook(ob);
                            Vec::new()
                        }
                        MarketEvent::Trade(t) => {
                            strategy.on_trade_tick(t);
                            Vec::new()
                        }
                        // Quote / SpotPrice update internal state only — the
                        // quote cadence is driven exclusively by OrderBook
                        // events (see the OrderBook trigger block below).
                        MarketEvent::Quote(q) => {
                            strategy.on_quote_tick(q);
                            Vec::new()
                        }
                        MarketEvent::Bar(b) => {
                            strategy.on_bar(b);
                            Vec::new()
                        }
                        MarketEvent::SpotPrice(sp) => {
                            strategy.on_spot_price(sp);
                            Vec::new()
                        }
                        MarketEvent::Instrument(inst) => {
                            // Hist gap-fill BEFORE on_instrument (matches v1).
                            let hist_reqs = strategy.load_hist_data(ts);
                            for req in &hist_reqs {
                                // Streamed (2026-07-26): identical bar sequence to
                                // the former collect-then-feed, but a 29d 1s
                                // refetch no longer materializes a ~430 MB Vec.
                                // Errors fire before the first emitted bar, so the
                                // fed-nothing-on-error behavior is preserved.
                                if let Err(e) = crate::recorder::load_hist_bars_streamed(
                                    &hist_data_dir,
                                    req,
                                    &mut |bar| {
                                        strategy.on_hist_bar(bar);
                                    },
                                ) {
                                    warn!("[Strategy v2] Failed to load hist bars: {}", e);
                                }
                            }
                            if !hist_reqs.is_empty() {
                                strategy.on_hist_data_loaded(ts);
                            }
                            // Per-event prev_p RTT-gate override (sim_rtt_mode=exact):
                            // forward live's prev_event place-p60 (× overhead factor)
                            // so the gate's N matches live at this event start.
                            if let (
                                Some(ref table),
                                crate::types::instrument::Instrument::BinaryOption(bo),
                            ) = (per_event_rtt_table.as_ref(), inst)
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
                        MarketEvent::TickSizeChange(tsc) => strategy.on_tick_size_change(tsc),
                        MarketEvent::Connected { exchange } => {
                            strategy.on_connected(*exchange);
                            Vec::new()
                        }
                        MarketEvent::Disconnected { exchange, reason } => {
                            strategy.on_disconnected(*exchange, reason);
                            Vec::new()
                        }
                        MarketEvent::MarketDataHealth(health) => {
                            strategy.on_market_data_health(health)
                        }
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
                        let tbt = strategy.quote_tick_by_tick() && !strategy.cadence_rtt_throttle();
                        if venue_ok && (tbt || interval > 0) {
                            let fire = if tbt {
                                true
                            } else {
                                let frac = strategy.quote_interval_tolerance_frac().clamp(0.0, 1.0);
                                let threshold_ns =
                                    ((interval as f64) * 1_000_000.0 * (1.0 - frac)) as u64;
                                ts.saturating_sub(last_quote_ns[i]) >= threshold_ns
                            };
                            if fire {
                                last_quote_ns[i] = ts;
                                quote_signal_batch.clear();
                                strategy.on_quote_into(ts, &mut quote_signal_batch);
                                stamp_quote_trigger(&mut quote_signal_batch, ob, false);
                                for sig in quote_signal_batch.drain(..) {
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

            // Synthetic RTT-probe emit (PROBE recovery), carried over from the removed v1 sim engine.
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
        info!(
            "  Sim v2:   trade-ts reconstruction: {} anchored, {} fallback (no prior book)",
            anchored, fallback
        );
        let (taker_fills, maker_fills, rejects) = sim.core_stats();
        info!(
            "  Sim v2:   taker_fills={}  maker_fills={}  rejects={}",
            taker_fills, maker_fills, rejects
        );
        let (rj_tb, rj_ts, rj_rb, rj_rs, rj_rs_short) = sim.reject_breakdown();
        info!(
            "  Sim v2:   reject reasons: taker_buy={} taker_sell={} rest_buy={} rest_sell={} (rest_sell short Σ={:.0} shares, mean={:.1})",
            rj_tb,
            rj_ts,
            rj_rb,
            rj_rs,
            rj_rs_short,
            if rj_rs > 0 {
                rj_rs_short / rj_rs as f64
            } else {
                0.0
            }
        );
        let (self_clean_n, self_clean_qty) = sim.replay_self_taker_stats();
        info!(
            "  Sim v2:   replay self-depth taker cleaning: sweeps={} removed_depth={:.4}",
            self_clean_n, self_clean_qty
        );
        for s in &self.config.strategies {
            if s.enabled
                && self.registry.capabilities(&s.name).needs_sim_wallet
                && !s.instance_id.is_empty()
            {
                if let Some(bal) = sim.wallet_usdc(&s.instance_id) {
                    let seed = s
                        .params
                        .get("init_balance")
                        .and_then(|v| v.as_float().or_else(|| v.as_integer().map(|i| i as f64)))
                        .unwrap_or(0.0);
                    info!(
                        "  Sim v2:   gating-wallet USDC [{}]: final={:.2} (seeded={:.2}, net={:.2}) — settlement-aware (split cost debited at seed, payouts credited at retire); net ≈ in-flight seed float, NOT a bleed",
                        s.instance_id,
                        bal,
                        seed,
                        bal - seed
                    );
                }
            }
        }
        let (timeouts, matched_cant_cancel) = sim.timeout_stats();
        info!(
            "  Sim v2:   timeouts={}  matched_cant_cancel={}",
            timeouts, matched_cant_cancel
        );
        let (finality_delayed, finality_matched) = sim.cancel_finality_stats();
        if finality_delayed > 0 {
            info!(
                "  Sim v2:   cancel_finality delayed={} matched_before_finality={}",
                finality_delayed, finality_matched
            );
        }
        let (po_rejects, po_seen) = sim.post_only_stats();
        info!(
            "  Sim v2:   post_only_rejects={}/{} ({:.2}% cross at reach)",
            po_rejects,
            po_seen,
            if po_seen > 0 {
                100.0 * po_rejects as f64 / po_seen as f64
            } else {
                0.0
            }
        );
        let (stale_orders, stale_trades, stale_exchange, stale_local, stale_rebases) =
            sim.book_stale_stats();
        if bt.sim_v2_book_stale_after_ms > 0 {
            info!(
                "  Sim v2:   book stale gate={}ms order_blocks={} trade_blocks={} exchange_hits={} local_hits={} recovery_rebases={}",
                bt.sim_v2_book_stale_after_ms,
                stale_orders,
                stale_trades,
                stale_exchange,
                stale_local,
                stale_rebases,
            );
        }
        let (mean_age_ms, over1s, mean_life_ms) = sim.fill_timing_stats();
        info!(
            "  Sim v2:   maker fill-age mean={:.0}ms  >1s={:.1}%  | removed-order lifetime mean={:.0}ms",
            mean_age_ms,
            100.0 * over1s,
            mean_life_ms
        );
        let (race_infl, race_plc, race_ratio, taker_capped, taker_capped_zero) = sim.race_stats();
        if race_plc > 0 {
            info!(
                "  Sim v2:   maker race inflated {}/{} ({:.1}%) placements, mean q_ahead×{:.2}",
                race_infl,
                race_plc,
                100.0 * race_infl as f64 / race_plc as f64,
                race_ratio
            );
        }
        // Report taker-race caps independently of maker-race (the two are
        // separate knobs; gating this on maker placements hid taker-race effect).
        if taker_capped > 0 {
            let zero_pct = 100.0 * taker_capped_zero as f64 / taker_capped as f64;
            info!(
                "  Sim v2:   taker race capped {} fills ({} to ~0 = full miss, {:.1}%)",
                taker_capped, taker_capped_zero, zero_pct
            );
        }
        let adv_adv = sim.adverse_advanced();
        if adv_adv > 0 {
            info!(
                "  Sim v2:   adverse-sel tilt advanced queue on {} resyncs (cancel-attribution ahead_frac→1 on adverse mid moves)",
                adv_adv
            );
        }
        let (own_positioned, own_initial, own_cancel_n, own_cancel_qty) =
            sim.own_queue_position_stats();
        if own_positioned > 0 || own_cancel_n > 0 {
            info!(
                "  Sim v2:   own FIFO positioned_orders={} initial_ahead_qty={:.4} cancel_advances={} cancel_advance_qty={:.4}",
                own_positioned, own_initial, own_cancel_n, own_cancel_qty
            );
        }
        let (toxicity_n, toxicity_qty) = sim.maker_toxicity_stats();
        if toxicity_n > 0 {
            info!(
                "  Sim v2:   causal maker toxicity suppressed_fragments={} suppressed_qty={:.4} (fills stay at real limit)",
                toxicity_n, toxicity_qty
            );
        }
        if let Some((n, mean, min, max)) = sim.dynamic_ahead_frac_stats() {
            info!(
                "  Sim v2:   dynamic ahead_frac cancel-resyncs={} mean={:.3} range=[{:.3},{:.3}]",
                n, mean, min, max
            );
        }
        let bt_fills = sim.book_through_fills();
        if bt_fills > 0 {
            info!(
                "  Sim v2:   book-through adverse fills: {} (resting orders the contra swept through → picked off)",
                bt_fills
            );
        }
        let depletion_fills = sim.unexplained_depletion_fills();
        if depletion_fills > 0 {
            info!(
                "  Sim v2:   unexplained-depletion maker fills: {} (residual L2 shrinkage treated as hidden execution)",
                depletion_fills
            );
        }
        let (residual_orders, residual_qty) = sim.inferred_maker_residual_stats();
        if residual_orders > 0 {
            info!(
                "  Sim v2:   inferred maker residuals: orders={} retained_qty={:.6} (exchange-side dust remains after 99%-coverage release)",
                residual_orders, residual_qty
            );
        }
        let hc = sim.fill_haircuts();
        if hc > 0 {
            info!(
                "  Sim v2:   forward-markout haircuts: {} favorable maker fills downweighted (markout → live −0.75c)",
                hc
            );
        }
        let (book_hc_n, book_hc_qty, book_hc_cost) = sim.book_fill_markout_stats();
        if book_hc_n > 0 {
            info!(
                "  Sim v2:   book-fill markout haircuts: fragments={} qty={:.4} settlement_cost_usdc={:.4} (depletion/book-through, volume preserved)",
                book_hc_n, book_hc_qty, book_hc_cost
            );
        }
        let (mq, tv) = sim.depth_distributions();
        info!(
            "  Sim v2:   maker q_init (shares ahead at placement) n={:.0} mean={:.1} | p10={:.0} p25={:.0} p50={:.0} p75={:.0} p90={:.0} p99={:.0} | zero-queue={:.1}%",
            mq[0],
            mq[1],
            mq[2],
            mq[3],
            mq[4],
            mq[5],
            mq[6],
            mq[7],
            100.0 * mq[8]
        );
        info!(
            "  Sim v2:   taker avail-vol (fillable within limit at match) n={:.0} mean={:.1} | p10={:.0} p25={:.0} p50={:.0} p75={:.0} p90={:.0} p99={:.0} | zero={:.1}%",
            tv[0],
            tv[1],
            tv[2],
            tv[3],
            tv[4],
            tv[5],
            tv[6],
            tv[7],
            100.0 * tv[8]
        );
        let pb = sim.placement_buckets();
        let ptot: u64 = pb.iter().map(|b| b[0]).sum::<u64>().max(1);
        let pnames = [
            "improve(inside/new-best)",
            "join(==best)",
            "behind(deeper)",
            "no-book-this-side",
        ];
        info!("  Sim v2:   maker placement price-vs-BBO (why q_init=0):");
        for (b, nm) in pb.iter().zip(pnames) {
            let q0pct = if b[0] > 0 {
                100.0 * b[1] as f64 / b[0] as f64
            } else {
                0.0
            };
            info!(
                "  Sim v2:     {:<26} {:>7} ({:>4.1}% of placements)  q_init=0 in {:>5.1}%",
                nm,
                b[0],
                100.0 * b[0] as f64 / ptot as f64,
                q0pct
            );
        }
        let (q0_extra, q0_best) = sim.q0_fallback_split();
        info!(
            "  Sim v2:     q_init=0 resolved by: extrapolation(beyond-window)={} | best-level rule(in-window gap)={}",
            q0_extra, q0_best
        );
        if let Some((n, mean, min, max)) = sim.dynamic_deep_queue_stats() {
            info!(
                "  Sim v2:   dynamic deep-queue extrapolations={} mean decay={:.3} range=[{:.3},{:.3}]",
                n, mean, min, max
            );
        }
        let (tcc, tccz, tc_mean) = sim.taker_comp_stats();
        if tcc > 0 {
            let zpct = 100.0 * tccz as f64 / tcc as f64;
            info!(
                "  Sim v2:   taker trade-flow competition capped {} fills ({} to ~0 = full miss, {:.1}%) | mean competing vol={:.1}",
                tcc, tccz, zpct, tc_mean
            );
        }
        if let Some((n, mean_rtt, mean_race, mean_comp, min_mult, max_mult)) =
            sim.dynamic_window_stats()
        {
            info!(
                "  Sim v2:   dynamic taker windows events={} | RTT state mean={:.2}ms | race mean={:.0}ms comp mean={:.0}ms | multiplier range=[{:.3},{:.3}]",
                n, mean_rtt, mean_race, mean_comp, min_mult, max_mult
            );
        }
        if let Some((n, p50, p95, p99)) = sim.dynamic_taker_overhead_stats() {
            info!(
                "[Backtest v2] dynamic taker overhead applied: n={} mean p50/p95/p99={:.1}/{:.1}/{:.1} ms",
                n, p50, p95, p99
            );
        }
        if let Some(s) = sim.dynamic_markout_stats() {
            info!(
                "  Sim v2:   dynamic markout state n={:.0} mean={:.2} {} p50/p75/p90/p99={:.2}/{:.2}/{:.2}/{:.2} | vn mean={:.3} range=[{:.3},{:.3}]",
                s[0],
                s[1],
                sim.dynamic_markout_state_unit(),
                s[2],
                s[3],
                s[4],
                s[5],
                s[6],
                s[7],
                s[8]
            );
        }
        if bt.sim_v2_fill_audit {
            for a in sim.fill_audit_rows() {
                info!(
                    "  Sim v2 fill audit event: slug={} iid={} place_n={} place_qty={:.4} passive_place_n={} passive_place_qty={:.4} cancel_before_place_n={} cancel_before_place_qty={:.4} stale_order_n={} stale_order_qty={:.4} po_reject_n={} po_reject_qty={:.4} maker_rest_n={} maker_rest_qty={:.4} passive_rest_n={} passive_rest_qty={:.4} maker_rest_s={:.6} maker_rest_qty_s={:.6} passive_rest_s={:.6} passive_rest_qty_s={:.6} maker_cancel_n={} maker_cancel_qty={:.4} maker_fill_order_n={} maker_open_n={} passive_cancel_n={} passive_cancel_qty={:.4} passive_fill_order_n={} passive_fill_qty={:.4} passive_open_n={} maker_q_init_sum={:.4} maker_own_q_init_sum={:.4} maker_own_cancel_queue_advance_qty={:.4} maker_race_added_q={:.4} maker_replay_self_depth_credit={:.4} maker_trade_match_n={} maker_trade_qty={:.4} maker_queue_drained_qty={:.4} maker_candidate_qty={:.4} maker_toxicity_suppressed_qty={:.4} maker_depletion_observed_qty={:.4} maker_depletion_exec_qty={:.4} maker_depletion_cancel_advance_qty={:.4} maker_depletion_candidate_qty={:.4} maker_depletion_budget_suppressed_qty={:.4} maker_depletion_fill_qty={:.4} maker_book_markout_qty={:.4} maker_book_markout_cost_usdc={:.4} maker_fill_qty={:.4} stale_trade_match_n={} stale_trade_candidate_qty={:.4} taker_candidate_n={} taker_requested_qty={:.4} taker_available_qty={:.4} taker_replay_self_depth_qty={:.4} taker_race_suppressed_qty={:.4} taker_comp_suppressed_qty={:.4} taker_zero_n={} taker_fill_qty={:.4}",
                    a.slug,
                    a.iid,
                    a.place_orders,
                    a.place_qty,
                    a.passive_place_orders,
                    a.passive_place_qty,
                    a.cancel_before_place_orders,
                    a.cancel_before_place_qty,
                    a.stale_order_blocks,
                    a.stale_order_qty,
                    a.post_only_rejects,
                    a.post_only_reject_qty,
                    a.maker_rests,
                    a.maker_rest_qty,
                    a.passive_rests,
                    a.passive_rest_qty,
                    a.maker_rest_time_ns as f64 / 1_000_000_000.0,
                    a.maker_rest_qty_ns / 1_000_000_000.0,
                    a.passive_rest_time_ns as f64 / 1_000_000_000.0,
                    a.passive_rest_qty_ns / 1_000_000_000.0,
                    a.maker_cancel_orders,
                    a.maker_cancel_qty,
                    a.maker_orders_with_fill,
                    a.maker_open_orders,
                    a.passive_cancel_orders,
                    a.passive_cancel_qty,
                    a.passive_orders_with_fill,
                    a.passive_fill_qty,
                    a.passive_open_orders,
                    a.maker_q_init_sum,
                    a.maker_own_q_init_sum,
                    a.maker_own_cancel_queue_advance_qty,
                    a.maker_race_added_q,
                    a.maker_replay_self_depth_credit,
                    a.maker_trade_matches,
                    a.maker_trade_qty,
                    a.maker_queue_drained_qty,
                    a.maker_candidate_qty,
                    a.maker_toxicity_suppressed_qty,
                    a.maker_depletion_observed_qty,
                    a.maker_depletion_exec_qty,
                    a.maker_depletion_cancel_advance_qty,
                    a.maker_depletion_candidate_qty,
                    a.maker_depletion_budget_suppressed_qty,
                    a.maker_depletion_fill_qty,
                    a.maker_book_markout_qty,
                    a.maker_book_markout_cost_usdc,
                    a.maker_fill_qty,
                    a.stale_trade_matches,
                    a.stale_trade_candidate_qty,
                    a.taker_candidates,
                    a.taker_requested_qty,
                    a.taker_available_qty,
                    a.taker_replay_self_depth_qty,
                    a.taker_race_suppressed_qty,
                    a.taker_comp_suppressed_qty,
                    a.taker_zero_fills,
                    a.taker_fill_qty,
                );
            }
            for a in sim.maker_order_audit_rows() {
                info!(
                    "  Sim v2 maker order audit: slug={} iid={} coid={} token={} side={} order_type={:?} price={} quantity={} post_only={} strategy_emit_ns={} trigger_exchange_ns={} trigger_local_ns={} place_arrival_ns={} rest_ms={:.6} rest_qty_s={:.6} await_fresh_book={} visible_depth_at_entry={:.4} entry_mid={:.6} queue_seq={} q_init={:.4} simulated_own_ahead_qty={:.4} own_cancel_queue_advance_qty={:.4} replay_self_depth_credit={:.4} trade_match_n={} trade_match_qty={:.4} queue_drained_qty={:.4} candidate_qty={:.4} maker_toxicity_suppressed_qty={:.4} depletion_observed_qty={:.4} depletion_exec_qty={:.4} depletion_cancel_advance_qty={:.4} depletion_candidate_qty={:.4} depletion_budget_suppressed_qty={:.4} depletion_fill_qty={:.4} inferred_residual_floor={:.6} inferred_residual_suppressed_qty={:.6} book_through_candidate_qty={:.4} book_through_fill_qty={:.4} book_markout_qty={:.4} book_markout_cost_usdc={:.4} fill_qty={:.4} first_fill_ns={} last_fill_ns={} first_fill_delivery_ns={} last_fill_delivery_ns={} cancel_arrival_ns={} cancel_result={} q_ahead_final={:.4} remaining_final={:.4}",
                    a.slug,
                    a.iid,
                    a.coid,
                    a.token,
                    a.side,
                    a.order_type,
                    a.price,
                    a.quantity,
                    a.post_only,
                    a.strategy_emit_ns,
                    a.trigger_exchange_ns,
                    a.trigger_local_ns,
                    a.place_arrival_ns,
                    a.rest_time_ns as f64 / 1_000_000.0,
                    a.rest_qty_ns / 1_000_000_000.0,
                    a.await_fresh_book,
                    a.visible_depth_at_entry,
                    a.entry_mid,
                    a.queue_seq,
                    a.q_init,
                    a.simulated_own_ahead_qty,
                    a.own_cancel_queue_advance_qty,
                    a.replay_self_depth_credit,
                    a.trade_match_n,
                    a.trade_match_qty,
                    a.queue_drained_qty,
                    a.candidate_qty,
                    a.maker_toxicity_suppressed_qty,
                    a.depletion_observed_qty,
                    a.depletion_exec_qty,
                    a.depletion_cancel_advance_qty,
                    a.depletion_candidate_qty,
                    a.depletion_budget_suppressed_qty,
                    a.depletion_fill_qty,
                    a.inferred_residual_floor,
                    a.inferred_residual_suppressed_qty,
                    a.book_through_candidate_qty,
                    a.book_through_fill_qty,
                    a.book_markout_qty,
                    a.book_markout_cost_usdc,
                    a.fill_qty,
                    a.first_fill_ns,
                    a.last_fill_ns,
                    a.first_fill_delivery_ns,
                    a.last_fill_delivery_ns,
                    a.cancel_arrival_ns,
                    a.cancel_result,
                    a.q_ahead_final,
                    a.remaining_final,
                );
            }
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
    /// book live); the queue/taker/book-through/depletion/fold knobs are mirrored
    /// from the backtest config so paper fills track calibrated backtest behaviour.
    fn spawn_paper_execution_thread(
        signal_rx: Receiver<RoutedSignal>,
        sim_feed_rx: Receiver<MarketEvent>,
        update_tx: Sender<OrderUpdate>,
        sim_latency_ms: u64,
        bt: crate::config::BacktestConfig,
        shutdown_done_tx: Sender<()>,
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
                sim.configure_unexplained_depletion_execution(
                    bt.sim_v2_unexplained_depletion_exec_rate,
                );
                sim.configure_inferred_maker_residual(
                    bt.sim_v2_inferred_maker_residual_rate,
                    bt.sim_v2_inferred_maker_residual_fraction,
                );
                sim.configure_replay_self_depth(bt.sim_v2_replay_self_depth_rate);
                sim.configure_replay_self_taker_depth(
                    bt.sim_v2_replay_self_taker_depth_rate,
                );
                sim.set_fold_outcomes(bt.sim_v2_fold_outcomes);
                sim.configure_book_stale_gate(
                    bt.sim_v2_book_stale_after_ms.saturating_mul(1_000_000),
                );
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
                                    // v2 `on_orderbook` returns causal maker
                                    // fills from unexplained depletion and/or a
                                    // trade-confirmed book-through sweep.
                                    let fills = sim.on_orderbook(ob);
                                    sim.on_local_orderbook(ob, ob.local_timestamp_ns);
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
                            let msg = msg.map(|routed| routed.signal);
                            // Simulate network latency: signal → exchange
                            if latency.as_millis() > 0 {
                                std::thread::sleep(latency);
                            }
                            let mut updates = Vec::new();
                            // Paper mode runs at wall-clock — pass `now_ns()`
                            // as the sim clock so cancel timestamps and the
                            // matched-cant-cancel age check live on real time.
                            let sim_now = crate::types::now_ns();
                            let mut acknowledge_shutdown = false;
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
                                Ok(Signal::BatchUpdateOrders { exchange, ref cancel_client_order_ids, ref place_orders, .. })
                                | Ok(Signal::ReplaceOrder { exchange, ref cancel_client_order_ids, ref place_orders, .. }) => {
                                    // Places before cancels — same rationale as the
                                    // BT main-loop branch: gives `submit_order` a
                                    // realistic view of resting orders for queue /
                                    // cascade-cancel / synthetic-balance-error paths.
                                    // ReplaceOrder is handled identically to
                                    // BatchUpdateOrders in the sim fill path.
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
                                Ok(Signal::RetainPolymarketEventAudit { .. })
                                | Ok(Signal::RetirePolymarketEventAudit { .. }) => {
                                    // Paper execution has no durable exchange
                                    // audit history to retire.
                                }
                                Ok(Signal::PolymarketCancelAllOrders { ref reason, ref market, .. }) => {
                                    if is_routine_expiry_cancel(reason, market.is_some()) {
                                        info!("[PaperExec] PolymarketCancelAllOrders: reason={}", reason);
                                    } else {
                                        warn!("[PaperExec] PolymarketCancelAllOrders: reason={}", reason);
                                    }
                                    updates.extend(sim.cancel_all(Exchange::Polymarket, "", sim_now));
                                }
                                Ok(Signal::BeginShutdown) => {
                                    info!("[PaperExec] Beginning coordinated shutdown cancel barrier");
                                    // Public feeds observe the shutdown flag
                                    // before the strategy emits this signal.
                                    // Apply anything already buffered so a
                                    // pre-shutdown fill cannot land after the
                                    // simulated cancel acknowledgement.
                                    loop {
                                        let event = match sim_feed_rx.recv_timeout(
                                            std::time::Duration::from_millis(20),
                                        ) {
                                            Ok(event) => event,
                                            Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                                            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                                        };
                                        match event {
                                            MarketEvent::OrderBook(ref ob) => {
                                                updates.extend(sim.on_orderbook(ob));
                                                sim.on_local_orderbook(ob, ob.local_timestamp_ns);
                                            }
                                            MarketEvent::Trade(ref trade) => {
                                                updates.extend(sim.on_trade_tick(trade));
                                            }
                                            MarketEvent::TickSizeChange(ref change) => {
                                                sim.on_tick_size_change(change);
                                            }
                                            MarketEvent::Instrument(ref instrument) => {
                                                sim.on_instrument(instrument);
                                            }
                                            _ => {}
                                        }
                                    }
                                    updates.extend(sim.cancel_all(Exchange::Polymarket, "", sim_now));
                                    acknowledge_shutdown = true;
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
                            if acknowledge_shutdown {
                                let _ = shutdown_done_tx.send(());
                            }
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
            if !cfg.enabled {
                continue;
            }
            // Per-source symbol derives from the strategy's event_series_slug.
            let asset = cfg
                .params
                .get("event_series_slug")
                .and_then(|v| v.as_str())
                .and_then(crate::config::derive_asset_symbols)
                .map(|s| s.asset);
            if let Some(arr) = cfg
                .params
                .get("prediction_sources")
                .and_then(|v| v.as_array())
            {
                for item in arr {
                    if let Some(t) = item.as_table() {
                        let ex = t
                            .get("exchange")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        // Slug-derived (polymaker) if event_series_slug is set;
                        // else fall back to an explicit `symbol` (hexmaker/legacy).
                        let sym = match asset.as_deref() {
                            Some(a) => crate::config::venue_symbol(a, &ex),
                            None => t
                                .get("symbol")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                        };
                        if !ex.is_empty() && !sym.is_empty() {
                            sources.push((ex, sym));
                        }
                    }
                }
            }
            let hours = cfg
                .params
                .get("prediction_training_period_hours")
                .and_then(|v| v.as_float().or_else(|| v.as_integer().map(|i| i as f64)))
                .unwrap_or(24.0);
            if hours > max_hours {
                max_hours = hours;
            }
        }
        // Order-preserving set dedup. `Vec::dedup` only drops ADJACENT
        // duplicates, but multi-strategy sources are interleaved per-strategy
        // (two BTC instances → [(bn,BTC),(cb,BTC),(bn,BTC),(cb,BTC)]), so the
        // adjacent-only pass keeps every duplicate. The warm-up replay would
        // then build N replayers per (exchange,symbol) and feed each spot event
        // to every strategy's apv2 N times — inflating the apv2 warm-up activity
        // baseline N× versus the 1× live path (market-router single-feed). That
        // scale mismatch biases the run-period z-score / S_C3 negative → over-
        // skip. Affects multi-instance live only (single-instance / all BT have
        // one source set, so this is byte-identical for them).
        let mut seen = std::collections::HashSet::new();
        sources.retain(|s| seen.insert(s.clone()));
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
            if !cfg.enabled {
                continue;
            }
            let v2_on = cfg
                .params
                .get("adaptive_params_v2_enabled")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if !v2_on {
                continue;
            }
            let on = cfg
                .params
                .get("apv2_warmup")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if !on {
                continue;
            }
            // full lookback. Default z_window = 2016 (7d).
            let zwin = cfg
                .params
                .get("apv2_z_window")
                .and_then(|v| v.as_integer())
                .map(|i| i.max(2) as f64)
                .unwrap_or(2016.0);
            let days = zwin / BUCKETS_PER_DAY;
            if days > max_days {
                max_days = days;
            }
        }
        max_days
    }

    fn spawn_strategy_thread(
        &self,
        market_rx: PublicMarketReceiver,
        signal_tx: SignalSender,
        update_rx: Receiver<OrderUpdate>,
        mut executor_update_rx: Option<Receiver<RoutedOrderUpdate>>,
        backtest: bool,
        recorder_tx: Option<Sender<Arc<MarketEvent>>>,
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
        shutdown_done_rx: Option<Receiver<()>>,
    ) -> thread::JoinHandle<()> {
        let mut strategies =
            self.build_strategies(rtt_probe_install, stale_threshold_handles, poly_states);
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
        // Detect whether live recordings and canonical warm-up data share a
        // root. A split root is valid for a dedicated calibration archive;
        // prediction warm-up continues to read only `backtest.data_dir`.
        if self.config.general.mode == RunMode::Live {
            let recorder_out = PathBuf::from(&self.config.recording.output_dir);
            let recorder_out_canon =
                std::fs::canonicalize(&recorder_out).unwrap_or_else(|_| recorder_out.clone());
            let unified = data_dirs.iter().any(|d| {
                let dc = std::fs::canonicalize(d).unwrap_or_else(|_| d.clone());
                dc == recorder_out_canon
            });
            if unified {
                log::info!(
                    "[Strategy] Live mode: unified storage detected \
                     (backtest.data_dir == recording.output_dir == {}). \
                     Warm-up will use this dir for external spot/index history.",
                    recorder_out.display(),
                );
            } else {
                log::info!(
                    "[Strategy] Live mode: dedicated recorder storage — \
                     backtest.data_dir={} ≠ recording.output_dir={}. \
                     prediction warm-up remains on the canonical data root.",
                    self.config.backtest.data_dir,
                    self.config.recording.output_dir,
                );
            }
        }

        // ── Historical-bars preload: one fixed snapshot for all instances ──
        //
        // Previously each per-instance worker waited for its first Instrument
        // and called `load_hist_data(event.timestamp_ns())` independently.
        // Two otherwise-identical BTC instances therefore used slightly
        // different end timestamps and performed the same parquet/REST load
        // twice, producing different initial volatility fits. Capture the
        // anchor once, before any worker is spawned, load each exact raw-data
        // request once, and fan the immutable bars into every matching
        // strategy. A later freshness retry still follows the existing
        // Instrument path with the then-current event timestamp.
        let hist_end_ns = crate::types::now_ns();
        let hist_stats = preload_hist_bars(&mut strategies, &data_dirs, hist_end_ns);
        if hist_stats.requests > 0 {
            info!(
                "[Strategy] Historical preload complete: end_ns={} strategies={} requests={} unique_loads={} cache_hits={} failed={}",
                hist_end_ns,
                hist_stats.initialized_strategies,
                hist_stats.requests,
                hist_stats.unique_loads,
                hist_stats.cache_hits,
                hist_stats.failed_loads,
            );
        }

        // ── Prediction warm-up: done BEFORE spawning thread (before exchange feeds start) ──
        {
            let (warmup_sources, warmup_hours) = self.prediction_warmup_sources();
            if !warmup_sources.is_empty() && warmup_hours > 0.0 {
                let end = if backtest && !self.config.backtest.start_date.is_empty() {
                    chrono::DateTime::parse_from_rfc3339(&self.config.backtest.start_date)
                        .or_else(|_| {
                            chrono::NaiveDateTime::parse_from_str(
                                &self.config.backtest.start_date,
                                "%Y-%m-%dT%H:%M:%SZ",
                            )
                            .map(|ndt| ndt.and_utc().fixed_offset())
                        })
                        .map(|dt| dt.with_timezone(&chrono::Utc))
                        .unwrap_or_else(|_| chrono::Utc::now())
                } else {
                    chrono::Utc::now()
                };
                let start = end - chrono::TimeDelta::seconds((warmup_hours * 3600.0) as i64);
                info!(
                    "[Strategy] Prediction warm-up: loading {:.1}h of history for {} sources (end={})",
                    warmup_hours,
                    warmup_sources.len(),
                    end.format("%Y-%m-%d %H:%M")
                );
                // Tell strategies we're entering warm-up so they can suppress
                // per-hour retrains while samples stream in.
                for s in &mut strategies {
                    s.on_prediction_warmup_start();
                }
                for (exchange, symbol) in &warmup_sources {
                    // Try each data dir in order (primary then fallback)
                    let mut loaded = false;
                    for dir in &data_dirs {
                        match crate::recorder::MarketReplayer::new(
                            dir, exchange, symbol, start, end,
                        ) {
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
                                    info!(
                                        "[Strategy] Warm-up: {} events from {}/{} ({})",
                                        count,
                                        exchange,
                                        symbol,
                                        dir.display()
                                    );
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
                    forward_recorder_event(recorder_tx.as_ref(), &event);
                    for strategy in &mut strategies {
                        match &event {
                            MarketEvent::OrderBook(ob) => strategy.on_orderbook(ob),
                            MarketEvent::Trade(t) => strategy.on_trade_tick(t),
                            MarketEvent::Quote(q) => strategy.on_quote_tick(q),
                            MarketEvent::Bar(b) => strategy.on_bar(b),
                            MarketEvent::SpotPrice(sp) => strategy.on_spot_price(sp),
                            MarketEvent::AssetCtx(ac) => strategy.on_asset_ctx(ac),
                            MarketEvent::Instrument(inst) => {
                                // Mirror the live thread's Instrument
                                // handler — gap-fill hist bars FIRST so
                                // the strategy's vol_model is populated
                                // before on_instrument's per-event
                                // adaptive-params compute runs (same rationale
                                // as the live-mode Instrument handler —
                                // first event after restart was getting
                                // a cold-vol compute_for_event).
                                let ts_event = event.timestamp_ns();
                                let hist_reqs = strategy.load_hist_data(ts_event);
                                for req in &hist_reqs {
                                    for dir in &data_dirs {
                                        match crate::recorder::load_hist_bars(dir, req) {
                                            Ok(bars) if !bars.is_empty() => {
                                                for bar in &bars {
                                                    strategy.on_hist_bar(bar);
                                                }
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
                                // run its per-event adaptive-params
                                // compute against populated bars.
                                strategy.on_instrument(inst);
                            }
                            MarketEvent::Connected { exchange } => strategy.on_connected(*exchange),
                            MarketEvent::Disconnected { exchange, reason } => {
                                strategy.on_disconnected(*exchange, reason)
                            }
                            MarketEvent::MarketDataHealth(health) => {
                                let _ = strategy.on_market_data_health(health);
                            }
                            MarketEvent::TickSizeChange(tsc) => {
                                let _ = strategy.on_tick_size_change(tsc);
                            }
                            MarketEvent::EventStart { .. }
                            | MarketEvent::EventEnd { .. }
                            | MarketEvent::Exit => {}
                        }
                    }
                    drained += 1;
                    // Soft cap — prevents a pathological WS flood from
                    // blocking strategy startup indefinitely. One million
                    // events at ~30 bytes each is ~30 MB; if we see this
                    // something is very wrong upstream.
                    if drained >= 1_000_000 {
                        break;
                    }
                }
                if drained > 0 {
                    info!(
                        "[Strategy] Drained {} live events buffered during warm-up",
                        drained
                    );
                }

                // Warm-up done — strategies run a single final retrain and
                // resume normal per-hour retrain cadence.
                for s in &mut strategies {
                    s.on_prediction_warmup_end(crate::types::now_ns());
                }
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
                        .or_else(|_| {
                            chrono::NaiveDateTime::parse_from_str(
                                &self.config.backtest.start_date,
                                "%Y-%m-%dT%H:%M:%SZ",
                            )
                            .map(|ndt| ndt.and_utc().fixed_offset())
                        })
                        .map(|dt| dt.with_timezone(&chrono::Utc))
                        .unwrap_or_else(|_| chrono::Utc::now())
                } else {
                    chrono::Utc::now()
                };
                let aw_start = aw_end - chrono::TimeDelta::seconds((aw_days * 86400.0) as i64);
                // apv2 warm-up cache: import cached per-bucket baseline and
                // narrow the raw replay to the uncached tail gap (skips the
                // ~7d parquet read on a warm restart). Strategies without a
                // usable cache return aw_start ⇒ full replay (unchanged).
                let aw_start_ns = aw_start.timestamp_nanos_opt().unwrap_or(0).max(0) as u64;
                let aw_end_ns = aw_end.timestamp_nanos_opt().unwrap_or(0).max(0) as u64;
                let resume_ns = strategies
                    .iter_mut()
                    .map(|s| s.apv2_warmup_resume_ns(aw_start_ns, aw_end_ns))
                    .min()
                    .unwrap_or(aw_start_ns);
                let replay_start =
                    chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(resume_ns as i64);
                info!(
                    "[Strategy] apv2 warm-up: {:.1}d window [{} → {}], raw replay from {}",
                    aw_days,
                    aw_start.format("%Y-%m-%d %H:%M"),
                    aw_end.format("%Y-%m-%d %H:%M"),
                    replay_start.format("%Y-%m-%d %H:%M")
                );
                // Per source, pick the first data_dir that actually yields
                // events (mirrors the prediction warm-up's primary→fallback
                // selection), priming one buffered event each.
                let mut replayers: Vec<crate::recorder::MarketReplayer> = Vec::new();
                let mut peeked: Vec<Option<(u64, MarketEvent)>> = Vec::new();
                for (exchange, symbol) in &spot_sources {
                    for dir in &data_dirs {
                        if let Ok(mut r) = crate::recorder::MarketReplayer::new(
                            dir,
                            exchange,
                            symbol,
                            replay_start,
                            aw_end,
                        ) {
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
                    let best = peeked
                        .iter()
                        .enumerate()
                        .filter_map(|(i, p)| p.as_ref().map(|(ts, _)| (i, *ts)))
                        .min_by_key(|&(_, ts)| ts);
                    let Some((idx, _)) = best else {
                        break;
                    };
                    let (_, event) = peeked[idx].take().unwrap();
                    match &event {
                        MarketEvent::OrderBook(ob) => {
                            for s in &mut strategies {
                                s.on_apv2_warmup_orderbook(ob);
                            }
                        }
                        MarketEvent::Trade(t) => {
                            for s in &mut strategies {
                                s.on_apv2_warmup_trade(t);
                            }
                        }
                        _ => {}
                    }
                    fed += 1;
                    peeked[idx] = replayers[idx].next_event().ok().flatten();
                }
                info!("[Strategy] apv2 warm-up complete: {} spot events fed", fed);
                for s in &mut strategies {
                    s.apv2_warmup_finalize_cache();
                }

                // Drain live events buffered on `market_rx` during the apv2
                // warm-up replay above. The earlier prediction-warm-up drain
                // already emptied the buffer, but this apv2 replay runs AFTER
                // it and can take tens of seconds (a cache-miss full 7-day
                // replay; ~1s on a warm cache), so a fresh backlog of OBs /
                // Trades has queued up meanwhile. If we hand off to the live
                // thread now, the FIRST events it processes are these
                // seconds-old buffered ticks: myindex's per-component ts is
                // set to that stale value and the wall-clock staleness gate
                // (now_ns_for_myindex_gate is wall-clock in live/paper) fires
                // "component <ex> stale" + pauses quoting until the live loop
                // grinds through the backlog (observed: ~57s-old binance tick →
                // ~1.2k "Skipping quote" warns over the first 5m event). Catch
                // myindex / spot_price up to the freshest buffered tick HERE,
                // via the real market handlers, so the live thread starts
                // already-fresh. We feed market handlers only — never on_quote
                // — so no quoting/signal emission happens during the drain
                // (mirrors the prediction warm-up drain above).
                let mut drained = 0u64;
                while let Ok(event) = market_rx.try_recv() {
                    forward_recorder_event(recorder_tx.as_ref(), &event);
                    for strategy in &mut strategies {
                        match &event {
                            MarketEvent::OrderBook(ob) => strategy.on_orderbook(ob),
                            MarketEvent::Trade(t) => strategy.on_trade_tick(t),
                            MarketEvent::Quote(q) => strategy.on_quote_tick(q),
                            MarketEvent::Bar(b) => strategy.on_bar(b),
                            MarketEvent::SpotPrice(sp) => strategy.on_spot_price(sp),
                            MarketEvent::AssetCtx(ac) => strategy.on_asset_ctx(ac),
                            MarketEvent::Instrument(inst) => {
                                let ts_event = event.timestamp_ns();
                                let hist_reqs = strategy.load_hist_data(ts_event);
                                for req in &hist_reqs {
                                    for dir in &data_dirs {
                                        match crate::recorder::load_hist_bars(dir, req) {
                                            Ok(bars) if !bars.is_empty() => {
                                                for bar in &bars {
                                                    strategy.on_hist_bar(bar);
                                                }
                                                break;
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                if !hist_reqs.is_empty() {
                                    strategy.on_hist_data_loaded(ts_event);
                                }
                                strategy.on_instrument(inst);
                            }
                            MarketEvent::Connected { exchange } => strategy.on_connected(*exchange),
                            MarketEvent::Disconnected { exchange, reason } => {
                                strategy.on_disconnected(*exchange, reason)
                            }
                            MarketEvent::MarketDataHealth(health) => {
                                let _ = strategy.on_market_data_health(health);
                            }
                            MarketEvent::TickSizeChange(tsc) => {
                                let _ = strategy.on_tick_size_change(tsc);
                            }
                            MarketEvent::EventStart { .. }
                            | MarketEvent::EventEnd { .. }
                            | MarketEvent::Exit => {}
                        }
                    }
                    drained += 1;
                    if drained >= 1_000_000 {
                        break;
                    }
                }
                if drained > 0 {
                    info!(
                        "[Strategy] Drained {} live events buffered during apv2 warm-up",
                        drained
                    );
                }
            }
        }

        for s in &mut strategies {
            s.on_init();
        }

        // ── LIVE/PAPER per-instance workers (P1+P2) ──
        // Every live/paper strategy instance runs on its OWN watchdog-enabled
        // thread (pinned via `strategy_cores`), including a deployment with a
        // single enabled instance. The market router delivers each event only
        // to instances subscribing to its symbol. This preserves instance
        // ownership and ensures wall-clock safety/initialization work runs even
        // while every market-data source is silent.
        //
        // Backtest keeps the deterministic single-thread loop below.
        if should_spawn_per_instance_strategy_workers(backtest, strategies.len()) {
            return self.spawn_per_instance_strategy_threads(
                strategies,
                market_rx,
                signal_tx,
                update_rx,
                executor_update_rx.take(),
                recorder_tx,
                data_dirs,
                shutdown_done_rx,
            );
        }
        drop(executor_update_rx.take());

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
                let mut quote_signal_batch = Vec::with_capacity(32);
                let market_ready_rx = market_rx.ready_receiver().clone();

                loop {
                    crossbeam_channel::select! {
                        recv(market_ready_rx) -> _ready => {
                            let msg = market_rx.try_recv();
                            match msg {
                                Ok(MarketEvent::Exit) => {
                                    info!("[Strategy] Exit event received");
                                    for s in &mut strategies {
                                        s.on_exit();
                                    }
                                    forward_recorder_event(
                                        recorder_tx.as_ref(), &MarketEvent::Exit,
                                    );
                                    if backtest || shutdown_done_rx.is_none() {
                                        for s in &mut strategies {
                                            for sig in s.on_shutdown() { let _ = signal_tx.send(sig); }
                                        }
                                        let _ = signal_tx.send(Signal::Exit);
                                        return;
                                    }
                                    // Live/paper uses a two-phase shutdown:
                                    // final reports are delayed until all
                                    // cancels, order audits, and late trades
                                    // have flowed through normal accounting.
                                    if signal_tx.send(Signal::BeginShutdown).is_err() {
                                        warn!("[Strategy] executor disappeared before shutdown barrier");
                                    } else if let Some(done_rx) = shutdown_done_rx.as_ref() {
                                        loop {
                                            crossbeam_channel::select! {
                                                recv(update_rx) -> update => match update {
                                                    Ok(update) => {
                                                        for s in &mut strategies {
                                                            let _ = s.on_order_update(&update);
                                                        }
                                                    }
                                                    Err(_) => break,
                                                },
                                                recv(done_rx) -> _ => break,
                                            }
                                        }
                                        // The acknowledgement is sent after
                                        // updates are enqueued, but select may
                                        // observe the independent done channel
                                        // first. Drain that final tail.
                                        while let Ok(update) = update_rx.try_recv() {
                                            for s in &mut strategies {
                                                let _ = s.on_order_update(&update);
                                            }
                                        }
                                    }
                                    for s in &mut strategies {
                                        let _ = s.on_shutdown();
                                    }
                                    let _ = signal_tx.send(Signal::Exit);
                                    return;
                                }
                                Ok(event) => {
                                    // Record market data if recorder is active
                                    forward_recorder_event(recorder_tx.as_ref(), &event);
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
                                            MarketEvent::AssetCtx(ac) => { strategy.on_asset_ctx(ac); Vec::new() }
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
                                            MarketEvent::MarketDataHealth(health) => strategy.on_market_data_health(health),
                                            MarketEvent::TickSizeChange(tsc) => { strategy.on_tick_size_change(tsc) }
                                            MarketEvent::EventStart { .. }
                                            | MarketEvent::EventEnd { .. }
                                            | MarketEvent::Exit => Vec::new(),
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
                                                    ts.saturating_sub(last_quote_ns[i]) >= threshold_ns
                                                };
                                                if fire {
                                                    last_quote_ns[i] = ts;
                                                    quote_signal_batch.clear();
                                                    strategy.on_quote_into(ts, &mut quote_signal_batch);
                                                    stamp_quote_trigger(&mut quote_signal_batch, ob, true);
                                                    for sig in quote_signal_batch.drain(..) {
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
                                Err(crossbeam_channel::TryRecvError::Empty) => continue,
                                Err(crossbeam_channel::TryRecvError::Disconnected) => break,
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

    /// LIVE/PAPER per-instance routing (P1+P2). Spawns one worker
    /// thread per strategy instance — each pinned to its own core via
    /// `strategy_cores[instance_id]` — and a router thread that fans
    /// each market event only to the instances subscribing its symbol
    /// (spot symbols matched statically from `subscribed_symbols`,
    /// Polymarket token_ids learned dynamically from `Instrument`
    /// events). Order updates are routed directly by the numeric owner encoded
    /// in each instance's client-order-id namespace; unknown or quarantined
    /// owners are fail-closed and never broadcast to sibling strategies.
    ///
    /// Returns the router/supervisor thread's handle (it joins the
    /// workers internally), so the caller's single `.join()` still works.
    fn spawn_per_instance_strategy_threads(
        &self,
        strategies: Vec<Box<dyn Strategy>>,
        market_rx: PublicMarketReceiver,
        signal_tx: SignalSender,
        update_rx: Receiver<OrderUpdate>,
        executor_update_rx: Option<Receiver<RoutedOrderUpdate>>,
        recorder_tx: Option<Sender<Arc<MarketEvent>>>,
        data_dirs: Vec<PathBuf>,
        shutdown_done_rx: Option<Receiver<()>>,
    ) -> thread::JoinHandle<()> {
        // Static symbol → instance routing map (lowercased keys). A
        // symbol shared by several instances (e.g. two BTC timeframes on
        // BTCUSDT) maps to all of them — that's the shared-subscription
        // fan-out the design calls for.
        assert!(
            strategies.len() <= RouteMask::BITS as usize,
            "numeric strategy route mask supports at most {} instances",
            RouteMask::BITS,
        );
        let mut sym_to_instances: HashMap<SymbolId, RouteMask> = HashMap::new();
        let mut instance_ids: Vec<String> = Vec::with_capacity(strategies.len());
        for (i, s) in strategies.iter().enumerate() {
            instance_ids.push(s.instance_id().to_string());
            let subscriptions = s.subscribed_symbols();
            if subscriptions.is_empty() {
                warn!(
                    "[strategy_router] instance={} has no explicit subscriptions; symbol-scoped market data will be dropped",
                    s.instance_id(),
                );
            }
            for sym in subscriptions {
                *sym_to_instances.entry(SymbolId::of(&sym)).or_default() |= 1u64 << i;
            }
        }

        // Per-instance channels + move each strategy into a worker spec.
        // Market events are immutable after parsing. Fan out one allocation
        // through Arc instead of deep-cloning order-book vectors and Strings
        // once per subscribing strategy instance.
        let mut market_lanes: Vec<MarketEventLane> = Vec::with_capacity(strategies.len());
        let mut update_txs: Vec<Sender<QueuedOrderUpdate>> = Vec::with_capacity(strategies.len());
        let mut specs: Vec<(
            Box<dyn Strategy>,
            Receiver<QueuedMarketEvent>,
            Arc<LatestMarketStore>,
            Receiver<QueuedOrderUpdate>,
        )> = Vec::with_capacity(strategies.len());
        for s in strategies.into_iter() {
            let (mtx, mrx) = bounded::<QueuedMarketEvent>(CHANNEL_CAPACITY);
            let latest = Arc::new(LatestMarketStore::default());
            // One direct bounded lane per strategy. Private updates must never
            // block the shared router behind a stalled instance: a full lane is
            // treated as event loss, quarantines only that owner, and triggers
            // its fail-closed emergency cancellation path.
            let (utx, urx) = bounded::<QueuedOrderUpdate>(CHANNEL_CAPACITY);
            market_lanes.push(MarketEventLane {
                tx: mtx,
                latest: Arc::clone(&latest),
                barrier_epoch: Arc::new(AtomicU64::new(0)),
            });
            update_txs.push(utx);
            specs.push((s, mrx, latest, urx));
        }
        let supervisor_origin = Arc::new(std::time::Instant::now());
        let worker_heartbeats: Vec<Arc<AtomicU64>> = (0..instance_ids.len())
            .map(|_| Arc::new(AtomicU64::new(0)))
            .collect();
        let worker_quarantined: Vec<Arc<AtomicBool>> = (0..instance_ids.len())
            .map(|_| Arc::new(AtomicBool::new(false)))
            .collect();
        let worker_shutdown_requested: Vec<Arc<AtomicBool>> = (0..instance_ids.len())
            .map(|_| Arc::new(AtomicBool::new(false)))
            .collect();

        thread::Builder::new()
            .name("strategy-router".into())
            .spawn(move || {
                // Spawn one worker per instance, each on its own core.
                let mut handles: Vec<thread::JoinHandle<()>> = Vec::with_capacity(specs.len());
                // Each worker emits at most one terminal status and one
                // shutdown acknowledgement. Capacity equals producer count,
                // so neither path can block and no unbounded control backlog
                // exists during teardown.
                let worker_control_capacity = instance_ids.len().max(1);
                let (worker_status_tx, worker_status_rx) =
                    bounded::<(usize, bool)>(worker_control_capacity);
                let (shutdown_ack_tx, shutdown_ack_rx) =
                    bounded::<usize>(worker_control_capacity);
                for (idx, (strategy, mrx, latest, urx)) in specs.into_iter().enumerate() {
                    let stx = signal_tx.with_owner(idx);
                    let dd = data_dirs.clone();
                    let iid = instance_ids[idx].clone();
                    let heartbeat = Arc::clone(&worker_heartbeats[idx]);
                    let quarantined = Arc::clone(&worker_quarantined[idx]);
                    let shutdown_requested = Arc::clone(&worker_shutdown_requested[idx]);
                    let clock_origin = Arc::clone(&supervisor_origin);
                    let status_tx = worker_status_tx.clone();
                    let ack_tx = shutdown_ack_tx.clone();
                    let h = thread::Builder::new()
                        .name(format!("strategy-{}", if iid.is_empty() { idx.to_string() } else { iid.clone() }))
                        .spawn(move || {
                            let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                Self::run_strategy_worker(
                                    strategy, mrx, latest, urx, stx, dd, &iid, idx,
                                    heartbeat, quarantined, shutdown_requested,
                                    ack_tx, clock_origin,
                                );
                            })).is_err();
                            let _ = status_tx.send((idx, panicked));
                        })
                        .unwrap();
                    handles.push(h);
                }

                // Router runs on the fallback strategy core. Production
                // live/paper layouts dedicate that core to the router; every
                // configured instance uses `strategy_cores` instead.
                // Fan-out below clones only Arc pointers, never event payloads.
                crate::os_tune::pin_strategy("strategy-router");
                // `quanta` may calibrate its process-wide monotonic clock on
                // first use. Pay that cost before the router accepts data.
                let _ = market_queue_monotonic_ns();
                crate::latency::prepare_polymarket_private_stages();
                info!(
                    "[Strategy] Per-instance routing active: {} instances {:?}, {} routed symbols",
                    instance_ids.len(), instance_ids, sym_to_instances.len(),
                );

                // Learned Polymarket token_id → instances (from Instrument).
                let mut token_to_instances: HashMap<SymbolId, RouteMask> =
                    HashMap::with_capacity(64);
                let mut latest_key_ids = LatestMarketKeyRegistry::default();
                let mut market_overflow_drops = vec![0u64; instance_ids.len()];
                let mut market_overflow_log_at = std::time::Instant::now();
                let mut latest_capacity_fallbacks_reported = 0u64;
                let mut last_emergency_cancel_attempt_ns = vec![0u64; instance_ids.len()];
                let supervisor_tick = crossbeam_channel::tick(std::time::Duration::from_millis(100));
                // instance_id → numeric worker owner. Coids are minted as
                // "{instance_id}-{counter}" in live/paper, so the prefix names
                // the placing instance.
                let iid_to_idx: HashMap<String, usize> = instance_ids
                    .iter().enumerate().map(|(i, s)| (s.clone(), i)).collect();
                let shutdown_done_rx = shutdown_done_rx
                    .unwrap_or_else(crossbeam_channel::never);
                let executor_update_rx = executor_update_rx
                    .unwrap_or_else(crossbeam_channel::never);
                let mut shutdown_in_progress = false;
                let never_root_update_rx = crossbeam_channel::never::<OrderUpdate>();
                let never_executor_update_rx =
                    crossbeam_channel::never::<RoutedOrderUpdate>();
                let mut root_private_update_burst = 0usize;
                let market_ready_rx = market_rx.ready_receiver().clone();
                loop {
                    // Private/order lifecycle is lossless and higher priority
                    // than replaceable market data. Bound the priority burst
                    // so a pathological update storm still permits one public
                    // event, then immediately return to private-first routing.
                    let selectable_update_rx = if private_update_lane_enabled(
                        root_private_update_burst,
                        !market_rx.is_empty(),
                    ) {
                        &update_rx
                    } else {
                        &never_root_update_rx
                    };
                    let selectable_executor_update_rx = if private_update_lane_enabled(
                        root_private_update_burst,
                        !market_rx.is_empty(),
                    ) {
                        &executor_update_rx
                    } else {
                        &never_executor_update_rx
                    };
                    crossbeam_channel::select_biased! {
                        recv(selectable_update_rx) -> msg => match msg {
                            // Route by coid → numeric owner. Missing ownership
                            // is unowned and dropped rather than contaminating
                            // sibling StrategyAccounts.
                            Ok(u) => {
                                root_private_update_burst =
                                    root_private_update_burst.saturating_add(1);
                                Self::route_private_update(
                                    u,
                                    &iid_to_idx,
                                    &update_txs,
                                    &worker_quarantined,
                                    &instance_ids,
                                    &signal_tx,
                                );
                            }
                            Err(_) => break,
                        },
                        recv(selectable_executor_update_rx) -> msg => match msg {
                            Ok(routed) => {
                                root_private_update_burst =
                                    root_private_update_burst.saturating_add(1);
                                Self::route_executor_update(
                                    routed,
                                    &iid_to_idx,
                                    &update_txs,
                                    &worker_quarantined,
                                    &instance_ids,
                                    &signal_tx,
                                );
                            }
                            Err(_) => break,
                        },
                        recv(market_ready_rx) -> _ready => match market_rx.try_recv() {
                            Ok(MarketEvent::Exit) => {
                                if shutdown_in_progress { continue; }
                                forward_recorder_event(
                                    recorder_tx.as_ref(), &MarketEvent::Exit,
                                );
                                let mut waiting: HashSet<usize> = (0..instance_ids.len())
                                    .filter(|idx| !worker_quarantined[*idx].load(Ordering::Acquire))
                                    .collect();
                                for idx in &waiting {
                                    worker_shutdown_requested[*idx].store(true, Ordering::Release);
                                }
                                // A worker acknowledges only after its current
                                // callback has returned and on_exit has run, so
                                // all order-producing signals are ahead of the
                                // executor barrier.
                                let deadline = std::time::Instant::now()
                                    + std::time::Duration::from_nanos(STRATEGY_WORKER_STALL_NS);
                                while !waiting.is_empty() {
                                    let now = std::time::Instant::now();
                                    if now >= deadline { break; }
                                    match shutdown_ack_rx.recv_timeout(deadline.saturating_duration_since(now)) {
                                        Ok(idx) => { waiting.remove(&idx); }
                                        Err(_) => break,
                                    }
                                }
                                for idx in waiting {
                                    quarantine_strategy_worker(
                                        idx,
                                        "shutdown acknowledgement timed out",
                                        &instance_ids,
                                        &worker_quarantined,
                                        &signal_tx,
                                    );
                                }
                                shutdown_in_progress = true;
                                if signal_tx.send(Signal::BeginShutdown).is_err() {
                                    warn!("[Strategy] executor disappeared before shutdown barrier");
                                    break;
                                }
                            }
                            Ok(event) => {
                                root_private_update_burst = 0;
                                if shutdown_in_progress { continue; }
                                let event = Arc::new(event);
                                let mut dropped_mask = Self::route_market_event(
                                    Arc::clone(&event),
                                    &sym_to_instances,
                                    &mut token_to_instances,
                                    &mut latest_key_ids,
                                    &market_lanes,
                                );
                                while dropped_mask != 0 {
                                    let idx = dropped_mask.trailing_zeros() as usize;
                                    dropped_mask &= dropped_mask - 1;
                                    if let Some(drops) = market_overflow_drops.get_mut(idx) {
                                        *drops = drops.saturating_add(1);
                                    }
                                    quarantine_strategy_worker(
                                        idx,
                                        "market queue overflow (event loss)",
                                        &instance_ids,
                                        &worker_quarantined,
                                        &signal_tx,
                                    );
                                }
                                forward_recorder_shared(recorder_tx.as_ref(), event);
                                if market_overflow_drops.iter().any(|drops| *drops > 0)
                                    && market_overflow_log_at.elapsed()
                                        >= std::time::Duration::from_secs(1)
                                {
                                    for (idx, drops) in market_overflow_drops.iter_mut().enumerate() {
                                        if *drops == 0 {
                                            continue;
                                        }
                                        warn!(
                                            "[market_queue_metric] instance={} router_overflow_drops={} window_ms={}",
                                            instance_ids.get(idx).map(String::as_str).unwrap_or("<unknown>"),
                                            *drops,
                                            market_overflow_log_at.elapsed().as_millis(),
                                        );
                                        *drops = 0;
                                    }
                                    market_overflow_log_at = std::time::Instant::now();
                                }
                            }
                            Err(crossbeam_channel::TryRecvError::Empty) => continue,
                            Err(crossbeam_channel::TryRecvError::Disconnected) => break,
                        },
                        recv(shutdown_done_rx) -> _ => {
                            if shutdown_in_progress {
                                // Done is sent after final updates are
                                // enqueued, but select may see this independent
                                // channel first. Drain the root tail into the
                                // same lossless per-instance spools.
                                while let Ok(u) = update_rx.try_recv() {
                                    Self::route_private_update(
                                        u,
                                        &iid_to_idx,
                                        &update_txs,
                                        &worker_quarantined,
                                        &instance_ids,
                                        &signal_tx,
                                    );
                                }
                                while let Ok(routed) = executor_update_rx.try_recv() {
                                    Self::route_executor_update(
                                        routed,
                                        &iid_to_idx,
                                        &update_txs,
                                        &worker_quarantined,
                                        &instance_ids,
                                        &signal_tx,
                                    );
                                }
                                break;
                            }
                        },
                        recv(worker_status_rx) -> msg => {
                            if let Ok((idx, panicked)) = msg {
                                let reason = if panicked {
                                    "strategy worker panicked"
                                } else {
                                    "strategy worker exited unexpectedly"
                                };
                                quarantine_strategy_worker(
                                    idx, reason, &instance_ids, &worker_quarantined, &signal_tx,
                                );
                            }
                        },
                        recv(supervisor_tick) -> _ => {
                            if latest_key_ids.capacity_fallbacks
                                != latest_capacity_fallbacks_reported
                            {
                                warn!(
                                    "[market_queue_metric] latest_key_capacity_fallbacks={} active_keys={} capacity={}",
                                    latest_key_ids.capacity_fallbacks,
                                    latest_key_ids.key_to_handle.len(),
                                    MAX_LATEST_MARKET_KEYS,
                                );
                                latest_capacity_fallbacks_reported =
                                    latest_key_ids.capacity_fallbacks;
                            }
                            let now = elapsed_ns(&supervisor_origin);
                            for idx in 0..worker_heartbeats.len() {
                                if worker_quarantined[idx].load(Ordering::Acquire) {
                                    if now.saturating_sub(last_emergency_cancel_attempt_ns[idx])
                                        >= 1_000_000_000
                                    {
                                        let _ = enqueue_emergency_instance_cancel(
                                            idx,
                                            &instance_ids[idx],
                                            "periodic quarantine cancel retry",
                                            &signal_tx,
                                        );
                                        last_emergency_cancel_attempt_ns[idx] = now;
                                    }
                                    continue;
                                }
                                let last = worker_heartbeats[idx].load(Ordering::Acquire);
                                if now.saturating_sub(last) >= STRATEGY_WORKER_STALL_NS {
                                    quarantine_strategy_worker(
                                        idx,
                                        "strategy worker heartbeat stalled for at least 5s",
                                        &instance_ids,
                                        &worker_quarantined,
                                        &signal_tx,
                                    );
                                }
                            }
                        },
                    }
                }

                // Close direct private-update lanes before workers are
                // released to generate their final reports.
                drop(update_txs);
                drop(market_lanes);
                for (idx, handle) in handles.into_iter().enumerate() {
                    if worker_quarantined[idx].load(Ordering::Acquire) {
                        drop(handle);
                    } else {
                        let _ = handle.join();
                    }
                }
                // Single terminal Exit to the executor (workers don't send it).
                let _ = signal_tx.send(Signal::Exit);
            })
            .unwrap()
    }

    fn route_executor_update(
        routed: RoutedOrderUpdate,
        iid_to_idx: &HashMap<String, usize>,
        update_txs: &[Sender<QueuedOrderUpdate>],
        worker_quarantined: &[Arc<AtomicBool>],
        instance_ids: &[String],
        signal_tx: &SignalSender,
    ) {
        if routed.owner == SYSTEM_SIGNAL_OWNER {
            // Account-wide shutdown/recovery operations are not emitted by a
            // strategy instance. Preserve the cold coid recovery route.
            Self::route_private_update(
                routed.update,
                iid_to_idx,
                update_txs,
                worker_quarantined,
                instance_ids,
                signal_tx,
            );
            return;
        }
        let owner = routed.owner as usize;
        if owner >= update_txs.len() {
            error!(
                "[strategy_router] invalid executor owner={} coid={}",
                owner, routed.update.client_order_id,
            );
            return;
        }
        if worker_quarantined[owner].load(Ordering::Acquire) {
            warn!(
                "[strategy_router] dropping executor update for quarantined owner instance={} coid={}",
                instance_ids.get(owner).map(String::as_str).unwrap_or(""),
                routed.update.client_order_id,
            );
            return;
        }
        if update_txs[owner]
            .try_send(QueuedOrderUpdate {
                update: routed.update,
                enqueued_at: std::time::Instant::now(),
            })
            .is_err()
        {
            quarantine_strategy_worker(
                owner,
                "executor update queue overflow/disconnect (event loss)",
                instance_ids,
                worker_quarantined,
                signal_tx,
            );
        }
    }

    fn route_private_update(
        update: OrderUpdate,
        iid_to_idx: &HashMap<String, usize>,
        update_txs: &[Sender<QueuedOrderUpdate>],
        worker_quarantined: &[Arc<AtomicBool>],
        instance_ids: &[String],
        signal_tx: &SignalSender,
    ) {
        // RTT probes reserve/release their synthetic orders directly in the
        // shared monitoring account.  Their executor-style acknowledgements
        // deliberately have no numeric strategy owner and must not enter (or
        // contaminate the latency metrics of) the lossless strategy lane.
        if is_synthetic_probe_update(&update) {
            return;
        }
        // Polymarket execution completions and private owner-fast routing stamp
        // OrderUpdate with a local wall-clock timestamp. Reject stale/exchange
        // timestamps so this boundary metric cannot poison its histogram.
        if update.exchange == Exchange::Polymarket && update.timestamp_ns > 0 {
            let producer_to_root_ns = now_ns().saturating_sub(update.timestamp_ns);
            if producer_to_root_ns <= 5_000_000_000 {
                hexagent_runtime::latency::record_ns(
                    "polymarket.update.producer_to_root_router",
                    producer_to_root_ns,
                );
            }
        }
        let is_market_cancel_finality = update.error.as_deref().is_some_and(|error| {
            error.starts_with(POLYMARKET_MARKET_CANCEL_FINALITY_CONFIRMED)
                || error.starts_with(POLYMARKET_MARKET_CANCEL_FINALITY_PENDING)
        });
        let owner = if is_market_cancel_finality {
            iid_to_idx.get(&update.client_order_id).copied()
        } else {
            owner_from_coid(&update.client_order_id, iid_to_idx)
        };

        match classify_private_update_route(owner, update_txs.len(), worker_quarantined) {
            PrivateUpdateRoute::Owner(i) => {
                let sent = update_txs[i].try_send(QueuedOrderUpdate {
                    update,
                    enqueued_at: std::time::Instant::now(),
                });
                if sent.is_err() {
                    quarantine_strategy_worker(
                        i,
                        "private update queue overflow/disconnect (event loss)",
                        instance_ids,
                        worker_quarantined,
                        signal_tx,
                    );
                }
            }
            PrivateUpdateRoute::DropQuarantined(i) => {
                warn!(
                    "[strategy_router] dropping private update for quarantined owner instance={} coid={}",
                    instance_ids.get(i).map(String::as_str).unwrap_or(""),
                    update.client_order_id
                );
            }
            PrivateUpdateRoute::DropInvalid(i) => {
                error!(
                    "[strategy_router] invalid owner index={} coid={}",
                    i, update.client_order_id
                );
            }
            PrivateUpdateRoute::Unowned => {
                error!(
                    "[strategy_router] dropping private update without numeric owner coid={} status={:?}",
                    update.client_order_id, update.status,
                );
            }
        }
    }

    /// Route ONE market event to the subscribing instances' channels.
    /// Known symbol → those instances. Every unknown symbol is dropped:
    /// broadcasting it contaminates per-instance state and can fill unrelated
    /// workers' bounded market queues. Learns Polymarket
    /// token_id → instance from `Instrument(BinaryOption)`.
    fn route_market_event(
        event: Arc<MarketEvent>,
        sym_to_instances: &HashMap<SymbolId, RouteMask>,
        token_to_instances: &mut HashMap<SymbolId, RouteMask>,
        latest_key_ids: &mut LatestMarketKeyRegistry,
        market_lanes: &[MarketEventLane],
    ) -> RouteMask {
        let send_to = |mut mask: RouteMask, latest_key: Option<LatestMarketKeyHandle>| {
            let mut dropped = 0u64;
            while mask != 0 {
                let i = mask.trailing_zeros() as usize;
                mask &= mask - 1;
                if let Some(lane) = market_lanes.get(i) {
                    if !enqueue_market_event(lane, Arc::clone(&event), latest_key) {
                        dropped |= 1u64 << i;
                    }
                }
            }
            dropped
        };

        // EventEnd is an ordered router-control message. The router is the
        // sole writer of both maps and the free-list. Retire every dynamic
        // Polymarket token before the following EventStart/new books, discard
        // pending payloads for the old generation, then make the IDs reusable.
        if let MarketEvent::EventEnd {
            exchange: Exchange::Polymarket,
            retired_symbols,
            ..
        } = event.as_ref()
        {
            for lane in market_lanes {
                lane.barrier_epoch.fetch_add(1, Ordering::Relaxed);
            }
            for symbol in retired_symbols {
                let symbol_id = SymbolId::of(symbol);
                token_to_instances.remove(&symbol_id);
                for kind in [0, 1] {
                    let key = LatestMarketKey {
                        kind,
                        exchange: Exchange::Polymarket,
                        symbol: symbol_id,
                    };
                    if let Some(handle) = latest_key_ids.remove(&key) {
                        retire_latest_market_key(handle, market_lanes);
                        latest_key_ids.release(handle);
                    }
                }
            }
            return 0;
        }

        // Instrument(BinaryOption) → attribute its token_ids to the owner
        // instance(s) of its slug, then deliver to those owners.
        if let MarketEvent::Instrument(Instrument::BinaryOption(bo)) = event.as_ref() {
            let route_symbol = if bo.series_slug.trim().is_empty() {
                bo.slug.as_str()
            } else {
                bo.series_slug.as_str()
            };
            let route_key = SymbolId::of(route_symbol);
            if let Some(&owners) = sym_to_instances.get(&route_key) {
                for tok in &bo.clob_token_ids {
                    token_to_instances.insert(SymbolId::of(tok), owners);
                }
                return send_to(owners, None);
            } else {
                warn!(
                    "[strategy_router] dropping unroutable Polymarket instrument series={} event_slug={}",
                    route_symbol, bo.slug,
                );
                return 0;
            }
        }

        let all_instances = match market_lanes.len() {
            64 => u64::MAX,
            len => (1u64 << len) - 1,
        };
        let targets: RouteMask = match event.as_ref() {
            // Lifecycle / spot-instrument → all instances.
            MarketEvent::Connected { .. } | MarketEvent::Disconnected { .. } => all_instances,
            MarketEvent::Instrument(Instrument::Spot(spot)) => sym_to_instances
                .get(&SymbolId::of(&spot.symbol))
                .copied()
                .unwrap_or_default(),
            // Polymarket market data keyed by dynamic token_id.
            MarketEvent::OrderBook(ob) if ob.exchange == Exchange::Polymarket => token_to_instances
                .get(&SymbolId::of(&ob.symbol))
                .copied()
                .unwrap_or_default(),
            MarketEvent::Trade(t) if t.exchange == Exchange::Polymarket => token_to_instances
                .get(&SymbolId::of(&t.symbol))
                .copied()
                .unwrap_or_default(),
            MarketEvent::Quote(q) if q.exchange == Exchange::Polymarket => token_to_instances
                .get(&SymbolId::of(&q.symbol))
                .copied()
                .unwrap_or_default(),
            MarketEvent::TickSizeChange(tsc) => token_to_instances
                .get(&SymbolId::of(&tsc.symbol))
                .copied()
                .or_else(|| sym_to_instances.get(&SymbolId::of(&tsc.symbol)).copied())
                .unwrap_or_default(),
            // Spot venues keyed by stable symbol.
            MarketEvent::OrderBook(ob) => sym_to_instances
                .get(&SymbolId::of(&ob.symbol))
                .copied()
                .unwrap_or_default(),
            MarketEvent::Trade(t) => sym_to_instances
                .get(&SymbolId::of(&t.symbol))
                .copied()
                .unwrap_or_default(),
            MarketEvent::Quote(q) => sym_to_instances
                .get(&SymbolId::of(&q.symbol))
                .copied()
                .unwrap_or_default(),
            MarketEvent::Bar(b) => sym_to_instances
                .get(&SymbolId::of(&b.symbol))
                .copied()
                .unwrap_or_default(),
            MarketEvent::SpotPrice(sp) => sym_to_instances
                .get(&SymbolId::of(&sp.symbol))
                .copied()
                .unwrap_or_default(),
            MarketEvent::AssetCtx(ac) => sym_to_instances
                .get(&SymbolId::of(&ac.symbol))
                .copied()
                .unwrap_or_default(),
            MarketEvent::MarketDataHealth(health) => token_to_instances
                .get(&SymbolId::of(&health.symbol))
                .copied()
                .or_else(|| sym_to_instances.get(&SymbolId::of(&health.symbol)).copied())
                .unwrap_or_default(),
            MarketEvent::EventStart { symbol, .. } => sym_to_instances
                .get(&SymbolId::of(symbol))
                .copied()
                .unwrap_or_default(),
            MarketEvent::EventEnd { .. } | MarketEvent::Exit => 0,
            MarketEvent::Instrument(Instrument::BinaryOption(_)) => unreachable!(),
        };
        if targets == 0 {
            return 0;
        }
        // Register coverable keys only after routing resolved at least one
        // owner. Unknown dynamic token traffic must not consume fixed slots.
        let latest_key = latest_market_key(event.as_ref())
            .and_then(|key| latest_key_ids.get_or_register(key));
        send_to(targets, latest_key)
    }

    /// Per-instance worker loop (live/paper). Runs ONE
    /// strategy, pinned to its own core. Mirrors the per-strategy body
    /// of the single-thread loop (market dispatch + quote-cadence
    /// trigger + order-update reaction), minus the recorder/sim-clock
    /// (the router owns recording; sim-clock is backtest-only). Registers
    /// numeric ownership in the signal envelope lets the router return fills
    /// to this instance without a shared client-order-id registry.
    fn run_strategy_worker(
        mut strategy: Box<dyn Strategy>,
        market_rx: Receiver<QueuedMarketEvent>,
        latest_market: Arc<LatestMarketStore>,
        update_rx: Receiver<QueuedOrderUpdate>,
        signal_tx: SignalSender,
        data_dirs: Vec<PathBuf>,
        instance_id: &str,
        idx: usize,
        heartbeat: Arc<AtomicU64>,
        quarantined: Arc<AtomicBool>,
        shutdown_requested: Arc<AtomicBool>,
        shutdown_ack_tx: Sender<usize>,
        clock_origin: Arc<std::time::Instant>,
    ) {
        // Optional venue-authenticated private feed. The lane is transferred at
        // startup and consumed only by this strategy owner. Its FIFO updates and
        // one-slot replaceable control state are selected before public market
        // data, so fills do not depend on quote cadence.
        let private_lane = strategy.take_private_update_lane();
        let private_feed_update_rx = private_lane
            .as_ref()
            .map(|lane| lane.updates.clone())
            .unwrap_or_else(crossbeam_channel::never);
        let private_feed_control_rx = private_lane
            .as_ref()
            .map(|lane| lane.control.clone())
            .unwrap_or_else(crossbeam_channel::never);
        let mut private_feed_updates_open = private_lane.is_some();
        let mut private_feed_control_open = private_lane.is_some();
        // File/REST-backed history loading never runs on the strategy owner.
        // A bounded SPSC worker prepares owned immutable snapshots, then this
        // strategy thread alone replays them into mutable strategy state.
        let (hist_job_tx, hist_job_rx) = bounded::<HistoricalLoadJob>(8);
        let (hist_result_tx, hist_result_rx) = bounded::<HistoricalLoadResult>(8);
        let hist_loader_name = format!("strategy-hist-prefetch-{instance_id}");
        let hist_loader = thread::Builder::new()
            .name(hist_loader_name.clone())
            .spawn(move || {
                crate::os_tune::pin_background(&hist_loader_name);
                while let Ok(job) = hist_job_rx.recv() {
                    let mut loaded = Vec::with_capacity(job.requests.len());
                    for request in job.requests {
                        let mut bars = Vec::new();
                        for dir in &data_dirs {
                            if let Ok(candidate) = crate::recorder::load_hist_bars(dir, &request) {
                                if !candidate.is_empty() {
                                    bars = candidate;
                                    break;
                                }
                            }
                        }
                        loaded.push((request, Arc::from(bars.into_boxed_slice())));
                    }
                    if hist_result_tx
                        .send(HistoricalLoadResult {
                            epoch: job.epoch,
                            ts_event: job.ts_event,
                            loaded,
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            })
            .expect("spawn strategy history prefetch worker");
        crate::os_tune::pin_strategy_instance(&format!("strategy-{}", instance_id), instance_id);
        crate::latency::prepare_thread_stages(&[
            "strategy.market.queue",
            "strategy.market.callback",
            "strategy.private_update.queue",
            "strategy.private_update.callback",
            "strategy.watchdog.callback",
            "strategy.private_feed.callback",
        ]);
        // The sender is permanently tagged with this worker's numeric owner.
        // Normal traffic is non-blocking; overflow quarantines only this owner
        // and submits an emergency cancel on the independent control lane.
        let emit = |sig: Signal| -> bool {
            emit_strategy_signal(sig, &signal_tx, &quarantined, instance_id)
        };
        let mut last_quote_ns: u64 = 0;
        let mut quote_signal_batch = Vec::with_capacity(32);
        let mut private_update_burst = 0usize;
        let mut historical_epoch = 0u64;
        let mut shutdown_started = false;
        let watchdog_rx = crossbeam_channel::tick(std::time::Duration::from_millis(100));
        let never_update_rx = crossbeam_channel::never::<QueuedOrderUpdate>();
        let never_private_update_rx = crossbeam_channel::never::<OrderUpdate>();
        let never_private_control_rx =
            crossbeam_channel::never::<crate::exchange::PrivateFeedControl>();
        let never_watchdog_rx = crossbeam_channel::never::<std::time::Instant>();
        let mut last_watchdog_run = std::time::Instant::now();
        'worker: loop {
            if !shutdown_started && shutdown_requested.load(Ordering::Acquire) {
                strategy.on_exit();
                shutdown_started = true;
                heartbeat.store(elapsed_ns(&clock_origin), Ordering::Release);
                let _ = shutdown_ack_tx.send(idx);
            }
            // Private lifecycle changes retain priority, but only for a finite
            // burst. Once market work is waiting, force one public event so a
            // fill storm cannot indefinitely starve quote freshness.
            let selectable_update_rx =
                if !private_update_lane_enabled(private_update_burst, !market_rx.is_empty()) {
                    &never_update_rx
                } else {
                    &update_rx
                };
            let selectable_private_update_rx = if private_feed_updates_open
                && private_update_lane_enabled(private_update_burst, !market_rx.is_empty())
            {
                &private_feed_update_rx
            } else {
                &never_private_update_rx
            };
            let selectable_private_control_rx = if private_feed_control_open {
                &private_feed_control_rx
            } else {
                &never_private_control_rx
            };
            let selectable_watchdog_rx =
                if !market_rx.is_empty() && last_watchdog_run.elapsed() < WATCHDOG_MAX_DEFERRAL {
                &never_watchdog_rx
            } else {
                &watchdog_rx
            };
            crossbeam_channel::select_biased! {
                recv(selectable_private_control_rx) -> msg => match msg {
                    Ok(control) => {
                        if quarantined.load(Ordering::Acquire) { break 'worker; }
                        heartbeat.store(elapsed_ns(&clock_origin), Ordering::Release);
                        if shutdown_started { continue; }
                        for sig in strategy.on_private_feed_control(control) {
                            if !emit(sig) { break 'worker; }
                        }
                    }
                    Err(_) => private_feed_control_open = false,
                },
                recv(selectable_private_update_rx) -> msg => match msg {
                    Ok(update) => {
                        private_update_burst = private_update_burst.saturating_add(1);
                        if quarantined.load(Ordering::Acquire) { break 'worker; }
                        heartbeat.store(elapsed_ns(&clock_origin), Ordering::Release);
                        if shutdown_started { continue; }
                        let callback_started = crate::latency::Instant::now();
                        let signals = strategy.on_order_update_owned(update);
                        crate::latency::record(
                            "strategy.private_feed.callback",
                            callback_started,
                        );
                        for sig in signals {
                            if !emit(sig) { break 'worker; }
                        }
                    }
                    Err(_) => private_feed_updates_open = false,
                },
                recv(selectable_update_rx) -> msg => match msg {
                    Ok(queued) => {
                        if queued.update.exchange == Exchange::Polymarket {
                            hexagent_runtime::latency::record_ns(
                                "polymarket.update.root_router_to_instance_worker",
                                queued.enqueued_at.elapsed().as_nanos().min(u64::MAX as u128)
                                    as u64,
                            );
                        }
                        private_update_burst = private_update_burst.saturating_add(1);
                        if quarantined.load(Ordering::Acquire) { break 'worker; }
                        heartbeat.store(elapsed_ns(&clock_origin), Ordering::Release);
                        crate::latency::record_ns(
                            "strategy.private_update.queue",
                            queued.enqueued_at.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                        );
                        let callback_started = crate::latency::Instant::now();
                        let signals = strategy.on_order_update_owned(queued.update);
                        if !shutdown_started {
                            for sig in signals {
                                if !emit(sig) { break 'worker; }
                            }
                        }
                        crate::latency::record(
                            "strategy.private_update.callback",
                            callback_started,
                        );
                    }
                    Err(_) => break,
                },
                recv(hist_result_rx) -> msg => match msg {
                    Ok(result) => {
                        if result.epoch != historical_epoch || shutdown_started {
                            continue;
                        }
                        for (request, bars) in result.loaded {
                            if bars.is_empty() {
                                warn!(
                                    "[Strategy] Failed to load hist bars for {}/{}",
                                    request.exchange,
                                    request.symbol,
                                );
                                continue;
                            }
                            for bar in bars.iter() {
                                strategy.on_hist_bar(bar);
                            }
                        }
                        strategy.on_hist_data_loaded(result.ts_event);
                    }
                    Err(_) => break,
                },
                recv(selectable_watchdog_rx) -> _ => {
                    if quarantined.load(Ordering::Acquire) { break 'worker; }
                    heartbeat.store(elapsed_ns(&clock_origin), Ordering::Release);
                    if shutdown_started { continue; }
                    let callback_started = crate::latency::Instant::now();
                    for sig in strategy.on_watchdog(crate::types::now_ns()) {
                        if !emit(sig) { break 'worker; }
                    }
                    crate::latency::record("strategy.watchdog.callback", callback_started);
                    last_watchdog_run = std::time::Instant::now();
                },
                recv(market_rx) -> msg => match msg {
                    Ok(queued) => {
                        private_update_burst = 0;
                        let Some(queued) = resolve_market_event(queued, &latest_market) else {
                            continue;
                        };
                        if matches!(queued.event.as_ref(), MarketEvent::Exit) {
                        heartbeat.store(elapsed_ns(&clock_origin), Ordering::Release);
                        if !shutdown_started {
                            strategy.on_exit();
                            shutdown_started = true;
                        }
                        break;
                        }
                        if quarantined.load(Ordering::Acquire) { break 'worker; }
                        heartbeat.store(elapsed_ns(&clock_origin), Ordering::Release);
                        if shutdown_started { continue; }
                        crate::latency::record_ns(
                            "strategy.market.queue",
                            market_queue_monotonic_ns().saturating_sub(queued.enqueued_ns),
                        );
                        let event = queued.event;
                        let callback_started = crate::latency::Instant::now();
                        let signals = match event.as_ref() {
                            MarketEvent::OrderBook(ob) => { strategy.on_orderbook(ob); Vec::new() }
                            MarketEvent::Trade(t) => { strategy.on_trade_tick(t); Vec::new() }
                            MarketEvent::Quote(q) => { strategy.on_quote_tick(q); Vec::new() }
                            MarketEvent::Bar(b) => { strategy.on_bar(b); Vec::new() }
                            MarketEvent::SpotPrice(sp) => { strategy.on_spot_price(sp); Vec::new() }
                            MarketEvent::AssetCtx(ac) => { strategy.on_asset_ctx(ac); Vec::new() }
                            MarketEvent::Instrument(inst) => {
                                strategy.on_instrument(inst);
                                let ts_event = event.timestamp_ns();
                                let hist_reqs = strategy.load_hist_data(ts_event);
                                if !hist_reqs.is_empty() {
                                    historical_epoch = historical_epoch.wrapping_add(1).max(1);
                                    let job = HistoricalLoadJob {
                                        epoch: historical_epoch,
                                        ts_event,
                                        requests: hist_reqs,
                                    };
                                    if hist_job_tx.try_send(job).is_err() {
                                        error!(
                                            "[Strategy] historical prefetch queue overflow instance={}; quarantining fail-closed",
                                            instance_id,
                                        );
                                        quarantined.store(true, Ordering::Release);
                                        break 'worker;
                                    }
                                }
                                Vec::new()
                            }
                            MarketEvent::Connected { exchange } => { strategy.on_connected(*exchange); Vec::new() }
                            MarketEvent::Disconnected { exchange, reason } => { strategy.on_disconnected(*exchange, reason); Vec::new() }
                            MarketEvent::MarketDataHealth(health) => strategy.on_market_data_health(health),
                            MarketEvent::TickSizeChange(tsc) => strategy.on_tick_size_change(tsc),
                            MarketEvent::EventStart { .. }
                            | MarketEvent::EventEnd { .. }
                            | MarketEvent::Exit => Vec::new(),
                        };
                        if let MarketEvent::OrderBook(ob) = event.as_ref() {
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
                                    ts.saturating_sub(last_quote_ns) >= threshold_ns
                                };
                                if fire {
                                    last_quote_ns = ts;
                                    quote_signal_batch.clear();
                                    strategy.on_quote_into(ts, &mut quote_signal_batch);
                                    stamp_quote_trigger(&mut quote_signal_batch, ob, true);
                                    for sig in quote_signal_batch.drain(..) {
                                        if !emit(sig) { break 'worker; }
                                    }
                                }
                            }
                        }
                        for sig in signals {
                            if !emit(sig) { break 'worker; }
                        }
                        crate::latency::record("strategy.market.callback", callback_started);
                    }
                    Err(_) => break,
                },
            }
        }
        drop(hist_job_tx);
        drop(hist_result_rx);
        let _ = hist_loader.join();
        if !shutdown_started {
            strategy.on_exit();
        }
        // Dispatchers are joined before the router closes market_txs, so all
        // final updates are available here before the report is generated.
        while let Ok(queued) = update_rx.try_recv() {
            let _ = strategy.on_order_update(&queued.update);
        }
        let _ = strategy.on_shutdown();
    }

    fn wait_for_shutdown(shutdown: &Arc<AtomicBool>, shutdown_tx: &PublicMarketPublisher) {
        let _ = Self::wait_for_shutdown_or_critical(shutdown, None, shutdown_tx, &[]);
    }

    fn wait_for_shutdown_or_critical(
        shutdown: &Arc<AtomicBool>,
        shutdown_token: Option<&ShutdownToken>,
        shutdown_tx: &PublicMarketPublisher,
        critical_threads: &[(&'static str, &thread::JoinHandle<()>)],
    ) -> ShutdownTrigger {
        use signal_hook::consts::{SIGINT, SIGTERM};
        use signal_hook::iterator::Signals;

        let start_time = std::time::Instant::now();
        let mut signals =
            Signals::new(&[SIGINT, SIGTERM]).expect("Failed to register signal handlers");
        info!("Press Ctrl-C to stop...");

        // Poll both OS signals and root worker liveness. Blocking forever in
        // `signals.forever()` left a process alive after its execution thread
        // panicked; `JoinHandle::is_finished` makes either root lane an
        // explicit shutdown source.
        let trigger = loop {
            if let Some(sig) = signals.pending().next() {
                break ShutdownTrigger::Signal(sig);
            }
            if let Some(name) = first_finished_critical_thread(critical_threads) {
                break ShutdownTrigger::CriticalThread(name);
            }
            thread::sleep(std::time::Duration::from_millis(50));
        };

        let uptime = start_time.elapsed();
        let hours = uptime.as_secs() / 3600;
        let mins = (uptime.as_secs() % 3600) / 60;
        let secs = uptime.as_secs() % 60;
        let rss_mb = Self::get_rss_mb().unwrap_or(0.0);
        match trigger {
            ShutdownTrigger::Signal(sig) => {
                let sig_name = match sig {
                    SIGINT => "SIGINT (Ctrl-C)",
                    SIGTERM => "SIGTERM",
                    _ => "unknown",
                };
                info!(
                    "Shutdown: signal={}, uptime={}h{}m{}s, rss={:.1}MB, pid={}",
                    sig_name,
                    hours,
                    mins,
                    secs,
                    rss_mb,
                    std::process::id()
                );
            }
            ShutdownTrigger::CriticalThread(name) => error!(
                "Shutdown: critical_thread={} exited unexpectedly, uptime={}h{}m{}s, rss={:.1}MB, pid={}; starting coordinated shutdown",
                name,
                hours,
                mins,
                secs,
                rss_mb,
                std::process::id(),
            ),
        }

        if let Some(token) = shutdown_token {
            token.request();
        } else {
            shutdown.store(true, Ordering::Release);
        }
        if let Err(error) =
            shutdown_tx.send_ordered_timeout(MarketEvent::Exit, std::time::Duration::from_secs(1))
        {
            warn!("Shutdown: could not enqueue strategy Exit event: {error}");
        }

        if let Err(error) = thread::Builder::new()
            .name("forced-shutdown-signal".to_string())
            .spawn(move || {
                if let Some(sig) = signals.forever().next() {
                    let exit_code = 128_i32.saturating_add(sig);
                    error!(
                        "Shutdown: second termination signal={} received; forcing process exit code={}",
                        sig, exit_code,
                    );
                    eprintln!(
                        "Shutdown: second termination signal received; forcing exit ({exit_code})"
                    );
                    std::process::exit(exit_code);
                }
            })
        {
            warn!("Shutdown: failed to arm second-signal forced exit: {error}");
        }
        trigger
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
                .output()
                .ok()?;
            let kb: f64 = String::from_utf8_lossy(&output.stdout)
                .trim()
                .parse()
                .ok()?;
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
        market_tx: PublicMarketPublisher,
        sim_feed_tx: Option<Sender<MarketEvent>>,
        shutdown: Arc<AtomicBool>,
    ) -> Result<Vec<thread::JoinHandle<()>>> {
        self.spawn_exchange_feeds_inner(market_tx, sim_feed_tx, shutdown)
    }

    pub fn spawn_exchange_feeds(
        &self,
        market_tx: PublicMarketPublisher,
        shutdown: Arc<AtomicBool>,
    ) -> Result<Vec<thread::JoinHandle<()>>> {
        self.spawn_exchange_feeds_inner(market_tx, None, shutdown)
    }

    fn spawn_exchange_feeds_inner(
        &self,
        market_tx: PublicMarketPublisher,
        sim_feed_tx: Option<Sender<MarketEvent>>,
        shutdown: Arc<AtomicBool>,
    ) -> Result<Vec<thread::JoinHandle<()>>> {
        let mut handles = Vec::new();

        for exchange_cfg in &self.config.exchanges {
            if !exchange_cfg.enabled {
                continue;
            }

            let tx = market_tx.clone();
            let sim_tx = if exchange_cfg.name == "polymarket" {
                sim_feed_tx.clone()
            } else {
                None
            };
            let cfg = exchange_cfg.clone();
            let shutdown = shutdown.clone();
            let feed_readiness = self.feed_readiness.clone();
            if cfg.name == "polymarket" {
                handles.extend(spawn_polymarket_feed_manager(
                    cfg,
                    tx,
                    sim_tx,
                    shutdown,
                    feed_readiness,
                )?);
                continue;
            }
            let polymarket_liveness: Option<Arc<PolymarketLiveness>> = None;
            // Resolve the spot kline interval BEFORE moving into the
            // feed thread — `self` (or its &refs) can't escape the
            // method into the thread closure's 'static bound.
            let spot_kline_interval: String = self
                .config
                .strategies
                .iter()
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
                    if let Some(liveness) = polymarket_liveness.as_ref() {
                        liveness.set_phase(PolymarketFeedPhase::Starting);
                        liveness.heartbeat_feed_loop();
                    }
                    let exchange = match cfg.name.as_str() {
                        "binance" => Exchange::Binance,
                        "bybit" => Exchange::Bybit,
                        "binance_futures" => Exchange::Binance, // placeholder; sends SpotPrice events
                        "chainlink" => Exchange::Chainlink,
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
                        "hyperliquid" => Exchange::Hyperliquid,
                        "aster" => Exchange::Aster,
                        "lighter" => Exchange::Lighter,
                        other => {
                            set_feed_readiness(
                                &feed_readiness,
                                &cfg.name,
                                FeedReadiness::NotReady {
                                    stage: "configuration".to_string(),
                                    reason: format!("unknown exchange: {}", other),
                                },
                            );
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
                            let mut pm = PolymarketMarket::with_liveness(
                                polymarket_liveness
                                    .as_ref()
                                    .expect("polymarket liveness")
                                    .clone(),
                            );
                            pm.set_market_tx(tx.clone(), shutdown.clone());
                            Box::new(pm)
                        }
                        "hexmarket" => Box::new(HexmarketMarket::new(&cfg.api_url_prefix, &cfg.wss_url)),
                        "hyperliquid" => {
                            // Resolve WS host: explicit override, else the
                            // network (mainnet/testnet) default.
                            let ws = if !cfg.wss_url.is_empty() {
                                cfg.wss_url.clone()
                            } else {
                                crate::exchange::hyperliquid::auth::Network::from_str(&cfg.network)
                                    .ws_url()
                                    .to_string()
                            };
                            Box::new(crate::exchange::hyperliquid::HyperliquidMarket::new(&ws))
                        }
                        "aster" => {
                            // Resolve WS host: explicit override, else the
                            // network (mainnet/testnet) default.
                            let ws = if !cfg.wss_url.is_empty() {
                                cfg.wss_url.clone()
                            } else {
                                crate::exchange::aster::auth::Network::from_str(&cfg.network)
                                    .ws_base()
                                    .to_string()
                            };
                            Box::new(crate::exchange::aster::AsterMarket::new(&ws))
                        }
                        "lighter" => {
                            // Resolve hosts: explicit override, else the
                            // network (mainnet/testnet) default. Market ids
                            // come from a one-shot orderBookDetails fetch.
                            let net = crate::exchange::lighter::auth::Network::from_str(&cfg.network);
                            let rest = if !cfg.api_url_prefix.is_empty() {
                                cfg.api_url_prefix.trim_end_matches('/').to_string()
                            } else {
                                net.rest_base().to_string()
                            };
                            let ws = if !cfg.wss_url.is_empty() {
                                cfg.wss_url.clone()
                            } else {
                                net.ws_url().to_string()
                            };
                            let meta = match crate::exchange::lighter::info::fetch_meta(&rest) {
                                Ok(m) => m,
                                Err(e) => {
                                    set_feed_readiness(
                                        &feed_readiness,
                                        &cfg.name,
                                        FeedReadiness::NotReady {
                                            stage: "initialization".to_string(),
                                            reason: e.to_string(),
                                        },
                                    );
                                    error!("[Lighter] orderBookDetails fetch failed, feed disabled: {}", e);
                                    return;
                                }
                            };
                            Box::new(crate::exchange::lighter::LighterMarket::new(&ws, meta))
                        }
                        _ => return,
                    };

                    // `PolymarketMarket::subscribe` resolves the *current*
                    // series event through Gamma before the CLOB WS can be
                    // opened. A transient Gamma outage used to terminate this
                    // feed thread permanently here, so every later 5-minute
                    // event was missed until the whole process restarted.
                    //
                    // Keep the short HTTP-level retry inside `subscribe`, then
                    // retry the complete subscription for the lifetime of the
                    // process. Rebuild PolymarketMarket after every failed
                    // round so a partially-populated token/series map can
                    // never leak into the next attempt. A fresh subscribe also
                    // recomputes `end_date_min` from wall clock and therefore
                    // follows event rotation instead of retrying a stale URL.
                    let mut subscribe_backoff =
                        crate::exchange::ReconnectBackoff::new(1_000, 30_000);
                    let mut subscribe_failures = 0_u64;
                    let subscribe_started = std::time::Instant::now();
                    loop {
                        if let Some(liveness) = polymarket_liveness.as_ref() {
                            liveness.set_phase(PolymarketFeedPhase::Subscribe);
                            liveness.heartbeat_feed_loop();
                        }
                        if shutdown.load(Ordering::Relaxed) {
                            feed.disconnect();
                            return;
                        }

                        match feed.subscribe(&cfg.symbols) {
                            Ok(()) => {
                                if subscribe_failures > 0 {
                                    info!(
                                        "[feed_health] {} readiness=SUBSCRIBED recovered_after={:.1}s attempts={}",
                                        cfg.name,
                                        subscribe_started.elapsed().as_secs_f64(),
                                        subscribe_failures + 1,
                                    );
                                }
                                break;
                            }
                            Err(e) if cfg.name == "polymarket" => {
                                subscribe_failures = subscribe_failures.saturating_add(1);
                                let delay = subscribe_backoff
                                    .next_delay()
                                    .min(std::time::Duration::from_secs(30));
                                set_feed_readiness(
                                    &feed_readiness,
                                    &cfg.name,
                                    FeedReadiness::NotReady {
                                        stage: "subscribe".to_string(),
                                        reason: e.to_string(),
                                    },
                                );
                                if subscribe_failures == 1 {
                                    error!(
                                        "[feed_health] polymarket readiness=NOT_READY stage=subscribe \
                                         error={} retrying_in={:.1}s",
                                        e,
                                        delay.as_secs_f64(),
                                    );
                                    let _ = send_feed_event_with_timeout(
                                        &tx,
                                        MarketEvent::Disconnected {
                                            exchange,
                                            reason: format!("subscription unavailable: {}", e),
                                        },
                                        FEED_SEND_TIMEOUT,
                                    );
                                } else {
                                    warn!(
                                        "[polymarket] Subscribe attempt {} failed: {}; retrying in {:.1}s",
                                        subscribe_failures,
                                        e,
                                        delay.as_secs_f64(),
                                    );
                                }

                                feed.disconnect();
                                let mut replacement = PolymarketMarket::with_liveness(
                                    polymarket_liveness
                                        .as_ref()
                                        .expect("polymarket liveness")
                                        .clone(),
                                );
                                replacement.set_market_tx(tx.clone(), shutdown.clone());
                                feed = Box::new(replacement);

                                if let Some(liveness) = polymarket_liveness.as_ref() {
                                    liveness.set_phase(PolymarketFeedPhase::Backoff);
                                    liveness.heartbeat_feed_loop();
                                }
                                if !sleep_with_shutdown(&shutdown, delay) {
                                    feed.disconnect();
                                    return;
                                }
                            }
                            Err(e) => {
                                set_feed_readiness(
                                    &feed_readiness,
                                    &cfg.name,
                                    FeedReadiness::NotReady {
                                        stage: "subscribe".to_string(),
                                        reason: e.to_string(),
                                    },
                                );
                                error!("[{}] Subscribe error: {}", cfg.name, e);
                                return;
                            }
                        }
                    }

                    let mut backoff = crate::exchange::ReconnectBackoff::new(100, 30_000);

                    loop {
                        if shutdown.load(Ordering::Relaxed) {
                            break;
                        }

                        if let Some(liveness) = polymarket_liveness.as_ref() {
                            liveness.set_phase(PolymarketFeedPhase::Connect);
                            liveness.heartbeat_feed_loop();
                        }
                        if let Err(e) = feed.connect() {
                            let delay = backoff.next_delay();
                            set_feed_readiness(
                                &feed_readiness,
                                &cfg.name,
                                FeedReadiness::NotReady {
                                    stage: "connect".to_string(),
                                    reason: e.to_string(),
                                },
                            );
                            warn!("[{}] Connect error: {}, retrying in {:.1}s...", cfg.name, e, delay.as_secs_f64());
                            if cfg.name == "polymarket" {
                                warn!(
                                    "[feed_health] polymarket readiness=NOT_READY stage=connect error={}",
                                    e,
                                );
                            }
                            let _ = send_feed_event_with_timeout(
                                &tx,
                                MarketEvent::Disconnected {
                                    exchange,
                                    reason: e.to_string(),
                                },
                                FEED_SEND_TIMEOUT,
                            );
                            if let Some(liveness) = polymarket_liveness.as_ref() {
                                liveness.set_phase(PolymarketFeedPhase::Backoff);
                                liveness.heartbeat_feed_loop();
                            }
                            std::thread::sleep(delay);
                            continue;
                        }

                        let connected_at = std::time::Instant::now();
                        // Polymarket's connect() only launches its async CLOB
                        // task. The task emits Connected after the actual
                        // subscription frame has been sent, so do not expose
                        // a premature READY window here.
                        if cfg.name == "polymarket" {
                            set_feed_readiness(
                                &feed_readiness,
                                &cfg.name,
                                FeedReadiness::NotReady {
                                    stage: "subscription".to_string(),
                                    reason: "awaiting CLOB subscription confirmation".to_string(),
                                },
                            );
                            info!(
                                "[feed_health] polymarket readiness=NOT_READY stage=subscription reason=awaiting_clob_subscription"
                            );
                        } else {
                            set_feed_readiness(
                                &feed_readiness,
                                &cfg.name,
                                FeedReadiness::Ready,
                            );
                            let _ = send_feed_event_with_timeout(
                                &tx,
                                MarketEvent::Connected { exchange },
                                FEED_SEND_TIMEOUT,
                            );
                        }
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
                            if let Some(liveness) = polymarket_liveness.as_ref() {
                                liveness.set_phase(PolymarketFeedPhase::Poll);
                                liveness.heartbeat_feed_loop();
                            }
                            if shutdown.load(Ordering::Relaxed) {
                                break;
                            }
                            let next_event = feed.next_event();
                            if let Some(liveness) = polymarket_liveness.as_ref() {
                                liveness.heartbeat_feed_loop();
                            }
                            match next_event {
                                Ok(Some(event)) => {
                                    if !event.has_finite_market_values() {
                                        warn!(
                                            "[feed_health] {} rejected market event with NaN/Infinity before dispatch",
                                            cfg.name,
                                        );
                                        continue;
                                    }
                                    last_data_at = std::time::Instant::now();
                                    if cfg.name == "polymarket" {
                                        if let Some(state) =
                                            polymarket_readiness_transition(&event)
                                        {
                                            set_feed_readiness(
                                                &feed_readiness,
                                                &cfg.name,
                                                state.clone(),
                                            );
                                            match state {
                                                FeedReadiness::Ready => info!(
                                                    "[feed_health] polymarket readiness=READY stage=market_stream"
                                                ),
                                                FeedReadiness::NotReady { reason, .. } => {
                                                    if is_routine_clob_resubscribe(&reason) {
                                                        info!(
                                                            "[feed_health] polymarket readiness=NOT_READY stage=data_stream error={}",
                                                            reason,
                                                        );
                                                    } else {
                                                        warn!(
                                                            "[feed_health] polymarket readiness=NOT_READY stage=data_stream error={}",
                                                            reason,
                                                        );
                                                    }
                                                }
                                                FeedReadiness::Starting => {}
                                            }
                                        }
                                    }
                                    // Paper mode: also send Polymarket events to the sim_v2 core
                                    if let Some(ref stx) = sim_tx {
                                        if let Err(error) = stx.send_timeout(
                                            event.clone(),
                                            FEED_SEND_TIMEOUT,
                                        ) {
                                            request_feed_process_shutdown(&format!(
                                                "{} sim-feed dispatch blocked for {}ms: {}",
                                                cfg.name,
                                                FEED_SEND_TIMEOUT.as_millis(),
                                                error,
                                            ));
                                            return;
                                        }
                                    }
                                    if let Some(liveness) = polymarket_liveness.as_ref() {
                                        liveness.set_phase(PolymarketFeedPhase::Dispatch);
                                    }
                                    if let Err(error) = send_feed_event_with_timeout(
                                        &tx,
                                        event,
                                        FEED_SEND_TIMEOUT,
                                    ) {
                                        set_feed_readiness(
                                            &feed_readiness,
                                            &cfg.name,
                                            FeedReadiness::NotReady {
                                                stage: "dispatch".to_string(),
                                                reason: error.to_string(),
                                            },
                                        );
                                        request_feed_process_shutdown(&format!(
                                            "{} market dispatch blocked for {}ms: {}",
                                            cfg.name,
                                            FEED_SEND_TIMEOUT.as_millis(),
                                            error,
                                        ));
                                        return;
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
                                        set_feed_readiness(
                                            &feed_readiness,
                                            &cfg.name,
                                            FeedReadiness::NotReady {
                                                stage: "data_stream".to_string(),
                                                reason: "data timeout".to_string(),
                                            },
                                        );
                                        warn!("[{}] No data for {:.0}s, reconnecting...",
                                            cfg.name, last_data_at.elapsed().as_secs_f64());
                                        let _ = send_feed_event_with_timeout(
                                            &tx,
                                            MarketEvent::Disconnected {
                                                exchange,
                                                reason: "data timeout".to_string(),
                                            },
                                            FEED_SEND_TIMEOUT,
                                        );
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
                                    set_feed_readiness(
                                        &feed_readiness,
                                        &cfg.name,
                                        FeedReadiness::NotReady {
                                            stage: "data_stream".to_string(),
                                            reason: e.to_string(),
                                        },
                                    );
                                    warn!("[{}] Feed error: {}", cfg.name, e);
                                    let _ = send_feed_event_with_timeout(
                                        &tx,
                                        MarketEvent::Disconnected {
                                            exchange,
                                            reason: e.to_string(),
                                        },
                                        FEED_SEND_TIMEOUT,
                                    );
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
                        if let Some(liveness) = polymarket_liveness.as_ref() {
                            liveness.set_phase(PolymarketFeedPhase::Backoff);
                            liveness.heartbeat_feed_loop();
                        }
                        std::thread::sleep(delay);
                    }

                    feed.disconnect();
                    if let Some(liveness) = polymarket_liveness.as_ref() {
                        liveness.set_phase(PolymarketFeedPhase::Stopped);
                        liveness.heartbeat_feed_loop();
                    }
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
        let hex_cfg = self
            .config
            .exchanges
            .iter()
            .find(|e| e.name == "hexmarket" && e.enabled)?;
        let private_key = &hex_cfg.private_key;
        let mnemonic = &hex_cfg.mnemonic;
        let wss_url = &hex_cfg.wss_url;

        if private_key.is_empty() && mnemonic.is_empty() {
            info!("[Engine] No hex wallet configured, skipping user feed");
            return None;
        }

        use crate::exchange::hexmarket::auth::{resolve_auth, wss_url_or_default};
        let wss_url = wss_url_or_default(wss_url).to_string();
        let api_url_prefix =
            crate::exchange::hexmarket::auth::api_url_prefix_or_default(&hex_cfg.api_url_prefix);

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
    /// means they all share the process-wide role-separated HTTP/1.1 pools.
    /// **Multi-instance** SharedState builder — Phase 2a of the
    /// multi-strategy refactor. Loads `secrets.toml` and constructs
    /// one `Arc<SharedState>` per polymaker strategy in the config,
    /// keyed by its `instance_id` from `[strategies.params].instance_id`.
    ///
    /// Common Polymarket transport config (`clob_version`,
    /// `api_url_prefix`, `use_batch_orders`, `rate_limit_per_second`,
    /// `http_timeout_*_ms`) still lives in `[[exchanges]] polymarket`
    /// and is shared across all instances (shared role pools, single
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
        self.build_poly_shared_states_map_with_shutdown(ShutdownToken::new())
    }

    fn build_poly_shared_states_map_with_shutdown(
        &self,
        shutdown_token: ShutdownToken,
    ) -> HashMap<String, Arc<crate::exchange::polymarket::trade::SharedState>> {
        use crate::config::SecretsFile;

        let mut out: HashMap<String, Arc<crate::exchange::polymarket::trade::SharedState>> =
            HashMap::new();

        let poly_cfg = match self
            .config
            .exchanges
            .iter()
            .find(|e| e.name == "polymarket" && e.enabled)
        {
            Some(c) => c,
            None => {
                info!("[Engine] Polymarket exchange disabled; no SharedState built");
                return out;
            }
        };

        let enabled_polymarket_accounts: HashSet<String> = self
            .config
            .strategies
            .iter()
            .filter(|strategy| {
                strategy.enabled
                    && self
                        .registry
                        .capabilities(&strategy.name)
                        .needs_poly_user_feed
            })
            .map(|strategy| strategy.account_id().to_string())
            .collect();
        let bootstrap_accounts = match validate_account_ledger_bootstrap_accounts(
            &poly_cfg.account_ledger_bootstrap_accounts,
            poly_cfg.account_ledger_require_existing,
            &enabled_polymarket_accounts,
        ) {
            Ok(accounts) => accounts,
            Err(error) => {
                error!("[Engine] refusing every Polymarket strategy: {}", error,);
                return out;
            }
        };

        // Build the complete account→instances routing table before the first
        // SharedState prewarms transport. Every shared wallet receives one
        // physical pool group sized from N: order=4N, reconcile=2N,
        // gap_replay=2. Global pools are fixed fallbacks and never duplicate
        // these account slots.
        if !hexagent_runtime::http1_pool::account_pools_ready() {
            let mut accounts: HashMap<String, Vec<String>> = HashMap::new();
            for sc in self.config.strategies.iter().filter(|sc| {
                sc.enabled
                    && self.registry.capabilities(&sc.name).needs_poly_user_feed
                    && !sc.instance_id.is_empty()
            }) {
                accounts
                    .entry(sc.account_id().to_string())
                    .or_default()
                    .push(sc.instance_id.clone());
            }
            if !accounts.is_empty() {
                if let Err(error) = hexagent_runtime::http1_pool::init_account_pools(&accounts) {
                    warn!(
                        "[Engine] account admission pools not initialised before prewarm: {}",
                        error,
                    );
                }
            }
        }

        // Install global FAST/CANCEL timeout ONCE — shared by all instances.
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
                    secrets_path.display(),
                    e,
                );
                return out;
            }
        };
        // Engine callers may construct `Config` directly instead of going
        // through `Config::load`. Load the shared builder/Polygon secrets here
        // as part of the same live preflight that resolves account wallets, so
        // maintenance never depends on an earlier CLI-only side effect.
        secrets.apply_shared_to_env();

        // Build one SharedState per unique `account_id` (= one
        // Polymarket wallet / signer / user-feed / nonce stream).
        // Multiple strategy instances (distinct `instance_id`, e.g.
        // BTC + ETH) that share an `account_id` share ONE SharedState.
        // The returned map is keyed by `instance_id` (every enabled
        // instance present, shared accounts pointing at the same Arc)
        // so instance-keyed lookups (build_strategies / executor)
        // resolve to the shared state. Per-account spawners
        // (user-feed / heartbeat) dedup by Arc identity downstream.
        //
        // `account_id` defaults to `instance_id` when unset, so the
        // single-instance (one wallet per strategy) path is unchanged.
        let mut by_account: HashMap<String, Arc<crate::exchange::polymarket::trade::SharedState>> =
            HashMap::new();
        let mut wallet_accounts: HashMap<String, String> = HashMap::new();
        let mut bootstrapped_ledgers: HashMap<String, PathBuf> = HashMap::new();
        for sc in &self.config.strategies {
            if !sc.enabled || !self.registry.capabilities(&sc.name).needs_poly_user_feed {
                continue;
            }
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
            let account_id = sc.account_id().to_string();
            // Another instance already built this account's SharedState
            // → share it (one wallet, one user-feed, one nonce stream).
            if let Some(shared) = by_account.get(&account_id) {
                shared
                    .account_state
                    .register_instance(&instance_id, sc.account_allocation_weight);
                if let Some(scope) = sc.params.get("event_series_slug").and_then(|v| v.as_str()) {
                    shared
                        .account_state
                        .register_market_scope(&instance_id, scope);
                }
                info!(
                    "[Engine] instance_id={} shares Polymarket account `{}` \
                     with an earlier instance — reusing its SharedState",
                    instance_id, account_id,
                );
                out.insert(instance_id, shared.clone());
                continue;
            }
            let creds = match secrets.poly_for(&account_id) {
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
            // The global POLY_* env is single-valued — with multiple
            // accounts the LAST one built here wins. We keep setting it
            // for the single-account / CLI fallback, but ALSO register
            // this account's split/redeem creds in the per-account
            // wallet registry (keyed by account_id) so the maintenance
            // thread resolves the RIGHT wallet under multi-account live
            // (P4 — see `spawn_maintenance_thread(account_id)`).
            // builder_code is sourced solely from the shared `[builder]`
            // block — one attribution code for all of the operator's
            // wallets (per-instance `[poly.<id>].builder_code` was removed).
            let builder_code = secrets
                .builder
                .as_ref()
                .map(|b| b.builder_code.clone())
                .unwrap_or_default();
            let neg_risk = false;
            let sig_type =
                crate::exchange::polymarket::signer::parse_signature_type(&creds.signature_type);
            let clob_version = match crate::exchange::polymarket::trade::ClobVersion::parse(
                &poly_cfg.clob_version,
            ) {
                Ok(version) => version,
                Err(error) => {
                    log::error!(
                        "[Engine] refusing Polymarket account {}: {}",
                        account_id,
                        error,
                    );
                    continue;
                }
            };
            let ledger_path = if poly_cfg.account_ledger_dir.trim().is_empty() {
                None
            } else {
                let safe_account: String = account_id
                    .chars()
                    .map(|ch| {
                        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                            ch
                        } else {
                            '_'
                        }
                    })
                    .collect();
                Some(
                    PathBuf::from(&poly_cfg.account_ledger_dir)
                        .join(format!("{}.json", safe_account)),
                )
            };
            let ledger_status = match validate_required_account_ledger(
                ledger_path.as_deref(),
                poly_cfg.account_ledger_require_existing,
                bootstrap_accounts.contains(&account_id),
            ) {
                Ok(status) => status,
                Err(error) => {
                    error!(
                        "[Engine] refusing every Polymarket strategy: account={} {}; a fresh ledger would lose historical/disabled-instance ownership and offline auto-redeem attribution",
                        account_id, error,
                    );
                    return HashMap::new();
                }
            };
            if ledger_status == RequiredAccountLedgerStatus::BootstrapMissing {
                let Some(path) = ledger_path.as_ref() else {
                    error!(
                        "[Engine] refusing every Polymarket strategy: account={} bootstrap admitted without a durable ledger path",
                        account_id,
                    );
                    return HashMap::new();
                };
                warn!(
                    "[Engine] ONE-TIME ACCOUNT LEDGER BOOTSTRAP account={} path={}; only a genuinely new wallet with no historical bot activity may use this path",
                    account_id,
                    account_ledger_display_path(path).display(),
                );
                bootstrapped_ledgers.insert(account_id.clone(), path.clone());
            }
            if let Some(path) = ledger_path.as_deref() {
                info!(
                    "[Engine] account={} durable ledger path={} require_existing={} bootstrap={} bytes={}",
                    account_id,
                    account_ledger_display_path(path).display(),
                    poly_cfg.account_ledger_require_existing,
                    ledger_status == RequiredAccountLedgerStatus::BootstrapMissing,
                    std::fs::metadata(path)
                        .map(|metadata| metadata.len())
                        .unwrap_or(0),
                );
            }
            match PolymarketTrade::new_with_pool_for_startup_query_repair_and_shutdown(
                &creds.api_key,
                &creds.api_secret,
                &creds.api_passphrase,
                &creds.private_key,
                neg_risk,
                poly_cfg.rate_limit_per_second,
                sig_type,
                clob_version,
                &builder_code,
                &poly_cfg.api_url_prefix,
                poly_cfg.use_batch_orders,
                &account_id,
                &creds.funder,
                crate::exchange::polymarket::trade::GapReplayConfig {
                    interval_ms: poly_cfg.gap_replay_interval_ms,
                    periodic_rewind_ms: poly_cfg.gap_replay_periodic_rewind_ms,
                    reconnect_rewind_ms: poly_cfg.gap_replay_reconnect_rewind_ms,
                },
                ledger_path.as_deref(),
                shutdown_token.clone(),
            ) {
                Ok(trade) => {
                    if let Err(error) = register_polymarket_wallet_identity(
                        &mut wallet_accounts,
                        &account_id,
                        trade.order_maker_address(),
                    ) {
                        error!("[Engine] {error}; refusing to start any Polymarket strategy");
                        return HashMap::new();
                    }
                    // Publish credentials only after physical-wallet identity
                    // validation. A rejected alias must not remain reachable
                    // through either the global fallback or account registry.
                    crate::exchange::polymarket::cli_account::apply_creds_to_env(creds);
                    crate::exchange::polymarket::wallet::register_account_wallet(
                        &account_id,
                        &creds.private_key,
                        &creds.signature_type,
                        &creds.funder,
                    );
                    let maintenance_enabled = !self.config.general.all_probe
                        && self.config.strategies.iter().any(|strategy| {
                            strategy.enabled
                                && strategy.account_id() == account_id
                                && self
                                    .registry
                                    .capabilities(&strategy.name)
                                    .needs_poly_user_feed
                                && strategy
                                    .params
                                    .get("redeem_split_enabled")
                                    .and_then(|value| value.as_bool())
                                    .unwrap_or(false)
                        });
                    if maintenance_enabled {
                        if let Err(error) =
                            crate::exchange::polymarket::wallet::validate_registered_account_wallet(
                                &account_id,
                                trade.order_maker_address(),
                            )
                        {
                            error!(
                                "[Engine] account={} maintenance wallet preflight failed: {}; refusing Polymarket startup",
                                account_id, error,
                            );
                            return HashMap::new();
                        }
                        info!(
                            "[Engine] account={} maintenance wallet preflight OK maker/funder={}",
                            account_id,
                            trade.order_maker_address(),
                        );
                    }
                    trade.prewarm_connections();
                    let shared = trade.shared_state();
                    shared
                        .account_state
                        .register_instance(&instance_id, sc.account_allocation_weight);
                    if let Some(scope) = sc.params.get("event_series_slug").and_then(|v| v.as_str())
                    {
                        shared
                            .account_state
                            .register_market_scope(&instance_id, scope);
                    }
                    if maintenance_enabled {
                        match crate::exchange::polymarket::wallet::recover_registered_account_maintenance_operations(
                            &account_id,
                            &shared.account_state,
                        ) {
                            Ok(0) => {}
                            Ok(recovered) => info!(
                                "[Engine] account={} startup maintenance finality recovery completed for {} operation(s)",
                                account_id, recovered,
                            ),
                            Err(error) => warn!(
                                "[Engine] account={} startup maintenance finality recovery remains unresolved: {}; account admission stays fail-closed",
                                account_id, error,
                            ),
                        }
                    }
                    info!(
                        "[Engine] Built Polymarket SharedState for account_id={} \
                         (first instance_id={} sig_type={} builder_code={})",
                        account_id,
                        instance_id,
                        creds.signature_type,
                        if builder_code.is_empty() {
                            "<none>"
                        } else {
                            &builder_code
                        },
                    );
                    by_account.insert(account_id, shared.clone());
                    out.insert(instance_id, shared);
                }
                Err(e) => {
                    warn!(
                        "[Engine] Failed to init Polymarket SharedState for account_id={} \
                         (instance_id={}): {}",
                        account_id, instance_id, e,
                    );
                }
            }
        }

        // A bootstrap exception is safe only after every enabled sibling and
        // market scope has been written to the new ledger and fsynced. Keep
        // strategy workers stopped until the on-disk JSON envelope and account
        // identity are independently verified.
        for (account_id, path) in &bootstrapped_ledgers {
            let Some(shared) = by_account.get(account_id) else {
                error!(
                    "[Engine] refusing every Polymarket strategy: bootstrapped account={} did not build a SharedState",
                    account_id,
                );
                return HashMap::new();
            };
            if let Err(error) = shared
                .account_state
                .flush_persistence(std::time::Duration::from_secs(5))
            {
                error!(
                    "[Engine] refusing every Polymarket strategy: account={} bootstrap ledger fsync failed: {}",
                    account_id, error,
                );
                return HashMap::new();
            }
            let bytes = match verify_bootstrapped_account_ledger(path, account_id) {
                Ok(bytes) => bytes,
                Err(error) => {
                    error!(
                        "[Engine] refusing every Polymarket strategy: account={} bootstrap verification failed: {}",
                        account_id, error,
                    );
                    return HashMap::new();
                }
            };
            warn!(
                "[Engine] ONE-TIME ACCOUNT LEDGER BOOTSTRAP COMPLETE account={} path={} bytes={}; remove `{}` from account_ledger_bootstrap_accounts before the next restart",
                account_id,
                account_ledger_display_path(path).display(),
                bytes,
                account_id,
            );
        }

        // The persisted account ledger may contain orders that were live when
        // the previous process stopped. Reconcile them once per account only
        // after every configured instance has joined the shared state, and
        // before any strategy worker can quote against the restored balances.
        for (account_id, shared) in &by_account {
            let configured_strategies: Vec<_> = self
                .config
                .strategies
                .iter()
                .filter(|strategy| {
                    strategy.enabled
                        && self
                            .registry
                            .capabilities(&strategy.name)
                            .needs_poly_user_feed
                        && strategy.account_id() == account_id
                        && !strategy.instance_id.is_empty()
                        && out.contains_key(&strategy.instance_id)
                })
                .collect();
            // `enabled=false` stops new workers/orders; it does not erase the
            // instance's historical ownership. Keep disabled configured
            // instances in the registry so offline auto-redeem can credit
            // their persisted positions instead of treating them as orphaned.
            let configured_instances: HashSet<String> = self
                .config
                .strategies
                .iter()
                .filter(|strategy| {
                    self.registry
                        .capabilities(&strategy.name)
                        .needs_poly_user_feed
                        && strategy.account_id() == account_id
                        && !strategy.instance_id.is_empty()
                })
                .map(|strategy| strategy.instance_id.clone())
                .collect();
            let target_weights: std::collections::BTreeMap<String, f64> = configured_strategies
                .iter()
                .map(|strategy| {
                    let weight = if strategy.account_allocation_weight.is_finite()
                        && strategy.account_allocation_weight > 0.0
                    {
                        strategy.account_allocation_weight
                    } else {
                        1.0
                    };
                    (strategy.instance_id.clone(), weight)
                })
                .collect();
            let migration_ids: HashSet<&str> = configured_strategies
                .iter()
                .map(|strategy| strategy.account_allocation_migration_id.trim())
                .filter(|migration_id| !migration_id.is_empty())
                .collect();
            let every_member_acknowledged = configured_strategies
                .iter()
                .all(|strategy| !strategy.account_allocation_migration_id.trim().is_empty());
            if shared.account_state.is_seeded() && !migration_ids.is_empty() {
                if migration_ids.len() != 1 || !every_member_acknowledged {
                    error!(
                        "[Engine] account={} cash allocation migration rejected: every configured sibling must supply the same non-empty account_allocation_migration_id",
                        account_id,
                    );
                } else if let Some(migration_id) = migration_ids.iter().next() {
                    match shared
                        .account_state
                        .migrate_cash_allocation(migration_id, &target_weights)
                    {
                        Ok(migration) => info!(
                            "[Engine] account={} applied/idempotently recovered cash allocation migration={} targets={:?}",
                            account_id, migration.operation_id, migration.target_weights,
                        ),
                        Err(error) => error!(
                            "[Engine] account={} cash allocation migration={} rejected: {}",
                            account_id, migration_id, error,
                        ),
                    }
                }
            }
            shared
                .account_state
                .reconcile_configured_instances(&configured_instances);
            let orphan_repair = PolymarketTrade::from_shared(shared.clone(), "", "")
                .reconcile_persisted_orphan_order_anomalies();
            if orphan_repair.examined > 0 {
                info!(
                    "[Engine] Polymarket account={} startup orphan ownership audit examined={} rebuilt={} tombstoned={} unresolved={} failures={:?}",
                    account_id,
                    orphan_repair.examined,
                    orphan_repair.rebuilt,
                    orphan_repair.tombstoned,
                    orphan_repair.unresolved,
                    orphan_repair.failures,
                );
            }
            let startup_query_repairs = shared
                .account_state
                .startup_query_repair_pending_order_ids();
            if !startup_query_repairs.is_empty() {
                warn!(
                    "[Engine] Polymarket account={} repairing {} durable FAILED-trade reservation mismatch(es) from authoritative order queries before LIVE admission: coids={:?}",
                    account_id,
                    startup_query_repairs.len(),
                    startup_query_repairs,
                );
            }
            let recovery = PolymarketTrade::from_shared(shared.clone(), "", "");
            let unresolved = recovery.reconcile_recovered_orders();
            if !startup_query_repairs.is_empty() {
                if let Err(error) = shared
                    .account_state
                    .flush_persistence(std::time::Duration::from_secs(5))
                {
                    error!(
                        "[Engine] refusing every Polymarket strategy: account={} startup query repair fsync failed: {}",
                        account_id, error,
                    );
                    return HashMap::new();
                }
                let unresolved_query_repairs = shared
                    .account_state
                    .startup_query_repair_pending_order_ids();
                if !unresolved_query_repairs.is_empty() {
                    error!(
                        "[Engine] refusing every Polymarket strategy: account={} authoritative startup query could not resolve repaired ledger order(s) {:?}; conservative reservations were fsynced and no feeds or strategies will start",
                        account_id, unresolved_query_repairs,
                    );
                    return HashMap::new();
                }
                info!(
                    "[Engine] Polymarket account={} startup ledger query repair complete for {} order(s); repaired state fsynced before LIVE admission",
                    account_id,
                    startup_query_repairs.len(),
                );
            }
            if unresolved > 0 {
                warn!(
                    "[Engine] Polymarket account={} startup recovery pending for {} order(s); quoting continues under retained reservations while owner maintenance waits for authoritative metadata",
                    account_id, unresolved,
                );
            }
        }

        info!(
            "[Engine] Built {} Polymarket SharedState(s) across {} account(s) for {} instance(s)",
            by_account.len(),
            by_account.len(),
            out.len(),
        );
        // Keep every pooled CLOB connection warm through quiet stretches
        // (event rollovers, closed sessions). The one-shot staggered
        // prewarm in `prewarm_connections()` only covers startup — idle
        // slots evicted later (hyper pool_idle_timeout, or the LB's own
        // idle close) would otherwise pay DNS+TCP+TLS inside the 2000 ms
        // order budget on their next pick. `/time` is free and
        // unauthenticated, same endpoint the prewarm uses.
        if !by_account.is_empty() {
            hexagent_runtime::http1_pool::spawn_keep_warm(
                "clob",
                format!("{}/time", poly_cfg.api_url_prefix.trim_end_matches('/')),
                std::time::Duration::from_secs(20),
            );
        }
        out
    }

    /// Deduplicate an `instance_id → Arc<SharedState>` map down to one
    /// representative `(instance_id, Arc)` per unique `account_id`.
    /// Used by per-account spawners (user-feed,
    /// heartbeat) so two strategy instances sharing one wallet don't
    /// open two authenticated user streams (which would double-count
    /// fills) or two redundant heartbeats. Deterministic order: sorted
    /// by the lexicographically-smallest instance_id mapping to each account.
    fn dedup_states_by_account(
        states: &HashMap<String, Arc<crate::exchange::polymarket::trade::SharedState>>,
    ) -> Vec<(String, Arc<crate::exchange::polymarket::trade::SharedState>)> {
        let mut seen = HashSet::new();
        let mut out: Vec<(String, Arc<crate::exchange::polymarket::trade::SharedState>)> =
            Vec::new();
        let mut keys: Vec<&String> = states.keys().collect();
        keys.sort();
        for k in keys {
            if let Some(s) = states.get(k) {
                let account_id = s.account_state.account_id();
                if !seen.insert(account_id.to_string()) {
                    continue;
                }
                out.push((k.clone(), s.clone()));
            }
        }
        out
    }

    /// **Single-instance shim** — Phase 2a back-compat. Returns the
    /// FIRST SharedState from the multi-instance map. Existing
    /// callsites (user_feed / heartbeat / rtt_probe / executor) still
    /// consume one SharedState until Phase 2b–2e fans them out per
    /// instance. Logs a one-time WARN when more than one instance is
    /// configured (only the first will actually run).
    pub fn build_poly_shared_state(
        &self,
    ) -> Option<Arc<crate::exchange::polymarket::trade::SharedState>> {
        let map = self.build_poly_shared_states_map();
        if map.len() > 1 {
            let ids: Vec<&String> = map.keys().collect();
            warn!(
                "[Engine] {} polymaker instances configured but Phase 2b–2e not yet wired \
                 — only one will receive user_feed / heartbeat / rtt_probe / executor traffic. \
                 Instances: {:?}",
                map.len(),
                ids,
            );
        }
        // BTreeMap-ish stable pick: sort by key so "first" is deterministic
        // (HashMap iter order is randomised, would surface non-determinism).
        let mut keys: Vec<&String> = map.keys().collect();
        keys.sort();
        keys.first().and_then(|k| map.get(*k).cloned())
    }

    /// Spawn one authenticated user-feed thread per Polymarket account.
    /// Shared-wallet instances reuse the same `SharedState`, user stream and
    /// gap-replay loop; parsed updates then enter the common update router,
    /// which resolves client_order_id ownership to one strategy instance.
    pub fn spawn_poly_user_feeds(
        &self,
        update_tx: Sender<OrderUpdate>,
        shutdown: Arc<AtomicBool>,
        states: &HashMap<String, Arc<crate::exchange::polymarket::trade::SharedState>>,
    ) -> Vec<thread::JoinHandle<()>> {
        let mut handles = Vec::with_capacity(states.len().saturating_mul(2));
        if states.is_empty() {
            info!("[Engine] No Polymarket SharedState(s); skipping user feeds");
            return handles;
        }
        // One user feed per ACCOUNT (not per instance): instances that
        // share a wallet share one authenticated stream — two streams
        // on the same wallet would deliver every fill twice and
        // double-count inventory. Deterministic spawn order.
        for (id, shared) in Self::dedup_states_by_account(states) {
            let account_id = shared.account_state.account_id().to_string();
            let api_key = shared.auth.api_key.clone();
            let api_secret_b64 = shared.auth.api_secret_b64().to_string();
            let passphrase = shared.auth.passphrase.clone();
            match crate::exchange::polymarket::user_feed::spawn_user_feed(
                &api_key,
                &api_secret_b64,
                &passphrase,
                shared.clone(),
                update_tx.clone(),
                shutdown.clone(),
            ) {
                Ok(h) => {
                    info!(
                        "[Engine] Polymarket user feed started for account_id={} (lead instance_id={})",
                        account_id, id,
                    );
                    handles.push(h);
                }
                Err(e) => {
                    warn!(
                        "[Engine] Failed to start Polymarket user feed for account_id={} (lead instance_id={}): {}",
                        account_id, id, e,
                    );
                }
            }

            // Startup's bounded reconciliation is intentionally short so boot
            // cannot hang. Continue retrying unresolved durable orders on an
            // account-scoped background thread until their complete terminal
            // trade audit is observed or shutdown begins. Quote admission
            // continues throughout under durable reservations; only
            // balance-changing owner maintenance waits for pre-audit metadata.
            // Exact trade replay proceeds concurrently after bridge handoff.
            let recovery_shared = shared.clone();
            let recovery_shutdown = shutdown.clone();
            let recovery_account_id = account_id.clone();
            let recovery_update_tx = update_tx.clone();
            match thread::Builder::new()
                .name(format!("poly-order-recovery-{account_id}"))
                .spawn(move || {
                    crate::os_tune::pin_background("poly-order-recovery");
                    let mut audit_generation =
                        recovery_shared.account_state.order_audit_generation();
                    let mut warned_pending_ids = Vec::<String>::new();
                    let mut delivered_audit_fingerprints = HashMap::<String, String>::new();
                    let mut completed_recoveries = 0u64;
                    let mut completion_log_started = std::time::Instant::now();
                    let mut orphan_audit_started = std::time::Instant::now()
                        .checked_sub(std::time::Duration::from_secs(30))
                        .unwrap_or_else(std::time::Instant::now);
                    while !recovery_shutdown.load(Ordering::Relaxed) {
                        if orphan_audit_started.elapsed()
                            >= std::time::Duration::from_secs(30)
                        {
                            let orphan_count = recovery_shared
                                .account_state
                                .persisted_orphan_order_anomalies()
                                .len();
                            if orphan_count > 0 {
                                let recovery = PolymarketTrade::from_shared(
                                    recovery_shared.clone(),
                                    "",
                                    "",
                                );
                                let repaired =
                                    recovery.reconcile_persisted_orphan_order_anomalies();
                                if repaired.rebuilt > 0 || repaired.tombstoned > 0 {
                                    info!(
                                        "[Engine] Polymarket account={} background orphan ownership audit rebuilt={} tombstoned={} unresolved={} failures={:?}",
                                        recovery_account_id,
                                        repaired.rebuilt,
                                        repaired.tombstoned,
                                        repaired.unresolved,
                                        repaired.failures,
                                    );
                                } else {
                                    log::debug!(
                                        "[Engine] Polymarket account={} background orphan ownership audit unchanged={} failures={:?}",
                                        recovery_account_id,
                                        repaired.unresolved,
                                        repaired.failures,
                                    );
                                }
                            }
                            orphan_audit_started = std::time::Instant::now();
                        }
                        let (before_recovery_pending, before_routine_cancel_audits) =
                            recovery_shared.account_state.pending_order_audit_counts();
                        let pending = before_recovery_pending
                            .saturating_add(before_routine_cancel_audits);
                        if pending > 0 {
                            let recovery = PolymarketTrade::from_shared(
                                recovery_shared.clone(),
                                "",
                                "",
                            );
                            let (unresolved, updates) =
                                recovery.reconcile_recovered_orders_with_updates();
                            for update in updates {
                                if let Some(audit) = update.order_audit.as_ref() {
                                    let fingerprint = format!(
                                        "{:?}|{:?}|{:?}|{:?}",
                                        update.status,
                                        audit.original_size,
                                        audit.size_matched,
                                        audit.associate_trades,
                                    );
                                    if delivered_audit_fingerprints
                                        .get(&update.client_order_id)
                                        == Some(&fingerprint)
                                    {
                                        continue;
                                    }
                                    delivered_audit_fingerprints.insert(
                                        update.client_order_id.clone(),
                                        fingerprint,
                                    );
                                }
                                if send_root_private_update(&recovery_update_tx, update).is_err() {
                                    return;
                                }
                            }
                            let (after_recovery_pending, after_routine_cancel_audits) =
                                recovery_shared.account_state.pending_order_audit_counts();
                            completed_recoveries = completed_recoveries.saturating_add(
                                u64::try_from(
                                    before_recovery_pending
                                        .saturating_sub(after_recovery_pending),
                                )
                                .unwrap_or(u64::MAX),
                            );
                            if unresolved > 0 {
                                let pending_ids = recovery_shared
                                    .account_state
                                    .recovery_pending_order_ids();
                                if pending_ids != warned_pending_ids {
                                    warn!(
                                        "[Engine] Polymarket account={} background recovery still pending: {} order(s) coids={:?}",
                                        recovery_account_id,
                                        unresolved,
                                        pending_ids,
                                    );
                                    warned_pending_ids = pending_ids;
                                } else {
                                    log::debug!(
                                        "[Engine] Polymarket account={} background recovery unchanged: {} order(s)",
                                        recovery_account_id,
                                        unresolved,
                                    );
                                }
                            } else {
                                warned_pending_ids.clear();
                                if after_routine_cancel_audits > 0 {
                                    log::debug!(
                                        "[Engine] Polymarket account={} routine cancel audits still pending: {} order(s)",
                                        recovery_account_id,
                                        after_routine_cancel_audits,
                                    );
                                }
                            }
                        } else {
                            // A later audit for the same coid is a fresh edge
                            // and should receive one new WARN.
                            warned_pending_ids.clear();
                        }

                        // Flush on the worker's regular wakeup as well as on a
                        // completion edge. Low-volume accounts must not retain
                        // a non-empty aggregation bucket indefinitely.
                        if completed_recoveries > 0
                            && completion_log_started.elapsed()
                                >= std::time::Duration::from_secs(30)
                        {
                            info!(
                                "[Engine] Polymarket account={} background order recovery summary completed={} window_ms={}",
                                recovery_account_id,
                                completed_recoveries,
                                completion_log_started.elapsed().as_millis(),
                            );
                            completed_recoveries = 0;
                            completion_log_started = std::time::Instant::now();
                        }
                        // New terminal audits wake this account worker on the
                        // insertion edge. The bounded 500 ms timeout is only
                        // for shutdown responsiveness and retrying unresolved
                        // HTTP/trade rows; there is no longer a 5 s idle poll
                        // that can delay a newly-created safety blocker.
                        audit_generation = recovery_shared
                            .account_state
                            .wait_for_order_audit_work(
                                audit_generation,
                                std::time::Duration::from_millis(500),
                            );
                    }
                    if completed_recoveries > 0 {
                        info!(
                            "[Engine] Polymarket account={} background order recovery summary completed={} window_ms={} shutdown=true",
                            recovery_account_id,
                            completed_recoveries,
                            completion_log_started.elapsed().as_millis(),
                        );
                    }
                })
            {
                Ok(handle) => handles.push(handle),
                Err(error) => warn!(
                    "[Engine] Failed to start Polymarket order recovery for account={}: {}",
                    account_id,
                    error,
                ),
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
            &api_key,
            &api_secret_b64,
            &passphrase,
            shared,
            update_tx,
            shutdown,
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
        // One heartbeat per ACCOUNT — shared-wallet instances share the
        // same session keep-alive (see `dedup_states_by_account`).
        for (id, shared) in Self::dedup_states_by_account(states) {
            let api_key = shared.auth.api_key.clone();
            // Heartbeat route places no orders → instance_id unused ("").
            let trade = PolymarketTrade::from_shared(shared, &api_key, "");
            handles.push(trade.spawn_heartbeat(shutdown.clone()));
            info!(
                "[Engine] Polymarket heartbeat started for account (lead instance_id={})",
                id
            );
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
        // Heartbeat route places no orders → instance_id unused ("").
        let trade = PolymarketTrade::from_shared(shared, &api_key, "");
        Some(trade.spawn_heartbeat(shutdown))
    }

    /// Spawn the execution thread that processes Signal → OrderUpdate.
    pub fn spawn_execution_thread(
        &self,
        signal_rx: Receiver<RoutedSignal>,
        update_tx: Sender<RoutedOrderUpdate>,
    ) -> thread::JoinHandle<()> {
        // Standalone caller (no live polymaker wiring) — pass an empty
        // per-instance stale-threshold map. The executor's fallback
        // dispatch will use the 150 ms legacy default for every signal.
        self.spawn_execution_thread_with_poly(signal_rx, update_tx, HashMap::new(), HashMap::new())
    }

    /// Same as `spawn_execution_thread` but wires a pre-built Polymarket
    /// `SharedState` into the LiveRouter so the execution thread shares its
    /// HTTP agent / connection pool with the heartbeat and user_feed.
    pub fn spawn_execution_thread_with_poly(
        &self,
        signal_rx: Receiver<RoutedSignal>,
        update_tx: Sender<RoutedOrderUpdate>,
        poly_states: HashMap<String, Arc<crate::exchange::polymarket::trade::SharedState>>,
        stale_threshold_handles: HashMap<String, Arc<std::sync::atomic::AtomicU64>>,
    ) -> thread::JoinHandle<()> {
        let (shutdown_done_tx, _shutdown_done_rx) = bounded::<()>(1);
        self.spawn_execution_thread_with_poly_shutdown(
            signal_rx,
            update_tx,
            poly_states,
            stale_threshold_handles,
            shutdown_done_tx,
            ShutdownToken::new(),
        )
    }

    fn spawn_execution_thread_with_poly_shutdown(
        &self,
        signal_rx: Receiver<RoutedSignal>,
        update_tx: Sender<RoutedOrderUpdate>,
        poly_states: HashMap<String, Arc<crate::exchange::polymarket::trade::SharedState>>,
        stale_threshold_handles: HashMap<String, Arc<std::sync::atomic::AtomicU64>>,
        shutdown_done_tx: Sender<()>,
        shutdown_token: ShutdownToken,
    ) -> thread::JoinHandle<()> {
        let config = self.config.clone();
        let hex_max_connections = config
            .exchanges
            .iter()
            .find(|e| e.name == "hexmarket")
            .map(|e| e.max_connections)
            .unwrap_or(4);
        // Pre-compute (with the registry, before the `move`) which strategies
        // need Hexmarket execution workers, so the spawned thread — which only
        // captures a `config` clone — gates on a capability, not a strategy name.
        let hex_worker_flags: Vec<bool> = config
            .strategies
            .iter()
            .map(|s| self.registry.capabilities(&s.name).needs_hex_workers)
            .collect();
        // Same enabled-strategy order used to allocate owner lanes and build
        // strategy workers. Numeric ownership is authoritative at execution;
        // embedded string IDs are validated but no longer select an account.
        let mut active_owner_index = 0usize;
        let owner_instance_ids: Vec<String> = config
            .strategies
            .iter()
            .filter_map(|strategy| {
                if !strategy.enabled {
                    return None;
                }
                let owner_index = active_owner_index;
                active_owner_index += 1;
                let instance_id = if strategy.instance_id.trim().is_empty() {
                    format!("{}_{owner_index}", strategy.name)
                } else {
                    strategy.instance_id.clone()
                };
                Some(instance_id)
            })
            .collect();

        thread::Builder::new()
            .name("execution".into())
            .spawn(move || {
                crate::os_tune::pin_execution("execution");
                crate::latency::prepare_polymarket_order_stages();
                let execution_shutdown_rx = shutdown_token.subscribe();
                let hex_cfg = config.exchanges.iter().find(|e| e.name == "hexmarket");
                let mut instance_pools: HashMap<
                    String,
                    Vec<Sender<(Signal, ExecutorUpdateSender)>>,
                > = HashMap::new();

                // Gate Hexmarket execution workers via the pre-computed
                // capability flags (needs_hex_workers), not a strategy name.
                for (idx, strategy_cfg) in config.strategies.iter().enumerate() {
                    if !hex_worker_flags[idx] || !strategy_cfg.enabled {
                        continue;
                    }
                    let owner = config.strategies[..idx]
                        .iter()
                        .filter(|strategy| strategy.enabled)
                        .count();
                    let instance_id = owner_instance_ids
                        .get(owner)
                        .cloned()
                        .unwrap_or_else(|| strategy_cfg.instance_id.clone());

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

                    let pool: Vec<Sender<(Signal, ExecutorUpdateSender)>> = (0..hex_max_connections)
                        .map(|i| {
                            let mut worker = trade.clone_worker();
                            let inst_id = instance_id.clone();
                            let (tx, rx) = bounded::<(Signal, ExecutorUpdateSender)>(64);
                            let worker_name = format!("{}-worker-{}", inst_id, i);
                            thread::Builder::new()
                                .name(worker_name.clone())
                                .spawn(move || {
                                    crate::os_tune::pin_execution(&worker_name);
                                    while let Ok((signal, update_tx)) = rx.recv() {
                                        let updates = execute_hex_signal(&mut worker, signal);
                                        for update in updates {
                                            let _ = send_executor_update(&update_tx, update);
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
                    bounded::<(Signal, u64, ExecutorUpdateSender)>(CHANNEL_CAPACITY);
                // Must-complete cancel work owns a bounded lossless lane. A
                // full lane backpressures only the execution actor; it cannot
                // grow process memory without limit or block a strategy owner.
                let (poly_cancel_tx, poly_cancel_rx) =
                    bounded::<(Signal, u64, ExecutorUpdateSender)>(CHANNEL_CAPACITY);
                // Reconcile performs synchronous multi-request HTTP. Keep it
                // off both the place and cancel lanes; bounded admission lets
                // the strategy receive explicit deferred feedback instead of
                // building an unbounded orphan backlog.
                let (poly_reconcile_tx, poly_reconcile_rx) =
                    bounded::<(Signal, u64, ExecutorUpdateSender)>(CHANNEL_CAPACITY);
                // Fire-and-track completion lane: a worker fires (admission
                // permit + kickoff on the reserved connection) then hands the
                // "await reply + book it" closure here; a pool of drainers runs
                // them so no worker `block_on`s the RTT. The permit is captured
                // by the closure and released when it completes.
                // Fired completions are bounded by held HTTP permits. Mirror
                // that physical ceiling in the queue as an explicit invariant
                // rather than relying on an unbounded container.
                let completion_capacity =
                    hexagent_runtime::http1_pool::total_account_order_capacity()
                        .saturating_add(4)
                        .max(4);
                let (poly_done_tx, poly_done_rx) =
                    bounded::<QueuedPolyCompletion>(completion_capacity);
                let mut poly_worker_handles: Vec<thread::JoinHandle<()>> = Vec::new();
                let mut poly_cancel_handles: Vec<thread::JoinHandle<()>> = Vec::new();
                let mut poly_reconcile_handles: Vec<thread::JoinHandle<()>> = Vec::new();
                let mut poly_drainer_handles: Vec<thread::JoinHandle<()>> = Vec::new();
                // Admission-control observability (component 7): a stop flag +
                // handle for the periodic stats daemon, set/joined at shutdown.
                let poly_stats_stop =
                    std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                let mut poly_stats_handle: Option<thread::JoinHandle<()>> = None;
                if !poly_states.is_empty() {
                    // Hot-path admission is account-scoped: all instances on
                    // one wallet share 4N placement, 4N cancel, and 2N
                    // reconcile slots; each account also owns two gap-replay slots. The pools were
                    // built before SharedState prewarm.
                    if !hexagent_runtime::http1_pool::account_pools_ready() {
                        warn!(
                            "[Executor] account admission pools are unavailable; hot-path requests will be shed",
                        );
                    }
                    // One completion worker per possible in-flight fired
                    // place/cancel attempt. This is derived from the real
                    // account pool (Fast=4, Cancel=4 per account), without the
                    // old fixed +4 oversubscription.
                    let n_drainers =
                        hexagent_runtime::http1_pool::total_account_order_capacity().max(1);
                    for i in 0..n_drainers {
                        let mut router = LiveRouter::new_with_poly_map(&config, &poly_states);
                        let rx = poly_done_rx.clone();
                        let dname = format!("poly-done-{}", i);
                        let h = thread::Builder::new()
                            .name(dname.clone())
                            .spawn(move || {
                                crate::os_tune::pin_execution(&dname);
                                crate::latency::prepare_polymarket_order_stages();
                                while let Ok(queued) = rx.recv() {
                                    let queue_stage = if queued.kind == "cancel" {
                                        "polymarket.cancel.completion_queue"
                                    } else {
                                        "polymarket.order.completion_queue"
                                    };
                                    crate::latency::record_ns(
                                        queue_stage,
                                        queued.enqueued_at.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                                    );
                                    for update in (queued.complete)(&mut router) {
                                        if send_executor_update(&queued.update_tx, update).is_err() { break; }
                                    }
                                }
                            })
                            .unwrap();
                        poly_drainer_handles.push(h);
                    }
                    for i in 0..poly_worker_n {
                        let mut worker = LiveRouter::new_with_poly_map(&config, &poly_states);
                        let rx = poly_pool_rx.clone();
                        let done_tx = poly_done_tx.clone();
                        let cancel_tx = poly_cancel_tx.clone();
                        let wname = format!("poly-exec-{}", i);
                        let h = thread::Builder::new()
                            .name(wname.clone())
                            .spawn(move || {
                                crate::os_tune::pin_execution(&wname);
                                crate::latency::prepare_polymarket_order_stages();
                                while let Ok((signal, stale_ms, utx)) = rx.recv() {
                                    fire_or_execute(
                                        &mut worker,
                                        signal,
                                        stale_ms,
                                        &done_tx,
                                        Some(&cancel_tx),
                                        &utx,
                                    );
                                }
                            })
                            .unwrap();
                        poly_worker_handles.push(h);
                    }
                    let cancel_worker_n =
                        hexagent_runtime::http1_pool::total_account_cancel_capacity().max(1);
                    for i in 0..cancel_worker_n {
                        let mut worker = LiveRouter::new_with_poly_map(&config, &poly_states);
                        let rx = poly_cancel_rx.clone();
                        let done_tx = poly_done_tx.clone();
                        let wname = format!("poly-cancel-{}", i);
                        let h = thread::Builder::new()
                            .name(wname.clone())
                            .spawn(move || {
                                crate::os_tune::pin_execution(&wname);
                                crate::latency::prepare_polymarket_order_stages();
                                while let Ok((signal, stale_ms, utx)) = rx.recv() {
                                    fire_or_execute(
                                        &mut worker,
                                        signal,
                                        stale_ms,
                                        &done_tx,
                                        None,
                                        &utx,
                                    );
                                }
                            })
                            .unwrap();
                        poly_cancel_handles.push(h);
                    }
                    // One owner task per account route is sufficient: each
                    // request already owns a separate Reconcile connection
                    // permit, and duplicate OS workers only increase lock/map
                    // contention in cold lifecycle bookkeeping.
                    let reconcile_worker_n = poly_states.len().max(1);
                    for i in 0..reconcile_worker_n {
                        let mut worker = LiveRouter::new_with_poly_map(&config, &poly_states);
                        let rx = poly_reconcile_rx.clone();
                        let done_tx = poly_done_tx.clone();
                        let wname = format!("poly-reconcile-{}", i);
                        let h = thread::Builder::new()
                            .name(wname.clone())
                            .spawn(move || {
                                crate::os_tune::pin_background(&wname);
                                crate::latency::prepare_polymarket_order_stages();
                                while let Ok((signal, stale_ms, utx)) = rx.recv() {
                                    fire_or_execute(
                                        &mut worker,
                                        signal,
                                        stale_ms,
                                        &done_tx,
                                        None,
                                        &utx,
                                    );
                                }
                            })
                            .unwrap();
                        poly_reconcile_handles.push(h);
                    }
                    info!(
                        "[Executor] Polymarket dispatch pools: place={} cancel={} reconcile={} completion={}",
                        poly_worker_n, cancel_worker_n, reconcile_worker_n, n_drainers
                    );
                    // Admission-control observability daemon: every 30 s log the
                    // per-(account,role) delta — acquires/skips, retained cancel
                    // waits, and current busy. Placement skips shed a business
                    // operation and are WARN; cancel waits remain INFO because
                    // the cancel is retained until a slot becomes available.
                    {
                        let stop = poly_stats_stop.clone();
                        let stats_shutdown_rx = shutdown_token.subscribe();
                        let mut seen_accounts = HashSet::new();
                        let monitoring_accounts: Vec<_> = poly_states.values()
                            .filter_map(|shared| {
                                let account_id = shared.account_state.account_id().to_string();
                                seen_accounts.insert(account_id)
                                    .then(|| shared.account_state.clone())
                            })
                            .collect();
                        let h = thread::Builder::new()
                            .name("poly-admission-stats".into())
                            .spawn(move || {
                                crate::os_tune::pin_background("poly-admission-stats");
                                let mut prev = HashMap::new();
                                let mut gap_prev: HashMap<String, (u64, u64)> = HashMap::new();
                                let mut reservation_lock_prev: HashMap<String, u64> =
                                    HashMap::new();
                                loop {
                                    let mut slept = 0;
                                    while slept < 30 {
                                        if stop.load(std::sync::atomic::Ordering::Relaxed) {
                                            return;
                                        }
                                        crossbeam_channel::select! {
                                            recv(stats_shutdown_rx) -> _ => return,
                                            recv(crossbeam_channel::after(std::time::Duration::from_secs(1))) -> _ => {}
                                        }
                                        slept += 1;
                                    }
                                    let by_account = admission_log_snapshot(
                                        &mut prev,
                                        hexagent_runtime::http1_pool::admission_stats(),
                                    );
                                    for (account_id, (line, primary_skip)) in by_account {
                                        if primary_skip {
                                            warn!(
                                                "[admission] account={} {}",
                                                account_id,
                                                line.trim_end(),
                                            );
                                        } else {
                                            info!(
                                                "[admission] account={} {}",
                                                account_id,
                                                line.trim_end(),
                                            );
                                        }
                                    }
                                    for (account_id, gap_acq, gap_skip, gap_busy, gap_slots) in
                                        hexagent_runtime::http1_pool::all_gap_replay_stats()
                                    {
                                        let previous = gap_prev
                                            .insert(account_id.clone(), (gap_acq, gap_skip))
                                            .unwrap_or((0, 0));
                                        let gap_acq_delta = gap_acq.saturating_sub(previous.0);
                                        let gap_skip_delta = gap_skip.saturating_sub(previous.1);
                                        if gap_skip_delta > 0 {
                                            warn!(
                                                "[admission] account={} GapReplay(+{} skip+{} busy{} slots={:?})",
                                                account_id,
                                                gap_acq_delta,
                                                gap_skip_delta,
                                                gap_busy,
                                                gap_slots,
                                            );
                                        } else {
                                            info!(
                                                "[admission] account={} GapReplay(+{} skip+{} busy{} slots={:?})",
                                                account_id,
                                                gap_acq_delta,
                                                gap_skip_delta,
                                                gap_busy,
                                                gap_slots,
                                            );
                                        }
                                    }
                                    for account in &monitoring_accounts {
                                        let snapshot = account.monitoring_snapshot_fast();
                                        let reservation_acquisitions = snapshot
                                            .reservation_control_lock
                                            .acquisitions
                                            .saturating_add(snapshot.reservation_coid_route_lock.acquisitions)
                                            .saturating_add(snapshot.reservation_oid_route_lock.acquisitions)
                                            .saturating_add(snapshot.reservation_lifecycle_lock.acquisitions);
                                        let prior_acquisitions = reservation_lock_prev
                                            .insert(snapshot.account_id.clone(), reservation_acquisitions)
                                            .unwrap_or(0);
                                        if reservation_acquisitions != prior_acquisitions {
                                            let lock = |name: &str, metrics: &hexagent_account::account::shared_account::LockMonitoringSnapshot| {
                                                format!(
                                                    "{}:wait={}/{} hold={}/{} count={}",
                                                    name,
                                                    metrics.wait_last_us,
                                                    metrics.wait_max_us,
                                                    metrics.hold_last_us,
                                                    metrics.hold_max_us,
                                                    metrics.acquisitions,
                                                )
                                            };
                                            info!(
                                                "[account_lock_metric] account={} units=us {} {} {} {}",
                                                snapshot.account_id,
                                                lock("reservation_control", &snapshot.reservation_control_lock),
                                                lock("coid_route", &snapshot.reservation_coid_route_lock),
                                                lock("oid_route", &snapshot.reservation_oid_route_lock),
                                                lock("lifecycle", &snapshot.reservation_lifecycle_lock),
                                            );
                                        }
                                        let log_account = || {
                                            let physical = account_metric_position_summary(&snapshot.physical_positions);
                                            let virtual_total = account_metric_position_summary(&snapshot.virtual_positions);
                                            let unallocated = account_metric_position_summary(&snapshot.unallocated_positions);
                                            let reserved = account_metric_position_summary(&snapshot.reserved_positions);
                                            format!(
                                                "physical_cash={:.6} virtual_cash={:.6} unallocated_cash={:.6} reserved_cash={:.6} pos_physical(count/abs/invalid)={}/{:.4}/{} pos_virtual={}/{:.4}/{} pos_unallocated={}/{:.4}/{} pos_reserved={}/{:.4}/{} uncertain={} uncertain_since_ms={:?} reason={:?} recovery_pending_orders={} routine_cancel_audits={} retired_trade_tombstones={} verified_trade_replay_recoveries={} gap_pages(last/max/total)={}/{}/{} maintenance_wait_ms(last/max/jobs)={}/{}/{} account_lock_wait_us(last/max)={}/{} account_lock_hold_us(last/max/count)={}/{}/{} persistence={:?} persistence_error={:?} persistence_write_us(last/max/count)={}/{}/{} persistence_flush_us(last/max/count)={}/{}/{} persistence_generation(scheduled/completed/lag)={}/{}/{} persistence_pending_jobs(current/high)={}/{} settled_gc(candidate/inflight)={}/{} settled_gc_request_queue(depth/high/overflow)={}/{}/{} settled_gc_completion_queue(depth/high/overflow)={}/{}/{} settled_gc_certificates={}",
                                                snapshot.physical_cash,
                                                snapshot.virtual_cash,
                                                snapshot.unallocated_cash,
                                                snapshot.reserved_cash,
                                                physical.0, physical.1, physical.2,
                                                virtual_total.0, virtual_total.1, virtual_total.2,
                                                unallocated.0, unallocated.1, unallocated.2,
                                                reserved.0, reserved.1, reserved.2,
                                                snapshot.uncertain,
                                                snapshot.uncertain_since_ms,
                                                snapshot.uncertain_reason,
                                                snapshot.recovery_pending_orders,
                                                snapshot.routine_cancel_audits,
                                                snapshot.retired_trade_ownership_tombstones,
                                                snapshot.verified_trade_replay_recoveries,
                                                snapshot.gap_replay_last_pages,
                                                snapshot.gap_replay_max_pages,
                                                snapshot.gap_replay_total_pages,
                                                snapshot.maintenance_queue_last_wait_ms,
                                                snapshot.maintenance_queue_max_wait_ms,
                                                snapshot.maintenance_queue_jobs,
                                                snapshot.account_lock_wait_last_us,
                                                snapshot.account_lock_wait_max_us,
                                                snapshot.account_lock_hold_last_us,
                                                snapshot.account_lock_hold_max_us,
                                                snapshot.account_lock_acquisitions,
                                                snapshot.persistence_path,
                                                snapshot.persistence_error,
                                                snapshot.persistence_write_last_us,
                                                snapshot.persistence_write_max_us,
                                                snapshot.persistence_writes,
                                                snapshot.persistence_flush_last_us,
                                                snapshot.persistence_flush_max_us,
                                                snapshot.persistence_flushes,
                                                snapshot.persistence_scheduled_generation,
                                                snapshot.persistence_completed_generation,
                                                snapshot.persistence_generation_lag,
                                                snapshot.persistence_pending_jobs,
                                                snapshot.persistence_pending_high_water,
                                                snapshot.settled_gc_candidates,
                                                snapshot.settled_gc_inflight_requests,
                                                snapshot.settled_gc_request_queue_depth,
                                                snapshot.settled_gc_request_queue_high_water,
                                                snapshot.settled_gc_request_queue_overflows,
                                                snapshot.settled_gc_completion_queue_depth,
                                                snapshot.settled_gc_completion_queue_high_water,
                                                snapshot.settled_gc_completion_queue_overflows,
                                                snapshot.settled_gc_certificates_completed,
                                            )
                                        };
                                        if snapshot.uncertain || snapshot.persistence_error.is_some() {
                                            warn!(
                                                "[account_metric] account={} {}",
                                                snapshot.account_id,
                                                log_account(),
                                            );
                                        } else {
                                            info!(
                                                "[account_metric] account={} {}",
                                                snapshot.account_id,
                                                log_account(),
                                            );
                                        }
                                        if snapshot.uncertain || snapshot.persistence_error.is_some() {
                                            for instance in snapshot.instances {
                                                let positions = account_metric_position_summary(&instance.positions);
                                                let reserved_positions = account_metric_position_summary(&instance.reserved_positions);
                                                warn!(
                                                    "[account_metric] account={} instance={} weight={:.4} virtual_cash={:.6} reserved_cash={:.6} positions(count/abs/invalid)={}/{:.4}/{} reserved_positions={}/{:.4}/{}",
                                                    snapshot.account_id,
                                                    instance.instance_id,
                                                    instance.weight,
                                                    instance.cash,
                                                    instance.reserved_cash,
                                                    positions.0, positions.1, positions.2,
                                                    reserved_positions.0, reserved_positions.1, reserved_positions.2,
                                                );
                                            }
                                        }
                                    }
                                }
                            })
                            .unwrap();
                        poly_stats_handle = Some(h);
                    }
                }
                drop(poly_pool_rx); // main loop only sends; workers hold their clones
                drop(poly_cancel_rx);
                drop(poly_reconcile_rx);
                drop(poly_done_rx); // workers hold their clones of done_tx
                // Options so Exit can drop senders + join (drain all in-flight
                // dispatches AND completions) BEFORE the shutdown cancel-all, so
                // nothing places an order after cancel-all snapshots the book.
                let mut poly_pool_tx = Some(poly_pool_tx);
                let mut poly_cancel_tx = Some(poly_cancel_tx);
                let mut poly_reconcile_tx = Some(poly_reconcile_tx);
                let mut poly_done_tx = Some(poly_done_tx);

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
                let mut shutdown_finalized = false;

                loop {
                    // The executor has a direct shutdown subscription. A
                    // dead strategy/router therefore cannot strand it on a
                    // blocking signal receive or prevent admission teardown.
                    let routed = crossbeam_channel::select_biased! {
                        recv(execution_shutdown_rx) -> _ => RoutedSignal {
                            owner: SYSTEM_SIGNAL_OWNER,
                            signal: Signal::BeginShutdown,
                        },
                        recv(signal_rx) -> routed => match routed {
                            Ok(routed) => routed,
                            Err(_) => break,
                        },
                    };
                    let update_sender = ExecutorUpdateSender {
                        owner: routed.owner,
                        tx: update_tx.clone(),
                    };
                    let embedded_instance_id = extract_instance_id(&routed.signal);
                    let numeric_instance_id = (routed.owner != SYSTEM_SIGNAL_OWNER)
                        .then(|| owner_instance_ids.get(routed.owner as usize))
                        .flatten()
                        .filter(|instance_id| !instance_id.is_empty());
                    if let Some(expected) = numeric_instance_id {
                        assert!(
                            embedded_instance_id.is_empty() || embedded_instance_id == *expected,
                            "signal owner mismatch: owner={} expected_instance={} embedded_instance={}",
                            routed.owner,
                            expected,
                            embedded_instance_id,
                        );
                    }
                    let instance_id = numeric_instance_id
                        .cloned()
                        .unwrap_or(embedded_instance_id);
                    let signal = routed.signal;
                    if shutdown_finalized
                        && !matches!(&signal, Signal::BeginShutdown | Signal::Exit)
                    {
                        warn!("[Executor] dropping signal received after shutdown barrier: {:?}", signal);
                        continue;
                    }
                    match &signal {
                        Signal::BeginShutdown | Signal::Exit => {
                            let terminal = matches!(&signal, Signal::Exit);
                            if !shutdown_finalized {
                                // Stop the dispatch pool first: drop the sender
                                // and join all in-flight/queued work before
                                // cancel-all can snapshot the remote book.
                                poly_pool_tx = None;
                                for h in std::mem::take(&mut poly_worker_handles) {
                                    let _ = h.join();
                                }
                                // Replace work on the place lane may enqueue a
                                // cancel leg. Cancel/reconcile workers must not
                                // retain a sender for the cancel lane they are
                                // waiting to drain; that self-ownership was the
                                // old shutdown deadlock.
                                poly_reconcile_tx = None;
                                for h in std::mem::take(&mut poly_reconcile_handles) {
                                    let _ = h.join();
                                }
                                poly_cancel_tx = None;
                                for h in std::mem::take(&mut poly_cancel_handles) {
                                    let _ = h.join();
                                }
                                // Drain every fired completion before the
                                // account-wide cancellation barrier.
                                poly_done_tx = None;
                                for h in std::mem::take(&mut poly_drainer_handles) {
                                    let _ = h.join();
                                }
                                poly_stats_stop.store(true, std::sync::atomic::Ordering::Relaxed);
                                if let Some(h) = poly_stats_handle.take() { let _ = h.join(); }
                                let account_states = Self::dedup_states_by_account(&poly_states);
                                info!("[Executor] coordinated shutdown: canceling/auditing {} Polymarket account(s) to finality",
                                    account_states.len());
                                for (instance_id, shared) in account_states {
                                    let account_id = shared.account_state.account_id().to_string();
                                    let trade = PolymarketTrade::from_shared(shared, "", &instance_id);
                                    info!("[Executor] shutdown cancel barrier account_id={} representative_instance={}",
                                        account_id, instance_id);
                                    trade.cancel_all_orders_until_final(|update| {
                                        let _ = send_executor_update(&update_sender, update);
                                    });
                                }
                                shutdown_finalized = true;
                            }
                            if !terminal {
                                let _ = shutdown_done_tx.send(());
                                info!("[Executor] coordinated shutdown barrier complete");
                                continue;
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
                            if let Some(pool) = instance_pools.get(&instance_id) {
                                let rr = round_robins.entry(instance_id.clone()).or_insert(0);
                                let idx = *rr % pool.len();
                                *rr += 1;
                                let _ = pool[idx].send((signal, update_sender.clone()));
                            } else {
                                warn!("[Executor] Unknown instance '{}', dropping signal", instance_id);
                            }
                        }
                        Signal::NewOrder(order) if order.exchange == Exchange::Hexmarket => {
                            if let Some(pool) = instance_pools.get(&instance_id) {
                                let rr = round_robins.entry(instance_id.clone()).or_insert(0);
                                let idx = *rr % pool.len();
                                *rr += 1;
                                let _ = pool[idx].send((signal, update_sender.clone()));
                            } else {
                                warn!("[Executor] Unknown instance '{}', dropping signal", instance_id);
                            }
                        }
                        _ => {
                            // Phase 2e-4: lookup per-instance stale
                            // threshold. Falls back to 150 ms (legacy
                            // default) for signals whose instance_id
                            // isn't in the map — e.g. non-polymaker
                            // exchanges or paper/BT shims that pass
                            // an empty map.
                            let stale_threshold_ms = stale_threshold_handles
                                .get(&instance_id)
                                .map(|h| h.load(std::sync::atomic::Ordering::Relaxed))
                                .unwrap_or(150);
                            // Plan A: Polymarket signals for a known instance go
                            // to the pipelined worker pool; everything else
                            // (binance, unknown iid, or pool disabled) runs
                            // inline on this thread as before.
                            if poly_states.contains_key(&instance_id) && poly_pool_tx.is_some() {
                                let work = (signal, stale_threshold_ms, update_sender.clone());
                                match &work.0 {
                                    Signal::CancelOrder {
                                        exchange: Exchange::Polymarket,
                                        ..
                                    }
                                    | Signal::CancelAll {
                                        exchange: Exchange::Polymarket,
                                        ..
                                    }
                                    | Signal::BatchCancelOrders {
                                        exchange: Exchange::Polymarket,
                                        ..
                                    }
                                    | Signal::PolymarketCancelAllOrders { .. } => {
                                        if let Some(tx) = poly_cancel_tx.as_ref() {
                                            let _ = tx.send(work);
                                        }
                                    }
                                    Signal::ReconcilePolymarket { .. } => {
                                        if let Some(tx) = poly_reconcile_tx.as_ref() {
                                            match tx.try_send(work) {
                                                Ok(()) => {}
                                                Err(crossbeam_channel::TrySendError::Full((
                                                    signal,
                                                    _,
                                                    utx,
                                                )))
                                                | Err(crossbeam_channel::TrySendError::Disconnected((
                                                    signal,
                                                    _,
                                                    utx,
                                                ))) => defer_reconcile_signal(signal, &utx),
                                            }
                                        }
                                    }
                                    Signal::NewOrder(_)
                                    | Signal::BatchNewOrders { .. }
                                    | Signal::BatchUpdateOrders { .. }
                                    | Signal::ReplaceOrder { .. } => {
                                        match poly_pool_tx.as_ref().unwrap().try_send(work) {
                                            Ok(()) => {}
                                            Err(crossbeam_channel::TrySendError::Full((
                                                signal,
                                                stale_ms,
                                                utx,
                                            )))
                                            | Err(crossbeam_channel::TrySendError::Disconnected((
                                                signal,
                                                stale_ms,
                                                utx,
                                            ))) => shed_saturated_place_signal(
                                                signal,
                                                stale_ms,
                                                poly_cancel_tx
                                                    .as_ref()
                                                    .expect("cancel lane active with place lane"),
                                                &utx,
                                            ),
                                        }
                                    }
                                    _ => {
                                        // Lifecycle/audit control messages are
                                        // low-volume and lossless. Reuse the
                                        // bounded must-complete lane so place
                                        // pressure cannot grow process memory.
                                        if let Some(tx) = poly_cancel_tx.as_ref() {
                                            let _ = tx.send(work);
                                        }
                                    }
                                }
                            } else {
                                let updates = execute_fallback_signal(&mut fallback, signal, stale_threshold_ms);
                                for update in updates {
                                    if send_executor_update(&update_sender, update).is_err() { break; }
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
                drop(poly_reconcile_tx.take());
                for h in poly_reconcile_handles { let _ = h.join(); }
                drop(poly_cancel_tx.take());
                for h in poly_cancel_handles { let _ = h.join(); }
                // Drain fire-and-track completions after the workers stop.
                drop(poly_done_tx.take());
                for h in poly_drainer_handles { let _ = h.join(); }
                poly_stats_stop.store(true, std::sync::atomic::Ordering::Relaxed);
                if let Some(h) = poly_stats_handle.take() { let _ = h.join(); }
                info!("[Executor] Thread stopped");
            })
            .unwrap()
    }
}

/// Recover the placing instance's worker index from a coid minted as
/// `"{instance_id}-{counter}"` (live/paper multi-account). The counter suffix
/// never contains '-', so `rsplit_once('-')` cleanly splits off the
/// instance_id prefix even when the instance_id itself contains dashes.
/// Returns `None` for legacy un-prefixed (all-numeric) coids or an unknown
/// instance. Multi-instance private routing treats that as unowned/fail-closed.
fn owner_from_coid(coid: &str, iid_to_idx: &HashMap<String, usize>) -> Option<usize> {
    let (iid, _counter) = coid.rsplit_once('-')?;
    iid_to_idx.get(iid).copied()
}

fn is_synthetic_probe_update(update: &OrderUpdate) -> bool {
    update.exchange == Exchange::Polymarket && update.client_order_id.starts_with("probe:")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrivateUpdateRoute {
    Owner(usize),
    Unowned,
    DropQuarantined(usize),
    DropInvalid(usize),
}

fn classify_private_update_route(
    owner: Option<usize>,
    worker_count: usize,
    quarantined: &[Arc<AtomicBool>],
) -> PrivateUpdateRoute {
    match owner {
        Some(i) if i >= worker_count || i >= quarantined.len() => {
            PrivateUpdateRoute::DropInvalid(i)
        }
        Some(i) if quarantined[i].load(Ordering::Acquire) => PrivateUpdateRoute::DropQuarantined(i),
        Some(i) => PrivateUpdateRoute::Owner(i),
        None => PrivateUpdateRoute::Unowned,
    }
}

// ── Signal Execution Helpers ─────────────────────────────────────────────

/// Parse `BinaryOption.event_start_time` (ISO 8601, e.g. `"2026-03-29T06:10:00Z"`)
/// into unix seconds floored to the 5-min event boundary used by
/// `per_event_rtt::extract_per_event_rtt`. Returns `None` on parse
/// failure so the caller can skip the per-event override push.
fn parse_event_start_ts_secs(iso: &str) -> Option<u64> {
    if iso.is_empty() {
        return None;
    }
    let dt = chrono::DateTime::parse_from_rfc3339(iso).ok()?;
    let secs = dt.timestamp();
    if secs < 0 {
        return None;
    }
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
        Signal::BatchNewOrders {
            instance_id,
            orders,
            ..
        } => {
            // Prefer the explicit field; fall back to the first order's
            // instance_id for backward-compat with emit sites that pre-
            // dated the explicit-field addition.
            if !instance_id.is_empty() {
                return instance_id.clone();
            }
            orders
                .first()
                .map(|o| o.instance_id.clone())
                .unwrap_or_default()
        }
        Signal::BatchCancelOrders { instance_id, .. } => instance_id.clone(),
        Signal::BatchUpdateOrders {
            instance_id,
            place_orders,
            ..
        }
        | Signal::ReplaceOrder {
            instance_id,
            place_orders,
            ..
        } => {
            if !instance_id.is_empty() {
                return instance_id.clone();
            }
            place_orders
                .first()
                .map(|o| o.instance_id.clone())
                .unwrap_or_default()
        }
        Signal::ReconcilePolymarket { instance_id, .. } => instance_id.clone(),
        Signal::PolymarketCancelAllOrders { instance_id, .. } => instance_id.clone(),
        Signal::RetainPolymarketEventAudit { instance_id, .. }
        | Signal::RetirePolymarketEventAudit { instance_id, .. } => instance_id.clone(),
        _ => String::new(),
    }
}

type AdmissionCounters = (u64, u64, u64);
type AdmissionStat = (
    String,
    hexagent_runtime::http1_pool::Role,
    u64,
    u64,
    u64,
    usize,
);

/// Convert cumulative pool counters into per-window, per-instance log lines.
/// The boolean is true only when a business request was shed. Retained cancel
/// waits remain visible but never elevate the line to WARN.
fn admission_log_snapshot(
    prev: &mut HashMap<String, AdmissionCounters>,
    stats: Vec<AdmissionStat>,
) -> std::collections::BTreeMap<String, (String, bool)> {
    let mut by_inst: std::collections::BTreeMap<String, (String, bool)> = Default::default();
    for (iid, role, acq, sk, waits, busy) in stats {
        let key = format!("{}/{:?}", iid, role);
        let (pa, ps, pw) = prev.get(&key).copied().unwrap_or((0, 0, 0));
        let dacq = acq.saturating_sub(pa);
        let dsk = sk.saturating_sub(ps);
        let dwaits = waits.saturating_sub(pw);
        prev.insert(key, (acq, sk, waits));

        let entry = by_inst.entry(iid).or_insert_with(Default::default);
        entry.1 |= dsk > 0;
        if role == hexagent_runtime::http1_pool::Role::Cancel {
            entry.0.push_str(&format!(
                "{:?}(+{} skip+{} retained_wait+{} busy{}) ",
                role, dacq, dsk, dwaits, busy
            ));
        } else {
            entry
                .0
                .push_str(&format!("{:?}(+{} skip+{} busy{}) ", role, dacq, dsk, busy));
        }
    }
    by_inst
}

fn execute_hex_signal(worker: &mut HexmarketTrade, signal: Signal) -> Vec<OrderUpdate> {
    match signal {
        Signal::NewOrder(order) => match worker.submit_order(&order) {
            Ok(update) => vec![update],
            Err(e) => {
                error!("[Executor] Submit error: {}", e);
                vec![OrderUpdate {
                    client_order_id: order.client_order_id,
                    exchange: order.exchange,
                    symbol: order.symbol,
                    side: order.side,
                    exchange_order_id: None,
                    status: OrderStatus::Rejected,
                    liquidity: None,
                    filled_quantity: 0.0,
                    remaining_quantity: order.quantity,
                    avg_fill_price: 0.0,
                    timestamp_ns: now_ns(),
                    exchange_event_timestamp_ns: None,
                    trade_id: None,
                    order_audit: None,
                    error: None,
                }]
            }
        },
        Signal::CancelOrder {
            exchange,
            client_order_id,
            ..
        } => match worker.cancel_order(exchange, &client_order_id) {
            Ok(update) => vec![update],
            Err(e) => {
                error!("[Executor] Cancel error: {}", e);
                vec![]
            }
        },
        Signal::CancelAll {
            exchange, symbol, ..
        } => worker.cancel_all(exchange, &symbol).unwrap_or_else(|e| {
            error!("[Executor] Cancel-all error: {}", e);
            vec![]
        }),
        Signal::BatchNewOrders {
            market_id, orders, ..
        } => worker
            .batch_submit_orders(&market_id, &orders)
            .unwrap_or_else(|e| {
                error!("[Executor] Batch place error: {}", e);
                vec![]
            }),
        Signal::BatchCancelOrders {
            exchange,
            market_id,
            client_order_ids,
            ..
        } => worker
            .batch_cancel_orders(exchange, &market_id, &client_order_ids)
            .unwrap_or_else(|e| {
                error!("[Executor] Batch cancel error: {}", e);
                vec![]
            }),
        Signal::BatchUpdateOrders {
            exchange,
            market_id,
            cancel_client_order_ids,
            place_orders,
            ..
        } => worker
            .batch_update_orders(
                exchange,
                &market_id,
                &cancel_client_order_ids,
                &place_orders,
            )
            .unwrap_or_else(|e| {
                error!("[Executor] Batch update error: {}", e);
                vec![]
            }),
        _ => vec![],
    }
}

/// A fired request's completion: run on a drainer thread, it awaits the
/// reply and books it, returning the resulting update(s). It captures the
/// admission permit, so the reserved connection is released when this runs.
type PolyCompletionFn = Box<dyn FnOnce(&mut LiveRouter) -> Vec<OrderUpdate> + Send>;

struct QueuedPolyCompletion {
    complete: PolyCompletionFn,
    update_tx: ExecutorUpdateSender,
    kind: &'static str,
    enqueued_at: std::time::Instant,
}

/// Stamp the actual executor-producer boundary immediately before publishing
/// an update to the root strategy router. Some cold operations (notably an
/// orphan reconcile batch) construct an `OrderUpdate`, perform more HTTP
/// requests, and only then publish the accumulated results. Keeping the
/// construction timestamp in that case incorrectly charges the remaining
/// cold HTTP work to `producer_to_root_router` and can also make a prompt
/// reconcile-triggered quote look stale. Private-feed producers retain their
/// own receipt timestamp because they publish each routed update immediately.
fn send_executor_update(
    tx: &ExecutorUpdateSender,
    mut update: OrderUpdate,
) -> Result<(), crossbeam_channel::SendError<RoutedOrderUpdate>> {
    if update.exchange == Exchange::Polymarket {
        update.timestamp_ns = now_ns();
    }
    tx.tx.send(RoutedOrderUpdate {
        owner: tx.owner,
        update,
    })
}

fn send_root_private_update(
    tx: &Sender<OrderUpdate>,
    mut update: OrderUpdate,
) -> Result<(), crossbeam_channel::SendError<OrderUpdate>> {
    if update.exchange == Exchange::Polymarket {
        update.timestamp_ns = now_ns();
    }
    tx.send(update)
}

/// ExecutorRejected update for a placement we never sent (admission skip /
/// stale / pre-flight). Free-fn twin of the closure in `execute_fallback_signal`.
fn exec_rejected_place(order: &OrderRequest) -> OrderUpdate {
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
        exchange_event_timestamp_ns: None,
        trade_id: None,
        order_audit: None,
        error: None,
    }
}

/// ExecutorRejected update for a cancel we cannot route because its instance
/// is unknown. Ordinary pool saturation is retained and never reaches here.
fn exec_rejected_cancel(coid: String, exchange: Exchange) -> OrderUpdate {
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
        exchange_event_timestamp_ns: None,
        trade_id: None,
        order_audit: None,
        error: None,
    }
}

/// Build lossless admission feedback for a reconcile signal that never
/// acquired an HTTP connection. Each coid/trade-id is returned once; trade
/// backfills use a synthetic instance-prefixed coid for strategy routing.
fn reconcile_deferred_updates(
    instance_id: &str,
    pending_places: &[(String, String, Side, f64, Option<String>)],
    pending_cancels: &[(String, String)],
    pending_trade_ids: &[String],
) -> Vec<OrderUpdate> {
    let mut updates = Vec::new();
    let mut coids = HashSet::new();
    for (coid, symbol, side, price, _) in pending_places {
        if !coids.insert(coid.clone()) {
            continue;
        }
        updates.push(OrderUpdate {
            client_order_id: coid.clone(),
            exchange: Exchange::Polymarket,
            symbol: symbol.clone(),
            side: *side,
            exchange_order_id: None,
            status: OrderStatus::ExecutorRejected,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: 0.0,
            avg_fill_price: *price,
            timestamp_ns: now_ns(),
            exchange_event_timestamp_ns: None,
            trade_id: None,
            order_audit: None,
            error: Some(ORPHAN_RECONCILE_DEFERRED.to_string()),
        });
    }
    for (coid, order_id) in pending_cancels {
        if !coids.insert(coid.clone()) {
            continue;
        }
        updates.push(OrderUpdate {
            client_order_id: coid.clone(),
            exchange: Exchange::Polymarket,
            symbol: String::new(),
            side: Side::Buy,
            exchange_order_id: (!order_id.is_empty()).then(|| order_id.clone()),
            status: OrderStatus::ExecutorRejected,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: 0.0,
            avg_fill_price: 0.0,
            timestamp_ns: now_ns(),
            exchange_event_timestamp_ns: None,
            trade_id: None,
            order_audit: None,
            error: Some(ORPHAN_RECONCILE_DEFERRED.to_string()),
        });
    }
    let synthetic_coid = if instance_id.is_empty() {
        "reconcile_deferred".to_string()
    } else {
        // Keep the suffix dash-free: owner_from_coid() splits at the final
        // dash, preserving instance IDs that themselves contain dashes.
        format!("{instance_id}-reconcile_deferred")
    };
    let mut trade_ids = HashSet::new();
    for trade_id in pending_trade_ids {
        if !trade_ids.insert(trade_id.clone()) {
            continue;
        }
        updates.push(OrderUpdate {
            client_order_id: synthetic_coid.clone(),
            exchange: Exchange::Polymarket,
            symbol: String::new(),
            side: Side::Buy,
            exchange_order_id: None,
            status: OrderStatus::ExecutorRejected,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: 0.0,
            avg_fill_price: 0.0,
            timestamp_ns: now_ns(),
            exchange_event_timestamp_ns: None,
            trade_id: Some(trade_id.clone()),
            order_audit: None,
            error: Some(ORPHAN_RECONCILE_DEFERRED.to_string()),
        });
    }
    updates
}

/// Shed only place work when the bounded hot lane is saturated. Any cancel
/// half of a combined update is converted to a lossless cancel-lane job, while
/// each unsent place gets an explicit ExecutorRejected update so its account
/// reservation is released by the normal lifecycle path.
fn shed_saturated_place_signal(
    signal: Signal,
    stale_ms: u64,
    cancel_tx: &Sender<(Signal, u64, ExecutorUpdateSender)>,
    utx: &ExecutorUpdateSender,
) {
    let (places, cancel) = match signal {
        Signal::NewOrder(order) => (vec![order], None),
        Signal::BatchNewOrders { orders, .. } => (orders, None),
        Signal::ReplaceOrder {
            exchange,
            market_id,
            cancel_client_order_ids,
            place_orders,
            timestamp_ns,
            instance_id,
        }
        | Signal::BatchUpdateOrders {
            exchange,
            market_id,
            cancel_client_order_ids,
            place_orders,
            timestamp_ns,
            instance_id,
        } => {
            let cancel = (!cancel_client_order_ids.is_empty()).then(|| Signal::BatchCancelOrders {
                exchange,
                market_id,
                client_order_ids: cancel_client_order_ids,
                instance_id,
                timestamp_ns,
            });
            (place_orders, cancel)
        }
        other => {
            // The dispatcher only calls this helper for place-bearing signals.
            // Preserve a future control variant losslessly if classification
            // and the Signal enum temporarily drift apart.
            let _ = cancel_tx.send((other, stale_ms, utx.clone()));
            return;
        }
    };
    if let Some(cancel) = cancel {
        let _ = cancel_tx.send((cancel, stale_ms, utx.clone()));
    }
    warn!(
        "[Executor] Polymarket place lane saturated; shedding {} unsent place(s), cancels retained",
        places.len(),
    );
    for order in places {
        if send_executor_update(utx, exec_rejected_place(&order)).is_err() {
            break;
        }
    }
}

fn defer_reconcile_signal(signal: Signal, utx: &ExecutorUpdateSender) {
    let iid = extract_instance_id(&signal);
    let Signal::ReconcilePolymarket {
        pending_places,
        pending_cancels,
        pending_trade_ids,
        ..
    } = signal
    else {
        return;
    };
    warn!(
        "[Executor] Polymarket reconcile lane saturated iid={}; returning deferred feedback",
        iid,
    );
    for update in
        reconcile_deferred_updates(&iid, &pending_places, &pending_cancels, &pending_trade_ids)
    {
        if send_executor_update(utx, update).is_err() {
            break;
        }
    }
}

/// Fire-and-track + admission dispatch for the hot single-leg Polymarket
/// paths (place / cancel / 1×1 replace). Maps instance→account, acquires an
/// account-pool permit, fires on the reserved connection WITHOUT blocking, and hands the
/// reply-completion closure to a drainer. Placement may be skipped when stale
/// or saturated; cancellation is retained and woken by permit release.
/// Everything else (batch, reconcile, cancel-all, non-poly, empty/unknown
/// instance) falls through to the synchronous `execute_fallback_signal`.
fn fire_or_execute(
    worker: &mut LiveRouter,
    signal: Signal,
    stale_ms: u64,
    done_tx: &Sender<QueuedPolyCompletion>,
    cancel_tx: Option<&Sender<(Signal, u64, ExecutorUpdateSender)>>,
    utx: &ExecutorUpdateSender,
) {
    use hexagent_runtime::http1_pool::{acquire, try_acquire, Role};
    // Same stale semantics as the sync path's `is_stale`.
    let is_stale =
        |ts: u64| ts != 0 && stale_ms != 0 && now_ns().saturating_sub(ts) / 1_000_000 > stale_ms;
    let iid = extract_instance_id(&signal);
    log_executor_receive(&signal);

    match signal {
        Signal::NewOrder(order)
            if order.exchange == Exchange::Polymarket && !order.instance_id.is_empty() =>
        {
            if order.timestamp_ns > 0 {
                crate::latency::record_ns(
                    if order.post_only {
                        "polymarket.order.post_only.quote_age_at_dispatch"
                    } else {
                        "polymarket.order.quote_age_at_dispatch"
                    },
                    now_ns().saturating_sub(order.timestamp_ns),
                );
            }
            if order.quote_trigger_local_timestamp_ns > 0 {
                crate::latency::record_ns(
                    "polymarket.order.trigger_age_at_dispatch",
                    now_ns().saturating_sub(order.quote_trigger_local_timestamp_ns),
                );
            }
            // `timestamp_ns` only proves the strategy emitted recently. Also
            // reject a freshly-emitted order computed from a market trigger
            // that sat stale in a scheduler/callback queue.
            if is_stale(order.timestamp_ns) || is_stale(order.quote_trigger_local_timestamp_ns) {
                let _ = send_executor_update(utx, exec_rejected_place(&order));
                return;
            }
            match try_acquire(&iid, Role::Fast) {
                None => {
                    let _ = send_executor_update(utx, exec_rejected_place(&order));
                    // skip = hold
                }
                Some(permit) => {
                    let client = permit.pooled_client();
                    let route = match worker.poly_route_mut(&iid) {
                        Ok(route) => route,
                        Err(error) => {
                            error!("[Executor] Polymarket place route error: {}", error);
                            drop(permit);
                            let _ = send_executor_update(utx, exec_rejected_place(&order));
                            return;
                        }
                    };
                    match route.submit_fire(&order, client) {
                        Ok(pending) => {
                            let iid2 = iid;
                            let f: PolyCompletionFn = Box::new(move |r| {
                                let route = match r.poly_route_mut(&iid2) {
                                    Ok(route) => route,
                                    Err(error) => {
                                        error!(
                                            "[Executor] Polymarket completion route error: {}",
                                            error
                                        );
                                        drop(permit);
                                        return vec![exec_rejected_place(&order)];
                                    }
                                };
                                let u = route.complete_submit(&order, pending);
                                drop(permit); // release the reserved connection
                                vec![u]
                            });
                            let _ = done_tx.send(QueuedPolyCompletion {
                                complete: f,
                                update_tx: utx.clone(),
                                kind: "place",
                                enqueued_at: std::time::Instant::now(),
                            });
                        }
                        Err(update) => {
                            drop(permit); // pre-flight reject: nothing sent
                            let _ = send_executor_update(utx, update);
                        }
                    }
                }
            }
        }
        Signal::CancelOrder {
            exchange,
            client_order_id,
            timestamp_ns,
            ..
        } if exchange == Exchange::Polymarket && !iid.is_empty() => {
            // A cancel is monotonic while the order is live: unlike a place,
            // age does not make it unsafe. Retain it across temporary pool
            // saturation and wake on permit release instead of reverting the
            // order to Active for a quote-tick retry.
            let wait_started = std::time::Instant::now();
            match acquire(&iid, Role::Cancel) {
                None => {
                    // Unknown instance/role is permanent; ordinary saturation
                    // never reaches this branch.
                    let _ =
                        send_executor_update(utx, exec_rejected_cancel(client_order_id, exchange));
                }
                Some(permit) => {
                    let wait = wait_started.elapsed();
                    if wait >= std::time::Duration::from_millis(1) {
                        info!(
                            "[Executor] retained cancel acquired iid={} coid={} wait_ms={:.3}",
                            iid,
                            client_order_id,
                            wait.as_secs_f64() * 1000.0,
                        );
                    }
                    let client = permit.pooled_client();
                    let route = match worker.poly_route_mut(&iid) {
                        Ok(route) => route,
                        Err(error) => {
                            error!("[Executor] Polymarket cancel route error: {}", error);
                            drop(permit);
                            let _ = send_executor_update(
                                utx,
                                exec_rejected_cancel(client_order_id, exchange),
                            );
                            return;
                        }
                    };
                    route.set_gen_ns_hint(timestamp_ns);
                    let pending = route.cancel_fire(&client_order_id, client);
                    let iid2 = iid;
                    let f: PolyCompletionFn = Box::new(move |r| {
                        let route = match r.poly_route_mut(&iid2) {
                            Ok(route) => route,
                            Err(error) => {
                                error!(
                                    "[Executor] Polymarket cancel completion route error: {}",
                                    error
                                );
                                drop(permit);
                                return vec![exec_rejected_cancel(client_order_id, exchange)];
                            }
                        };
                        let u = route.complete_cancel(exchange, &client_order_id, pending);
                        drop(permit);
                        vec![u]
                    });
                    let _ = done_tx.send(QueuedPolyCompletion {
                        complete: f,
                        update_tx: utx.clone(),
                        kind: "cancel",
                        enqueued_at: std::time::Instant::now(),
                    });
                }
            }
        }
        Signal::ReplaceOrder {
            exchange,
            cancel_client_order_ids,
            place_orders,
            timestamp_ns,
            ..
        } if exchange == Exchange::Polymarket
            && !iid.is_empty()
            && cancel_client_order_ids.len() == 1
            && place_orders.len() == 1 =>
        {
            let coid = cancel_client_order_ids.into_iter().next().unwrap();
            let place = place_orders.into_iter().next().unwrap();
            if place.timestamp_ns > 0 {
                crate::latency::record_ns(
                    if place.post_only {
                        "polymarket.order.post_only.quote_age_at_dispatch"
                    } else {
                        "polymarket.order.quote_age_at_dispatch"
                    },
                    now_ns().saturating_sub(place.timestamp_ns),
                );
            }
            if place.quote_trigger_local_timestamp_ns > 0 {
                crate::latency::record_ns(
                    "polymarket.order.trigger_age_at_dispatch",
                    now_ns().saturating_sub(place.quote_trigger_local_timestamp_ns),
                );
            }
            // ReplaceOrder is emitted only for a strictly more-aggressive
            // replacement. Admit/fire the place leg independently BEFORE a
            // potentially saturated retained-cancel wait, so Cancel-pool
            // pressure cannot serialize or stale an otherwise-free Fast leg.
            // Account admission still requires enough physical + virtual funds
            // for both old and new reservations at the same time.
            if is_stale(timestamp_ns) || is_stale(place.quote_trigger_local_timestamp_ns) {
                let _ = send_executor_update(utx, exec_rejected_place(&place));
            } else {
                match try_acquire(&iid, Role::Fast) {
                    None => {
                        let _ = send_executor_update(utx, exec_rejected_place(&place));
                    }
                    Some(ppermit) => {
                        let pclient = ppermit.pooled_client();
                        let route = match worker.poly_route_mut(&iid) {
                            Ok(route) => route,
                            Err(error) => {
                                error!(
                                    "[Executor] Polymarket replace-place route error: {}",
                                    error
                                );
                                drop(ppermit);
                                let _ = send_executor_update(utx, exec_rejected_place(&place));
                                let _ =
                                    send_executor_update(utx, exec_rejected_cancel(coid, exchange));
                                return;
                            }
                        };
                        match route.submit_fire(&place, pclient) {
                            Ok(pending) => {
                                let iid_p = iid.clone();
                                let pf: PolyCompletionFn = Box::new(move |r| {
                                    let route = match r.poly_route_mut(&iid_p) {
                                        Ok(route) => route,
                                        Err(error) => {
                                            error!(
                                                "[Executor] Polymarket replace-place completion route error: {}",
                                                error
                                            );
                                            drop(ppermit);
                                            return vec![exec_rejected_place(&place)];
                                        }
                                    };
                                    let u = route.complete_submit(&place, pending);
                                    drop(ppermit);
                                    vec![u]
                                });
                                let _ = done_tx.send(QueuedPolyCompletion {
                                    complete: pf,
                                    update_tx: utx.clone(),
                                    kind: "place",
                                    enqueued_at: std::time::Instant::now(),
                                });
                            }
                            Err(update) => {
                                drop(ppermit);
                                let _ = send_executor_update(utx, update);
                            }
                        }
                    }
                }
            }

            // Cancel remains monotonic and lossless, but its permit wait must
            // never occupy this place worker. The dedicated cancel lane owns
            // both waiting and fire/track completion for the split leg.
            let cancel_signal = Signal::CancelOrder {
                exchange,
                client_order_id: coid.clone(),
                instance_id: iid,
                timestamp_ns,
            };
            if cancel_tx.is_none_or(|cancel_tx| {
                cancel_tx
                    .send((cancel_signal, stale_ms, utx.clone()))
                    .is_err()
            })
            {
                let _ = send_executor_update(utx, exec_rejected_cancel(coid, exchange));
            }
        }
        // Reconcile: concurrency gate on the dedicated per-account Reconcile
        // pool (NOT full fire-track). The permit's exact client is threaded
        // through every order GET, so this is both admission control and a real
        // connection reservation. When none are free, return explicit
        // admission feedback so the strategy rolls back the in-flight/backoff
        // state it committed before emitting. Gating on the Reconcile pool
        // (disjoint from Fast/Cancel) means it never steals hot-path capacity.
        Signal::ReconcilePolymarket {
            pending_places,
            pending_cancels,
            pending_trade_ids,
            ..
        } if !iid.is_empty() => match try_acquire(&iid, Role::Reconcile) {
            None => {
                for update in reconcile_deferred_updates(
                    &iid,
                    &pending_places,
                    &pending_cancels,
                    &pending_trade_ids,
                ) {
                    if send_executor_update(utx, update).is_err() {
                        break;
                    }
                }
            }
            Some(permit) => {
                let updates = match worker.poly_route_mut(&iid) {
                    Ok(route) => route.reconcile_orphans_on(
                        &permit,
                        &pending_places,
                        &pending_cancels,
                        &pending_trade_ids,
                    ),
                    Err(error) => {
                        error!("[Executor] Polymarket reconcile route error: {}", error);
                        reconcile_deferred_updates(
                            &iid,
                            &pending_places,
                            &pending_cancels,
                            &pending_trade_ids,
                        )
                    }
                };
                for update in updates {
                    if send_executor_update(utx, update).is_err() {
                        break;
                    }
                }
            }
        },
        // Batch / cancel-all / non-poly / empty-iid → synchronous fallback.
        // polymaker's only batch signals are BatchCancelOrders (a leg pulling
        // >1 accumulated/orphan order with no replace — the `live_count >= 2`
        // "cancel-stale-don't-place" cleanup) and the rare BatchNewOrders (a
        // leg seeding both tokens from cold). Both are low-volume edge/cleanup
        // paths (~6/hr live), NOT the hot reprice churn — that is ReplaceOrder
        // 1×1 (leg carries a single token in steady state), already fire-
        // tracked above. Their HTTP internals best-effort borrow account slots
        // and spill to the fixed global fallback-order pool when saturated.
        other => {
            for update in execute_fallback_signal(worker, other, stale_ms) {
                if send_executor_update(utx, update).is_err() {
                    break;
                }
            }
        }
    }
}

fn execute_fallback_signal(
    executor: &mut LiveRouter,
    signal: Signal,
    stale_threshold_ms: u64,
) -> Vec<OrderUpdate> {
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
            exchange_event_timestamp_ns: None,
            trade_id: None,
            order_audit: None,
            error: None,
        }
    };
    let is_stale = |ts: u64| -> bool {
        if ts == 0 || stale_threshold_ms == 0 {
            return false;
        }
        let now = now_ns();
        now.saturating_sub(ts) / 1_000_000 > stale_threshold_ms
    };
    let has_stale_trigger = |orders: &[OrderRequest]| {
        orders
            .iter()
            .any(|order| is_stale(order.quote_trigger_local_timestamp_ns))
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
            if is_stale(order.timestamp_ns) || is_stale(order.quote_trigger_local_timestamp_ns) {
                warn!(
                    "[Executor] Signal stale ({}ms > {}ms), dropping NewOrder coid={}",
                    (now_ns().saturating_sub(order.timestamp_ns)) / 1_000_000,
                    stale_threshold_ms,
                    order.client_order_id
                );
                return vec![build_exec_rejected_place(&order)];
            }
            let result = if order.exchange == Exchange::Polymarket {
                executor
                    .poly_route_mut(&instance_id)
                    .and_then(|route| route.submit_order(&order))
            } else {
                executor.submit_order(&order)
            };
            match result {
                Ok(update) => vec![update],
                Err(e) => {
                    error!("[Executor] Submit error: {}", e);
                    vec![OrderUpdate {
                        client_order_id: order.client_order_id,
                        exchange: order.exchange,
                        symbol: order.symbol,
                        side: order.side,
                        exchange_order_id: None,
                        status: OrderStatus::Rejected,
                        liquidity: None,
                        filled_quantity: 0.0,
                        remaining_quantity: order.quantity,
                        avg_fill_price: 0.0,
                        timestamp_ns: now_ns(),
                        exchange_event_timestamp_ns: None,
                        trade_id: None,
                        order_audit: None,
                        error: None,
                    }]
                }
            }
        }
        Signal::CancelOrder {
            exchange,
            client_order_id,
            timestamp_ns,
            ..
        } => {
            // Cancellation is monotonic: age can make a replacement quote
            // stale, but never makes pulling the old order unsafe.
            let result = if exchange == Exchange::Polymarket {
                executor.poly_route_mut(&instance_id).and_then(|route| {
                    route.set_gen_ns_hint(timestamp_ns); // `gen_ns=` on the cancel log line
                    route.cancel_order(exchange, &client_order_id)
                })
            } else {
                executor.cancel_order(exchange, &client_order_id)
            };
            match result {
                Ok(update) => vec![update],
                Err(e) => {
                    error!("[Executor] Cancel error: {}", e);
                    vec![]
                }
            }
        }
        Signal::CancelAll {
            exchange, symbol, ..
        } => {
            let result = if exchange == Exchange::Polymarket {
                executor
                    .poly_route_mut(&instance_id)
                    .and_then(|route| route.cancel_all(exchange, &symbol))
            } else {
                executor.cancel_all(exchange, &symbol)
            };
            result.unwrap_or_else(|e| {
                error!("[Executor] Cancel-all error: {}", e);
                vec![]
            })
        }
        Signal::BatchNewOrders {
            exchange,
            market_id,
            orders,
            ..
        } => {
            let oldest_ts = orders.iter().map(|o| o.timestamp_ns).min().unwrap_or(0);
            if is_stale(oldest_ts) || has_stale_trigger(&orders) {
                warn!(
                    "[Executor] Signal stale, dropping BatchNewOrders ({} orders)",
                    orders.len()
                );
                return orders.iter().map(build_exec_rejected_place).collect();
            }
            let result = if exchange == Exchange::Polymarket {
                executor
                    .poly_route_mut(&instance_id)
                    .and_then(|route| route.batch_submit_orders(&market_id, &orders))
            } else {
                executor.batch_submit_orders(&market_id, &orders)
            };
            result.unwrap_or_else(|e| {
                error!("[Executor] Batch place error: {}", e);
                vec![]
            })
        }
        Signal::BatchCancelOrders {
            exchange,
            market_id,
            client_order_ids,
            timestamp_ns,
            ..
        } => {
            let result = if exchange == Exchange::Polymarket {
                executor.poly_route_mut(&instance_id).and_then(|route| {
                    route.set_gen_ns_hint(timestamp_ns); // `gen_ns=` on the cancel log lines
                    route.batch_cancel_orders(exchange, &market_id, &client_order_ids)
                })
            } else {
                executor.batch_cancel_orders(exchange, &market_id, &client_order_ids)
            };
            result.unwrap_or_else(|e| {
                error!("[Executor] Batch cancel error: {}", e);
                vec![]
            })
        }
        Signal::BatchUpdateOrders {
            exchange,
            market_id,
            cancel_client_order_ids,
            place_orders,
            timestamp_ns,
            ..
        } => {
            if is_stale(timestamp_ns) || has_stale_trigger(&place_orders) {
                warn!(
                    "[Executor] Signal stale, retaining {} BatchUpdateOrders cancels and dropping {} places",
                    cancel_client_order_ids.len(),
                    place_orders.len(),
                );
                let mut out = if exchange == Exchange::Polymarket {
                    executor.poly_route_mut(&instance_id).and_then(|route| {
                        route.set_gen_ns_hint(timestamp_ns);
                        route.batch_cancel_orders(exchange, &market_id, &cancel_client_order_ids)
                    })
                } else {
                    executor.batch_cancel_orders(exchange, &market_id, &cancel_client_order_ids)
                }
                .unwrap_or_else(|error| {
                    error!("[Executor] Retained batch cancel error: {}", error);
                    vec![]
                });
                out.extend(place_orders.iter().map(build_exec_rejected_place));
                return out;
            }
            let result = if exchange == Exchange::Polymarket {
                executor.poly_route_mut(&instance_id).and_then(|route| {
                    route.set_gen_ns_hint(timestamp_ns); // `gen_ns=` on the cancel log lines
                    route.batch_update_orders(
                        exchange,
                        &market_id,
                        &cancel_client_order_ids,
                        &place_orders,
                    )
                })
            } else {
                executor.batch_update_orders(
                    exchange,
                    &market_id,
                    &cancel_client_order_ids,
                    &place_orders,
                )
            };
            result.unwrap_or_else(|e| {
                error!("[Executor] Batch update error: {}", e);
                vec![]
            })
        }
        Signal::ReplaceOrder {
            exchange,
            market_id,
            cancel_client_order_ids,
            place_orders,
            timestamp_ns,
            ..
        } => {
            if is_stale(timestamp_ns) || has_stale_trigger(&place_orders) {
                warn!(
                    "[Executor] Signal stale, retaining {} ReplaceOrder cancels and dropping {} places",
                    cancel_client_order_ids.len(),
                    place_orders.len(),
                );
                let mut out = if exchange == Exchange::Polymarket {
                    executor.poly_route_mut(&instance_id).and_then(|route| {
                        route.set_gen_ns_hint(timestamp_ns);
                        route.batch_cancel_orders(exchange, &market_id, &cancel_client_order_ids)
                    })
                } else {
                    executor.batch_cancel_orders(exchange, &market_id, &cancel_client_order_ids)
                }
                .unwrap_or_else(|error| {
                    error!("[Executor] Retained replace cancel error: {}", error);
                    vec![]
                });
                out.extend(place_orders.iter().map(build_exec_rejected_place));
                return out;
            }
            let result = if exchange == Exchange::Polymarket {
                executor.poly_route_mut(&instance_id).and_then(|route| {
                    route.set_gen_ns_hint(timestamp_ns); // `gen_ns=` on the cancel log lines
                    route.replace_order(
                        exchange,
                        &market_id,
                        &cancel_client_order_ids,
                        &place_orders,
                    )
                })
            } else {
                executor.replace_order(
                    exchange,
                    &market_id,
                    &cancel_client_order_ids,
                    &place_orders,
                )
            };
            result.unwrap_or_else(|e| {
                error!("[Executor] Replace error: {}", e);
                vec![]
            })
        }
        Signal::ReconcilePolymarket {
            pending_places,
            pending_cancels,
            pending_trade_ids,
            ..
        } => match executor.poly_route_mut(&instance_id) {
            Ok(route) => {
                route.reconcile_orphans(&pending_places, &pending_cancels, &pending_trade_ids)
            }
            Err(error) => {
                error!("[Executor] Polymarket reconcile route error: {}", error);
                reconcile_deferred_updates(
                    &instance_id,
                    &pending_places,
                    &pending_cancels,
                    &pending_trade_ids,
                )
            }
        },
        Signal::PolymarketCancelAllOrders {
            reason,
            market,
            asset_ids,
            ..
        } => {
            let route = match executor.poly_route_mut(&instance_id) {
                Ok(route) => route,
                Err(error) => {
                    error!(
                        "[Executor] Polymarket cancel-all route error instance_id={}: {}",
                        instance_id, error,
                    );
                    return vec![OrderUpdate {
                        client_order_id: instance_id,
                        exchange: Exchange::Polymarket,
                        symbol: market.unwrap_or_default(),
                        side: Side::Buy,
                        exchange_order_id: None,
                        status: OrderStatus::ExecutorRejected,
                        liquidity: None,
                        filled_quantity: 0.0,
                        remaining_quantity: 0.0,
                        avg_fill_price: 0.0,
                        timestamp_ns: now_ns(),
                        exchange_event_timestamp_ns: None,
                        trade_id: None,
                        order_audit: None,
                        error: Some(error.to_string()),
                    }];
                }
            };
            match market {
                Some(cid) => {
                    let routine_expiry_cancel = is_routine_expiry_cancel(&reason, true);
                    if routine_expiry_cancel {
                        info!(
                            "[Executor] PolymarketCancelAllOrders market={} ({} tokens, instance_id={}): reason={}",
                            cid,
                            asset_ids.len(),
                            instance_id,
                            reason
                        );
                    } else {
                        warn!(
                            "[Executor] PolymarketCancelAllOrders market={} ({} tokens, instance_id={}): reason={}",
                            cid,
                            asset_ids.len(),
                            instance_id,
                            reason
                        );
                    }
                    let result = route.cancel_market_orders_until_final(
                        &cid,
                        &asset_ids,
                        routine_expiry_cancel,
                    );
                    let mut updates = result.updates;
                    let (status, error) = if result.confirmed {
                        (
                            OrderStatus::Cancelled,
                            POLYMARKET_MARKET_CANCEL_FINALITY_CONFIRMED.to_string(),
                        )
                    } else {
                        (
                            OrderStatus::CancelUncertain,
                            format!(
                                "{}: {}",
                                POLYMARKET_MARKET_CANCEL_FINALITY_PENDING, result.detail,
                            ),
                        )
                    };
                    updates.push(OrderUpdate {
                        client_order_id: instance_id.clone(),
                        exchange: Exchange::Polymarket,
                        symbol: cid,
                        side: Side::Buy,
                        exchange_order_id: None,
                        status,
                        liquidity: None,
                        filled_quantity: 0.0,
                        remaining_quantity: 0.0,
                        avg_fill_price: 0.0,
                        timestamp_ns: now_ns(),
                        exchange_event_timestamp_ns: None,
                        trade_id: None,
                        order_audit: None,
                        error: Some(error),
                    });
                    return updates;
                }
                None => {
                    if instance_id.is_empty() {
                        warn!(
                            "[Executor] PolymarketCancelAllOrders account-wide: reason={}",
                            reason
                        );
                        route.cancel_all_orders();
                    } else {
                        return route.cancel_instance_orders().unwrap_or_else(|error| {
                            error!("[Executor] instance-scoped emergency cancel failed instance_id={}: {}", instance_id, error);
                            vec![]
                        });
                    }
                }
            }
            vec![]
        }
        Signal::RetainPolymarketEventAudit {
            condition_id,
            asset_ids,
            ..
        } => {
            let retained = executor
                .poly_route_mut(&instance_id)
                .and_then(|route| route.retain_event_audit(&condition_id, &asset_ids));
            if let Err(error) = retained {
                error!(
                    "[Executor] failed to retain Polymarket event audit instance_id={} condition_id={}: {}",
                    instance_id, condition_id, error,
                );
            }
            vec![]
        }
        Signal::RetirePolymarketEventAudit {
            condition_id,
            asset_ids,
            ..
        } => {
            match executor.poly_route_mut(&instance_id) {
                Ok(route) => route.retire_event_audit(&condition_id, &asset_ids),
                Err(error) => error!(
                    "[Executor] failed to retire Polymarket event audit instance_id={} condition_id={}: {}",
                    instance_id, condition_id, error,
                ),
            }
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
    /// Live-mutable back-compat view: returns the default instance's
    /// `PolymarketTrade` for callers that haven't yet been migrated
    /// to per-instance routing. Kept as a separate clone so methods
    /// taking `&mut self.polymarket` keep compiling. `None` when no
    /// polymaker instances are configured (e.g. a hypermaker-only live
    /// deployment) — poly routes are never hit in that case.
    polymarket: Option<PolymarketTrade>,
    hexmarket: HexmarketTrade,
    /// Hyperliquid executor — `None` unless an enabled `[[exchanges]]`
    /// `hyperliquid` block is present (and its meta fetch succeeded).
    hyperliquid: Option<HyperliquidTrade>,
    /// Aster executor — `None` unless an enabled `[[exchanges]]` `aster`
    /// block is present (and its exchangeInfo fetch succeeded).
    aster: Option<AsterTrade>,
    /// Lighter executor — `None` unless an enabled `[[exchanges]]` `lighter`
    /// block is present (and its orderBookDetails fetch succeeded).
    lighter: Option<LighterTrade>,
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
            // Tag the route with its instance_id so orders placed through it
            // carry it (TrackedOrder.instance_id) → instance-scoped cancels.
            poly_routes.insert(
                (*id).clone(),
                PolymarketTrade::from_shared(shared, &owner, id),
            );
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
        // `None` when no polymaker instances are configured — a valid state
        // for a hypermaker-only (or other non-poly) live deployment. Poly
        // routes are only ever hit for `Exchange::Polymarket` signals, which
        // can't be emitted without a polymaker strategy, so the `None` here
        // is never dereferenced in that case.
        let polymarket = if !poly_default_id.is_empty() {
            let shared = states.get(&poly_default_id).cloned().unwrap();
            let owner = shared.auth.api_key.clone();
            Some(PolymarketTrade::from_shared(
                shared,
                &owner,
                &poly_default_id,
            ))
        } else {
            None
        };

        // Build the Hyperliquid executor. Credentials come from
        // `[hyperliquid.<account_id>]` in secrets.toml, keyed by the first
        // enabled hypermaker strategy's `account_id` (fallback `instance_id`);
        // non-secret settings (network / host overrides) come from the
        // `[[exchanges]] hyperliquid` block. Meta (coin→asset index) is fetched
        // once here; any failure logs and leaves the venue disabled rather than
        // aborting the engine.
        let hyperliquid = {
            let hl_cfg = config
                .exchanges
                .iter()
                .find(|e| e.name == "hyperliquid" && e.enabled);
            let acct = config
                .strategies
                .iter()
                .find(|s| s.enabled && s.name == "hypermaker")
                .map(|s| {
                    if s.account_id.is_empty() {
                        s.instance_id.clone()
                    } else {
                        s.account_id.clone()
                    }
                });
            match (hl_cfg, acct) {
                (Some(hl), Some(acct)) if !acct.is_empty() => {
                    let secrets = crate::config::SecretsFile::load_from_config(config);
                    match secrets.hyperliquid.get(&acct) {
                        Some(cred) => match crate::exchange::hyperliquid::auth::HlAuth::new(
                            &cred.private_key,
                            &cred.account_address,
                            &hl.network,
                            &hl.api_url_prefix,
                            &hl.wss_url,
                        ) {
                            Ok(auth) => match crate::exchange::hyperliquid::info::fetch_meta(
                                &auth.info_url(),
                            ) {
                                Ok(meta) => {
                                    info!(
                                        "[Hyperliquid] executor ready (account_id={}, network={}, account={}, signer={})",
                                        acct, hl.network, auth.account_address, auth.signer_address,
                                    );
                                    Some(HyperliquidTrade::new(auth, meta, &acct))
                                }
                                Err(e) => {
                                    error!(
                                        "[Hyperliquid] meta fetch failed, venue disabled: {}",
                                        e
                                    );
                                    None
                                }
                            },
                            Err(e) => {
                                error!(
                                    "[Hyperliquid] auth build failed (account_id={}), venue disabled: {}",
                                    acct, e
                                );
                                None
                            }
                        },
                        None => {
                            error!(
                                "[Hyperliquid] no `[hyperliquid.{}]` block in secrets — venue disabled",
                                acct,
                            );
                            None
                        }
                    }
                }
                _ => None,
            }
        };

        // Build the Aster executor. Credentials come from
        // `[aster.<account_id>]` in secrets.toml, keyed by the first enabled
        // astermaker strategy's `account_id` (fallback `instance_id`);
        // non-secret settings (network / host overrides) come from the
        // `[[exchanges]] aster` block. exchangeInfo (tick/step size) is
        // fetched once here; any failure logs and leaves the venue disabled
        // rather than aborting the engine.
        let aster = {
            let as_cfg = config
                .exchanges
                .iter()
                .find(|e| e.name == "aster" && e.enabled);
            let acct = config
                .strategies
                .iter()
                .find(|s| s.enabled && s.name == "astermaker")
                .map(|s| {
                    if s.account_id.is_empty() {
                        s.instance_id.clone()
                    } else {
                        s.account_id.clone()
                    }
                });
            match (as_cfg, acct) {
                (Some(ax), Some(acct)) if !acct.is_empty() => {
                    let secrets = crate::config::SecretsFile::load_from_config(config);
                    match secrets.aster.get(&acct) {
                        Some(cred) => {
                            match crate::exchange::aster::auth::AsterAuth::new(
                                &cred.private_key,
                                &cred.user_address,
                                &ax.network,
                                &ax.api_url_prefix,
                                &ax.wss_url,
                            ) {
                                Ok(auth) => {
                                    // Prewarm the h1.1 pools against the Aster host
                                    // BEFORE the first REST call (exchangeInfo below,
                                    // positionRisk in the strategy factory), then
                                    // keep every pooled connection warm through
                                    // quiet stretches.
                                    hexagent_runtime::http1_pool::spawn_keep_warm(
                                        "aster",
                                        format!("{}/fapi/v3/time", auth.rest_base()),
                                        std::time::Duration::from_secs(20),
                                    );
                                    match crate::exchange::aster::info::fetch_meta(
                                        &auth.rest_base(),
                                    ) {
                                        Ok(meta) => {
                                            info!(
                                                "[Aster] executor ready (account_id={}, network={}, user={}, signer={})",
                                                acct,
                                                ax.network,
                                                auth.user_address,
                                                auth.signer_address,
                                            );
                                            Some(AsterTrade::new(auth, meta, &acct))
                                        }
                                        Err(e) => {
                                            error!(
                                                "[Aster] exchangeInfo fetch failed, venue disabled: {}",
                                                e
                                            );
                                            None
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "[Aster] auth build failed (account_id={}), venue disabled: {}",
                                        acct, e
                                    );
                                    None
                                }
                            }
                        }
                        None => {
                            error!(
                                "[Aster] no `[aster.{}]` block in secrets — venue disabled",
                                acct,
                            );
                            None
                        }
                    }
                }
                _ => None,
            }
        };

        // Build the Lighter executor. Credentials come from
        // `[lighter.<account_id>]` in secrets.toml, keyed by the first
        // enabled litmaker strategy's `account_id` (fallback `instance_id`);
        // non-secret settings (network / host overrides) come from the
        // `[[exchanges]] lighter` block. orderBookDetails (market ids +
        // price/size decimals) is fetched once here; any failure logs and
        // leaves the venue disabled rather than aborting the engine.
        let lighter = {
            let lt_cfg = config
                .exchanges
                .iter()
                .find(|e| e.name == "lighter" && e.enabled);
            let acct = config
                .strategies
                .iter()
                .find(|s| s.enabled && s.name == "litmaker")
                .map(|s| {
                    if s.account_id.is_empty() {
                        s.instance_id.clone()
                    } else {
                        s.account_id.clone()
                    }
                });
            match (lt_cfg, acct) {
                (Some(lt), Some(acct)) if !acct.is_empty() => {
                    let secrets = crate::config::SecretsFile::load_from_config(config);
                    match secrets.lighter.get(&acct) {
                        Some(cred) => match crate::exchange::lighter::auth::LighterAuth::new(
                            &cred.private_key,
                            cred.account_index,
                            cred.api_key_index,
                            &lt.network,
                            &lt.api_url_prefix,
                            &lt.wss_url,
                        ) {
                            Ok(auth) => {
                                match crate::exchange::lighter::info::fetch_meta(&auth.rest_base())
                                {
                                    Ok(meta) => {
                                        info!(
                                            "[Lighter] executor ready (account_id={}, network={}, account_index={}, api_key_index={})",
                                            acct,
                                            lt.network,
                                            auth.account_index(),
                                            auth.api_key_index(),
                                        );
                                        Some(LighterTrade::new(auth, meta, &acct))
                                    }
                                    Err(e) => {
                                        error!(
                                            "[Lighter] orderBookDetails fetch failed, venue disabled: {}",
                                            e
                                        );
                                        None
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    "[Lighter] auth build failed (account_id={}), venue disabled: {}",
                                    acct, e
                                );
                                None
                            }
                        },
                        None => {
                            error!(
                                "[Lighter] no `[lighter.{}]` block in secrets — venue disabled",
                                acct,
                            );
                            None
                        }
                    }
                }
                _ => None,
            }
        };

        Self {
            binance: BinanceTrade::new(),
            poly_routes,
            polymarket,
            hexmarket: HexmarketTrade::new(
                hex_private_key,
                hex_mnemonic,
                hex_api_host,
                hex_cfg.map(|e| e.rate_limit_per_second).unwrap_or(10),
            ),
            hyperliquid,
            aster,
            lighter,
        }
    }

    /// Look up the `PolymarketTrade` for a given `instance_id`. Legacy signals
    /// with an empty id may use the default route, but an explicit unknown id
    /// is never sent through a sibling wallet. Missing routes are ordinary
    /// executor errors rather than process panics.
    #[allow(dead_code)]
    fn poly_route_mut(&mut self, instance_id: &str) -> Result<&mut PolymarketTrade> {
        if !instance_id.is_empty() {
            if !self.poly_routes.contains_key(instance_id) {
                let mut routes: Vec<&str> = self.poly_routes.keys().map(String::as_str).collect();
                routes.sort_unstable();
                return Err(anyhow::anyhow!(
                    "unknown Polymarket instance_id `{}` (available routes: [{}])",
                    instance_id,
                    routes.join(","),
                ));
            }
            return match self.poly_routes.get_mut(instance_id) {
                Some(route) => Ok(route),
                None => Err(anyhow::anyhow!(
                    "Polymarket route `{}` disappeared during executor lookup",
                    instance_id,
                )),
            };
        }
        self.polymarket.as_mut().ok_or_else(|| {
            anyhow::anyhow!("Polymarket route requested with no PolymarketTrade configured")
        })
    }

    /// The default `PolymarketTrade`, or a clear error if no polymaker
    /// instance is configured (a hypermaker-only deployment).
    fn poly_mut(&mut self) -> Result<&mut PolymarketTrade> {
        self.polymarket.as_mut().ok_or_else(|| {
            anyhow::anyhow!("polymarket venue not configured (no polymaker instances)")
        })
    }

    /// The Hyperliquid executor, or a clear error if the venue isn't
    /// configured/enabled.
    fn hl_mut(&mut self) -> Result<&mut HyperliquidTrade> {
        self.hyperliquid
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("hyperliquid venue not configured/enabled"))
    }

    /// The Aster executor, or a clear error if the venue isn't
    /// configured/enabled.
    fn aster_mut(&mut self) -> Result<&mut AsterTrade> {
        self.aster
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("aster venue not configured/enabled"))
    }

    /// The Lighter executor, or a clear error if the venue isn't
    /// configured/enabled.
    fn lighter_mut(&mut self) -> Result<&mut LighterTrade> {
        self.lighter
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("lighter venue not configured/enabled"))
    }
}

impl ExchangeTrade for LiveRouter {
    fn submit_order(&mut self, order: &OrderRequest) -> Result<OrderUpdate> {
        match order.exchange {
            Exchange::Binance => self.binance.submit_order(order),
            Exchange::Polymarket => self.poly_mut()?.submit_order(order),
            Exchange::Hexmarket => self.hexmarket.submit_order(order),
            Exchange::Hyperliquid => self.hl_mut()?.submit_order(order),
            Exchange::Aster => self.aster_mut()?.submit_order(order),
            Exchange::Lighter => self.lighter_mut()?.submit_order(order),
            _ => Err(anyhow::anyhow!(
                "Trading not supported on {:?}",
                order.exchange
            )),
        }
    }

    fn cancel_order(&mut self, exchange: Exchange, client_order_id: &str) -> Result<OrderUpdate> {
        match exchange {
            Exchange::Binance => self.binance.cancel_order(exchange, client_order_id),
            Exchange::Polymarket => self.poly_mut()?.cancel_order(exchange, client_order_id),
            Exchange::Hexmarket => self.hexmarket.cancel_order(exchange, client_order_id),
            Exchange::Hyperliquid => self.hl_mut()?.cancel_order(exchange, client_order_id),
            Exchange::Aster => self.aster_mut()?.cancel_order(exchange, client_order_id),
            Exchange::Lighter => self.lighter_mut()?.cancel_order(exchange, client_order_id),
            _ => Err(anyhow::anyhow!("Trading not supported on {:?}", exchange)),
        }
    }

    fn cancel_all(&mut self, exchange: Exchange, symbol: &str) -> Result<Vec<OrderUpdate>> {
        match exchange {
            Exchange::Binance => self.binance.cancel_all(exchange, symbol),
            Exchange::Polymarket => self.poly_mut()?.cancel_all(exchange, symbol),
            Exchange::Hexmarket => self.hexmarket.cancel_all(exchange, symbol),
            Exchange::Hyperliquid => self.hl_mut()?.cancel_all(exchange, symbol),
            Exchange::Aster => self.aster_mut()?.cancel_all(exchange, symbol),
            Exchange::Lighter => self.lighter_mut()?.cancel_all(exchange, symbol),
            _ => Err(anyhow::anyhow!("Trading not supported on {:?}", exchange)),
        }
    }

    fn batch_submit_orders(
        &mut self,
        market_id: &str,
        orders: &[OrderRequest],
    ) -> Result<Vec<OrderUpdate>> {
        if let Some(first) = orders.first() {
            match first.exchange {
                Exchange::Hexmarket => self.hexmarket.batch_submit_orders(market_id, orders),
                Exchange::Polymarket => self.poly_mut()?.batch_submit_orders(market_id, orders),
                Exchange::Hyperliquid => self.hl_mut()?.batch_submit_orders(market_id, orders),
                Exchange::Aster => self.aster_mut()?.batch_submit_orders(market_id, orders),
                Exchange::Lighter => self.lighter_mut()?.batch_submit_orders(market_id, orders),
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

    fn batch_cancel_orders(
        &mut self,
        exchange: Exchange,
        market_id: &str,
        client_order_ids: &[String],
    ) -> Result<Vec<OrderUpdate>> {
        match exchange {
            Exchange::Hexmarket => {
                self.hexmarket
                    .batch_cancel_orders(exchange, market_id, client_order_ids)
            }
            Exchange::Polymarket => {
                self.poly_mut()?
                    .batch_cancel_orders(exchange, market_id, client_order_ids)
            }
            Exchange::Hyperliquid => {
                self.hl_mut()?
                    .batch_cancel_orders(exchange, market_id, client_order_ids)
            }
            Exchange::Aster => {
                self.aster_mut()?
                    .batch_cancel_orders(exchange, market_id, client_order_ids)
            }
            Exchange::Lighter => {
                self.lighter_mut()?
                    .batch_cancel_orders(exchange, market_id, client_order_ids)
            }
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
            Exchange::Hexmarket => self.hexmarket.batch_update_orders(
                exchange,
                market_id,
                cancel_client_order_ids,
                place_orders,
            ),
            // Polymarket has its own parallel cancel+place via thread::scope
            // (uses DELETE /orders and POST /orders batch endpoints in
            // parallel). Route straight through so we don't fall back to a
            // serial cancel_order → submit_order loop.
            Exchange::Polymarket => self.poly_mut()?.batch_update_orders(
                exchange,
                market_id,
                cancel_client_order_ids,
                place_orders,
            ),
            Exchange::Hyperliquid => self.hl_mut()?.batch_update_orders(
                exchange,
                market_id,
                cancel_client_order_ids,
                place_orders,
            ),
            Exchange::Aster => self.aster_mut()?.batch_update_orders(
                exchange,
                market_id,
                cancel_client_order_ids,
                place_orders,
            ),
            Exchange::Lighter => self.lighter_mut()?.batch_update_orders(
                exchange,
                market_id,
                cancel_client_order_ids,
                place_orders,
            ),
            _ => {
                let mut updates = Vec::new();
                if !cancel_client_order_ids.is_empty() {
                    updates.extend(self.batch_cancel_orders(
                        exchange,
                        market_id,
                        cancel_client_order_ids,
                    )?);
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

#[cfg(test)]
mod market_router_tests {
    use super::*;
    use crossbeam_channel::Receiver;

    struct WatchdogTestStrategy {
        watchdog_tx: Sender<u64>,
    }

    impl Strategy for WatchdogTestStrategy {
        fn name(&self) -> &str {
            "watchdog-test"
        }

        fn instance_id(&self) -> &str {
            "single"
        }

        fn on_watchdog(&mut self, now_ns: u64) -> Vec<Signal> {
            let _ = self.watchdog_tx.try_send(now_ns);
            Vec::new()
        }
    }

    #[derive(Default)]
    struct HistConsumerState {
        load_anchors: Vec<u64>,
        bar_open_times: Vec<u64>,
        loaded_ends: Vec<u64>,
    }

    struct HistTestStrategy {
        id: &'static str,
        request: HistDataRequest,
        state: Arc<std::sync::Mutex<HistConsumerState>>,
    }

    impl Strategy for HistTestStrategy {
        fn name(&self) -> &str {
            "hist-test"
        }
        fn instance_id(&self) -> &str {
            self.id
        }

        fn load_hist_data(&self, ts_event: u64) -> Vec<HistDataRequest> {
            self.state.lock().unwrap().load_anchors.push(ts_event);
            vec![self.request.clone()]
        }

        fn on_hist_bar(&mut self, bar: &BarData) {
            self.state
                .lock()
                .unwrap()
                .bar_open_times
                .push(bar.open_time_ns);
        }

        fn on_hist_data_loaded(&mut self, end_ns: u64) {
            self.state.lock().unwrap().loaded_ends.push(end_ns);
        }
    }

    fn hist_request(symbol: &str, interval: &str, start_ns: u64, end_ns: u64) -> HistDataRequest {
        HistDataRequest {
            exchange: Exchange::Binance,
            symbol: symbol.to_string(),
            interval: interval.to_string(),
            start_date_ns: start_ns,
            end_date_ns: end_ns,
        }
    }

    fn hist_bar(req: &HistDataRequest) -> BarData {
        BarData {
            exchange: req.exchange,
            symbol: req.symbol.clone(),
            interval: req.interval.clone(),
            open_time_ns: req.start_date_ns,
            close_time_ns: req.start_date_ns.saturating_add(1),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: 1.0,
            taker_buy_base: 0.5,
            quote_volume: 100.5,
            is_closed: true,
            exchange_timestamp_ns: req.start_date_ns.saturating_add(1),
            local_timestamp_ns: req.start_date_ns.saturating_add(1),
        }
    }

    #[test]
    fn historical_preload_uses_one_anchor_and_one_load_for_identical_inputs() {
        let s1 = Arc::new(std::sync::Mutex::new(HistConsumerState::default()));
        let s2 = Arc::new(std::sync::Mutex::new(HistConsumerState::default()));
        // The original request ends differ, simulating two sequential instance
        // callbacks. Equal lookback durations become one exact anchored input.
        let mut strategies: Vec<Box<dyn Strategy>> = vec![
            Box::new(HistTestStrategy {
                id: "btc01",
                request: hist_request("BTCUSDT", "1s", 100, 200),
                state: s1.clone(),
            }),
            Box::new(HistTestStrategy {
                id: "btc02",
                request: hist_request("BTCUSDT", "1s", 300, 400),
                state: s2.clone(),
            }),
        ];
        let mut loads = 0usize;
        let stats = preload_hist_bars_with(&mut strategies, 1_000, |req| {
            loads += 1;
            assert_eq!(req.start_date_ns, 900);
            assert_eq!(req.end_date_ns, 1_000);
            Some(vec![hist_bar(req)])
        });

        assert_eq!(loads, 1);
        assert_eq!(
            stats,
            HistPreloadStats {
                requests: 2,
                unique_loads: 1,
                cache_hits: 1,
                failed_loads: 0,
                initialized_strategies: 2,
            },
        );
        for state in [&s1, &s2] {
            let state = state.lock().unwrap();
            assert_eq!(state.load_anchors, vec![1_000]);
            assert_eq!(state.bar_open_times, vec![900]);
            assert_eq!(state.loaded_ends, vec![1_000]);
        }
    }

    #[test]
    fn historical_preload_does_not_share_different_symbols_or_windows() {
        let states: Vec<_> = (0..3)
            .map(|_| Arc::new(std::sync::Mutex::new(HistConsumerState::default())))
            .collect();
        let mut strategies: Vec<Box<dyn Strategy>> = vec![
            Box::new(HistTestStrategy {
                id: "btc",
                request: hist_request("BTCUSDT", "1s", 100, 200),
                state: states[0].clone(),
            }),
            Box::new(HistTestStrategy {
                id: "eth",
                request: hist_request("ETHUSDT", "1s", 100, 200),
                state: states[1].clone(),
            }),
            Box::new(HistTestStrategy {
                id: "btc-longer",
                request: hist_request("BTCUSDT", "1s", 50, 200),
                state: states[2].clone(),
            }),
        ];
        let mut loaded_keys = Vec::new();
        let stats = preload_hist_bars_with(&mut strategies, 1_000, |req| {
            loaded_keys.push(HistBarsKey::from(req));
            Some(vec![hist_bar(req)])
        });

        assert_eq!(stats.requests, 3);
        assert_eq!(stats.unique_loads, 3);
        assert_eq!(stats.cache_hits, 0);
        assert_eq!(loaded_keys.len(), 3);
        assert_ne!(
            loaded_keys[0], loaded_keys[1],
            "different assets must not share"
        );
        assert_ne!(
            loaded_keys[0], loaded_keys[2],
            "different lookbacks must not share"
        );
    }

    #[test]
    fn feed_readiness_records_stage_reason_and_recovery() {
        let states = Arc::new(RwLock::new(HashMap::from([(
            "polymarket".to_string(),
            FeedReadiness::Starting,
        )])));
        set_feed_readiness(
            &states,
            "polymarket",
            FeedReadiness::NotReady {
                stage: "subscribe".into(),
                reason: "gamma 500".into(),
            },
        );
        assert_eq!(
            states.read().unwrap().get("polymarket"),
            Some(&FeedReadiness::NotReady {
                stage: "subscribe".into(),
                reason: "gamma 500".into(),
            }),
        );

        set_feed_readiness(&states, "polymarket", FeedReadiness::Ready);
        assert_eq!(
            states.read().unwrap().get("polymarket"),
            Some(&FeedReadiness::Ready),
        );
    }

    #[test]
    fn polymarket_lifecycle_events_drive_not_ready_then_ready() {
        let disconnected = MarketEvent::Disconnected {
            exchange: Exchange::Polymarket,
            reason: "WS read error: reset".into(),
        };
        assert_eq!(
            polymarket_readiness_transition(&disconnected),
            Some(FeedReadiness::NotReady {
                stage: "data_stream".into(),
                reason: "WS read error: reset".into(),
            }),
        );
        assert_eq!(
            polymarket_readiness_transition(&MarketEvent::Connected {
                exchange: Exchange::Polymarket,
            }),
            Some(FeedReadiness::Ready),
        );
        assert_eq!(
            polymarket_readiness_transition(&MarketEvent::Connected {
                exchange: Exchange::Chainlink,
            }),
            None,
            "Chainlink lifecycle must not mutate Polymarket readiness",
        );
    }

    fn binary_option(slug: &str, tokens: &[&str]) -> Instrument {
        Instrument::BinaryOption(BinaryOption {
            exchange: Exchange::Polymarket,
            id: "id".into(),
            question: "q".into(),
            condition_id: "cond".into(),
            series_slug: slug.into(),
            slug: slug.into(),
            clob_token_ids: tokens.iter().map(|s| s.to_string()).collect(),
            outcomes: vec!["Up".into(), "Down".into()],
            outcome_prices: vec!["0.5".into(), "0.5".into()],
            active: true,
            closed: false,
            volume: 0.0,
            liquidity: 0.0,
            tick_size: 0.001,
            order_min_size: 5.0,
            group_item_title: String::new(),
            event_start_time: String::new(),
            base_fee: 0,
            fee_exponent: 0.0,
            fee_rate: 0.0,
        })
    }

    fn ob(exchange: Exchange, symbol: &str) -> MarketEvent {
        MarketEvent::OrderBook(OrderBookSnapshot {
            exchange,
            symbol: symbol.into(),
            bids: vec![],
            asks: vec![],
            exchange_timestamp_ns: 1,
            local_timestamp_ns: 1,
        })
    }

    fn trade(exchange: Exchange, symbol: &str) -> MarketEvent {
        MarketEvent::Trade(TradeTick {
            exchange,
            symbol: symbol.into(),
            exchange_trade_id: None,
            price: 0.5,
            quantity: 1.0,
            side: Side::Buy,
            exchange_timestamp_ns: 1,
            local_timestamp_ns: 1,
        })
    }

    fn quote(exchange: Exchange, symbol: &str) -> MarketEvent {
        MarketEvent::Quote(QuoteTick {
            exchange,
            symbol: symbol.into(),
            bid_price: 0.4,
            bid_qty: 1.0,
            ask_price: 0.6,
            ask_qty: 1.0,
            exchange_timestamp_ns: 1,
            local_timestamp_ns: 1,
        })
    }

    fn tick_size(symbol: &str) -> MarketEvent {
        MarketEvent::TickSizeChange(TickSizeChange {
            exchange: Exchange::Polymarket,
            symbol: symbol.into(),
            old_tick_size: 0.01,
            new_tick_size: 0.001,
            exchange_timestamp_ns: 1,
            local_timestamp_ns: 1,
        })
    }

    fn spot(symbol: &str) -> MarketEvent {
        MarketEvent::SpotPrice(SpotPrice {
            source: "chainlink".into(),
            symbol: symbol.into(),
            price: 1.0,
            timestamp_ns: 1,
            local_timestamp_ns: 1,
        })
    }

    fn asset_ctx(symbol: &str) -> MarketEvent {
        MarketEvent::AssetCtx(AssetCtxTick {
            exchange: Exchange::Hyperliquid,
            symbol: symbol.into(),
            mark_px: 1.0,
            oracle_px: 1.0,
            mid_px: 1.0,
            funding: 0.0,
            open_interest: 0.0,
            premium: 0.0,
            impact_bid_px: 1.0,
            impact_ask_px: 1.0,
            day_ntl_vlm: 0.0,
            prev_day_px: 1.0,
            local_timestamp_ns: 1,
        })
    }

    fn event_end(event_id: &str, tokens: &[&str]) -> MarketEvent {
        MarketEvent::EventEnd {
            exchange: Exchange::Polymarket,
            symbol: "series:btc-up-or-down-5m".into(),
            event_id: event_id.into(),
            retired_symbols: tokens.iter().map(|token| (*token).to_string()).collect(),
            event_end_ns: 1,
        }
    }

    /// Drain a receiver into a count (non-blocking).
    fn drain(rx: &Receiver<QueuedMarketEvent>) -> usize {
        let mut n = 0;
        while rx.try_recv().is_ok() {
            n += 1;
        }
        n
    }

    #[test]
    fn live_recorder_forwards_polymarket_and_external_sources_once() {
        let (tx, rx) = bounded::<Arc<MarketEvent>>(4);
        forward_recorder_event(Some(&tx), &ob(Exchange::Polymarket, "up"));
        assert!(matches!(
            rx.try_recv(),
            Ok(event) if matches!(event.as_ref(), MarketEvent::OrderBook(OrderBookSnapshot {
                exchange: Exchange::Polymarket,
                ..
            }))
        ));
        assert!(
            rx.try_recv().is_err(),
            "Polymarket event must be enqueued once"
        );

        forward_recorder_event(Some(&tx), &spot("btc/usd"));
        assert!(
            matches!(rx.try_recv(), Ok(event) if matches!(event.as_ref(), MarketEvent::SpotPrice(_)))
        );
        assert!(
            rx.try_recv().is_err(),
            "external event must be enqueued once"
        );

        forward_recorder_event(Some(&tx), &MarketEvent::Exit);
        assert!(matches!(rx.try_recv(), Ok(event) if matches!(event.as_ref(), MarketEvent::Exit)));
        assert!(rx.try_recv().is_err(), "exit event must be enqueued once");
    }

    fn two_instance_map() -> HashMap<SymbolId, RouteMask> {
        // Instance 0 = BTC, instance 1 = ETH.
        let mut m: HashMap<SymbolId, RouteMask> = HashMap::new();
        for s in [
            "btcusdt",
            "btc-usd",
            "btc/usd",
            "series:btc-up-or-down-5m",
            "btc-up-or-down-5m",
        ] {
            *m.entry(SymbolId::of(s)).or_default() |= 1;
        }
        for s in [
            "ethusdt",
            "eth-usd",
            "eth/usd",
            "series:eth-up-or-down-5m",
            "eth-up-or-down-5m",
        ] {
            *m.entry(SymbolId::of(s)).or_default() |= 1 << 1;
        }
        m
    }

    fn test_market_lane(capacity: usize) -> (MarketEventLane, Receiver<QueuedMarketEvent>) {
        let (tx, rx) = bounded(capacity);
        (
            MarketEventLane {
                tx,
                latest: Arc::new(LatestMarketStore::default()),
                barrier_epoch: Arc::new(AtomicU64::new(0)),
            },
            rx,
        )
    }

    #[test]
    fn quote_and_orderbook_lane_keeps_only_latest_payload() {
        let (lane, rx) = test_market_lane(8);
        let first = Arc::new(ob(Exchange::Binance, "BTCUSDT"));
        let second = Arc::new(ob(Exchange::Binance, "BTCUSDT"));
        let key = Some(LatestMarketKeyHandle {
            id: 0,
            generation: 1,
        });
        assert!(enqueue_market_event(&lane, first, key));
        assert!(enqueue_market_event(&lane, Arc::clone(&second), key));
        assert_eq!(rx.len(), 1, "one latest-only marker per venue/symbol/kind");
        let payload = resolve_market_event(rx.recv().unwrap(), &lane.latest).unwrap();
        assert!(Arc::ptr_eq(&payload.event, &second));
        assert_eq!(lane.latest.replacements.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn spot_and_asset_context_are_replaceable_latest_values() {
        let chainlink = latest_market_key(&spot("BTC/USD")).expect("spot latest key");
        let mut pyth = spot("BTC/USD");
        let MarketEvent::SpotPrice(ref mut tick) = pyth else {
            unreachable!()
        };
        tick.source = "pyth".into();
        assert_ne!(
            chainlink,
            latest_market_key(&pyth).expect("source-scoped latest key"),
            "independent spot sources must not overwrite each other",
        );
        assert!(latest_market_key(&asset_ctx("BTC")).is_some());
    }

    #[test]
    fn latest_only_never_reorders_across_a_direct_event() {
        let (lane, rx) = test_market_lane(8);
        let first = Arc::new(ob(Exchange::Binance, "BTCUSDT"));
        let second = Arc::new(ob(Exchange::Binance, "BTCUSDT"));
        let key = Some(LatestMarketKeyHandle {
            id: 0,
            generation: 1,
        });
        assert!(enqueue_market_event(&lane, Arc::clone(&first), key));
        assert!(enqueue_market_event(
            &lane,
            Arc::new(MarketEvent::Connected {
                exchange: Exchange::Binance,
            }),
            None,
        ));
        assert!(enqueue_market_event(&lane, Arc::clone(&second), key));
        assert_eq!(rx.len(), 3);

        let before_barrier = resolve_market_event(rx.recv().unwrap(), &lane.latest).unwrap();
        assert!(Arc::ptr_eq(&before_barrier.event, &first));
        let barrier = resolve_market_event(rx.recv().unwrap(), &lane.latest).unwrap();
        assert!(matches!(
            barrier.event.as_ref(),
            MarketEvent::Connected {
                exchange: Exchange::Binance
            }
        ));
        let after_barrier = resolve_market_event(rx.recv().unwrap(), &lane.latest).unwrap();
        assert!(Arc::ptr_eq(&after_barrier.event, &second));
    }

    #[test]
    fn private_update_priority_yields_after_bounded_burst() {
        assert!(private_update_lane_enabled(
            PRIVATE_UPDATE_BURST_BUDGET - 1,
            true,
        ));
        assert!(!private_update_lane_enabled(
            PRIVATE_UPDATE_BURST_BUDGET,
            true,
        ));
        assert!(private_update_lane_enabled(
            PRIVATE_UPDATE_BURST_BUDGET,
            false,
        ));
    }

    #[test]
    fn live_and_paper_select_instance_workers_even_for_one_strategy() {
        assert!(should_spawn_per_instance_strategy_workers(false, 1));
        assert!(should_spawn_per_instance_strategy_workers(false, 5));
        assert!(!should_spawn_per_instance_strategy_workers(false, 0));
        assert!(!should_spawn_per_instance_strategy_workers(true, 1));
        assert!(!should_spawn_per_instance_strategy_workers(true, 5));
    }

    fn shutdown_test_engine() -> Engine {
        let config: Config = toml::from_str("[general]\nmode = 'live'\n").unwrap();
        Engine::new(config, StrategyRegistry::new())
    }

    fn assert_thread_exits(handle: &thread::JoinHandle<()>, label: &str) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !handle.is_finished() && std::time::Instant::now() < deadline {
            thread::yield_now();
        }
        assert!(handle.is_finished(), "{label} did not exit after shutdown");
    }

    #[test]
    fn strategy_panic_and_signal_disconnect_cannot_strand_execution() {
        let engine = shutdown_test_engine();
        let shutdown = ShutdownToken::new();
        let (signal_tx, signal_rx) = bounded(1);
        let (update_tx, _update_rx) = bounded(1);
        let (shutdown_done_tx, shutdown_done_rx) = bounded(1);
        let execution = engine.spawn_execution_thread_with_poly_shutdown(
            signal_rx,
            update_tx,
            HashMap::new(),
            HashMap::new(),
            shutdown_done_tx,
            shutdown.clone(),
        );
        let strategy = thread::spawn(|| panic!("injected strategy panic"));
        assert_thread_exits(&strategy, "injected strategy");
        assert_eq!(
            first_finished_critical_thread(&[("strategy", &strategy)]),
            Some("strategy"),
        );

        // This is the supervisor action taken by run_live. The executor sees
        // it directly even though no strategy can forward BeginShutdown.
        shutdown.request();
        assert!(
            shutdown_done_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .is_ok(),
            "execution did not finish its coordinated barrier",
        );
        drop(signal_tx);
        assert_thread_exits(&execution, "execution");
        let _ = strategy.join();
        execution.join().unwrap();
        shutdown.finish();
    }

    #[test]
    fn strategy_panic_and_channel_disconnect_stop_all_workers() {
        crate::async_rt::init().expect("test async runtime and admission pools start");
        let engine = shutdown_test_engine();
        let shutdown = ShutdownToken::new();
        let latency_dump = crate::latency::spawn_periodic_dump(
            std::time::Duration::from_secs(3_600),
            shutdown.clone(),
        )
        .expect("test latency worker starts");
        let memory_telemetry =
            spawn_memory_telemetry(shutdown.clone()).expect("test memory worker starts");
        let trade = PolymarketTrade::new_with_pool_for_startup_query_repair_and_shutdown(
            "api-key",
            "c2VjcmV0",
            "passphrase",
            "0x0000000000000000000000000000000000000000000000000000000000000001",
            false,
            10,
            crate::exchange::polymarket::signer::SignatureType::Eoa,
            crate::exchange::polymarket::trade::ClobVersion::V2,
            "",
            "",
            true,
            "shutdown-test",
            "",
            crate::exchange::polymarket::trade::GapReplayConfig::default(),
            None,
            shutdown.clone(),
        )
        .unwrap();
        let shared = trade.shared_state();
        let mut poly_states = HashMap::new();
        poly_states.insert("shutdown-test".to_string(), shared.clone());
        let (signal_tx, signal_rx) = bounded(1);
        let (update_tx, _update_rx) = bounded(8);
        let (shutdown_done_tx, _shutdown_done_rx) = bounded(1);
        let execution = engine.spawn_execution_thread_with_poly_shutdown(
            signal_rx,
            update_tx,
            poly_states,
            HashMap::new(),
            shutdown_done_tx,
            shutdown.clone(),
        );

        // The strategy owns the last ingress producer. Its panic disconnects
        // that lane exactly as the live router would; the supervisor then
        // requests the shared shutdown phase.
        let strategy = thread::spawn(move || {
            drop(signal_tx);
            panic!("injected strategy panic with live account workers");
        });
        assert_thread_exits(&strategy, "injected strategy");
        assert_eq!(
            first_finished_critical_thread(&[("strategy", &strategy)]),
            Some("strategy"),
        );
        // The disconnected ingress tears down and joins the executor's
        // admission/dispatch tree without entering the live remote cancel
        // barrier (which is intentionally unbounded until the venue proves
        // clean). The common token then stops the remaining run workers.
        assert_thread_exits(&execution, "execution/admission worker tree");
        let _ = strategy.join();
        execution.join().unwrap();
        shutdown.request();
        shutdown.finish();
        assert_eq!(shared.join_background_workers(), 2);
        assert_thread_exits(&latency_dump, "latency worker");
        latency_dump.join().unwrap();
        assert_thread_exits(&memory_telemetry, "memory worker");
        memory_telemetry.join().unwrap();
    }

    #[test]
    fn single_instance_worker_runs_watchdog_without_market_events() {
        let (market_tx, market_rx) = bounded(1);
        let (_update_tx, update_rx) = bounded(1);
        let (routed_signal_tx, _routed_signal_rx) = bounded(1);
        let (shutdown_ack_tx, _shutdown_ack_rx) = bounded(1);
        let (watchdog_tx, watchdog_rx) = bounded(1);
        let heartbeat = Arc::new(AtomicU64::new(0));
        let quarantined = Arc::new(AtomicBool::new(false));
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let clock_origin = Arc::new(std::time::Instant::now());

        let worker = thread::Builder::new()
            .name("strategy-single-test".into())
            .spawn(move || {
                Engine::run_strategy_worker(
                    Box::new(WatchdogTestStrategy { watchdog_tx }),
                    market_rx,
                    Arc::new(LatestMarketStore::default()),
                    update_rx,
                    SignalSender::system(routed_signal_tx).with_owner(0),
                    Vec::new(),
                    "single",
                    0,
                    heartbeat,
                    quarantined,
                    shutdown_requested,
                    shutdown_ack_tx,
                    clock_origin,
                );
            })
            .unwrap();

        let watchdog_result = watchdog_rx.recv_timeout(std::time::Duration::from_secs(2));
        market_tx
            .send(QueuedMarketEvent::Direct(QueuedMarketPayload {
                event: Arc::new(MarketEvent::Exit),
                enqueued_ns: crate::types::now_ns(),
            }))
            .unwrap();
        worker.join().unwrap();

        assert!(
            watchdog_result.is_ok(),
            "single-instance worker did not run its watchdog while market data was silent"
        );
    }

    #[test]
    #[ignore = "manual dispatch-latency benchmark; run with --release --ignored"]
    fn single_instance_router_dispatch_latency_benchmark() {
        const EVENTS: usize = 100_000;

        fn percentiles(mut samples_ns: Vec<u64>) -> (u64, u64, u64, u64) {
            samples_ns.sort_unstable();
            let at = |numerator: usize, denominator: usize| {
                samples_ns[(samples_ns.len() - 1) * numerator / denominator]
            };
            (at(1, 2), at(99, 100), at(999, 1_000), at(1, 1))
        }

        // Production prewarms the raw monotonic clock on the router thread;
        // keep calibration outside this measured boundary as well.
        let _ = market_queue_monotonic_ns();
        let event = Arc::new(ob(Exchange::Binance, "BTCUSDT"));

        // Legacy comparison boundary: direct dispatch enqueue through dequeue.
        let (direct_tx, direct_rx) = bounded::<Arc<MarketEvent>>(CHANNEL_CAPACITY);
        let mut direct_samples = Vec::with_capacity(EVENTS);
        let mut direct_peak_depth = 0usize;
        let mut direct_overflow = 0u64;
        for _ in 0..EVENTS {
            let started = std::time::Instant::now();
            if direct_tx.try_send(Arc::clone(&event)).is_err() {
                direct_overflow += 1;
                continue;
            }
            direct_peak_depth = direct_peak_depth.max(direct_tx.len());
            std::hint::black_box(direct_rx.try_recv().unwrap());
            direct_samples.push(started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        }

        // New boundary: single-instance router entry through lane resolution.
        // The same-thread dequeue deliberately excludes OS scheduling noise.
        let (lane, routed_rx) = test_market_lane(CHANNEL_CAPACITY);
        let sym_to_instances = HashMap::from([(SymbolId::of("BTCUSDT"), 1)]);
        let mut token_to_instances = HashMap::new();
        let mut latest_key_ids = LatestMarketKeyRegistry::default();
        let mut routed_samples = Vec::with_capacity(EVENTS);
        let mut routed_peak_depth = 0usize;
        let mut routed_overflow = 0u64;
        for _ in 0..EVENTS {
            let started = std::time::Instant::now();
            routed_overflow += Engine::route_market_event(
                Arc::clone(&event),
                &sym_to_instances,
                &mut token_to_instances,
                &mut latest_key_ids,
                std::slice::from_ref(&lane),
            )
            .count_ones() as u64;
            routed_peak_depth = routed_peak_depth.max(routed_rx.len());
            let queued = routed_rx.try_recv().unwrap();
            std::hint::black_box(resolve_market_event(queued, &lane.latest).unwrap());
            routed_samples.push(started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        }

        let direct = percentiles(direct_samples);
        let routed = percentiles(routed_samples);
        println!(
            "single_instance_dispatch events={EVENTS} boundary=dispatch_entry_to_lane_dequeue unit=ns legacy_direct_p50={} legacy_direct_p99={} legacy_direct_p999={} legacy_direct_max={} legacy_direct_peak_depth={} legacy_direct_overflow={} routed_p50={} routed_p99={} routed_p999={} routed_max={} routed_peak_depth={} routed_overflow={}",
            direct.0,
            direct.1,
            direct.2,
            direct.3,
            direct_peak_depth,
            direct_overflow,
            routed.0,
            routed.1,
            routed.2,
            routed.3,
            routed_peak_depth,
            routed_overflow,
        );
    }

    #[test]
    #[ignore = "manual strategy-signal lane benchmark; run with --release --ignored"]
    fn instance_signal_arbiter_latency_benchmark() {
        const EVENTS: usize = 100_000;

        fn percentiles(mut samples_ns: Vec<u64>) -> (u64, u64, u64, u64) {
            samples_ns.sort_unstable();
            let at = |numerator: usize, denominator: usize| {
                samples_ns[(samples_ns.len() - 1) * numerator / denominator]
            };
            (at(1, 2), at(99, 100), at(999, 1_000), at(1, 1))
        }

        let (root_tx, root_rx) = bounded(8);
        let (owner_tx, owner_rx) = bounded(INSTANCE_SIGNAL_LANE_CAPACITY);
        let depth_tx = owner_tx.clone();
        let (control_tx, control_rx) = bounded(2);
        let arbiter = spawn_strategy_signal_arbiter(root_tx.clone(), vec![owner_rx], control_rx)
            .expect("arbiter");
        let system = SignalSender::system_with_owner_lanes(root_tx, vec![owner_tx], control_tx);
        let owner = system.with_owner(0);
        let template = Signal::CancelAll {
            exchange: Exchange::Polymarket,
            symbol: String::new(),
            instance_id: String::new(),
            timestamp_ns: 0,
        };
        let mut samples = Vec::with_capacity(EVENTS);
        let mut peak_depth = 0usize;
        for _ in 0..EVENTS {
            let signal = template.clone();
            let started = std::time::Instant::now();
            owner.send(signal).unwrap();
            peak_depth = peak_depth.max(depth_tx.len());
            std::hint::black_box(root_rx.recv().unwrap());
            samples.push(started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        }
        system.send(Signal::Exit).unwrap();
        std::hint::black_box(root_rx.recv().unwrap());
        arbiter.join().unwrap();

        let measured = percentiles(samples);
        println!(
            "instance_signal_arbiter events={EVENTS} boundary=owner_lane_send_to_root_dequeue unit=ns p50={} p99={} p999={} max={} owner_peak_depth={} owner_overflow=0 root_peak_depth=1 root_overflow=0",
            measured.0, measured.1, measured.2, measured.3, peak_depth,
        );
    }

    #[test]
    fn spot_and_binance_ob_route_to_owning_instance_only() {
        let sym = two_instance_map();
        let mut tok: HashMap<SymbolId, RouteMask> = HashMap::new();
        let mut latest = LatestMarketKeyRegistry::default();
        let (lane0, rx0) = test_market_lane(64);
        let (lane1, rx1) = test_market_lane(64);
        let txs = [lane0, lane1];

        // BTC Binance OB → only instance 0 (fixes cross-asset cadence).
        Engine::route_market_event(
            Arc::new(ob(Exchange::Binance, "BTCUSDT")),
            &sym,
            &mut tok,
            &mut latest,
            &txs,
        );
        // BTC chainlink spot (lowercase "btc/usd") → only instance 0.
        Engine::route_market_event(Arc::new(spot("btc/usd")), &sym, &mut tok, &mut latest, &txs);
        // ETH Coinbase OB → only instance 1.
        Engine::route_market_event(
            Arc::new(ob(Exchange::Coinbase, "ETH-USD")),
            &sym,
            &mut tok,
            &mut latest,
            &txs,
        );

        assert_eq!(drain(&rx0), 2, "instance 0 should get BTC OB + BTC spot");
        assert_eq!(drain(&rx1), 1, "instance 1 should get ETH OB only");
    }

    #[test]
    fn full_latest_market_queue_replaces_that_instance_without_blocking_router() {
        let sym = two_instance_map();
        let mut tok: HashMap<SymbolId, RouteMask> = HashMap::new();
        let mut latest = LatestMarketKeyRegistry::default();
        let (lane0, rx0) = test_market_lane(1);
        let (lane1, rx1) = test_market_lane(1);
        let txs = [lane0, lane1];
        let first = Arc::new(spot("btc/usd"));

        assert_eq!(
            Engine::route_market_event(
                first,
                &sym,
                &mut tok,
                &mut latest,
                &txs,
            ),
            0
        );
        // BTC queue is now full. Routing ETH must remain independent.
        assert_eq!(
            Engine::route_market_event(
                Arc::new(spot("eth/usd")),
                &sym,
                &mut tok,
                &mut latest,
                &txs,
            ),
            0
        );
        assert_eq!(drain(&rx1), 1);
        let second = Arc::new(spot("btc/usd"));
        assert_eq!(
            Engine::route_market_event(
                Arc::clone(&second),
                &sym,
                &mut tok,
                &mut latest,
                &txs,
            ),
            0,
            "a full latest-value lane replaces its pending snapshot"
        );
        assert_eq!(rx0.len(), 1, "replacement must reuse the queued marker");
        let payload = resolve_market_event(rx0.recv().unwrap(), &txs[0].latest).unwrap();
        assert!(Arc::ptr_eq(&payload.event, &second));
        assert_eq!(txs[0].latest.replacements.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn polymarket_token_learned_from_instrument_then_routed() {
        let sym = two_instance_map();
        let mut tok: HashMap<SymbolId, RouteMask> = HashMap::new();
        let mut latest = LatestMarketKeyRegistry::default();
        let (lane0, rx0) = test_market_lane(64);
        let (lane1, rx1) = test_market_lane(64);
        let txs = [lane0, lane1];

        // Instrument for BTC series carrying token "TOKxyz" → instance 0,
        // and the router learns TOKxyz → [0].
        let mut rotating = binary_option("btc-up-or-down-5m", &["TOKxyz"]);
        let Instrument::BinaryOption(ref mut option) = rotating else {
            unreachable!()
        };
        option.slug = "btc-updown-5m-1782840600".into();
        Engine::route_market_event(
            Arc::new(MarketEvent::Instrument(rotating)),
            &sym,
            &mut tok,
            &mut latest,
            &txs,
        );
        assert_eq!(drain(&rx0), 1, "instrument delivered to owner");
        assert_eq!(drain(&rx1), 0);

        // A Polymarket OB on that dynamic token → only instance 0.
        Engine::route_market_event(
            Arc::new(ob(Exchange::Polymarket, "TOKxyz")),
            &sym,
            &mut tok,
            &mut latest,
            &txs,
        );
        assert_eq!(drain(&rx0), 1, "poly OB routed to learned owner");
        assert_eq!(drain(&rx1), 0, "ETH instance must not see BTC poly OB");

        Engine::route_market_event(
            Arc::new(MarketEvent::MarketDataHealth(MarketDataHealth {
                exchange: Exchange::Polymarket,
                market_id: "btc-condition".into(),
                symbol: "TOKxyz".into(),
                state: MarketDataHealthState::Repairing,
                passive_ready: true,
                taker_ready: false,
                reason: "injected mismatch".into(),
                local_timestamp_ns: 1,
            })),
            &sym,
            &mut tok,
            &mut latest,
            &txs,
        );
        assert_eq!(drain(&rx0), 1, "condition health routed to learned owner");
        assert_eq!(drain(&rx1), 0, "other instances stay isolated");
    }

    #[test]
    fn retired_latest_slot_id_does_not_alias_a_queued_old_marker() {
        let sym = two_instance_map();
        let mut tok: HashMap<SymbolId, RouteMask> = HashMap::new();
        let mut latest = LatestMarketKeyRegistry::default();
        let (lane, rx) = test_market_lane(64);
        let lanes = [lane];

        let route_instrument = |token: &str,
                                tok: &mut HashMap<SymbolId, RouteMask>,
                                latest: &mut LatestMarketKeyRegistry| {
            Engine::route_market_event(
                Arc::new(MarketEvent::Instrument(binary_option(
                    "btc-up-or-down-5m",
                    &[token],
                ))),
                &sym,
                tok,
                latest,
                &lanes,
            )
        };
        let route_book = |token: &str,
                          tok: &mut HashMap<SymbolId, RouteMask>,
                          latest: &mut LatestMarketKeyRegistry| {
            Engine::route_market_event(
                Arc::new(ob(Exchange::Polymarket, token)),
                &sym,
                tok,
                latest,
                &lanes,
            )
        };
        let handle_for = |token: &str, latest: &LatestMarketKeyRegistry| {
            latest.key_to_handle[&LatestMarketKey {
                kind: 0,
                exchange: Exchange::Polymarket,
                symbol: SymbolId::of(token),
            }]
        };

        // Keep every marker queued. Two lifecycle barriers wrap the four
        // epoch slots, so generation 3 deliberately reuses generation 1's
        // exact physical slot while generation 1's marker is still present.
        assert_eq!(route_instrument("OLD", &mut tok, &mut latest), 0);
        assert_eq!(route_book("OLD", &mut tok, &mut latest), 0);
        let first = handle_for("OLD", &latest);
        assert_eq!(
            Engine::route_market_event(
                Arc::new(event_end("event-1", &["OLD"])),
                &sym,
                &mut tok,
                &mut latest,
                &lanes,
            ),
            0,
        );
        assert_eq!(latest.len(), 0);
        assert!(!tok.contains_key(&SymbolId::of("OLD")));

        assert_eq!(route_instrument("MIDDLE", &mut tok, &mut latest), 0);
        assert_eq!(route_book("MIDDLE", &mut tok, &mut latest), 0);
        let second = handle_for("MIDDLE", &latest);
        assert_eq!(
            Engine::route_market_event(
                Arc::new(event_end("event-2", &["MIDDLE"])),
                &sym,
                &mut tok,
                &mut latest,
                &lanes,
            ),
            0,
        );

        assert_eq!(route_instrument("NEW", &mut tok, &mut latest), 0);
        assert_eq!(route_book("NEW", &mut tok, &mut latest), 0);
        let third = handle_for("NEW", &latest);
        assert_eq!(first.id, second.id);
        assert_eq!(second.id, third.id);
        assert!(first.generation < second.generation);
        assert!(second.generation < third.generation);

        let mut delivered_books = Vec::new();
        let mut stale_markers = 0;
        while let Ok(queued) = rx.try_recv() {
            match resolve_market_event(queued, &lanes[0].latest) {
                Some(payload) => {
                    if let MarketEvent::OrderBook(book) = payload.event.as_ref() {
                        delivered_books.push(book.symbol.clone());
                    }
                }
                None => stale_markers += 1,
            }
        }
        assert_eq!(stale_markers, 2);
        assert_eq!(delivered_books, vec!["NEW"]);
    }

    #[test]
    fn dynamic_polymarket_latest_ids_recycle_beyond_fixed_capacity() {
        let sym = two_instance_map();
        let mut tok: HashMap<SymbolId, RouteMask> = HashMap::new();
        let mut latest = LatestMarketKeyRegistry::default();
        let (lane, rx) = test_market_lane(8);
        let lanes = [lane];

        for cycle in 0..MAX_LATEST_MARKET_KEYS * 3 {
            let token = format!("TOKEN-{cycle}");
            Engine::route_market_event(
                Arc::new(MarketEvent::Instrument(binary_option(
                    "btc-up-or-down-5m",
                    &[token.as_str()],
                ))),
                &sym,
                &mut tok,
                &mut latest,
                &lanes,
            );
            assert!(resolve_market_event(rx.try_recv().unwrap(), &lanes[0].latest).is_some());

            for event in [
                ob(Exchange::Polymarket, &token),
                quote(Exchange::Polymarket, &token),
            ] {
                Engine::route_market_event(
                    Arc::new(event),
                    &sym,
                    &mut tok,
                    &mut latest,
                    &lanes,
                );
                assert!(
                    resolve_market_event(rx.try_recv().unwrap(), &lanes[0].latest).is_some()
                );
            }
            assert_eq!(latest.len(), 2);

            Engine::route_market_event(
                Arc::new(event_end(&format!("event-{cycle}"), &[token.as_str()])),
                &sym,
                &mut tok,
                &mut latest,
                &lanes,
            );
            assert_eq!(latest.len(), 0);
            assert_eq!(latest.free_ids.len(), MAX_LATEST_MARKET_KEYS);
            assert!(!tok.contains_key(&SymbolId::of(&token)));
        }

        assert_eq!(latest.capacity_fallbacks, 0);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn exhausted_latest_registry_falls_back_without_aliasing_active_ids() {
        let mut latest = LatestMarketKeyRegistry::default();
        for symbol in 0..MAX_LATEST_MARKET_KEYS {
            assert!(latest
                .get_or_register(LatestMarketKey {
                    kind: 0,
                    exchange: Exchange::Binance,
                    symbol: SymbolId(symbol as u64, 0),
                })
                .is_some());
        }
        assert!(latest
            .get_or_register(LatestMarketKey {
                kind: 0,
                exchange: Exchange::Binance,
                symbol: SymbolId(u64::MAX, 0),
            })
            .is_none());
        assert_eq!(latest.len(), MAX_LATEST_MARKET_KEYS);
        assert_eq!(latest.capacity_fallbacks, 1);
    }

    #[test]
    fn unknown_polymarket_series_is_never_broadcast() {
        let sym = two_instance_map();
        let mut tok: HashMap<SymbolId, RouteMask> = HashMap::new();
        let mut latest = LatestMarketKeyRegistry::default();
        let (lane0, rx0) = test_market_lane(64);
        let (lane1, rx1) = test_market_lane(64);
        let txs = [lane0, lane1];
        Engine::route_market_event(
            Arc::new(MarketEvent::Instrument(binary_option(
                "sol-up-or-down-5m",
                &["SOL-TOKEN"],
            ))),
            &sym,
            &mut tok,
            &mut latest,
            &txs,
        );
        assert_eq!(drain(&rx0), 0);
        assert_eq!(drain(&rx1), 0);
        assert!(!tok.contains_key(&SymbolId::of("sol-token")));
    }

    #[test]
    fn unknown_polymarket_market_data_is_never_broadcast() {
        let sym = two_instance_map();
        let mut tok: HashMap<SymbolId, RouteMask> = HashMap::new();
        let mut latest = LatestMarketKeyRegistry::default();
        let (lane0, rx0) = test_market_lane(64);
        let (lane1, rx1) = test_market_lane(64);
        let txs = [lane0, lane1];

        Engine::route_market_event(
            Arc::new(ob(Exchange::Polymarket, "UNMAPPED-TOKEN")),
            &sym,
            &mut tok,
            &mut latest,
            &txs,
        );
        Engine::route_market_event(
            Arc::new(trade(Exchange::Polymarket, "UNMAPPED-TOKEN")),
            &sym,
            &mut tok,
            &mut latest,
            &txs,
        );
        Engine::route_market_event(
            Arc::new(quote(Exchange::Polymarket, "UNMAPPED-TOKEN")),
            &sym,
            &mut tok,
            &mut latest,
            &txs,
        );
        Engine::route_market_event(
            Arc::new(tick_size("UNMAPPED-TOKEN")),
            &sym,
            &mut tok,
            &mut latest,
            &txs,
        );
        Engine::route_market_event(
            Arc::new(MarketEvent::MarketDataHealth(MarketDataHealth {
                exchange: Exchange::Polymarket,
                market_id: "unknown-condition".into(),
                symbol: "UNMAPPED-TOKEN".into(),
                state: MarketDataHealthState::Degraded,
                passive_ready: false,
                taker_ready: false,
                reason: "test".into(),
                local_timestamp_ns: 1,
            })),
            &sym,
            &mut tok,
            &mut latest,
            &txs,
        );
        assert_eq!(drain(&rx0), 0);
        assert_eq!(drain(&rx1), 0);
    }

    #[test]
    fn lifecycle_broadcasts_but_unknown_spot_symbols_are_dropped() {
        let sym = two_instance_map();
        let mut tok: HashMap<SymbolId, RouteMask> = HashMap::new();
        let mut latest = LatestMarketKeyRegistry::default();
        let (lane0, rx0) = test_market_lane(64);
        let (lane1, rx1) = test_market_lane(64);
        let txs = [lane0, lane1];

        // Connected → all instances.
        Engine::route_market_event(
            Arc::new(MarketEvent::Connected {
                exchange: Exchange::Binance,
            }),
            &sym,
            &mut tok,
            &mut latest,
            &txs,
        );
        // Unknown market data is fail-closed and never broadcast.
        Engine::route_market_event(
            Arc::new(spot("dogeusdt")),
            &sym,
            &mut tok,
            &mut latest,
            &txs,
        );

        assert_eq!(drain(&rx0), 1);
        assert_eq!(drain(&rx1), 1);
    }

    #[test]
    fn full_owner_signal_lane_quarantines_without_blocking_and_uses_control_lane() {
        let (root_tx, _root_rx) = bounded::<RoutedSignal>(1);
        let (owner_tx, owner_rx) = bounded::<RoutedSignal>(1);
        let (control_tx, control_rx) = bounded::<RoutedSignal>(1);
        let system = SignalSender::system_with_owner_lanes(root_tx, vec![owner_tx], control_tx);
        let owner = system.with_owner(0);
        let quarantined = AtomicBool::new(false);

        assert!(owner
            .try_send(Signal::NewOrder(order_req("instance-0-1", "instance-0")))
            .is_ok());
        assert!(!emit_strategy_signal(
            Signal::NewOrder(order_req("instance-0-2", "instance-0")),
            &owner,
            &quarantined,
            "instance-0",
        ));
        assert!(quarantined.load(Ordering::Acquire));
        assert_eq!(owner_rx.len(), 1, "already-admitted work remains ordered");
        let emergency = control_rx.try_recv().unwrap();
        assert_eq!(emergency.owner, 0);
        assert!(matches!(
            emergency.signal,
            Signal::PolymarketCancelAllOrders { market: None, ref instance_id, .. }
                if instance_id == "instance-0"
        ));
    }

    fn order_req(coid: &str, instance_id: &str) -> OrderRequest {
        OrderRequest {
            client_order_id: coid.into(),
            exchange: Exchange::Polymarket,
            symbol: "TOK".into(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(0.5),
            quantity: 10.0,
            quote_trigger_exchange_timestamp_ns: 0,
            quote_trigger_local_timestamp_ns: 1,
            quote_event_id: "event".into(),
            quote_trigger_source: QuoteTriggerSource::StrategyCallback,
            timestamp_ns: 1,
            instance_id: instance_id.into(),
            fee_rate_bps: 0,
            post_only: true,
            reduce_only: false,
            outcome_label: "Up".into(),
        }
    }

    #[test]
    fn quote_trigger_stamps_exact_orderbook_clock_on_every_place() {
        let mut signals = vec![
            Signal::NewOrder(order_req("single", "btc")),
            Signal::BatchNewOrders {
                exchange: Exchange::Polymarket,
                market_id: "market".into(),
                orders: vec![order_req("batch", "btc")],
                instance_id: "btc".into(),
            },
        ];
        let trigger = OrderBookSnapshot {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".into(),
            bids: vec![PriceLevel {
                price: 100.0,
                quantity: 1.0,
            }],
            asks: vec![PriceLevel {
                price: 101.0,
                quantity: 1.0,
            }],
            exchange_timestamp_ns: 11,
            local_timestamp_ns: 22,
        };

        stamp_quote_trigger(&mut signals, &trigger, false);

        let orders: Vec<&OrderRequest> = signals
            .iter()
            .flat_map(|signal| match signal {
                Signal::NewOrder(order) => vec![order],
                Signal::BatchNewOrders { orders, .. } => orders.iter().collect(),
                _ => Vec::new(),
            })
            .collect();
        assert_eq!(orders.len(), 2);
        for order in orders {
            assert_eq!(order.quote_trigger_exchange_timestamp_ns, 11);
            assert_eq!(order.quote_trigger_local_timestamp_ns, 22);
            assert_eq!(
                order.quote_trigger_source,
                QuoteTriggerSource::OrderBook(Exchange::Binance),
            );
        }
    }

    #[test]
    fn signal_sender_tags_numeric_owner_without_registry() {
        let (raw_tx, rx) = bounded(1);
        let sender = SignalSender::system(raw_tx).with_owner(7);
        sender
            .send(Signal::NewOrder(order_req("c1", "btc")))
            .unwrap();
        let routed = rx.recv().unwrap();
        assert_eq!(routed.owner, 7);
        assert!(matches!(routed.signal, Signal::NewOrder(_)));
    }

    #[test]
    fn instance_signal_lane_preserves_owner_order_before_shutdown_barrier() {
        let (root_tx, root_rx) = bounded(8);
        let (owner_tx, owner_rx) = bounded(8);
        let (control_tx, control_rx) = bounded(2);
        let arbiter = spawn_strategy_signal_arbiter(root_tx.clone(), vec![owner_rx], control_rx)
            .expect("arbiter");
        let system = SignalSender::system_with_owner_lanes(root_tx, vec![owner_tx], control_tx);
        let owner = system.with_owner(0);

        owner
            .send(Signal::NewOrder(order_req("c1", "btc")))
            .unwrap();
        owner
            .send(Signal::NewOrder(order_req("c2", "btc")))
            .unwrap();
        system.send(Signal::BeginShutdown).unwrap();
        system.send(Signal::Exit).unwrap();

        let routed = (0..4).map(|_| root_rx.recv().unwrap()).collect::<Vec<_>>();
        assert!(
            matches!(&routed[0].signal, Signal::NewOrder(order) if order.client_order_id == "c1")
        );
        assert!(
            matches!(&routed[1].signal, Signal::NewOrder(order) if order.client_order_id == "c2")
        );
        assert!(matches!(routed[2].signal, Signal::BeginShutdown));
        assert!(matches!(routed[3].signal, Signal::Exit));
        arbiter.join().unwrap();
    }

    #[test]
    fn owner_from_coid_recovers_instance_from_prefix() {
        let mut m: HashMap<String, usize> = HashMap::new();
        m.insert("btc01".into(), 0);
        m.insert("btc02".into(), 1);
        m.insert("zhu-03".into(), 2); // instance_id with an internal dash
                                      // Prefixed coid → owning instance index.
        assert_eq!(owner_from_coid("btc01-1782840607342", &m), Some(0));
        assert_eq!(owner_from_coid("btc02-9", &m), Some(1));
        // rsplit_once('-') keeps the dash-bearing instance_id intact.
        assert_eq!(owner_from_coid("zhu-03-42", &m), Some(2));
        // Legacy bare-numeric coid has no multi-instance owner.
        assert_eq!(owner_from_coid("1782840607342", &m), None);
        // Unknown instance is unowned.
        assert_eq!(owner_from_coid("eth09-7", &m), None);
    }

    #[test]
    fn reconcile_pool_saturation_returns_routeable_deduplicated_feedback() {
        let places = vec![
            (
                "zhu-03-place".to_string(),
                "UP".to_string(),
                Side::Buy,
                0.41,
                None,
            ),
            (
                "zhu-03-place".to_string(),
                "UP".to_string(),
                Side::Buy,
                0.41,
                None,
            ),
        ];
        let cancels = vec![
            ("zhu-03-cancel".to_string(), "oid-1".to_string()),
            ("zhu-03-cancel".to_string(), "oid-1".to_string()),
        ];
        let trade_ids = vec!["trade-1".to_string(), "trade-1".to_string()];

        let updates = reconcile_deferred_updates("zhu-03", &places, &cancels, &trade_ids);
        assert_eq!(updates.len(), 3);
        assert!(updates.iter().all(|update| {
            update.status == OrderStatus::ExecutorRejected
                && update.error.as_deref() == Some(ORPHAN_RECONCILE_DEFERRED)
        }));

        let mut owners = HashMap::new();
        owners.insert("zhu-03".to_string(), 7usize);
        assert!(updates
            .iter()
            .all(|update| { owner_from_coid(&update.client_order_id, &owners) == Some(7) }));
        assert_eq!(
            updates
                .iter()
                .filter_map(|update| update.trade_id.as_deref())
                .collect::<Vec<_>>(),
            vec!["trade-1"]
        );
    }

    #[test]
    fn executor_publish_restamps_reconcile_result_at_actual_send_boundary() {
        let mut update = reconcile_deferred_updates(
            "zhu-03",
            &[(
                "zhu-03-place".to_string(),
                "UP".to_string(),
                Side::Buy,
                0.41,
                None,
            )],
            &[],
            &[],
        )
        .pop()
        .unwrap();
        update.timestamp_ns = 1;
        let (raw_tx, rx) = bounded(1);
        let tx = ExecutorUpdateSender {
            owner: 7,
            tx: raw_tx,
        };
        let before = now_ns();
        send_executor_update(&tx, update).unwrap();
        let published = rx.recv().unwrap();
        let after = now_ns();
        assert_eq!(published.owner, 7);
        assert!(published.update.timestamp_ns >= before);
        assert!(published.update.timestamp_ns <= after);
    }

    #[test]
    fn saturated_replace_sheds_place_but_retains_cancel_lane_work() {
        let (cancel_tx, cancel_rx) = bounded(8);
        let (raw_update_tx, update_rx) = bounded(8);
        let update_tx = ExecutorUpdateSender {
            owner: 3,
            tx: raw_update_tx,
        };
        shed_saturated_place_signal(
            Signal::ReplaceOrder {
                exchange: Exchange::Polymarket,
                market_id: "market".into(),
                cancel_client_order_ids: vec!["old-coid".into()],
                place_orders: vec![order_req("new-coid", "zhu-03")],
                timestamp_ns: 42,
                instance_id: "zhu-03".into(),
            },
            150,
            &cancel_tx,
            &update_tx,
        );

        let (cancel, stale_ms, _) = cancel_rx.try_recv().unwrap();
        assert_eq!(stale_ms, 150);
        assert!(matches!(
            cancel,
            Signal::BatchCancelOrders {
                client_order_ids,
                instance_id,
                ..
            } if client_order_ids == vec!["old-coid"] && instance_id == "zhu-03"
        ));
        let rejected = update_rx.try_recv().unwrap();
        assert_eq!(rejected.owner, 3);
        assert_eq!(rejected.update.client_order_id, "new-coid");
        assert_eq!(rejected.update.status, OrderStatus::ExecutorRejected);
    }

    #[test]
    fn worker_quarantine_is_idempotent_and_enqueues_instance_cancel() {
        let (raw_tx, signal_rx) = bounded::<RoutedSignal>(4);
        let signal_tx = SignalSender::system(raw_tx);
        let flags = vec![Arc::new(AtomicBool::new(false))];
        let instances = vec!["btc01".to_string()];
        assert!(quarantine_strategy_worker(
            0,
            "market queue overflow (event loss)",
            &instances,
            &flags,
            &signal_tx,
        ));
        assert!(flags[0].load(Ordering::Acquire));
        let routed = signal_rx.try_recv().unwrap();
        assert_eq!(routed.owner, 0);
        match routed.signal {
            Signal::PolymarketCancelAllOrders {
                instance_id,
                market,
                reason,
                ..
            } => {
                assert_eq!(instance_id, "btc01");
                assert!(market.is_none());
                assert!(reason.contains("market queue overflow"));
            }
            other => panic!("unexpected supervisor signal: {other:?}"),
        }
        assert!(!quarantine_strategy_worker(
            0,
            "duplicate",
            &instances,
            &flags,
            &signal_tx,
        ));
        assert!(signal_rx.try_recv().is_err());
    }

    #[test]
    fn emergency_instance_cancel_can_retry_after_queue_saturation() {
        let (raw_tx, signal_rx) = bounded::<RoutedSignal>(1);
        let signal_tx = SignalSender::system(raw_tx);
        signal_tx.send(Signal::Exit).unwrap();
        assert!(!enqueue_emergency_instance_cancel(
            0,
            "btc01",
            "first attempt",
            &signal_tx,
        ));
        assert!(matches!(signal_rx.recv().unwrap().signal, Signal::Exit));
        assert!(enqueue_emergency_instance_cancel(
            0,
            "btc01",
            "periodic retry",
            &signal_tx,
        ));
        assert!(matches!(
            signal_rx.recv().unwrap().signal,
            Signal::PolymarketCancelAllOrders { instance_id, .. } if instance_id == "btc01"
        ));
    }

    #[test]
    fn known_quarantined_private_owner_never_broadcasts_to_siblings() {
        let flags = vec![
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(false)),
        ];
        assert_eq!(
            classify_private_update_route(Some(0), 2, &flags),
            PrivateUpdateRoute::DropQuarantined(0),
        );
        assert_eq!(
            classify_private_update_route(Some(1), 2, &flags),
            PrivateUpdateRoute::Owner(1),
        );
        assert_eq!(
            classify_private_update_route(None, 2, &flags),
            PrivateUpdateRoute::Unowned,
        );
        assert_eq!(
            classify_private_update_route(Some(9), 2, &flags),
            PrivateUpdateRoute::DropInvalid(9),
        );
    }

    #[test]
    fn executor_update_uses_numeric_owner_without_coid_parsing() {
        let (owner0_tx, owner0_rx) = bounded(4);
        let (owner1_tx, owner1_rx) = bounded(4);
        let (signal_raw_tx, _signal_raw_rx) = bounded(4);
        let signal_tx = SignalSender::system(signal_raw_tx);
        let flags = vec![
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        ];
        Engine::route_executor_update(
            RoutedOrderUpdate {
                owner: 1,
                // Deliberately not namespaced: string recovery routing would
                // reject this update as unowned.
                update: OrderUpdate {
                    client_order_id: "opaque-coid".into(),
                    exchange: Exchange::Hexmarket,
                    symbol: "market".into(),
                    side: Side::Buy,
                    exchange_order_id: Some("oid".into()),
                    status: OrderStatus::Accepted,
                    liquidity: None,
                    filled_quantity: 0.0,
                    remaining_quantity: 1.0,
                    avg_fill_price: 0.0,
                    timestamp_ns: 1,
                    exchange_event_timestamp_ns: None,
                    trade_id: None,
                    order_audit: None,
                    error: None,
                },
            },
            &HashMap::new(),
            &[owner0_tx, owner1_tx],
            &flags,
            &["zero".into(), "one".into()],
            &signal_tx,
        );
        assert!(owner0_rx.try_recv().is_err());
        assert_eq!(
            owner1_rx.try_recv().unwrap().update.client_order_id,
            "opaque-coid",
        );
    }

    #[test]
    fn polymarket_rtt_probe_updates_are_not_strategy_private_events() {
        let probe = OrderUpdate {
            client_order_id: "probe:btc01:0xabc".into(),
            exchange: Exchange::Polymarket,
            symbol: "UP".into(),
            side: Side::Buy,
            exchange_order_id: Some("0xabc".into()),
            status: OrderStatus::Accepted,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: 100.0,
            avg_fill_price: 0.0,
            timestamp_ns: now_ns(),
            exchange_event_timestamp_ns: None,
            trade_id: None,
            order_audit: None,
            error: None,
        };
        assert!(is_synthetic_probe_update(&probe));

        let mut strategy = probe;
        strategy.client_order_id = "btc01-42".into();
        assert!(!is_synthetic_probe_update(&strategy));
    }

    #[test]
    fn polymarket_wallet_identity_cannot_alias_two_account_ids() {
        let mut wallets = HashMap::new();
        register_polymarket_wallet_identity(&mut wallets, "hex001", "0xAbC").unwrap();
        register_polymarket_wallet_identity(&mut wallets, "hex001", "0xabc").unwrap();
        let error =
            register_polymarket_wallet_identity(&mut wallets, "zhu02", "0xABC").unwrap_err();
        assert!(error.contains("both account_id `hex001` and `zhu02`"));
    }

    #[test]
    fn empty_polymarket_route_returns_error_without_panicking() {
        let config: Config = toml::from_str("[general]\nmode = 'live'").unwrap();
        let mut router = LiveRouter::new_with_poly_map(&config, &HashMap::new());
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            router.poly_route_mut("btc03").map(|_| ())
        }));
        assert!(result.is_ok(), "route lookup must never panic");
        let error = result.unwrap().unwrap_err().to_string();
        assert!(error.contains("unknown Polymarket instance_id `btc03`"));
    }

    #[test]
    fn critical_thread_completion_is_detected_without_joining() {
        let finished = thread::spawn(|| {});
        while !finished.is_finished() {
            thread::yield_now();
        }

        assert_eq!(
            first_finished_critical_thread(&[("execution", &finished)]),
            Some("execution"),
        );
        finished.join().unwrap();
    }

    #[test]
    fn admission_retained_wait_stays_info() {
        use hexagent_runtime::http1_pool::Role;

        let mut prev = HashMap::new();
        let lines =
            admission_log_snapshot(&mut prev, vec![("btc".into(), Role::Cancel, 10, 0, 4, 3)]);
        let (line, should_warn) = lines.get("btc").unwrap();
        assert!(!should_warn, "a retained cancel is not shed");
        assert!(line.contains("skip+0 retained_wait+4 busy3"));
    }

    #[test]
    fn admission_primary_skip_warns_only_affected_instance() {
        use hexagent_runtime::http1_pool::Role;

        let mut prev = HashMap::new();
        let lines = admission_log_snapshot(
            &mut prev,
            vec![
                ("btc".into(), Role::Cancel, 10, 1, 4, 3),
                ("eth".into(), Role::Cancel, 8, 0, 2, 2),
            ],
        );
        assert!(lines.get("btc").unwrap().1);
        assert!(!lines.get("eth").unwrap().1);
    }

    #[test]
    fn routine_polymarket_lifecycle_classification_is_narrow() {
        assert!(is_routine_expiry_cancel("event_expiry_sweep", true));
        assert!(
            !is_routine_expiry_cancel("event_expiry_sweep", false),
            "account-wide cancellation remains a warning",
        );
        assert!(!is_routine_expiry_cancel("risk_limit", true));

        assert!(is_routine_clob_resubscribe("CLOB resubscribe requested"));
        assert!(!is_routine_clob_resubscribe("websocket read failed"));
    }

    #[test]
    fn shared_symbol_fans_out_one_allocation_to_five_subscribers() {
        // Five BTC instances all subscribe BTCUSDT, but the router must not
        // allocate five copies of the order-book payload.
        let mut sym: HashMap<SymbolId, RouteMask> = HashMap::new();
        sym.insert(SymbolId::of("btcusdt"), 0b1_1111);
        let mut tok: HashMap<SymbolId, RouteMask> = HashMap::new();
        let mut latest = LatestMarketKeyRegistry::default();
        let (txs, rxs): (Vec<_>, Vec<_>) = (0..5).map(|_| test_market_lane(64)).unzip();

        Engine::route_market_event(
            Arc::new(ob(Exchange::Binance, "BTCUSDT")),
            &sym,
            &mut tok,
            &mut latest,
            &txs,
        );
        let first =
            resolve_market_event(rxs[0].try_recv().expect("instance 0 event"), &txs[0].latest)
                .expect("instance 0 payload");
        for (idx, rx) in rxs.iter().enumerate().skip(1) {
            let event = resolve_market_event(
                rx.try_recv()
                    .unwrap_or_else(|_| panic!("instance {idx} event")),
                &txs[idx].latest,
            )
            .unwrap_or_else(|| panic!("instance {idx} payload"));
            assert!(
                Arc::ptr_eq(&first.event, &event.event),
                "all five subscribers must share one MarketEvent allocation"
            );
        }
    }
}
