//! The sim_v2 driver the engine's thin loop calls.
//!
//! Owns the unified wall-clock `Scheduler`, the `ServerFeed` (server-axis
//! market replay), the stub matching `core`, and the `LatencyModel`. The
//! engine merges `peek_when()` against its own strat-lane market feed; when the
//! sim wins it calls `step()` (which advances one internal event and returns
//! any acks/fills now due for strategy delivery) and `submit()` (which schedules
//! a strategy signal's outbound effect with L1 latency).

use std::collections::{HashMap, VecDeque};
use std::path::Path;

use anyhow::Result;
use chrono::{DateTime, Utc};

use crate::exchange::sim::per_event_rtt::EventRttOverride;
use crate::types::{Exchange, Instrument, OrderRequest, OrderStatus, OrderUpdate, Side, Signal};

use super::clock::Scheduler;
use super::event::{ReachAction, SimEvent};
use super::exchange::{FillAuditRow, MakerOrderAuditRow, SimExchangeV2};
use super::feed::ServerFeed;
use super::latency::LatencyModel;
use crate::exchange::sim::latency::LatencyProfile;

pub struct SimV2Config {
    pub data_dir: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    /// Polymarket `(exchange, symbol)` sources; non-polymarket ignored.
    pub sources: Vec<(String, String)>,
    pub place_p50_ms: f64,
    pub place_p95_ms: f64,
    pub place_p99_ms: f64,
    pub cancel_p50_ms: f64,
    pub cancel_p95_ms: f64,
    pub cancel_p99_ms: f64,
    pub rho: f64,
    pub rho_cross: f64,
    pub seed: u64,
    pub client_timeout_ns: u64,
    /// Per-instance starting USDC (enables balance gating when seeded).
    pub wallet_usdc_by_iid: HashMap<String, f64>,
    /// Per-instance per-event split shares (CTF split mirror; credited at each
    /// new event's instrument).
    pub split_by_iid: HashMap<String, f64>,
    /// Cancel-attribution ahead-fraction override; `None` = proportional (§5).
    pub ahead_frac: Option<f64>,
    pub dynamic_ahead_frac_strength: f64,
    /// Adverse-selection conditioning of the cancel attribution: `rate` (0 = off)
    /// + `scale_ticks` (adverse mid-move mapping to full tilt). See `exchange.rs`.
    pub adverse_sel_rate: f64,
    pub adverse_scale_ticks: f64,
    /// Book-through adverse fill rate ∈ [0,1] (0 = off): a resting order the
    /// contra side sweeps strictly through gets picked off (latency adverse
    /// selection). See `exchange.rs`.
    pub book_through_rate: f64,
    /// Maximum fraction of adverse, unexplained same-level depletion treated as
    /// hidden execution volume. 0 keeps cancel-only attribution result-neutral.
    pub unexplained_depletion_exec_rate: f64,
    /// Deterministic fraction of orders whose maker executions retain
    /// an exchange-side residual small enough for 99%-coverage release.
    pub inferred_maker_residual_rate: f64,
    pub inferred_maker_residual_fraction: f64,
    /// Approximate leave-one-out correction for tapes containing this
    /// strategy's original live resting order.
    pub replay_self_depth_rate: f64,
    /// Leave-one-out correction for taker sweeps on a self-contaminated tape.
    pub replay_self_taker_depth_rate: f64,
    /// Multiplier of sampled cancel L2 reserved for exchange-side matching
    /// finality before cancellation becomes effective. Values above 1 model
    /// processing tails beyond the nominal response boundary.
    pub cancel_finality_delay_frac: f64,
    /// Volume-neutral forward-markout adverse-reprice strength (0 = off). Keeps the
    /// full fill, settles it adverse toward the forward mid (peeked `horizon` ns
    /// ahead) → edge drops at preserved maker volume → the sim's maker-fill markout
    /// matches live's −0.75¢. See `exchange.rs`.
    pub fill_markout_vn: f64,
    /// Forward-markout adverse reprice for maker fills inferred from book
    /// depletion / book-through rather than an explicit public trade.
    pub book_fill_markout_vn: f64,
    pub fill_markout_horizon_ns: u64,
    pub dynamic_fill_markout: bool,
    pub dynamic_markout_spot_vol: bool,
    pub dynamic_markout_lookback_ns: u64,
    pub dynamic_markout_vol_ref_ticks: f64,
    pub dynamic_markout_vol_elasticity: f64,
    pub dynamic_markout_min_mult: f64,
    pub dynamic_markout_max_mult: f64,
    /// WS fill-push latency multiplier on the half-RTT.
    pub fill_push_mult: f64,
    /// Independent private WebSocket fill-observation latency anchors (ms).
    /// A non-positive p50 preserves the historical HTTP-derived path.
    pub private_fill_p50_ms: f64,
    pub private_fill_p95_ms: f64,
    pub private_fill_p99_ms: f64,
    /// matched-can't-cancel window (ns).
    pub matched_cant_cancel_window_ns: u64,
    /// Per-event RTT override table (sim_rtt_mode="exact"); `None` = pooled.
    pub per_event_rtt: Option<HashMap<u64, EventRttOverride>>,
    /// TAKER matching-engine overhead quantiles (ms): added to place RTT for
    /// taker fills.
    pub taker_overhead_p50_ms: f64,
    pub taker_overhead_p95_ms: f64,
    pub taker_overhead_p99_ms: f64,
    /// Causal rolling p50/p95/p99 overhead anchors keyed by event start.
    pub dynamic_taker_overhead_by_event: Option<HashMap<u64, (f64, f64, f64)>>,
    /// Maker/taker "race" rates in [0,1] (0 = off). See `exchange.rs`.
    pub maker_race_rate: f64,
    pub taker_race_rate: f64,
    /// Earlier same-level simulated orders included in a new order's FIFO
    /// queue position (0 = legacy independent orders, 1 = full own FIFO).
    pub order_queue_position_strength: f64,
    /// Causal suppression of favorable maker fills at the real limit.
    pub maker_toxicity_strength: f64,
    pub maker_toxicity_scale_ticks: f64,
    /// Maker / taker race lookahead horizons (ns): the entry / match peek looks
    /// this far ahead (0 = immediate next snapshot).
    pub maker_race_horizon_ns: u64,
    pub taker_race_horizon_ns: u64,
    /// Outcome-folding: fold the two outcome tokens into one canonical up-frame
    /// book (down mapped p↔1−p, bid↔ask / buy↔sell). Removes the cross-outcome
    /// double-count. See `exchange.rs`.
    pub fold_outcomes: bool,
    /// Fail-closed max age for both full-book clocks. 0 disables the gate.
    pub book_stale_after_ns: u64,
    /// Disable book lookahead after an order reaches the simulated engine.
    pub causal_matching: bool,
    /// Existing resting maker fills ignore local-clock-only staleness.
    pub stale_resting_exchange_only: bool,
    /// Trade-flow taker competition rate ∈ [0,1] (0 = off): fraction of competing
    /// in-flight taker trade volume consumed ahead of us — we fill only the
    /// overflow. With the taker race, the taker-volume model. See `exchange.rs`.
    pub taker_comp_rate: f64,
    /// Taker competition in-flight window (ns) ≈ taker overhead exposure.
    pub taker_comp_window_ns: u64,
    /// Collapse overlapping race/competition liquidity-loss observations into
    /// one cap (the less restrictive independent cap).
    pub taker_overlap_dedup: bool,
    /// Causal rolling place-RTT state keyed by 5-minute event start. `None`
    /// keeps the historical fixed-window model byte-identical.
    pub dynamic_window_rtt_by_event: Option<HashMap<u64, f64>>,
    pub dynamic_window_rtt_ref_ms: f64,
    pub dynamic_race_rtt_elasticity: f64,
    pub dynamic_comp_rtt_elasticity: f64,
    pub dynamic_window_min_mult: f64,
    pub dynamic_window_max_mult: f64,
    /// Deep-queue model for resting prices beyond the recorded 5-level window:
    /// 0 = legacy least-squares linear extrapolation; >0 = outermost-level
    /// flat/geometric-decay (1.0 = flat, <1 = decay). See `book.rs`.
    pub deep_queue_decay: f64,
    pub dynamic_deep_queue_strength: f64,
    pub dynamic_deep_queue_min_decay: f64,
    /// Mirror of `exchanges[polymarket].use_batch_orders`. When `false`,
    /// each place / cancel in a batch is dispatched as its OWN API call
    /// with its OWN RTT draw + timeout (matching the live executor's
    /// concurrent single-`POST /order` / `DELETE /order` fan-out). When
    /// `true`, a batch shares one RTT. Decisive for cancel timeouts: with
    /// batching the reprice `BatchUpdateOrders` glues cancels to the PLACE
    /// RTT, so the cancel sampler is never exercised → ~0 cancel timeouts.
    pub use_batch_orders: bool,
    /// **Pre-built place/cancel latency profiles** (2026-06-16). When
    /// `Some`, these REPLACE the `*_p{50,95,99}_ms` scalar Empirical
    /// profiles — used for the record-replay source
    /// (`LatencyProfile::RecordReplay`) which the engine builds from a
    /// `latency_record` directory. `None` (default) = the legacy scalar
    /// path (byte-identical). `rho_cross` still applies via the coupled
    /// wrapper; each profile carries its own AR(1) `rho`.
    pub place_profile: Option<LatencyProfile>,
    pub cancel_profile: Option<LatencyProfile>,
}

/// A potentially marketable order observed by the simulator's single writer
/// from client emission through the matching-engine window. Entries are
/// bounded by concurrent in-flight places and removed at engine admission or
/// when their `TakerMatch` event runs.
struct PendingTakerRace {
    order: OrderRequest,
    /// `None` until the order first becomes marketable. This prevents a limit
    /// that starts outside the touch from being treated as a full race miss.
    min_available_qty: Option<f64>,
    observe_until_ns: u64,
}

pub struct Simulator {
    sched: Scheduler,
    feed: ServerFeed,
    core: SimExchangeV2,
    latency: LatencyModel,
    client_timeout_ns: u64,
    timeouts: u64,
    cancel_finality_delay_frac: f64,
    cancel_finality_delayed: u64,
    cancel_finality_matched: u64,
    per_event_rtt: Option<HashMap<u64, EventRttOverride>>,
    dynamic_taker_overhead_by_event: Option<HashMap<u64, (f64, f64, f64)>>,
    last_dynamic_overhead_event: Option<u64>,
    dynamic_overhead_n: u64,
    dynamic_overhead_p50_sum_ms: f64,
    dynamic_overhead_p95_sum_ms: f64,
    dynamic_overhead_p99_sum_ms: f64,
    /// Cached `core.race_enabled()` — skips the peek when the race is off.
    race_enabled: bool,
    /// Matching uses only state observed by the simulated engine clock.
    causal_matching: bool,
    /// Causal taker-race state, owned and mutated only by the simulator thread.
    pending_taker_races: HashMap<String, PendingTakerRace>,
    /// Maker / taker race lookahead horizons (ns).
    maker_race_horizon_ns: u64,
    taker_race_horizon_ns: u64,
    base_taker_race_horizon_ns: u64,
    base_taker_comp_window_ns: u64,
    taker_comp_rate: f64,
    dynamic_window_rtt_by_event: Option<HashMap<u64, f64>>,
    dynamic_window_rtt_ref_ms: f64,
    dynamic_race_rtt_elasticity: f64,
    dynamic_comp_rtt_elasticity: f64,
    dynamic_window_min_mult: f64,
    dynamic_window_max_mult: f64,
    last_dynamic_window_event: Option<u64>,
    dynamic_window_n: u64,
    dynamic_window_rtt_sum_ms: f64,
    dynamic_race_window_sum_ns: u128,
    dynamic_comp_window_sum_ns: u128,
    dynamic_window_mult_min: f64,
    dynamic_window_mult_max: f64,
    /// See `SimV2Config::use_batch_orders`.
    use_batch_orders: bool,
    /// Forward horizon (ns) for markout fill haircuts; peek the canonical mid
    /// this far past each eligible trade/book event. Separate gates avoid any
    /// future-book lookup on a disabled origin path.
    fill_markout_horizon_ns: u64,
    markout_on: bool,
    book_markout_on: bool,
    base_fill_markout_vn: f64,
    dynamic_fill_markout: bool,
    dynamic_markout_spot_vol: bool,
    dynamic_markout_lookback_ns: u64,
    dynamic_markout_vol_ref_ticks: f64,
    dynamic_markout_vol_elasticity: f64,
    dynamic_markout_min_mult: f64,
    dynamic_markout_max_mult: f64,
    markout_mid_history: HashMap<String, VecDeque<(u64, f64)>>,
    markout_last_book_ts: HashMap<String, u64>,
    markout_tick_by_symbol: HashMap<String, f64>,
    markout_symbol_fifo: VecDeque<String>,
    /// One-second Binance BTCUSDT closes with cumulative squared log returns.
    /// The cumulative representation makes rolling realised volatility O(1).
    markout_spot_rv: VecDeque<(u64, f64, f64)>,
    dynamic_markout_states: Vec<f32>,
    dynamic_markout_vn_sum: f64,
    dynamic_markout_vn_min: f64,
    dynamic_markout_vn_max: f64,
}

/// Floor an ISO-8601 event_start_time to its 5-min boundary unix-secs key
/// (matches `per_event_rtt`'s table key + v1's `parse_event_start_ts_secs`).
fn parse_event_start_ts_secs(iso: &str) -> Option<u64> {
    if iso.is_empty() {
        return None;
    }
    let dt = chrono::DateTime::parse_from_rfc3339(iso).ok()?;
    let secs = dt.timestamp();
    if secs < 0 {
        return None;
    }
    Some(((secs as u64) / 300) * 300)
}

/// Scale a fixed execution window with a power-law RTT elasticity.  A power
/// law is dimensionless, monotone and anchored exactly at the training
/// reference.  Bounds keep sparse/extreme latency episodes from producing an
/// implausible zero or multi-second runaway window.
fn dynamic_window_ns(
    base_ns: u64,
    rtt_state_ms: f64,
    ref_ms: f64,
    elasticity: f64,
    min_mult: f64,
    max_mult: f64,
) -> (u64, f64) {
    if base_ns == 0
        || !rtt_state_ms.is_finite()
        || rtt_state_ms <= 0.0
        || !ref_ms.is_finite()
        || ref_ms <= 0.0
        || !elasticity.is_finite()
    {
        return (base_ns, 1.0);
    }
    let lo = if min_mult.is_finite() && min_mult > 0.0 {
        min_mult
    } else {
        0.5
    };
    let hi = if max_mult.is_finite() && max_mult >= lo {
        max_mult
    } else {
        lo.max(2.0)
    };
    let mult = (rtt_state_ms / ref_ms).powf(elasticity).clamp(lo, hi);
    let scaled = ((base_ns as f64) * mult)
        .round()
        .clamp(0.0, u64::MAX as f64) as u64;
    (scaled, mult)
}

fn dynamic_markout_strength(
    base_vn: f64,
    vol_state_ticks: f64,
    ref_ticks: f64,
    elasticity: f64,
    min_mult: f64,
    max_mult: f64,
) -> (f64, f64) {
    if base_vn <= 0.0
        || !vol_state_ticks.is_finite()
        || vol_state_ticks <= 0.0
        || !ref_ticks.is_finite()
        || ref_ticks <= 0.0
        || !elasticity.is_finite()
    {
        return (base_vn.max(0.0), 1.0);
    }
    let lo = if min_mult.is_finite() && min_mult > 0.0 {
        min_mult
    } else {
        0.5
    };
    let hi = if max_mult.is_finite() && max_mult >= lo {
        max_mult
    } else {
        lo.max(2.0)
    };
    let mult = (vol_state_ticks / ref_ticks).powf(elasticity).clamp(lo, hi);
    (base_vn * mult, mult)
}

impl Simulator {
    pub fn new(cfg: SimV2Config) -> Result<Self> {
        let feed = ServerFeed::new(Path::new(&cfg.data_dir), &cfg.sources, cfg.start, cfg.end)?;
        // Record-replay (or any pre-built) profile wins; otherwise build the
        // legacy scalar Empirical from the calibrated p50/p95/p99 anchors.
        let place = cfg.place_profile.clone().unwrap_or_else(|| {
            LatencyModel::empirical_profile(
                cfg.place_p50_ms,
                cfg.place_p95_ms,
                cfg.place_p99_ms,
                cfg.rho,
            )
        });
        let cancel = cfg.cancel_profile.clone().unwrap_or_else(|| {
            LatencyModel::empirical_profile(
                cfg.cancel_p50_ms,
                cfg.cancel_p95_ms,
                cfg.cancel_p99_ms,
                cfg.rho,
            )
        });
        let mut latency = LatencyModel::new(place, cancel, cfg.rho_cross, cfg.seed);
        latency.set_fill_push_mult(cfg.fill_push_mult);
        latency.set_private_fill_anchors(
            cfg.private_fill_p50_ms,
            cfg.private_fill_p95_ms,
            cfg.private_fill_p99_ms,
        );
        // Censoring threshold for the per-event timeout-rate injection — must
        // equal the engine's timeout boundary (`rtt > client_timeout_ns`).
        latency.set_client_timeout_ms(cfg.client_timeout_ns as f64 / 1_000_000.0);
        latency.set_taker_overhead_anchors(
            cfg.taker_overhead_p50_ms,
            cfg.taker_overhead_p95_ms,
            cfg.taker_overhead_p99_ms,
        );
        let mut core = SimExchangeV2::new(
            cfg.client_timeout_ns,
            cfg.wallet_usdc_by_iid,
            cfg.split_by_iid,
        );
        core.configure(cfg.ahead_frac, cfg.matched_cant_cancel_window_ns);
        core.configure_dynamic_ahead_frac(cfg.dynamic_ahead_frac_strength);
        core.configure_adverse_sel(cfg.adverse_sel_rate, cfg.adverse_scale_ticks);
        core.configure_book_through(cfg.book_through_rate);
        core.configure_unexplained_depletion_execution(cfg.unexplained_depletion_exec_rate);
        core.configure_inferred_maker_residual(
            cfg.inferred_maker_residual_rate,
            cfg.inferred_maker_residual_fraction,
        );
        core.configure_replay_self_depth(cfg.replay_self_depth_rate);
        core.configure_replay_self_taker_depth(cfg.replay_self_taker_depth_rate);
        core.configure_fill_markout_vn(cfg.fill_markout_vn);
        core.configure_book_fill_markout_vn(cfg.book_fill_markout_vn);
        core.configure_race(cfg.maker_race_rate, cfg.taker_race_rate);
        core.configure_order_queue_position(cfg.order_queue_position_strength);
        core.configure_maker_toxicity(
            cfg.maker_toxicity_strength,
            cfg.maker_toxicity_scale_ticks,
        );
        core.set_fold_outcomes(cfg.fold_outcomes);
        core.configure_book_stale_gate(cfg.book_stale_after_ns);
        core.configure_stale_resting_exchange_only(cfg.stale_resting_exchange_only);
        core.configure_taker_comp(cfg.taker_comp_rate, cfg.taker_comp_window_ns);
        core.configure_taker_overlap_dedup(cfg.taker_overlap_dedup);
        core.set_deep_queue_decay(cfg.deep_queue_decay);
        core.set_dynamic_deep_queue(
            cfg.dynamic_deep_queue_strength,
            cfg.dynamic_deep_queue_min_decay,
        );
        let race_enabled = core.race_enabled();
        Ok(Self {
            sched: Scheduler::new(),
            feed,
            core,
            latency,
            client_timeout_ns: cfg.client_timeout_ns,
            timeouts: 0,
            cancel_finality_delay_frac: if cfg.cancel_finality_delay_frac.is_finite() {
                cfg.cancel_finality_delay_frac.clamp(0.0, 64.0)
            } else {
                0.0
            },
            cancel_finality_delayed: 0,
            cancel_finality_matched: 0,
            per_event_rtt: cfg.per_event_rtt,
            dynamic_taker_overhead_by_event: cfg.dynamic_taker_overhead_by_event,
            last_dynamic_overhead_event: None,
            dynamic_overhead_n: 0,
            dynamic_overhead_p50_sum_ms: 0.0,
            dynamic_overhead_p95_sum_ms: 0.0,
            dynamic_overhead_p99_sum_ms: 0.0,
            race_enabled,
            causal_matching: cfg.causal_matching,
            pending_taker_races: HashMap::new(),
            maker_race_horizon_ns: cfg.maker_race_horizon_ns,
            taker_race_horizon_ns: cfg.taker_race_horizon_ns,
            base_taker_race_horizon_ns: cfg.taker_race_horizon_ns,
            base_taker_comp_window_ns: cfg.taker_comp_window_ns,
            taker_comp_rate: cfg.taker_comp_rate,
            dynamic_window_rtt_by_event: cfg.dynamic_window_rtt_by_event,
            dynamic_window_rtt_ref_ms: cfg.dynamic_window_rtt_ref_ms,
            dynamic_race_rtt_elasticity: cfg.dynamic_race_rtt_elasticity,
            dynamic_comp_rtt_elasticity: cfg.dynamic_comp_rtt_elasticity,
            dynamic_window_min_mult: cfg.dynamic_window_min_mult,
            dynamic_window_max_mult: cfg.dynamic_window_max_mult,
            last_dynamic_window_event: None,
            dynamic_window_n: 0,
            dynamic_window_rtt_sum_ms: 0.0,
            dynamic_race_window_sum_ns: 0,
            dynamic_comp_window_sum_ns: 0,
            dynamic_window_mult_min: f64::INFINITY,
            dynamic_window_mult_max: f64::NEG_INFINITY,
            use_batch_orders: cfg.use_batch_orders,
            fill_markout_horizon_ns: cfg.fill_markout_horizon_ns,
            markout_on: cfg.fill_markout_vn > 0.0 && cfg.fill_markout_horizon_ns > 0,
            book_markout_on: cfg.book_fill_markout_vn > 0.0
                && cfg.fill_markout_horizon_ns > 0,
            base_fill_markout_vn: cfg.fill_markout_vn.max(0.0),
            dynamic_fill_markout: cfg.dynamic_fill_markout,
            dynamic_markout_spot_vol: cfg.dynamic_markout_spot_vol,
            dynamic_markout_lookback_ns: cfg.dynamic_markout_lookback_ns.max(1),
            dynamic_markout_vol_ref_ticks: cfg.dynamic_markout_vol_ref_ticks,
            dynamic_markout_vol_elasticity: cfg.dynamic_markout_vol_elasticity,
            dynamic_markout_min_mult: cfg.dynamic_markout_min_mult,
            dynamic_markout_max_mult: cfg.dynamic_markout_max_mult,
            markout_mid_history: HashMap::new(),
            markout_last_book_ts: HashMap::new(),
            markout_tick_by_symbol: HashMap::new(),
            markout_symbol_fifo: VecDeque::new(),
            markout_spot_rv: VecDeque::new(),
            dynamic_markout_states: Vec::new(),
            dynamic_markout_vn_sum: 0.0,
            dynamic_markout_vn_min: f64::INFINITY,
            dynamic_markout_vn_max: f64::NEG_INFINITY,
        })
    }

    /// Race lookahead: stash the book snapshot(s) `horizon_ns` after `when` for
    /// `token` (and its cross-outcome complement) so the core's queue-init /
    /// taker-cap can compare now vs future. `horizon_ns` is the configured maker
    /// entry / taker match horizon. No-op when the race is off.
    ///
    /// Maker (single-snapshot): the queue the resting order faces just past the
    /// entry horizon — peek the first book strictly after `when+horizon`.
    fn prime_next_books(&mut self, token: &str, when: u64, horizon_ns: u64) {
        if !self.race_enabled || self.causal_matching {
            self.core.clear_next_books();
            return;
        }
        let at = when.saturating_add(horizon_ns);
        self.core.clear_next_books();
        if self.core.fold_on() {
            // Folding: prime the SINGLE canonical frame's next book. The next
            // snapshot can come from either outcome stream — pick the earlier ts;
            // mirror it if it came from the sibling (down) stream.
            let canon = self.core.canonical_token(token);
            let from_canon = self.feed.peek_next_book(&canon, at);
            let sibling = self.core.fold_sibling_of(&canon);
            let from_sib = sibling
                .as_ref()
                .and_then(|s| self.feed.peek_next_book(s, at));
            match (from_canon, from_sib) {
                (Some((tc, bc, ac)), Some((ts, bs, as_))) => {
                    if tc <= ts {
                        self.core.set_next_book(&canon, bc, ac);
                    } else {
                        self.core.set_next_book_mirrored(&canon, &bs, &as_);
                    }
                }
                (Some((_, bc, ac)), None) => self.core.set_next_book(&canon, bc, ac),
                (None, Some((_, bs, as_))) => self.core.set_next_book_mirrored(&canon, &bs, &as_),
                (None, None) => {}
            }
            return;
        }
        if let Some((_, b, a)) = self.feed.peek_next_book(token, at) {
            self.core.set_next_book(token, b, a);
        }
        if let Some(comp) = self.core.complement_of(token) {
            if let Some((_, b, a)) = self.feed.peek_next_book(&comp, at) {
                self.core.set_next_book(&comp, b, a);
            }
        }
    }

    /// Taker windowed race lookahead: stash EVERY book snapshot in the in-flight
    /// window `(when, when+horizon_ns]` for `token` so the core's taker-cap takes
    /// the MIN fillable volume over the whole window — liquidity pulled at ANY
    /// instant counts as a miss, not just the endpoint. Folding only; mirrors
    /// sibling-stream snapshots into the canonical frame. No-op when race off.
    fn prime_taker_window(&mut self, token: &str, when: u64, horizon_ns: u64) {
        if !self.race_enabled || self.causal_matching {
            self.core.clear_next_books();
            return;
        }
        let at = when.saturating_add(horizon_ns);
        self.core.clear_next_books();
        if self.core.fold_on() {
            let canon = self.core.canonical_token(token);
            for (_, b, a) in self.feed.peek_books_in_window(&canon, when, at) {
                self.core.push_next_window(&canon, b, a);
            }
            if let Some(sib) = self.core.fold_sibling_of(&canon) {
                for (_, b, a) in self.feed.peek_books_in_window(&sib, when, at) {
                    self.core.push_next_window_mirrored(&canon, &b, &a);
                }
            }
            return;
        }
        // Non-folding legacy path: keep the single-snapshot behavior.
        if let Some((_, b, a)) = self.feed.peek_next_book(token, at) {
            self.core.set_next_book(token, b, a);
        }
        if let Some(comp) = self.core.complement_of(token) {
            if let Some((_, b, a)) = self.feed.peek_next_book(&comp, at) {
                self.core.set_next_book(&comp, b, a);
            }
        }
    }

    #[allow(dead_code)]
    pub fn client_timeout_ns(&self) -> u64 {
        self.client_timeout_ns
    }

    /// (anchored, fallback) trade counts for the end-of-run summary.
    pub fn trade_anchor_stats(&self) -> (u64, u64) {
        self.feed.trade_anchor_stats()
    }

    /// (taker_fills, maker_fills, rejects) from the matching core.
    pub fn core_stats(&self) -> (u64, u64, u64) {
        (
            self.core.taker_fills,
            self.core.maker_fills,
            self.core.rejects,
        )
    }

    /// Final gating-wallet USDC for an instance (diagnostic: detect the
    /// settlement-credit bleed — wallet drains toward 0 over the run because
    /// retire_token drops winning shares without crediting $1/share back).
    pub fn wallet_usdc(&self, iid: &str) -> Option<f64> {
        self.core.wallet_usdc_raw(iid)
    }

    /// Per-reason reject breakdown: (taker_buy, taker_sell, rest_buy,
    /// rest_sell, rest_sell_short_sum) — diagnostic for size/seed mismatch.
    pub fn reject_breakdown(&self) -> (u64, u64, u64, u64, f64) {
        (
            self.core.rej_taker_buy,
            self.core.rej_taker_sell,
            self.core.rej_rest_buy,
            self.core.rej_rest_sell,
            self.core.rej_rest_sell_short_sum,
        )
    }

    /// (timeouts, matched_cant_cancel) for the summary.
    pub fn timeout_stats(&self) -> (u64, u64) {
        (self.timeouts, self.core.matched_cant_cancel)
    }

    /// (cancels whose exchange finality was delayed, those that matched before
    /// the delayed finalization) for end-of-run calibration diagnostics.
    pub fn cancel_finality_stats(&self) -> (u64, u64) {
        (self.cancel_finality_delayed, self.cancel_finality_matched)
    }

    /// (post_only_rejects, post_only_seen) for the summary.
    pub fn post_only_stats(&self) -> (u64, u64) {
        (self.core.post_only_rejects, self.core.post_only_seen)
    }

    pub fn fill_audit_rows(&self) -> Vec<FillAuditRow> {
        self.core.fill_audit_rows()
    }

    pub fn configure_maker_order_audit(&mut self, enabled: bool) {
        self.core.configure_maker_order_audit(enabled);
    }

    pub fn maker_order_audit_rows(&self) -> Vec<MakerOrderAuditRow> {
        self.core.maker_order_audit_rows()
    }

    /// Double-clock full-book stale gate diagnostics:
    /// `(order blocks, trade blocks, exchange-clock hits, local-clock hits,
    /// queue rebases on recovery)`.
    pub fn book_stale_stats(&self) -> (u64, u64, u64, u64, u64) {
        let c = &self.core;
        (
            c.book_stale_order_blocks,
            c.book_stale_trade_blocks,
            c.book_stale_exchange_hits,
            c.book_stale_local_hits,
            c.book_stale_rebases,
        )
    }

    /// Phase-A diagnostics: (mean maker fill-age ms, frac fills on orders >1s,
    /// mean removed-order lifetime ms).
    pub fn fill_timing_stats(&self) -> (f64, f64, f64) {
        let c = &self.core;
        let mean_age = if c.maker_fill_n > 0 {
            (c.maker_fill_age_sum_ns / c.maker_fill_n as u128) as f64 / 1e6
        } else {
            0.0
        };
        let over1s = if c.maker_fill_n > 0 {
            c.maker_fill_age_over1s as f64 / c.maker_fill_n as f64
        } else {
            0.0
        };
        let mean_life = if c.maker_life_n > 0 {
            (c.maker_life_sum_ns / c.maker_life_n as u128) as f64 / 1e6
        } else {
            0.0
        };
        (mean_age, over1s, mean_life)
    }

    /// Race diagnostics: (maker placements inflated, total maker placements,
    /// mean blended/now ratio over inflated, taker fills capped, taker caps that
    /// drove fill to ~0 = full miss).
    pub fn race_stats(&self) -> (u64, u64, f64, u64, u64) {
        let c = &self.core;
        let mean_ratio = if c.maker_race_inflated > 0 {
            c.maker_race_ratio_sum / c.maker_race_inflated as f64
        } else {
            0.0
        };
        (
            c.maker_race_inflated,
            c.maker_race_placements,
            mean_ratio,
            c.taker_race_capped,
            c.taker_race_capped_zero,
        )
    }

    /// # resyncs where the adverse-selection tilt advanced the queue past its
    /// proportional baseline (diagnostic for `sim_v2_adverse_sel_rate`).
    pub fn adverse_advanced(&self) -> u64 {
        self.core.adverse_advanced
    }

    /// Per-order own FIFO diagnostics: positioned orders, initial own queue
    /// quantity, later-order cancellation advances, and their summed quantity.
    pub fn own_queue_position_stats(&self) -> (u64, f64, u64, f64) {
        let c = &self.core;
        (
            c.own_queue_positioned_orders,
            c.own_queue_initial_qty,
            c.own_queue_cancel_advances_n,
            c.own_queue_cancel_advance_qty,
        )
    }

    /// Maker trade fragments/quantity suppressed by the causal favorable-move
    /// selection model.
    pub fn maker_toxicity_stats(&self) -> (u64, f64) {
        (
            self.core.maker_toxicity_suppressed_n,
            self.core.maker_toxicity_suppressed_qty,
        )
    }

    /// Dynamic ahead-fraction diagnostics: cancel-resync count, mean and range.
    pub fn dynamic_ahead_frac_stats(&self) -> Option<(u64, f64, f64, f64)> {
        let c = &self.core;
        (c.dynamic_ahead_frac_n > 0).then(|| (
            c.dynamic_ahead_frac_n,
            c.dynamic_ahead_frac_sum / c.dynamic_ahead_frac_n as f64,
            c.dynamic_ahead_frac_min,
            c.dynamic_ahead_frac_max,
        ))
    }

    /// # book-through adverse fills produced (diagnostic for
    /// `sim_v2_book_through_rate`).
    pub fn book_through_fills(&self) -> u64 {
        self.core.book_through_fills_n
    }

    /// # maker fill fragments produced by unexplained L2 depletion.
    pub fn unexplained_depletion_fills(&self) -> u64 {
        self.core.unexplained_depletion_fills_n
    }

    /// Orders where a maker fill stopped at the configured inferred residual,
    /// and total fill quantity withheld by that physical lifecycle model.
    pub fn inferred_maker_residual_stats(&self) -> (u64, f64) {
        (
            self.core.inferred_maker_residual_orders_n,
            self.core.inferred_maker_residual_qty,
        )
    }

    /// Taker sweeps corrected by the same-instance leave-one-out book view and
    /// the total public depth removed from those sweep ladders.
    pub fn replay_self_taker_stats(&self) -> (u64, f64) {
        (
            self.core.taker_replay_self_sweeps_n,
            self.core.taker_replay_self_depth_qty,
        )
    }

    /// # maker fills haircut by the forward-markout conditioning (diagnostic).
    pub fn fill_haircuts(&self) -> u64 {
        self.core.fill_haircut_n
    }

    /// Book/depletion maker fills repriced by forward markout, their quantity,
    /// and the resulting settlement-cost increase in USDC.
    pub fn book_fill_markout_stats(&self) -> (u64, f64, f64) {
        (
            self.core.book_fill_haircut_n,
            self.core.book_fill_haircut_qty,
            self.core.book_fill_haircut_cost_usdc,
        )
    }

    /// Distribution of maker initial queue length (`q_ahead` at placement) and
    /// taker fillable volume at match. Returns (maker_pcts, taker_pcts) where
    /// each is [n, mean, p10, p25, p50, p75, p90, p99, frac_zero].
    pub fn depth_distributions(&self) -> (Vec<f64>, Vec<f64>) {
        fn pcts(v: &[f32]) -> Vec<f64> {
            if v.is_empty() {
                return vec![0.0; 9];
            }
            let mut a: Vec<f64> = v.iter().map(|x| *x as f64).collect();
            a.sort_by(|x, y| x.partial_cmp(y).unwrap());
            let n = a.len();
            let q = |p: f64| a[((p * n as f64) as usize).min(n - 1)];
            let mean = a.iter().sum::<f64>() / n as f64;
            let zero = a.iter().filter(|x| **x < 1e-6).count() as f64 / n as f64;
            vec![
                n as f64,
                mean,
                q(0.10),
                q(0.25),
                q(0.50),
                q(0.75),
                q(0.90),
                q(0.99),
                zero,
            ]
        }
        (pcts(&self.core.maker_q_init), pcts(&self.core.taker_avail))
    }

    /// Maker placement price-vs-BBO buckets: each [total, q0_count] for
    /// (improve, join, behind, nobook). Explains the zero-queue share.
    pub fn placement_buckets(&self) -> [[u64; 2]; 4] {
        let c = &self.core;
        [
            c.place_improve,
            c.place_join,
            c.place_behind,
            c.place_nobook,
        ]
    }

    /// q_init=0 fallback split: (extrapolated beyond-window, in-window best-rule).
    pub fn q0_fallback_split(&self) -> (u64, u64) {
        (self.core.q0_extrapolated, self.core.q0_bestrule)
    }

    pub fn dynamic_deep_queue_stats(&self) -> Option<(u64, f64, f64, f64)> {
        let c = &self.core;
        (c.dynamic_deep_queue_n > 0).then(|| (
            c.dynamic_deep_queue_n,
            c.dynamic_deep_queue_decay_sum / c.dynamic_deep_queue_n as f64,
            c.dynamic_deep_queue_decay_min,
            c.dynamic_deep_queue_decay_max,
        ))
    }

    /// Trade-flow taker competition diagnostics:
    /// (capped, capped_to_zero, mean competing volume seen at a marketable match).
    pub fn taker_comp_stats(&self) -> (u64, u64, f64) {
        let c = &self.core;
        let mean = if c.taker_comp_n > 0 {
            c.taker_comp_vol_sum / c.taker_comp_n as f64
        } else {
            0.0
        };
        (c.taker_comp_capped, c.taker_comp_capped_zero, mean)
    }

    /// Dynamic-window diagnostics: event count, mean RTT state, mean race and
    /// competition windows (ms), and the observed multiplier range.
    pub fn dynamic_window_stats(&self) -> Option<(u64, f64, f64, f64, f64, f64)> {
        let n = self.dynamic_window_n;
        if n == 0 {
            return None;
        }
        Some((
            n,
            self.dynamic_window_rtt_sum_ms / n as f64,
            self.dynamic_race_window_sum_ns as f64 / n as f64 / 1e6,
            self.dynamic_comp_window_sum_ns as f64 / n as f64 / 1e6,
            self.dynamic_window_mult_min,
            self.dynamic_window_mult_max,
        ))
    }

    /// Dynamic matching-overhead diagnostics: event count and mean anchors.
    pub fn dynamic_taker_overhead_stats(&self) -> Option<(u64, f64, f64, f64)> {
        let n = self.dynamic_overhead_n;
        (n > 0).then(|| {
            (
                n,
                self.dynamic_overhead_p50_sum_ms / n as f64,
                self.dynamic_overhead_p95_sum_ms / n as f64,
                self.dynamic_overhead_p99_sum_ms / n as f64,
            )
        })
    }

    /// Dynamic-markout diagnostics:
    /// `[n, state_mean, p50, p75, p90, p99, vn_mean, vn_min, vn_max]`.
    pub fn dynamic_markout_stats(&self) -> Option<Vec<f64>> {
        if self.dynamic_markout_states.is_empty() {
            return None;
        }
        let mut states = self.dynamic_markout_states.clone();
        states.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = states.len();
        let q = |p: f64| states[((p * n as f64).floor() as usize).min(n - 1)] as f64;
        let mean = states.iter().map(|x| *x as f64).sum::<f64>() / n as f64;
        Some(vec![
            n as f64,
            mean,
            q(0.50),
            q(0.75),
            q(0.90),
            q(0.99),
            self.dynamic_markout_vn_sum / n as f64,
            self.dynamic_markout_vn_min,
            self.dynamic_markout_vn_max,
        ])
    }

    pub fn dynamic_markout_state_unit(&self) -> &'static str {
        if self.dynamic_markout_spot_vol {
            "bps"
        } else {
            "ticks"
        }
    }

    /// Sample a synthetic place RTT (ms) for the strategy's RTT-gate probe loop.
    /// Mirrors v1's `coupled.sample_place` probe source so the gate accumulates
    /// samples and recovers Probe→Trade. Advances the shared latency state (as
    /// v1 does).
    pub fn sample_probe_rtt_ms(&mut self, now_ns: u64) -> f64 {
        let (l1, l2) = self.latency.sample_place_split(now_ns);
        (l1 + l2) as f64 / 1_000_000.0
    }

    /// Wall-clock time of the next internal event (server feed or scheduler).
    pub fn peek_when(&self) -> Option<u64> {
        match (self.feed.peek_when(), self.sched.peek_when()) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    /// Advance one internal event. Returns acks/fills now due for delivery to
    /// the strategy (empty for market events; non-empty when an `AckToStrategy`
    /// fires). The caller must have already confirmed the sim is the earliest
    /// source (i.e. `peek_when()` ≤ its strat-lane time).
    pub fn step(&mut self) -> Vec<OrderUpdate> {
        let feed_when = self.feed.peek_when();
        let sched_when = self.sched.peek_when();
        let take_feed = match (feed_when, sched_when) {
            // Tie → market event first, so an order reaching at the same ns
            // matches against the freshly-applied book.
            (Some(f), Some(s)) => f <= s,
            (Some(_), None) => true,
            _ => false,
        };
        if take_feed {
            self.step_feed()
        } else {
            self.step_sched()
        }
    }

    /// On each binary-option instrument (event start), in "exact" mode swap the
    /// latency sampler's anchors to that event's live RTT shape (or clear to
    /// pooled when the event has no coverage). No-op when no table is loaded.
    fn apply_per_event_rtt(&mut self, inst: &Instrument) {
        if self.per_event_rtt.is_none() {
            return;
        }
        let Instrument::BinaryOption(bo) = inst else {
            return;
        };
        let Some(secs) = parse_event_start_ts_secs(&bo.event_start_time) else {
            return;
        };
        let entry = self
            .per_event_rtt
            .as_ref()
            .and_then(|t| t.get(&secs).copied());
        match entry {
            Some(e) => self.latency.apply_per_event_override(&e, secs),
            None => self.latency.clear_per_event_override(),
        }
    }

    /// Apply the causal RTT state once per 5-minute event.  A missing state
    /// explicitly falls back to the configured fixed windows.
    fn apply_dynamic_taker_windows(&mut self, inst: &Instrument) {
        if self.dynamic_window_rtt_by_event.is_none() {
            return;
        }
        let Instrument::BinaryOption(bo) = inst else {
            return;
        };
        let Some(secs) = parse_event_start_ts_secs(&bo.event_start_time) else {
            return;
        };
        if self.last_dynamic_window_event == Some(secs) {
            return;
        }
        self.last_dynamic_window_event = Some(secs);
        let state = self
            .dynamic_window_rtt_by_event
            .as_ref()
            .and_then(|t| t.get(&secs).copied());
        let Some(rtt_ms) = state else {
            self.taker_race_horizon_ns = self.base_taker_race_horizon_ns;
            self.core
                .configure_taker_comp(self.taker_comp_rate, self.base_taker_comp_window_ns);
            return;
        };
        let (race_ns, race_mult) = dynamic_window_ns(
            self.base_taker_race_horizon_ns,
            rtt_ms,
            self.dynamic_window_rtt_ref_ms,
            self.dynamic_race_rtt_elasticity,
            self.dynamic_window_min_mult,
            self.dynamic_window_max_mult,
        );
        let (comp_ns, comp_mult) = dynamic_window_ns(
            self.base_taker_comp_window_ns,
            rtt_ms,
            self.dynamic_window_rtt_ref_ms,
            self.dynamic_comp_rtt_elasticity,
            self.dynamic_window_min_mult,
            self.dynamic_window_max_mult,
        );
        self.taker_race_horizon_ns = race_ns;
        self.core
            .configure_taker_comp(self.taker_comp_rate, comp_ns);
        self.dynamic_window_n += 1;
        self.dynamic_window_rtt_sum_ms += rtt_ms;
        self.dynamic_race_window_sum_ns += race_ns as u128;
        self.dynamic_comp_window_sum_ns += comp_ns as u128;
        self.dynamic_window_mult_min = self.dynamic_window_mult_min.min(race_mult).min(comp_mult);
        self.dynamic_window_mult_max = self.dynamic_window_mult_max.max(race_mult).max(comp_mult);
    }

    /// Apply event-specific overhead anchors without resetting the RNG/AR state.
    /// Missing coverage falls back explicitly to the configured fixed CDF.
    fn apply_dynamic_taker_overhead(&mut self, inst: &Instrument) {
        if self.dynamic_taker_overhead_by_event.is_none() {
            return;
        }
        let Instrument::BinaryOption(bo) = inst else {
            return;
        };
        let Some(secs) = parse_event_start_ts_secs(&bo.event_start_time) else {
            return;
        };
        if self.last_dynamic_overhead_event == Some(secs) {
            return;
        }
        self.last_dynamic_overhead_event = Some(secs);
        match self
            .dynamic_taker_overhead_by_event
            .as_ref()
            .and_then(|t| t.get(&secs).copied())
        {
            Some((p50, p95, p99)) => {
                self.latency.apply_taker_overhead_override(p50, p95, p99);
                self.dynamic_overhead_n += 1;
                self.dynamic_overhead_p50_sum_ms += p50;
                self.dynamic_overhead_p95_sum_ms += p95;
                self.dynamic_overhead_p99_sum_ms += p99;
            }
            None => self.latency.clear_taker_overhead_override(),
        }
    }

    fn observe_markout_instrument(&mut self, inst: &Instrument) {
        if !self.dynamic_fill_markout || self.dynamic_markout_spot_vol {
            return;
        }
        let Instrument::BinaryOption(bo) = inst else {
            return;
        };
        if bo.clob_token_ids.is_empty() {
            return;
        }
        let canon = self.core.canonical_token(&bo.clob_token_ids[0]);
        self.markout_tick_by_symbol
            .insert(canon.clone(), bo.tick_size.max(1e-6));
        if !self
            .markout_symbol_fifo
            .iter()
            .any(|symbol| symbol == &canon)
        {
            self.markout_symbol_fifo.push_back(canon);
        }
        // Mirror the matching core's bounded event retention. Old event tokens
        // never reappear, so retaining their 5-second book histories would make
        // memory grow linearly in a long backtest.
        while self.markout_symbol_fifo.len() > 16 {
            if let Some(old) = self.markout_symbol_fifo.pop_front() {
                self.markout_mid_history.remove(&old);
                self.markout_last_book_ts.remove(&old);
                self.markout_tick_by_symbol.remove(&old);
            }
        }
    }

    /// Record a canonical mid from a server-axis book snapshot. The history is
    /// causal: it is updated only when the book event reaches the simulator.
    fn observe_markout_book(&mut self, ob: &crate::types::OrderBookSnapshot) {
        if !self.dynamic_fill_markout || self.dynamic_markout_spot_vol {
            return;
        }
        let finite = |l: &&crate::types::PriceLevel| {
            l.quantity > 0.0 && l.price.is_finite() && l.price > 0.0 && l.price < 1.0
        };
        let bid = ob
            .bids
            .iter()
            .filter(finite)
            .map(|l| l.price)
            .fold(f64::NEG_INFINITY, f64::max);
        let ask = ob
            .asks
            .iter()
            .filter(finite)
            .map(|l| l.price)
            .fold(f64::INFINITY, f64::min);
        if !bid.is_finite() || !ask.is_finite() {
            return;
        }
        let canon = self.core.canonical_token(&ob.symbol);
        let ts = ob.exchange_timestamp_ns;
        if self
            .markout_last_book_ts
            .get(&canon)
            .is_some_and(|last| ts < *last)
        {
            return;
        }
        self.markout_last_book_ts.insert(canon.clone(), ts);
        let raw_mid = 0.5 * (bid + ask);
        let mid = if self.core.fold_on() && canon != ob.symbol {
            1.0 - raw_mid
        } else {
            raw_mid
        };
        let cutoff = ts.saturating_sub(self.dynamic_markout_lookback_ns);
        let history = self.markout_mid_history.entry(canon).or_default();
        history.push_back((ts, mid));
        while history
            .front()
            .is_some_and(|(front_ts, _)| *front_ts < cutoff)
        {
            history.pop_front();
        }
    }

    /// Observe the Binance BTCUSDT BBO on the strategy-visible clock and build
    /// one-second closes. The state is realised volatility in basis points:
    /// `sqrt(sum(log(close_t / close_t-1)^2)) * 10_000`.
    ///
    /// Bucketing prevents a change in websocket update cadence from changing
    /// the measured volatility. This method is called only after the snapshot
    /// reaches the strategy lane, so the factor cannot see future spot data.
    pub fn observe_dynamic_markout_spot_book(
        &mut self,
        ob: &crate::types::OrderBookSnapshot,
        observed_at: u64,
    ) {
        if !self.dynamic_fill_markout
            || !self.dynamic_markout_spot_vol
            || ob.exchange != Exchange::Binance
            || !ob.symbol.eq_ignore_ascii_case("BTCUSDT")
        {
            return;
        }
        let best_bid = ob
            .bids
            .iter()
            .filter(|l| l.quantity > 0.0 && l.price.is_finite() && l.price > 0.0)
            .map(|l| l.price)
            .fold(f64::NEG_INFINITY, f64::max);
        let best_ask = ob
            .asks
            .iter()
            .filter(|l| l.quantity > 0.0 && l.price.is_finite() && l.price > 0.0)
            .map(|l| l.price)
            .fold(f64::INFINITY, f64::min);
        if !best_bid.is_finite() || !best_ask.is_finite() || best_bid > best_ask {
            return;
        }
        let price = 0.5 * (best_bid + best_ask);
        let second = (observed_at / 1_000_000_000) * 1_000_000_000;
        let len = self.markout_spot_rv.len();
        if let Some(&(last_second, last_price, last_cum_sq)) = self.markout_spot_rv.back() {
            if second < last_second {
                return;
            }
            if second == last_second {
                let (prev_price, prev_cum_sq) = if len >= 2 {
                    let &(_, p, c) = self.markout_spot_rv.get(len - 2).unwrap();
                    (p, c)
                } else {
                    (price, 0.0)
                };
                let ret = (price / prev_price).ln();
                if let Some(last) = self.markout_spot_rv.back_mut() {
                    *last = (second, price, prev_cum_sq + ret * ret);
                }
            } else {
                let ret = (price / last_price).ln();
                self.markout_spot_rv
                    .push_back((second, price, last_cum_sq + ret * ret));
            }
        } else {
            self.markout_spot_rv.push_back((second, price, 0.0));
        }

        // Keep one point at or immediately before the cutoff as the return
        // anchor; all later state reads are then constant-time.
        let cutoff = observed_at.saturating_sub(self.dynamic_markout_lookback_ns);
        while self.markout_spot_rv.len() > 2
            && self
                .markout_spot_rv
                .get(1)
                .is_some_and(|(ts, _, _)| *ts <= cutoff)
        {
            self.markout_spot_rv.pop_front();
        }
    }

    /// Observe a full-book snapshot on the local/strategy clock. Kept separate
    /// from the server feed so the stale gate cannot see a future receive time.
    pub fn observe_local_orderbook(
        &mut self,
        ob: &crate::types::OrderBookSnapshot,
        observed_at: u64,
    ) {
        self.core.on_local_orderbook(ob, observed_at);
    }

    fn spot_realised_vol_bps(&mut self, when: u64) -> Option<f64> {
        let cutoff = when.saturating_sub(self.dynamic_markout_lookback_ns);
        while self.markout_spot_rv.len() > 2
            && self
                .markout_spot_rv
                .get(1)
                .is_some_and(|(ts, _, _)| *ts <= cutoff)
        {
            self.markout_spot_rv.pop_front();
        }
        let first = self.markout_spot_rv.front()?;
        let last = self.markout_spot_rv.back()?;
        (last.0 > first.0).then(|| (last.2 - first.2).max(0.0).sqrt() * 10_000.0)
    }

    /// Set `fill_markout_vn` immediately before a trade is matched, using only
    /// canonical mids already observed in the preceding lookback window.
    fn apply_dynamic_markout(&mut self, trade: &crate::types::TradeTick, when: u64) {
        if !self.dynamic_fill_markout {
            return;
        }
        let state = if self.dynamic_markout_spot_vol {
            self.spot_realised_vol_bps(when)
        } else {
            let canon = self.core.canonical_token(&trade.symbol);
            let cutoff = when.saturating_sub(self.dynamic_markout_lookback_ns);
            self.markout_mid_history.get_mut(&canon).and_then(|history| {
                while history
                    .front()
                    .is_some_and(|(front_ts, _)| *front_ts < cutoff)
                {
                    history.pop_front();
                }
                if history.is_empty() {
                    return None;
                }
                let (lo, hi) = history
                    .iter()
                    .fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), (_, mid)| {
                        (lo.min(*mid), hi.max(*mid))
                    });
                let tick = self
                    .markout_tick_by_symbol
                    .get(&canon)
                    .copied()
                    .unwrap_or(0.01)
                    .max(1e-6);
                Some(1.0 + (hi - lo).max(0.0) / tick)
            })
        };
        let Some(vol_state) = state else {
            self.core
                .configure_fill_markout_vn(self.base_fill_markout_vn);
            return;
        };
        let (vn, _) = dynamic_markout_strength(
            self.base_fill_markout_vn,
            vol_state,
            self.dynamic_markout_vol_ref_ticks,
            self.dynamic_markout_vol_elasticity,
            self.dynamic_markout_min_mult,
            self.dynamic_markout_max_mult,
        );
        self.core.configure_fill_markout_vn(vn);
        self.dynamic_markout_states.push(vol_state as f32);
        self.dynamic_markout_vn_sum += vn;
        self.dynamic_markout_vn_min = self.dynamic_markout_vn_min.min(vn);
        self.dynamic_markout_vn_max = self.dynamic_markout_vn_max.max(vn);
    }

    /// Peek the canonical token's mid `(best_bid+best_ask)/2` from the first book
    /// strictly after `at` — the forward-markout signal for the fill haircut.
    /// `None` if there's no future book or it's one-sided.
    fn peek_fwd_canonical_mid(&self, token: &str, at: u64) -> Option<f64> {
        let canon = self.core.canonical_token(token);
        // Borrowed peek (no clone): markout only reads the BBO to form a mid; the
        // full level vectors are never stored. Identical book selection + mid as
        // the owned `peek_next_book` path.
        let (_, bids, asks) = self.feed.peek_next_book_ref(&canon, at)?;
        let fin = |l: &crate::types::PriceLevel| l.quantity > 0.0 && l.price > 0.0 && l.price < 1.0;
        let best_bid = bids
            .iter()
            .filter(|l| fin(l))
            .map(|l| l.price)
            .fold(f64::NEG_INFINITY, f64::max);
        let best_ask = asks
            .iter()
            .filter(|l| fin(l))
            .map(|l| l.price)
            .fold(f64::INFINITY, f64::min);
        (best_bid.is_finite() && best_ask.is_finite()).then(|| 0.5 * (best_bid + best_ask))
    }

    fn observe_causal_taker_races(&mut self, now_ns: u64) {
        let core = &self.core;
        for pending in self.pending_taker_races.values_mut() {
            if now_ns > pending.observe_until_ns {
                continue;
            }
            let available = core.taker_available_qty(&pending.order);
            pending.min_available_qty = match pending.min_available_qty {
                Some(minimum) => Some(minimum.min(available)),
                None if available > 0.0 => Some(available),
                None => None,
            };
        }
    }

    /// Start the causal race clock when the client emits a place, not when the
    /// order reaches the exchange. The outbound transit is the main interval
    /// in which another taker can consume the touch that triggered our order.
    fn begin_causal_taker_race(&mut self, order: &OrderRequest, t_emit: u64) {
        if !self.causal_matching || !self.core.taker_race_enabled() {
            return;
        }
        let available = self.core.taker_available_qty(order);
        self.pending_taker_races.insert(
            order.client_order_id.clone(),
            PendingTakerRace {
                order: order.clone(),
                min_available_qty: (available > 0.0).then_some(available),
                observe_until_ns: t_emit.saturating_add(self.taker_race_horizon_ns),
            },
        );
    }

    fn step_feed(&mut self) -> Vec<OrderUpdate> {
        if let Some((when, ev)) = self.feed.next_server_event() {
            match ev {
                SimEvent::ServerBook(ob) => {
                    // Book-through adverse fills (a resting order the contra just
                    // swept through) surface here, delivered like trade fills
                    // after a ws fill-push delay. Empty unless book_through_rate>0.
                    let fwd_mid = if self.book_markout_on {
                        self.peek_fwd_canonical_mid(
                            &ob.symbol,
                            when.saturating_add(self.fill_markout_horizon_ns),
                        )
                    } else {
                        None
                    };
                    let fills = self.core.on_orderbook_fwd(&ob, fwd_mid);
                    self.observe_causal_taker_races(when);
                    self.observe_markout_book(&ob);
                    for mut fill in fills {
                        let push = self.latency.sample_fill_push(when);
                        let deliver = when.saturating_add(push);
                        self.core
                            .record_fill_delivery(&fill.client_order_id, deliver);
                        fill.timestamp_ns = deliver;
                        self.sched.push(deliver, SimEvent::FillToStrategy(fill));
                    }
                }
                SimEvent::ServerTrade(t) => {
                    // P3: maker fills from queue drain. Each fill is pushed back
                    // to the strategy after a ws fill-push delay (sampled once
                    // per fill), so it surfaces via FillToStrategy later.
                    // Forward-markout haircut: peek the canonical mid `horizon`
                    // past the trade so the core can downweight favorable fills.
                    self.apply_dynamic_markout(&t, when);
                    let fwd_mid = if self.markout_on {
                        self.peek_fwd_canonical_mid(
                            &t.symbol,
                            when.saturating_add(self.fill_markout_horizon_ns),
                        )
                    } else {
                        None
                    };
                    let fills = self.core.on_trade_tick_fwd(&t, fwd_mid);
                    for mut fill in fills {
                        let push = self.latency.sample_fill_push(when);
                        let deliver = when.saturating_add(push);
                        self.core
                            .record_fill_delivery(&fill.client_order_id, deliver);
                        fill.timestamp_ns = deliver;
                        self.sched.push(deliver, SimEvent::FillToStrategy(fill));
                    }
                }
                SimEvent::ServerInstrument(i) => {
                    self.core.on_instrument(&i);
                    self.observe_markout_instrument(&i);
                    self.apply_per_event_rtt(&i);
                    self.apply_dynamic_taker_windows(&i);
                    self.apply_dynamic_taker_overhead(&i);
                }
                SimEvent::ServerTickSize(tsc) => self.core.on_tick_size_change(&tsc),
                _ => {}
            }
        }
        Vec::new()
    }

    /// Schedule an ack for strategy delivery at `deliver`. Under timeout
    /// (suppress_ack) the strategy already got a *Timeout and will reconcile —
    /// suppress Accepted/Rejected/Cancelled, but ALWAYS deliver fills.
    fn deliver_ack(&mut self, mut u: OrderUpdate, deliver: u64, suppress_ack: bool) {
        let is_fill = matches!(u.status, OrderStatus::Filled | OrderStatus::PartiallyFilled);
        if !suppress_ack || is_fill {
            u.timestamp_ns = deliver;
            self.sched.push(deliver, SimEvent::AckToStrategy(u));
        }
    }

    fn step_sched(&mut self) -> Vec<OrderUpdate> {
        let Some((when, ev)) = self.sched.pop() else {
            return Vec::new();
        };
        match ev {
            SimEvent::OrderReachesEngine {
                action,
                l2_ns,
                suppress_ack,
            } => {
                // core uses `when` (server time) for matching + recent_fills.
                match action {
                    ReachAction::Place(o) => {
                        if self.core.would_cross(&o, when) {
                            // Genuine taker: defer the actual book-match to the
                            // MIDPOINT of the matching window (reach + overhead/2)
                            // so the book can move in-flight (natural taker miss).
                            let overhead = self.latency.sample_taker_overhead(when);
                            let match_at = when.saturating_add(overhead / 2);
                            if self.causal_matching && self.core.taker_race_enabled() {
                                let current = self.core.taker_available_qty(&o);
                                let pending = self
                                    .pending_taker_races
                                    .entry(o.client_order_id.clone())
                                    .or_insert_with(|| PendingTakerRace {
                                        min_available_qty: (current > 0.0).then_some(current),
                                        order: o.clone(),
                                        observe_until_ns: when
                                            .saturating_add(self.taker_race_horizon_ns),
                                    });
                                pending.observe_until_ns = pending.observe_until_ns.min(match_at);
                                if when <= pending.observe_until_ns && current > 0.0 {
                                    pending.min_available_qty = Some(
                                        pending
                                            .min_available_qty
                                            .map_or(current, |minimum| minimum.min(current)),
                                    );
                                }
                            }
                            self.sched.push(
                                match_at,
                                SimEvent::TakerMatch {
                                    order: o,
                                    l2_ns,
                                    overhead_ns: overhead,
                                    suppress_ack,
                                },
                            );
                        } else {
                            self.pending_taker_races.remove(&o.client_order_id);
                            // Maker race: peek the queue `maker_race_horizon` ahead
                            // (the book the resting order faces shortly after entry)
                            // for the q_ahead-init blend.
                            self.prime_next_books(&o.symbol, when, self.maker_race_horizon_ns);
                            let u = self.core.submit_order(&o, when);
                            self.deliver_ack(u, when.saturating_add(l2_ns), suppress_ack);
                        }
                    }
                    ReachAction::Cancel {
                        exchange,
                        client_order_id,
                    } => {
                        let ack_deliver_ns = when.saturating_add(l2_ns);
                        let delay_ns = ((l2_ns as f64) * self.cancel_finality_delay_frac)
                            .round()
                            .clamp(0.0, u64::MAX as f64) as u64;
                        if delay_ns == 0 {
                            let u = self.core.cancel_order(exchange, &client_order_id, when);
                            self.deliver_ack(u, ack_deliver_ns, suppress_ack);
                        } else {
                            self.cancel_finality_delayed += 1;
                            self.sched.push(
                                when.saturating_add(delay_ns),
                                SimEvent::CancelFinalizes {
                                    exchange,
                                    client_order_id,
                                    ack_deliver_ns,
                                    suppress_ack,
                                },
                            );
                        }
                    }
                    ReachAction::CancelAll { exchange, symbol } => {
                        let d = when.saturating_add(l2_ns);
                        for u in self.core.cancel_all(exchange, &symbol, when) {
                            self.deliver_ack(u, d, suppress_ack);
                        }
                    }
                }
                Vec::new()
            }
            SimEvent::CancelFinalizes {
                exchange,
                client_order_id,
                ack_deliver_ns,
                suppress_ack,
            } => {
                let u = self.core.cancel_order(exchange, &client_order_id, when);
                if u.status == OrderStatus::Filled {
                    self.cancel_finality_matched += 1;
                }
                self.deliver_ack(u, ack_deliver_ns.max(when), suppress_ack);
                Vec::new()
            }
            SimEvent::TakerMatch {
                order,
                l2_ns,
                overhead_ns,
                suppress_ack,
            } => {
                // Re-match against the (now possibly moved) book: still crossing
                // → taker fill; moved away → rests (miss) or cancels per type.
                // Causal mode consumes the minimum volume observed since this
                // order was emitted by the client; legacy mode retains its
                // post-match lookahead for byte-compatible experiments.
                let causal_race_cap = if self.causal_matching {
                    self.core.clear_next_books();
                    self.pending_taker_races
                        .remove(&order.client_order_id)
                        .and_then(|pending| pending.min_available_qty)
                } else {
                    self.prime_taker_window(&order.symbol, when, self.taker_race_horizon_ns);
                    None
                };
                let u = self
                    .core
                    .submit_order_with_taker_race_cap(&order, when, causal_race_cap);
                let is_fill =
                    matches!(u.status, OrderStatus::Filled | OrderStatus::PartiallyFilled);
                // Filled taker: residual overhead/2 + L2 to the ack. Missed→rest:
                // just L2 (a resting order doesn't traverse the matching engine).
                let deliver = if is_fill {
                    when.saturating_add(overhead_ns / 2).saturating_add(l2_ns)
                } else {
                    when.saturating_add(l2_ns)
                };
                self.deliver_ack(u, deliver, suppress_ack);
                Vec::new()
            }
            SimEvent::AckToStrategy(u) => vec![u],
            SimEvent::FillToStrategy(u) => vec![u],
            // Server-axis events never enter the scheduler heap.
            _ => Vec::new(),
        }
    }

    /// Schedule a strategy signal's outbound effect. Samples one RTT for the
    /// (single-API-call) signal, schedules `OrderReachesEngine` at `emit + L1`,
    /// and stashes `L2` for the eventual ack delivery.
    pub fn submit(&mut self, sig: &Signal, t_emit: u64) {
        // Reconcile: resolve orphans against current core state; deliver after a
        // (cancel-side) round trip.
        if let Signal::ReconcilePolymarket {
            pending_places,
            pending_cancels,
            ..
        } = sig
        {
            let (l1, l2) = self.latency.sample_cancel_split(t_emit);
            let deliver = t_emit.saturating_add(l1).saturating_add(l2);
            for u in self
                .core
                .reconcile(pending_places, pending_cancels, deliver)
            {
                self.sched.push(deliver, SimEvent::AckToStrategy(u));
            }
            return;
        }

        let (actions, cancel_only) = expand_signal(sig);
        if actions.is_empty() {
            return;
        }
        if self.use_batch_orders {
            // Batched: the whole signal is ONE API call (Polymarket
            // `/orders` or `/orders/cancel`) sharing a single RTT draw.
            let (l1, l2) = if cancel_only {
                self.latency.sample_cancel_split(t_emit)
            } else {
                self.latency.sample_place_split(t_emit)
            };
            for action in actions {
                self.dispatch_action(action, t_emit, l1, l2);
            }
        } else {
            // use_batch_orders=false: each place / cancel is its OWN
            // single-endpoint call with its OWN RTT + timeout, mirroring the
            // live executor's concurrent `POST /order` / `DELETE /order`
            // fan-out (trade.rs). Crucially, the cancel actions of a reprice
            // `BatchUpdateOrders` now sample the CANCEL RTT instead of being
            // glued to the batch's place RTT — so they can time out at the
            // cancel rate (live ~1.3 %/cancel). Concurrent calls ⇒ each
            // sampled at the same t_emit.
            for action in actions {
                let (l1, l2) = if action_is_cancel(&action) {
                    self.latency.sample_cancel_split(t_emit)
                } else {
                    self.latency.sample_place_split(t_emit)
                };
                self.dispatch_action(action, t_emit, l1, l2);
            }
        }
    }

    /// Schedule one action's engine-reach event and, when its round trip
    /// `l1 + l2` exceeds `client_timeout`, the suppressed-ack `*Timeout`
    /// delivered to the strategy. Shared by the batched (one RTT) and
    /// split (per-action RTT) dispatch paths.
    fn dispatch_action(&mut self, action: ReachAction, t_emit: u64, l1: u64, l2: u64) {
        if let ReachAction::Place(order) = &action {
            self.begin_causal_taker_race(order, t_emit);
        }
        let rtt = l1 + l2;
        let timed_out = rtt > self.client_timeout_ns;
        let reach = t_emit.saturating_add(l1);
        if timed_out {
            self.timeouts += 1;
            let timeout_deliver = t_emit.saturating_add(self.client_timeout_ns);
            if let Some(u) = self.timeout_update(&action, timeout_deliver) {
                self.sched.push(timeout_deliver, SimEvent::AckToStrategy(u));
            }
        }
        self.sched.push(
            reach,
            SimEvent::OrderReachesEngine {
                action,
                l2_ns: l2,
                suppress_ack: timed_out,
            },
        );
    }

    /// Build the *Timeout ack delivered to the strategy when the round trip
    /// exceeds client_timeout (the order still reaches the engine separately).
    fn timeout_update(&self, action: &ReachAction, ts: u64) -> Option<OrderUpdate> {
        let (coid, symbol, side, status, remaining, oid) = match action {
            ReachAction::Place(o) => (
                o.client_order_id.clone(),
                o.symbol.clone(),
                o.side,
                OrderStatus::NewOrderTimeout,
                o.quantity,
                // A new order has no exchange order id yet (matches live; the
                // NewOrderTimeout handler doesn't need one).
                None,
            ),
            ReachAction::Cancel {
                client_order_id, ..
            } => {
                let (symbol, side) = self
                    .core
                    .order_symbol_side(client_order_id)
                    .unwrap_or_else(|| (String::new(), Side::Buy));
                // CRITICAL: the strategy's CancelOrderTimeout handler only
                // logs + reconciles when `exchange_order_id` is `Some` (it
                // re-queries the order by id). In live a cancel always carries
                // the resting order's id; mirror that with the sim's synthetic
                // `simv2-{coid}` convention (see exchange.rs fills/accepts).
                // Without this the strategy silently drops every sim cancel
                // timeout → cancel timeouts never surface.
                (
                    client_order_id.clone(),
                    symbol,
                    side,
                    OrderStatus::CancelOrderTimeout,
                    0.0,
                    Some(format!("simv2-{client_order_id}")),
                )
            }
            // Cancel-all timeouts aren't modelled (rare; emergency path).
            ReachAction::CancelAll { .. } => return None,
        };
        Some(OrderUpdate {
            client_order_id: coid,
            exchange: Exchange::Polymarket,
            symbol,
            side,
            exchange_order_id: oid,
            status,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: remaining,
            avg_fill_price: 0.0,
            timestamp_ns: ts,
            exchange_event_timestamp_ns: None,
            trade_id: None,
            order_audit: None,
            error: None,
            order_slot: Default::default(),
        })
    }
}

/// True when a reach action is a cancel (vs a place). Used by the
/// `use_batch_orders=false` split path to pick the cancel vs place RTT
/// sampler per action.
fn action_is_cancel(a: &ReachAction) -> bool {
    matches!(
        a,
        ReachAction::Cancel { .. } | ReachAction::CancelAll { .. }
    )
}

/// Expand a `Signal` into reach actions + whether it is a cancel-only signal
/// (chooses the cancel vs place RTT sampler). Batches expand into several
/// actions sharing one sampled RTT (a batch is a single API call).
fn expand_signal(sig: &Signal) -> (Vec<ReachAction>, bool) {
    use crate::types::Exchange;
    match sig {
        Signal::NewOrder(o) => (vec![ReachAction::Place(o.clone())], false),
        Signal::CancelOrder {
            exchange,
            client_order_id,
            ..
        } => (
            vec![ReachAction::Cancel {
                exchange: *exchange,
                client_order_id: client_order_id.clone(),
            }],
            true,
        ),
        Signal::CancelAll {
            exchange, symbol, ..
        } => (
            vec![ReachAction::CancelAll {
                exchange: *exchange,
                symbol: symbol.clone(),
            }],
            true,
        ),
        Signal::BatchNewOrders { orders, .. } => (
            orders.iter().cloned().map(ReachAction::Place).collect(),
            false,
        ),
        Signal::BatchCancelOrders {
            exchange,
            client_order_ids,
            ..
        } => (
            client_order_ids
                .iter()
                .map(|c| ReachAction::Cancel {
                    exchange: *exchange,
                    client_order_id: c.clone(),
                })
                .collect(),
            true,
        ),
        Signal::BatchUpdateOrders {
            exchange,
            cancel_client_order_ids,
            place_orders,
            ..
        }
        | Signal::ReplaceOrder {
            exchange,
            cancel_client_order_ids,
            place_orders,
            ..
        } => {
            // Cancel BEFORE place: a same-token reprice must free the old
            // resting order's share/cash lock before the replacement tries to
            // rest, else the place sees the old order still locking inventory
            // and gets a spurious "insufficient shares (rest sell)" reject.
            // Under use_batch_orders=true the whole batch shares one reach
            // time, so emission order == processing order (the scheduler breaks
            // equal-`when` ties by insertion `seq`). (was: place-before-cancel)
            let mut actions: Vec<ReachAction> = cancel_client_order_ids
                .iter()
                .map(|c| ReachAction::Cancel {
                    exchange: *exchange,
                    client_order_id: c.clone(),
                })
                .collect();
            actions.extend(place_orders.iter().cloned().map(ReachAction::Place));
            (actions, false)
        }
        Signal::PolymarketCancelAllOrders { .. } => (
            vec![ReachAction::CancelAll {
                exchange: Exchange::Polymarket,
                symbol: String::new(),
            }],
            true,
        ),
        // P1: orphan reconcile has no effect (no timeouts generated); Exit is a
        // no-op for the sim.
        Signal::ReconcilePolymarket { .. }
        | Signal::RetainPolymarketEventAudit { .. }
        | Signal::RetirePolymarketEventAudit { .. }
        | Signal::BeginShutdown
        | Signal::Exit => (Vec::new(), false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::sim::latency::LatencyProfile;
    use crate::types::{
        Exchange, OrderBookSnapshot, OrderRequest, OrderStatus, PriceLevel, Side, TradeTick,
    };

    /// Build a Simulator with an empty feed and a deterministic fixed RTT so
    /// ack-delivery timing is exact.
    fn sim_with_fixed_rtt(rtt_ms: u64) -> Simulator {
        let feed = ServerFeed::new(
            Path::new("/nonexistent"),
            &[],
            DateTime::<Utc>::from_timestamp(0, 0).unwrap(),
            DateTime::<Utc>::from_timestamp(1, 0).unwrap(),
        )
        .unwrap();
        let latency = LatencyModel::new(
            LatencyProfile::Fixed(rtt_ms),
            LatencyProfile::Fixed(rtt_ms),
            0.0,
            1,
        );
        Simulator {
            sched: Scheduler::new(),
            feed,
            core: SimExchangeV2::new(
                500_000_000,
                std::collections::HashMap::new(),
                std::collections::HashMap::new(),
            ),
            latency,
            client_timeout_ns: 500_000_000,
            timeouts: 0,
            cancel_finality_delay_frac: 0.0,
            cancel_finality_delayed: 0,
            cancel_finality_matched: 0,
            per_event_rtt: None,
            dynamic_taker_overhead_by_event: None,
            last_dynamic_overhead_event: None,
            dynamic_overhead_n: 0,
            dynamic_overhead_p50_sum_ms: 0.0,
            dynamic_overhead_p95_sum_ms: 0.0,
            dynamic_overhead_p99_sum_ms: 0.0,
            race_enabled: false,
            causal_matching: false,
            pending_taker_races: HashMap::new(),
            maker_race_horizon_ns: 0,
            taker_race_horizon_ns: 0,
            base_taker_race_horizon_ns: 0,
            base_taker_comp_window_ns: 0,
            taker_comp_rate: 0.0,
            dynamic_window_rtt_by_event: None,
            dynamic_window_rtt_ref_ms: 60.0,
            dynamic_race_rtt_elasticity: 0.0,
            dynamic_comp_rtt_elasticity: 0.0,
            dynamic_window_min_mult: 0.5,
            dynamic_window_max_mult: 2.0,
            last_dynamic_window_event: None,
            dynamic_window_n: 0,
            dynamic_window_rtt_sum_ms: 0.0,
            dynamic_race_window_sum_ns: 0,
            dynamic_comp_window_sum_ns: 0,
            dynamic_window_mult_min: f64::INFINITY,
            dynamic_window_mult_max: f64::NEG_INFINITY,
            use_batch_orders: true,
            fill_markout_horizon_ns: 0,
            markout_on: false,
            book_markout_on: false,
            base_fill_markout_vn: 0.0,
            dynamic_fill_markout: false,
            dynamic_markout_spot_vol: false,
            dynamic_markout_lookback_ns: 5_000_000_000,
            dynamic_markout_vol_ref_ticks: 1.0,
            dynamic_markout_vol_elasticity: 0.0,
            dynamic_markout_min_mult: 0.5,
            dynamic_markout_max_mult: 2.0,
            markout_mid_history: HashMap::new(),
            markout_last_book_ts: HashMap::new(),
            markout_tick_by_symbol: HashMap::new(),
            markout_symbol_fifo: VecDeque::new(),
            markout_spot_rv: VecDeque::new(),
            dynamic_markout_states: Vec::new(),
            dynamic_markout_vn_sum: 0.0,
            dynamic_markout_vn_min: f64::INFINITY,
            dynamic_markout_vn_max: f64::NEG_INFINITY,
        }
    }

    /// Simulator with distinct fixed place / cancel RTTs and an explicit
    /// `use_batch_orders` flag — for exercising the split-dispatch path.
    fn sim_split_rtt(place_ms: u64, cancel_ms: u64, use_batch_orders: bool) -> Simulator {
        let feed = ServerFeed::new(
            Path::new("/nonexistent"),
            &[],
            DateTime::<Utc>::from_timestamp(0, 0).unwrap(),
            DateTime::<Utc>::from_timestamp(1, 0).unwrap(),
        )
        .unwrap();
        let latency = LatencyModel::new(
            LatencyProfile::Fixed(place_ms),
            LatencyProfile::Fixed(cancel_ms),
            0.0,
            1,
        );
        Simulator {
            sched: Scheduler::new(),
            feed,
            core: SimExchangeV2::new(
                500_000_000,
                std::collections::HashMap::new(),
                std::collections::HashMap::new(),
            ),
            latency,
            client_timeout_ns: 500_000_000,
            timeouts: 0,
            cancel_finality_delay_frac: 0.0,
            cancel_finality_delayed: 0,
            cancel_finality_matched: 0,
            per_event_rtt: None,
            dynamic_taker_overhead_by_event: None,
            last_dynamic_overhead_event: None,
            dynamic_overhead_n: 0,
            dynamic_overhead_p50_sum_ms: 0.0,
            dynamic_overhead_p95_sum_ms: 0.0,
            dynamic_overhead_p99_sum_ms: 0.0,
            race_enabled: false,
            causal_matching: false,
            pending_taker_races: HashMap::new(),
            maker_race_horizon_ns: 0,
            taker_race_horizon_ns: 0,
            base_taker_race_horizon_ns: 0,
            base_taker_comp_window_ns: 0,
            taker_comp_rate: 0.0,
            dynamic_window_rtt_by_event: None,
            dynamic_window_rtt_ref_ms: 60.0,
            dynamic_race_rtt_elasticity: 0.0,
            dynamic_comp_rtt_elasticity: 0.0,
            dynamic_window_min_mult: 0.5,
            dynamic_window_max_mult: 2.0,
            last_dynamic_window_event: None,
            dynamic_window_n: 0,
            dynamic_window_rtt_sum_ms: 0.0,
            dynamic_race_window_sum_ns: 0,
            dynamic_comp_window_sum_ns: 0,
            dynamic_window_mult_min: f64::INFINITY,
            dynamic_window_mult_max: f64::NEG_INFINITY,
            use_batch_orders,
            fill_markout_horizon_ns: 0,
            markout_on: false,
            book_markout_on: false,
            base_fill_markout_vn: 0.0,
            dynamic_fill_markout: false,
            dynamic_markout_spot_vol: false,
            dynamic_markout_lookback_ns: 5_000_000_000,
            dynamic_markout_vol_ref_ticks: 1.0,
            dynamic_markout_vol_elasticity: 0.0,
            dynamic_markout_min_mult: 0.5,
            dynamic_markout_max_mult: 2.0,
            markout_mid_history: HashMap::new(),
            markout_last_book_ts: HashMap::new(),
            markout_tick_by_symbol: HashMap::new(),
            markout_symbol_fifo: VecDeque::new(),
            markout_spot_rv: VecDeque::new(),
            dynamic_markout_states: Vec::new(),
            dynamic_markout_vn_sum: 0.0,
            dynamic_markout_vn_min: f64::INFINITY,
            dynamic_markout_vn_max: f64::NEG_INFINITY,
        }
    }

    fn reprice_signal(cancel_coid: &str, place_coid: &str) -> Signal {
        Signal::BatchUpdateOrders {
            exchange: Exchange::Polymarket,
            market_id: String::new(),
            cancel_client_order_ids: [cancel_coid.to_string()].into_iter().collect(),
            place_orders: [match place_signal(place_coid) {
                Signal::NewOrder(o) => o,
                _ => unreachable!(),
            }]
            .into_iter()
            .collect(),
            timestamp_ns: 0,
            instance_id: String::new(),
        }
    }

    /// **use_batch_orders=false splits the batch per action.** A reprice
    /// (place + cancel) where place RTT is fast (no timeout) but cancel RTT
    /// is slow (> client_timeout) must yield a CancelOrderTimeout and NO
    /// NewOrderTimeout — the cancel sampled its OWN (cancel) RTT. With
    /// batching ON the whole reprice shares the fast PLACE RTT → 0 timeouts,
    /// which is exactly why batched sim never produces cancel timeouts.
    #[test]
    fn split_dispatch_routes_cancel_to_cancel_rtt() {
        // place RTT 100ms (ok), cancel RTT 1200ms (> 500ms timeout).
        let mut split = sim_split_rtt(100, 1200, false);
        split.submit(&reprice_signal("old", "new"), 1_000_000_000);
        let mut statuses = Vec::new();
        let mut oids = Vec::new();
        while split.peek_when().is_some() {
            for u in split.step() {
                statuses.push((u.client_order_id.clone(), u.status));
                oids.push((u.client_order_id.clone(), u.exchange_order_id.clone()));
            }
        }
        assert!(
            statuses
                .iter()
                .any(|(c, s)| c == "old" && *s == OrderStatus::CancelOrderTimeout),
            "split: cancel must time out on the cancel RTT, got {statuses:?}",
        );
        // The CancelOrderTimeout must carry a non-None exchange_order_id, else
        // the strategy's handler silently drops it.
        assert!(
            oids.iter().any(|(c, oid)| c == "old" && oid.is_some()),
            "cancel timeout must carry exchange_order_id, got {oids:?}",
        );
        assert!(
            !statuses
                .iter()
                .any(|(_, s)| *s == OrderStatus::NewOrderTimeout),
            "split: fast place must NOT time out, got {statuses:?}",
        );
        assert_eq!(split.timeout_stats().0, 1, "exactly one (cancel) timeout");

        // Batched: same RTTs, whole reprice uses the fast place RTT → none.
        let mut batched = sim_split_rtt(100, 1200, true);
        batched.submit(&reprice_signal("old", "new"), 1_000_000_000);
        while batched.peek_when().is_some() {
            for _ in batched.step() {}
        }
        assert_eq!(
            batched.timeout_stats().0,
            0,
            "batched reprice shares the fast place RTT → no timeout"
        );
    }

    fn place_signal(coid: &str) -> Signal {
        Signal::NewOrder(OrderRequest {
            client_order_id: coid.to_string(),
            exchange: Exchange::Polymarket,
            symbol: "tok".into(),
            side: Side::Buy,
            order_type: crate::types::OrderType::Limit,
            price: Some(0.6),
            quantity: 10.0,
            quote_trigger_exchange_timestamp_ns: 0,
            quote_trigger_local_timestamp_ns: 0,
            quote_event_id: String::new(),
            quote_trigger_source: crate::types::QuoteTriggerSource::Unknown,
            timestamp_ns: 0,
            instance_id: String::new(),
            fee_rate_bps: 0,
            post_only: true,
            reduce_only: false,
            outcome_label: String::new(),
            order_slot: Default::default(),
        })
    }

    #[test]
    fn causal_taker_race_observes_outbound_transit() {
        let mut sim = sim_with_fixed_rtt(100);
        sim.causal_matching = true;
        sim.taker_race_horizon_ns = 950_000_000;
        sim.core.configure_race(0.0, 1.0);
        sim.core.on_orderbook(&OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: "tok".into(),
            bids: vec![],
            asks: vec![PriceLevel {
                price: 0.62,
                quantity: 100.0,
            }],
            exchange_timestamp_ns: 1,
            local_timestamp_ns: 1,
        });
        let Signal::NewOrder(mut order) = place_signal("race") else {
            unreachable!()
        };
        order.price = Some(0.7);
        order.post_only = false;
        let emit = 1_000_000_000;
        sim.submit(&Signal::NewOrder(order), emit);

        let pending = sim.pending_taker_races.get("race").unwrap();
        assert_eq!(pending.min_available_qty, Some(100.0));
        assert_eq!(pending.observe_until_ns, emit + 950_000_000);

        // Another taker consumes most of the touch before our L1 reach. The
        // causal cap must see that smaller quantity without future lookahead.
        sim.core.on_orderbook(&OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: "tok".into(),
            bids: vec![],
            asks: vec![PriceLevel {
                price: 0.62,
                quantity: 35.0,
            }],
            exchange_timestamp_ns: emit + 25_000_000,
            local_timestamp_ns: emit + 25_000_000,
        });
        sim.observe_causal_taker_races(emit + 25_000_000);
        assert_eq!(
            sim.pending_taker_races
                .get("race")
                .unwrap()
                .min_available_qty,
            Some(35.0)
        );
        assert_eq!(sim.peek_when(), Some(emit + 50_000_000));
    }

    #[test]
    #[ignore = "focused release benchmark"]
    fn benchmark_causal_taker_race_observation_latency() {
        fn sample(depth: usize, iterations: usize) -> Vec<u64> {
            let mut sim = sim_with_fixed_rtt(100);
            sim.causal_matching = true;
            sim.taker_race_horizon_ns = 950_000_000;
            sim.core.configure_race(0.0, 1.0);
            sim.core.on_orderbook(&OrderBookSnapshot {
                exchange: Exchange::Polymarket,
                symbol: "tok".into(),
                bids: vec![],
                asks: vec![PriceLevel {
                    price: 0.62,
                    quantity: 100.0,
                }],
                exchange_timestamp_ns: 1,
                local_timestamp_ns: 1,
            });
            for index in 0..depth {
                let Signal::NewOrder(mut order) = place_signal(&format!("race-{index}")) else {
                    unreachable!()
                };
                order.price = Some(0.7);
                order.post_only = false;
                sim.submit(&Signal::NewOrder(order), 1_000_000_000);
            }
            assert_eq!(sim.pending_taker_races.len(), depth);

            let mut samples = Vec::with_capacity(iterations);
            for _ in 0..iterations {
                let started = std::time::Instant::now();
                sim.observe_causal_taker_races(1_025_000_000);
                std::hint::black_box(&sim.pending_taker_races);
                samples.push(started.elapsed().as_nanos() as u64);
            }
            samples
        }

        fn describe(depth: usize, mut samples: Vec<u64>) {
            samples.sort_unstable();
            let at = |fraction: f64| {
                samples[((samples.len() - 1) as f64 * fraction) as usize]
            };
            println!(
                "SIMV2_TAKER_RACE_BENCH n={} median_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth={} overflow=0 boundary=observe_causal_taker_races",
                samples.len(),
                at(0.50),
                at(0.99),
                at(0.999),
                samples.last().copied().unwrap_or(0),
                depth,
            );
        }

        const N: usize = 50_000;
        describe(1, sample(1, N));
        describe(8, sample(8, N));
    }

    #[test]
    fn place_ack_delivered_after_full_rtt() {
        let mut sim = sim_with_fixed_rtt(100); // 100ms RTT → L1=50ms, L2=50ms.
        let emit = 1_000_000_000u64;
        sim.submit(&place_signal("a"), emit);
        // First internal event is OrderReachesEngine @ emit + 50ms.
        assert_eq!(sim.peek_when(), Some(emit + 50_000_000));
        let r1 = sim.step(); // process reach → schedules ack, returns nothing
        assert!(r1.is_empty());
        // Ack now due @ emit + 100ms.
        assert_eq!(sim.peek_when(), Some(emit + 100_000_000));
        let r2 = sim.step();
        assert_eq!(r2.len(), 1);
        assert_eq!(r2[0].client_order_id, "a");
        assert_eq!(r2[0].status, OrderStatus::Accepted);
        assert_eq!(r2[0].timestamp_ns, emit + 100_000_000);
        assert!(sim.peek_when().is_none());
    }

    fn seed_front_order(sim: &mut Simulator, coid: &str) {
        sim.core.configure_replay_self_depth(1.0);
        sim.core.on_orderbook(&OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: "tok".into(),
            bids: vec![PriceLevel {
                price: 0.6,
                quantity: 10.0,
            }],
            asks: vec![PriceLevel {
                price: 0.62,
                quantity: 100.0,
            }],
            exchange_timestamp_ns: 1,
            local_timestamp_ns: 1,
        });
        let Signal::NewOrder(request) = place_signal(coid) else {
            unreachable!()
        };
        assert_eq!(
            sim.core.submit_order(&request, 2).status,
            OrderStatus::Accepted
        );
        assert!(sim.core.order_symbol_side(coid).is_some());
    }

    fn cancel_signal(coid: &str, timestamp_ns: u64) -> Signal {
        Signal::CancelOrder {
            exchange: Exchange::Polymarket,
            client_order_id: coid.into(),
            instance_id: String::new(),
            timestamp_ns,
        }
    }

    #[test]
    fn zero_cancel_finality_delay_preserves_immediate_cancel() {
        let mut sim = sim_with_fixed_rtt(100);
        seed_front_order(&mut sim, "a");
        let emit = 1_000_000_000u64;
        sim.submit(&cancel_signal("a", emit), emit);

        assert_eq!(sim.peek_when(), Some(emit + 50_000_000));
        assert!(sim.step().is_empty());
        assert!(sim.core.order_symbol_side("a").is_none());
        let fills = sim.core.on_trade_tick(&TradeTick {
            exchange: Exchange::Polymarket,
            symbol: "tok".into(),
            exchange_trade_id: None,
            price: 0.6,
            quantity: 10.0,
            side: Side::Sell,
            exchange_timestamp_ns: emit + 75_000_000,
            local_timestamp_ns: emit + 75_000_000,
        });
        assert!(fills.is_empty());
        let ack = sim.step();
        assert_eq!(ack.len(), 1);
        assert_eq!(ack[0].status, OrderStatus::Cancelled);
        assert_eq!(sim.cancel_finality_stats(), (0, 0));
    }

    #[test]
    fn delayed_cancel_finality_allows_causal_match_within_sampled_l2() {
        let mut sim = sim_with_fixed_rtt(100);
        sim.cancel_finality_delay_frac = 1.0;
        seed_front_order(&mut sim, "a");
        let emit = 1_000_000_000u64;
        sim.submit(&cancel_signal("a", emit), emit);

        // Cancel reaches the API lane at L1 but remains live through L2.
        assert_eq!(sim.peek_when(), Some(emit + 50_000_000));
        assert!(sim.step().is_empty());
        assert!(sim.core.order_symbol_side("a").is_some());
        assert_eq!(sim.peek_when(), Some(emit + 100_000_000));

        let fills = sim.core.on_trade_tick(&TradeTick {
            exchange: Exchange::Polymarket,
            symbol: "tok".into(),
            exchange_trade_id: None,
            price: 0.6,
            quantity: 10.0,
            side: Side::Sell,
            exchange_timestamp_ns: emit + 75_000_000,
            local_timestamp_ns: emit + 75_000_000,
        });
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].status, OrderStatus::Filled);
        let trade_id = fills[0].trade_id.clone();

        // Finalization observes the immutable recent fill, then delivers the
        // idempotent Filled resolution at the original RTT boundary.
        assert!(sim.step().is_empty());
        let ack = sim.step();
        assert_eq!(ack.len(), 1);
        assert_eq!(ack[0].status, OrderStatus::Filled);
        assert_eq!(ack[0].trade_id, trade_id);
        assert_eq!(sim.cancel_finality_stats(), (1, 1));
    }

    #[test]
    fn cancel_finality_multiplier_can_model_processing_beyond_response_l2() {
        let mut sim = sim_with_fixed_rtt(100);
        sim.cancel_finality_delay_frac = 4.0;
        seed_front_order(&mut sim, "a");
        let emit = 1_000_000_000u64;
        sim.submit(&cancel_signal("a", emit), emit);

        // L1=50ms; four response legs add 200ms of exchange-side finality.
        assert_eq!(sim.peek_when(), Some(emit + 50_000_000));
        assert!(sim.step().is_empty());
        assert!(sim.core.order_symbol_side("a").is_some());
        assert_eq!(sim.peek_when(), Some(emit + 250_000_000));
        assert!(sim.step().is_empty());
        assert!(sim.core.order_symbol_side("a").is_none());
        let ack = sim.step();
        assert_eq!(ack.len(), 1);
        assert_eq!(ack[0].status, OrderStatus::Cancelled);
        assert_eq!(ack[0].timestamp_ns, emit + 250_000_000);
        assert_eq!(sim.cancel_finality_stats(), (1, 0));
    }

    #[test]
    fn place_timeout_suppresses_ack_emits_timeout_then_reconciles() {
        let mut sim = sim_with_fixed_rtt(600); // RTT 600ms > 500ms client timeout
        let emit = 1_000_000_000u64;
        sim.submit(&place_signal("a"), emit);
        // Drain: only NewOrderTimeout reaches the strategy (Accepted suppressed).
        let mut statuses = Vec::new();
        while sim.peek_when().is_some() {
            for u in sim.step() {
                statuses.push((u.client_order_id.clone(), u.status));
            }
        }
        assert_eq!(
            statuses,
            vec![("a".to_string(), OrderStatus::NewOrderTimeout)]
        );
        assert_eq!(sim.timeout_stats().0, 1);

        // Order rests in core → reconcile resolves it to Accepted.
        let recon = Signal::ReconcilePolymarket {
            pending_places: vec![("a".into(), "tok".into(), Side::Buy, 0.6, None)],
            pending_cancels: vec![],
            pending_trade_ids: vec![],
            instance_id: String::new(),
        };
        sim.submit(&recon, 2_000_000_000);
        let mut recon_status = None;
        while sim.peek_when().is_some() {
            for u in sim.step() {
                recon_status = Some(u.status);
            }
        }
        assert_eq!(recon_status, Some(OrderStatus::Accepted));
    }

    #[test]
    fn multiple_submits_acks_ordered_by_when() {
        let mut sim = sim_with_fixed_rtt(100);
        sim.submit(&place_signal("first"), 1_000);
        sim.submit(&place_signal("second"), 2_000);
        // Drain everything; collect ack coids in delivery order.
        let mut acks = Vec::new();
        while sim.peek_when().is_some() {
            for u in sim.step() {
                acks.push(u.client_order_id);
            }
        }
        assert_eq!(acks, vec!["first".to_string(), "second".to_string()]);
    }

    #[test]
    fn dynamic_window_power_law_is_anchored_and_bounded() {
        let base = 1_000_000_000;
        assert_eq!(
            dynamic_window_ns(base, 40.0, 40.0, 2.0, 0.5, 2.0),
            (base, 1.0)
        );
        assert_eq!(
            dynamic_window_ns(base, 20.0, 40.0, 1.0, 0.5, 2.0),
            (500_000_000, 0.5)
        );
        assert_eq!(
            dynamic_window_ns(base, 100.0, 40.0, 2.0, 0.5, 2.0),
            (2_000_000_000, 2.0)
        );
        assert_eq!(
            dynamic_window_ns(base, f64::NAN, 40.0, 1.0, 0.5, 2.0),
            (base, 1.0)
        );
    }

    #[test]
    fn dynamic_markout_power_law_is_anchored_and_bounded() {
        assert_eq!(
            dynamic_markout_strength(0.35, 2.0, 2.0, 1.0, 0.5, 2.0),
            (0.35, 1.0)
        );
        assert_eq!(
            dynamic_markout_strength(0.35, 1.0, 2.0, 1.0, 0.5, 2.0),
            (0.175, 0.5)
        );
        assert_eq!(
            dynamic_markout_strength(0.35, 10.0, 2.0, 2.0, 0.5, 2.0),
            (0.70, 2.0)
        );
    }

    #[test]
    fn spot_markout_vol_uses_one_second_closes() {
        let mut sim = sim_with_fixed_rtt(100);
        sim.dynamic_fill_markout = true;
        sim.dynamic_markout_spot_vol = true;
        sim.dynamic_markout_lookback_ns = 5_000_000_000;
        let book = |price: f64, ts: u64| OrderBookSnapshot {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".into(),
            bids: vec![PriceLevel {
                price: price - 0.5,
                quantity: 1.0,
            }],
            asks: vec![PriceLevel {
                price: price + 0.5,
                quantity: 1.0,
            }],
            exchange_timestamp_ns: ts,
            local_timestamp_ns: ts,
        };
        sim.observe_dynamic_markout_spot_book(&book(100.0, 1_000_000_000), 1_000_000_000);
        sim.observe_dynamic_markout_spot_book(&book(101.0, 2_000_000_000), 2_000_000_000);
        // A later update in the same second replaces that second's close; it
        // must not add a second intrasecond return to realised volatility.
        sim.observe_dynamic_markout_spot_book(&book(102.0, 2_500_000_000), 2_500_000_000);
        sim.observe_dynamic_markout_spot_book(&book(102.0, 3_000_000_000), 3_000_000_000);
        let got = sim.spot_realised_vol_bps(3_000_000_000).unwrap();
        let expected = (102.0_f64 / 100.0).ln().abs() * 10_000.0;
        assert!((got - expected).abs() < 1e-9, "got={got} expected={expected}");
    }
}
