//! The sim_v2 driver the engine's thin loop calls.
//!
//! Owns the unified wall-clock `Scheduler`, the `ServerFeed` (server-axis
//! market replay), the stub matching `core`, and the `LatencyModel`. The
//! engine merges `peek_when()` against its own strat-lane market feed; when the
//! sim wins it calls `step()` (which advances one internal event and returns
//! any acks/fills now due for strategy delivery) and `submit()` (which schedules
//! a strategy signal's outbound effect with L1 latency).

use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;
use chrono::{DateTime, Utc};

use crate::exchange::sim::per_event_rtt::EventRttOverride;
use crate::types::{Exchange, Instrument, OrderStatus, OrderUpdate, Side, Signal};

use super::clock::Scheduler;
use super::event::{ReachAction, SimEvent};
use super::exchange::SimExchangeV2;
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
    /// Adverse-selection conditioning of the cancel attribution: `rate` (0 = off)
    /// + `scale_ticks` (adverse mid-move mapping to full tilt). See `exchange.rs`.
    pub adverse_sel_rate: f64,
    pub adverse_scale_ticks: f64,
    /// Book-through adverse fill rate ∈ [0,1] (0 = off): a resting order the
    /// contra side sweeps strictly through gets picked off (latency adverse
    /// selection). See `exchange.rs`.
    pub book_through_rate: f64,
    /// Volume-neutral forward-markout adverse-reprice strength (0 = off). Keeps the
    /// full fill, settles it adverse toward the forward mid (peeked `horizon` ns
    /// ahead) → edge drops at preserved maker volume → the sim's maker-fill markout
    /// matches live's −0.75¢. See `exchange.rs`.
    pub fill_markout_vn: f64,
    pub fill_markout_horizon_ns: u64,
    /// WS fill-push latency multiplier on the half-RTT.
    pub fill_push_mult: f64,
    /// matched-can't-cancel window (ns).
    pub matched_cant_cancel_window_ns: u64,
    /// Per-event RTT override table (sim_rtt_mode="exact"); `None` = pooled.
    pub per_event_rtt: Option<HashMap<u64, EventRttOverride>>,
    /// TAKER matching-engine overhead quantiles (ms): added to place RTT for
    /// taker fills.
    pub taker_overhead_p50_ms: f64,
    pub taker_overhead_p95_ms: f64,
    pub taker_overhead_p99_ms: f64,
    /// Maker/taker "race" rates in [0,1] (0 = off). See `exchange.rs`.
    pub maker_race_rate: f64,
    pub taker_race_rate: f64,
    /// Maker / taker race lookahead horizons (ns): the entry / match peek looks
    /// this far ahead (0 = immediate next snapshot).
    pub maker_race_horizon_ns: u64,
    pub taker_race_horizon_ns: u64,
    /// Outcome-folding: fold the two outcome tokens into one canonical up-frame
    /// book (down mapped p↔1−p, bid↔ask / buy↔sell). Removes the cross-outcome
    /// double-count. See `exchange.rs`.
    pub fold_outcomes: bool,
    /// Trade-flow taker competition rate ∈ [0,1] (0 = off): fraction of competing
    /// in-flight taker trade volume consumed ahead of us — we fill only the
    /// overflow. With the taker race, the taker-volume model. See `exchange.rs`.
    pub taker_comp_rate: f64,
    /// Taker competition in-flight window (ns) ≈ taker overhead exposure.
    pub taker_comp_window_ns: u64,
    /// Deep-queue model for resting prices beyond the recorded 5-level window:
    /// 0 = legacy least-squares linear extrapolation; >0 = outermost-level
    /// flat/geometric-decay (1.0 = flat, <1 = decay). See `book.rs`.
    pub deep_queue_decay: f64,
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

pub struct Simulator {
    sched: Scheduler,
    feed: ServerFeed,
    core: SimExchangeV2,
    latency: LatencyModel,
    client_timeout_ns: u64,
    timeouts: u64,
    per_event_rtt: Option<HashMap<u64, EventRttOverride>>,
    /// Cached `core.race_enabled()` — skips the peek when the race is off.
    race_enabled: bool,
    /// Maker / taker race lookahead horizons (ns).
    maker_race_horizon_ns: u64,
    taker_race_horizon_ns: u64,
    /// See `SimV2Config::use_batch_orders`.
    use_batch_orders: bool,
    /// Forward horizon (ns) for the markout fill haircut; peek the canonical mid
    /// this far past each trade. `markout_on` gates the peek (vn>0 && horizon>0).
    fill_markout_horizon_ns: u64,
    markout_on: bool,
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

impl Simulator {
    pub fn new(cfg: SimV2Config) -> Result<Self> {
        let feed = ServerFeed::new(Path::new(&cfg.data_dir), &cfg.sources, cfg.start, cfg.end)?;
        // Record-replay (or any pre-built) profile wins; otherwise build the
        // legacy scalar Empirical from the calibrated p50/p95/p99 anchors.
        let place = cfg.place_profile.clone().unwrap_or_else(|| {
            LatencyModel::empirical_profile(cfg.place_p50_ms, cfg.place_p95_ms, cfg.place_p99_ms, cfg.rho)
        });
        let cancel = cfg.cancel_profile.clone().unwrap_or_else(|| {
            LatencyModel::empirical_profile(cfg.cancel_p50_ms, cfg.cancel_p95_ms, cfg.cancel_p99_ms, cfg.rho)
        });
        let mut latency = LatencyModel::new(place, cancel, cfg.rho_cross, cfg.seed);
        latency.set_fill_push_mult(cfg.fill_push_mult);
        // Censoring threshold for the per-event timeout-rate injection — must
        // equal the engine's timeout boundary (`rtt > client_timeout_ns`).
        latency.set_client_timeout_ms(cfg.client_timeout_ns as f64 / 1_000_000.0);
        latency.set_taker_overhead_anchors(
            cfg.taker_overhead_p50_ms,
            cfg.taker_overhead_p95_ms,
            cfg.taker_overhead_p99_ms,
        );
        let mut core = SimExchangeV2::new(cfg.client_timeout_ns, cfg.wallet_usdc_by_iid, cfg.split_by_iid);
        core.configure(cfg.ahead_frac, cfg.matched_cant_cancel_window_ns);
        core.configure_adverse_sel(cfg.adverse_sel_rate, cfg.adverse_scale_ticks);
        core.configure_book_through(cfg.book_through_rate);
        core.configure_fill_markout_vn(cfg.fill_markout_vn);
        core.configure_race(cfg.maker_race_rate, cfg.taker_race_rate);
        core.set_fold_outcomes(cfg.fold_outcomes);
        core.configure_taker_comp(cfg.taker_comp_rate, cfg.taker_comp_window_ns);
        core.set_deep_queue_decay(cfg.deep_queue_decay);
        let race_enabled = core.race_enabled();
        Ok(Self {
            sched: Scheduler::new(),
            feed,
            core,
            latency,
            client_timeout_ns: cfg.client_timeout_ns,
            timeouts: 0,
            per_event_rtt: cfg.per_event_rtt,
            race_enabled,
            maker_race_horizon_ns: cfg.maker_race_horizon_ns,
            taker_race_horizon_ns: cfg.taker_race_horizon_ns,
            use_batch_orders: cfg.use_batch_orders,
            fill_markout_horizon_ns: cfg.fill_markout_horizon_ns,
            markout_on: cfg.fill_markout_vn > 0.0 && cfg.fill_markout_horizon_ns > 0,
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
        if !self.race_enabled {
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
            let from_sib = sibling.as_ref().and_then(|s| self.feed.peek_next_book(s, at));
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
        if !self.race_enabled {
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
        (self.core.taker_fills, self.core.maker_fills, self.core.rejects)
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
        (self.core.rej_taker_buy, self.core.rej_taker_sell,
         self.core.rej_rest_buy, self.core.rej_rest_sell,
         self.core.rej_rest_sell_short_sum)
    }

    /// (timeouts, matched_cant_cancel) for the summary.
    pub fn timeout_stats(&self) -> (u64, u64) {
        (self.timeouts, self.core.matched_cant_cancel)
    }

    /// (post_only_rejects, post_only_seen) for the summary.
    pub fn post_only_stats(&self) -> (u64, u64) {
        (self.core.post_only_rejects, self.core.post_only_seen)
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

    /// # book-through adverse fills produced (diagnostic for
    /// `sim_v2_book_through_rate`).
    pub fn book_through_fills(&self) -> u64 {
        self.core.book_through_fills_n
    }

    /// # maker fills haircut by the forward-markout conditioning (diagnostic).
    pub fn fill_haircuts(&self) -> u64 {
        self.core.fill_haircut_n
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
            vec![n as f64, mean, q(0.10), q(0.25), q(0.50), q(0.75), q(0.90), q(0.99), zero]
        }
        (pcts(&self.core.maker_q_init), pcts(&self.core.taker_avail))
    }

    /// Maker placement price-vs-BBO buckets: each [total, q0_count] for
    /// (improve, join, behind, nobook). Explains the zero-queue share.
    pub fn placement_buckets(&self) -> [[u64; 2]; 4] {
        let c = &self.core;
        [c.place_improve, c.place_join, c.place_behind, c.place_nobook]
    }

    /// q_init=0 fallback split: (extrapolated beyond-window, in-window best-rule).
    pub fn q0_fallback_split(&self) -> (u64, u64) {
        (self.core.q0_extrapolated, self.core.q0_bestrule)
    }

    /// Trade-flow taker competition diagnostics:
    /// (capped, capped_to_zero, mean competing volume seen at a marketable match).
    pub fn taker_comp_stats(&self) -> (u64, u64, f64) {
        let c = &self.core;
        let mean = if c.taker_comp_n > 0 { c.taker_comp_vol_sum / c.taker_comp_n as f64 } else { 0.0 };
        (c.taker_comp_capped, c.taker_comp_capped_zero, mean)
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
        let entry = self.per_event_rtt.as_ref().and_then(|t| t.get(&secs).copied());
        match entry {
            Some(e) => self.latency.apply_per_event_override(&e, secs),
            None => self.latency.clear_per_event_override(),
        }
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
        let best_bid = bids.iter().filter(|l| fin(l)).map(|l| l.price).fold(f64::NEG_INFINITY, f64::max);
        let best_ask = asks.iter().filter(|l| fin(l)).map(|l| l.price).fold(f64::INFINITY, f64::min);
        (best_bid.is_finite() && best_ask.is_finite()).then(|| 0.5 * (best_bid + best_ask))
    }

    fn step_feed(&mut self) -> Vec<OrderUpdate> {
        if let Some((when, ev)) = self.feed.next_server_event() {
            match ev {
                SimEvent::ServerBook(ob) => {
                    // Book-through adverse fills (a resting order the contra just
                    // swept through) surface here, delivered like trade fills
                    // after a ws fill-push delay. Empty unless book_through_rate>0.
                    let fills = self.core.on_orderbook(&ob);
                    for mut fill in fills {
                        let push = self.latency.sample_fill_push(when);
                        let deliver = when.saturating_add(push);
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
                    let fwd_mid = if self.markout_on {
                        self.peek_fwd_canonical_mid(&t.symbol, when.saturating_add(self.fill_markout_horizon_ns))
                    } else {
                        None
                    };
                    let fills = self.core.on_trade_tick_fwd(&t, fwd_mid);
                    for mut fill in fills {
                        let push = self.latency.sample_fill_push(when);
                        let deliver = when.saturating_add(push);
                        fill.timestamp_ns = deliver;
                        self.sched.push(deliver, SimEvent::FillToStrategy(fill));
                    }
                }
                SimEvent::ServerInstrument(i) => {
                    self.core.on_instrument(&i);
                    self.apply_per_event_rtt(&i);
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
            SimEvent::OrderReachesEngine { action, l2_ns, suppress_ack } => {
                // core uses `when` (server time) for matching + recent_fills.
                match action {
                    ReachAction::Place(o) => {
                        if self.core.would_cross(&o) {
                            // Genuine taker: defer the actual book-match to the
                            // MIDPOINT of the matching window (reach + overhead/2)
                            // so the book can move in-flight (natural taker miss).
                            let overhead = self.latency.sample_taker_overhead(when);
                            let match_at = when.saturating_add(overhead / 2);
                            self.sched.push(
                                match_at,
                                SimEvent::TakerMatch { order: o, l2_ns, overhead_ns: overhead, suppress_ack },
                            );
                        } else {
                            // Maker race: peek the queue `maker_race_horizon` ahead
                            // (the book the resting order faces shortly after entry)
                            // for the q_ahead-init blend.
                            self.prime_next_books(&o.symbol, when, self.maker_race_horizon_ns);
                            let u = self.core.submit_order(&o, when);
                            self.deliver_ack(u, when.saturating_add(l2_ns), suppress_ack);
                        }
                    }
                    ReachAction::Cancel { exchange, client_order_id } => {
                        let u = self.core.cancel_order(exchange, &client_order_id, when);
                        self.deliver_ack(u, when.saturating_add(l2_ns), suppress_ack);
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
            SimEvent::TakerMatch { order, l2_ns, overhead_ns, suppress_ack } => {
                // Re-match against the (now possibly moved) book: still crossing
                // → taker fill; moved away → rests (miss) or cancels per type.
                // Taker race: take the MIN available volume over EVERY book in
                // the `(now, now+taker_race_horizon]` in-flight window -> tighter
                // liquidity-recede check than a single endpoint snapshot.
                self.prime_taker_window(&order.symbol, when, self.taker_race_horizon_ns);
                let u = self.core.submit_order(&order, when);
                let is_fill = matches!(u.status, OrderStatus::Filled | OrderStatus::PartiallyFilled);
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
        if let Signal::ReconcilePolymarket { pending_places, pending_cancels, .. } = sig {
            let (l1, l2) = self.latency.sample_cancel_split(t_emit);
            let deliver = t_emit.saturating_add(l1).saturating_add(l2);
            for u in self.core.reconcile(pending_places, pending_cancels, deliver) {
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
            SimEvent::OrderReachesEngine { action, l2_ns: l2, suppress_ack: timed_out },
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
            ReachAction::Cancel { client_order_id, .. } => {
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
            trade_id: None,
            error: None,
        })
    }
}

/// True when a reach action is a cancel (vs a place). Used by the
/// `use_batch_orders=false` split path to pick the cancel vs place RTT
/// sampler per action.
fn action_is_cancel(a: &ReachAction) -> bool {
    matches!(a, ReachAction::Cancel { .. } | ReachAction::CancelAll { .. })
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
        Signal::ReconcilePolymarket { .. } | Signal::Exit => (Vec::new(), false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::sim::latency::LatencyProfile;
    use crate::types::{Exchange, OrderRequest, OrderStatus, Side};

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
            core: SimExchangeV2::new(500_000_000, std::collections::HashMap::new(), std::collections::HashMap::new()),
            latency,
            client_timeout_ns: 500_000_000,
            timeouts: 0,
            per_event_rtt: None,
            race_enabled: false,
            maker_race_horizon_ns: 0,
            taker_race_horizon_ns: 0,
            use_batch_orders: true,
            fill_markout_horizon_ns: 0,
            markout_on: false,
        }
    }

    /// Simulator with distinct fixed place / cancel RTTs and an explicit
    /// `use_batch_orders` flag — for exercising the split-dispatch path.
    fn sim_split_rtt(place_ms: u64, cancel_ms: u64, use_batch_orders: bool) -> Simulator {
        let feed = ServerFeed::new(
            Path::new("/nonexistent"), &[],
            DateTime::<Utc>::from_timestamp(0, 0).unwrap(),
            DateTime::<Utc>::from_timestamp(1, 0).unwrap(),
        ).unwrap();
        let latency = LatencyModel::new(
            LatencyProfile::Fixed(place_ms),
            LatencyProfile::Fixed(cancel_ms),
            0.0, 1,
        );
        Simulator {
            sched: Scheduler::new(), feed,
            core: SimExchangeV2::new(500_000_000, std::collections::HashMap::new(), std::collections::HashMap::new()),
            latency, client_timeout_ns: 500_000_000, timeouts: 0,
            per_event_rtt: None, race_enabled: false,
            maker_race_horizon_ns: 0, taker_race_horizon_ns: 0,
            use_batch_orders,
            fill_markout_horizon_ns: 0,
            markout_on: false,
        }
    }

    fn reprice_signal(cancel_coid: &str, place_coid: &str) -> Signal {
        Signal::BatchUpdateOrders {
            exchange: Exchange::Polymarket,
            market_id: String::new(),
            cancel_client_order_ids: vec![cancel_coid.to_string()],
            place_orders: vec![match place_signal(place_coid) {
                Signal::NewOrder(o) => o,
                _ => unreachable!(),
            }],
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
            statuses.iter().any(|(c, s)| c == "old" && *s == OrderStatus::CancelOrderTimeout),
            "split: cancel must time out on the cancel RTT, got {statuses:?}",
        );
        // The CancelOrderTimeout must carry a non-None exchange_order_id, else
        // the strategy's handler silently drops it (see strategy.rs:6979).
        assert!(
            oids.iter().any(|(c, oid)| c == "old" && oid.is_some()),
            "cancel timeout must carry exchange_order_id, got {oids:?}",
        );
        assert!(
            !statuses.iter().any(|(_, s)| *s == OrderStatus::NewOrderTimeout),
            "split: fast place must NOT time out, got {statuses:?}",
        );
        assert_eq!(split.timeout_stats().0, 1, "exactly one (cancel) timeout");

        // Batched: same RTTs, whole reprice uses the fast place RTT → none.
        let mut batched = sim_split_rtt(100, 1200, true);
        batched.submit(&reprice_signal("old", "new"), 1_000_000_000);
        while batched.peek_when().is_some() { for _ in batched.step() {} }
        assert_eq!(batched.timeout_stats().0, 0, "batched reprice shares the fast place RTT → no timeout");
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
            timestamp_ns: 0,
            instance_id: String::new(),
            fee_rate_bps: 0,
            post_only: true,
            outcome_label: String::new(),
        })
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
        assert_eq!(statuses, vec![("a".to_string(), OrderStatus::NewOrderTimeout)]);
        assert_eq!(sim.timeout_stats().0, 1);

        // Order rests in core → reconcile resolves it to Accepted.
        let recon = Signal::ReconcilePolymarket {
            pending_places: vec![("a".into(), "tok".into(), Side::Buy, 0.6, None)],
            pending_cancels: vec![],
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
}
