//! Shared index price computation module.
//!
//! Tracks per-exchange mid prices, handles USDT→USD conversion,
//! and computes aggregate index prices (mean). Used by both
//! the polymaker and index_price strategies.

use std::collections::{HashMap, VecDeque};

use crate::types::{Exchange, OrderBookSnapshot, SpotPrice};

/// Derive Coinbase symbol from Binance symbol: "BTCUSDT" → "BTC-USD".
fn derive_coinbase_symbol(binance_sym: &str) -> String {
    if let Some(base) = binance_sym.strip_suffix("USDT") {
        format!("{}-USD", base)
    } else {
        binance_sym.to_string()
    }
}

/// Derive OKX symbol from Binance symbol: "BTCUSDT" → "BTC-USDT".
fn derive_okx_symbol(binance_sym: &str) -> String {
    if let Some(base) = binance_sym.strip_suffix("USDT") {
        format!("{}-USDT", base)
    } else {
        binance_sym.to_string()
    }
}

/// Derive Gate symbol from Binance symbol: "BTCUSDT" → "BTC_USDT".
fn derive_gate_symbol(binance_sym: &str) -> String {
    if let Some(base) = binance_sym.strip_suffix("USDT") {
        format!("{}_USDT", base)
    } else {
        binance_sym.to_string()
    }
}

/// Derive KuCoin symbol from Binance symbol: "BTCUSDT" → "BTC-USDT".
fn derive_kucoin_symbol(binance_sym: &str) -> String {
    if let Some(base) = binance_sym.strip_suffix("USDT") {
        format!("{}-USDT", base)
    } else {
        binance_sym.to_string()
    }
}

/// Check if an exchange uses USD (not USDT) denomination.
fn is_usd_exchange(exchange: &str) -> bool {
    matches!(exchange, "coinbase" | "binance_futures")
}

/// Shared index price tracker.
///
/// Maintains per-exchange mid prices (converted to USD) and computes
/// aggregate index prices (weighted mean / median).
pub struct IndexPrice {
    // ── Per-exchange symbol mapping ──
    pub binance_symbol: String,
    coinbase_symbol: String,
    okx_symbol: String,
    gate_symbol: String,
    kucoin_symbol: String,
    mexc_symbol: String,
    bitget_symbol: String,

    // ── Per-exchange mid prices (USD) and timestamps ──
    binance_mid: Option<f64>, binance_ts_ns: u64,
    bybit_mid: Option<f64>, bybit_ts_ns: u64,
    coinbase_mid: Option<f64>, coinbase_ts_ns: u64,
    okx_mid: Option<f64>, okx_ts_ns: u64,
    gate_mid: Option<f64>, gate_ts_ns: u64,
    kucoin_mid: Option<f64>, kucoin_ts_ns: u64,
    mexc_mid: Option<f64>, mexc_ts_ns: u64,
    bitget_mid: Option<f64>, bitget_ts_ns: u64,
    /// Binance Futures BTC/USD asset index (from SpotPrice, not OrderBook)
    binance_futures_mid: Option<f64>, binance_futures_ts_ns: u64,

    // ── USDT/USD rate (from Chainlink Data Streams or Pyth) ──
    pub usdt_price: f64,

    // ── Chainlink price ──
    pub chainlink_price: Option<f64>,
    pub chainlink_ts_ns: u64,

    // ── Index configuration ──
    /// (exchange_name, weight) pairs for myindex calculation.
    pub index_exchanges: Vec<(String, f64)>,

    /// Maximum allowed pair-wise relative divergence between any two
    /// component exchanges, as a fraction (e.g. 0.01 = 1%).
    /// `compute_myindex_validated` returns `MyindexInvalid::Divergent`
    /// when `(max - min) / median > divergence_pct`. 0.0 = disabled.
    ///
    /// Live evidence 2026-05-13 16:10–16:55: myindex bounced 41k-185k
    /// (true BTC ~79k) during startup + Coinbase reconnect, leading to
    /// 33.6 % crossed quotes and 94.7 % taker fills (mean −$4.43/event).
    /// The aggregate's weighted-average obscured per-exchange disagreement;
    /// a divergence gate would have refused to quote on any of those ticks.
    pub myindex_divergence_pct: f64,

    /// Maximum allowed age (ns) of any single component's last-update
    /// server timestamp compared to a "now" clock supplied by the caller.
    /// `compute_myindex_validated` returns `MyindexInvalid::Stale` when
    /// any included component's `exchange_ts_ns` is older than this.
    /// 0 = disabled.
    ///
    /// Same outage as above: during a reconnect window one feed kept
    /// publishing its last-known value (or never published) while the
    /// other kept moving. Comparing per-component timestamps to "now"
    /// catches a frozen feed even when its price looks plausible.
    pub myindex_staleness_ns: u64,

    /// **Quote-path market-data-freshness threshold (ns).** SEPARATE from
    /// `myindex_staleness_ns` (the per-component invalid gate). The polymaker
    /// `on_quote` skips quoting when `now − index_timestamp_ns` (the
    /// weighted-average component timestamp) exceeds this. Default
    /// `200_000_000` (200 ms) — the historical hardcoded value, now tunable.
    /// 0 = disabled. Config key `myindex_quote_staleness_ms`.
    pub quote_staleness_ns: u64,

    /// **Quote-path timestamp aggregation mode.** When `true`,
    /// `index_timestamp_ns()` returns the LATEST (max) per-component
    /// server timestamp instead of the weighted average — i.e. the gate
    /// age becomes "time since the freshest component updated" rather
    /// than "weight-blended age". At 1:2 bn:cb weights the weighted
    /// average is strictly older than the max, so the same threshold is
    /// LOOSER in max mode (pair with a tighter `myindex_quote_staleness_ms`).
    /// Default `false` = legacy weighted average (byte-identical).
    /// Config key `myindex_quote_ts_max`.
    pub quote_ts_use_max: bool,

    /// **P0 — Per-feed tick-to-tick mid jump clamp.**
    /// Maximum allowed relative change `|new_mid / prev_mid − 1|` between
    /// consecutive OB updates for the SAME exchange. New mids exceeding
    /// this fraction are rejected (the OB tick is dropped, `binance_mid`
    /// etc. stay at their previous value). 0.0 = disabled.
    ///
    /// Default 0.005 (0.5 %) — real BTC at 100 ms cadence never moves
    /// > 0.1 % per tick; 0.5 % gives 5× headroom while catching the
    /// L1 single-tick glitch pattern observed 2026-05-13 18:17:50–51
    /// (S=84k → 89k in 1.5 s via 4 stepwise 1-2 k jumps).
    pub myindex_max_tick_jump_pct: f64,

    /// **P1 — Per-feed bid-ask spread sanity.**
    /// Maximum allowed relative `(ask - bid) / bid` for the OB used to
    /// compute a mid. OBs whose top-of-book spread exceeds this fraction
    /// are rejected wholesale (don't update the feed's mid). 0.0 = disabled.
    ///
    /// Default 0.01 (1 %) — BTC L1 spread is typically < 0.05 %; 1 %
    /// catches the "stub quote" pattern where one side of the book has
    /// a far-out single quote that pulls `(bid+ask)/2` away from fair.
    pub myindex_max_bid_ask_pct: f64,

    /// **P2 — Per-feed peer-disagree clamp at write time.**
    /// Maximum allowed relative gap between an incoming mid and the
    /// median of OTHER live components. New mids whose `|new / median − 1|`
    /// exceeds this fraction are rejected — keeps a single rogue feed
    /// from polluting the aggregate before `compute_myindex_validated`
    /// even sees it. 0.0 = disabled.
    ///
    /// Default 0.01 (1 %) — same threshold as `myindex_divergence_pct`
    /// but applied at the write boundary instead of at aggregation, so
    /// the bad value never enters the index in the first place.
    pub myindex_max_peer_disagree_pct: f64,

    /// **Per-component EWMA smoothing half-life (milliseconds).**
    /// When `> 0`, each exchange's validated USD mid is smoothed with a
    /// time-aware exponential moving average BEFORE it enters the weighted
    /// myindex sum: `α = 1 − 0.5^(Δt_ms / halflife)`, `mid_ewma =
    /// α·new + (1−α)·prev`. Δt is the elapsed time since that feed's last
    /// update, so async feeds (binance 59/s vs coinbase 27/s) each smooth
    /// on their own cadence and gaps decay correctly (large Δt ⇒ α→1 ⇒
    /// no stale carry). Smoothing per-component (vs smoothing the fused
    /// index) is algebraically identical for synchronous feeds but
    /// correct for async ones. `0.0` (default) = raw mid (legacy).
    pub ewma_halflife_ms: f64,

    /// **Staleness-weight half-life (ms) for the index weighted average.**
    /// When `> 0`, each component's base weight is multiplied by
    /// `0.5^((now − last_update_ts) / halflife)` before the weighted mean,
    /// so a feed whose last trade/update is further in the past counts
    /// less (last-trade gaps ⇒ decay). `0.0` (default) = flat weights
    /// (legacy). Applied in `compute_myindex_validated` (the quoting path
    /// with a `now` ref).
    pub staleness_halflife_ms: f64,

    /// **Staleness gate mode.** Controls how `myindex_staleness_ns` is
    /// applied in `compute_myindex_validated`:
    ///   * `true` (default / legacy): **ALL** live components must be
    ///     fresh — if any one is stale the whole index is `Err(Stale)`
    ///     and quoting pauses.
    ///   * `false`: **ANY** — keep the index valid as long as ≥1
    ///     component is fresh. Stale components are NOT dropped; they
    ///     stay in the weighted average where the staleness-halflife
    ///     weight (`staleness_halflife_ms`) already down-weights them
    ///     smoothly. The index only goes `Err(Stale)` when *every*
    ///     component is stale.
    /// Config key `myindex_staleness_require_all`. ts==0 (never-warm)
    /// components are treated as "not stale" in both modes, consistent
    /// with the legacy skip.
    pub staleness_require_all: bool,

    /// **Mid-price bracket mode.** When `true`, a SECOND index
    /// (`myindex_midprice`) is computed from the per-component orderbook
    /// MID prices (stored in `ob_mid`) using the SAME component weights +
    /// half-life decay as the primary last-trade `myindex`. The quoter
    /// then brackets the two: the bid anchors at `min(myindex,
    /// myindex_mid)` and the ask at `max(myindex, myindex_mid)`, so when
    /// the trade index and the book-mid index disagree the quote widens
    /// (a conservative protection). The chainlink ↔ myindex `coff` is the
    /// SAME for both (applied once in the strategy). `false` (default) ⇒
    /// only the last-trade myindex is used, byte-for-byte legacy. Config
    /// key `index_midprice_bracket_enabled`.
    pub midprice_bracket_enabled: bool,

    /// **Mid-price bracket strength** (scales the widening). The per-endpoint
    /// band extension is `strength · ln(min/trade)` (≤0) on the bid and
    /// `strength · ln(max/trade)` (≥0) on the ask, so `1.0` (default) =
    /// the FULL mid-vs-trade gap, `0.0` = no widening (≡ disabled), and
    /// intermediate values dial the protection vs spread-capture trade-off.
    /// Config key `index_midprice_bracket_strength`.
    pub midprice_bracket_strength: f64,

    /// **Per-component orderbook MID price + timestamp (ns)**, keyed by
    /// the canonical exchange name. Populated by
    /// `update_ob_mid_from_orderbook` only when `midprice_bracket_enabled`.
    /// Separate from the `*_mid` slots (which now hold last-trade prices)
    /// so the trade index and the book-mid index coexist.
    ob_mid: HashMap<String, (f64, u64)>,

    /// **Per-component LOCAL arrival timestamp (ns)** of the last
    /// accepted update, keyed by the canonical exchange name. Recorded
    /// alongside every `update_from_trade` / `update_from_orderbook`
    /// write. Used by myindex2 mode to compute the staleness-decay age
    /// on the LOCAL time axis (`now − local_arrival`) instead of mixing
    /// axes (`now_local − server_ts` silently embeds each feed\'s
    /// transport delay — binance ~104 ms vs coinbase ~51 ms — which
    /// double-counts once an explicit lag offset is configured).
    /// Legacy (myindex2 off) paths never read this — bit-exact.
    local_ts: HashMap<String, u64>,



    /// **myindex2 state** — measured-USDT-basis deduction + online-α
    /// chainlink alignment. See the `Myindex2` struct doc for the
    /// algorithm and the offline evidence. Disabled by default; the
    /// engine wires `myindex2_*` TOML keys via `set_myindex2`.
    pub myindex2: Myindex2,
}



/// **myindex2 v2 — measured-basis deduction + direct ratio tracking.**
///
/// Two-stage construction (2026-06-12 redesign after the head-to-head
/// where the legacy coff corrector's direct 900 s error-correction beat
/// the structured "alpha*B" decomposition on chainlink tracking):
///
///   1. **Basis** `B = rollmean(ln(bn/cb_aligned), 300 s)` — the measured
///      USDT/USD basis. LOCAL-axis alignment: on the recorded local-arrival
///      axis coinbase leads binance (~40 ms BTC / ~20 ms ETH — bn
///      `@depth10@100ms` is throttled/late vs cb's denser, earlier stream),
///      so the latest bn pairs with cb as-of local time `bn_local −
///      cb_lead_bn_local` from a short local-ts ring (look back by the lead
///      so both legs reflect the same economic instant).
///   2. **my0** = the staleness-decayed weighted mean the validated
///      quote path uses (config weights x 0.5^(age/halflife), coinbase
///      age + optional offset) with the binance leg x e^{-B}.
///   3. **adjust** = rollmean(chainlink / my0_lagged, 900 s) where
///      my0_lagged is my0 as-of LOCAL time `now - cl_bn_local_lag`
///      (chainlink lags binance ~2.37 s on the local axis) — a direct,
///      model-free ratio tracker like the legacy coff, but referenced
///      to the basis-corrected, lag-aligned index.
///
/// Final: `myindex2 = my0(now) x adjust`.
///
/// Sampling mirrors a rolling-deque idiom (1 Hz deques, half-window
/// warm-up, 2 s freshness gates, |ln|>5 % sanity rejects). Disabled or
/// not-yet-warm => factors exactly 1.0 => bit-exact legacy myindex.
pub struct Myindex2 {
    /// Master switch. `false` (default) => factors pinned at 1.0.
    pub enabled: bool,

    /// Min gap (ns) between accepted samples. Default 1 s.
    pub sample_interval_ns: u64,

    /// Basis window in SAMPLES (seconds at 1 Hz). Default 300.
    pub basis_window_n: usize,

    /// Adjust-ratio window in SAMPLES. Default 900 — the legacy coff's
    /// window, empirically the best chainlink tracker (RMSE 0.92 vs
    /// 1.01 bps for the 30-min alpha decomposition it replaces).
    pub adjust_window_n: usize,

    /// Per-feed freshness threshold (ns). Default 2 s.
    pub staleness_threshold_ns: u64,

    /// LOCAL-axis lead of coinbase over binance (ns) — how much earlier cb's
    /// price arrives/updates locally than bn's. Offline (recorded local-arrival
    /// axis): cb leads bn ~40 ms (BTC) / ~20 ms (ETH) — bn `@depth10@100ms` is
    /// throttled/late vs cb's denser, earlier-stamped stream. Basis pairs the
    /// latest bn with cb as-of `bn_local − this` (look BACK by the lead so both
    /// legs reflect the same economic instant). 0 = pair as-of bn_local exactly.
    pub cb_lead_bn_local_ns: u64,

    /// LOCAL-axis lag of chainlink relative to binance (ns). Default
    /// 2370 ms (offline: cl lags bn locally ~2.36-2.38 s). Adjust pairs
    /// cl(now) with my0 as-of `now - this`. 0 = pair the latest my0.
    pub cl_bn_local_lag_ns: u64,

    /// BINANCE staleness-age offset (ns) for the weight decay. On the
    /// LOCAL arrival axis binance LAGS coinbase by ~39-50 ms (transport
    /// 104 vs 51 ms + 10 Hz book outweigh its server-axis discovery
    /// lead), so binance's content is the staler one at quote time —
    /// its decay age gets +offset. Used by the validated quote path AND
    /// my0. Default 0. (The opposite-direction coinbase offset was
    /// tested and REJECTED: server-axis discovery credit is the wrong
    /// sign for quoting, which cares about local freshness.)
    pub bn_staleness_offset_ns: u64,

    /// Coinbase usd-mid history keyed by SERVER ts (basis alignment).
    cb_hist: VecDeque<(u64, f64)>,

    /// my0 history keyed by LOCAL ts (adjust lag-lookup).
    my0_hist: VecDeque<(u64, f64)>,

    /// Rolling aligned ln(bn/cb) samples.
    basis_samples: VecDeque<f64>,

    /// Rolling chainlink / my0_lagged ratio samples.
    adjust_samples: VecDeque<f64>,

    /// Last accepted basis-sample time (rate-limit reference).
    last_sample_ns: u64,

    /// Cached B = mean(basis_samples); meaningful when `basis_warm`.
    basis: f64,

    /// Half-window warm-up flag for B (factors stay 1.0 before).
    basis_warm: bool,

    /// Cached adjust = mean(adjust_samples); 1.0-equivalent until warm.
    adjust: f64,
    adjust_warm: bool,

    /// Per-event freeze snapshot of `adjust` (same rationale as
    /// the corrector's per-event freeze rationale).
    frozen_adjust: Option<f64>,

    /// Master switch for the per-event freeze.
    pub freeze_per_event_enabled: bool,
}

impl Myindex2 {
    /// Construction default: disabled, lead-lag-study lag defaults.
    fn default_off() -> Self {
        Self {
            enabled: false,
            sample_interval_ns: 1_000_000_000,
            basis_window_n: 300,
            adjust_window_n: 900,
            staleness_threshold_ns: 2_000_000_000,
            cb_lead_bn_local_ns: 30_000_000,
            cl_bn_local_lag_ns: 2_370_000_000,
            bn_staleness_offset_ns: 0,
            cb_hist: VecDeque::new(),
            my0_hist: VecDeque::new(),
            basis_samples: VecDeque::new(),
            adjust_samples: VecDeque::new(),
            last_sample_ns: 0,
            basis: 0.0,
            basis_warm: false,
            adjust: 1.0,
            adjust_warm: false,
            frozen_adjust: None,
            freeze_per_event_enabled: true,
        }
    }

    /// adjust honouring an active per-event freeze; exactly 1.0 until warm.
    pub fn effective_adjust(&self) -> f64 {
        if !self.adjust_warm { return 1.0; }
        self.frozen_adjust.unwrap_or(self.adjust)
    }

    /// Live adjust bypassing any freeze — diagnostics.
    pub fn live_adjust(&self) -> f64 { self.adjust }

    /// Current smoothed basis B (ln units). 0.0 until warm — diagnostics.
    pub fn basis(&self) -> f64 { if self.basis_warm { self.basis } else { 0.0 } }

    /// Whether the basis window has warmed (bn factor live).
    pub fn is_warm(&self) -> bool { self.basis_warm }

    /// Snapshot the live adjust into the event-freeze slot (no-op when
    /// the freeze feature is disabled). Call once per event seed.
    pub fn freeze_for_event(&mut self) {
        if !self.freeze_per_event_enabled { return; }
        self.frozen_adjust = Some(self.adjust);
    }

    /// Toggle the per-event freeze; disabling clears any active snapshot.
    pub fn set_freeze_per_event_enabled(&mut self, enabled: bool) {
        self.freeze_per_event_enabled = enabled;
        if !enabled { self.frozen_adjust = None; }
    }

    /// Reset all sampled state (feed reconnect hygiene). Config untouched.
    pub fn reset_samples(&mut self) {
        self.basis_samples.clear();
        self.adjust_samples.clear();
        self.cb_hist.clear();
        self.my0_hist.clear();
        self.last_sample_ns = 0;
        self.basis = 0.0;
        self.basis_warm = false;
        self.adjust = 1.0;
        self.adjust_warm = false;
    }

    /// Append (ts, value), keeping the ring monotone in ts and trimming
    /// entries older than the retention horizon. Same-or-older ts skips
    /// (slots re-read between updates dedup naturally).
    fn hist_push(hist: &mut VecDeque<(u64, f64)>, ts_ns: u64, v: f64, retain_ns: u64) {
        if let Some(&(last_ts, _)) = hist.back() {
            if ts_ns <= last_ts { return; }
        }
        hist.push_back((ts_ns, v));
        let cutoff = ts_ns.saturating_sub(retain_ns);
        while hist.front().map_or(false, |&(ts, _)| ts < cutoff) {
            hist.pop_front();
        }
    }

    /// LOCF as-of lookup: last value with ts <= target. `None` when the
    /// history does not reach back that far (warm-up / after a gap).
    fn hist_asof(hist: &VecDeque<(u64, f64)>, target_ns: u64) -> Option<f64> {
        for &(ts, v) in hist.iter().rev() {
            if ts <= target_ns { return Some(v); }
        }
        None
    }
}

/// Failure modes of `compute_myindex_validated`. Returned in lieu of a
/// price when the caller-supplied thresholds are violated; callers in
/// hot paths should refuse to quote.
#[derive(Debug, Clone, PartialEq)]
pub enum MyindexInvalid {
    /// Fewer than 2 valid components — `compute_myindex` returned 0 or
    /// only one feed has reported a price. Divergence can't be evaluated
    /// in this state and the index value is likely unreliable.
    NoQuorum {
        live_components: usize,
        configured_components: usize,
    },
    /// Pair-wise spread (max − min) / median exceeds the configured
    /// `myindex_divergence_pct` threshold. Carries the actual values so
    /// the caller can log a useful diagnostic.
    Divergent {
        min: f64,
        max: f64,
        median: f64,
        spread_pct: f64,
        threshold_pct: f64,
    },
    /// At least one included component's server timestamp is older than
    /// `now_ns - myindex_staleness_ns`. Carries the worst stale component.
    Stale {
        exchange: String,
        age_ms: u64,
        threshold_ms: u64,
    },
}

impl IndexPrice {
    pub fn new(
        binance_symbol: &str,
        index_exchanges: Vec<(String, f64)>,
    ) -> Self {
        Self {
            coinbase_symbol: derive_coinbase_symbol(binance_symbol),
            okx_symbol: derive_okx_symbol(binance_symbol),
            gate_symbol: derive_gate_symbol(binance_symbol),
            kucoin_symbol: derive_kucoin_symbol(binance_symbol),
            mexc_symbol: binance_symbol.to_string(),
            bitget_symbol: binance_symbol.to_string(),
            binance_symbol: binance_symbol.to_string(),

            binance_mid: None, binance_ts_ns: 0,
            bybit_mid: None, bybit_ts_ns: 0,
            coinbase_mid: None, coinbase_ts_ns: 0,
            okx_mid: None, okx_ts_ns: 0,
            gate_mid: None, gate_ts_ns: 0,
            kucoin_mid: None, kucoin_ts_ns: 0,
            mexc_mid: None, mexc_ts_ns: 0,
            bitget_mid: None, bitget_ts_ns: 0,
            binance_futures_mid: None, binance_futures_ts_ns: 0,

            usdt_price: 1.0,

            chainlink_price: None, chainlink_ts_ns: 0,

            index_exchanges,
            // Defaults: feature disabled — preserve legacy behaviour for
            // any caller that doesn't opt in via `set_myindex_thresholds`.
            // The polymaker engine wires these from `[strategies.params]`.
            myindex_divergence_pct: 0.0,
            myindex_staleness_ns: 0,
            // Quote-path freshness gate default = 200 ms (legacy hardcoded value).
            quote_staleness_ns: 200_000_000,
            // Legacy weighted-average index timestamp (max mode opt-in).
            quote_ts_use_max: false,
            // Per-feed write-time filters (P0/P1/P2) default off so existing
            // strategies behave identically until opted in.
            myindex_max_tick_jump_pct: 0.0,
            myindex_max_bid_ask_pct: 0.0,
            myindex_max_peer_disagree_pct: 0.0,
            ewma_halflife_ms: 0.0,
            staleness_halflife_ms: 0.0,
            staleness_require_all: true,
            // Mid-price bracket off by default → only the last-trade
            // myindex is used, byte-for-byte legacy until opted in.
            midprice_bracket_enabled: false,
            midprice_bracket_strength: 1.0,
            ob_mid: HashMap::new(),
            local_ts: HashMap::new(),
            // myindex2 off by default — bit-exact legacy until the
            // engine opts in via `set_myindex2`.
            myindex2: Myindex2::default_off(),
        }
    }

    /// Set the per-feed write-time filters (P0 / P1 / P2). 0.0 disables
    /// each filter independently. See the field docs for what each does.
    pub fn set_myindex_write_filters(
        &mut self,
        max_tick_jump_pct: f64,
        max_bid_ask_pct: f64,
        max_peer_disagree_pct: f64,
    ) {
        self.myindex_max_tick_jump_pct = max_tick_jump_pct.max(0.0);
        self.myindex_max_bid_ask_pct = max_bid_ask_pct.max(0.0);
        self.myindex_max_peer_disagree_pct = max_peer_disagree_pct.max(0.0);
    }

    /// Set the per-component EWMA smoothing half-life (ms). 0 = disabled.
    pub fn set_index_ewma_halflife_ms(&mut self, hl_ms: f64) {
        self.ewma_halflife_ms = hl_ms.max(0.0);
    }

    /// Set the staleness-weight half-life (ms) for the index average. 0 = off.
    pub fn set_index_staleness_halflife_ms(&mut self, hl_ms: f64) {
        self.staleness_halflife_ms = hl_ms.max(0.0);
    }

    /// Set the staleness gate mode. `true` (default) = ALL components must
    /// be fresh; `false` = ANY (≥1 fresh keeps the index valid, stale
    /// components dropped from the average). See `staleness_require_all`.
    pub fn set_myindex_staleness_require_all(&mut self, v: bool) {
        self.staleness_require_all = v;
    }

    /// Enable/disable the orderbook-mid bracket. See `midprice_bracket_enabled`.
    pub fn set_index_midprice_bracket_enabled(&mut self, v: bool) {
        self.midprice_bracket_enabled = v;
    }

    /// Set the mid-price bracket widening strength. See `midprice_bracket_strength`.
    pub fn set_index_midprice_bracket_strength(&mut self, v: f64) {
        self.midprice_bracket_strength = v.max(0.0);
    }

    /// Per-component orderbook MID USD price (bracket mode). `None` when
    /// no orderbook has been recorded for `name`.
    pub fn exchange_ob_mid(&self, name: &str) -> Option<f64> {
        self.ob_mid.get(name).map(|&(p, _)| p)
    }

    /// Median of other live components' mids, EXCLUDING `excluded_ex`.
    /// Used by the P2 peer-disagree gate to compare an incoming mid
    /// against the consensus formed by every OTHER live feed.
    /// Returns `None` when no other component is live.
    fn peer_median_excluding(&self, excluded_ex: &str) -> Option<f64> {
        let mut prices: Vec<f64> = self.index_exchanges.iter()
            .filter(|(ex, _)| ex != excluded_ex)
            .filter_map(|(ex, _)| self.exchange_mid(ex).filter(|&v| v > 0.0))
            .collect();
        if prices.is_empty() { return None; }
        prices.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = prices.len();
        Some(if n % 2 == 1 { prices[n / 2] } else { (prices[n / 2 - 1] + prices[n / 2]) / 2.0 })
    }

    /// Set the divergence + staleness gates for `compute_myindex_validated`.
    /// 0.0 / 0 disables the respective check. Idempotent.
    ///
    /// Threshold suggestions (live):
    ///   * `divergence_pct = 0.01` (1 %) — BTC cross-exchange basis is
    ///     usually < 0.05 %; 1 % is well above noise but well below the
    ///     20–60 % divergence observed during the 2026-05-13 16:10 outage.
    ///   * `staleness_ns = 1_000_000_000` (1 s) — quote_interval is 100 ms
    ///     and the L1 feed cadence is sub-second, so 1 s gives an order
    ///     of magnitude of slack for a hiccup without locking out healthy
    ///     traffic.
    pub fn set_myindex_thresholds(&mut self, divergence_pct: f64, staleness_ns: u64) {
        self.myindex_divergence_pct = divergence_pct.max(0.0);
        self.myindex_staleness_ns = staleness_ns;
    }

    /// Set the quote-path freshness threshold (ns). See `quote_staleness_ns`.
    pub fn set_myindex_quote_staleness_ns(&mut self, ns: u64) {
        self.quote_staleness_ns = ns;
    }

    /// Set the quote-path timestamp aggregation mode. See `quote_ts_use_max`.
    pub fn set_myindex_quote_ts_max(&mut self, use_max: bool) {
        self.quote_ts_use_max = use_max;
    }








    // ── Binance-only bias corrector ────────────────────────────────────






    // ── myindex2 ───────────────────────────────────────────────────────

    /// Configure myindex2 v2. Window lengths in SAMPLES (seconds at the
    /// default 1 s cadence). `enabled = false` => factors exactly 1.0.
    pub fn set_myindex2(
        &mut self,
        enabled: bool,
        sample_interval_ns: u64,
        basis_window_n: usize,
        adjust_window_n: usize,
        staleness_threshold_ns: u64,
    ) {
        let m = &mut self.myindex2;
        m.enabled = enabled;
        m.sample_interval_ns = sample_interval_ns.max(1);
        m.basis_window_n = basis_window_n.max(1);
        m.adjust_window_n = adjust_window_n.max(1);
        m.staleness_threshold_ns = staleness_threshold_ns;
        while m.basis_samples.len() > m.basis_window_n { m.basis_samples.pop_front(); }
        while m.adjust_samples.len() > m.adjust_window_n { m.adjust_samples.pop_front(); }
    }

    /// Toggle the per-event adjust freeze. Wired from TOML
    /// `myindex2_freeze_per_event_enabled`.
    /// Freeze the index corrector for the running event — severs the
    /// self-contamination loop where late-event spot drift leaks into the
    /// correction estimate. Forwards to myindex2's per-event freeze (no-op
    /// when `myindex2_freeze_per_event_enabled = false`).
    pub fn freeze_index_for_event(&mut self) {
        self.myindex2.freeze_for_event();
    }

    pub fn set_myindex2_freeze_per_event(&mut self, enabled: bool) {
        self.myindex2.set_freeze_per_event_enabled(enabled);
    }

    /// Configure the alignment lags (ns): LOCAL-axis cb-leads-bn lead (basis
    /// pairing, `bn_local − this`) and LOCAL-axis cl-vs-bn lag (adjust pairing).
    /// Wired from TOML `myindex2_cb_lead_bn_ms` / `myindex2_cl_bn_local_lag_ms`.
    pub fn set_myindex2_lags(&mut self, cb_lead_bn_local_ns: u64, cl_bn_local_lag_ns: u64) {
        self.myindex2.cb_lead_bn_local_ns = cb_lead_bn_local_ns;
        self.myindex2.cl_bn_local_lag_ns = cl_bn_local_lag_ns;
    }

    /// Set the BINANCE staleness-age offset (ns) for the weight decay.
    /// See `Myindex2::bn_staleness_offset_ns`. Wired from TOML
    /// `myindex2_bn_staleness_offset_ms`.
    pub fn set_myindex2_bn_staleness_offset(&mut self, offset_ns: u64) {
        self.myindex2.bn_staleness_offset_ns = offset_ns;
    }

    /// The two myindex2 multipliers: `(e^{-B} on the binance leg,
    /// adjust on the whole index)`. Exactly `(1.0, 1.0)` when disabled
    /// or not yet warm — bit-exact legacy behaviour.
    pub fn myindex2_factors(&self) -> (f64, f64) {
        let m = &self.myindex2;
        if !m.enabled { return (1.0, 1.0); }
        (
            if m.basis_warm { (-m.basis).exp() } else { 1.0 },
            m.effective_adjust(),
        )
    }

    /// my0: the staleness-decayed weighted mean the validated quote path
    /// uses (same halflife + coinbase age offset), binance leg
    /// basis-corrected, NO adjust factor. This is the reference the
    /// adjust tracker measures against, so `my0 x adjust` cancels
    /// exactly the residual it estimates.
    fn compute_my0_decayed(&self, now_ns: u64) -> f64 {
        let m = &self.myindex2;
        let bn_factor = if m.basis_warm { (-m.basis).exp() } else { 1.0 };
        let bn_off = m.bn_staleness_offset_ns as f64;
        let hl_ns = self.staleness_halflife_ms * 1e6;
        let mut weighted_sum = 0.0;
        let mut total_weight = 0.0;
        for (ex, weight) in &self.index_exchanges {
            let p = match self.exchange_mid(ex) { Some(v) if v > 0.0 => v, _ => continue };
            let adjusted = if ex == "binance" { p * bn_factor } else { p };
            // LOCAL-axis age (fallback: server ts) — my0 always runs in
            // my2 mode so this mirrors the validated weighting exactly.
            let lts = self.exchange_local_ts_ns(ex);
            let age_ref = if lts > 0 { lts } else { self.exchange_ts_ns(ex) };
            let w = if hl_ns > 0.0 && age_ref > 0 && now_ns > age_ref {
                let mut age = (now_ns - age_ref) as f64;
                if bn_off > 0.0 && ex == "binance" { age += bn_off; }
                weight * 0.5_f64.powf(age / hl_ns)
            } else { *weight };
            weighted_sum += adjusted * w;
            total_weight += w;
        }
        if total_weight > 0.0 { weighted_sum / total_weight } else { 0.0 }
    }

    /// Drive the myindex2 samplers. Called once per quote/feed tick
    /// from the polymaker spot-price path; `pub` for tests.
    ///
    /// History feeds (every call, pre rate-limit): coinbase mid keyed by
    /// SERVER ts (basis-alignment ring), my0 keyed by LOCAL now (adjust
    /// lag ring).
    ///
    /// Rate-limited stages (1 Hz):
    ///   1. **Basis** — bn + cb fresh; pair the latest bn with cb as-of
    ///      server time `T_bn + bn_cb_server_lag`; push the ln ratio;
    ///      `B = mean` once >= half window.
    ///   2. **Adjust** (needs warm basis) — chainlink fresh; ratio =
    ///      `cl / my0_asof(now - cl_bn_local_lag)`; `adjust = mean` once
    ///      >= half window.
    pub fn maybe_sample_myindex2(&mut self, now_ns: u64) {
        let (enabled, interval, stale, last_ts) = {
            let m = &self.myindex2;
            (m.enabled, m.sample_interval_ns, m.staleness_threshold_ns, m.last_sample_ns)
        };
        if !enabled { return; }
        // ── history feeds (every call) ──
        // cb history is keyed by coinbase's LOCAL arrival ts (local-axis basis
        // alignment). Falls back to the exchange ts only if the local ts isn't
        // populated (never in live/backtest — both set ob.local_timestamp_ns).
        let cb_local_ts = {
            let l = self.exchange_local_ts_ns("coinbase");
            if l > 0 { l } else { self.coinbase_ts_ns }
        };
        if let Some(p) = self.coinbase_mid.filter(|&p| p > 0.0) {
            if cb_local_ts > 0 {
                let retain = self.myindex2.cb_lead_bn_local_ns + 2_000_000_000;
                Myindex2::hist_push(&mut self.myindex2.cb_hist, cb_local_ts, p, retain);
            }
        }
        {
            let my0 = self.compute_my0_decayed(now_ns);
            if my0 > 0.0 {
                let retain = self.myindex2.cl_bn_local_lag_ns + 1_500_000_000;
                Myindex2::hist_push(&mut self.myindex2.my0_hist, now_ns, my0, retain);
            }
        }
        if now_ns < last_ts.saturating_add(interval) { return; }
        // ── Stage 1: basis (LOCAL-axis aligned) ──
        // Pair the latest bn with cb as-of `bn_local − cb_lead`: cb leads bn
        // locally, so look back by the lead to compare the same economic instant.
        let bn = match self.binance_mid { Some(p) if p > 0.0 => p, _ => return };
        let bn_local_ts = {
            let l = self.exchange_local_ts_ns("binance");
            if l > 0 { l } else { self.binance_ts_ns }
        };
        if bn_local_ts == 0 || cb_local_ts == 0 { return; } // ts not yet known
        if now_ns.saturating_sub(bn_local_ts) > stale { return; }
        if now_ns.saturating_sub(cb_local_ts) > stale { return; }
        let t_cb = bn_local_ts.saturating_sub(self.myindex2.cb_lead_bn_local_ns);
        let cb = match Myindex2::hist_asof(&self.myindex2.cb_hist, t_cb) {
            Some(v) => v,
            None => return, // ring warming up — skip this sample
        };
        let b = (bn / cb).ln();
        if !b.is_finite() || b.abs() > 0.05 { return; }
        {
            let m = &mut self.myindex2;
            m.basis_samples.push_back(b);
            while m.basis_samples.len() > m.basis_window_n { m.basis_samples.pop_front(); }
            m.last_sample_ns = now_ns;
            if m.basis_samples.len() >= (m.basis_window_n / 2).max(1) {
                m.basis = m.basis_samples.iter().sum::<f64>() / m.basis_samples.len() as f64;
                m.basis_warm = true;
            }
        }
        // ── Stage 2: adjust (local-axis lagged reference) ──
        if !self.myindex2.basis_warm { return; }
        let cl = match self.chainlink_price { Some(p) if p > 0.0 => p, _ => return };
        if now_ns.saturating_sub(self.chainlink_ts_ns) > stale { return; }
        let my0_ref = match Myindex2::hist_asof(
            &self.myindex2.my0_hist,
            now_ns.saturating_sub(self.myindex2.cl_bn_local_lag_ns),
        ) {
            Some(v) if v > 0.0 => v,
            _ => return, // ring warming up — skip adjust this tick
        };
        let ratio = cl / my0_ref;
        if !ratio.is_finite() || ratio <= 0.0 || ratio.ln().abs() > 0.05 { return; }
        let m = &mut self.myindex2;
        m.adjust_samples.push_back(ratio);
        while m.adjust_samples.len() > m.adjust_window_n { m.adjust_samples.pop_front(); }
        if m.adjust_samples.len() >= (m.adjust_window_n / 2).max(1) {
            m.adjust = m.adjust_samples.iter().sum::<f64>() / m.adjust_samples.len() as f64;
            m.adjust_warm = true;
        }
    }


    /// Compact one-line summary of `compute_myindex_validated`'s sanity
    /// gates + per-feed write filters. Skips emitting `off` markers for
    /// disabled checks (zero-threshold) since seeing a 0-value is the
    /// signal — the helper is purely for greppable startup logs.
    /// Compact one-line summary of the myindex2 corrector config for
    /// SESSION SUMMARY-style logs. `off` when disabled.
    pub fn bias_summary(&self) -> String {
        let m2 = &self.myindex2;
        if !m2.enabled {
            return "myindex2: off".to_string();
        }
        format!(
            "myindex2: on  sample={:.2}s  basis_window={}  adjust_window={}  cb_lead_bn(local)={:.0}ms  cl_bn_lag={:.0}ms  staleness={:.2}s  freeze_per_event={}",
            m2.sample_interval_ns as f64 / 1e9,
            m2.basis_window_n, m2.adjust_window_n,
            m2.cb_lead_bn_local_ns as f64 / 1e6,
            m2.cl_bn_local_lag_ns as f64 / 1e6,
            m2.staleness_threshold_ns as f64 / 1e9,
            m2.freeze_per_event_enabled,
        )
    }

    pub fn gates_summary(&self) -> String {
        format!(
            "myindex_gates: divergence={:.3}%  staleness={}ms  tick_jump={:.3}%  bid_ask={:.3}%  peer_disagree={:.3}%",
            self.myindex_divergence_pct * 100.0,
            self.myindex_staleness_ns / 1_000_000,
            self.myindex_max_tick_jump_pct * 100.0,
            self.myindex_max_bid_ask_pct * 100.0,
            self.myindex_max_peer_disagree_pct * 100.0,
        )
    }

    /// Like `compute_myindex` but enforces the two operator-supplied
    /// sanity gates (pair-wise divergence, per-component staleness).
    /// Returns `Err(MyindexInvalid::*)` when either threshold is breached,
    /// so the caller can pause quoting; `Ok(price)` otherwise.
    ///
    /// `now_ns` is the wall-clock (live) or simulated (backtest) "now"
    /// reference for the staleness check. Pass `crate::types::now_ns()`
    /// in live and the event timestamp in backtest.
    ///
    /// When either threshold is 0 the corresponding check is skipped —
    /// so a caller that hasn't set them gets identical behaviour to the
    /// unguarded `compute_myindex`.
    pub fn compute_myindex_validated(&self, now_ns: u64) -> Result<f64, MyindexInvalid> {
        // ── 1. Collect live components (name, price, timestamp_ns, weight) ──
        // Binance leg gets the binance-only bias correction applied here
        // (multiplicatively), so every downstream check — divergence,
        // weighted average — operates on the post-correction price.
        // `coff = 1.0` when the binance corrector is disabled / warming
        // up, in which case this is identical to the pre-correction
        // behaviour.
        //
        // myindex2 (when enabled + warm): the binance leg is additionally
        // multiplied by `e^{−B}` (measured-basis deduction) HERE — so the
        // divergence check downstream sees the basis-corrected binance
        // sitting on top of coinbase instead of +6–9 bps above it — and
        // the final weighted average is multiplied by `e^{α̂·B}` at the
        // return. Both factors are exactly 1.0 when off ⇒ bit-exact legacy.
        let (m2_bn_factor, m2_index_factor) = self.myindex2_factors();
        let mut live: Vec<(&str, f64, u64, f64)> = Vec::with_capacity(self.index_exchanges.len());
        for (ex, weight) in &self.index_exchanges {
            if let Some(p) = self.exchange_mid(ex) {
                if p > 0.0 {
                    let adjusted = if ex == "binance" { p * m2_bn_factor } else { p };
                    let ts = self.exchange_ts_ns(ex);
                    live.push((ex.as_str(), adjusted, ts, *weight));
                }
            }
        }

        // ── 2. Quorum: need ≥ 2 live components for the divergence check to
        //   be meaningful. A single live component can't disagree with itself,
        //   so refuse to fall back to it during a feed outage. (When the
        //   operator has configured only one index_exchange, this path
        //   short-circuits below before the quorum check.)
        if self.index_exchanges.len() >= 2 && live.len() < 2 {
            return Err(MyindexInvalid::NoQuorum {
                live_components: live.len(),
                configured_components: self.index_exchanges.len(),
            });
        }
        if live.is_empty() {
            return Err(MyindexInvalid::NoQuorum {
                live_components: 0,
                configured_components: self.index_exchanges.len(),
            });
        }

        // ── 3. Staleness check (per-component, against caller-supplied "now") ──
        // Skip when threshold is 0 OR when a component has no timestamp
        // (ts == 0 means we've never received a tick for that feed; the
        // quorum check above ensures the index doesn't proceed if every
        // feed is in that state, so an isolated ts == 0 here is a feed
        // that's enabled but not yet warm — we let it pass and let
        // divergence catch any nonsense values it reports).
        if self.myindex_staleness_ns > 0 {
            let cutoff = now_ns.saturating_sub(self.myindex_staleness_ns);
            if self.staleness_require_all {
                // ── ALL mode (legacy): any stale component invalidates the
                //   whole index → quoting pauses.
                for &(name, _p, ts, _w) in &live {
                    if ts > 0 && ts < cutoff {
                        let age_ms = now_ns.saturating_sub(ts) / 1_000_000;
                        return Err(MyindexInvalid::Stale {
                            exchange: name.to_string(),
                            age_ms,
                            threshold_ms: self.myindex_staleness_ns / 1_000_000,
                        });
                    }
                }
            } else {
                // ── ANY mode: keep the index valid as long as ≥1 component
                //   is fresh. Do NOT drop stale components — the staleness-
                //   halflife weighting in the weighted average (step 5)
                //   already down-weights them *smoothly* (a hard drop would
                //   be a weight cliff at the cutoff). Stale components stay
                //   in `live` for both the divergence check and the average.
                //   Only fail when EVERY component is stale (none fresh); the
                //   error then reports the freshest (least-stale) one.
                //   ts==0 (never-warm) counts as not-stale, matching the
                //   ALL-mode skip.
                let mut any_fresh = false;
                let mut freshest_stale: Option<(&str, u64)> = None;
                for &(name, _p, ts, _w) in &live {
                    if ts == 0 || ts >= cutoff {
                        any_fresh = true;
                    } else if freshest_stale.map_or(true, |(_, fts)| ts > fts) {
                        freshest_stale = Some((name, ts));
                    }
                }
                if !any_fresh {
                    let (name, ts) = freshest_stale.unwrap_or(("?", 0));
                    let age_ms = now_ns.saturating_sub(ts) / 1_000_000;
                    return Err(MyindexInvalid::Stale {
                        exchange: name.to_string(),
                        age_ms,
                        threshold_ms: self.myindex_staleness_ns / 1_000_000,
                    });
                }
                // ≥1 fresh → proceed with ALL components; the halflife decay
                // (step 5) handles down-weighting the stale ones.
            }
        }

        // ── 4. Divergence check (pair-wise relative spread vs median) ──
        if self.myindex_divergence_pct > 0.0 && live.len() >= 2 {
            let mut prices: Vec<f64> = live.iter().map(|t| t.1).collect();
            prices.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let min = prices[0];
            let max = prices[prices.len() - 1];
            let median = if prices.len() % 2 == 1 {
                prices[prices.len() / 2]
            } else {
                (prices[prices.len() / 2 - 1] + prices[prices.len() / 2]) / 2.0
            };
            if median > 0.0 {
                let spread_pct = (max - min) / median;
                if spread_pct > self.myindex_divergence_pct {
                    return Err(MyindexInvalid::Divergent {
                        min, max, median, spread_pct,
                        threshold_pct: self.myindex_divergence_pct,
                    });
                }
            }
        }

        // ── 5. All gates passed — return the weighted average. Recomputed
        //   here from `live` so we don't double-iterate over `index_exchanges`.
        // Staleness weighting: multiply each base weight by
        // 0.5^((now − ts)/halflife) so older components count less.
        // halflife 0 ⇒ flat weights (legacy). Out-of-order / ts==0 ⇒ no decay.
        let hl_ns = self.staleness_halflife_ms * 1e6;
        // myindex2 mode: decay ages on the LOCAL axis (now − local
        // arrival) so each feed's transport delay doesn't leak into the
        // weights, + binance age offset (local-axis informational lag
        // ~39-50 ms). Falls back to the server ts when no local arrival
        // recorded yet. my2 off ⇒ server-ts ages, bit-exact legacy.
        let m2_on = self.myindex2.enabled;
        let m2_bn_off = if m2_on { self.myindex2.bn_staleness_offset_ns as f64 } else { 0.0 };
        let mut weighted_sum = 0.0;
        let mut total_weight = 0.0;
        for &(name, p, ts, w) in &live {
            let age_ref = if m2_on {
                let lts = self.exchange_local_ts_ns(name);
                if lts > 0 { lts } else { ts }
            } else { ts };
            let sw = if hl_ns > 0.0 && age_ref > 0 && now_ns > age_ref {
                let mut age = (now_ns - age_ref) as f64;
                if m2_bn_off > 0.0 && name == "binance" { age += m2_bn_off; }
                w * 0.5_f64.powf(age / hl_ns)
            } else { w };
            weighted_sum += p * sw;
            total_weight += sw;
        }
        // myindex2 α add-back (×1.0 when off — bit-exact legacy).
        Ok(if total_weight > 0.0 { (weighted_sum / total_weight) * m2_index_factor } else { 0.0 })
    }

    /// Re-derive all exchange symbols when binance_symbol changes dynamically.
    pub fn update_binance_symbol(&mut self, binance_symbol: &str) {
        self.binance_symbol = binance_symbol.to_string();
        self.coinbase_symbol = derive_coinbase_symbol(binance_symbol);
        self.okx_symbol = derive_okx_symbol(binance_symbol);
        self.gate_symbol = derive_gate_symbol(binance_symbol);
        self.kucoin_symbol = derive_kucoin_symbol(binance_symbol);
        self.mexc_symbol = binance_symbol.to_string();
        self.bitget_symbol = binance_symbol.to_string();
    }

    /// Get the per-exchange USD mid price.
    pub fn exchange_mid(&self, name: &str) -> Option<f64> {
        match name {
            "binance" => self.binance_mid,
            "bybit" => self.bybit_mid,
            "coinbase" => self.coinbase_mid,
            "okx" => self.okx_mid,
            "gate" => self.gate_mid,
            "kucoin" => self.kucoin_mid,
            "mexc" => self.mexc_mid,
            "bitget" => self.bitget_mid,
            "binance_futures" => self.binance_futures_mid,
            // chainlink as an index component: it's a single-price stream
            // (no book), so its "mid" is the chainlink price itself.
            // `chainlink_price` is already Option<f64> → slots in directly.
            "chainlink" => self.chainlink_price,
            _ => None,
        }
    }

    /// Per-component LOCAL arrival ts (ns) of the last accepted update.
    /// 0 when never recorded (e.g. slots seeded directly in tests).
    pub fn exchange_local_ts_ns(&self, name: &str) -> u64 {
        self.local_ts.get(name).copied().unwrap_or(0)
    }

    /// Get the per-exchange timestamp (ns).
    pub fn exchange_ts_ns(&self, name: &str) -> u64 {
        match name {
            "binance" => self.binance_ts_ns,
            "bybit" => self.bybit_ts_ns,
            "coinbase" => self.coinbase_ts_ns,
            "okx" => self.okx_ts_ns,
            "gate" => self.gate_ts_ns,
            "kucoin" => self.kucoin_ts_ns,
            "mexc" => self.mexc_ts_ns,
            "bitget" => self.bitget_ts_ns,
            "binance_futures" => self.binance_futures_ts_ns,
            "chainlink" => self.chainlink_ts_ns,
            _ => 0,
        }
    }

    /// myindex: weighted average of `index_exchanges` mid prices
    /// (all already USD-converted).
    ///
    /// **Binance basis correction** (myindex2): when myindex2 is enabled
    /// the binance leg's mid is multiplied by `e^{−B}` (measured-basis
    /// deduction) before it enters the weighted sum; off ⇒ raw mids.
    pub fn compute_myindex(&self) -> f64 {
        let (bn_factor, index_factor) = self.myindex2_factors();
        self.compute_myindex_inner(bn_factor, index_factor)
    }

    /// Weighted-average core shared by `compute_myindex` (live factors)
    /// and the myindex2 α sampler (`bn_factor = e^{−B}`,
    /// `index_factor = 1` → the `my0` reference). Factors of exactly
    /// 1.0 reproduce the legacy value bit-for-bit.
    fn compute_myindex_inner(&self, bn_factor: f64, index_factor: f64) -> f64 {
        let mut weighted_sum = 0.0;
        let mut total_weight = 0.0;
        for (ex, weight) in &self.index_exchanges {
            if let Some(p) = self.exchange_mid(ex) {
                if p > 0.0 {
                    let adjusted = if ex == "binance" { p * bn_factor } else { p };
                    weighted_sum += adjusted * weight;
                    total_weight += weight;
                }
            }
        }
        if total_weight > 0.0 { (weighted_sum / total_weight) * index_factor } else { 0.0 }
    }

    /// Bracket indices for `midprice_bracket_enabled`: returns
    /// `(trade_idx, mid_idx)` — the component-weighted **last-trade** price
    /// index and the component-weighted **orderbook-mid** price index —
    /// computed over the SAME component set with the SAME half-life-decayed
    /// weights. The decay uses each component's **last-trade timestamp**
    /// (`exchange_ts_ns`, the trade clock — the same clock the primary
    /// `myindex` decays on), so sharing the weights means `mid_idx /
    /// trade_idx` isolates purely the **mid-vs-trade price gap** (no weight
    /// difference leaks in). The binance bias `coff` is applied to the
    /// binance leg of BOTH legs (cancels in the ratio). Only components with
    /// BOTH a live last-trade price AND a live orderbook mid contribute; a
    /// component with trade ts == 0 (never traded) is not decayed (flat
    /// weight), matching the staleness gate's never-warm convention. Returns
    /// RAW values (no chainlink ↔ myindex `coff` — the caller applies the
    /// SAME one it uses for the center spot). `None` when no component has
    /// both prices live.
    pub fn compute_bracket_indices(&self, now_ns: u64) -> Option<(f64, f64)> {
        // myindex2 factors apply to both legs identically (basis factor on
        // the binance component, α factor on the aggregates) so the
        // mid-vs-trade gap the bracket isolates is unchanged. 1.0/1.0 when off.
        let (m2_bn_factor, m2_index_factor) = self.myindex2_factors();
        let hl_ns = self.staleness_halflife_ms * 1e6;
        let m2_bn_off = if self.myindex2.enabled {
            self.myindex2.bn_staleness_offset_ns as f64
        } else { 0.0 };
        let mut trade_sum = 0.0;
        let mut mid_sum = 0.0;
        let mut total_weight = 0.0;
        for (ex, weight) in &self.index_exchanges {
            let tp = match self.exchange_mid(ex) { Some(v) if v > 0.0 => v, _ => continue };
            let mp = match self.exchange_ob_mid(ex) { Some(v) if v > 0.0 => v, _ => continue };
            let coff = if ex == "binance" { m2_bn_factor } else { 1.0 };
            // Decay by the LAST-TRADE timestamp (trade clock), shared by
            // both legs. my2 mode: local-axis age (fallback server ts).
            let ts = self.exchange_ts_ns(ex);
            let age_ref = if self.myindex2.enabled {
                let lts = self.exchange_local_ts_ns(ex);
                if lts > 0 { lts } else { ts }
            } else { ts };
            let w = if hl_ns > 0.0 && age_ref > 0 && now_ns > age_ref {
                let mut age = (now_ns - age_ref) as f64;
                if m2_bn_off > 0.0 && ex == "binance" { age += m2_bn_off; }
                weight * 0.5_f64.powf(age / hl_ns)
            } else { *weight };
            trade_sum += tp * coff * w;
            mid_sum += mp * coff * w;
            total_weight += w;
        }
        if total_weight > 0.0 {
            Some((
                (trade_sum / total_weight) * m2_index_factor,
                (mid_sum / total_weight) * m2_index_factor,
            ))
        } else {
            None
        }
    }

    /// Weighted average server timestamp (ns) of index components.
    /// Returns 0 if no components have data.
    /// **Axis note (2026-06-12):** stays on SERVER timestamps by
    /// decision — a local-axis variant (LOCAL arrival ts + threshold
    /// 200→130 ms equivalence translation) was implemented, A/B'd
    /// NEUTRAL (5wk Δ−30 t−0.03, vol −0.6%) and then REVERTED. The
    /// server-axis age embeds a ~69 ms transport floor at 1:2 weights,
    /// so the 200 ms gate ≈ 131 ms of local quiet time; the threshold
    /// is TUNED in this metric (500 ms relax = Δ−4116 t−5.76).
    pub fn index_timestamp_ns(&self) -> u64 {
        // Max mode (`myindex_quote_ts_max`): latest per-component server
        // timestamp — age = time since the FRESHEST component updated.
        if self.quote_ts_use_max {
            let mut max_ts = 0_u64;
            for (ex, _) in &self.index_exchanges {
                let ts = self.exchange_ts_ns(ex);
                if ts > max_ts && self.exchange_mid(ex).map(|p| p > 0.0).unwrap_or(false) {
                    max_ts = ts;
                }
            }
            return max_ts;
        }
        let mut weighted_sum = 0.0_f64;
        let mut total_weight = 0.0_f64;
        for (ex, weight) in &self.index_exchanges {
            let ts = self.exchange_ts_ns(ex);
            if ts > 0 && self.exchange_mid(ex).map(|p| p > 0.0).unwrap_or(false) {
                weighted_sum += ts as f64 * weight;
                total_weight += weight;
            }
        }
        if total_weight > 0.0 { (weighted_sum / total_weight) as u64 } else { 0 }
    }

    /// Compute median of all valid exchange mid prices.
    pub fn compute_median(&self) -> f64 {
        let mut prices: Vec<f64> = self.index_exchanges.iter()
            .filter_map(|(ex, _)| self.exchange_mid(ex).filter(|&v| v > 0.0))
            .collect();
        if prices.is_empty() { return 0.0; }
        prices.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = prices.len();
        if n % 2 == 1 { prices[n / 2] } else { (prices[n / 2 - 1] + prices[n / 2]) / 2.0 }
    }

    /// Match an OrderBook's exchange+symbol to a known spot exchange name.
    pub fn match_exchange(&self, exchange: Exchange, symbol: &str) -> Option<&'static str> {
        match exchange {
            Exchange::Binance if symbol == self.binance_symbol => Some("binance"),
            Exchange::Bybit if symbol == self.binance_symbol => Some("bybit"),
            Exchange::Coinbase if symbol == self.coinbase_symbol => Some("coinbase"),
            Exchange::Okx if symbol == self.okx_symbol => Some("okx"),
            Exchange::Gate if symbol == self.gate_symbol => Some("gate"),
            Exchange::Kucoin if symbol == self.kucoin_symbol => Some("kucoin"),
            Exchange::Mexc if symbol == self.mexc_symbol => Some("mexc"),
            Exchange::Bitget if symbol == self.bitget_symbol => Some("bitget"),
            _ => None,
        }
    }

    /// Update from an orderbook snapshot. Returns the exchange name if
    /// matched **and** the new mid passes the per-feed write-time
    /// filters (P0 / P1 / P2). Returns `None` (no state change) when:
    ///   * exchange/symbol doesn't match,
    ///   * either side of L1 is empty,
    ///   * raw mid is non-positive,
    ///   * P1: bid-ask spread > `myindex_max_bid_ask_pct`,
    ///   * P0: |new_mid / prev_mid - 1| > `myindex_max_tick_jump_pct`,
    ///   * P2: |new_mid / peer_median - 1| > `myindex_max_peer_disagree_pct`.
    ///
    /// Each filter is independently disabled when its threshold is 0.0.
    /// Update a component's **orderbook MID** leg for the mid-price
    /// bracket (`midprice_bracket_enabled`). Stores the USD-converted L1
    /// mid in the SEPARATE `ob_mid` map (distinct from the last-trade
    /// `*_mid` slots that drive the primary myindex). Applies the same P1
    /// bid-ask-spread sanity reject as `update_from_orderbook`. Returns
    /// the exchange name when matched and accepted.
    pub fn update_ob_mid_from_orderbook(&mut self, ob: &OrderBookSnapshot) -> Option<&'static str> {
        let ex_name = self.match_exchange(ob.exchange, &ob.symbol)?;
        let best_bid = ob.best_bid()?;
        let best_ask = ob.best_ask()?;
        // P1: reject stub-quote / blown-out L1 spreads.
        if self.myindex_max_bid_ask_pct > 0.0 && best_bid.price > 0.0 {
            let spread_pct = (best_ask.price - best_bid.price) / best_bid.price;
            if spread_pct > self.myindex_max_bid_ask_pct { return None; }
        }
        let raw_mid = (best_bid.price + best_ask.price) / 2.0;
        if raw_mid <= 0.0 { return None; }
        let mid = if is_usd_exchange(ex_name) { raw_mid } else { raw_mid * self.usdt_price };
        self.ob_mid.insert(ex_name.to_string(), (mid, ob.exchange_timestamp_ns));
        Some(ex_name)
    }

    pub fn update_from_orderbook(&mut self, ob: &OrderBookSnapshot) -> Option<&'static str> {
        let ex_name = self.match_exchange(ob.exchange, &ob.symbol)?;
        let best_bid = ob.best_bid()?;
        let best_ask = ob.best_ask()?;

        // ── P1: bid-ask spread sanity ────────────────────────────────
        //
        // A healthy BTC L1 has spread < 0.05 %. A "stub quote" on one
        // side (e.g. a far-out maker placeholder briefly resting at the
        // top) inflates `(bid+ask)/2` and pulls the mid away from fair.
        // Reject the whole OB before computing raw_mid.
        if self.myindex_max_bid_ask_pct > 0.0 && best_bid.price > 0.0 {
            let spread_pct = (best_ask.price - best_bid.price) / best_bid.price;
            if spread_pct > self.myindex_max_bid_ask_pct {
                log::trace!(
                    "[index] {} OB rejected (P1 spread {:.3}% > {:.3}%): bid={} ask={}",
                    ex_name, spread_pct * 100.0,
                    self.myindex_max_bid_ask_pct * 100.0,
                    best_bid.price, best_ask.price,
                );
                return None;
            }
        }

        let raw_mid = (best_bid.price + best_ask.price) / 2.0;
        if raw_mid <= 0.0 { return None; }

        let mid = if is_usd_exchange(ex_name) { raw_mid } else { raw_mid * self.usdt_price };

        // ── P0: per-feed tick-to-tick mid jump clamp ────────────────
        //
        // Real BTC moves < 0.1 % per 100 ms tick. A 0.5 % jump is well
        // above noise but well below the 3-6 % jumps observed in the
        // 2026-05-13 18:17:50 cluster. Single-tick L1 glitches are
        // suppressed; the feed's mid stays at its last reasonable
        // value until the next OB tick brings it back in line.
        if self.myindex_max_tick_jump_pct > 0.0 {
            if let Some(prev) = self.exchange_mid(ex_name) {
                if prev > 0.0 {
                    let pct = (mid / prev - 1.0).abs();
                    if pct > self.myindex_max_tick_jump_pct {
                        log::trace!(
                            "[index] {} OB rejected (P0 tick-jump {:.3}% > {:.3}%): prev={:.2} new={:.2}",
                            ex_name, pct * 100.0,
                            self.myindex_max_tick_jump_pct * 100.0,
                            prev, mid,
                        );
                        return None;
                    }
                }
            }
        }

        // ── P2: per-feed peer-disagree clamp at write time ─────────
        //
        // Compare the incoming mid against the median of OTHER live
        // components. A rogue feed reporting a sustained-but-wrong
        // value gets blocked here so the aggregation never sees it,
        // even if its own per-tick changes look smooth (P0 only
        // catches deltas, not sustained offsets from peers).
        // Skipped when there's no peer to compare against — that's
        // a quorum problem the aggregation layer handles separately.
        if self.myindex_max_peer_disagree_pct > 0.0 {
            if let Some(peer_med) = self.peer_median_excluding(ex_name) {
                if peer_med > 0.0 {
                    let pct = (mid / peer_med - 1.0).abs();
                    if pct > self.myindex_max_peer_disagree_pct {
                        log::trace!(
                            "[index] {} OB rejected (P2 peer-disagree {:.3}% > {:.3}%): new={:.2} peer_median={:.2}",
                            ex_name, pct * 100.0,
                            self.myindex_max_peer_disagree_pct * 100.0,
                            mid, peer_med,
                        );
                        return None;
                    }
                }
            }
        }

        // ── Per-component time-aware EWMA smoothing (component-first) ──
        // Smooth the validated USD mid before it enters the weighted sum.
        // α = 1 − 0.5^(Δt_ms / halflife); large Δt (feed gap) ⇒ α→1 ⇒
        // raw mid (no stale carry). First sample / out-of-order ⇒ raw.
        let mid = if self.ewma_halflife_ms > 0.0 {
            match self.exchange_mid(ex_name) {
                Some(prev) if prev > 0.0 && ob.exchange_timestamp_ns > self.exchange_ts_ns(ex_name) => {
                    let dt_ms = (ob.exchange_timestamp_ns - self.exchange_ts_ns(ex_name)) as f64 / 1e6;
                    let alpha = 1.0 - 0.5_f64.powf(dt_ms / self.ewma_halflife_ms);
                    alpha * mid + (1.0 - alpha) * prev
                }
                _ => mid,
            }
        } else { mid };

        match ex_name {
            "binance" => { self.binance_mid = Some(mid); self.binance_ts_ns = ob.exchange_timestamp_ns; }
            "bybit" => { self.bybit_mid = Some(mid); self.bybit_ts_ns = ob.exchange_timestamp_ns; }
            "coinbase" => { self.coinbase_mid = Some(mid); self.coinbase_ts_ns = ob.exchange_timestamp_ns; }
            "okx" => { self.okx_mid = Some(mid); self.okx_ts_ns = ob.exchange_timestamp_ns; }
            "gate" => { self.gate_mid = Some(mid); self.gate_ts_ns = ob.exchange_timestamp_ns; }
            "kucoin" => { self.kucoin_mid = Some(mid); self.kucoin_ts_ns = ob.exchange_timestamp_ns; }
            "mexc" => { self.mexc_mid = Some(mid); self.mexc_ts_ns = ob.exchange_timestamp_ns; }
            "bitget" => { self.bitget_mid = Some(mid); self.bitget_ts_ns = ob.exchange_timestamp_ns; }
            _ => return None,
        }
        if ob.local_timestamp_ns > 0 {
            self.local_ts.insert(ex_name.to_string(), ob.local_timestamp_ns);
        }

        Some(ex_name)
    }

    /// Update a component price from a public trade tick. This is how the
    /// polymaker strategy drives the myindex component prices (last-trade
    /// based). Mirrors `update_from_orderbook`'s P0/P2/EWMA/store, but uses
    /// the trade price (no L1 ⇒ P1 spread check is skipped). Returns the
    /// exchange name when matched and accepted.
    pub fn update_from_trade(&mut self, exchange: Exchange, symbol: &str, price: f64, ts_ns: u64, local_ts_ns: u64) -> Option<&'static str> {
        let ex_name = self.match_exchange(exchange, symbol)?;
        if price <= 0.0 { return None; }
        let mut mid = if is_usd_exchange(ex_name) { price } else { price * self.usdt_price };
        // P0: tick-to-tick jump clamp (vs last stored component value)
        if self.myindex_max_tick_jump_pct > 0.0 {
            if let Some(prev) = self.exchange_mid(ex_name) {
                if prev > 0.0 && (mid / prev - 1.0).abs() > self.myindex_max_tick_jump_pct {
                    return None;
                }
            }
        }
        // P2: peer-disagree clamp vs median of other live feeds
        if self.myindex_max_peer_disagree_pct > 0.0 {
            if let Some(pm) = self.peer_median_excluding(ex_name) {
                if pm > 0.0 && (mid / pm - 1.0).abs() > self.myindex_max_peer_disagree_pct {
                    return None;
                }
            }
        }
        // Per-component time-aware EWMA (same as OB path; off by default)
        if self.ewma_halflife_ms > 0.0 {
            if let Some(prev) = self.exchange_mid(ex_name) {
                if prev > 0.0 && ts_ns > self.exchange_ts_ns(ex_name) {
                    let dt_ms = (ts_ns - self.exchange_ts_ns(ex_name)) as f64 / 1e6;
                    let a = 1.0 - 0.5_f64.powf(dt_ms / self.ewma_halflife_ms);
                    mid = a * mid + (1.0 - a) * prev;
                }
            }
        }
        match ex_name {
            "binance" => { self.binance_mid = Some(mid); self.binance_ts_ns = ts_ns; }
            "bybit" => { self.bybit_mid = Some(mid); self.bybit_ts_ns = ts_ns; }
            "coinbase" => { self.coinbase_mid = Some(mid); self.coinbase_ts_ns = ts_ns; }
            "okx" => { self.okx_mid = Some(mid); self.okx_ts_ns = ts_ns; }
            "gate" => { self.gate_mid = Some(mid); self.gate_ts_ns = ts_ns; }
            "kucoin" => { self.kucoin_mid = Some(mid); self.kucoin_ts_ns = ts_ns; }
            "mexc" => { self.mexc_mid = Some(mid); self.mexc_ts_ns = ts_ns; }
            "bitget" => { self.bitget_mid = Some(mid); self.bitget_ts_ns = ts_ns; }
            _ => return None,
        }
        if local_ts_ns > 0 {
            self.local_ts.insert(ex_name.to_string(), local_ts_ns);
        }
        Some(ex_name)
    }

    /// Update USDT/USD exchange rate (from Chainlink Data Streams or Pyth "usdt/usd").
    pub fn update_usdt_price(&mut self, price: f64) {
        if price > 0.0 {
            self.usdt_price = price;
        }
    }

    /// Update chainlink price.
    pub fn update_chainlink(&mut self, price: f64, timestamp_ns: u64) {
        if price > 0.0 {
            self.chainlink_price = Some(price);
            self.chainlink_ts_ns = timestamp_ns;
        }
    }

    /// Handle a SpotPrice event: routes usdt/usd to update_usdt_price,
    /// binance_futures btc prices to binance_futures_mid. Returns true if handled.
    pub fn on_spot_price(&mut self, sp: &SpotPrice) -> bool {
        if sp.price <= 0.0 { return false; }

        if sp.symbol.eq_ignore_ascii_case("usdt/usd") {
            // Debug visibility into the live FX rate that's applied to
            // every USDT-denominated exchange (binance / bybit / okx /
            // gate / kucoin / mexc / bitget) inside `update_from_orderbook`.
            // Emitted at debug! so the operator can opt-in via
            // log_level="debug"; one line per upstream tick (~1 Hz).
            log::debug!(
                "[fx] usdt/usd from {} = {:.8}  ts={}  prev={:.8}",
                sp.source, sp.price, sp.timestamp_ns, self.usdt_price,
            );
            self.update_usdt_price(sp.price);
            return true;
        }

        // Binance Futures BTC/USD asset index → store as binance_futures_mid (already USD)
        if sp.source == "binance_futures" && !sp.symbol.eq_ignore_ascii_case("usdt/usd") {
            self.binance_futures_mid = Some(sp.price);
            self.binance_futures_ts_ns = sp.timestamp_ns;
            return true;
        }

        false
    }

    /// Format all exchange symbols for logging.
    pub fn fmt_symbols(&self) -> String {
        format!("binance={}, coinbase={}, okx={}, gate={}, kucoin={}, mexc={}, bitget={}",
            self.binance_symbol, self.coinbase_symbol, self.okx_symbol,
            self.gate_symbol, self.kucoin_symbol, self.mexc_symbol, self.bitget_symbol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: construct an IndexPrice with two healthy components
    /// (binance + coinbase, both USD-equivalent) seeded with the
    /// given mids and timestamps.
    fn fresh(binance_mid: f64, binance_ts: u64, coinbase_mid: f64, coinbase_ts: u64) -> IndexPrice {
        let mut ip = IndexPrice::new(
            "BTCUSDT",
            vec![("binance".to_string(), 1.0), ("coinbase".to_string(), 2.0)],
        );
        ip.binance_mid = Some(binance_mid);
        ip.binance_ts_ns = binance_ts;
        ip.coinbase_mid = Some(coinbase_mid);
        ip.coinbase_ts_ns = coinbase_ts;
        ip
    }

    /// With both gates disabled (default), `compute_myindex_validated`
    /// must return the same weighted-average price as `compute_myindex`.
    /// Locks the "opt-in" property — turning the feature off is identical
    /// to the legacy behaviour.
    #[test]
    fn validated_matches_legacy_when_gates_disabled() {
        let ip = fresh(79_000.0, 1_000_000, 79_100.0, 1_000_000);
        let unguarded = ip.compute_myindex();
        let guarded = ip.compute_myindex_validated(1_000_000)
            .expect("disabled gates must not reject any input");
        assert!((guarded - unguarded).abs() < 1e-6,
            "validated path must match legacy path when gates disabled (legacy={} guarded={})",
            unguarded, guarded);
    }

    /// The bracket builds a trade index (last-trade prices) and a mid index
    /// (orderbook mids) over the SAME decayed weights, so the pair isolates
    /// the mid-vs-trade price gap. Both use component weights (binance 1,
    /// coinbase 2).
    #[test]
    fn bracket_indices_share_weights_isolate_price_gap() {
        let now = 10_000_000_000u64;
        let mut ip = fresh(100.0, now, 200.0, now); // last-trade slots
        // No orderbook mids yet → None.
        assert!(ip.compute_bracket_indices(now).is_none());
        // Seed OB mids (different from the trade prices), same age.
        ip.ob_mid.insert("binance".to_string(), (110.0, now));
        ip.ob_mid.insert("coinbase".to_string(), (210.0, now));
        ip.set_index_staleness_halflife_ms(1000.0);
        let (trade_idx, mid_idx) = ip.compute_bracket_indices(now).unwrap();
        // trade: binance w1 @100, coinbase w2 @200 → (100 + 400)/3.
        assert!((trade_idx - (500.0 / 3.0)).abs() < 1e-6, "trade {}", trade_idx);
        // mid:   binance w1 @110, coinbase w2 @210 → (110 + 420)/3.
        assert!((mid_idx - (530.0 / 3.0)).abs() < 1e-6, "mid {}", mid_idx);
    }

    /// Divergence within threshold passes; equal-or-just-above the
    /// threshold rejects with the actual spread + values reported.
    /// Regression guard for the 2026-05-13 16:10 outage: binance 79k
    /// + coinbase 92k would have a 17 % spread, far above any
    /// reasonable threshold.
    #[test]
    fn divergence_gate_fires_at_threshold() {
        // Same-price → spread = 0 → passes any threshold.
        let mut ip = fresh(79_000.0, 1_000, 79_000.0, 1_000);
        ip.set_myindex_thresholds(0.01, 0);
        assert!(ip.compute_myindex_validated(1_000).is_ok());

        // 0.5 % spread, threshold 1 % → passes.
        let mut ip = fresh(79_000.0, 1_000, 79_400.0, 1_000);
        ip.set_myindex_thresholds(0.01, 0);
        assert!(ip.compute_myindex_validated(1_000).is_ok(),
            "0.5% spread should pass under 1% threshold");

        // Live-outage shape: 79k vs 92k = ~17 % spread → reject.
        let mut ip = fresh(79_000.0, 1_000, 92_000.0, 1_000);
        ip.set_myindex_thresholds(0.01, 0);
        match ip.compute_myindex_validated(1_000) {
            Err(MyindexInvalid::Divergent { min, max, spread_pct, threshold_pct, .. }) => {
                assert_eq!(min, 79_000.0);
                assert_eq!(max, 92_000.0);
                assert!(spread_pct > 0.1, "spread should be > 10%, got {}", spread_pct);
                assert_eq!(threshold_pct, 0.01);
            }
            other => panic!("expected Divergent, got {:?}", other),
        }
    }

    /// Staleness gate: any one component older than threshold rejects.
    /// `now_ns` controls the comparison; uses ts_event in live, sim time
    /// in backtest. Threshold 0 disables. Component ts == 0 (never updated)
    /// is treated as "warming up", not stale — quorum will catch it.
    #[test]
    fn staleness_gate_per_component() {
        // Both fresh at now_ns=2_000_000_000 (2s): passes.
        let mut ip = fresh(79_000.0, 1_500_000_000, 79_050.0, 1_900_000_000);
        ip.set_myindex_thresholds(0.0, 1_000_000_000); // 1s
        assert!(ip.compute_myindex_validated(2_000_000_000).is_ok());

        // Binance frozen 3s ago, coinbase still fresh: reject with binance name.
        let mut ip = fresh(79_000.0, 1_000_000_000, 79_050.0, 3_900_000_000);
        ip.set_myindex_thresholds(0.0, 1_000_000_000);
        match ip.compute_myindex_validated(4_000_000_000) {
            Err(MyindexInvalid::Stale { exchange, age_ms, threshold_ms }) => {
                assert_eq!(exchange, "binance");
                assert_eq!(age_ms, 3_000);
                assert_eq!(threshold_ms, 1_000);
            }
            other => panic!("expected Stale(binance), got {:?}", other),
        }

        // Threshold 0 disables the check even when feeds are ancient.
        let mut ip = fresh(79_000.0, 1, 79_050.0, 1);
        ip.set_myindex_thresholds(0.0, 0);
        assert!(ip.compute_myindex_validated(1_000_000_000_000).is_ok(),
            "staleness_ns=0 must disable the staleness check");
    }

    /// Quorum: when ≥ 2 components are configured but fewer than 2 have
    /// reported a live price, refuse to fall through to a single-feed
    /// "average". Single-feed configurations don't trip this check.
    #[test]
    fn quorum_requires_two_live_when_two_configured() {
        // Only binance reporting, coinbase missing → NoQuorum.
        let mut ip = IndexPrice::new("BTCUSDT",
            vec![("binance".to_string(), 1.0), ("coinbase".to_string(), 2.0)]);
        ip.binance_mid = Some(79_000.0);
        ip.binance_ts_ns = 1_000_000_000;
        ip.set_myindex_thresholds(0.01, 1_000_000_000);
        match ip.compute_myindex_validated(1_500_000_000) {
            Err(MyindexInvalid::NoQuorum { live_components, configured_components }) => {
                assert_eq!(live_components, 1);
                assert_eq!(configured_components, 2);
            }
            other => panic!("expected NoQuorum, got {:?}", other),
        }

        // Single-feed config + that feed live → no quorum check fires.
        let mut ip = IndexPrice::new("BTCUSDT", vec![("binance".to_string(), 1.0)]);
        ip.binance_mid = Some(79_000.0);
        ip.binance_ts_ns = 1_000_000_000;
        ip.set_myindex_thresholds(0.01, 1_000_000_000);
        assert!(ip.compute_myindex_validated(1_500_000_000).is_ok(),
            "single configured feed shouldn't trip quorum");
    }

    /// Combined: divergence + staleness checks run in a defined order.
    /// Staleness wins over divergence — a frozen feed should be reported
    /// as stale rather than divergent (a stale price tends to disagree
    /// with the live one, and "frozen" is the more actionable signal).
    #[test]
    fn staleness_reported_before_divergence_when_both_breach() {
        // Stale binance (3s old, frozen at high price 92k) + fresh
        // coinbase (79k). Both gates would fire (17 % spread, 3s age).
        // Staleness should win.
        let mut ip = fresh(92_000.0, 1_000_000_000, 79_000.0, 3_900_000_000);
        ip.set_myindex_thresholds(0.01, 1_000_000_000);
        match ip.compute_myindex_validated(4_000_000_000) {
            Err(MyindexInvalid::Stale { exchange, .. }) => {
                assert_eq!(exchange, "binance",
                    "staleness should win over divergence on the frozen feed");
            }
            other => panic!("expected Stale, got {:?}", other),
        }
    }

    /// Regression guard for the 2026-05-13 18:00 "live + warm-up replay"
    /// outage: the polymaker engine replayed 24 h of historical parquet
    /// orderbooks for prediction warm-up while running in live mode. Both
    /// `binance_ts_ns` and `coinbase_ts_ns` got set to historical (5-12)
    /// timestamps, and the caller passed `ob.exchange_timestamp_ns` (also
    /// historical) as the gate's "now" — so the gate compared "historical
    /// ts" vs "historical-minus-1s" and decided everything was fresh. The
    /// engine then quoted against a 24 h-old BTC price.
    ///
    /// The fix is at the caller (polymaker strategy uses wall-clock for
    /// `now_ns` in live mode) but the gate behaviour locked in here is the
    /// invariant: when `now_ns` is far ahead of `ts_ns`, the gate MUST
    /// fire `Stale` regardless of how close the components agree.
    /// Helper: construct an `OrderBookSnapshot` with a single L1 level.
    /// Used by the P0/P1/P2 write-filter tests.
    fn ob(exchange: Exchange, symbol: &str, bid: f64, ask: f64, ts_ns: u64) -> OrderBookSnapshot {
        use crate::types::PriceLevel;
        OrderBookSnapshot {
            exchange,
            symbol: symbol.to_string(),
            bids: vec![PriceLevel { price: bid, quantity: 1.0 }],
            asks: vec![PriceLevel { price: ask, quantity: 1.0 }],
            exchange_timestamp_ns: ts_ns,
            local_timestamp_ns: ts_ns,
        }
    }

    /// P0: per-feed tick-to-tick mid jump above the threshold is rejected
    /// (the OB tick is dropped, the feed's mid stays at its previous value).
    /// Threshold = 0 disables the check.
    /// Regression guard for the 2026-05-13 18:17:50 single-tick L1 glitch.
    #[test]
    fn p0_tick_jump_clamp_rejects_above_threshold() {
        let mut ip = IndexPrice::new("BTCUSDT",
            vec![("binance".to_string(), 1.0), ("coinbase".to_string(), 1.0)]);
        ip.set_myindex_write_filters(0.005, 0.0, 0.0); // P0 only

        // First tick: no prior mid → accepted unconditionally.
        assert_eq!(ip.update_from_orderbook(
            &ob(Exchange::Binance, "BTCUSDT", 79_000.0, 79_010.0, 1_000_000_000)
        ), Some("binance"));
        assert_eq!(ip.exchange_mid("binance"), Some(79_005.0));

        // 0.1 % move → accepted (well within 0.5 % threshold).
        assert_eq!(ip.update_from_orderbook(
            &ob(Exchange::Binance, "BTCUSDT", 79_080.0, 79_090.0, 1_000_000_100)
        ), Some("binance"));
        assert_eq!(ip.exchange_mid("binance"), Some(79_085.0));

        // 5 % move → rejected, mid stays at 79_085.
        assert_eq!(ip.update_from_orderbook(
            &ob(Exchange::Binance, "BTCUSDT", 83_000.0, 83_010.0, 1_000_000_200)
        ), None);
        assert_eq!(ip.exchange_mid("binance"), Some(79_085.0),
            "P0 must keep prior mid when new mid jumps > threshold");

        // Disable P0 → same jump now accepted.
        ip.set_myindex_write_filters(0.0, 0.0, 0.0);
        assert_eq!(ip.update_from_orderbook(
            &ob(Exchange::Binance, "BTCUSDT", 83_000.0, 83_010.0, 1_000_000_300)
        ), Some("binance"));
        assert_eq!(ip.exchange_mid("binance"), Some(83_005.0));
    }

    /// P1: an OB whose top bid-ask spread exceeds the threshold is
    /// rejected wholesale before raw_mid is even computed. Protects
    /// against the "stub quote" pattern.
    #[test]
    fn p1_bid_ask_spread_clamp_rejects_above_threshold() {
        let mut ip = IndexPrice::new("BTCUSDT",
            vec![("binance".to_string(), 1.0), ("coinbase".to_string(), 1.0)]);
        ip.set_myindex_write_filters(0.0, 0.01, 0.0); // P1 only

        // Normal 0.013 % spread → accepted.
        assert_eq!(ip.update_from_orderbook(
            &ob(Exchange::Binance, "BTCUSDT", 79_000.0, 79_010.0, 1_000_000_000)
        ), Some("binance"));
        assert_eq!(ip.exchange_mid("binance"), Some(79_005.0));

        // 5 % spread (stub quote on ask side) → rejected.
        // bid=79000, ask=83000 → spread = (83000-79000)/79000 = 5.06 % > 1 %.
        assert_eq!(ip.update_from_orderbook(
            &ob(Exchange::Binance, "BTCUSDT", 79_000.0, 83_000.0, 1_000_000_100)
        ), None);
        assert_eq!(ip.exchange_mid("binance"), Some(79_005.0),
            "P1 must keep prior mid when bid-ask spread exceeds threshold");

        // Threshold 0 disables P1 → same OB accepted.
        ip.set_myindex_write_filters(0.0, 0.0, 0.0);
        assert!(ip.update_from_orderbook(
            &ob(Exchange::Binance, "BTCUSDT", 79_000.0, 83_000.0, 1_000_000_200)
        ).is_some(), "P1=0 must disable spread check");
    }

    /// P2: an incoming mid disagreeing with peer median by more than the
    /// threshold is rejected at write time, so the bad value never enters
    /// the index. Skipped when there's no peer to compare against.
    #[test]
    fn p2_peer_disagree_clamp_rejects_above_threshold() {
        let mut ip = IndexPrice::new("BTCUSDT",
            vec![("binance".to_string(), 1.0), ("coinbase".to_string(), 1.0)]);
        ip.set_myindex_write_filters(0.0, 0.0, 0.01); // P2 only

        // Seed coinbase at 79_000 first — it's the only peer for binance.
        assert_eq!(ip.update_from_orderbook(
            &ob(Exchange::Coinbase, "BTC-USD", 78_995.0, 79_005.0, 1_000_000_000)
        ), Some("coinbase"));
        assert_eq!(ip.exchange_mid("coinbase"), Some(79_000.0));

        // Binance posting near 79_000 → diff 0.06 % vs peer 79_000 → accepted.
        assert_eq!(ip.update_from_orderbook(
            &ob(Exchange::Binance, "BTCUSDT", 78_995.0, 79_105.0, 1_000_000_100)
        ), Some("binance"));

        // Binance posting at 83_000 → diff 5.06 % vs peer median 79_000 → rejected.
        // First seed binance to a reasonable value so we have a prior to assert against.
        let prev_binance = ip.exchange_mid("binance");
        assert_eq!(ip.update_from_orderbook(
            &ob(Exchange::Binance, "BTCUSDT", 82_995.0, 83_005.0, 1_000_000_200)
        ), None);
        assert_eq!(ip.exchange_mid("binance"), prev_binance,
            "P2 must keep prior mid when new mid disagrees with peer median > threshold");

        // Disable P2 → same disagree-write accepted.
        ip.set_myindex_write_filters(0.0, 0.0, 0.0);
        assert_eq!(ip.update_from_orderbook(
            &ob(Exchange::Binance, "BTCUSDT", 82_995.0, 83_005.0, 1_000_000_300)
        ), Some("binance"));
    }

    /// First-tick edge case: P2 must NOT reject the very first feed's
    /// first OB just because no peer is live yet. `peer_median_excluding`
    /// returns `None` in that state, and the gate is skipped.
    #[test]
    fn p2_skipped_when_no_peer_is_live() {
        let mut ip = IndexPrice::new("BTCUSDT",
            vec![("binance".to_string(), 1.0), ("coinbase".to_string(), 1.0)]);
        ip.set_myindex_write_filters(0.0, 0.0, 0.01);

        // Only binance reporting — coinbase still has mid=None.
        // P2 has no peer to compare against → must NOT reject.
        assert_eq!(ip.update_from_orderbook(
            &ob(Exchange::Binance, "BTCUSDT", 79_000.0, 79_010.0, 1_000_000_000)
        ), Some("binance"));
        assert_eq!(ip.exchange_mid("binance"), Some(79_005.0));
    }

    #[test]
    fn historical_replay_caught_by_staleness_when_now_is_real_time() {
        // Simulate a replayed tick: both feeds have ts = 24 h ago, prices
        // are reasonable (replay of yesterday's price, ~66k vs today's ~79k),
        // and divergence between them is small (replay is internally consistent).
        let yesterday_ns: u64 = 1_000_000_000_000_000_000; // arbitrary historical anchor
        let one_day_ns: u64 = 24 * 60 * 60 * 1_000_000_000;
        let mut ip = fresh(66_000.0, yesterday_ns, 66_100.0, yesterday_ns + 50_000_000); // both ~yesterday
        ip.set_myindex_thresholds(0.01, 1_000_000_000); // 1 s staleness

        // Caller mistake (pre-fix): pass the data's own ts as "now"
        //   → component_ts is "fresh" relative to that "now" → gate passes
        //   → bot writes stale spot_price → quotes against replay price.
        let buggy_result = ip.compute_myindex_validated(yesterday_ns + 60_000_000);
        assert!(buggy_result.is_ok(),
            "pre-fix demonstration: passing data's own ts as 'now' lets a 24 h-old replay pass the gate");

        // Caller fix (post-fix): pass wall-clock-now (24 h after the
        // replay's timestamp). Now the gate correctly fires Stale.
        let wall_clock_now = yesterday_ns + one_day_ns; // 24 h later
        match ip.compute_myindex_validated(wall_clock_now) {
            Err(MyindexInvalid::Stale { age_ms, threshold_ms, .. }) => {
                assert!(age_ms >= 24 * 60 * 60 * 1000 - 1000,
                    "age should be ~24 h, got {}ms", age_ms);
                assert_eq!(threshold_ms, 1_000);
            }
            other => panic!(
                "with wall-clock 'now', a 24 h-old replay tick MUST fire Stale — got {:?}",
                other
            ),
        }
    }

    // ─── Myindex bias correction ──────────────────────────────────────

    /// Helper: index with two fresh components + chainlink. Caller
    /// passes a common `now_ns` that's also stored on all three feeds
    /// so the fresh gate passes initially.
    fn fresh_with_chainlink(
        binance_mid: f64,
        coinbase_mid: f64,
        chainlink_price: f64,
        ts_ns: u64,
    ) -> IndexPrice {
        let mut ip = fresh(binance_mid, ts_ns, coinbase_mid, ts_ns);
        ip.update_chainlink(chainlink_price, ts_ns);
        ip
    }
















    // ─── Binance-only bias corrector ──────────────────────────────────








    // ── myindex2 v2 ───────────────────────────────────────────────────

    /// Feature off by default: factors pinned at exactly (1.0, 1.0) and
    /// both compute paths return the legacy value bit-for-bit.
    #[test]
    fn myindex2_disabled_is_bit_exact_legacy() {
        let mut ip = fresh_with_chainlink(80_000.0, 79_950.0, 79_980.0, 1_000_000_000);
        let legacy = ip.compute_myindex();
        let legacy_validated = ip.compute_myindex_validated(1_000_000_000).unwrap();
        for i in 0..100u64 {
            ip.maybe_sample_myindex2(1_000_000_000 + i * 1_000_000_000);
        }
        assert_eq!(ip.myindex2_factors(), (1.0, 1.0));
        assert_eq!(ip.compute_myindex().to_bits(), legacy.to_bits());
        assert_eq!(
            ip.compute_myindex_validated(1_000_000_000).unwrap().to_bits(),
            legacy_validated.to_bits(),
        );
    }

    /// LOCAL-axis basis alignment: cb pairs as-of `bn_local − cb_lead`.
    /// cb leads bn locally, so with cb_lead = 1 s the lookup must select the
    /// cb entry from 1 s ago, not the latest. (Here the server-ts fields act
    /// as the local ts via the fallback when `local_ts` isn't populated.)
    #[test]
    fn myindex2_basis_uses_local_axis_cb_lead() {
        let s = 1_000_000_000_u64;
        let mut ip = fresh(80_000.0, 0, 79_900.0, 0);
        ip.update_chainlink(79_900.0, 0);
        ip.set_myindex2(true, s, 2, 4, 10 * s);
        ip.set_myindex2_lags(s, 0); // cb_lead_bn_local = 1 s
        let cb_at = |t: u64| 79_900.0 + (t / s) as f64;
        let mut now = 10 * s;
        for _ in 0..8 {
            ip.binance_ts_ns = now;        // bn local arrival = now
            ip.coinbase_ts_ns = now;       // cb local arrival = now
            ip.coinbase_mid = Some(cb_at(now));
            ip.chainlink_ts_ns = now;
            ip.maybe_sample_myindex2(now);
            now += s;
        }
        assert!(ip.myindex2.is_warm());
        // Sample at time t: t_cb = t − 1s; the ring is keyed by cb local ts
        // (t, t−1s, …) → as-of(t−1s) = cb_at(t−1s) (the entry from 1 s ago).
        // With cb_lead 0 it would pick the latest cb_at(t). window=2 keeps the
        // last two accepted samples (t=16s, 17s → cb_at(15s), cb_at(16s)).
        let expected = ((80_000.0 / cb_at(now - 2 * s)).ln()
            + (80_000.0 / cb_at(now - 3 * s)).ln()) / 2.0;
        assert!((ip.myindex2.basis - expected).abs() < 1e-12,
            "basis={} expected={}", ip.myindex2.basis, expected);
    }

    /// Adjust tracker: with a constant chainlink/my0 offset, adjust must
    /// converge to that ratio and the validated index must land ON
    /// chainlink (my0 × adjust = cl).
    #[test]
    fn myindex2_adjust_tracks_chainlink_ratio() {
        let s = 1_000_000_000_u64;
        let bn = 80_000.0_f64;
        let cb = 79_940.0_f64;
        let cl = cb * 1.0001; // +1 bp above the post-deduction index
        let mut ip = fresh_with_chainlink(bn, cb, cl, s);
        ip.set_myindex2(true, s, 2, 4, 10 * s);
        ip.set_myindex2_lags(0, 0); // pair latest values (lag mechanics tested separately)
        let mut now = 10 * s;
        for _ in 0..10 {
            ip.binance_ts_ns = now;
            ip.coinbase_ts_ns = now;
            ip.chainlink_ts_ns = now;
            ip.maybe_sample_myindex2(now);
            now += s;
        }
        assert!(ip.myindex2.is_warm());
        // basis deduction lands the bn leg exactly on cb ⇒ my0 = cb.
        let adj = ip.myindex2.effective_adjust();
        assert!((adj - 1.0001).abs() < 1e-9, "adjust={}", adj);
        let idx = ip.compute_myindex_validated(now).unwrap();
        assert!((idx - cl).abs() / cl < 1e-9, "idx={} cl={}", idx, cl);
    }

    /// Lagged adjust reference: with cl_bn_local_lag = 2 s and a my0
    /// that steps at a known time, the ratio must use my0 from 2 s ago.
    #[test]
    fn myindex2_adjust_uses_lagged_my0() {
        let s = 1_000_000_000_u64;
        let mut ip = fresh_with_chainlink(80_000.0, 79_940.0, 79_940.0, s);
        ip.set_myindex2(true, s, 2, 2, 10 * s);
        ip.set_myindex2_lags(0, 2 * s);
        let mut now = 10 * s;
        // Constant prices: my0 history constant ⇒ lag immaterial for the
        // converged value, but the lookups must SUCCEED only once the
        // ring spans ≥ 2 s.
        let mut warm_at = 0u64;
        for i in 0..10 {
            ip.binance_ts_ns = now;
            ip.coinbase_ts_ns = now;
            ip.chainlink_ts_ns = now;
            ip.maybe_sample_myindex2(now);
            if ip.myindex2.adjust_warm && warm_at == 0 { warm_at = i; }
            now += s;
        }
        assert!(ip.myindex2.adjust_warm, "adjust must warm once ring spans the lag");
        assert!((ip.myindex2.effective_adjust() - 1.0).abs() < 1e-9);
    }

    /// Stale chainlink blocks adjust samples (basis still warms).
    #[test]
    fn myindex2_staleness_gates_adjust() {
        let s = 1_000_000_000_u64;
        let mut ip = fresh_with_chainlink(80_000.0, 79_940.0, 79_940.0, s);
        ip.set_myindex2(true, s, 2, 2, 2 * s);
        ip.set_myindex2_lags(0, 0);
        let mut now = 10 * s;
        for _ in 0..6 {
            ip.binance_ts_ns = now;
            ip.coinbase_ts_ns = now;
            // chainlink_ts stays at 1 s — stale vs the 2 s gate
            ip.maybe_sample_myindex2(now);
            now += s;
        }
        assert!(ip.myindex2.is_warm());
        assert!(!ip.myindex2.adjust_warm);
        assert_eq!(ip.myindex2.effective_adjust(), 1.0);
    }

    /// bn staleness offset: with halflife decay on and equal server-ts
    /// ages, the offset must downweight BINANCE by exactly
    /// 0.5^(offset/halflife), pulling the index toward coinbase.
    /// Off (offset=0) ⇒ bit-exact legacy.
    #[test]
    fn myindex2_bn_staleness_offset_downweights_binance() {
        let bn = 80_000.0_f64;
        let cb = 79_940.0_f64;
        let ts = 1_000_000_000_u64;
        let now = ts + 100_000_000;
        let mut ip = fresh(bn, ts, cb, ts);
        ip.set_index_staleness_halflife_ms(1000.0);
        let legacy = ip.compute_myindex_validated(now).unwrap();
        assert!((legacy - (bn + 2.0 * cb) / 3.0).abs() < 1e-9);
        ip.set_myindex2(true, 1_000_000_000, 4, 4, 2_000_000_000);
        ip.set_myindex2_bn_staleness_offset(1_000_000_000);
        let shifted = ip.compute_myindex_validated(now).unwrap();
        let w_bn = 1.0 * 0.5_f64.powf(1.1);
        let w_cb = 2.0 * 0.5_f64.powf(0.1);
        let expected = (bn * w_bn + cb * w_cb) / (w_bn + w_cb);
        assert!((shifted - expected).abs() < 1e-9, "got {} expected {}", shifted, expected);
        assert!(shifted < legacy, "offset must pull the index toward coinbase");
        ip.set_myindex2_bn_staleness_offset(0);
        assert_eq!(ip.compute_myindex_validated(now).unwrap().to_bits(), legacy.to_bits());
    }

    /// Local-axis decay ages: in my2 mode the staleness weight must use
    /// `now − local_arrival_ts` (not the server ts). binance fed with a
    /// 100 ms-older LOCAL arrival than coinbase at equal SERVER ts must
    /// be downweighted ⇒ index moves toward coinbase vs the legacy
    /// (server-ts) weighting.
    #[test]
    fn myindex2_decay_uses_local_axis_ages() {
        use crate::types::Exchange;
        let bn = 80_000.0_f64;
        let cb = 79_940.0_f64;
        let srv = 1_000_000_000_u64;
        let mut ip = IndexPrice::new(
            "BTCUSDT",
            vec![("binance".to_string(), 1.0), ("coinbase".to_string(), 2.0)],
        );
        ip.set_index_staleness_halflife_ms(1000.0);
        // Equal SERVER ts; binance arrived locally 100 ms LATER… meaning
        // its content is fresher in arrival terms? No — local arrival ts
        // measures when WE got it; older arrival = staler content. Feed
        // binance with an arrival 100 ms older than coinbase's.
        ip.update_from_trade(Exchange::Binance, "BTCUSDT", bn, srv, srv + 100_000_000);
        ip.update_from_trade(Exchange::Coinbase, "BTC-USD", cb, srv, srv + 200_000_000);
        let now = srv + 400_000_000;
        // Legacy (my2 off): server-ts ages equal ⇒ plain 1:2 mean.
        let legacy = ip.compute_myindex_validated(now).unwrap();
        assert!((legacy - (bn + 2.0 * cb) / 3.0).abs() < 1e-9);
        // my2 on: local ages 300 ms (bn) vs 200 ms (cb) ⇒ bn downweighted.
        ip.set_myindex2(true, 1_000_000_000, 4, 4, 2_000_000_000);
        let shifted = ip.compute_myindex_validated(now).unwrap();
        let w_bn = 1.0 * 0.5_f64.powf(0.3);
        let w_cb = 2.0 * 0.5_f64.powf(0.2);
        let expected = (bn * w_bn + cb * w_cb) / (w_bn + w_cb);
        assert!((shifted - expected).abs() < 1e-9, "got {} expected {}", shifted, expected);
        assert!(shifted < legacy);
    }

    /// Per-event freeze: adjust reads hold the snapshot while the live
    /// value keeps updating; disabling the freeze returns live reads.
    #[test]
    fn myindex2_freeze_holds_adjust() {
        let s = 1_000_000_000_u64;
        let bn = 80_000.0_f64;
        let cb = 79_940.0_f64;
        let mut ip = fresh_with_chainlink(bn, cb, cb * 1.0001, s);
        ip.set_myindex2(true, s, 2, 2, 10 * s);
        ip.set_myindex2_lags(0, 0);
        let mut now = 10 * s;
        for _ in 0..6 {
            ip.binance_ts_ns = now;
            ip.coinbase_ts_ns = now;
            ip.chainlink_ts_ns = now;
            ip.maybe_sample_myindex2(now);
            now += s;
        }
        let a0 = ip.myindex2.effective_adjust();
        assert!((a0 - 1.0001).abs() < 1e-9);
        ip.freeze_index_for_event();
        ip.update_chainlink(cb * 1.0005, now);
        for _ in 0..6 {
            ip.binance_ts_ns = now;
            ip.coinbase_ts_ns = now;
            ip.chainlink_ts_ns = now;
            ip.maybe_sample_myindex2(now);
            now += s;
        }
        assert!((ip.myindex2.effective_adjust() - a0).abs() < 1e-12,
            "frozen read must hold the snapshot");
        assert!(ip.myindex2.live_adjust() > a0 + 1e-5,
            "live adjust must keep tracking under the freeze");
        ip.set_myindex2_freeze_per_event(false);
        assert!((ip.myindex2.effective_adjust() - ip.myindex2.live_adjust()).abs() < 1e-15);
    }
}
