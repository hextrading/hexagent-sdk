//! Matching core for sim_v2.
//!
//! - P2: real per-token books + cross-outcome synthetic ladders + **taker
//!   matching** (marketable orders sweep the effective ladder, settle wallet +
//!   taker fee).
//! - P3: **resting-queue maker fills** (design doc §5). Each resting order
//!   tracks `q_ahead` (shares ahead in the FIFO queue at its synthetic level),
//!   initialised to the visible merged depth at placement. Trade prints (direct
//!   + cross-outcome mirror) drain `q_ahead`; the overflow fills us at our limit
//!   (maker, fee 0). Book snapshots attribute level shrinkage not explained by
//!   trades to cancels, advancing `q_ahead` proportionally (`ahead_frac` = the
//!   single microstructure parameter, default proportional).

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use crate::types::{
    Exchange, Instrument, Liquidity, OrderBookSnapshot, OrderRequest, OrderStatus, OrderType,
    OrderUpdate, PriceLevel, Side, TickSizeChange, TradeTick,
};

use super::book::{price_to_ticks, BookSet};
use super::wallet::WalletBook;

const EPS: f64 = 1e-9;

/// How many recent events' token state to keep before retiring (memory bound).
/// Each event contributes 2 tokens; a settled event's tokens never reappear in
/// the feed or in orders, so retiring well after settlement is result-neutral.
/// 16 events is a generous grace (≈80 min for 5-min series) while bounding the
/// token-keyed maps to ≈32 live tokens regardless of run length.
const RETAIN_EVENTS: usize = 16;

struct RestingOrder {
    request: OrderRequest,
    /// Canonical matching frame (outcome-folding): for a down order these are
    /// the up-frame mirror (symbol=canonical, side flipped, price 1−p) the book
    /// /trades are matched against. For a canonical/unfolded order they equal
    /// `request.symbol/side/price`. Wallet settle + acks always use `request`.
    match_symbol: String,
    match_side: Side,
    match_price: f64,
    /// USDC locked for a resting BUY (price × remaining). 0 for SELL.
    locked_usdc: f64,
    /// Remaining (unfilled) quantity resting on the book.
    remaining: f64,
    /// Tick size snapshot (avoids a self.tick borrow during re-sync).
    tick: f64,
    /// Shares ahead of us in the FIFO queue at our synthetic level (§5).
    q_ahead: f64,
    /// Visible level depth at the last book snapshot (cancel-attribution ref).
    level_qty_at_sync: f64,
    /// Canonical-frame effective mid at the last snapshot. The signed move
    /// vs the current mid is the adverse-selection signal for the cancel
    /// attribution (see `resync_queues`). 0.0 ⇒ no mid at placement.
    mid_at_sync: f64,
    /// Trade qty matched at our level since the last snapshot.
    traded_since_sync: f64,
    /// Server-time this order rested (for lifetime / fill-age diagnostics).
    placed_ns: u64,
}

#[derive(Clone, Copy)]
struct FeeParams {
    rate: f64,
    exponent: f64,
}

/// A recent fill, kept briefly so a cancel arriving just after the fill returns
/// Filled (matched-can't-cancel) with the original trade_id (PM dedupes → no
/// double count).
struct RecentFill {
    ts: u64,
    trade_id: String,
    cum_filled: f64,
    price: f64,
    side: Side,
    symbol: String,
}

struct MakerFill {
    coid: String,
    token: String,
    side: Side,
    iid: String,
    fill: f64,
    price: f64,
    remaining_after: f64,
    fully: bool,
}

fn flip(s: Side) -> Side {
    match s {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}

pub struct SimExchangeV2 {
    books: BookSet,
    wallets: WalletBook,
    // BTreeMap (NOT HashMap): `match_trade` / `run_book_through` iterate this to
    // emit maker fills, and the EMISSION ORDER is the order the strategy receives
    // its order-updates — which determines which order it cancels/replaces next.
    // HashMap's per-process-randomized iteration made that order non-deterministic
    // → ±0.3% edge/vol run-to-run noise. Sorted coid iteration = reproducible.
    orders: BTreeMap<String, RestingOrder>,
    fees: HashMap<String, FeeParams>,
    tick: HashMap<String, f64>,
    split_by_iid: HashMap<String, f64>,
    seeded_conditions: HashSet<String>,
    /// Outcome-folding (2026-05-30): the two outcome tokens are mirror views of
    /// ONE shared CLOB (verified: up.bid[p] ≈ down.ask[1−p], ~90% exact). When
    /// enabled, the NON-canonical (down) token's book/trade are mapped to the
    /// canonical frame (p↔1−p, bid↔ask / buy↔sell) and folded into a single
    /// canonical book — eliminating the double-count the old complement-merge
    /// produced when both tokens carried the same liquidity. `fold_to[down] =
    /// canon`; canonical token (clob_token_ids[0]) is absent from the map.
    fold_outcomes: bool,
    fold_to: HashMap<String, String>,
    /// Symmetric outcome-sibling map (a↔b) for the race lookahead (peek the
    /// canonical frame's next book from EITHER outcome stream).
    fold_sibling: HashMap<String, String>,
    /// Last applied book exchange_ts per canonical token (book staleness guard:
    /// an incoming snapshot older than this is dropped).
    last_book_ts: HashMap<String, u64>,
    /// Fraction of attributed cancels that sit ahead of us. `None` = the
    /// default proportional model (`q_ahead / level`); `Some(f)` pins it.
    ahead_frac_override: Option<f64>,
    /// **Adverse-selection conditioning** of the cancel attribution (2026-05-31).
    /// Cancellations are informed: when the canonical mid moves AGAINST a resting
    /// order between snapshots, the level's cancels are concentrated AHEAD of us
    /// (front makers pull on the adverse signal) → `ahead_frac → 1` → we advance
    /// to the front and fill the toxic flow. A favorable move → `ahead_frac → 0`
    /// (cancels are noise/behind) → we hold and miss the favorable move. This is
    /// the missing physics behind v2's maker over-fill (edge/vol +1.7% sim vs
    /// −1.2% live). `rate` is the master strength (0 = off → pure proportional);
    /// `scale_ticks` is the adverse mid-move (in ticks) that maps to full
    /// conditioning (`s = ±1`). See `resync_queues`.
    adverse_sel_rate: f64,
    adverse_scale_ticks: f64,
    /// # resyncs where the adverse tilt pushed ahead_frac above its proportional
    /// baseline (advanced the queue → toxic-fill exposure). Diagnostic.
    pub adverse_advanced: u64,
    /// **Book-through adverse fill** rate ∈ [0,1] (2026-05-31, option C). When
    /// the contra side TOUCHES or crosses a resting order's price (for a bid:
    /// `eff_best_ask ≤ p`) AND a trade in the same book interval CONFIRMS a real
    /// match at our price (sell ≤ p for a bid), the order is filled
    /// `rate·(through_vol − q_ahead)` at its limit (adverse: the mid is now at /
    /// through it). The trade-gate (see `pend_cross`) filters the ~56 % of locks
    /// that are flicker (no trade); `rate` is the latency-race fraction. 0 = off.
    book_through_rate: f64,
    /// # book-through adverse fills produced. Diagnostic.
    pub book_through_fills_n: u64,
    /// **Volume-neutral forward-markout adverse selection** (`vn>0`; 2026-05-31).
    /// The sim fills makers symmetrically on trades → fill markout ≈ 0; live makers
    /// are adversely selected (markout ≈ −0.75¢ at 1-5 s: the mid moves against the
    /// fill right after). `vn` keeps the FULL fill quantity and RE-PRICES it adverse
    /// toward the forward mid: settle a favorable fill (markout = signed
    /// fwd_mid(t+h) − limit > 0) at `limit ± vn·markout` (BUY pays more / SELL gets
    /// less); adverse fills settle at the limit. vn=1 ⇒ the fill captures none of
    /// the favorable move (settles at fwd mid); vn>1 ⇒ net adverse. Edge drops at
    /// preserved maker VOLUME. 0 = off.
    fill_markout_vn: f64,
    /// # maker fills haircut/repriced by the forward-markout conditioning. Diagnostic.
    pub fill_haircut_n: u64,
    /// **Trade-gate for the book-through fill** (option C): per canonical symbol,
    /// the (min canonical-SELL price, max canonical-BUY price) of trades since the
    /// last book update. A touch/cross only fills if a trade CONFIRMS a real
    /// match at the order's price (sell ≤ p for a bid / buy ≥ p for an ask),
    /// filtering the ~56 % of locks that are flicker (no trade). Cleared each
    /// `run_book_through`. Defaults `(+∞, −∞)` ⇒ no trade.
    pend_cross: HashMap<String, (f64, f64)>,
    /// Maker race rate ∈ [0,1]: when a resting order's queue GROWS in the next
    /// snapshot, init `q_ahead = rate·next + (1−rate)·now` (favorable moves build
    /// the queue → we fill less → adverse selection). 0 = off.
    maker_race_rate: f64,
    /// Taker race rate ∈ [0,1]: when fillable volume SHRINKS in the next
    /// snapshot, cap the fill at `rate·next + (1−rate)·now` (liquidity recedes →
    /// taker misses). 0 = off.
    taker_race_rate: f64,
    /// **Trade-flow taker competition** (the taker-volume model alongside the race).
    /// Rolling buffer of recent trades per match-symbol in the CANONICAL frame
    /// (server_ts, aggressor_side, price, qty) — exactly the (sym, side, price)
    /// each `match_trade` was invoked with. A marketable order arriving at `t`
    /// competes for the touch with same-direction taker trades in the in-flight
    /// window `(t − taker_comp_window, t]`: that volume was consumed by takers
    /// who beat us to the engine, so we fill only the overflow. Trades capture
    /// sub-snapshot burst competition that the book heals (re-quotes) between
    /// snapshots — invisible to the book-volume race. 0 = off.
    recent_trades: HashMap<String, std::collections::VecDeque<(u64, Side, f64, f64)>>,
    taker_comp_rate: f64,
    taker_comp_window_ns: u64,
    /// # taker fills the competition model capped (competing vol < now within limit).
    pub taker_comp_capped: u64,
    /// Subset capped to ~0 (competition consumed the whole touch → full miss).
    pub taker_comp_capped_zero: u64,
    /// Sum / count of competing taker volume seen at a marketable match (mean diag).
    pub taker_comp_vol_sum: f64,
    pub taker_comp_n: u64,
    /// coid → recent fill (matched-can't-cancel window).
    recent_fills: HashMap<String, RecentFill>,
    /// FIFO of seeded events `(condition_id, [token_a, token_b])` in arrival
    /// order. When it exceeds `RETAIN_EVENTS`, the oldest event's tokens are
    /// retired from every token-keyed map to bound memory over long runs.
    event_fifo: VecDeque<(String, [String; 2])>,
    matched_cant_cancel_window_ns: u64,
    #[allow(dead_code)]
    client_timeout_ns: u64,
    pub taker_fills: u64,
    pub maker_fills: u64,
    pub rejects: u64,
    /// Per-reason reject breakdown (diagnostic): taker-buy/taker-sell/
    /// rest-buy/rest-sell insufficiency. Σ = rejects − post_only_rejects.
    pub rej_taker_buy: u64,
    pub rej_taker_sell: u64,
    pub rej_rest_buy: u64,
    pub rej_rest_sell: u64,
    /// Σ (requested − available) shares over rest-sell rejects — how far the
    /// strategy's ask over-asked the sim's share balance (mismatch magnitude).
    pub rej_rest_sell_short_sum: f64,
    pub post_only_rejects: u64,
    /// post-only orders seen at reach (denominator for the cross rate).
    pub post_only_seen: u64,
    pub matched_cant_cancel: u64,
    /// Cancel-on-arrival ledger. A cancel whose coid is neither resting nor
    /// recently-filled is most likely a cancel that RACED AHEAD of its own
    /// place ack — the placement is still in flight and will rest in a moment.
    /// Recording the coid here lets `submit_order` honour the cancel when the
    /// place finally arrives, instead of resting an order the strategy has
    /// already removed (it acts on the `Cancelled` we return for the race),
    /// which otherwise becomes a forgotten orphan that rests to settlement and
    /// locks the wallet. coid → cancel-arrival ts (for stale pruning).
    pending_cancels: std::collections::HashMap<String, u64>,
    // ── Phase-A diagnostics (maker fill timing) ──
    /// Σ (fill_ts − placed_ts) over maker fills, + count, + #fills on orders
    /// older than 1s. Mean fill-age = sum/n. High age ⇒ orders linger before
    /// filling (cancel/reprice race leaking).
    pub maker_fill_age_sum_ns: u128,
    pub maker_fill_n: u64,
    pub maker_fill_age_over1s: u64,
    /// Σ lifetime (removal_ts − placed_ts) over removed resting orders + count.
    pub maker_life_sum_ns: u128,
    pub maker_life_n: u64,
    // ── race diagnostics ──
    /// # resting placements where the maker race inflated q_ahead (next>now),
    /// out of total placements, + Σ (q_ahead_blended / now_depth) over those.
    pub maker_race_inflated: u64,
    pub maker_race_placements: u64,
    pub maker_race_ratio_sum: f64,
    /// # taker fills the taker race capped (next_avail<now within limit).
    pub taker_race_capped: u64,
    /// Subset of `taker_race_capped` where the cap drove the fillable volume to
    /// ~0 (eff≤EPS) — a FULL taker miss: liquidity entirely pulled in the
    /// in-flight window → Limit rests as maker / FAK cancels (no taker fill).
    pub taker_race_capped_zero: u64,
    /// Distribution samples: maker resting order's initial queue length
    /// (`q_ahead` at placement) and taker order's fillable volume at match
    /// (`now_avail` within limit). For the engine's end-of-run histogram.
    pub maker_q_init: Vec<f32>,
    pub taker_avail: Vec<f32>,
    /// Maker-placement price-vs-BBO classification (why q_init is often 0).
    /// [total, q0] per bucket: improve (better than our-side best = inside
    /// spread / new best level), join (== our-side best), behind (worse than
    /// best, deeper in book), nobook (our-side best absent).
    pub place_improve: [u64; 2],
    pub place_join: [u64; 2],
    pub place_behind: [u64; 2],
    pub place_nobook: [u64; 2],
    /// q_init=0 fallback split: # resolved by beyond-window extrapolation vs the
    /// in-window best-level default rule.
    pub q0_extrapolated: u64,
    pub q0_bestrule: u64,
}

impl SimExchangeV2 {
    pub fn new(
        client_timeout_ns: u64,
        wallet_usdc_by_iid: HashMap<String, f64>,
        split_by_iid: HashMap<String, f64>,
    ) -> Self {
        let mut wallets = WalletBook::new();
        for (iid, bal) in &wallet_usdc_by_iid {
            wallets.seed_usdc(iid, *bal);
        }
        Self {
            books: BookSet::new(),
            wallets,
            orders: BTreeMap::new(),
            fees: HashMap::new(),
            tick: HashMap::new(),
            split_by_iid,
            seeded_conditions: HashSet::new(),
            fold_outcomes: false,
            fold_to: HashMap::new(),
            fold_sibling: HashMap::new(),
            last_book_ts: HashMap::new(),
            ahead_frac_override: None,
            adverse_sel_rate: 0.0,
            adverse_scale_ticks: 1.0,
            adverse_advanced: 0,
            book_through_rate: 0.0,
            book_through_fills_n: 0,
            fill_markout_vn: 0.0,
            fill_haircut_n: 0,
            pend_cross: HashMap::new(),
            maker_race_rate: 0.0,
            taker_race_rate: 0.0,
            recent_trades: HashMap::new(),
            taker_comp_rate: 0.0,
            taker_comp_window_ns: 0,
            taker_comp_capped: 0,
            taker_comp_capped_zero: 0,
            taker_comp_vol_sum: 0.0,
            taker_comp_n: 0,
            recent_fills: HashMap::new(),
            event_fifo: VecDeque::new(),
            matched_cant_cancel_window_ns: 2_000_000_000,
            client_timeout_ns,
            taker_fills: 0,
            maker_fills: 0,
            rejects: 0,
            rej_taker_buy: 0,
            rej_taker_sell: 0,
            rej_rest_buy: 0,
            rej_rest_sell: 0,
            rej_rest_sell_short_sum: 0.0,
            post_only_rejects: 0,
            post_only_seen: 0,
            matched_cant_cancel: 0,
            pending_cancels: std::collections::HashMap::new(),
            maker_fill_age_sum_ns: 0,
            maker_fill_n: 0,
            maker_fill_age_over1s: 0,
            maker_life_sum_ns: 0,
            maker_life_n: 0,
            maker_q_init: Vec::new(),
            taker_avail: Vec::new(),
            place_improve: [0; 2],
            place_join: [0; 2],
            place_behind: [0; 2],
            place_nobook: [0; 2],
            q0_extrapolated: 0,
            q0_bestrule: 0,
            maker_race_inflated: 0,
            maker_race_placements: 0,
            maker_race_ratio_sum: 0.0,
            taker_race_capped: 0,
            taker_race_capped_zero: 0,
        }
    }

    fn record_lifetime(&mut self, placed_ns: u64, now_ns: u64) {
        self.maker_life_sum_ns += now_ns.saturating_sub(placed_ns) as u128;
        self.maker_life_n += 1;
    }

    /// Record a fill so a cancel arriving within the window returns Filled.
    fn record_recent_fill(
        &mut self,
        coid: &str,
        trade_id: String,
        add_qty: f64,
        price: f64,
        side: Side,
        symbol: &str,
        ts: u64,
    ) {
        let e = self.recent_fills.entry(coid.to_string()).or_insert(RecentFill {
            ts,
            trade_id: trade_id.clone(),
            cum_filled: 0.0,
            price,
            side,
            symbol: symbol.to_string(),
        });
        e.ts = ts;
        e.trade_id = trade_id;
        e.cum_filled += add_qty;
        e.price = price;
        // Memory bound: drop fills older than the matched-can't-cancel window.
        // `cancel_order` only matches a fill within that window (it checks
        // `now - rf.ts <= window`), and the sim processes events in ts order, so
        // an entry pruned here at the latest `ts` could never be validly matched
        // by a later cancel → result-neutral. Keeps `recent_fills` to ≈one
        // window of fills instead of growing once per fill forever.
        let window = self.matched_cant_cancel_window_ns;
        self.recent_fills.retain(|_, f| ts.saturating_sub(f.ts) <= window);
    }

    /// Symbol/side of a resting order (for building timeout updates).
    pub fn order_symbol_side(&self, coid: &str) -> Option<(String, Side)> {
        self.orders.get(coid).map(|o| (o.request.symbol.clone(), o.request.side))
    }

    fn tick_of(&self, token: &str) -> f64 {
        self.tick.get(token).copied().unwrap_or(0.01)
    }

    /// Apply v2 model knobs from config (ahead_frac override, matched-can't-
    /// cancel window). `ahead_frac=None` keeps the default proportional model.
    pub fn configure(&mut self, ahead_frac: Option<f64>, matched_cant_cancel_window_ns: u64) {
        self.ahead_frac_override = ahead_frac.map(|f| f.clamp(0.0, 1.0));
        if matched_cant_cancel_window_ns > 0 {
            self.matched_cant_cancel_window_ns = matched_cant_cancel_window_ns;
        }
    }

    /// Configure adverse-selection conditioning of the cancel attribution.
    /// `rate=0` disables it (pure proportional/override ahead_frac). `scale_ticks`
    /// is the adverse mid-move (ticks) mapping to full conditioning; clamped to a
    /// small positive floor so the gain stays finite.
    pub fn configure_adverse_sel(&mut self, rate: f64, scale_ticks: f64) {
        self.adverse_sel_rate = rate.max(0.0);
        self.adverse_scale_ticks = scale_ticks.max(1e-6);
    }

    /// Configure the book-through adverse fill rate (latency-race fraction in
    /// [0,1]; 0 = off). See the `book_through_rate` field.
    pub fn configure_book_through(&mut self, rate: f64) {
        self.book_through_rate = rate.clamp(0.0, 1.0);
    }

    /// Configure the VOLUME-NEUTRAL forward-markout adverse-reprice strength `vn`
    /// (favorable fills settle at limit ± vn·markout, full quantity kept; 0 = off).
    /// See `fill_markout_vn`.
    pub fn configure_fill_markout_vn(&mut self, vn: f64) {
        self.fill_markout_vn = vn.max(0.0);
    }

    /// Enable outcome-folding (single canonical up-frame book; down mapped in).
    pub fn set_fold_outcomes(&mut self, on: bool) {
        self.fold_outcomes = on;
        // Mirror the flag into the book set so the cross-outcome merge chokepoint
        // (`comp_book`) can debug_assert it stays inert under folding.
        self.books.set_folded(on);
    }

    /// Deep-queue model for resting prices beyond the recorded window (0 = legacy
    /// linear extrapolation; >0 = outermost-level flat/decay). See `book.rs`.
    pub fn set_deep_queue_decay(&mut self, d: f64) {
        self.books.set_deep_queue_decay(d);
    }

    /// Maker/taker one-step "race" rates (0 = off). See the struct fields.
    pub fn configure_race(&mut self, maker_race: f64, taker_race: f64) {
        self.maker_race_rate = maker_race.clamp(0.0, 1.0);
        self.taker_race_rate = taker_race.clamp(0.0, 1.0);
    }

    /// Trade-flow taker competition (0 = off). `rate` ∈ [0,1] scales how much of
    /// the competing in-flight taker volume is consumed ahead of us; `window_ns`
    /// is the backward in-flight exposure (≈ taker overhead). See `recent_trades`.
    pub fn configure_taker_comp(&mut self, rate: f64, window_ns: u64) {
        self.taker_comp_rate = rate.clamp(0.0, 1.0);
        self.taker_comp_window_ns = window_ns;
    }
    pub fn race_enabled(&self) -> bool {
        self.maker_race_rate > 0.0 || self.taker_race_rate > 0.0
    }

    /// Complement token of `token`, if paired (for the simulator to prime the
    /// cross-outcome leg of the next-book lookahead). Empty under folding.
    pub fn complement_of(&self, token: &str) -> Option<String> {
        self.books.complement(token).cloned()
    }
    /// Whether outcome-folding is on (the simulator primes the canonical frame).
    pub fn fold_on(&self) -> bool {
        self.fold_outcomes
    }
    /// Canonical token for the race lookahead (itself if unfolded/canonical).
    pub fn canonical_token(&self, token: &str) -> String {
        self.canonical_of(token).to_string()
    }
    /// Outcome sibling (the other token) under folding, else None.
    pub fn fold_sibling_of(&self, token: &str) -> Option<String> {
        self.fold_sibling.get(token).cloned()
    }
    /// Stash the next book snapshot for `token` (one-step race lookahead).
    pub fn set_next_book(&mut self, token: &str, bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) {
        self.books.set_next(token, bids, asks);
    }
    /// Stash the next book for the canonical frame from a sibling (down) stream
    /// snapshot, mirroring it (p→1−p, bid↔ask).
    pub fn set_next_book_mirrored(&mut self, canon: &str, bids: &[PriceLevel], asks: &[PriceLevel]) {
        let (b, a) = Self::mirror_levels(bids, asks);
        self.books.set_next(canon, b, a);
    }
    /// Append a canonical-frame window snapshot (taker windowed race).
    pub fn push_next_window(&mut self, token: &str, bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) {
        self.books.push_next_window(token, bids, asks);
    }
    /// Append a window snapshot from a sibling (down) stream, mirrored to the
    /// canonical frame (taker windowed race).
    pub fn push_next_window_mirrored(&mut self, canon: &str, bids: &[PriceLevel], asks: &[PriceLevel]) {
        let (b, a) = Self::mirror_levels(bids, asks);
        self.books.push_next_window(canon, b, a);
    }
    /// Drop all stashed lookahead books (called before each priming).
    pub fn clear_next_books(&mut self) {
        self.books.clear_next();
    }

    // ── market data ──────────────────────────────────────────────
    /// Apply a book snapshot. Returns any **book-through adverse fills** (a
    /// resting order the contra just swept through) for the caller to deliver,
    /// empty unless `book_through_rate > 0`.
    pub fn on_orderbook(&mut self, ob: &OrderBookSnapshot) -> Vec<OrderUpdate> {
        let now_ns = ob.exchange_timestamp_ns;
        if self.fold_outcomes {
            // Fold onto the canonical frame: the non-canonical token's snapshot
            // is mirrored (p→1−p, bid↔ask); the canonical token is applied as-is.
            // Both write the SINGLE canonical book — the two outcomes are one
            // shared CLOB, so a single up-frame book carries all liquidity (no
            // double-count). Staleness guard: drop a snapshot whose server ts is
            // older than the last one applied to this canonical frame (the two
            // outcome streams interleave; an older snapshot would regress the
            // shared book).
            let canon = self.canonical_of(&ob.symbol).to_string();
            if let Some(&last) = self.last_book_ts.get(&canon) {
                if ob.exchange_timestamp_ns < last {
                    return Vec::new();
                }
            }
            self.last_book_ts.insert(canon.clone(), ob.exchange_timestamp_ns);
            if ob.symbol == canon {
                self.books.update(&canon, ob.bids.clone(), ob.asks.clone());
            } else {
                let (b, a) = Self::mirror_levels(&ob.bids, &ob.asks);
                self.books.update(&canon, b, a);
            }
            self.resync_queues();
            return self.run_book_through(now_ns);
        }
        self.books.update(&ob.symbol, ob.bids.clone(), ob.asks.clone());
        self.resync_queues();
        self.run_book_through(now_ns)
    }

    /// Book-through adverse fills: a resting order whose price the contra side
    /// just swept STRICTLY through is marketable — the stale maker, lingering
    /// while faster makers cancelled (the price moved via repricing, not a trade
    /// — verified 99.9% of crosses), gets picked off. Fill `rate·(through_vol −
    /// q_ahead)` at the order's limit (adverse: mid is now through it). No-op
    /// when `book_through_rate == 0`.
    fn run_book_through(&mut self, now_ns: u64) -> Vec<OrderUpdate> {
        let rate = self.book_through_rate;
        if rate <= 0.0 {
            return Vec::new();
        }
        let mut mfills: Vec<MakerFill> = Vec::new();
        {
            let books = &self.books;
            let pend = &self.pend_cross;
            let mut n = 0u64;
            for (coid, o) in self.orders.iter_mut() {
                if o.remaining <= EPS {
                    continue;
                }
                let p = o.match_price;
                let is_buy = o.match_side == Side::Buy;
                // Contra TOUCHED or crossed our price (option C: touch-inclusive)…
                let touched = match o.match_side {
                    Side::Buy => books.eff_best_ask(&o.match_symbol).is_some_and(|a| a <= p + EPS),
                    Side::Sell => books.eff_best_bid(&o.match_symbol).is_some_and(|b| b >= p - EPS),
                };
                if !touched {
                    continue;
                }
                // …AND a trade since the last book update CONFIRMS a real match at
                // our price (sell ≤ p for a bid / buy ≥ p for an ask) — filters the
                // ~56 % of locks that are flicker (no trade). Verified physical.
                let trade_confirmed = pend.get(&o.match_symbol).is_some_and(|&(min_sell, max_buy)| match o.match_side {
                    Side::Buy => min_sell <= p + EPS,
                    Side::Sell => max_buy >= p - EPS,
                });
                if !trade_confirmed {
                    continue;
                }
                // Contra volume marketable at our limit (asks≤p for a buy).
                let through = books.available_volume(&o.match_symbol, is_buy, Some(p));
                let fillable = (through - o.q_ahead).max(0.0) * rate;
                let fill = fillable.min(o.remaining);
                if fill <= EPS {
                    continue;
                }
                // The sweep consumes the queue ahead of us then takes our fill.
                o.q_ahead = (o.q_ahead - through).max(0.0);
                o.remaining -= fill;
                let limit = o.request.price.unwrap_or(0.0);
                if o.request.side == Side::Buy {
                    o.locked_usdc = limit * o.remaining;
                }
                n += 1;
                mfills.push(MakerFill {
                    coid: coid.clone(),
                    token: o.request.symbol.clone(),
                    side: o.request.side,
                    iid: o.request.instance_id.clone(),
                    fill,
                    price: limit,
                    remaining_after: o.remaining,
                    fully: o.remaining <= EPS,
                });
            }
            self.book_through_fills_n += n;
        }
        // Reset the trade-gate window for the next book interval.
        self.pend_cross.clear();
        if mfills.is_empty() {
            return Vec::new();
        }
        self.apply_maker_fills(mfills, now_ns)
    }

    /// Cancel attribution (§5): level shrinkage not explained by trades since the
    /// last snapshot is attributed to cancels; a fraction `ahead_frac` sits ahead
    /// of us and advances our queue. (The maker race is NOT applied here — it
    /// fires once, at the order's entry-match moment; see `insert_resting`.)
    fn resync_queues(&mut self) {
        let books = &self.books;
        let af_override = self.ahead_frac_override;
        let adv_rate = self.adverse_sel_rate;
        let adv_scale = self.adverse_scale_ticks;
        let mut advanced = 0u64;
        for o in self.orders.values_mut() {
            // Queue depth tracked in the canonical matching frame.
            let l_now = books.level_depth(&o.match_symbol, o.match_side, o.match_price, o.tick);
            let l_prev = o.level_qty_at_sync;
            let cancels = (l_prev - o.traded_since_sync - l_now).max(0.0);
            // Baseline (neutral) ahead-fraction: pinned override or proportional.
            let base = match af_override {
                Some(f) => f.clamp(0.0, 1.0),
                None if l_prev > EPS => (o.q_ahead / l_prev).clamp(0.0, 1.0),
                None => 0.0,
            };
            // Adverse-selection tilt: cancellations are informed. The canonical
            // mid move since the last sync, signed AGAINST the order, says whether
            // the level's cancels were informed (adverse → front makers pull →
            // ahead_frac→1 → advance, fill toxic) or noise (favorable → →0 →
            // hold, miss). `s∈[-1,1]`; at s=0 (no move / rate=0) ahead_frac=base,
            // exactly the prior model. The fill's adverse cost is realised later
            // at settlement (down move ⇒ a filled `up` bid loses).
            let mid_now = books.eff_mid(&o.match_symbol);
            let ahead_frac = if adv_rate > 0.0 && mid_now > 0.0 && o.mid_at_sync > 0.0 {
                let raw = mid_now - o.mid_at_sync; // + = canonical(up) mid rose
                let adverse = match o.match_side { Side::Buy => -raw, Side::Sell => raw };
                let s = (adv_rate * adverse / (adv_scale * o.tick)).clamp(-1.0, 1.0);
                if s > 0.0 {
                    advanced += 1;
                    base + (1.0 - base) * s // adverse: base → 1
                } else {
                    base * (1.0 + s) // favorable (s≤0): base → 0
                }
            } else {
                base
            };
            o.q_ahead = (o.q_ahead - cancels * ahead_frac).max(0.0);
            o.level_qty_at_sync = l_now;
            if mid_now > 0.0 {
                o.mid_at_sync = mid_now;
            }
            o.traded_since_sync = 0.0;
        }
        self.adverse_advanced += advanced;
    }

    /// Maker fills: a trade print drains the resting queue at the matched level
    /// (direct) and at the mirrored complement level (cross-outcome).
    pub fn on_trade_tick(&mut self, t: &TradeTick) -> Vec<OrderUpdate> {
        self.on_trade_tick_inner(t, None)
    }

    /// Like `on_trade_tick` but with the canonical forward mid at `t+h` (peeked
    /// by the simulator) for the forward-markout adverse reprice. `None` ⇒ no
    /// reprice (also a no-op when `fill_markout_vn == 0`).
    pub fn on_trade_tick_fwd(&mut self, t: &TradeTick, fwd_mid: Option<f64>) -> Vec<OrderUpdate> {
        self.on_trade_tick_inner(t, fwd_mid)
    }

    fn on_trade_tick_inner(&mut self, t: &TradeTick, fwd_mid: Option<f64>) -> Vec<OrderUpdate> {
        let mut fills: Vec<MakerFill> = Vec::new();
        let ts = t.exchange_timestamp_ns;
        if self.fold_outcomes {
            // Fold the trade onto the canonical frame and drain the single
            // canonical queue once (a down trade mirrors: flip side, 1−price).
            let canon = self.canonical_of(&t.symbol).to_string();
            if canon == t.symbol {
                self.record_trade(&canon, t.side, t.price, t.quantity, ts);
                self.match_trade(&canon, t.side, t.price, t.quantity, fwd_mid, &mut fills);
            } else {
                self.record_trade(&canon, flip(t.side), 1.0 - t.price, t.quantity, ts);
                self.match_trade(&canon, flip(t.side), 1.0 - t.price, t.quantity, fwd_mid, &mut fills);
            }
        } else {
            // Direct: aggressor side / price as recorded.
            self.record_trade(&t.symbol, t.side, t.price, t.quantity, ts);
            self.match_trade(&t.symbol, t.side, t.price, t.quantity, fwd_mid, &mut fills);
            // Cross-outcome mirror: flip side, 1 − price on the complement token.
            if let Some(comp) = self.books.complement(&t.symbol).cloned() {
                self.record_trade(&comp, flip(t.side), 1.0 - t.price, t.quantity, ts);
                self.match_trade(&comp, flip(t.side), 1.0 - t.price, t.quantity, fwd_mid.map(|m| 1.0 - m), &mut fills);
            }
        }
        self.apply_maker_fills(fills, ts)
    }

    /// Append a trade to the rolling competition buffer (canonical frame) and
    /// trim to `taker_comp_window_ns`. No-op when taker competition is off.
    fn record_trade(&mut self, sym: &str, side: Side, price: f64, qty: f64, ts: u64) {
        if self.taker_comp_rate <= 0.0 || self.taker_comp_window_ns == 0 {
            return;
        }
        let cutoff = ts.saturating_sub(self.taker_comp_window_ns);
        let buf = self.recent_trades.entry(sym.to_string()).or_default();
        buf.push_back((ts, side, price, qty));
        while let Some(&(front_ts, _, _, _)) = buf.front() {
            if front_ts < cutoff {
                buf.pop_front();
            } else {
                break;
            }
        }
    }

    /// Competing same-direction taker volume in the in-flight window
    /// `(now_ns − taker_comp_window, now_ns]` at prices that cross our limit —
    /// takers who beat us to the touch. For a BUY (lifting asks) competitors are
    /// BUY-aggressor trades at price ≤ limit; for a SELL, SELL-aggressor at ≥ limit.
    fn taker_competition_volume(&self, msym: &str, mside: Side, lim: Option<f64>, now_ns: u64) -> f64 {
        let Some(buf) = self.recent_trades.get(msym) else { return 0.0 };
        let from = now_ns.saturating_sub(self.taker_comp_window_ns);
        let mut comp = 0.0;
        for &(ts, side, price, qty) in buf.iter() {
            if ts <= from || ts > now_ns || side != mside {
                continue;
            }
            let within = match (lim, mside) {
                (None, _) => true,
                (Some(p), Side::Buy) => price <= p + EPS,
                (Some(p), Side::Sell) => price >= p - EPS,
            };
            if within {
                comp += qty;
            }
        }
        comp
    }

    /// Drain `q_ahead` for resting orders at the matched level; the overflow
    /// fills us. `aggressor_side`/`price` already mirrored by the caller.
    fn match_trade(
        &mut self,
        symbol: &str,
        aggressor_side: Side,
        price: f64,
        qty: f64,
        fwd_mid: Option<f64>,
        out: &mut Vec<MakerFill>,
    ) {
        // Book-through trade-gate (option C): remember this trade's crossing
        // extent for the next book update — a SELL at `price` can confirm a
        // bid-fill at ≥ price, a BUY confirms an ask-fill at ≤ price.
        if self.book_through_rate > 0.0 {
            let e = self.pend_cross.entry(symbol.to_string()).or_insert((f64::INFINITY, f64::NEG_INFINITY));
            match aggressor_side {
                Side::Sell => e.0 = e.0.min(price),
                Side::Buy => e.1 = e.1.max(price),
            }
        }
        let tick = self.tick_of(symbol);
        let trade_ticks = price_to_ticks(price, tick);
        let vn = self.fill_markout_vn;
        let mut haircuts = 0u64;
        for (coid, o) in self.orders.iter_mut() {
            // Match in the canonical frame: `symbol`/`aggressor_side`/`price`
            // are canonical (the caller already folded the trade). Fills settle
            // in the ORIGINAL frame via `o.request.*`.
            if o.match_symbol != symbol {
                continue;
            }
            let order_ticks = price_to_ticks(o.match_price, tick);
            let matches = match o.match_side {
                Side::Buy => aggressor_side == Side::Sell && trade_ticks <= order_ticks,
                Side::Sell => aggressor_side == Side::Buy && trade_ticks >= order_ticks,
            };
            if !matches {
                continue;
            }
            let over = qty - o.q_ahead;
            o.q_ahead = (o.q_ahead - qty).max(0.0);
            o.traded_since_sync += qty;
            if over <= EPS {
                continue;
            }
            let fill = over.min(o.remaining);
            if fill <= EPS {
                continue;
            }
            // Forward-markout adverse selection (VOLUME-NEUTRAL): FAVORABLE fills
            // (canonical fwd mid moved in our favor after the fill) are over-
            // represented — the sim fills symmetrically, but live makers escape
            // favorable touches. Keep the FULL fill and RE-PRICE it adverse toward
            // the forward mid (limit ± vn·markout) → edge drops at preserved maker
            // volume. Settle in the ORIGINAL frame (down price q, not up-frame 1−q).
            let limit = o.request.price.unwrap_or(0.0);
            let mut eff_price = limit;
            if let (Some(fm), true) = (fwd_mid, vn > 0.0) {
                let markout = match o.match_side { Side::Buy => fm - o.match_price, Side::Sell => o.match_price - fm };
                if markout > 0.0 {
                    // Penalty magnitude is frame-invariant (p↔1−p preserves |Δ|);
                    // apply by ORIGINAL side: BUY pays more, SELL receives less.
                    let pen = vn * markout;
                    eff_price = match o.request.side {
                        Side::Buy => (limit + pen).min(1.0),
                        Side::Sell => (limit - pen).max(0.0),
                    };
                    haircuts += 1;
                }
            }
            o.remaining -= fill;
            // Resting remainder stays at the real limit (only the FILLED share is
            // repriced under vn); locked USDC tracks the limit.
            if o.request.side == Side::Buy {
                o.locked_usdc = limit * o.remaining;
            }
            out.push(MakerFill {
                coid: coid.clone(),
                token: o.request.symbol.clone(),
                side: o.request.side,
                iid: o.request.instance_id.clone(),
                fill,
                price: eff_price,
                remaining_after: o.remaining,
                fully: o.remaining <= EPS,
            });
        }
        self.fill_haircut_n += haircuts;
    }

    fn apply_maker_fills(&mut self, fills: Vec<MakerFill>, now_ns: u64) -> Vec<OrderUpdate> {
        let mut out = Vec::with_capacity(fills.len());
        for f in fills {
            // Maker fills settle at our limit; Polymarket maker fee = 0.
            match f.side {
                Side::Buy => self.wallets.settle_buy(&f.iid, &f.token, f.fill, f.price * f.fill),
                Side::Sell => self.wallets.settle_sell(&f.iid, &f.token, f.fill, f.price * f.fill),
            }
            self.maker_fills += 1;
            // Diagnostic: fill-age = how long after placement this maker order
            // filled. High age ⇒ orders linger before filling (race leaking).
            if let Some(placed) = self.orders.get(&f.coid).map(|o| o.placed_ns) {
                let age = now_ns.saturating_sub(placed);
                self.maker_fill_age_sum_ns += age as u128;
                self.maker_fill_n += 1;
                if age > 1_000_000_000 {
                    self.maker_fill_age_over1s += 1;
                }
                if f.fully {
                    self.record_lifetime(placed, now_ns);
                }
            }
            let trade_id = format!("simv2-maker-{}-{}", f.coid, self.maker_fills);
            self.record_recent_fill(&f.coid, trade_id.clone(), f.fill, f.price, f.side, &f.token, now_ns);
            let status = if f.fully { OrderStatus::Filled } else { OrderStatus::PartiallyFilled };
            out.push(OrderUpdate {
                client_order_id: f.coid.clone(),
                exchange: Exchange::Polymarket,
                symbol: f.token,
                side: f.side,
                exchange_order_id: Some(format!("simv2-{}", f.coid)),
                status,
                liquidity: Some(Liquidity::Maker),
                filled_quantity: f.fill,
                remaining_quantity: f.remaining_after,
                avg_fill_price: f.price,
                timestamp_ns: now_ns,
                trade_id: Some(trade_id),
                error: None,
            });
            if f.fully {
                self.orders.remove(&f.coid);
            }
        }
        out
    }

    /// Canonical token for `token` (itself if canonical / unpaired; the
    /// `fold_to` target otherwise). Outcome-folding maps the non-canonical
    /// (down) token onto the canonical (up) frame.
    fn canonical_of<'a>(&'a self, token: &'a str) -> &'a str {
        self.fold_to.get(token).map(|s| s.as_str()).unwrap_or(token)
    }

    /// Canonical matching frame for an order: `(symbol, side, price)`. For a
    /// folded (non-canonical/down) order this mirrors symbol→canonical,
    /// side→flipped, price→1−p; otherwise it's the order unchanged.
    fn match_view(&self, o: &OrderRequest) -> (String, Side, Option<f64>) {
        if self.fold_outcomes {
            let canon = self.canonical_of(&o.symbol);
            if canon != o.symbol {
                return (canon.to_string(), flip(o.side), o.price.map(|p| 1.0 - p));
            }
        }
        (o.symbol.clone(), o.side, o.price)
    }

    /// Mirror a single outcome's L2 levels into the complement frame:
    /// `price → 1 − price`, `bids ↔ asks`. `BUY tok @ p ≡ SELL comp @ (1−p)`,
    /// so the complement's bids become this frame's asks and vice-versa.
    fn mirror_levels(bids: &[PriceLevel], asks: &[PriceLevel]) -> (Vec<PriceLevel>, Vec<PriceLevel>) {
        let map = |ls: &[PriceLevel]| -> Vec<PriceLevel> {
            ls.iter()
                .filter(|l| l.quantity > 0.0 && l.price > 0.0 && l.price < 1.0)
                .map(|l| PriceLevel { price: 1.0 - l.price, quantity: l.quantity })
                .collect()
        };
        // canonical bids ← complement asks(1−p); canonical asks ← complement bids(1−p)
        (map(asks), map(bids))
    }

    pub fn on_instrument(&mut self, inst: &Instrument) {
        if let Instrument::BinaryOption(bo) = inst {
            if bo.clob_token_ids.len() == 2 {
                let a = &bo.clob_token_ids[0];
                let b = &bo.clob_token_ids[1];
                if self.fold_outcomes {
                    // Single canonical frame: canonical = clob_token_ids[0],
                    // fold [1] → [0]. Do NOT pair the books — folding maps the
                    // down snapshot into the canonical book directly, so the
                    // complement-merge in `buy_ladder`/`level_depth` must stay
                    // inert (else the shared liquidity is counted twice).
                    self.fold_to.insert(b.clone(), a.clone());
                    self.fold_sibling.insert(a.clone(), b.clone());
                    self.fold_sibling.insert(b.clone(), a.clone());
                } else {
                    self.books.set_pair(a, b);
                }
                let fp = FeeParams { rate: bo.fee_rate, exponent: bo.fee_exponent };
                self.fees.insert(a.clone(), fp);
                self.fees.insert(b.clone(), fp);
                if bo.tick_size > 0.0 {
                    self.tick.insert(a.clone(), bo.tick_size);
                    self.tick.insert(b.clone(), bo.tick_size);
                }
                if !self.seeded_conditions.contains(&bo.condition_id) {
                    self.seeded_conditions.insert(bo.condition_id.clone());
                    let credits: Vec<(String, f64)> = self
                        .split_by_iid
                        .iter()
                        .filter(|(_, s)| **s > 0.0)
                        .map(|(iid, s)| (iid.clone(), *s))
                        .collect();
                    for (iid, split) in credits {
                        self.wallets.credit_shares(&iid, a, split);
                        self.wallets.credit_shares(&iid, b, split);
                        // Mirror the strategy's virtual split: minting `split`
                        // of each token costs `split` USDC (1 USDC → 1 Up + 1
                        // Down). The settlement credit at retire pays it back
                        // ($1 from the winning side of the pair → nets $0).
                        self.wallets.adjust_usdc(&iid, -split);
                    }
                    // Memory + speed bound: record this event and retire events
                    // beyond the retain window (long settled → never referenced
                    // again). `retire_event` first drops any residual resting
                    // orders for the event, so they stop accumulating in
                    // `self.orders` — otherwise the per-book-event `resync_queues`
                    // / `run_book_through` loops grow O(n_orders) and the backtest
                    // slows quadratically over a long run.
                    self.event_fifo.push_back((bo.condition_id.clone(), [a.clone(), b.clone()]));
                    while self.event_fifo.len() > RETAIN_EVENTS {
                        let (cond, toks) = self.event_fifo.pop_front().unwrap();
                        self.retire_event(&cond, &toks);
                    }
                }
            }
        }
    }

    /// Drop all state for a long-settled event (see `RETAIN_EVENTS`): first any
    /// residual resting orders (the strategy abandoned them ~RETAIN_EVENTS ago
    /// and the tokens are being retired, so they can never fill or be cancelled
    /// to a different outcome), then the per-token book/fee/tick/wallet maps.
    ///
    /// Removing the residual orders is what stops `self.orders` growing without
    /// bound — the root cause of the long-run quadratic slowdown (`resync_queues`
    /// / `run_book_through` iterate every order on every book event). Dropping an
    /// order also frees its `locked_usdc`/share reservation, identical to the
    /// cancel path; verified result-neutral by the 5-day per-event PnL key.
    fn retire_event(&mut self, condition: &str, tokens: &[String; 2]) {
        // Residual orders for this dead event — remove before retiring the tokens.
        self.orders
            .retain(|_, o| !(tokens.contains(&o.request.symbol) || tokens.contains(&o.match_symbol)));
        // Settlement payout to the gating wallet (mirror the strategy's pm so the
        // wallet doesn't bleed). The event is long settled by retire time, so the
        // canonical mid has converged to ~0/1: tokens[0] (canonical) wins iff its
        // mid ≥ 0.5; settle prices are complementary (exactly one side pays $1).
        // Because s0+s1=1, matched Up/Down pairs net to $1/pair regardless of the
        // winner read — only the directional residual depends on it. PnL is
        // unaffected (the wallet never feeds PnL; it only gates orders).
        let p0 = self.books.eff_mid(&tokens[0]);
        let (s0, s1) = if p0 >= 0.5 { (1.0, 0.0) } else { (0.0, 1.0) };
        let iids: Vec<String> = self.split_by_iid.keys().cloned().collect();
        for iid in &iids {
            let payout = self.wallets.shares(iid, &tokens[0]) * s0
                + self.wallets.shares(iid, &tokens[1]) * s1;
            if payout != 0.0 {
                self.wallets.adjust_usdc(iid, payout);
            }
        }
        for t in tokens {
            self.fees.remove(t);
            self.tick.remove(t);
            self.fold_to.remove(t);
            self.fold_sibling.remove(t);
            self.last_book_ts.remove(t);
            self.recent_trades.remove(t);
            self.books.retire_token(t);
            self.wallets.retire_token(t);
        }
        self.seeded_conditions.remove(condition);
    }

    pub fn on_tick_size_change(&mut self, t: &TickSizeChange) {
        if t.new_tick_size <= 0.0 {
            return;
        }
        self.tick.insert(t.symbol.clone(), t.new_tick_size);
        // Folding: matching runs in the canonical frame, so the canonical token's
        // tick must track the change even if only the sibling stream emitted it.
        let canon = self.canonical_of(&t.symbol).to_string();
        if canon != t.symbol {
            self.tick.insert(canon.clone(), t.new_tick_size);
        }
        // Re-baseline resting orders matched in the affected (canonical) frame.
        // Their `tick` snapshot drives the level_depth bucketing in resync_queues;
        // leaving it stale across a 0.01→0.001 regrid would merge the new fine
        // levels into one coarse bucket (l_prev), and the next resync would read a
        // huge spurious "cancel" (or "grow") from the bucketing discontinuity —
        // corrupting q_ahead. Update the tick, re-anchor level_qty_at_sync at the
        // new grid, clamp q_ahead to the now-narrower level, and reset the trade
        // accumulator so the next resync compares like-for-like.
        let books = &self.books;
        for o in self.orders.values_mut() {
            if o.match_symbol != canon {
                continue;
            }
            o.tick = t.new_tick_size;
            let d = books.level_depth(&o.match_symbol, o.match_side, o.match_price, t.new_tick_size);
            o.q_ahead = o.q_ahead.min(d);
            o.level_qty_at_sync = d;
            o.traded_since_sync = 0.0;
        }
    }

    // ── balance helpers (gate only when USDC seeded) ─────────────
    fn locked_usdc_for(&self, iid: &str) -> f64 {
        self.orders.values().filter(|o| o.request.instance_id == iid).map(|o| o.locked_usdc).sum()
    }

    /// Raw gating-wallet USDC (no locked-order subtraction). Diagnostic.
    pub fn wallet_usdc_raw(&self, iid: &str) -> Option<f64> {
        self.wallets.usdc(iid)
    }
    fn locked_sell_shares_for(&self, iid: &str, token: &str) -> f64 {
        self.orders
            .values()
            .filter(|o| o.request.instance_id == iid && o.request.symbol == token && o.request.side == Side::Sell)
            .map(|o| o.remaining)
            .sum()
    }
    fn available_usdc(&self, iid: &str) -> Option<f64> {
        self.wallets.usdc(iid).map(|b| b - self.locked_usdc_for(iid))
    }
    fn available_shares(&self, iid: &str, token: &str) -> f64 {
        (self.wallets.shares(iid, token) - self.locked_sell_shares_for(iid, token)).max(0.0)
    }
    fn fee(&self, token: &str, size: f64, price: f64) -> f64 {
        match self.fees.get(token) {
            Some(fp) if fp.rate > 0.0 && size > 0.0 => {
                let p = price.clamp(0.0, 1.0);
                let pp = (p * (1.0 - p)).max(0.0);
                size * fp.rate * pp.powf(fp.exponent)
            }
            _ => 0.0,
        }
    }

    /// Would this order taker-fill against the *current* book if it arrived
    /// now? (marketable & not post-only). Used to decide whether to defer the
    /// match to the midpoint of the matching window. Post-only / non-marketable
    /// orders take the immediate rest/reject path in `submit_order`.
    pub fn would_cross(&self, o: &OrderRequest) -> bool {
        if o.post_only {
            return false;
        }
        // Cross-check in the canonical matching frame (folded down → up mirror).
        let (msym, mside, mprice) = self.match_view(o);
        let is_market = matches!(o.order_type, OrderType::Market) || o.price.is_none();
        let lim = if is_market { None } else { mprice };
        let ladder = match mside {
            Side::Buy => self.books.buy_ladder(&msym),
            Side::Sell => self.books.sell_ladder(&msym),
        };
        match (ladder.first().map(|l| l.price), mside, lim) {
            (Some(bp), Side::Buy, Some(l)) => bp <= l + EPS,
            (Some(bp), Side::Sell, Some(l)) => bp >= l - EPS,
            (Some(_), _, None) => true,
            (None, _, _) => false,
        }
    }

    // ── order entry ──────────────────────────────────────────────
    pub fn submit_order(&mut self, o: &OrderRequest, now_ns: u64) -> OrderUpdate {
        // Cancel-on-arrival: a cancel for this coid already arrived (it raced
        // ahead of this place ack). Honour the strategy's cancel intent now —
        // return Cancelled WITHOUT booking the order (no rest, no fill), so it
        // never becomes a forgotten orphan resting to settlement. See
        // `pending_cancels`.
        if self.pending_cancels.remove(&o.client_order_id).is_some() {
            return self.cancelled(o, now_ns, o.quantity);
        }
        // Match in the CANONICAL frame (folded down → up mirror): the down book
        // is empty under folding, so the ladder / marketable check / sweep must
        // run on the canonical frame. Wallet settle + the OrderUpdate use the
        // ORIGINAL `o` (down token, down price).
        let (msym, mside, mprice) = self.match_view(o);
        let is_market = matches!(o.order_type, OrderType::Market) || o.price.is_none();
        let lim = if is_market { None } else { mprice };

        let ladder = match mside {
            Side::Buy => self.books.buy_ladder(&msym),
            Side::Sell => self.books.sell_ladder(&msym),
        };
        let best_opposing = ladder.first().map(|l| l.price);
        let marketable = match (best_opposing, mside, lim) {
            (Some(bp), Side::Buy, Some(l)) => bp <= l + EPS,
            (Some(bp), Side::Sell, Some(l)) => bp >= l - EPS,
            (Some(_), _, None) => true,
            (None, _, _) => false,
        };

        if o.post_only {
            self.post_only_seen += 1;
        }
        if marketable && o.post_only {
            self.rejects += 1;
            self.post_only_rejects += 1;
            return self.rejected(o, now_ns, "invalid post-only order: order crosses book");
        }
        if marketable {
            return self.take(o, &msym, mside, &ladder, lim, now_ns);
        }
        if is_market || matches!(o.order_type, OrderType::Fak | OrderType::Fok) {
            return self.cancelled(o, now_ns, o.quantity);
        }
        self.rest(o, now_ns, o.quantity)
    }

    /// Taker sweep. `msym`/`mside`/`lim`/`ladder` are the CANONICAL frame; `o` is
    /// the original order. Notional accrues in canonical prices, then is
    /// translated to the original frame (down price = 1 − canonical) for the
    /// wallet settle + ack.
    fn take(
        &mut self,
        o: &OrderRequest,
        msym: &str,
        mside: Side,
        ladder: &[PriceLevel],
        lim: Option<f64>,
        now_ns: u64,
    ) -> OrderUpdate {
        let folded = msym != o.symbol;
        // Distribution sample: fillable volume within our limit at the match
        // moment (what this taker order can actually hit on the current book).
        self.taker_avail
            .push(self.books.available_volume(msym, mside == Side::Buy, lim) as f32);
        let mut filled = 0.0;
        let mut notional = 0.0; // canonical-frame notional
        let mut fee = 0.0;
        // Taker race: if the fillable volume within our limit RECEDES in the next
        // snapshot, liquidity is being pulled away (adverse) — the taker can only
        // hit the blended volume; the unfilled remainder misses (rests/cancels).
        let mut rem = o.quantity;
        if self.taker_race_rate > 0.0 {
            let is_buy = mside == Side::Buy;
            if let Some(next_avail) = self.books.available_volume_next(msym, is_buy, lim) {
                let now_avail = self.books.available_volume(msym, is_buy, lim);
                if next_avail < now_avail {
                    let eff = self.taker_race_rate * next_avail
                        + (1.0 - self.taker_race_rate) * now_avail;
                    if eff.max(0.0) < rem {
                        self.taker_race_capped += 1;
                        if eff.max(0.0) <= EPS {
                            // Capped to ~0: full miss (Limit→rest / FAK→cancel).
                            self.taker_race_capped_zero += 1;
                        }
                    }
                    rem = rem.min(eff.max(0.0));
                }
            }
        }
        // Trade-flow taker competition (physical replacement for capture_rate):
        // same-direction takers that traded the touch in our in-flight window
        // beat us to the engine and consumed that liquidity. We fill only the
        // overflow `(now_avail − rate·competing_vol)`. Trades reveal burst
        // competition the book heals between snapshots (invisible to the race).
        if self.taker_comp_rate > 0.0 {
            let is_buy = mside == Side::Buy;
            let now_avail = self.books.available_volume(msym, is_buy, lim);
            let comp = self.taker_competition_volume(msym, mside, lim, now_ns);
            self.taker_comp_vol_sum += comp;
            self.taker_comp_n += 1;
            let eff = (now_avail - self.taker_comp_rate * comp).max(0.0);
            if eff < rem {
                self.taker_comp_capped += 1;
                if eff <= EPS {
                    self.taker_comp_capped_zero += 1;
                }
            }
            rem = rem.min(eff);
        }
        for l in ladder {
            if rem <= EPS {
                break;
            }
            let within = match (lim, mside) {
                (None, _) => true,
                (Some(p), Side::Buy) => l.price <= p + EPS,
                (Some(p), Side::Sell) => l.price >= p - EPS,
            };
            if !within {
                break;
            }
            let take = rem.min(l.quantity);
            filled += take;
            notional += take * l.price;
            // Fee is frame-invariant (p·(1−p) symmetric); compute on the original.
            fee += self.fee(&o.symbol, take, l.price);
            rem -= take;
        }

        // Translate canonical notional → original frame (down: Σ qty·(1−p) =
        // filled − Σ qty·p). For an unfolded order they're equal.
        let notional_orig = if folded { (filled - notional).max(0.0) } else { notional };

        let iid = &o.instance_id;
        if self.wallets.lockup_enabled(iid) {
            match o.side {
                Side::Buy => {
                    let avail = self.available_usdc(iid).unwrap_or(f64::MAX);
                    if notional_orig + fee > avail + EPS {
                        self.rejects += 1;
                        self.rej_taker_buy += 1;
                        return self.rejected(o, now_ns, "insufficient balance (taker buy)");
                    }
                }
                Side::Sell => {
                    if filled > self.available_shares(iid, &o.symbol) + EPS {
                        self.rejects += 1;
                        self.rej_taker_sell += 1;
                        return self.rejected(o, now_ns, "insufficient shares (taker sell)");
                    }
                }
            }
        }

        if matches!(o.order_type, OrderType::Fok) && filled + EPS < o.quantity {
            return self.cancelled(o, now_ns, o.quantity);
        }
        if filled <= EPS {
            if matches!(o.order_type, OrderType::Limit | OrderType::LimitMaker) {
                return self.rest(o, now_ns, o.quantity);
            }
            return self.cancelled(o, now_ns, o.quantity);
        }

        let avg = notional_orig / filled; // original-frame avg fill price
        match o.side {
            Side::Buy => self.wallets.settle_buy(iid, &o.symbol, filled, notional_orig + fee),
            Side::Sell => self.wallets.settle_sell(iid, &o.symbol, filled, notional_orig - fee),
        }
        self.taker_fills += 1;

        let remainder = (o.quantity - filled).max(0.0);
        if remainder > EPS && matches!(o.order_type, OrderType::Limit) {
            self.insert_resting(o, remainder, now_ns);
        }
        let taker_tid = format!("simv2-taker-{}", o.client_order_id);
        self.record_recent_fill(&o.client_order_id, taker_tid.clone(), filled, avg, o.side, &o.symbol, now_ns);
        let status = if remainder > EPS { OrderStatus::PartiallyFilled } else { OrderStatus::Filled };
        OrderUpdate {
            client_order_id: o.client_order_id.clone(),
            exchange: o.exchange,
            symbol: o.symbol.clone(),
            side: o.side,
            exchange_order_id: Some(format!("simv2-{}", o.client_order_id)),
            status,
            liquidity: Some(Liquidity::Taker),
            filled_quantity: filled,
            remaining_quantity: remainder,
            avg_fill_price: avg,
            timestamp_ns: now_ns,
            trade_id: Some(format!("simv2-taker-{}", o.client_order_id)),
            error: None,
        }
    }

    /// Insert a resting maker order, initialising its queue position to the
    /// visible merged depth at its level (§5).
    fn insert_resting(&mut self, o: &OrderRequest, remaining: f64, now_ns: u64) {
        let price = o.request_price();
        // Canonical matching frame (folded down → up mirror). q_ahead / level /
        // race peek run against the single canonical book; the original `o` is
        // kept for settle + acks. locked_usdc stays in the ORIGINAL frame.
        let (msym, mside, mprice) = self.match_view(o);
        let match_price = mprice.unwrap_or(0.0);
        // Tick of the CANONICAL frame (consistent with resync_queues / match_trade,
        // which also bucket on msym's tick) — not the original token's, which can
        // transiently differ if only one outcome stream emitted a tick-size change.
        let tick = self.tick_of(&msym);
        let now_depth = self.books.level_depth(&msym, mside, match_price, tick);
        // Maker race: if the queue at our (canonical) level GROWS in the next
        // snapshot, the level is strengthening (price about to move favorably) —
        // init q_ahead higher so we sit further back and DON'T fill on that
        // favorable move. Queue shrinking (adverse) keeps q_ahead = now, so we
        // still fill on adverse flow. Pure queue+book, one-step lookahead.
        if self.maker_race_rate > 0.0 {
            self.maker_race_placements += 1;
        }
        let q_ahead = match self.books.level_depth_next(&msym, mside, match_price, tick) {
            Some(next_depth) if self.maker_race_rate > 0.0 && next_depth > now_depth => {
                let blended =
                    self.maker_race_rate * next_depth + (1.0 - self.maker_race_rate) * now_depth;
                self.maker_race_inflated += 1;
                self.maker_race_ratio_sum += if now_depth > EPS { blended / now_depth } else { 1.0 };
                blended
            }
            _ => now_depth,
        };
        // Data-truncation fallback: the recorded book is only 5 levels deep, so a
        // quote reads level_depth = 0 (empty queue) even though live has real
        // resting size + competition there. Two cases:
        //   (1) price BEYOND the recorded window (deeper than the deepest level)
        //       → EXTRAPOLATE the depth profile (least-squares trend, clamped to
        //         the recorded qty band);
        //   (2) a gap INSIDE the window (inside the spread / between levels)
        //       → keep the best-level default: own side, else opposite side.
        let q_ahead = if q_ahead < EPS {
            if let Some(extra) = self
                .books
                .extrapolate_level_depth(&msym, mside, match_price, tick)
            {
                self.q0_extrapolated += 1;
                extra
            } else {
                self.q0_bestrule += 1;
                let (same, opp) = match mside {
                    Side::Buy => (
                        self.books.best_bid_qty(&msym, tick),
                        self.books.best_ask_qty(&msym, tick),
                    ),
                    Side::Sell => (
                        self.books.best_ask_qty(&msym, tick),
                        self.books.best_bid_qty(&msym, tick),
                    ),
                };
                if same > EPS { same } else { opp }
            }
        } else {
            q_ahead
        };
        // Distribution sample: this resting (maker) order's initial queue length.
        self.maker_q_init.push(q_ahead as f32);
        // Classify placement price vs our-side BBO (explains why q_init is 0):
        // SELL → compare to best ask; BUY → compare to best bid.
        {
            let q0 = (now_depth < EPS) as usize; // 1 if zero-queue
            let best = match mside {
                Side::Sell => self.books.eff_best_ask(&msym),
                Side::Buy => self.books.eff_best_bid(&msym),
            };
            let bucket = match best {
                None => &mut self.place_nobook,
                Some(b) => {
                    let wt = price_to_ticks(match_price, tick);
                    let bt = price_to_ticks(b, tick);
                    // "improve" = our price is better than the current best on our
                    // side (SELL lower / BUY higher) → a new/inside level.
                    let improves = match mside {
                        Side::Sell => wt < bt,
                        Side::Buy => wt > bt,
                    };
                    if wt == bt {
                        &mut self.place_join
                    } else if improves {
                        &mut self.place_improve
                    } else {
                        &mut self.place_behind
                    }
                }
            };
            bucket[0] += 1;
            bucket[1] += q0 as u64;
        }
        let locked = if o.side == Side::Buy { price * remaining } else { 0.0 };
        let mid0 = self.books.eff_mid(&msym);
        self.orders.insert(
            o.client_order_id.clone(),
            RestingOrder {
                request: o.clone(),
                match_symbol: msym,
                match_side: mside,
                match_price,
                locked_usdc: locked,
                remaining,
                tick,
                q_ahead,
                level_qty_at_sync: now_depth,
                mid_at_sync: mid0,
                traded_since_sync: 0.0,
                placed_ns: now_ns,
            },
        );
    }

    fn rest(&mut self, o: &OrderRequest, now_ns: u64, remaining: f64) -> OrderUpdate {
        let price = o.request_price();
        // Balance gate on resting placement (only when seeded).
        let iid = &o.instance_id;
        if self.wallets.lockup_enabled(iid) {
            match o.side {
                Side::Buy => {
                    if price * remaining > self.available_usdc(iid).unwrap_or(f64::MAX) + EPS {
                        self.rejects += 1;
                        self.rej_rest_buy += 1;
                        return self.rejected(o, now_ns, "insufficient balance (rest buy)");
                    }
                }
                Side::Sell => {
                    let avail = self.available_shares(iid, &o.symbol);
                    if remaining > avail + EPS {
                        self.rejects += 1;
                        self.rej_rest_sell += 1;
                        self.rej_rest_sell_short_sum += remaining - avail;
                        return self.rejected(o, now_ns, "insufficient shares (rest sell)");
                    }
                }
            }
        }
        self.insert_resting(o, remaining, now_ns);
        OrderUpdate {
            client_order_id: o.client_order_id.clone(),
            exchange: o.exchange,
            symbol: o.symbol.clone(),
            side: o.side,
            exchange_order_id: Some(format!("simv2-{}", o.client_order_id)),
            status: OrderStatus::Accepted,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: remaining,
            avg_fill_price: 0.0,
            timestamp_ns: now_ns,
            trade_id: None,
            error: None,
        }
    }

    fn rejected(&self, o: &OrderRequest, now_ns: u64, err: &str) -> OrderUpdate {
        OrderUpdate {
            client_order_id: o.client_order_id.clone(),
            exchange: o.exchange,
            symbol: o.symbol.clone(),
            side: o.side,
            exchange_order_id: None,
            status: OrderStatus::Rejected,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: o.quantity,
            // Carry the requested price (v1 contract: the strategy's post-only
            // recovery reads the rejected price from `avg_fill_price` and gates
            // on `> 0.0`, then nudges its inferred BBO).
            avg_fill_price: o.price.unwrap_or(0.0),
            timestamp_ns: now_ns,
            trade_id: None,
            error: Some(err.to_string()),
        }
    }

    fn cancelled(&self, o: &OrderRequest, now_ns: u64, remaining: f64) -> OrderUpdate {
        OrderUpdate {
            client_order_id: o.client_order_id.clone(),
            exchange: o.exchange,
            symbol: o.symbol.clone(),
            side: o.side,
            exchange_order_id: None,
            status: OrderStatus::Cancelled,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: remaining,
            avg_fill_price: 0.0,
            timestamp_ns: now_ns,
            trade_id: None,
            error: None,
        }
    }

    pub fn cancel_order(&mut self, exchange: Exchange, coid: &str, now_ns: u64) -> OrderUpdate {
        if let Some(o) = self.orders.remove(coid) {
            self.record_lifetime(o.placed_ns, now_ns);
            return OrderUpdate {
                client_order_id: coid.to_string(),
                exchange,
                symbol: o.request.symbol,
                side: o.request.side,
                exchange_order_id: None,
                status: OrderStatus::Cancelled,
                liquidity: None,
                filled_quantity: 0.0,
                remaining_quantity: 0.0,
                avg_fill_price: 0.0,
                timestamp_ns: now_ns,
                trade_id: None,
                error: None,
            };
        }
        // Not resting — matched-can't-cancel if it just filled. Re-emit the
        // original fill's trade_id so the PositionManager dedupes (no double
        // count); the strategy learns the order matched rather than cancelled.
        let window = self.matched_cant_cancel_window_ns;
        let hit = self
            .recent_fills
            .get(coid)
            .filter(|rf| now_ns.saturating_sub(rf.ts) <= window)
            .map(|rf| (rf.symbol.clone(), rf.side, rf.cum_filled, rf.price, rf.trade_id.clone()));
        if let Some((symbol, side, cum, price, trade_id)) = hit {
            self.matched_cant_cancel += 1;
            return OrderUpdate {
                client_order_id: coid.to_string(),
                exchange,
                symbol,
                side,
                exchange_order_id: None,
                status: OrderStatus::Filled,
                liquidity: None,
                filled_quantity: cum,
                remaining_quantity: 0.0,
                avg_fill_price: price,
                timestamp_ns: now_ns,
                trade_id: Some(trade_id),
                error: None,
            };
        }
        // Unknown / stale: not resting and didn't just fill. Almost always a
        // cancel that RACED AHEAD of its own place ack — the placement is still
        // in flight and will rest momentarily. Record the cancel intent so
        // `submit_order` cancels it ON ARRIVAL instead of letting it rest as an
        // orphan the strategy has already forgotten (it removes the order on the
        // `Cancelled` we return here). Without this, the order rests to
        // settlement and locks the wallet → the rest-sell-reject cascade.
        // Bound the map: drop entries whose place never arrived (e.g. a
        // timed-out placement) past a generous window.
        if self.pending_cancels.len() > 1024 {
            let cutoff = now_ns.saturating_sub(10_000_000_000);
            self.pending_cancels.retain(|_, ts| *ts >= cutoff);
        }
        self.pending_cancels.insert(coid.to_string(), now_ns);
        OrderUpdate {
            client_order_id: coid.to_string(),
            exchange,
            symbol: String::new(),
            side: Side::Buy,
            exchange_order_id: None,
            status: OrderStatus::Cancelled,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: 0.0,
            avg_fill_price: 0.0,
            timestamp_ns: now_ns,
            trade_id: None,
            error: None,
        }
    }

    /// Resolve timed-out orphans (Signal::ReconcilePolymarket). By the time this
    /// fires the order's real state is in the core: still resting → Accepted;
    /// gone → Cancelled (a fill would have been delivered independently via the
    /// fill path, which also clears the orphan). Cancels always resolve to
    /// Cancelled. No engine-side stash needed (unlike v1).
    pub fn reconcile(
        &mut self,
        pending_places: &[(String, String, Side, f64, Option<String>)],
        pending_cancels: &[(String, String)],
        now_ns: u64,
    ) -> Vec<OrderUpdate> {
        let mut out = Vec::new();
        for (coid, symbol, side, _price, _hash) in pending_places {
            let (status, remaining) = match self.orders.get(coid) {
                Some(o) => (OrderStatus::Accepted, o.remaining),
                None => (OrderStatus::Cancelled, 0.0),
            };
            out.push(OrderUpdate {
                client_order_id: coid.clone(),
                exchange: Exchange::Polymarket,
                symbol: symbol.clone(),
                side: *side,
                exchange_order_id: Some(format!("simv2-{}", coid)),
                status,
                liquidity: None,
                filled_quantity: 0.0,
                remaining_quantity: remaining,
                avg_fill_price: 0.0,
                timestamp_ns: now_ns,
                trade_id: None,
                error: None,
            });
        }
        for (coid, _oid) in pending_cancels {
            out.push(OrderUpdate {
                client_order_id: coid.clone(),
                exchange: Exchange::Polymarket,
                symbol: String::new(),
                side: Side::Buy,
                exchange_order_id: None,
                status: OrderStatus::Cancelled,
                liquidity: None,
                filled_quantity: 0.0,
                remaining_quantity: 0.0,
                avg_fill_price: 0.0,
                timestamp_ns: now_ns,
                trade_id: None,
                error: None,
            });
        }
        out
    }

    pub fn cancel_all(&mut self, exchange: Exchange, symbol: &str, now_ns: u64) -> Vec<OrderUpdate> {
        let coids: Vec<String> = self
            .orders
            .iter()
            .filter(|(_, o)| symbol.is_empty() || o.request.symbol == symbol)
            .map(|(c, _)| c.clone())
            .collect();
        coids.into_iter().map(|c| self.cancel_order(exchange, &c, now_ns)).collect()
    }
}

trait RequestPrice {
    fn request_price(&self) -> f64;
}
impl RequestPrice for OrderRequest {
    fn request_price(&self) -> f64 {
        self.price.unwrap_or(0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binary_instrument() -> Instrument {
        Instrument::BinaryOption(crate::types::instrument::BinaryOption {
            exchange: Exchange::Polymarket,
            id: "e".into(),
            question: "q".into(),
            condition_id: "cond1".into(),
            slug: "s".into(),
            clob_token_ids: vec!["up".into(), "down".into()],
            outcomes: vec!["Up".into(), "Down".into()],
            outcome_prices: vec![],
            active: true,
            closed: false,
            volume: 0.0,
            liquidity: 0.0,
            tick_size: 0.01,
            order_min_size: 5.0,
            group_item_title: String::new(),
            event_start_time: String::new(),
            base_fee: 0,
            fee_exponent: 0.0,
            fee_rate: 0.0,
        })
    }

    fn book(symbol: &str, bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) -> OrderBookSnapshot {
        book_ts(symbol, bids, asks, 0)
    }

    fn book_ts(symbol: &str, bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>, ts: u64) -> OrderBookSnapshot {
        OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: symbol.into(),
            bids: bids.into_iter().map(|(p, q)| PriceLevel { price: p, quantity: q }).collect(),
            asks: asks.into_iter().map(|(p, q)| PriceLevel { price: p, quantity: q }).collect(),
            exchange_timestamp_ns: ts,
            local_timestamp_ns: ts,
        }
    }

    fn order(coid: &str, symbol: &str, side: Side, price: f64, qty: f64, post_only: bool, ot: OrderType) -> OrderRequest {
        OrderRequest {
            client_order_id: coid.into(),
            exchange: Exchange::Polymarket,
            symbol: symbol.into(),
            side,
            order_type: ot,
            price: Some(price),
            quantity: qty,
            timestamp_ns: 0,
            instance_id: "iid".into(),
            fee_rate_bps: 0,
            post_only,
            reduce_only: false,
            outcome_label: String::new(),
        }
    }

    fn trade(symbol: &str, side: Side, price: f64, qty: f64) -> TradeTick {
        TradeTick {
            exchange: Exchange::Polymarket,
            symbol: symbol.into(),
            price,
            quantity: qty,
            side,
            exchange_timestamp_ns: 100,
            local_timestamp_ns: 100,
        }
    }

    fn core() -> SimExchangeV2 {
        let mut c = SimExchangeV2::new(500_000_000, HashMap::new(), HashMap::new());
        c.on_instrument(&binary_instrument());
        c
    }

    // ── P2 taker tests (unchanged behaviour) ──
    #[test]
    fn post_only_crossing_is_rejected() {
        let mut c = core();
        c.on_orderbook(&book("up", vec![(0.58, 100.0)], vec![(0.62, 80.0)]));
        let u = c.submit_order(&order("a", "up", Side::Buy, 0.63, 10.0, true, OrderType::Limit), 1);
        assert_eq!(u.status, OrderStatus::Rejected);
    }

    #[test]
    fn taker_buy_prefers_cross_outcome_price() {
        let mut c = core();
        c.on_orderbook(&book("up", vec![(0.58, 100.0)], vec![(0.62, 80.0)]));
        c.on_orderbook(&book("down", vec![(0.40, 70.0)], vec![(0.43, 50.0)]));
        let u = c.submit_order(&order("a", "up", Side::Buy, 0.61, 10.0, false, OrderType::Limit), 1);
        assert_eq!(u.status, OrderStatus::Filled);
        assert!((u.avg_fill_price - 0.60).abs() < 1e-9);
    }

    #[test]
    fn fok_partial_is_cancelled() {
        let mut c = core();
        c.on_orderbook(&book("up", vec![(0.58, 100.0)], vec![(0.62, 5.0)]));
        let u = c.submit_order(&order("a", "up", Side::Buy, 0.62, 10.0, false, OrderType::Fok), 1);
        assert_eq!(u.status, OrderStatus::Cancelled);
    }

    // ── P3 maker-fill tests ──
    #[test]
    fn maker_buy_fills_after_queue_drains() {
        let mut c = core();
        // Our BUY up @ 0.60 rests behind 50 visible at that level.
        c.on_orderbook(&book("up", vec![(0.60, 50.0)], vec![(0.62, 80.0)]));
        let u = c.submit_order(&order("a", "up", Side::Buy, 0.60, 10.0, true, OrderType::Limit), 1);
        assert_eq!(u.status, OrderStatus::Accepted);
        // A SELL trade @ 0.60 of 45 → q_ahead 50→5, no fill yet.
        let f0 = c.on_trade_tick(&trade("up", Side::Sell, 0.60, 45.0));
        assert!(f0.is_empty());
        // Another SELL @ 0.60 of 12 → drains remaining 5 ahead, 7 overflow → fill 7.
        let f1 = c.on_trade_tick(&trade("up", Side::Sell, 0.60, 12.0));
        assert_eq!(f1.len(), 1);
        assert_eq!(f1[0].liquidity, Some(Liquidity::Maker));
        assert!((f1[0].filled_quantity - 7.0).abs() < 1e-9);
        assert_eq!(f1[0].status, OrderStatus::PartiallyFilled);
    }

    #[test]
    fn maker_race_inflates_queue_when_next_grows() {
        // Queue at our level GROWS in the next snapshot (favorable move building
        // support) → q_ahead inflated → a trade that WOULD fill (no race) doesn't.
        let mut c = core();
        c.configure_race(1.0, 0.0); // full weight on the next snapshot
        c.on_orderbook(&book("up", vec![(0.60, 10.0)], vec![(0.62, 80.0)]));
        // Next book: same level grows 10 → 100.
        c.set_next_book("up", vec![PriceLevel { price: 0.60, quantity: 100.0 }], vec![PriceLevel { price: 0.62, quantity: 80.0 }]);
        let u = c.submit_order(&order("a", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit), 1);
        assert_eq!(u.status, OrderStatus::Accepted);
        // q_ahead ≈ 100 (race). A SELL @ 0.60 of 50 drains to 50 — still no fill.
        c.clear_next_books();
        let f = c.on_trade_tick(&trade("up", Side::Sell, 0.60, 50.0));
        assert!(f.is_empty(), "race-inflated queue (≈100) must not fill on 50");
    }

    #[test]
    fn maker_race_noop_when_next_shrinks() {
        // Queue SHRINKS next (adverse: swept/cancelled through) → q_ahead = now,
        // so we still fill on adverse flow (no protection on the adverse side).
        let mut c = core();
        c.configure_race(1.0, 0.0);
        c.on_orderbook(&book("up", vec![(0.60, 10.0)], vec![(0.62, 80.0)]));
        c.set_next_book("up", vec![PriceLevel { price: 0.60, quantity: 2.0 }], vec![PriceLevel { price: 0.62, quantity: 80.0 }]);
        let u = c.submit_order(&order("a", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit), 1);
        assert_eq!(u.status, OrderStatus::Accepted);
        // q_ahead = now (10). SELL @ 0.60 of 12 → 2 overflow → fill 2.
        c.clear_next_books();
        let f = c.on_trade_tick(&trade("up", Side::Sell, 0.60, 12.0));
        assert_eq!(f.len(), 1);
        assert!((f[0].filled_quantity - 2.0).abs() < 1e-9);
    }

    #[test]
    fn taker_race_caps_fill_when_volume_recedes() {
        // Fillable volume RECEDES next (liquidity pulled in-flight) → fill capped.
        let mut c = core();
        c.configure_race(0.0, 1.0); // full weight on the next snapshot
        c.on_orderbook(&book("up", vec![(0.58, 10.0)], vec![(0.62, 100.0)]));
        // Next book: the ask we wanted recedes 100 → 3.
        c.set_next_book("up", vec![PriceLevel { price: 0.58, quantity: 10.0 }], vec![PriceLevel { price: 0.62, quantity: 3.0 }]);
        let u = c.submit_order(&order("t", "up", Side::Buy, 0.62, 20.0, false, OrderType::Limit), 1);
        // now_avail=100, next_avail=3 → cap=3 → fills 3, remainder 17 rests.
        assert_eq!(u.status, OrderStatus::PartiallyFilled);
        assert!((u.filled_quantity - 3.0).abs() < 1e-6, "fill {} != 3", u.filled_quantity);
        assert_eq!(u.liquidity, Some(Liquidity::Taker));
    }

    #[test]
    fn taker_race_window_takes_min_volume() {
        // Windowed taker race: liquidity dips MID-window (frame 1 = 2 shares) and
        // recovers by the endpoint (frame 2 = 50). The min over the window (2) caps
        // the fill — a single endpoint snapshot would have allowed 50.
        let mut c = core();
        c.configure_race(0.0, 1.0); // full weight on the windowed next leg
        c.on_orderbook(&book("up", vec![(0.58, 10.0)], vec![(0.62, 100.0)]));
        // Two window frames: ask 2 (recedes), then ask 50 (recovers).
        c.push_next_window("up", vec![PriceLevel { price: 0.58, quantity: 10.0 }], vec![PriceLevel { price: 0.62, quantity: 2.0 }]);
        c.push_next_window("up", vec![PriceLevel { price: 0.58, quantity: 10.0 }], vec![PriceLevel { price: 0.62, quantity: 50.0 }]);
        let u = c.submit_order(&order("t", "up", Side::Buy, 0.62, 20.0, false, OrderType::Limit), 1);
        // min(2, 50) = 2 → fill capped at 2, remainder 18 rests.
        assert_eq!(u.status, OrderStatus::PartiallyFilled);
        assert!((u.filled_quantity - 2.0).abs() < 1e-6, "fill {} != 2", u.filled_quantity);
        assert_eq!(u.liquidity, Some(Liquidity::Taker));
    }

    #[test]
    fn taker_competition_caps_fill_by_inflight_trades() {
        // Trade-flow competition: only 25 shares at our ask, but competing BUY
        // takers traded 20 of them in our in-flight window → we get the overflow
        // (25 − 20 = 5), the rest misses (rests). No book recession needed; the
        // book still shows 25 (healed) — competition is read from TRADES.
        let mut c = core();
        c.configure_taker_comp(1.0, 250_000_000); // full competition, 250ms window
        c.on_orderbook(&book("up", vec![], vec![(0.62, 25.0)]));
        // Competing BUY-aggressor trade (20 @ 0.62) within the window, ts before us.
        let comp = TradeTick {
            exchange: Exchange::Polymarket,
            symbol: "up".into(),
            price: 0.62,
            quantity: 20.0,
            side: Side::Buy,
            exchange_timestamp_ns: 1_000,
            local_timestamp_ns: 1_000,
        };
        c.on_trade_tick(&comp);
        let u = c.submit_order(&order("t", "up", Side::Buy, 0.62, 20.0, false, OrderType::Limit), 200_000);
        // now_avail=25, comp=20 → eff=5 → fill 5, remainder 15 rests.
        assert_eq!(u.status, OrderStatus::PartiallyFilled);
        assert!((u.filled_quantity - 5.0).abs() < 1e-6, "fill {} != 5", u.filled_quantity);
        assert_eq!(u.liquidity, Some(Liquidity::Taker));
        assert_eq!(c.taker_comp_capped, 1);
    }

    #[test]
    fn taker_competition_off_fills_full() {
        // Same setup, competition OFF → no trade-flow cap, fills the full 20.
        let mut c = core();
        c.on_orderbook(&book("up", vec![], vec![(0.62, 25.0)]));
        let comp = TradeTick {
            exchange: Exchange::Polymarket, symbol: "up".into(), price: 0.62,
            quantity: 20.0, side: Side::Buy, exchange_timestamp_ns: 1_000, local_timestamp_ns: 1_000,
        };
        c.on_trade_tick(&comp);
        let u = c.submit_order(&order("t", "up", Side::Buy, 0.62, 20.0, false, OrderType::Limit), 200_000);
        assert_eq!(u.status, OrderStatus::Filled);
        assert!((u.filled_quantity - 20.0).abs() < 1e-6, "fill {} != 20", u.filled_quantity);
    }

    #[test]
    fn tick_size_change_rebaselines_resting_order() {
        // A resting maker straddles a 0.01→0.001 tick refinement. Its `tick`
        // snapshot must update and its queue re-baseline at the new grid, so a
        // subsequent identical book snapshot does NOT produce a spurious
        // cancel/grow (which would corrupt q_ahead via the bucketing discontinuity).
        let mut c = core();
        c.on_orderbook(&book("up", vec![(0.95, 100.0)], vec![]));
        let u = c.submit_order(&order("m", "up", Side::Buy, 0.95, 10.0, true, OrderType::Limit), 1);
        assert_eq!(u.status, OrderStatus::Accepted);
        assert!((c.orders["m"].tick - 0.01).abs() < 1e-12);
        assert!((c.orders["m"].q_ahead - 100.0).abs() < 1e-6);
        let tsc = TickSizeChange {
            exchange: Exchange::Polymarket,
            symbol: "up".into(),
            old_tick_size: 0.01,
            new_tick_size: 0.001,
            local_timestamp_ns: 2,
        };
        c.on_tick_size_change(&tsc);
        assert!((c.tick_of("up") - 0.001).abs() < 1e-12);
        assert!((c.orders["m"].tick - 0.001).abs() < 1e-12, "o.tick stale: {}", c.orders["m"].tick);
        // Identical book → re-baselined → no spurious q_ahead change.
        c.on_orderbook(&book("up", vec![(0.95, 100.0)], vec![]));
        assert!((c.orders["m"].q_ahead - 100.0).abs() < 1e-6,
            "q_ahead spuriously changed to {}", c.orders["m"].q_ahead);
    }

    #[test]
    fn tick_size_change_propagates_to_canonical_under_folding() {
        // Under folding, matching runs in the canonical (up) frame. A tick change
        // emitted only for the sibling (down) stream must still update the
        // canonical token's tick.
        let mut c = SimExchangeV2::new(
            500_000_000,
            HashMap::from([("iid".to_string(), 1000.0)]),
            HashMap::from([("iid".to_string(), 100.0)]),
        );
        c.set_fold_outcomes(true);
        c.on_instrument(&binary_instrument()); // canonical = "up", sibling = "down"
        let tsc = TickSizeChange {
            exchange: Exchange::Polymarket,
            symbol: "down".into(),
            old_tick_size: 0.01,
            new_tick_size: 0.001,
            local_timestamp_ns: 1,
        };
        c.on_tick_size_change(&tsc);
        assert!((c.tick_of("up") - 0.001).abs() < 1e-12, "canonical tick not updated: {}", c.tick_of("up"));
        assert!((c.tick_of("down") - 0.001).abs() < 1e-12);
    }

    #[test]
    fn maker_sell_down_fills_via_cross_outcome_mirror() {
        let mut c = SimExchangeV2::new(
            500_000_000,
            HashMap::from([("iid".to_string(), 1000.0)]),
            HashMap::from([("iid".to_string(), 100.0)]),
        );
        c.on_instrument(&binary_instrument()); // seeds 100 down shares
        // Empty book both sides so our SELL-down rests at the front with q_ahead=0
        // (the q_init data-truncation fallback only fires when a best level exists).
        c.on_orderbook(&book("down", vec![], vec![]));
        let u = c.submit_order(&order("s", "down", Side::Sell, 0.40, 10.0, true, OrderType::Limit), 1);
        assert_eq!(u.status, OrderStatus::Accepted);
        // A SELL-up @ 0.60 trade mirrors to down: flip→BUY, price 1−0.60=0.40.
        // BUY aggressor @ 0.40 ≥ our sell 0.40 → fills our resting SELL-down.
        let f = c.on_trade_tick(&trade("up", Side::Sell, 0.60, 10.0));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].side, Side::Sell);
        assert_eq!(f[0].symbol, "down");
        assert_eq!(f[0].liquidity, Some(Liquidity::Maker));
        assert_eq!(f[0].status, OrderStatus::Filled);
        assert!((f[0].avg_fill_price - 0.40).abs() < 1e-9);
    }

    // ── q_init fallback tests ──
    #[test]
    fn q_init_extrapolates_beyond_recorded_window() {
        // 5-level ask book; a SELL placed BEYOND the deepest recorded ask gets an
        // extrapolated (non-zero, clamped) queue — not 0 and not the best-level
        // default. A SELL inside the recorded window at an empty tick (gap) takes
        // the best-level rule instead.
        let mut c = core();
        // up asks: 0.60×40, 0.61×100, 0.62×130, 0.63×130, 0.64×80 (5 levels).
        c.on_orderbook(&book(
            "up",
            vec![(0.50, 50.0)],
            vec![(0.60, 40.0), (0.61, 100.0), (0.62, 130.0), (0.63, 130.0), (0.64, 80.0)],
        ));
        // SELL @ 0.70 is beyond the deepest recorded ask (0.64) → extrapolated.
        let beyond = c.books.extrapolate_level_depth("up", Side::Sell, 0.70, 0.01);
        assert!(beyond.is_some(), "beyond-window must extrapolate");
        let q = beyond.unwrap();
        assert!(q >= 40.0 - 1e-6 && q <= 130.0 + 1e-6, "extrapolated {} out of recorded band", q);
        // SELL @ 0.615 is INSIDE the window (between 0.61 and 0.62) → None.
        assert!(c.books.extrapolate_level_depth("up", Side::Sell, 0.615, 0.01).is_none(),
            "in-window gap must not extrapolate");
        // End-to-end: resting SELL @ 0.70 gets the extrapolated q_ahead, so a
        // small SELL-aggressor trade at 0.70 does NOT immediately fill us.
        let u = c.submit_order(&order("a", "up", Side::Sell, 0.70, 5.0, true, OrderType::Limit), 1);
        assert_eq!(u.status, OrderStatus::Accepted);
        let f = c.on_trade_tick(&trade("up", Side::Buy, 0.70, 3.0)); // 3 < extrapolated queue
        assert!(f.is_empty(), "extrapolated queue must protect against tiny fill");
    }

    // ── outcome-folding tests ──
    #[test]
    fn fold_book_no_double_count() {
        // With folding the down snapshot maps into the SINGLE canonical book;
        // level_depth must NOT double-count (old complement-merge added both).
        // NOTE: fold must be enabled BEFORE on_instrument (which builds fold_to).
        let mut c = SimExchangeV2::new(500_000_000, HashMap::new(), HashMap::new());
        c.set_fold_outcomes(true);
        c.on_instrument(&binary_instrument());
        c.on_orderbook(&book_ts("up", vec![(0.60, 50.0)], vec![(0.62, 80.0)], 100));
        // Newer down ask 0.40 ×30 mirrors to up bid 0.60 and REPLACES the book.
        c.on_orderbook(&book_ts("down", vec![(0.38, 20.0)], vec![(0.40, 30.0)], 200));
        let d = c.books.level_depth("up", Side::Buy, 0.60, 0.01);
        assert!((d - 30.0).abs() < 1e-9, "expected single-count 30, got {}", d);
    }

    #[test]
    fn fold_book_staleness_drops_older_snapshot() {
        let mut c = SimExchangeV2::new(500_000_000, HashMap::new(), HashMap::new());
        c.set_fold_outcomes(true);
        c.on_instrument(&binary_instrument());
        c.on_orderbook(&book_ts("up", vec![(0.60, 50.0)], vec![(0.62, 80.0)], 200));
        c.on_orderbook(&book_ts("up", vec![(0.60, 5.0)], vec![(0.62, 80.0)], 100)); // older → dropped
        let d = c.books.level_depth("up", Side::Buy, 0.60, 0.01);
        assert!((d - 50.0).abs() < 1e-9, "stale snapshot must be dropped, got {}", d);
    }

    #[test]
    fn fold_down_maker_fills_via_canonical_settles_original() {
        // Down maker SELL @ 0.40 → matched in canonical up frame (BUY up @ 0.60),
        // drained by a folded down trade, settled as DOWN @ 0.40.
        let mut c = SimExchangeV2::new(
            500_000_000,
            HashMap::from([("iid".to_string(), 1000.0)]),
            HashMap::from([("iid".to_string(), 100.0)]),
        );
        c.set_fold_outcomes(true);
        c.on_instrument(&binary_instrument());
        c.on_orderbook(&book("up", vec![(0.60, 10.0)], vec![(0.62, 80.0)]));
        let u = c.submit_order(&order("s", "down", Side::Sell, 0.40, 10.0, true, OrderType::Limit), 1);
        assert_eq!(u.status, OrderStatus::Accepted);
        assert_eq!(u.symbol, "down");
        // Folded trade: down BUY @ 0.40 of 15 → canonical SELL up @ 0.60 of 15.
        // Drains q_ahead 10, overflow 5 → fills our down SELL 5 @ 0.40.
        let f = c.on_trade_tick(&trade("down", Side::Buy, 0.40, 15.0));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].symbol, "down");
        assert_eq!(f[0].side, Side::Sell);
        assert!((f[0].filled_quantity - 5.0).abs() < 1e-9, "fill {}", f[0].filled_quantity);
        assert!((f[0].avg_fill_price - 0.40).abs() < 1e-9);
    }

    #[test]
    fn fold_down_taker_crosses_canonical_settles_original() {
        // A marketable DOWN BUY must execute as a TAKER in the canonical frame
        // (not silently rest because the down book is empty under folding).
        // Down BUY @ 0.45 ≡ up SELL @ 0.55; up best bid 0.60 ≥ 0.55 → crosses.
        let mut c = SimExchangeV2::new(
            500_000_000,
            HashMap::from([("iid".to_string(), 1000.0)]),
            HashMap::new(),
        );
        c.set_fold_outcomes(true);
        c.on_instrument(&binary_instrument());
        c.on_orderbook(&book("up", vec![(0.60, 50.0)], vec![(0.62, 80.0)]));
        let u = c.submit_order(&order("t", "down", Side::Buy, 0.45, 10.0, false, OrderType::Limit), 1);
        assert_eq!(u.status, OrderStatus::Filled, "down taker must fill, not rest");
        assert_eq!(u.liquidity, Some(Liquidity::Taker));
        assert_eq!(u.symbol, "down");
        // Canonical fill @ up bid 0.60 → original down price 1−0.60 = 0.40.
        assert!((u.avg_fill_price - 0.40).abs() < 1e-9, "down avg {} != 0.40", u.avg_fill_price);
    }

    #[test]
    fn matched_cant_cancel_returns_filled_with_same_trade_id() {
        let mut c = core();
        c.on_orderbook(&book("up", vec![(0.60, 50.0)], vec![(0.62, 80.0)]));
        let _ = c.submit_order(&order("a", "up", Side::Buy, 0.60, 10.0, true, OrderType::Limit), 1);
        // Big SELL @ 0.60 drains 50 ahead + fills our 10.
        let fills = c.on_trade_tick(&trade("up", Side::Sell, 0.60, 100.0));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].status, OrderStatus::Filled);
        let fill_tid = fills[0].trade_id.clone().unwrap();
        // Cancel within window → Filled with the SAME trade_id (PM dedupes).
        let u = c.cancel_order(Exchange::Polymarket, "a", 200);
        assert_eq!(u.status, OrderStatus::Filled);
        assert_eq!(u.trade_id, Some(fill_tid));
        assert_eq!(c.matched_cant_cancel, 1);
    }

    #[test]
    fn reconcile_resting_accepted_gone_cancelled() {
        let mut c = core();
        c.on_orderbook(&book("up", vec![(0.60, 50.0)], vec![(0.62, 80.0)]));
        let _ = c.submit_order(&order("a", "up", Side::Buy, 0.60, 10.0, true, OrderType::Limit), 1);
        let out = c.reconcile(&[("a".into(), "up".into(), Side::Buy, 0.60, None)], &[], 100);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].status, OrderStatus::Accepted);
        let out2 = c.reconcile(&[("ghost".into(), "up".into(), Side::Buy, 0.60, None)], &[], 100);
        assert_eq!(out2[0].status, OrderStatus::Cancelled);
        let out3 = c.reconcile(&[], &[("c".into(), "oid".into())], 100);
        assert_eq!(out3[0].status, OrderStatus::Cancelled);
    }

    #[test]
    fn cancel_attribution_advances_queue() {
        let mut c = core();
        c.on_orderbook(&book("up", vec![(0.60, 100.0)], vec![(0.62, 80.0)]));
        let _ = c.submit_order(&order("a", "up", Side::Buy, 0.60, 10.0, true, OrderType::Limit), 1);
        // Level shrinks 100→40 with no trades → 60 cancels; proportional
        // ahead_frac = q_ahead/level = 100/100 = 1 → q_ahead 100→40.
        c.on_orderbook(&book("up", vec![(0.60, 40.0)], vec![(0.62, 80.0)]));
        // A SELL @ 0.60 of 45 → drains 40 ahead, 5 overflow → fill 5.
        let f = c.on_trade_tick(&trade("up", Side::Sell, 0.60, 45.0));
        assert_eq!(f.len(), 1);
        assert!((f[0].filled_quantity - 5.0).abs() < 1e-9);
    }

    #[test]
    fn adverse_sel_conditioning_tilts_queue_advance() {
        // Adverse mid move → cancels are informed (ahead) → ahead_frac→1 → queue
        // advances → fill the toxic flow. Favorable move → ahead_frac→0 → queue
        // holds → miss. Needs base = q_ahead/level < 1 to have room to tilt, so
        // drain the queue below the level first.
        let probe = |ask_after: f64| {
            let mut c = core();
            c.configure_adverse_sel(4.0, 1.0); // strong tilt (|s|→1 either way)
            c.on_orderbook(&book("up", vec![(0.60, 100.0)], vec![(0.62, 100.0)]));
            let u = c.submit_order(&order("a", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit), 1);
            assert_eq!(u.status, OrderStatus::Accepted); // q_ahead=100, mid_at_sync=0.61
            // Drain q_ahead 100→50 (no fill); records traded_since_sync=50.
            assert!(c.on_trade_tick(&trade("up", Side::Sell, 0.60, 50.0)).is_empty());
            // Book: level cancels 100→30 (cancels = 100−50−30 = 20); the ask moves
            // to `ask_after` → the mid signal. base = q_ahead/l_prev = 50/100 = 0.5.
            c.on_orderbook(&book("up", vec![(0.60, 30.0)], vec![(ask_after, 100.0)]));
            // Probe SELL 40 @ 0.60 fills iff q_ahead < 40.
            c.on_trade_tick(&trade("up", Side::Sell, 0.60, 40.0))
        };
        // ask 0.61 → mid fell 0.61→0.605 = ADVERSE for a bid: ahead_frac→1 →
        // q_ahead 50→30 (<40) → the toxic flow fills us.
        assert!(!probe(0.61).is_empty(), "adverse move must advance queue → fill");
        // ask 0.63 → mid rose 0.61→0.615 = FAVORABLE: ahead_frac→0 → q_ahead
        // holds at 50 (>40) → we miss (the move we'd have wanted).
        assert!(probe(0.63).is_empty(), "favorable move must hold queue → no fill");
    }

    #[test]
    fn book_through_fills_only_on_trade_confirmed_cross() {
        // Option C: a touch/cross fills ONLY when a trade in the interval confirms
        // a real match (sell ≤ p for a bid). A touch with NO trade is flicker →
        // no fill. Resting BID @ 0.55, q_ahead=100; ask touches/crosses to 0.54.
        let probe = |with_trade: bool| {
            let mut c = core();
            c.configure_book_through(1.0);
            c.on_orderbook(&book("up", vec![(0.55, 100.0)], vec![(0.57, 100.0)]));
            let u = c.submit_order(&order("a", "up", Side::Buy, 0.55, 10.0, true, OrderType::Limit), 1);
            assert_eq!(u.status, OrderStatus::Accepted); // q_ahead=100
            if with_trade {
                // Small sell @ 0.55 — too small to fill via the trade path
                // (over = 10−100 < 0) but it RECORDS the trade-cross gate.
                assert!(c.on_trade_tick(&trade("up", Side::Sell, 0.55, 10.0)).is_empty());
            }
            c.on_orderbook(&book("up", vec![(0.55, 100.0)], vec![(0.54, 200.0)]))
        };
        let fills = probe(true);
        assert_eq!(fills.len(), 1, "trade-confirmed cross → book-through fill");
        assert_eq!(fills[0].status, OrderStatus::Filled);
        assert_eq!(fills[0].liquidity, Some(Liquidity::Maker));
        assert!((fills[0].avg_fill_price - 0.55).abs() < 1e-9, "fills at the limit (adverse)");
        assert!(probe(false).is_empty(), "cross with NO trade is flicker → no fill");
    }

    #[test]
    fn forward_markout_vn_reprices_favorable_fills() {
        // A trade that fully fills a maker bid (10 sh) keeps the FULL quantity but
        // is RE-PRICED adverse toward the forward mid when markout>0 (vn>0); adverse
        // / no-signal fills settle at the limit. vn=1, markout +0.01 → BUY pays 0.56.
        let probe = |fwd: Option<f64>| {
            let mut c = core();
            c.configure_fill_markout_vn(1.0);
            c.on_orderbook(&book("up", vec![(0.55, 10.0)], vec![(0.57, 100.0)]));
            let u = c.submit_order(&order("a", "up", Side::Buy, 0.55, 10.0, true, OrderType::Limit), 1);
            assert_eq!(u.status, OrderStatus::Accepted); // q_ahead=10
            // SELL 20 @ 0.55 → over = 20−10 = 10 → fully fills (vn keeps quantity).
            c.on_trade_tick_fwd(&trade("up", Side::Sell, 0.55, 20.0), fwd)
        };
        // Favorable: fwd mid 0.56 → markout +0.01 → BUY repriced to 0.56, FULL fill.
        let f = probe(Some(0.56));
        assert_eq!(f.len(), 1);
        assert!((f[0].filled_quantity - 10.0).abs() < 1e-9, "vn keeps full fill {}", f[0].filled_quantity);
        assert!((f[0].avg_fill_price - 0.56).abs() < 1e-9, "repriced adverse {}", f[0].avg_fill_price);
        assert_eq!(f[0].status, OrderStatus::Filled);
        // Adverse: fwd mid 0.54 → markout −0.01 → no reprice → fill at the limit.
        let fa = probe(Some(0.54));
        assert!((fa[0].avg_fill_price - 0.55).abs() < 1e-9, "adverse at limit {}", fa[0].avg_fill_price);
        assert!((fa[0].filled_quantity - 10.0).abs() < 1e-9, "adverse full fill");
        // No forward signal → no reprice → fill at the limit.
        assert!((probe(None)[0].avg_fill_price - 0.55).abs() < 1e-9, "no-signal at limit");
    }
}
