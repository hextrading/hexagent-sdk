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
    /// Exchange-side dust retained when a selected order is nearly exhausted
    /// by maker matching. Public aggregate prints cannot identify whether this
    /// individual residual was allocated, so it remains until cancel/retire.
    /// 0 preserves exact historical full-fill behaviour.
    inferred_residual_floor: f64,
    inferred_residual_realized: bool,
    /// Tick size snapshot (avoids a self.tick borrow during re-sync).
    tick: f64,
    /// Shares ahead of us in the FIFO queue at our synthetic level (§5).
    q_ahead: f64,
    /// Portion of `q_ahead` contributed by earlier simulated orders at this
    /// exact canonical level. It is tracked separately so a fresh-book rebase
    /// cannot erase own FIFO priority and an earlier cancel can advance only
    /// the later orders it actually preceded.
    own_q_ahead: f64,
    /// Monotonic exchange-arrival identity. Unlike client ids and nanosecond
    /// timestamps this remains a strict FIFO tie-break for same-batch orders.
    queue_seq: u64,
    /// Approximate own-live-order depth removed from queue-ahead for a replay
    /// tape captured while that strategy was active. The raw book reference is
    /// intentionally retained so disappearance of this depth remains visible
    /// to the unexplained-depletion execution model.
    replay_self_depth_credit: f64,
    /// Visible level depth at the last book snapshot (cancel-attribution ref).
    level_qty_at_sync: f64,
    /// Canonical-frame effective mid at the last snapshot. The signed move
    /// vs the current mid is the adverse-selection signal for the cancel
    /// attribution (see `resync_queues`). 0.0 ⇒ no mid at placement.
    mid_at_sync: f64,
    /// Canonical effective mid when the order first rested. This is immutable
    /// and is the causal maker-toxicity reference (no forward book peek).
    entry_mid: f64,
    /// Trade qty matched at our level since the last snapshot.
    traded_since_sync: f64,
    /// Server-time this order rested (for lifetime / fill-age diagnostics).
    placed_ns: u64,
    /// The order reached the engine while its cached full book was stale. It
    /// cannot maker-fill until the next accepted full book re-bases its queue
    /// at the then-visible level depth.
    await_fresh_book: bool,
}

fn accrue_order_exposure(audit: &mut MakerOrderAuditRow, now_ns: u64, remaining_before: f64) {
    let bounded_now_ns = if audit.exposure_end_ns > 0 {
        now_ns.min(audit.exposure_end_ns)
    } else {
        now_ns
    };
    let delta_ns = bounded_now_ns.saturating_sub(audit.exposure_last_ns);
    audit.rest_time_ns = audit.rest_time_ns.saturating_add(delta_ns);
    audit.rest_qty_ns += remaining_before.max(0.0) * delta_ns as f64;
    audit.exposure_last_ns = audit.exposure_last_ns.max(bounded_now_ns);
}

fn event_exposure_end_ns(slug: &str) -> u64 {
    slug.rsplit_once('-')
        .and_then(|(_, epoch)| epoch.parse::<u64>().ok())
        .and_then(|epoch| epoch.checked_add(300))
        .and_then(|epoch| epoch.checked_mul(1_000_000_000))
        .unwrap_or(0)
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
    filled_quantity: f64,
    price: f64,
    side: Side,
    symbol: String,
    liquidity: Liquidity,
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
    queue_seq: u64,
}

/// A possible maker fill caused by one shared same-level depletion event.
/// Queue advancement is evaluated per order, but the execution volume that
/// crosses the queue is a property of the public level and may be consumed
/// only once across all of our orders at that level.
struct DepletionCandidate {
    match_symbol: String,
    match_side: Side,
    match_price: f64,
    match_side_rank: u8,
    match_price_tick: i64,
    match_tick_bits: u64,
    placed_ns: u64,
    coid: String,
    token: String,
    side: Side,
    iid: String,
    price: f64,
    potential: f64,
    level_execution: f64,
}

/// Diagnostic-only attribution for one strategy instance in one binary event.
/// Quantities are deliberately kept separate from decision counts: a gate that
/// blocks many tiny probes is materially different from one that suppresses a
/// single large executable order. None of these fields feed matching decisions.
#[derive(Clone, Debug, Default)]
pub struct FillAuditRow {
    pub slug: String,
    pub iid: String,
    pub place_orders: u64,
    pub place_qty: f64,
    pub passive_place_orders: u64,
    pub passive_place_qty: f64,
    pub cancel_before_place_orders: u64,
    pub cancel_before_place_qty: f64,
    pub stale_order_blocks: u64,
    pub stale_order_qty: f64,
    pub post_only_rejects: u64,
    pub post_only_reject_qty: f64,
    pub maker_rests: u64,
    pub maker_rest_qty: f64,
    pub passive_rests: u64,
    pub passive_rest_qty: f64,
    pub maker_rest_time_ns: u128,
    pub maker_rest_qty_ns: f64,
    pub passive_rest_time_ns: u128,
    pub passive_rest_qty_ns: f64,
    pub maker_cancel_orders: u64,
    pub maker_cancel_qty: f64,
    pub maker_orders_with_fill: u64,
    pub maker_open_orders: u64,
    pub passive_cancel_orders: u64,
    pub passive_cancel_qty: f64,
    pub passive_orders_with_fill: u64,
    pub passive_fill_qty: f64,
    pub passive_open_orders: u64,
    pub maker_q_init_sum: f64,
    pub maker_own_q_init_sum: f64,
    pub maker_own_cancel_queue_advance_qty: f64,
    pub maker_race_added_q: f64,
    pub maker_replay_self_depth_credit: f64,
    pub maker_trade_matches: u64,
    pub maker_trade_qty: f64,
    pub maker_queue_drained_qty: f64,
    pub maker_candidate_qty: f64,
    pub maker_toxicity_suppressed_qty: f64,
    pub maker_depletion_observed_qty: f64,
    pub maker_depletion_exec_qty: f64,
    pub maker_depletion_cancel_advance_qty: f64,
    pub maker_depletion_candidate_qty: f64,
    pub maker_depletion_budget_suppressed_qty: f64,
    pub maker_depletion_fill_qty: f64,
    pub maker_book_markout_qty: f64,
    pub maker_book_markout_cost_usdc: f64,
    pub maker_fill_qty: f64,
    pub stale_trade_matches: u64,
    pub stale_trade_candidate_qty: f64,
    pub taker_candidates: u64,
    pub taker_requested_qty: f64,
    pub taker_available_qty: f64,
    pub taker_replay_self_depth_qty: f64,
    pub taker_race_suppressed_qty: f64,
    pub taker_comp_suppressed_qty: f64,
    pub taker_zero_fills: u64,
    pub taker_fill_qty: f64,
}

/// Diagnostic-only maker lifecycle for one simulated order.
///
/// Enabled only with the backtest fill-audit switch.  These immutable/request
/// fields plus causal queue transitions are the sim half of the live
/// `[order_attempt]` replica join; none feed back into matching.
#[derive(Clone, Debug)]
pub struct MakerOrderAuditRow {
    pub slug: String,
    pub iid: String,
    pub coid: String,
    pub token: String,
    pub side: Side,
    pub order_type: OrderType,
    pub price: f64,
    pub quantity: f64,
    pub post_only: bool,
    pub strategy_emit_ns: u64,
    pub trigger_exchange_ns: u64,
    pub trigger_local_ns: u64,
    pub place_arrival_ns: u64,
    pub exposure_last_ns: u64,
    pub exposure_end_ns: u64,
    pub rest_time_ns: u64,
    pub rest_qty_ns: f64,
    pub await_fresh_book: bool,
    pub visible_depth_at_entry: f64,
    pub entry_mid: f64,
    pub queue_seq: u64,
    pub q_init: f64,
    pub simulated_own_ahead_qty: f64,
    pub own_cancel_queue_advance_qty: f64,
    pub replay_self_depth_credit: f64,
    pub trade_match_n: u64,
    pub trade_match_qty: f64,
    pub queue_drained_qty: f64,
    pub candidate_qty: f64,
    pub maker_toxicity_suppressed_qty: f64,
    pub depletion_observed_qty: f64,
    pub depletion_exec_qty: f64,
    pub depletion_cancel_advance_qty: f64,
    pub depletion_candidate_qty: f64,
    pub depletion_budget_suppressed_qty: f64,
    pub depletion_fill_qty: f64,
    pub inferred_residual_floor: f64,
    pub inferred_residual_suppressed_qty: f64,
    pub book_through_candidate_qty: f64,
    pub book_through_fill_qty: f64,
    pub book_markout_qty: f64,
    pub book_markout_cost_usdc: f64,
    pub fill_qty: f64,
    pub first_fill_ns: u64,
    pub last_fill_ns: u64,
    pub first_fill_delivery_ns: u64,
    pub last_fill_delivery_ns: u64,
    pub cancel_arrival_ns: u64,
    pub cancel_result: &'static str,
    pub q_ahead_final: f64,
    pub remaining_final: f64,
}

fn flip(s: Side) -> Side {
    match s {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}

/// Stable, allocation-free [0,1) sample for a client order id. FNV-1a is
/// sufficient here because this is deterministic cohort selection, not
/// randomness or an execution hot-path probability draw.
fn stable_order_sample(coid: &str) -> f64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in coid.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // Avalanche the structured iid/timestamp/counter suffixes before taking
    // the high 53 bits. Raw FNV high bits are biased for adjacent client ids.
    hash ^= hash >> 30;
    hash = hash.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash ^= hash >> 27;
    hash = hash.wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^= hash >> 31;
    ((hash >> 11) as f64) / ((1_u64 << 53) as f64)
}

/// Reprice only the favorable component of a maker fill toward the future
/// canonical mid. The penalty magnitude is invariant under outcome folding;
/// settlement direction follows the original order side.
fn maker_markout_reprice(
    limit: f64,
    original_side: Side,
    match_side: Side,
    match_price: f64,
    fwd_mid: Option<f64>,
    strength: f64,
) -> (f64, f64) {
    let Some(fm) = fwd_mid.filter(|mid| mid.is_finite()) else {
        return (limit, 0.0);
    };
    if strength <= 0.0 {
        return (limit, 0.0);
    }
    let favorable = match match_side {
        Side::Buy => fm - match_price,
        Side::Sell => match_price - fm,
    };
    if favorable <= 0.0 {
        return (limit, 0.0);
    }
    let requested_penalty = strength * favorable;
    let price = match original_side {
        Side::Buy => (limit + requested_penalty).min(1.0),
        Side::Sell => (limit - requested_penalty).max(0.0),
    };
    (price, (price - limit).abs())
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
    /// Single-writer exchange-arrival sequence for per-order FIFO identity.
    next_queue_seq: u64,
    /// Fraction of earlier simulated same-level remaining quantity included in
    /// a new resting order's queue ahead. 0 = exact historical behaviour.
    order_queue_position_strength: f64,
    pub own_queue_positioned_orders: u64,
    pub own_queue_initial_qty: f64,
    pub own_queue_cancel_advances_n: u64,
    pub own_queue_cancel_advance_qty: f64,
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
    /// Token → event slug, retained only while the token is live. Audit rows
    /// themselves are retained for the complete replay and emitted at exit.
    event_slug_by_token: HashMap<String, String>,
    fill_audit: BTreeMap<(String, String), FillAuditRow>,
    /// Per-order rows are opt-in because a multi-day replay can place hundreds
    /// of thousands of orders.  BTreeMap keeps output deterministic by coid.
    maker_order_audit_enabled: bool,
    maker_order_audit: BTreeMap<String, MakerOrderAuditRow>,
    /// Latest causal exchange timestamp observed by the matching core. Used
    /// only to snapshot still-open exposure at the end of an audit run.
    audit_clock_ns: u64,
    /// Symmetric outcome-sibling map (a↔b) for the race lookahead (peek the
    /// canonical frame's next book from EITHER outcome stream).
    fold_sibling: HashMap<String, String>,
    /// Last applied full-book exchange timestamp per canonical token. This is
    /// both the folded out-of-order guard and one clock of the matching stale
    /// gate.
    last_book_ts: HashMap<String, u64>,
    /// Local receive timestamp carried by the last accepted full book. Together
    /// with `last_book_ts` this detects both a silent recorder/client feed and a
    /// server snapshot whose own timestamp stopped advancing.
    last_book_local_ts: HashMap<String, u64>,
    /// Shared max age for both clocks. 0 preserves the historical model.
    book_stale_after_ns: u64,
    /// Existing resting makers remain live at the exchange when only the local
    /// full-book receive clock is stale. Order admission still uses both clocks.
    stale_resting_exchange_only: bool,
    pub book_stale_order_blocks: u64,
    pub book_stale_trade_blocks: u64,
    pub book_stale_exchange_hits: u64,
    pub book_stale_local_hits: u64,
    pub book_stale_rebases: u64,
    /// Fraction of attributed cancels that sit ahead of us. `None` = the
    /// default proportional model (`q_ahead / level`); `Some(f)` pins it.
    ahead_frac_override: Option<f64>,
    /// Blend from the configured fixed override toward the causal queue-position
    /// fraction (`q_ahead / level depth`) on every queue resync.
    dynamic_ahead_frac_strength: f64,
    pub dynamic_ahead_frac_n: u64,
    pub dynamic_ahead_frac_sum: f64,
    pub dynamic_ahead_frac_min: f64,
    pub dynamic_ahead_frac_max: f64,
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
    /// Causal maker selection: favorable movement since entry makes an
    /// apparent public-trade overflow less likely to execute our order. The
    /// suppressed portion becomes latent queue ahead; actual fills remain at
    /// the real limit. 0 preserves the historical trade matcher.
    maker_toxicity_strength: f64,
    maker_toxicity_scale_ticks: f64,
    pub maker_toxicity_suppressed_n: u64,
    pub maker_toxicity_suppressed_qty: f64,
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
    /// Maximum fraction of same-level shrinkage left after public trades that
    /// can be interpreted as hidden/aggregated execution rather than
    /// cancellation. A causal adverse-mid-move gate scales the fraction;
    /// unlike a fill-probability scalar, the resulting volume must drain queue
    /// ahead before its overflow can fill.
    unexplained_depletion_exec_rate: f64,
    /// # maker fill fragments produced by unexplained depletion. Diagnostic.
    pub unexplained_depletion_fills_n: u64,
    /// Deterministic share of orders whose inferred fills retain exchange-side
    /// dust, plus the configured fraction of original order size.
    inferred_maker_residual_rate: f64,
    inferred_maker_residual_fraction: f64,
    pub inferred_maker_residual_orders_n: u64,
    pub inferred_maker_residual_qty: f64,
    /// Approximate leave-one-out fraction for a replay tape containing this
    /// strategy's original live order at the same level. 0 = ordinary clean
    /// tape; positive values subtract up to rate·remaining from queue-ahead.
    replay_self_depth_rate: f64,
    /// Leave-one-out fraction for taker sweeps on a tape recorded while this
    /// same strategy instance was live. Opposite-side simulated resting orders
    /// are removed from their exact canonical levels before marketability,
    /// available-volume, and sweep-price decisions. The exchange core is the
    /// sole writer; no shared account or cross-instance state is consulted.
    replay_self_taker_depth_rate: f64,
    pub taker_replay_self_sweeps_n: u64,
    pub taker_replay_self_depth_qty: f64,
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
    /// Independent markout strength for maker fills inferred from order-book
    /// transitions rather than public trade prints.
    book_fill_markout_vn: f64,
    pub book_fill_haircut_n: u64,
    pub book_fill_haircut_qty: f64,
    pub book_fill_haircut_cost_usdc: f64,
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
    /// If true, race and trade-flow competition are assumed to overlap fully:
    /// compute both caps from the original request and apply the less
    /// restrictive cap once. False preserves the historical stricter cap.
    taker_overlap_dedup: bool,
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
    pub dynamic_deep_queue_n: u64,
    pub dynamic_deep_queue_decay_sum: f64,
    pub dynamic_deep_queue_decay_min: f64,
    pub dynamic_deep_queue_decay_max: f64,
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
            next_queue_seq: 0,
            order_queue_position_strength: 0.0,
            own_queue_positioned_orders: 0,
            own_queue_initial_qty: 0.0,
            own_queue_cancel_advances_n: 0,
            own_queue_cancel_advance_qty: 0.0,
            fees: HashMap::new(),
            tick: HashMap::new(),
            split_by_iid,
            seeded_conditions: HashSet::new(),
            fold_outcomes: false,
            fold_to: HashMap::new(),
            event_slug_by_token: HashMap::new(),
            fill_audit: BTreeMap::new(),
            maker_order_audit_enabled: false,
            maker_order_audit: BTreeMap::new(),
            audit_clock_ns: 0,
            fold_sibling: HashMap::new(),
            last_book_ts: HashMap::new(),
            last_book_local_ts: HashMap::new(),
            book_stale_after_ns: 0,
            stale_resting_exchange_only: false,
            book_stale_order_blocks: 0,
            book_stale_trade_blocks: 0,
            book_stale_exchange_hits: 0,
            book_stale_local_hits: 0,
            book_stale_rebases: 0,
            ahead_frac_override: None,
            dynamic_ahead_frac_strength: 0.0,
            dynamic_ahead_frac_n: 0,
            dynamic_ahead_frac_sum: 0.0,
            dynamic_ahead_frac_min: f64::INFINITY,
            dynamic_ahead_frac_max: f64::NEG_INFINITY,
            adverse_sel_rate: 0.0,
            adverse_scale_ticks: 1.0,
            adverse_advanced: 0,
            maker_toxicity_strength: 0.0,
            maker_toxicity_scale_ticks: 1.0,
            maker_toxicity_suppressed_n: 0,
            maker_toxicity_suppressed_qty: 0.0,
            book_through_rate: 0.0,
            book_through_fills_n: 0,
            unexplained_depletion_exec_rate: 0.0,
            unexplained_depletion_fills_n: 0,
            inferred_maker_residual_rate: 0.0,
            inferred_maker_residual_fraction: 0.0006,
            inferred_maker_residual_orders_n: 0,
            inferred_maker_residual_qty: 0.0,
            replay_self_depth_rate: 0.0,
            replay_self_taker_depth_rate: 0.0,
            taker_replay_self_sweeps_n: 0,
            taker_replay_self_depth_qty: 0.0,
            fill_markout_vn: 0.0,
            fill_haircut_n: 0,
            book_fill_markout_vn: 0.0,
            book_fill_haircut_n: 0,
            book_fill_haircut_qty: 0.0,
            book_fill_haircut_cost_usdc: 0.0,
            pend_cross: HashMap::new(),
            maker_race_rate: 0.0,
            taker_race_rate: 0.0,
            recent_trades: HashMap::new(),
            taker_comp_rate: 0.0,
            taker_comp_window_ns: 0,
            taker_overlap_dedup: false,
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
            dynamic_deep_queue_n: 0,
            dynamic_deep_queue_decay_sum: 0.0,
            dynamic_deep_queue_decay_min: f64::INFINITY,
            dynamic_deep_queue_decay_max: f64::NEG_INFINITY,
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
        liquidity: Liquidity,
        ts: u64,
    ) {
        let e = self.recent_fills.entry(coid.to_string()).or_insert(RecentFill {
            ts,
            trade_id: trade_id.clone(),
            filled_quantity: add_qty,
            price,
            side,
            symbol: symbol.to_string(),
            liquidity,
        });
        e.ts = ts;
        e.trade_id = trade_id;
        // `trade_id` identifies one immutable execution.  If an order fills in
        // multiple fragments, the newest id replaces the previous one, so its
        // quantity must be replaced as well.  Summing quantities while
        // replacing the id makes a matched-can't-cancel replay mutate the
        // trade tuple and defeats downstream idempotence checks.
        e.filled_quantity = add_qty;
        e.price = price;
        e.liquidity = liquidity;
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

    #[inline]
    fn same_queue_level(
        o: &RestingOrder,
        match_symbol: &str,
        match_side: Side,
        match_price: f64,
        tick: f64,
    ) -> bool {
        o.match_symbol == match_symbol
            && o.match_side == match_side
            && o.tick.to_bits() == tick.to_bits()
            && price_to_ticks(o.match_price, tick) == price_to_ticks(match_price, tick)
    }

    /// Apply v2 model knobs from config (ahead_frac override, matched-can't-
    /// cancel window). `ahead_frac=None` keeps the default proportional model.
    pub fn configure(&mut self, ahead_frac: Option<f64>, matched_cant_cancel_window_ns: u64) {
        self.ahead_frac_override = ahead_frac.map(|f| f.clamp(0.0, 1.0));
        if matched_cant_cancel_window_ns > 0 {
            self.matched_cant_cancel_window_ns = matched_cant_cancel_window_ns;
        }
    }

    pub fn configure_dynamic_ahead_frac(&mut self, strength: f64) {
        self.dynamic_ahead_frac_strength = strength.clamp(0.0, 1.0);
    }

    /// Include earlier simulated orders at the same canonical price level in
    /// each later order's FIFO queue position. The core is a single writer, so
    /// sequence assignment and cancellation advancement require no shared
    /// mutable state or cross-thread synchronization.
    pub fn configure_order_queue_position(&mut self, strength: f64) {
        self.order_queue_position_strength = strength.clamp(0.0, 1.0);
    }

    fn audit_key(&self, token: &str, iid: &str) -> Option<(String, String)> {
        let slug = self
            .event_slug_by_token
            .get(token)
            .or_else(|| self.event_slug_by_token.get(self.canonical_of(token)))?
            .clone();
        Some((slug, iid.to_string()))
    }

    fn audit_row_mut(&mut self, token: &str, iid: &str) -> Option<&mut FillAuditRow> {
        let key = self.audit_key(token, iid)?;
        Some(self.fill_audit.entry(key.clone()).or_insert_with(|| FillAuditRow {
            slug: key.0,
            iid: key.1,
            ..FillAuditRow::default()
        }))
    }

    pub fn fill_audit_rows(&self) -> Vec<FillAuditRow> {
        let mut rows = self.fill_audit.clone();
        for order in self.maker_order_audit.values() {
            let key = (order.slug.clone(), order.iid.clone());
            let row = rows.entry(key.clone()).or_insert_with(|| FillAuditRow {
                slug: key.0,
                iid: key.1,
                ..FillAuditRow::default()
            });
            let mut snapshot = order.clone();
            if snapshot.cancel_result == "open" {
                let remaining = snapshot.remaining_final;
                accrue_order_exposure(&mut snapshot, self.audit_clock_ns, remaining);
                row.maker_open_orders += 1;
                if snapshot.post_only {
                    row.passive_open_orders += 1;
                }
            } else if matches!(snapshot.cancel_result, "cancelled" | "event_retired") {
                row.maker_cancel_orders += 1;
                row.maker_cancel_qty += snapshot.remaining_final.max(0.0);
                if snapshot.post_only {
                    row.passive_cancel_orders += 1;
                    row.passive_cancel_qty += snapshot.remaining_final.max(0.0);
                }
            }
            if snapshot.fill_qty > EPS {
                row.maker_orders_with_fill += 1;
                if snapshot.post_only {
                    row.passive_orders_with_fill += 1;
                    row.passive_fill_qty += snapshot.fill_qty;
                }
            }
            row.maker_rest_time_ns = row
                .maker_rest_time_ns
                .saturating_add(snapshot.rest_time_ns as u128);
            row.maker_rest_qty_ns += snapshot.rest_qty_ns;
            if snapshot.post_only {
                row.passive_rest_time_ns = row
                    .passive_rest_time_ns
                    .saturating_add(snapshot.rest_time_ns as u128);
                row.passive_rest_qty_ns += snapshot.rest_qty_ns;
            }
        }
        rows.into_values().collect()
    }

    pub fn configure_maker_order_audit(&mut self, enabled: bool) {
        self.maker_order_audit_enabled = enabled;
        if !enabled {
            self.maker_order_audit.clear();
        }
    }

    pub fn maker_order_audit_rows(&self) -> Vec<MakerOrderAuditRow> {
        self.maker_order_audit
            .values()
            .cloned()
            .map(|mut row| {
                if row.cancel_result == "open" {
                    let remaining = row.remaining_final;
                    accrue_order_exposure(&mut row, self.audit_clock_ns, remaining);
                }
                row
            })
            .collect()
    }

    /// Record when a matched maker fill becomes visible on the simulated
    /// strategy's private-event lane. Matching owns `first_fill_ns`; the
    /// simulator owns this delivery timestamp after sampling private latency.
    pub fn record_fill_delivery(&mut self, coid: &str, delivery_ns: u64) {
        if let Some(a) = self.maker_order_audit.get_mut(coid) {
            if a.first_fill_delivery_ns == 0 || delivery_ns < a.first_fill_delivery_ns {
                a.first_fill_delivery_ns = delivery_ns;
            }
            a.last_fill_delivery_ns = a.last_fill_delivery_ns.max(delivery_ns);
        }
    }

    /// Configure the fail-closed full-book age gate. A single threshold is
    /// applied independently to the last local receive timestamp and the last
    /// exchange timestamp; either clock expiring makes the book unusable for a
    /// fill. 0 disables the gate byte-for-byte at the decision points.
    pub fn configure_book_stale_gate(&mut self, stale_after_ns: u64) {
        self.book_stale_after_ns = stale_after_ns;
    }

    pub fn configure_stale_resting_exchange_only(&mut self, enabled: bool) {
        self.stale_resting_exchange_only = enabled;
    }

    fn resting_trade_is_stale(&self, exchange_stale: bool, local_stale: bool) -> bool {
        exchange_stale || (!self.stale_resting_exchange_only && local_stale)
    }

    fn book_stale_reasons(&self, token: &str, now_ns: u64) -> (bool, bool) {
        let max_age = self.book_stale_after_ns;
        if max_age == 0 {
            return (false, false);
        }
        let canon = self.canonical_of(token);
        let exchange_stale = self
            .last_book_ts
            .get(canon)
            .is_none_or(|ts| now_ns.saturating_sub(*ts) > max_age);
        let local_stale = self
            .last_book_local_ts
            .get(canon)
            .is_none_or(|ts| now_ns.saturating_sub(*ts) > max_age);
        (exchange_stale, local_stale)
    }

    fn count_book_stale_block(&mut self, exchange_stale: bool, local_stale: bool, order: bool) {
        if order {
            self.book_stale_order_blocks += 1;
        } else {
            self.book_stale_trade_blocks += 1;
        }
        self.book_stale_exchange_hits += exchange_stale as u64;
        self.book_stale_local_hits += local_stale as u64;
    }

    /// Estimate how much maker volume the stale gate suppressed without
    /// mutating queue state. This mirrors only the price/side/queue predicate of
    /// `match_trade`; it is attribution, not a counterfactual fill injection.
    fn audit_stale_trade(&mut self, symbol: &str, side: Side, price: f64, qty: f64) {
        let Some(slug) = self.event_slug_by_token.get(symbol).cloned() else {
            return;
        };
        let tick = self.tick_of(symbol);
        let trade_ticks = price_to_ticks(price, tick);
        let audits = &mut self.fill_audit;
        for o in self.orders.values() {
            if o.match_symbol != symbol {
                continue;
            }
            let order_ticks = price_to_ticks(o.match_price, tick);
            let matches = match o.match_side {
                Side::Buy => side == Side::Sell && trade_ticks <= order_ticks,
                Side::Sell => side == Side::Buy && trade_ticks >= order_ticks,
            };
            if !matches {
                continue;
            }
            let candidate = (qty - o.q_ahead).max(0.0).min(o.remaining);
            let key = (slug.clone(), o.request.instance_id.clone());
            let a = audits.entry(key.clone()).or_insert_with(|| FillAuditRow {
                slug: key.0,
                iid: key.1,
                ..FillAuditRow::default()
            });
            a.stale_trade_matches += 1;
            a.stale_trade_candidate_qty += candidate;
        }
    }

    /// Orders admitted while stale join behind the complete visible queue on
    /// the first fresh full book. This avoids carrying a queue position derived
    /// from the stale cached ladder into future maker matching.
    fn rebase_stale_orders(&mut self, canon: &str) {
        let books = &self.books;
        let mut rebased = 0u64;
        for o in self.orders.values_mut() {
            if !o.await_fresh_book || o.match_symbol != canon {
                continue;
            }
            let depth = books.level_depth(&o.match_symbol, o.match_side, o.match_price, o.tick);
            o.replay_self_depth_credit = o
                .replay_self_depth_credit
                .min(depth)
                .min(o.remaining);
            o.q_ahead = (depth - o.replay_self_depth_credit).max(0.0) + o.own_q_ahead;
            o.level_qty_at_sync = depth;
            let mid = books.eff_mid(&o.match_symbol);
            o.mid_at_sync = mid;
            if o.entry_mid <= 0.0 && mid > 0.0 {
                o.entry_mid = mid;
            }
            o.traded_since_sync = 0.0;
            o.await_fresh_book = false;
            rebased += 1;
        }
        self.book_stale_rebases += rebased;
    }

    fn maybe_rebase_stale_orders(&mut self, canon: &str, now_ns: u64) {
        let (exchange_stale, local_stale) = self.book_stale_reasons(canon, now_ns);
        if !exchange_stale && !local_stale {
            self.rebase_stale_orders(canon);
        }
    }

    /// Advance the independent strategy-visible full-book clock. Server books
    /// are applied at exchange time, which can precede `local_timestamp_ns`;
    /// updating this clock from `on_orderbook` would therefore leak a future
    /// local receipt into matching decisions. The engine calls this only when
    /// the same snapshot actually reaches the local/strategy lane.
    pub fn on_local_orderbook(&mut self, ob: &OrderBookSnapshot, observed_at_ns: u64) {
        if ob.exchange != Exchange::Polymarket {
            return;
        }
        let canon = self.canonical_of(&ob.symbol).to_string();
        self.last_book_local_ts
            .entry(canon.clone())
            .and_modify(|ts| *ts = (*ts).max(observed_at_ns))
            .or_insert(observed_at_ns);
        self.maybe_rebase_stale_orders(&canon, observed_at_ns);
    }

    /// Configure adverse-selection conditioning of the cancel attribution.
    /// `rate=0` disables it (pure proportional/override ahead_frac). `scale_ticks`
    /// is the adverse mid-move (ticks) mapping to full conditioning; clamped to a
    /// small positive floor so the gain stays finite.
    pub fn configure_adverse_sel(&mut self, rate: f64, scale_ticks: f64) {
        self.adverse_sel_rate = rate.max(0.0);
        self.adverse_scale_ticks = scale_ticks.max(1e-6);
    }

    /// Configure causal maker fill selection at the actual resting limit.
    /// Favorable movement since entry suppresses a bounded fraction of an
    /// otherwise eligible trade overflow; adverse/no movement remains fully
    /// fillable. No future book state is read.
    pub fn configure_maker_toxicity(&mut self, strength: f64, scale_ticks: f64) {
        self.maker_toxicity_strength = strength.clamp(0.0, 1.0);
        self.maker_toxicity_scale_ticks = scale_ticks.max(1e-6);
    }

    /// Configure the book-through adverse fill rate (latency-race fraction in
    /// [0,1]; 0 = off). See the `book_through_rate` field.
    pub fn configure_book_through(&mut self, rate: f64) {
        self.book_through_rate = rate.clamp(0.0, 1.0);
    }

    /// Configure the maximum adverse unexplained-depletion execution fraction
    /// in [0,1].
    /// At zero, book shrinkage remains cancel-only and follows the exact legacy
    /// arithmetic path. At a positive value, only causally available residual
    /// shrinkage can drain the queue and its gated execution component caps the
    /// fill.
    pub fn configure_unexplained_depletion_execution(&mut self, rate: f64) {
        self.unexplained_depletion_exec_rate = rate.clamp(0.0, 1.0);
    }

    /// Model the exchange/live-order split exposed by near-complete maker
    /// matches: the strategy order manager releases at 99% cumulative coverage,
    /// while a tiny residual can remain physically executable on the CLOB.
    /// Selection is stable per client order id, so replay output is deterministic.
    pub fn configure_inferred_maker_residual(&mut self, rate: f64, fraction: f64) {
        self.inferred_maker_residual_rate = rate.clamp(0.0, 1.0);
        self.inferred_maker_residual_fraction = fraction.clamp(0.0, 0.01);
    }

    /// Configure approximate leave-one-out queue credit in [0,1]. This must be
    /// enabled only for a tape recorded while the replayed strategy was live.
    pub fn configure_replay_self_depth(&mut self, rate: f64) {
        self.replay_self_depth_rate = rate.clamp(0.0, 1.0);
    }

    /// Configure exact-level leave-one-out cleaning for taker sweeps. Enable
    /// only when the replay tape contains this strategy instance's live quotes.
    pub fn configure_replay_self_taker_depth(&mut self, rate: f64) {
        self.replay_self_taker_depth_rate = rate.clamp(0.0, 1.0);
    }

    /// Configure the VOLUME-NEUTRAL forward-markout adverse-reprice strength `vn`
    /// (favorable fills settle at limit ± vn·markout, full quantity kept; 0 = off).
    /// See `fill_markout_vn`.
    pub fn configure_fill_markout_vn(&mut self, vn: f64) {
        self.fill_markout_vn = vn.max(0.0);
    }

    /// Configure the independent VOLUME-NEUTRAL markout strength for maker
    /// fills inferred from queue depletion and trade-confirmed book-through.
    pub fn configure_book_fill_markout_vn(&mut self, vn: f64) {
        self.book_fill_markout_vn = vn.max(0.0);
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

    pub fn set_dynamic_deep_queue(&mut self, strength: f64, min_decay: f64) {
        self.books.set_dynamic_deep_queue(strength, min_decay);
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

    pub fn configure_taker_overlap_dedup(&mut self, on: bool) {
        self.taker_overlap_dedup = on;
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
    /// Apply a book snapshot. Returns any maker fills caused by causally
    /// available book depletion or a trade-confirmed book-through sweep.
    pub fn on_orderbook(&mut self, ob: &OrderBookSnapshot) -> Vec<OrderUpdate> {
        self.on_orderbook_inner(ob, None)
    }

    /// Apply a book snapshot with the canonical forward mid at `t+h`. The
    /// future value is used only by the opt-in volume-neutral book-fill
    /// markout; matching eligibility and fill quantity remain causal.
    pub fn on_orderbook_fwd(
        &mut self,
        ob: &OrderBookSnapshot,
        fwd_mid: Option<f64>,
    ) -> Vec<OrderUpdate> {
        self.on_orderbook_inner(ob, fwd_mid)
    }

    fn on_orderbook_inner(
        &mut self,
        ob: &OrderBookSnapshot,
        fwd_mid: Option<f64>,
    ) -> Vec<OrderUpdate> {
        let now_ns = ob.exchange_timestamp_ns;
        self.audit_clock_ns = self.audit_clock_ns.max(now_ns);
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
            self.maybe_rebase_stale_orders(&canon, now_ns);
            let mut depletion_fills = self.resync_queues(now_ns, &canon, fwd_mid);
            let book_through_fills =
                self.run_book_through_if_fresh(&canon, now_ns, fwd_mid);
            if depletion_fills.is_empty() {
                return book_through_fills;
            }
            depletion_fills.extend(book_through_fills);
            return depletion_fills;
        }
        self.books.update(&ob.symbol, ob.bids.clone(), ob.asks.clone());
        self.last_book_ts
            .entry(ob.symbol.clone())
            .and_modify(|ts| *ts = (*ts).max(ob.exchange_timestamp_ns))
            .or_insert(ob.exchange_timestamp_ns);
        self.maybe_rebase_stale_orders(&ob.symbol, now_ns);
        let mut depletion_fills = self.resync_queues(now_ns, &ob.symbol, fwd_mid);
        let book_through_fills =
            self.run_book_through_if_fresh(&ob.symbol, now_ns, fwd_mid);
        if depletion_fills.is_empty() {
            return book_through_fills;
        }
        depletion_fills.extend(book_through_fills);
        depletion_fills
    }

    fn run_book_through_if_fresh(
        &mut self,
        token: &str,
        now_ns: u64,
        fwd_mid: Option<f64>,
    ) -> Vec<OrderUpdate> {
        let (exchange_stale, local_stale) = self.book_stale_reasons(token, now_ns);
        if exchange_stale || local_stale {
            // A trade-confirmation belongs only to the immediately following
            // book interval. Do not let a confirmation suppressed by the stale
            // gate leak forward and fill on a later fresh book.
            self.pend_cross.clear();
            Vec::new()
        } else {
            self.run_book_through(now_ns, fwd_mid)
        }
    }

    /// Book-through adverse fills: a resting order whose price the contra side
    /// just swept STRICTLY through is marketable — the stale maker, lingering
    /// while faster makers cancelled (the price moved via repricing, not a trade
    /// — verified 99.9% of crosses), gets picked off. Fill `rate·(through_vol −
    /// q_ahead)` at the order's limit (adverse: mid is now through it). No-op
    /// when `book_through_rate == 0`.
    fn run_book_through(&mut self, now_ns: u64, fwd_mid: Option<f64>) -> Vec<OrderUpdate> {
        let rate = self.book_through_rate;
        if rate <= 0.0 {
            return Vec::new();
        }
        let markout_vn = self.book_fill_markout_vn;
        let audit_enabled = self.maker_order_audit_enabled;
        let mut mfills: Vec<MakerFill> = Vec::new();
        let mut haircut_n = 0u64;
        let mut haircut_qty = 0.0;
        let mut haircut_cost = 0.0;
        let mut residual_orders_n = 0u64;
        let mut residual_qty = 0.0;
        {
            let books = &self.books;
            let pend = &self.pend_cross;
            let slugs = &self.event_slug_by_token;
            let audits = &mut self.fill_audit;
            let order_audits = &mut self.maker_order_audit;
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
                let uncapped_fill = fillable.min(o.remaining);
                let inferred_capacity =
                    (o.remaining - o.inferred_residual_floor).max(0.0);
                let fill = uncapped_fill.min(inferred_capacity);
                let residual_suppressed = (uncapped_fill - fill).max(0.0);
                if fill <= EPS {
                    continue;
                }
                if residual_suppressed > EPS {
                    o.inferred_residual_realized = true;
                    residual_orders_n += 1;
                    residual_qty += residual_suppressed;
                }
                // The sweep consumes the queue ahead of us then takes our fill.
                o.q_ahead = (o.q_ahead - through).max(0.0);
                o.own_q_ahead = o.own_q_ahead.min(o.q_ahead);
                o.remaining -= fill;
                let limit = o.request.price.unwrap_or(0.0);
                let (effective_price, price_penalty) = maker_markout_reprice(
                    limit,
                    o.request.side,
                    o.match_side,
                    o.match_price,
                    fwd_mid,
                    markout_vn,
                );
                let fill_cost = price_penalty * fill;
                if o.request.side == Side::Buy {
                    o.locked_usdc = limit * o.remaining;
                }
                if let Some(a) = order_audits.get_mut(coid) {
                    accrue_order_exposure(a, now_ns, o.remaining + fill);
                    a.book_through_candidate_qty += fillable.min(o.remaining + fill);
                    a.book_through_fill_qty += fill;
                    a.inferred_residual_suppressed_qty += residual_suppressed;
                    if price_penalty > 0.0 {
                        a.book_markout_qty += fill;
                        a.book_markout_cost_usdc += fill_cost;
                    }
                    a.q_ahead_final = o.q_ahead;
                    a.remaining_final = o.remaining;
                }
                if audit_enabled && price_penalty > 0.0 {
                    if let Some(slug) = slugs.get(&o.request.symbol) {
                        let key = (slug.clone(), o.request.instance_id.clone());
                        if let Some(a) = audits.get_mut(&key) {
                            a.maker_book_markout_qty += fill;
                            a.maker_book_markout_cost_usdc += fill_cost;
                        }
                    }
                }
                if price_penalty > 0.0 {
                    haircut_n += 1;
                    haircut_qty += fill;
                    haircut_cost += fill_cost;
                }
                n += 1;
                mfills.push(MakerFill {
                    coid: coid.clone(),
                    token: o.request.symbol.clone(),
                    side: o.request.side,
                    iid: o.request.instance_id.clone(),
                    fill,
                    price: effective_price,
                    remaining_after: o.remaining,
                    fully: o.remaining <= EPS,
                    queue_seq: o.queue_seq,
                });
            }
            self.book_through_fills_n += n;
        }
        self.book_fill_haircut_n += haircut_n;
        self.book_fill_haircut_qty += haircut_qty;
        self.book_fill_haircut_cost_usdc += haircut_cost;
        self.inferred_maker_residual_orders_n += residual_orders_n;
        self.inferred_maker_residual_qty += residual_qty;
        // Reset the trade-gate window for the next book interval.
        self.pend_cross.clear();
        if mfills.is_empty() {
            return Vec::new();
        }
        self.apply_maker_fills(mfills, now_ns)
    }

    /// Depletion attribution (§5): level shrinkage not explained by public trades
    /// since the last snapshot is split into hidden execution and cancellation.
    /// Hidden execution drains the queue before it can fill us; only the fraction
    /// of cancellations represented by `ahead_frac` advances our queue. (The maker
    /// race is NOT applied here — it fires once, at the order's entry-match moment;
    /// see `insert_resting`.)
    fn resync_queues(
        &mut self,
        now_ns: u64,
        changed_token: &str,
        fwd_mid: Option<f64>,
    ) -> Vec<OrderUpdate> {
        let books = &self.books;
        let af_override = self.ahead_frac_override;
        let dynamic_af_strength = self.dynamic_ahead_frac_strength;
        let adv_rate = self.adverse_sel_rate;
        let adv_scale = self.adverse_scale_ticks;
        let depletion_rate = self.unexplained_depletion_exec_rate;
        let markout_vn = self.book_fill_markout_vn;
        let own_fifo_strength = self.order_queue_position_strength;
        let audit_enabled = self.maker_order_audit_enabled;
        let mut advanced = 0u64;
        let mut dynamic_n = 0u64;
        let mut dynamic_sum = 0.0;
        let mut dynamic_min = f64::INFINITY;
        let mut dynamic_max = f64::NEG_INFINITY;
        let slugs = &self.event_slug_by_token;
        let audits = &mut self.fill_audit;
        let order_audits = &mut self.maker_order_audit;
        let mut candidates: Vec<DepletionCandidate> = Vec::new();
        let mut haircut_n = 0u64;
        let mut haircut_qty = 0.0;
        let mut haircut_cost = 0.0;
        let mut residual_orders_n = 0u64;
        let mut residual_qty = 0.0;
        for (coid, o) in self.orders.iter_mut() {
            // Queue depth tracked in the canonical matching frame.
            let l_now = books.level_depth(&o.match_symbol, o.match_side, o.match_price, o.tick);
            let l_prev = o.level_qty_at_sync;
            let unexplained = (l_prev - o.traded_since_sync - l_now).max(0.0);
            // Baseline (neutral) ahead-fraction: pinned override or proportional.
            let proportional = if l_prev > EPS {
                (o.q_ahead / l_prev).clamp(0.0, 1.0)
            } else {
                0.0
            };
            let fixed = af_override.unwrap_or(proportional).clamp(0.0, 1.0);
            let base = (fixed + dynamic_af_strength * (proportional - fixed)).clamp(0.0, 1.0);
            if dynamic_af_strength > 0.0 && unexplained > EPS {
                dynamic_n += 1;
                dynamic_sum += base;
                dynamic_min = dynamic_min.min(base);
                dynamic_max = dynamic_max.max(base);
            }
            // Adverse-selection tilt: cancellations are informed. The canonical
            // mid move since the last sync, signed AGAINST the order, says whether
            // the level's cancels were informed (adverse → front makers pull →
            // ahead_frac→1 → advance, fill toxic) or noise (favorable → →0 →
            // hold, miss). `s∈[-1,1]`; at s=0 (no move / rate=0) ahead_frac=base,
            // exactly the prior model. The fill's adverse cost is realised later
            // at settlement (down move ⇒ a filled `up` bid loses).
            let mid_now = books.eff_mid(&o.match_symbol);
            let adverse = if (adv_rate > 0.0 || depletion_rate > 0.0)
                && mid_now > 0.0
                && o.mid_at_sync > 0.0
            {
                let raw = mid_now - o.mid_at_sync; // + = canonical(up) mid rose
                match o.match_side { Side::Buy => -raw, Side::Sell => raw }
            } else {
                0.0
            };
            let ahead_frac = if adv_rate > 0.0 {
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
            let q_before = o.q_ahead;
            // A zero rate deliberately takes the original cancel-only
            // arithmetic path. This makes the feature result-neutral when it
            // is absent from config or explicitly disabled.
            let execution = if depletion_rate > 0.0 && o.match_symbol == changed_token {
                let adverse_gate = (adverse / (adv_scale * o.tick)).clamp(0.0, 1.0);
                unexplained * depletion_rate * adverse_gate
            } else {
                0.0
            };
            let cancels = unexplained - execution;
            let raw_cancel_advance = cancels * ahead_frac;
            // Public cancellations can remove only the public portion of our
            // queue. Earlier simulated orders remain ahead until they fill or
            // cancel explicitly; treating public L2 shrinkage as their cancel
            // would reintroduce the same multi-order volume duplication FIFO is
            // meant to prevent. The disabled path keeps legacy arithmetic.
            let cancel_advance = if own_fifo_strength > 0.0 && o.own_q_ahead > EPS {
                raw_cancel_advance.min((q_before - o.own_q_ahead).max(0.0))
            } else {
                raw_cancel_advance
            };
            let total_advance = cancel_advance + execution;
            o.q_ahead = (q_before - total_advance).max(0.0);
            o.own_q_ahead = o.own_q_ahead.min(o.q_ahead);

            let candidate = if execution > EPS {
                (total_advance - q_before)
                    .max(0.0)
                    .min(execution)
                    .min(o.remaining)
            } else {
                0.0
            };
            if audit_enabled {
                if let Some(a) = order_audits.get_mut(coid) {
                    a.depletion_observed_qty += unexplained;
                    a.depletion_exec_qty += execution;
                    a.depletion_cancel_advance_qty += cancel_advance;
                    a.depletion_candidate_qty += candidate;
                    a.q_ahead_final = o.q_ahead;
                }
                if let Some(slug) = slugs.get(&o.request.symbol) {
                    let key = (slug.clone(), o.request.instance_id.clone());
                    let a = audits.entry(key.clone()).or_insert_with(|| FillAuditRow {
                        slug: key.0,
                        iid: key.1,
                        ..FillAuditRow::default()
                    });
                    a.maker_depletion_observed_qty += unexplained;
                    a.maker_depletion_exec_qty += execution;
                    a.maker_depletion_cancel_advance_qty += cancel_advance;
                    a.maker_depletion_candidate_qty += candidate;
                }
            }
            if candidate > EPS {
                candidates.push(DepletionCandidate {
                    match_symbol: o.match_symbol.clone(),
                    match_side: o.match_side,
                    match_price: o.match_price,
                    match_side_rank: match o.match_side {
                        Side::Buy => 0,
                        Side::Sell => 1,
                    },
                    match_price_tick: (o.match_price / o.tick).round() as i64,
                    match_tick_bits: o.tick.to_bits(),
                    placed_ns: o.placed_ns,
                    coid: coid.clone(),
                    token: o.request.symbol.clone(),
                    side: o.request.side,
                    iid: o.request.instance_id.clone(),
                    price: o.request.price.unwrap_or(0.0),
                    potential: candidate,
                    level_execution: execution,
                });
            }
            // Own depth cannot survive after it disappears from the replayed
            // level. Capping here prevents a later, unrelated level re-entry
            // from receiving the same leave-one-out credit a second time.
            o.replay_self_depth_credit = o
                .replay_self_depth_credit
                .min(l_now)
                .min(o.remaining);
            o.level_qty_at_sync = l_now;
            if mid_now > 0.0 {
                o.mid_at_sync = mid_now;
            }
            o.traded_since_sync = 0.0;
        }

        // One public level depletion supplies one execution budget even when
        // several of our orders observe it. Allocate only the fill overflow in
        // exchange-arrival FIFO order; queue advancement above was correctly
        // applied to every queue position. The max (rather than sum) handles
        // orders whose sync anchors differ without manufacturing volume.
        candidates.sort_by(|a, b| {
            a.match_symbol
                .cmp(&b.match_symbol)
                .then(a.match_side_rank.cmp(&b.match_side_rank))
                .then(a.match_tick_bits.cmp(&b.match_tick_bits))
                .then(a.match_price_tick.cmp(&b.match_price_tick))
                .then(a.placed_ns.cmp(&b.placed_ns))
                .then(a.coid.cmp(&b.coid))
        });
        let same_level = |a: &DepletionCandidate, b: &DepletionCandidate| {
            a.match_symbol == b.match_symbol
                && a.match_side_rank == b.match_side_rank
                && a.match_tick_bits == b.match_tick_bits
                && a.match_price_tick == b.match_price_tick
        };
        let mut mfills: Vec<MakerFill> = Vec::new();
        let mut begin = 0usize;
        while begin < candidates.len() {
            let mut end = begin + 1;
            while end < candidates.len() && same_level(&candidates[begin], &candidates[end]) {
                end += 1;
            }
            let mut budget = candidates[begin..end]
                .iter()
                .map(|candidate| candidate.level_execution)
                .fold(0.0, f64::max);
            for candidate in &candidates[begin..end] {
                let actual = candidate.potential.min(budget);
                budget = (budget - actual).max(0.0);
                let suppressed = (candidate.potential - actual).max(0.0);
                if audit_enabled && suppressed > EPS {
                    if let Some(a) = order_audits.get_mut(&candidate.coid) {
                        a.depletion_budget_suppressed_qty += suppressed;
                    }
                    if let Some(slug) = slugs.get(&candidate.token) {
                        let key = (slug.clone(), candidate.iid.clone());
                        if let Some(a) = audits.get_mut(&key) {
                            a.maker_depletion_budget_suppressed_qty += suppressed;
                        }
                    }
                }
                if actual <= EPS {
                    continue;
                }
                let Some(o) = self.orders.get_mut(&candidate.coid) else {
                    continue;
                };
                let uncapped_fill = actual.min(o.remaining);
                let inferred_capacity =
                    (o.remaining - o.inferred_residual_floor).max(0.0);
                let fill = uncapped_fill.min(inferred_capacity);
                let residual_suppressed = (uncapped_fill - fill).max(0.0);
                if fill <= EPS {
                    continue;
                }
                if residual_suppressed > EPS {
                    o.inferred_residual_realized = true;
                    residual_orders_n += 1;
                    residual_qty += residual_suppressed;
                }
                o.remaining -= fill;
                let (effective_price, price_penalty) = maker_markout_reprice(
                    candidate.price,
                    candidate.side,
                    candidate.match_side,
                    candidate.match_price,
                    fwd_mid,
                    markout_vn,
                );
                let fill_cost = price_penalty * fill;
                if o.request.side == Side::Buy {
                    o.locked_usdc = candidate.price * o.remaining;
                }
                if audit_enabled {
                    if let Some(a) = order_audits.get_mut(&candidate.coid) {
                        accrue_order_exposure(a, now_ns, o.remaining + fill);
                        a.depletion_fill_qty += fill;
                        a.inferred_residual_suppressed_qty += residual_suppressed;
                        if price_penalty > 0.0 {
                            a.book_markout_qty += fill;
                            a.book_markout_cost_usdc += fill_cost;
                        }
                        a.remaining_final = o.remaining;
                    }
                    if let Some(slug) = slugs.get(&candidate.token) {
                        let key = (slug.clone(), candidate.iid.clone());
                        if let Some(a) = audits.get_mut(&key) {
                            a.maker_depletion_fill_qty += fill;
                            if price_penalty > 0.0 {
                                a.maker_book_markout_qty += fill;
                                a.maker_book_markout_cost_usdc += fill_cost;
                            }
                        }
                    }
                }
                if price_penalty > 0.0 {
                    haircut_n += 1;
                    haircut_qty += fill;
                    haircut_cost += fill_cost;
                }
                mfills.push(MakerFill {
                    coid: candidate.coid.clone(),
                    token: candidate.token.clone(),
                    side: candidate.side,
                    iid: candidate.iid.clone(),
                    fill,
                    price: effective_price,
                    remaining_after: o.remaining,
                    fully: o.remaining <= EPS,
                    queue_seq: o.queue_seq,
                });
            }
            begin = end;
        }
        self.adverse_advanced += advanced;
        self.dynamic_ahead_frac_n += dynamic_n;
        self.dynamic_ahead_frac_sum += dynamic_sum;
        self.dynamic_ahead_frac_min = self.dynamic_ahead_frac_min.min(dynamic_min);
        self.dynamic_ahead_frac_max = self.dynamic_ahead_frac_max.max(dynamic_max);
        self.unexplained_depletion_fills_n += mfills.len() as u64;
        self.book_fill_haircut_n += haircut_n;
        self.book_fill_haircut_qty += haircut_qty;
        self.book_fill_haircut_cost_usdc += haircut_cost;
        self.inferred_maker_residual_orders_n += residual_orders_n;
        self.inferred_maker_residual_qty += residual_qty;
        if mfills.is_empty() {
            Vec::new()
        } else {
            self.apply_maker_fills(mfills, now_ns)
        }
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
        self.audit_clock_ns = self.audit_clock_ns.max(ts);
        if self.fold_outcomes {
            // Fold the trade onto the canonical frame and drain the single
            // canonical queue once (a down trade mirrors: flip side, 1−price).
            let canon = self.canonical_of(&t.symbol).to_string();
            let (exchange_stale, local_stale) = self.book_stale_reasons(&canon, ts);
            if canon == t.symbol {
                self.record_trade(&canon, t.side, t.price, t.quantity, ts);
                if self.resting_trade_is_stale(exchange_stale, local_stale) {
                    self.audit_stale_trade(&canon, t.side, t.price, t.quantity);
                    self.count_book_stale_block(exchange_stale, local_stale, false);
                } else {
                    self.match_trade(&canon, t.side, t.price, t.quantity, ts, fwd_mid, &mut fills);
                }
            } else {
                self.record_trade(&canon, flip(t.side), 1.0 - t.price, t.quantity, ts);
                if self.resting_trade_is_stale(exchange_stale, local_stale) {
                    self.audit_stale_trade(&canon, flip(t.side), 1.0 - t.price, t.quantity);
                    self.count_book_stale_block(exchange_stale, local_stale, false);
                } else {
                    self.match_trade(
                        &canon,
                        flip(t.side),
                        1.0 - t.price,
                        t.quantity,
                        ts,
                        fwd_mid,
                        &mut fills,
                    );
                }
            }
        } else {
            // Direct: aggressor side / price as recorded.
            self.record_trade(&t.symbol, t.side, t.price, t.quantity, ts);
            let (exchange_stale, local_stale) = self.book_stale_reasons(&t.symbol, ts);
            if self.resting_trade_is_stale(exchange_stale, local_stale) {
                self.audit_stale_trade(&t.symbol, t.side, t.price, t.quantity);
                self.count_book_stale_block(exchange_stale, local_stale, false);
            } else {
                self.match_trade(&t.symbol, t.side, t.price, t.quantity, ts, fwd_mid, &mut fills);
            }
            // Cross-outcome mirror: flip side, 1 − price on the complement token.
            if let Some(comp) = self.books.complement(&t.symbol).cloned() {
                self.record_trade(&comp, flip(t.side), 1.0 - t.price, t.quantity, ts);
                let (exchange_stale, local_stale) = self.book_stale_reasons(&comp, ts);
                if self.resting_trade_is_stale(exchange_stale, local_stale) {
                    self.audit_stale_trade(&comp, flip(t.side), 1.0 - t.price, t.quantity);
                    self.count_book_stale_block(exchange_stale, local_stale, false);
                } else {
                    self.match_trade(
                        &comp,
                        flip(t.side),
                        1.0 - t.price,
                        t.quantity,
                        ts,
                        fwd_mid.map(|m| 1.0 - m),
                        &mut fills,
                    );
                }
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
        now_ns: u64,
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
        let toxicity_strength = self.maker_toxicity_strength;
        let toxicity_scale_ticks = self.maker_toxicity_scale_ticks;
        let mid_now = if toxicity_strength > 0.0 {
            self.books.eff_mid(symbol)
        } else {
            0.0
        };
        let mut haircuts = 0u64;
        let mut toxicity_suppressed_n = 0u64;
        let mut toxicity_suppressed_qty = 0.0;
        let mut residual_orders_n = 0u64;
        let mut residual_qty = 0.0;
        let audit_slug = self.event_slug_by_token.get(symbol).cloned();
        let audits = &mut self.fill_audit;
        let order_audits = &mut self.maker_order_audit;
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
            let q_before = o.q_ahead;
            let over = qty - q_before;
            if let Some(a) = order_audits.get_mut(coid) {
                a.trade_match_n += 1;
                a.trade_match_qty += qty;
                a.queue_drained_qty += qty.min(q_before);
                a.candidate_qty += over.max(0.0).min(o.remaining);
                a.q_ahead_final = (q_before - qty).max(0.0);
                a.remaining_final = o.remaining;
                debug_assert!(now_ns >= a.place_arrival_ns);
            }
            if let Some(slug) = audit_slug.as_ref() {
                let key = (slug.clone(), o.request.instance_id.clone());
                let a = audits.entry(key.clone()).or_insert_with(|| FillAuditRow {
                    slug: key.0,
                    iid: key.1,
                    ..FillAuditRow::default()
                });
                a.maker_trade_matches += 1;
                a.maker_trade_qty += qty;
                a.maker_queue_drained_qty += qty.min(q_before);
                a.maker_candidate_qty += over.max(0.0).min(o.remaining);
            }
            o.q_ahead = (o.q_ahead - qty).max(0.0);
            o.own_q_ahead = o.own_q_ahead.min(o.q_ahead);
            o.traded_since_sync += qty;
            if over <= EPS {
                continue;
            }
            let candidate = over.min(o.remaining);
            if candidate <= EPS {
                continue;
            }
            // Causal maker selection at the real limit. A move in our favor
            // since exchange entry is exactly the regime where a public print
            // is least likely to have reached our individual queue position.
            // Suppress a bounded fraction and turn it into latent queue ahead;
            // adverse/no movement remains fully fillable. Only current book
            // state is used — no future markout and no execution repricing.
            let favorable_ticks = if toxicity_strength > 0.0
                && mid_now > 0.0
                && o.entry_mid > 0.0
                && o.tick > 0.0
            {
                let favorable_move = match o.match_side {
                    Side::Buy => mid_now - o.entry_mid,
                    Side::Sell => o.entry_mid - mid_now,
                };
                (favorable_move / o.tick).max(0.0)
            } else {
                0.0
            };
            let suppress_frac = if favorable_ticks > 0.0 {
                toxicity_strength * (favorable_ticks / toxicity_scale_ticks).clamp(0.0, 1.0)
            } else {
                0.0
            };
            let suppressed = candidate * suppress_frac;
            let uncapped_fill = (candidate - suppressed).max(0.0);
            let inferred_capacity =
                (o.remaining - o.inferred_residual_floor).max(0.0);
            let fill = uncapped_fill.min(inferred_capacity);
            let residual_suppressed = (uncapped_fill - fill).max(0.0);
            if residual_suppressed > EPS && !o.inferred_residual_realized {
                o.inferred_residual_realized = true;
                residual_orders_n += 1;
                residual_qty += residual_suppressed;
                if let Some(a) = order_audits.get_mut(coid) {
                    a.inferred_residual_suppressed_qty += residual_suppressed;
                }
            }
            if suppressed > EPS {
                o.q_ahead += suppressed;
                toxicity_suppressed_n += 1;
                toxicity_suppressed_qty += suppressed;
                if let Some(a) = order_audits.get_mut(coid) {
                    a.maker_toxicity_suppressed_qty += suppressed;
                    a.q_ahead_final = o.q_ahead;
                }
                if let Some(slug) = audit_slug.as_ref() {
                    let key = (slug.clone(), o.request.instance_id.clone());
                    if let Some(a) = audits.get_mut(&key) {
                        a.maker_toxicity_suppressed_qty += suppressed;
                    }
                }
            }
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
            let (eff_price, price_penalty) = maker_markout_reprice(
                limit,
                o.request.side,
                o.match_side,
                o.match_price,
                fwd_mid,
                vn,
            );
            if price_penalty > 0.0 {
                haircuts += 1;
            }
            o.remaining -= fill;
            // Resting remainder stays at the real limit (only the FILLED share is
            // repriced under vn); locked USDC tracks the limit.
            if o.request.side == Side::Buy {
                o.locked_usdc = limit * o.remaining;
            }
            if let Some(a) = order_audits.get_mut(coid) {
                accrue_order_exposure(a, now_ns, o.remaining + fill);
                a.q_ahead_final = o.q_ahead;
                a.remaining_final = o.remaining;
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
                queue_seq: o.queue_seq,
            });
        }
        self.fill_haircut_n += haircuts;
        self.maker_toxicity_suppressed_n += toxicity_suppressed_n;
        self.maker_toxicity_suppressed_qty += toxicity_suppressed_qty;
        self.inferred_maker_residual_orders_n += residual_orders_n;
        self.inferred_maker_residual_qty += residual_qty;
    }

    fn apply_maker_fills(&mut self, mut fills: Vec<MakerFill>, now_ns: u64) -> Vec<OrderUpdate> {
        if self.order_queue_position_strength > 0.0 {
            fills.sort_unstable_by_key(|fill| fill.queue_seq);
        }
        let mut out = Vec::with_capacity(fills.len());
        for f in fills {
            if let Some(order) = self.orders.get_mut(&f.coid) {
                order.replay_self_depth_credit = order
                    .replay_self_depth_credit
                    .min(f.remaining_after.max(0.0));
            }
            // Maker fills settle at our limit; Polymarket maker fee = 0.
            match f.side {
                Side::Buy => self.wallets.settle_buy(&f.iid, &f.token, f.fill, f.price * f.fill),
                Side::Sell => self.wallets.settle_sell(&f.iid, &f.token, f.fill, f.price * f.fill),
            }
            self.maker_fills += 1;
            if let Some(a) = self.audit_row_mut(&f.token, &f.iid) {
                a.maker_fill_qty += f.fill;
            }
            if let Some(a) = self.maker_order_audit.get_mut(&f.coid) {
                a.fill_qty += f.fill;
                if a.first_fill_ns == 0 {
                    a.first_fill_ns = now_ns;
                }
                a.last_fill_ns = now_ns;
                a.remaining_final = f.remaining_after;
                if f.fully {
                    a.cancel_result = "filled";
                    a.q_ahead_final = 0.0;
                }
            }
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
            self.record_recent_fill(
                &f.coid,
                trade_id.clone(),
                f.fill,
                f.price,
                f.side,
                &f.token,
                Liquidity::Maker,
                now_ns,
            );
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
                exchange_event_timestamp_ns: None,
                trade_id: Some(trade_id),
                order_audit: None,
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
                self.event_slug_by_token.insert(a.clone(), bo.slug.clone());
                self.event_slug_by_token.insert(b.clone(), bo.slug.clone());
                for iid in self.split_by_iid.keys() {
                    let key = (bo.slug.clone(), iid.clone());
                    self.fill_audit.entry(key.clone()).or_insert_with(|| FillAuditRow {
                        slug: key.0,
                        iid: key.1,
                        ..FillAuditRow::default()
                    });
                }
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
        // Residual orders for this dead event — finalize their exposure at the
        // current causal exchange clock before retiring the tokens. Leaving the
        // audit row "open" would otherwise snapshot it through the end of the
        // entire replay and attribute later events' wall time to this quote.
        let retired_coids: Vec<String> = self
            .orders
            .iter()
            .filter(|(_, o)| tokens.contains(&o.request.symbol) || tokens.contains(&o.match_symbol))
            .map(|(coid, _)| coid.clone())
            .collect();
        for coid in retired_coids {
            if let Some(order) = self.orders.remove(&coid) {
                if let Some(audit) = self.maker_order_audit.get_mut(&coid) {
                    accrue_order_exposure(audit, self.audit_clock_ns, order.remaining);
                    audit.cancel_result = "event_retired";
                    audit.remaining_final = order.remaining;
                }
            }
        }
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
            self.event_slug_by_token.remove(t);
            self.last_book_ts.remove(t);
            self.last_book_local_ts.remove(t);
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
            o.replay_self_depth_credit = o
                .replay_self_depth_credit
                .min(d)
                .min(o.remaining);
            o.q_ahead = o
                .q_ahead
                .min((d - o.replay_self_depth_credit).max(0.0));
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

    /// Quantity at one public taker level after removing the replay instance's
    /// currently-resting opposite-side simulated orders at that exact canonical
    /// level. This is a leave-one-out view of a tape captured while the same
    /// strategy was live; it never subtracts another instance's orders.
    fn replay_clean_taker_level_qty(
        &self,
        iid: &str,
        match_symbol: &str,
        taker_side: Side,
        price: f64,
        public_qty: f64,
        tick: f64,
    ) -> f64 {
        let rate = self.replay_self_taker_depth_rate;
        if rate <= 0.0 || public_qty <= EPS {
            return public_qty;
        }
        let maker_side = flip(taker_side);
        let price_tick = price_to_ticks(price, tick);
        let own_qty = self
            .orders
            .values()
            .filter(|resting| {
                resting.request.instance_id == iid
                    && resting.match_symbol == match_symbol
                    && resting.match_side == maker_side
                    && price_to_ticks(resting.match_price, tick) == price_tick
            })
            .map(|resting| resting.remaining)
            .sum::<f64>();
        (public_qty - rate * own_qty).max(0.0)
    }

    fn replay_clean_taker_available(
        &self,
        iid: &str,
        match_symbol: &str,
        taker_side: Side,
        ladder: &[PriceLevel],
        lim: Option<f64>,
    ) -> f64 {
        let tick = self.tick_of(match_symbol);
        ladder
            .iter()
            .take_while(|level| match (lim, taker_side) {
                (None, _) => true,
                (Some(p), Side::Buy) => level.price <= p + EPS,
                (Some(p), Side::Sell) => level.price >= p - EPS,
            })
            .map(|level| {
                self.replay_clean_taker_level_qty(
                    iid,
                    match_symbol,
                    taker_side,
                    level.price,
                    level.quantity,
                    tick,
                )
            })
            .sum()
    }

    fn replay_clean_best_taker_price(
        &self,
        iid: &str,
        match_symbol: &str,
        taker_side: Side,
        ladder: &[PriceLevel],
    ) -> Option<f64> {
        let tick = self.tick_of(match_symbol);
        ladder.iter().find_map(|level| {
            (self.replay_clean_taker_level_qty(
                iid,
                match_symbol,
                taker_side,
                level.price,
                level.quantity,
                tick,
            ) > EPS)
                .then_some(level.price)
        })
    }

    /// Would this order taker-fill against the *current* book if it arrived
    /// now? (marketable & not post-only). Used to decide whether to defer the
    /// match to the midpoint of the matching window. Post-only / non-marketable
    /// orders take the immediate rest/reject path in `submit_order`.
    pub fn would_cross(&self, o: &OrderRequest, now_ns: u64) -> bool {
        if o.post_only {
            return false;
        }
        let (exchange_stale, local_stale) = self.book_stale_reasons(&o.symbol, now_ns);
        if exchange_stale || local_stale {
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
        let best_opposing =
            self.replay_clean_best_taker_price(&o.instance_id, &msym, mside, &ladder);
        match (best_opposing, mside, lim) {
            (Some(bp), Side::Buy, Some(l)) => bp <= l + EPS,
            (Some(bp), Side::Sell, Some(l)) => bp >= l - EPS,
            (Some(_), _, None) => true,
            (None, _, _) => false,
        }
    }

    // ── order entry ──────────────────────────────────────────────
    /// Fillable quantity for a taker order against the currently observed
    /// canonical book, after replay self-depth removal. The simulator samples
    /// this while a marketable order is inside the matching-engine window so a
    /// causal race can retain the minimum liquidity seen before the match.
    pub fn taker_available_qty(&self, o: &OrderRequest) -> f64 {
        let (msym, mside, mprice) = self.match_view(o);
        let is_market = matches!(o.order_type, OrderType::Market) || o.price.is_none();
        let lim = if is_market { None } else { mprice };
        let ladder = match mside {
            Side::Buy => self.books.buy_ladder(&msym),
            Side::Sell => self.books.sell_ladder(&msym),
        };
        self.replay_clean_taker_available(&o.instance_id, &msym, mside, &ladder, lim)
    }

    pub fn taker_race_enabled(&self) -> bool {
        self.taker_race_rate > 0.0
    }

    pub fn submit_order(&mut self, o: &OrderRequest, now_ns: u64) -> OrderUpdate {
        self.submit_order_with_taker_race_cap(o, now_ns, None)
    }

    /// Submit with an optional one-order causal race observation. `cap` is the
    /// minimum fillable quantity seen from exchange arrival through the
    /// pre-match window. Passing it by value prevents a miss or cancel from
    /// leaking race state into a later order.
    pub fn submit_order_with_taker_race_cap(
        &mut self,
        o: &OrderRequest,
        now_ns: u64,
        causal_race_cap: Option<f64>,
    ) -> OrderUpdate {
        self.audit_clock_ns = self.audit_clock_ns.max(now_ns);
        if let Some(a) = self.audit_row_mut(&o.symbol, &o.instance_id) {
            a.place_orders += 1;
            a.place_qty += o.quantity;
            if o.post_only {
                a.passive_place_orders += 1;
                a.passive_place_qty += o.quantity;
            }
        }
        // Cancel-on-arrival: a cancel for this coid already arrived (it raced
        // ahead of this place ack). Honour the strategy's cancel intent now —
        // return Cancelled WITHOUT booking the order (no rest, no fill), so it
        // never becomes a forgotten orphan resting to settlement. See
        // `pending_cancels`.
        if self.pending_cancels.remove(&o.client_order_id).is_some() {
            if let Some(a) = self.audit_row_mut(&o.symbol, &o.instance_id) {
                a.cancel_before_place_orders += 1;
                a.cancel_before_place_qty += o.quantity;
            }
            return self.cancelled(o, now_ns, o.quantity);
        }
        if o.post_only {
            self.post_only_seen += 1;
        }
        let (exchange_stale, local_stale) = self.book_stale_reasons(&o.symbol, now_ns);
        if exchange_stale || local_stale {
            self.count_book_stale_block(exchange_stale, local_stale, true);
            if let Some(a) = self.audit_row_mut(&o.symbol, &o.instance_id) {
                a.stale_order_blocks += 1;
                a.stale_order_qty += o.quantity;
            }
            if matches!(o.order_type, OrderType::Market | OrderType::Fak | OrderType::Fok)
                || o.price.is_none()
            {
                return self.cancelled(o, now_ns, o.quantity);
            }
            return self.rest_stale(o, now_ns, o.quantity);
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
        let best_opposing =
            self.replay_clean_best_taker_price(&o.instance_id, &msym, mside, &ladder);
        let marketable = match (best_opposing, mside, lim) {
            (Some(bp), Side::Buy, Some(l)) => bp <= l + EPS,
            (Some(bp), Side::Sell, Some(l)) => bp >= l - EPS,
            (Some(_), _, None) => true,
            (None, _, _) => false,
        };

        if marketable && o.post_only {
            self.rejects += 1;
            self.post_only_rejects += 1;
            if let Some(a) = self.audit_row_mut(&o.symbol, &o.instance_id) {
                a.post_only_rejects += 1;
                a.post_only_reject_qty += o.quantity;
            }
            return self.rejected(o, now_ns, "invalid post-only order: order crosses book");
        }
        if marketable {
            return self.take(o, &msym, mside, &ladder, lim, now_ns, causal_race_cap);
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
        causal_race_cap: Option<f64>,
    ) -> OrderUpdate {
        let folded = msym != o.symbol;
        let raw_available = self.books.available_volume(msym, mside == Side::Buy, lim);
        let now_available = self.replay_clean_taker_available(
            &o.instance_id,
            msym,
            mside,
            ladder,
            lim,
        );
        let replay_self_depth = (raw_available - now_available).max(0.0);
        if replay_self_depth > EPS {
            self.taker_replay_self_sweeps_n += 1;
            self.taker_replay_self_depth_qty += replay_self_depth;
        }
        if let Some(a) = self.audit_row_mut(&o.symbol, &o.instance_id) {
            a.taker_candidates += 1;
            a.taker_requested_qty += o.quantity;
            a.taker_available_qty += now_available.min(o.quantity);
            a.taker_replay_self_depth_qty += replay_self_depth;
        }
        // Distribution sample: fillable volume within our limit at the match
        // moment (what this taker order can actually hit on the current book).
        self.taker_avail.push(now_available as f32);
        let mut filled = 0.0;
        let mut notional = 0.0; // canonical-frame notional
        let mut fee = 0.0;
        let mut rem = o.quantity;
        if self.taker_overlap_dedup
            && self.taker_race_rate > 0.0
            && self.taker_comp_rate > 0.0
        {
            // Race and competition can observe the same touch consumption via
            // different feeds. Compute both against the ORIGINAL request. If
            // both independently suppress this order, assume full overlap and
            // retain only the smaller suppression (the larger cap). If only one
            // fires, keep it: a healed trade burst can be competition-only, and
            // a book pull can be race-only.
            let requested = o.quantity;
            let is_buy = mside == Side::Buy;
            let mut race_cap = requested;
            let observed_avail = causal_race_cap.or_else(|| {
                let mut next_avail = self.books.available_volume_next(msym, is_buy, lim)?;
                if self.replay_self_taker_depth_rate > 0.0 {
                    // The future tape can contain the same historical live
                    // quotes too. Without exact historical order tuples, the
                    // current clean availability is a conservative upper bound
                    // for the lookahead race leg.
                    next_avail = next_avail.min(now_available);
                }
                Some(next_avail)
            });
            if let Some(next_avail) = observed_avail {
                if next_avail < now_available {
                    let eff = self.taker_race_rate * next_avail
                        + (1.0 - self.taker_race_rate) * now_available;
                    race_cap = requested.min(eff.max(0.0));
                    if race_cap + EPS < requested {
                        self.taker_race_capped += 1;
                        if race_cap <= EPS {
                            self.taker_race_capped_zero += 1;
                        }
                    }
                }
            }

            let comp = self.taker_competition_volume(msym, mside, lim, now_ns);
            self.taker_comp_vol_sum += comp;
            self.taker_comp_n += 1;
            let comp_cap = requested.min(
                (now_available - self.taker_comp_rate * comp).max(0.0),
            );
            if comp_cap + EPS < requested {
                self.taker_comp_capped += 1;
                if comp_cap <= EPS {
                    self.taker_comp_capped_zero += 1;
                }
            }

            let race_suppressed = (requested - race_cap).max(0.0);
            let comp_suppressed = (requested - comp_cap).max(0.0);
            let (final_cap, race_attribution, comp_attribution) =
                if race_suppressed > EPS && comp_suppressed > EPS {
                    if race_cap >= comp_cap {
                        (race_cap, race_suppressed, 0.0)
                    } else {
                        (comp_cap, 0.0, comp_suppressed)
                    }
                } else {
                    (
                        race_cap.min(comp_cap),
                        race_suppressed,
                        comp_suppressed,
                    )
                };
            rem = final_cap;
            if let Some(a) = self.audit_row_mut(&o.symbol, &o.instance_id) {
                a.taker_race_suppressed_qty += race_attribution;
                a.taker_comp_suppressed_qty += comp_attribution;
            }
        } else {
            // Taker race: if the fillable volume within our limit RECEDES in the next
            // snapshot, liquidity is being pulled away (adverse) — the taker can only
            // hit the blended volume; the unfilled remainder misses (rests/cancels).
            if self.taker_race_rate > 0.0 {
                let before = rem;
                let is_buy = mside == Side::Buy;
                let observed_avail = causal_race_cap.or_else(|| {
                    let mut next_avail = self.books.available_volume_next(msym, is_buy, lim)?;
                    if self.replay_self_taker_depth_rate > 0.0 {
                        next_avail = next_avail.min(now_available);
                    }
                    Some(next_avail)
                });
                if let Some(next_avail) = observed_avail {
                    if next_avail < now_available {
                        let eff = self.taker_race_rate * next_avail
                            + (1.0 - self.taker_race_rate) * now_available;
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
                let suppressed = (before - rem).max(0.0);
                if suppressed > EPS {
                    if let Some(a) = self.audit_row_mut(&o.symbol, &o.instance_id) {
                        a.taker_race_suppressed_qty += suppressed;
                    }
                }
            }
            // Trade-flow taker competition (physical replacement for capture_rate):
            // same-direction takers that traded the touch in our in-flight window
            // beat us to the engine and consumed that liquidity. We fill only the
            // overflow `(now_avail − rate·competing_vol)`. Trades reveal burst
            // competition the book heals between snapshots (invisible to the race).
            if self.taker_comp_rate > 0.0 {
                let before = rem;
                let comp = self.taker_competition_volume(msym, mside, lim, now_ns);
                self.taker_comp_vol_sum += comp;
                self.taker_comp_n += 1;
                let eff = (now_available - self.taker_comp_rate * comp).max(0.0);
                if eff < rem {
                    self.taker_comp_capped += 1;
                    if eff <= EPS {
                        self.taker_comp_capped_zero += 1;
                    }
                }
                rem = rem.min(eff);
                let suppressed = (before - rem).max(0.0);
                if suppressed > EPS {
                    if let Some(a) = self.audit_row_mut(&o.symbol, &o.instance_id) {
                        a.taker_comp_suppressed_qty += suppressed;
                    }
                }
            }
        }
        let tick = self.tick_of(msym);
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
            let clean_qty = self.replay_clean_taker_level_qty(
                &o.instance_id,
                msym,
                mside,
                l.price,
                l.quantity,
                tick,
            );
            let take = rem.min(clean_qty);
            if take <= EPS {
                continue;
            }
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
            if let Some(a) = self.audit_row_mut(&o.symbol, &o.instance_id) {
                a.taker_zero_fills += 1;
            }
            return self.cancelled(o, now_ns, o.quantity);
        }
        if filled <= EPS {
            if let Some(a) = self.audit_row_mut(&o.symbol, &o.instance_id) {
                a.taker_zero_fills += 1;
            }
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
        if let Some(a) = self.audit_row_mut(&o.symbol, &o.instance_id) {
            a.taker_fill_qty += filled;
        }

        let remainder = (o.quantity - filled).max(0.0);
        if remainder > EPS && matches!(o.order_type, OrderType::Limit) {
            self.insert_resting(o, remainder, now_ns, false);
        }
        let taker_tid = format!("simv2-taker-{}", o.client_order_id);
        self.record_recent_fill(
            &o.client_order_id,
            taker_tid.clone(),
            filled,
            avg,
            o.side,
            &o.symbol,
            Liquidity::Taker,
            now_ns,
        );
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
            exchange_event_timestamp_ns: None,
            trade_id: Some(format!("simv2-taker-{}", o.client_order_id)),
            order_audit: None,
            error: None,
        }
    }

    /// Insert a resting maker order, initialising its queue position to the
    /// visible merged depth at its level (§5).
    fn insert_resting(
        &mut self,
        o: &OrderRequest,
        remaining: f64,
        now_ns: u64,
        await_fresh_book: bool,
    ) {
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
        let replay_self_depth_credit = if now_depth > EPS {
            (self.replay_self_depth_rate * remaining).min(now_depth)
        } else {
            0.0
        };
        let queue_now_depth = (now_depth - replay_self_depth_credit).max(0.0);
        // Maker race: if the queue at our (canonical) level GROWS in the next
        // snapshot, the level is strengthening (price about to move favorably) —
        // init q_ahead higher so we sit further back and DON'T fill on that
        // favorable move. Queue shrinking (adverse) keeps q_ahead = now, so we
        // still fill on adverse flow. Pure queue+book, one-step lookahead.
        if self.maker_race_rate > 0.0 {
            self.maker_race_placements += 1;
        }
        let race_q_ahead = match self.books.level_depth_next(&msym, mside, match_price, tick) {
            Some(next_depth)
                if self.maker_race_rate > 0.0
                    && (next_depth - replay_self_depth_credit).max(0.0) > queue_now_depth =>
            {
                let queue_next_depth = (next_depth - replay_self_depth_credit).max(0.0);
                let blended =
                    self.maker_race_rate * queue_next_depth
                        + (1.0 - self.maker_race_rate) * queue_now_depth;
                self.maker_race_inflated += 1;
                self.maker_race_ratio_sum += if queue_now_depth > EPS {
                    blended / queue_now_depth
                } else {
                    1.0
                };
                blended
            }
            _ => queue_now_depth,
        };
        let maker_race_added_q = (race_q_ahead - queue_now_depth).max(0.0);
        // Data-truncation fallback: the recorded book is only 5 levels deep, so a
        // quote reads level_depth = 0 (empty queue) even though live has real
        // resting size + competition there. Two cases:
        //   (1) price BEYOND the recorded window (deeper than the deepest level)
        //       → EXTRAPOLATE the depth profile (least-squares trend, clamped to
        //         the recorded qty band);
        //   (2) a gap INSIDE the window (inside the spread / between levels)
        //       → keep the best-level default: own side, else opposite side.
        let public_q_ahead = if race_q_ahead < EPS && replay_self_depth_credit > EPS {
            // The visible level was entirely attributable to the replayed
            // strategy's own original order. It is not queue ahead; do not
            // invoke the missing-level extrapolation fallback.
            0.0
        } else if race_q_ahead < EPS {
            if let Some((extra, effective_decay)) = self
                .books
                .extrapolate_level_depth_with_decay(&msym, mside, match_price, tick)
            {
                self.q0_extrapolated += 1;
                if effective_decay > 0.0 {
                    self.dynamic_deep_queue_n += 1;
                    self.dynamic_deep_queue_decay_sum += effective_decay;
                    self.dynamic_deep_queue_decay_min =
                        self.dynamic_deep_queue_decay_min.min(effective_decay);
                    self.dynamic_deep_queue_decay_max =
                        self.dynamic_deep_queue_decay_max.max(effective_decay);
                }
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
            race_q_ahead
        };
        let queue_seq = self.next_queue_seq;
        self.next_queue_seq = self
            .next_queue_seq
            .checked_add(1)
            .expect("sim_v2 queue sequence exhausted");
        // A public trade is applied to every order at this level. Giving later
        // orders the remaining size of earlier simulated orders as an explicit
        // queue offset turns those repeated observations into one FIFO volume
        // budget: the print must consume public depth, then earlier own size,
        // before it can reach the later order.
        let simulated_own_ahead_qty = if self.order_queue_position_strength > 0.0 {
            self.order_queue_position_strength
                * self
                    .orders
                    .values()
                    .filter(|prior| {
                        prior.request.instance_id == o.instance_id
                            && Self::same_queue_level(prior, &msym, mside, match_price, tick)
                    })
                    .map(|prior| prior.remaining.max(0.0))
                    .sum::<f64>()
        } else {
            0.0
        };
        let q_ahead = public_q_ahead + simulated_own_ahead_qty;
        if simulated_own_ahead_qty > EPS {
            self.own_queue_positioned_orders += 1;
            self.own_queue_initial_qty += simulated_own_ahead_qty;
        }
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
        let inferred_residual_floor = if self.inferred_maker_residual_rate > 0.0
            && self.inferred_maker_residual_fraction > 0.0
            && stable_order_sample(&o.client_order_id) < self.inferred_maker_residual_rate
        {
            (o.quantity * self.inferred_maker_residual_fraction).min(remaining)
        } else {
            0.0
        };
        if let Some(a) = self.audit_row_mut(&o.symbol, &o.instance_id) {
            a.maker_rests += 1;
            a.maker_rest_qty += remaining;
            if o.post_only {
                a.passive_rests += 1;
                a.passive_rest_qty += remaining;
            }
            a.maker_q_init_sum += q_ahead;
            a.maker_own_q_init_sum += simulated_own_ahead_qty;
            a.maker_race_added_q += maker_race_added_q;
            a.maker_replay_self_depth_credit += replay_self_depth_credit;
        }
        if self.maker_order_audit_enabled {
            if let Some((slug, _)) = self.audit_key(&o.symbol, &o.instance_id) {
                let exposure_end_ns = event_exposure_end_ns(&slug);
                self.maker_order_audit.insert(
                    o.client_order_id.clone(),
                    MakerOrderAuditRow {
                        slug,
                        iid: o.instance_id.clone(),
                        coid: o.client_order_id.clone(),
                        token: o.symbol.clone(),
                        side: o.side,
                        order_type: o.order_type,
                        price,
                        quantity: o.quantity,
                        post_only: o.post_only,
                        strategy_emit_ns: o.timestamp_ns,
                        trigger_exchange_ns: o.quote_trigger_exchange_timestamp_ns,
                        trigger_local_ns: o.quote_trigger_local_timestamp_ns,
                        place_arrival_ns: now_ns,
                        exposure_last_ns: now_ns,
                        exposure_end_ns,
                        rest_time_ns: 0,
                        rest_qty_ns: 0.0,
                        await_fresh_book,
                        visible_depth_at_entry: now_depth,
                        entry_mid: mid0,
                        queue_seq,
                        q_init: q_ahead,
                        simulated_own_ahead_qty,
                        own_cancel_queue_advance_qty: 0.0,
                        replay_self_depth_credit,
                        trade_match_n: 0,
                        trade_match_qty: 0.0,
                        queue_drained_qty: 0.0,
                        candidate_qty: 0.0,
                        maker_toxicity_suppressed_qty: 0.0,
                        depletion_observed_qty: 0.0,
                        depletion_exec_qty: 0.0,
                        depletion_cancel_advance_qty: 0.0,
                        depletion_candidate_qty: 0.0,
                        depletion_budget_suppressed_qty: 0.0,
                        depletion_fill_qty: 0.0,
                        inferred_residual_floor,
                        inferred_residual_suppressed_qty: 0.0,
                        book_through_candidate_qty: 0.0,
                        book_through_fill_qty: 0.0,
                        book_markout_qty: 0.0,
                        book_markout_cost_usdc: 0.0,
                        fill_qty: 0.0,
                        first_fill_ns: 0,
                        last_fill_ns: 0,
                        first_fill_delivery_ns: 0,
                        last_fill_delivery_ns: 0,
                        cancel_arrival_ns: 0,
                        cancel_result: "open",
                        q_ahead_final: q_ahead,
                        remaining_final: remaining,
                    },
                );
            }
        }
        self.orders.insert(
            o.client_order_id.clone(),
            RestingOrder {
                request: o.clone(),
                match_symbol: msym,
                match_side: mside,
                match_price,
                locked_usdc: locked,
                remaining,
                inferred_residual_floor,
                inferred_residual_realized: false,
                tick,
                q_ahead,
                own_q_ahead: simulated_own_ahead_qty,
                queue_seq,
                replay_self_depth_credit,
                level_qty_at_sync: now_depth,
                mid_at_sync: mid0,
                entry_mid: mid0,
                traded_since_sync: 0.0,
                placed_ns: now_ns,
                await_fresh_book,
            },
        );
    }

    fn rest(&mut self, o: &OrderRequest, now_ns: u64, remaining: f64) -> OrderUpdate {
        self.rest_inner(o, now_ns, remaining, false)
    }

    fn rest_stale(&mut self, o: &OrderRequest, now_ns: u64, remaining: f64) -> OrderUpdate {
        self.rest_inner(o, now_ns, remaining, true)
    }

    fn rest_inner(
        &mut self,
        o: &OrderRequest,
        now_ns: u64,
        remaining: f64,
        await_fresh_book: bool,
    ) -> OrderUpdate {
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
        self.insert_resting(o, remaining, now_ns, await_fresh_book);
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
            exchange_event_timestamp_ns: None,
            trade_id: None,
            order_audit: None,
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
            exchange_event_timestamp_ns: None,
            trade_id: None,
            order_audit: None,
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
            exchange_event_timestamp_ns: None,
            trade_id: None,
            order_audit: None,
            error: None,
        }
    }

    pub fn cancel_order(&mut self, exchange: Exchange, coid: &str, now_ns: u64) -> OrderUpdate {
        self.audit_clock_ns = self.audit_clock_ns.max(now_ns);
        if let Some(o) = self.orders.remove(coid) {
            // Cancelling an earlier own order removes only its still-resting
            // contribution from later same-level FIFO positions. Public queue
            // depth and orders at other levels/instances remain untouched.
            let max_advance = self.order_queue_position_strength * o.remaining.max(0.0);
            let mut advanced_n = 0u64;
            let mut advanced_qty = 0.0;
            if max_advance > EPS {
                let slugs = &self.event_slug_by_token;
                let audits = &mut self.fill_audit;
                let order_audits = &mut self.maker_order_audit;
                for (later_coid, later) in self.orders.iter_mut() {
                    if later.queue_seq <= o.queue_seq
                        || later.request.instance_id != o.request.instance_id
                        || !Self::same_queue_level(
                            later,
                            &o.match_symbol,
                            o.match_side,
                            o.match_price,
                            o.tick,
                        )
                    {
                        continue;
                    }
                    let advance = max_advance
                        .min(later.own_q_ahead)
                        .min(later.q_ahead);
                    if advance <= EPS {
                        continue;
                    }
                    later.own_q_ahead = (later.own_q_ahead - advance).max(0.0);
                    later.q_ahead = (later.q_ahead - advance).max(0.0);
                    advanced_n += 1;
                    advanced_qty += advance;
                    if let Some(a) = order_audits.get_mut(later_coid) {
                        a.own_cancel_queue_advance_qty += advance;
                        a.q_ahead_final = later.q_ahead;
                    }
                    if let Some(slug) = slugs
                        .get(&later.request.symbol)
                        .or_else(|| slugs.get(&later.match_symbol))
                    {
                        let key = (slug.clone(), later.request.instance_id.clone());
                        let a = audits.entry(key.clone()).or_insert_with(|| FillAuditRow {
                            slug: key.0,
                            iid: key.1,
                            ..FillAuditRow::default()
                        });
                        a.maker_own_cancel_queue_advance_qty += advance;
                    }
                }
            }
            self.own_queue_cancel_advances_n += advanced_n;
            self.own_queue_cancel_advance_qty += advanced_qty;
            self.record_lifetime(o.placed_ns, now_ns);
            if let Some(a) = self.maker_order_audit.get_mut(coid) {
                accrue_order_exposure(a, now_ns, o.remaining);
                a.cancel_arrival_ns = now_ns;
                a.cancel_result = "cancelled";
                a.q_ahead_final = o.q_ahead;
                a.remaining_final = o.remaining;
            }
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
                exchange_event_timestamp_ns: None,
                trade_id: None,
                order_audit: None,
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
            .map(|rf| {
                (
                    rf.symbol.clone(),
                    rf.side,
                    rf.filled_quantity,
                    rf.price,
                    rf.trade_id.clone(),
                    rf.liquidity,
                )
            });
        if let Some((symbol, side, filled_quantity, price, trade_id, liquidity)) = hit {
            self.matched_cant_cancel += 1;
            if let Some(a) = self.maker_order_audit.get_mut(coid) {
                a.cancel_arrival_ns = now_ns;
                a.cancel_result = "matched_before_cancel";
            }
            return OrderUpdate {
                client_order_id: coid.to_string(),
                exchange,
                symbol,
                side,
                exchange_order_id: None,
                status: OrderStatus::Filled,
                liquidity: Some(liquidity),
                filled_quantity,
                remaining_quantity: 0.0,
                avg_fill_price: price,
                timestamp_ns: now_ns,
                exchange_event_timestamp_ns: None,
                trade_id: Some(trade_id),
                order_audit: None,
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
            exchange_event_timestamp_ns: None,
            trade_id: None,
            order_audit: None,
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
                exchange_event_timestamp_ns: None,
                trade_id: None,
                order_audit: None,
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
                exchange_event_timestamp_ns: None,
                trade_id: None,
                order_audit: None,
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
            series_slug: "s".into(),
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
        book_dual_ts(symbol, bids, asks, ts, ts)
    }

    fn book_dual_ts(
        symbol: &str,
        bids: Vec<(f64, f64)>,
        asks: Vec<(f64, f64)>,
        exchange_ts: u64,
        local_ts: u64,
    ) -> OrderBookSnapshot {
        OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: symbol.into(),
            bids: bids.into_iter().map(|(p, q)| PriceLevel { price: p, quantity: q }).collect(),
            asks: asks.into_iter().map(|(p, q)| PriceLevel { price: p, quantity: q }).collect(),
            exchange_timestamp_ns: exchange_ts,
            local_timestamp_ns: local_ts,
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
            quote_trigger_exchange_timestamp_ns: 0,
            quote_trigger_local_timestamp_ns: 0,
            quote_event_id: String::new(),
            quote_trigger_source: crate::types::QuoteTriggerSource::Unknown,
            timestamp_ns: 0,
            instance_id: "iid".into(),
            fee_rate_bps: 0,
            post_only,
            reduce_only: false,
            outcome_label: String::new(),
        }
    }

    fn trade(symbol: &str, side: Side, price: f64, qty: f64) -> TradeTick {
        trade_ts(symbol, side, price, qty, 100)
    }

    fn trade_ts(symbol: &str, side: Side, price: f64, qty: f64, ts: u64) -> TradeTick {
        TradeTick {
            exchange: Exchange::Polymarket,
            symbol: symbol.into(),
            exchange_trade_id: None,
            price,
            quantity: qty,
            side,
            exchange_timestamp_ns: ts,
            local_timestamp_ns: ts,
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
    fn replay_self_taker_cleaning_skips_own_recorded_level() {
        let mut c = core();
        c.configure_replay_self_taker_depth(1.0);
        c.on_orderbook(&book(
            "up",
            vec![(0.58, 100.0)],
            vec![(0.62, 5.0), (0.63, 10.0)],
        ));
        let maker = order(
            "maker",
            "up",
            Side::Sell,
            0.62,
            5.0,
            true,
            OrderType::Limit,
        );
        assert_eq!(c.submit_order(&maker, 1).status, OrderStatus::Accepted);

        let taker = order(
            "taker",
            "up",
            Side::Buy,
            0.63,
            5.0,
            false,
            OrderType::Fak,
        );
        assert!(c.would_cross(&taker, 2));
        let fill = c.submit_order(&taker, 2);
        assert_eq!(fill.status, OrderStatus::Filled);
        assert!((fill.filled_quantity - 5.0).abs() < EPS);
        assert!((fill.avg_fill_price - 0.63).abs() < EPS);
        assert_eq!(c.taker_replay_self_sweeps_n, 1);
        assert!((c.taker_replay_self_depth_qty - 5.0).abs() < EPS);
        let audit = c.fill_audit_rows();
        assert_eq!(audit.len(), 1);
        assert!((audit[0].taker_replay_self_depth_qty - 5.0).abs() < EPS);
    }

    #[test]
    fn replay_self_taker_cleaning_is_disabled_and_instance_isolated() {
        fn probe(rate: f64, taker_iid: &str) -> OrderUpdate {
            let mut c = core();
            c.configure_replay_self_taker_depth(rate);
            c.on_orderbook(&book(
                "up",
                vec![(0.58, 100.0)],
                vec![(0.62, 5.0), (0.63, 10.0)],
            ));
            let maker = order(
                "maker",
                "up",
                Side::Sell,
                0.62,
                5.0,
                true,
                OrderType::Limit,
            );
            assert_eq!(c.submit_order(&maker, 1).status, OrderStatus::Accepted);
            let mut taker = order(
                "taker",
                "up",
                Side::Buy,
                0.63,
                5.0,
                false,
                OrderType::Fak,
            );
            taker.instance_id = taker_iid.to_string();
            c.submit_order(&taker, 2)
        }

        let disabled = probe(0.0, "iid");
        assert_eq!(disabled.status, OrderStatus::Filled);
        assert!((disabled.avg_fill_price - 0.62).abs() < EPS);

        let other_instance = probe(1.0, "other");
        assert_eq!(other_instance.status, OrderStatus::Filled);
        assert!((other_instance.avg_fill_price - 0.62).abs() < EPS);
    }

    #[test]
    fn maker_order_audit_preserves_causal_queue_fill_and_cancel_timeline() {
        let mut c = core();
        c.configure_maker_order_audit(true);
        c.on_orderbook(&book("up", vec![(0.60, 10.0)], vec![(0.62, 80.0)]));
        let accepted = c.submit_order(
            &order("m", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit),
            1,
        );
        assert_eq!(accepted.status, OrderStatus::Accepted);

        let fills = c.on_trade_tick(&trade_ts("up", Side::Sell, 0.60, 12.0, 100));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].status, OrderStatus::PartiallyFilled);
        assert_eq!(fills[0].filled_quantity, 2.0);
        let cancelled = c.cancel_order(Exchange::Polymarket, "m", 150);
        assert_eq!(cancelled.status, OrderStatus::Cancelled);
        // Independent private-lane samples may reorder partial fragments; the
        // audit retains the true earliest/latest strategy-visible boundaries.
        c.record_fill_delivery("m", 300);
        c.record_fill_delivery("m", 250);
        c.record_fill_delivery("m", 350);

        let rows = c.maker_order_audit_rows();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.slug, "s");
        assert_eq!(row.coid, "m");
        assert_eq!(row.place_arrival_ns, 1);
        assert!(row.post_only);
        assert_eq!(row.rest_time_ns, 149);
        assert!((row.rest_qty_ns - 645.0).abs() < EPS);
        assert_eq!(row.q_init, 10.0);
        assert_eq!(row.replay_self_depth_credit, 0.0);
        assert_eq!(row.trade_match_n, 1);
        assert_eq!(row.trade_match_qty, 12.0);
        assert_eq!(row.queue_drained_qty, 10.0);
        assert_eq!(row.candidate_qty, 2.0);
        assert_eq!(row.book_through_candidate_qty, 0.0);
        assert_eq!(row.book_through_fill_qty, 0.0);
        assert_eq!(row.depletion_fill_qty, 0.0);
        assert_eq!(row.fill_qty, 2.0);
        assert_eq!(row.first_fill_ns, 100);
        assert_eq!(row.first_fill_delivery_ns, 250);
        assert_eq!(row.last_fill_delivery_ns, 350);
        assert_eq!(row.cancel_arrival_ns, 150);
        assert_eq!(row.cancel_result, "cancelled");
        assert_eq!(row.q_ahead_final, 0.0);
        assert_eq!(row.remaining_final, 3.0);

        let event_rows = c.fill_audit_rows();
        assert_eq!(event_rows.len(), 1);
        let event = &event_rows[0];
        assert_eq!(event.passive_place_orders, 1);
        assert_eq!(event.passive_rests, 1);
        assert_eq!(event.passive_cancel_orders, 1);
        assert_eq!(event.passive_orders_with_fill, 1);
        assert_eq!(event.passive_open_orders, 0);
        assert_eq!(event.maker_rest_time_ns, 149);
        assert!((event.maker_rest_qty_ns - 645.0).abs() < EPS);
        assert_eq!(event.passive_rest_time_ns, 149);
        assert!((event.passive_rest_qty_ns - 645.0).abs() < EPS);
        assert!((event.passive_fill_qty - 2.0).abs() < EPS);
    }

    #[test]
    fn passive_exposure_snapshots_still_open_orders_at_latest_exchange_time() {
        let mut c = core();
        c.configure_maker_order_audit(true);
        c.on_orderbook(&book_ts(
            "up",
            vec![(0.60, 10.0)],
            vec![(0.62, 80.0)],
            1,
        ));
        assert_eq!(
            c.submit_order(
                &order("open", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit),
                2,
            )
            .status,
            OrderStatus::Accepted
        );
        c.on_orderbook(&book_ts(
            "up",
            vec![(0.60, 10.0)],
            vec![(0.62, 80.0)],
            102,
        ));

        let order = &c.maker_order_audit_rows()[0];
        assert_eq!(order.rest_time_ns, 100);
        assert!((order.rest_qty_ns - 500.0).abs() < EPS);
        let event = &c.fill_audit_rows()[0];
        assert_eq!(event.maker_open_orders, 1);
        assert_eq!(event.passive_open_orders, 1);
        assert_eq!(event.passive_rest_time_ns, 100);
        assert!((event.passive_rest_qty_ns - 500.0).abs() < EPS);
    }

    #[test]
    fn passive_exposure_stops_when_event_is_retired() {
        let mut c = core();
        c.configure_maker_order_audit(true);
        c.on_orderbook(&book_ts(
            "up",
            vec![(0.60, 10.0)],
            vec![(0.62, 80.0)],
            1,
        ));
        assert_eq!(
            c.submit_order(
                &order("retired", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit),
                2,
            )
            .status,
            OrderStatus::Accepted
        );
        c.on_orderbook(&book_ts(
            "up",
            vec![(0.60, 10.0)],
            vec![(0.62, 80.0)],
            102,
        ));
        c.retire_event("cond1", &["up".to_string(), "down".to_string()]);
        // Later exchange activity must not extend the retired quote's lifetime.
        c.audit_clock_ns = 1_000;

        let order = &c.maker_order_audit_rows()[0];
        assert_eq!(order.cancel_result, "event_retired");
        assert_eq!(order.rest_time_ns, 100);
        assert!((order.rest_qty_ns - 500.0).abs() < EPS);
        let event = &c.fill_audit_rows()[0];
        assert_eq!(event.passive_cancel_orders, 1);
        assert_eq!(event.passive_open_orders, 0);
        assert_eq!(event.passive_rest_time_ns, 100);
        assert!((event.passive_rest_qty_ns - 500.0).abs() < EPS);
    }

    #[test]
    fn passive_exposure_is_capped_at_market_end() {
        let mut c = core();
        c.configure_maker_order_audit(true);
        c.on_orderbook(&book_ts(
            "up",
            vec![(0.60, 10.0)],
            vec![(0.62, 80.0)],
            1,
        ));
        assert_eq!(
            c.submit_order(
                &order("capped", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit),
                2,
            )
            .status,
            OrderStatus::Accepted
        );
        c.maker_order_audit.get_mut("capped").unwrap().exposure_end_ns = 52;
        c.on_orderbook(&book_ts(
            "up",
            vec![(0.60, 10.0)],
            vec![(0.62, 80.0)],
            102,
        ));

        let order = &c.maker_order_audit_rows()[0];
        assert_eq!(order.rest_time_ns, 50);
        assert!((order.rest_qty_ns - 250.0).abs() < EPS);
    }

    #[test]
    fn same_level_own_orders_consume_one_public_trade_fifo_budget() {
        let mut c = core();
        c.configure_order_queue_position(1.0);
        c.configure_maker_order_audit(true);
        c.on_orderbook(&book("up", vec![(0.60, 10.0)], vec![(0.62, 80.0)]));

        // Reverse lexical ids prove both queue position and delivery order use
        // exchange arrival, not BTreeMap/client-id order.
        assert_eq!(
            c.submit_order(
                &order("z-older", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit),
                1,
            )
            .status,
            OrderStatus::Accepted
        );
        assert_eq!(
            c.submit_order(
                &order("a-newer", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit),
                2,
            )
            .status,
            OrderStatus::Accepted
        );
        assert_eq!(c.orders["z-older"].q_ahead, 10.0);
        assert_eq!(c.orders["a-newer"].q_ahead, 15.0);
        assert_eq!(c.orders["a-newer"].own_q_ahead, 5.0);

        // Six shares exceed the ten-share public queue: five fill the older
        // order and exactly one reaches the newer order.
        let fills = c.on_trade_tick(&trade("up", Side::Sell, 0.60, 16.0));
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].client_order_id, "z-older");
        assert_eq!(fills[0].filled_quantity, 5.0);
        assert_eq!(fills[1].client_order_id, "a-newer");
        assert_eq!(fills[1].filled_quantity, 1.0);
        assert_eq!(c.orders["a-newer"].remaining, 4.0);
        assert_eq!(c.own_queue_positioned_orders, 1);
        assert_eq!(c.own_queue_initial_qty, 5.0);
    }

    #[test]
    fn cancelling_older_own_order_advances_only_later_same_instance_order() {
        let mut c = core();
        c.configure_order_queue_position(1.0);
        c.configure_maker_order_audit(true);
        c.on_orderbook(&book("up", vec![(0.60, 10.0)], vec![(0.62, 80.0)]));
        let older = order("older", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit);
        let newer = order("newer", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit);
        assert_eq!(c.submit_order(&older, 1).status, OrderStatus::Accepted);
        assert_eq!(c.submit_order(&newer, 2).status, OrderStatus::Accepted);
        assert_eq!(c.orders["newer"].q_ahead, 15.0);

        assert_eq!(
            c.cancel_order(Exchange::Polymarket, "older", 3).status,
            OrderStatus::Cancelled
        );
        assert_eq!(c.orders["newer"].q_ahead, 10.0);
        assert_eq!(c.orders["newer"].own_q_ahead, 0.0);
        assert_eq!(c.own_queue_cancel_advances_n, 1);
        assert_eq!(c.own_queue_cancel_advance_qty, 5.0);
        let newer_audit = c
            .maker_order_audit_rows()
            .into_iter()
            .find(|row| row.coid == "newer")
            .unwrap();
        assert_eq!(newer_audit.own_cancel_queue_advance_qty, 5.0);
        assert_eq!(newer_audit.q_ahead_final, 10.0);
    }

    #[test]
    fn public_book_cancellation_cannot_erase_earlier_own_fifo_quantity() {
        let mut c = core();
        c.configure(Some(1.0), 2_000_000_000);
        c.configure_order_queue_position(1.0);
        c.on_orderbook(&book("up", vec![(0.60, 10.0)], vec![(0.62, 80.0)]));
        assert_eq!(
            c.submit_order(
                &order("older", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit),
                1,
            )
            .status,
            OrderStatus::Accepted
        );
        assert_eq!(
            c.submit_order(
                &order("newer", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit),
                2,
            )
            .status,
            OrderStatus::Accepted
        );

        // The ten public shares disappear without a trade. They can advance
        // both orders through public depth, but the older order's five shares
        // must remain ahead of the newer order.
        assert!(c
            .on_orderbook(&book("up", vec![], vec![(0.62, 80.0)]))
            .is_empty());
        assert_eq!(c.orders["older"].q_ahead, 0.0);
        assert_eq!(c.orders["newer"].q_ahead, 5.0);
        assert_eq!(c.orders["newer"].own_q_ahead, 5.0);

        // The next five-share print fills only the older FIFO head. One more
        // share is required before the newer order can fill.
        let head = c.on_trade_tick(&trade("up", Side::Sell, 0.60, 5.0));
        assert_eq!(head.len(), 1);
        assert_eq!(head[0].client_order_id, "older");
        assert!(c
            .on_trade_tick(&trade("up", Side::Sell, 0.60, 1.0))
            .iter()
            .any(|fill| fill.client_order_id == "newer"));
    }

    #[test]
    fn own_queue_position_is_disabled_and_instance_isolated() {
        let legacy_duplicate_fill = |strength: f64| {
            let mut c = core();
            c.configure_order_queue_position(strength);
            c.on_orderbook(&book("up", vec![(0.60, 10.0)], vec![(0.62, 80.0)]));
            assert_eq!(
                c.submit_order(
                    &order("one", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit),
                    1,
                )
                .status,
                OrderStatus::Accepted
            );
            let mut second = order("two", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit);
            second.instance_id = "other-iid".into();
            assert_eq!(c.submit_order(&second, 2).status, OrderStatus::Accepted);
            assert_eq!(c.orders["one"].q_ahead, 10.0);
            assert_eq!(c.orders["two"].q_ahead, 10.0);
            c.on_trade_tick(&trade("up", Side::Sell, 0.60, 11.0))
        };

        // Disabled mode preserves the historical independent-order result.
        let disabled = legacy_duplicate_fill(0.0);
        assert_eq!(disabled.len(), 2);
        assert!(disabled.iter().all(|fill| fill.filled_quantity == 1.0));
        // Full FIFO remains isolated between strategy instances/accounts.
        let isolated = legacy_duplicate_fill(1.0);
        assert_eq!(isolated.len(), 2);
        assert!(isolated.iter().all(|fill| fill.filled_quantity == 1.0));
    }

    #[test]
    fn causal_maker_toxicity_suppresses_only_favorable_trade_overflow() {
        let probe = |ask_after: f64, strength: f64| {
            let mut c = core();
            c.configure_maker_toxicity(strength, 1.0);
            c.configure_maker_order_audit(true);
            c.on_orderbook(&book("up", vec![(0.60, 10.0)], vec![(0.62, 80.0)]));
            assert_eq!(
                c.submit_order(
                    &order("maker", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit),
                    1,
                )
                .status,
                OrderStatus::Accepted
            );
            c.on_orderbook(&book("up", vec![(0.60, 10.0)], vec![(ask_after, 80.0)]));
            let fills = c.on_trade_tick(&trade("up", Side::Sell, 0.60, 15.0));
            (fills, c)
        };

        // Mid 0.61→0.62 is one favorable tick for the resting bid. Strength
        // 0.5 suppresses half of the five-share candidate without repricing.
        let (favorable, favorable_core) = probe(0.64, 0.5);
        assert_eq!(favorable.len(), 1);
        assert_eq!(favorable[0].filled_quantity, 2.5);
        assert_eq!(favorable[0].avg_fill_price, 0.60);
        assert_eq!(favorable_core.orders["maker"].q_ahead, 2.5);
        assert_eq!(favorable_core.maker_toxicity_suppressed_n, 1);
        assert_eq!(favorable_core.maker_toxicity_suppressed_qty, 2.5);
        assert_eq!(
            favorable_core.maker_order_audit_rows()[0].maker_toxicity_suppressed_qty,
            2.5
        );

        // An adverse move is fully fillable; strength zero is exact legacy
        // behavior even after the same favorable book move.
        let (adverse, _) = probe(0.60, 1.0);
        assert_eq!(adverse.len(), 1);
        assert_eq!(adverse[0].filled_quantity, 5.0);
        let (disabled, disabled_core) = probe(0.64, 0.0);
        assert_eq!(disabled.len(), 1);
        assert_eq!(disabled[0].filled_quantity, 5.0);
        assert_eq!(disabled_core.maker_toxicity_suppressed_n, 0);
    }

    #[test]
    fn disabled_book_stale_gate_preserves_old_book_taker_fill() {
        let mut c = core();
        c.configure_book_stale_gate(0);
        c.on_orderbook(&book_ts(
            "up",
            vec![(0.58, 100.0)],
            vec![(0.62, 80.0)],
            1_000_000_000,
        ));
        let o = order("t", "up", Side::Buy, 0.62, 10.0, false, OrderType::Limit);
        assert!(c.would_cross(&o, 30_000_000_000));
        let u = c.submit_order(&o, 30_000_000_000);
        assert_eq!(u.status, OrderStatus::Filled);
        assert_eq!(c.book_stale_order_blocks, 0);
    }

    #[test]
    fn exchange_clock_stale_blocks_taker_while_local_clock_is_fresh() {
        let mut c = core();
        c.configure_book_stale_gate(2_000_000_000);
        // The recorder received this snapshot recently, but its exchange
        // timestamp stopped advancing three seconds ago.
        let ob = book_dual_ts(
            "up",
            vec![(0.58, 100.0)],
            vec![(0.62, 80.0)],
            1_000_000_000,
            3_000_000_000,
        );
        c.on_orderbook(&ob);
        c.on_local_orderbook(&ob, 3_000_000_000);
        let o = order("t", "up", Side::Buy, 0.62, 10.0, false, OrderType::Limit);
        assert!(!c.would_cross(&o, 4_000_000_000));
        let u = c.submit_order(&o, 4_000_000_000);
        assert_eq!(u.status, OrderStatus::Accepted);
        assert_eq!(u.filled_quantity, 0.0);
        assert_eq!(c.book_stale_order_blocks, 1);
        assert_eq!(c.book_stale_exchange_hits, 1);
        assert_eq!(c.book_stale_local_hits, 0);
    }

    #[test]
    fn local_clock_stale_blocks_fill_under_exchange_clock_skew() {
        let mut c = core();
        c.configure_book_stale_gate(2_000_000_000);
        // A future/skewed exchange timestamp must not mask a silent local feed.
        let ob = book_dual_ts(
            "up",
            vec![(0.58, 100.0)],
            vec![(0.62, 80.0)],
            3_500_000_000,
            1_000_000_000,
        );
        c.on_orderbook(&ob);
        c.on_local_orderbook(&ob, 1_000_000_000);
        let u = c.submit_order(
            &order("t", "up", Side::Buy, 0.62, 10.0, false, OrderType::Fak),
            4_000_000_000,
        );
        assert_eq!(u.status, OrderStatus::Cancelled);
        assert_eq!(u.filled_quantity, 0.0);
        assert_eq!(c.book_stale_exchange_hits, 0);
        assert_eq!(c.book_stale_local_hits, 1);
    }

    #[test]
    fn stale_trade_cannot_fill_existing_maker() {
        let mut c = core();
        c.set_fold_outcomes(true);
        c.configure_book_stale_gate(2_000_000_000);
        let ob = book_ts(
            "up",
            vec![(0.60, 10.0)],
            vec![(0.62, 80.0)],
            1_000_000_000,
        );
        c.on_orderbook(&ob);
        c.on_local_orderbook(&ob, 1_000_000_000);
        let u = c.submit_order(
            &order("m", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit),
            1_100_000_000,
        );
        assert_eq!(u.status, OrderStatus::Accepted);
        let stale = c.on_trade_tick(&trade_ts(
            "up",
            Side::Sell,
            0.60,
            20.0,
            4_000_000_000,
        ));
        assert!(stale.is_empty());
        assert_eq!(c.book_stale_trade_blocks, 1);
        assert!(c.orders.contains_key("m"));
    }

    #[test]
    fn local_clock_only_stale_can_fill_existing_maker_when_configured() {
        let mut c = core();
        c.set_fold_outcomes(true);
        c.configure_book_stale_gate(2_000_000_000);
        c.configure_stale_resting_exchange_only(true);
        // Exchange book is only 0.5s old at the trade, while the last local
        // full-book receipt is 3s old. The already-resting exchange order is
        // still eligible; new order admission would remain dual-clock gated.
        let ob = book_dual_ts(
            "up",
            vec![(0.60, 10.0)],
            vec![(0.62, 80.0)],
            3_500_000_000,
            1_000_000_000,
        );
        c.on_orderbook(&ob);
        c.on_local_orderbook(&ob, 1_000_000_000);
        let u = c.submit_order(
            &order("m", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit),
            1_100_000_000,
        );
        assert_eq!(u.status, OrderStatus::Accepted);
        let fills = c.on_trade_tick(&trade_ts(
            "up",
            Side::Sell,
            0.60,
            20.0,
            4_000_000_000,
        ));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].status, OrderStatus::Filled);
        assert_eq!(c.book_stale_trade_blocks, 0);
    }

    #[test]
    fn local_stale_book_through_cannot_fill_existing_maker() {
        let mut c = core();
        c.set_fold_outcomes(true);
        c.configure_book_stale_gate(2_000_000_000);
        c.configure_book_through(1.0);
        let initial = book_ts(
            "up",
            vec![(0.60, 10.0)],
            vec![(0.62, 80.0)],
            1_000_000_000,
        );
        c.on_orderbook(&initial);
        c.on_local_orderbook(&initial, 1_000_000_000);
        let u = c.submit_order(
            &order("m", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit),
            1_100_000_000,
        );
        assert_eq!(u.status, OrderStatus::Accepted);
        // A small fresh trade confirms the price but does not drain q_ahead.
        assert!(c
            .on_trade_tick(&trade_ts(
                "up",
                Side::Sell,
                0.60,
                5.0,
                1_200_000_000,
            ))
            .is_empty());
        // Exchange book is fresh, but the strategy has received no full book
        // for three seconds: book-through must not fill and its confirmation
        // must not carry into the next fresh interval.
        let through = book_ts(
            "up",
            vec![(0.58, 10.0)],
            vec![(0.59, 100.0)],
            4_000_000_000,
        );
        assert!(c.on_orderbook(&through).is_empty());
        assert!(c.orders.contains_key("m"));
        c.on_local_orderbook(&through, 4_000_000_000);
        let next = book_ts(
            "up",
            vec![(0.58, 10.0)],
            vec![(0.59, 100.0)],
            4_100_000_000,
        );
        assert!(c.on_orderbook(&next).is_empty());
        assert!(c.orders.contains_key("m"));
    }

    #[test]
    fn stale_entry_rebases_behind_next_fresh_visible_queue() {
        let mut c = core();
        c.configure_book_stale_gate(2_000_000_000);
        let initial = book_ts(
            "up",
            vec![(0.60, 50.0)],
            vec![(0.62, 80.0)],
            1_000_000_000,
        );
        c.on_orderbook(&initial);
        c.on_local_orderbook(&initial, 1_000_000_000);
        let u = c.submit_order(
            &order("m", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit),
            4_000_000_000,
        );
        assert_eq!(u.status, OrderStatus::Accepted);
        assert!(c.orders.get("m").unwrap().await_fresh_book);

        let fresh = book_ts(
            "up",
            vec![(0.60, 100.0)],
            vec![(0.62, 80.0)],
            5_000_000_000,
        );
        c.on_orderbook(&fresh);
        assert!(c.orders.get("m").unwrap().await_fresh_book);
        c.on_local_orderbook(&fresh, 5_000_000_000);
        let resting = c.orders.get("m").unwrap();
        assert!(!resting.await_fresh_book);
        assert!((resting.q_ahead - 100.0).abs() < 1e-9);
        assert_eq!(c.book_stale_rebases, 1);

        assert!(c
            .on_trade_tick(&trade_ts(
                "up",
                Side::Sell,
                0.60,
                90.0,
                5_100_000_000,
            ))
            .is_empty());
        let fills = c.on_trade_tick(&trade_ts(
            "up",
            Side::Sell,
            0.60,
            20.0,
            5_200_000_000,
        ));
        assert_eq!(fills.len(), 1);
        assert!((fills[0].filled_quantity - 5.0).abs() < 1e-9);
    }

    #[test]
    fn server_book_cannot_refresh_local_clock_before_local_receipt() {
        let mut c = core();
        c.configure_book_stale_gate(2_000_000_000);
        let future_local = book_dual_ts(
            "up",
            vec![(0.58, 100.0)],
            vec![(0.62, 80.0)],
            1_000_000_000,
            10_000_000_000,
        );
        c.on_orderbook(&future_local);
        let u = c.submit_order(
            &order("t", "up", Side::Buy, 0.62, 10.0, false, OrderType::Fak),
            1_500_000_000,
        );
        assert_eq!(u.status, OrderStatus::Cancelled);
        assert_eq!(u.filled_quantity, 0.0);
        assert_eq!(c.book_stale_exchange_hits, 0);
        assert_eq!(c.book_stale_local_hits, 1);
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
    fn causal_taker_race_cap_does_not_require_a_future_book() {
        let mut c = core();
        c.configure_race(0.0, 1.0);
        c.on_orderbook(&book("up", vec![(0.58, 10.0)], vec![(0.62, 25.0)]));
        let o = order("t", "up", Side::Buy, 0.62, 20.0, false, OrderType::Limit);
        assert!((c.taker_available_qty(&o) - 25.0).abs() < 1e-9);

        // The simulator observed a transient dip to seven shares between
        // exchange arrival and match. No post-match next-book peek is present.
        let u = c.submit_order_with_taker_race_cap(&o, 1, Some(7.0));
        assert_eq!(u.status, OrderStatus::PartiallyFilled);
        assert!((u.filled_quantity - 7.0).abs() < 1e-9);
        assert_eq!(c.taker_race_capped, 1);
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
            exchange_trade_id: None,
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
    fn taker_overlap_dedup_applies_one_overlapping_cap() {
        // Both feeds observe a liquidity loss: race says only 3 remain, while
        // recent taker trades imply 5 remain. Historical composition takes the
        // stricter min (=3); overlap de-dup applies one suppression (=5).
        let mut historical = core();
        historical.configure_race(0.0, 1.0);
        historical.configure_taker_comp(1.0, 250_000_000);
        historical.on_orderbook(&book("up", vec![], vec![(0.62, 25.0)]));
        let comp = TradeTick {
            exchange: Exchange::Polymarket,
            symbol: "up".into(),
            exchange_trade_id: None,
            price: 0.62,
            quantity: 20.0,
            side: Side::Buy,
            exchange_timestamp_ns: 1_000,
            local_timestamp_ns: 1_000,
        };
        historical.on_trade_tick(&comp);
        historical.set_next_book(
            "up",
            vec![],
            vec![PriceLevel {
                price: 0.62,
                quantity: 3.0,
            }],
        );
        let old = historical.submit_order(
            &order("old", "up", Side::Buy, 0.62, 20.0, false, OrderType::Limit),
            200_000,
        );
        assert!((old.filled_quantity - 3.0).abs() < 1e-6);

        let mut dedup = core();
        dedup.configure_race(0.0, 1.0);
        dedup.configure_taker_comp(1.0, 250_000_000);
        dedup.configure_taker_overlap_dedup(true);
        dedup.on_orderbook(&book("up", vec![], vec![(0.62, 25.0)]));
        dedup.on_trade_tick(&comp);
        dedup.set_next_book(
            "up",
            vec![],
            vec![PriceLevel {
                price: 0.62,
                quantity: 3.0,
            }],
        );
        let new = dedup.submit_order(
            &order("new", "up", Side::Buy, 0.62, 20.0, false, OrderType::Limit),
            200_000,
        );
        assert!((new.filled_quantity - 5.0).abs() < 1e-6);
    }

    #[test]
    fn taker_overlap_dedup_keeps_non_overlapping_signals() {
        // Competition-only observation: the future book does not recede.
        let mut competition_only = core();
        competition_only.configure_race(0.0, 1.0);
        competition_only.configure_taker_comp(1.0, 250_000_000);
        competition_only.configure_taker_overlap_dedup(true);
        competition_only.on_orderbook(&book("up", vec![], vec![(0.62, 25.0)]));
        let comp = TradeTick {
            exchange: Exchange::Polymarket,
            symbol: "up".into(),
            exchange_trade_id: None,
            price: 0.62,
            quantity: 20.0,
            side: Side::Buy,
            exchange_timestamp_ns: 1_000,
            local_timestamp_ns: 1_000,
        };
        competition_only.on_trade_tick(&comp);
        competition_only.set_next_book(
            "up",
            vec![],
            vec![PriceLevel {
                price: 0.62,
                quantity: 25.0,
            }],
        );
        let comp_fill = competition_only.submit_order(
            &order("comp", "up", Side::Buy, 0.62, 20.0, false, OrderType::Limit),
            200_000,
        );
        assert!((comp_fill.filled_quantity - 5.0).abs() < 1e-6);

        // Race-only observation: no recent competing trade exists.
        let mut race_only = core();
        race_only.configure_race(0.0, 1.0);
        race_only.configure_taker_comp(1.0, 250_000_000);
        race_only.configure_taker_overlap_dedup(true);
        race_only.on_orderbook(&book("up", vec![], vec![(0.62, 25.0)]));
        race_only.set_next_book(
            "up",
            vec![],
            vec![PriceLevel {
                price: 0.62,
                quantity: 3.0,
            }],
        );
        let race_fill = race_only.submit_order(
            &order("race", "up", Side::Buy, 0.62, 20.0, false, OrderType::Limit),
            200_000,
        );
        assert!((race_fill.filled_quantity - 3.0).abs() < 1e-6);
    }

    #[test]
    fn taker_competition_off_fills_full() {
        // Same setup, competition OFF → no trade-flow cap, fills the full 20.
        let mut c = core();
        c.on_orderbook(&book("up", vec![], vec![(0.62, 25.0)]));
        let comp = TradeTick {
            exchange: Exchange::Polymarket, symbol: "up".into(), exchange_trade_id: None, price: 0.62,
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
            exchange_timestamp_ns: 2,
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
            exchange_timestamp_ns: 1,
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
    fn matched_cant_cancel_replays_exact_maker_fill_idempotently() {
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
        assert_eq!(u.liquidity, Some(Liquidity::Maker));
        assert_eq!(u.filled_quantity, fills[0].filled_quantity);
        assert_eq!(u.avg_fill_price, fills[0].avg_fill_price);
        assert_eq!(u.side, fills[0].side);
        assert_eq!(u.symbol, fills[0].symbol);
        assert_eq!(c.matched_cant_cancel, 1);

        // A duplicate cancel is another replay of the same immutable trade
        // tuple. Downstream trade-id dedupe must see no invariant change.
        let duplicate = c.cancel_order(Exchange::Polymarket, "a", 300);
        assert_eq!(duplicate.trade_id, u.trade_id);
        assert_eq!(duplicate.liquidity, u.liquidity);
        assert_eq!(duplicate.filled_quantity, u.filled_quantity);
        assert_eq!(duplicate.avg_fill_price, u.avg_fill_price);
        assert_eq!(duplicate.side, u.side);
        assert_eq!(duplicate.symbol, u.symbol);
        assert_eq!(c.matched_cant_cancel, 2);
    }

    #[test]
    fn matched_cant_cancel_replays_latest_fragment_not_order_cumulative() {
        let mut c = core();
        c.on_orderbook(&book("up", vec![(0.60, 50.0)], vec![(0.62, 80.0)]));
        let _ = c.submit_order(&order("a", "up", Side::Buy, 0.60, 10.0, true, OrderType::Limit), 1);

        // The first trade drains the 50 shares ahead and fills four; the next
        // trade fills the remaining six under a distinct trade id.
        let first = c.on_trade_tick(&trade_ts("up", Side::Sell, 0.60, 54.0, 100));
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].status, OrderStatus::PartiallyFilled);
        assert!((first[0].filled_quantity - 4.0).abs() < 1e-9);
        let final_fill = c.on_trade_tick(&trade_ts("up", Side::Sell, 0.60, 6.0, 150));
        assert_eq!(final_fill.len(), 1);
        assert_eq!(final_fill[0].status, OrderStatus::Filled);
        assert!((final_fill[0].filled_quantity - 6.0).abs() < 1e-9);

        let replay = c.cancel_order(Exchange::Polymarket, "a", 200);
        assert_eq!(replay.trade_id, final_fill[0].trade_id);
        assert_eq!(replay.liquidity, Some(Liquidity::Maker));
        assert_eq!(replay.filled_quantity, final_fill[0].filled_quantity);
        assert_ne!(replay.trade_id, first[0].trade_id);
    }

    #[test]
    fn matched_cant_cancel_preserves_taker_role() {
        let mut c = core();
        c.on_orderbook(&book("up", vec![(0.60, 50.0)], vec![(0.62, 80.0)]));
        let fill = c.submit_order(
            &order("t", "up", Side::Buy, 0.62, 10.0, false, OrderType::Limit),
            100,
        );
        assert_eq!(fill.status, OrderStatus::Filled);
        assert_eq!(fill.liquidity, Some(Liquidity::Taker));

        let replay = c.cancel_order(Exchange::Polymarket, "t", 200);
        assert_eq!(replay.status, OrderStatus::Filled);
        assert_eq!(replay.trade_id, fill.trade_id);
        assert_eq!(replay.liquidity, Some(Liquidity::Taker));
        assert_eq!(replay.filled_quantity, fill.filled_quantity);
        assert_eq!(replay.avg_fill_price, fill.avg_fill_price);
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
    fn dynamic_ahead_frac_blends_fixed_override_to_queue_position() {
        let remaining = |strength: f64| {
            let mut c = core();
            c.configure(Some(1.0), 2_000_000_000);
            c.configure_dynamic_ahead_frac(strength);
            c.on_orderbook(&book("up", vec![(0.60, 100.0)], vec![(0.62, 100.0)]));
            let _ = c.submit_order(
                &order("a", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit),
                1,
            );
            // q_ahead 100→50 via trade. Then level 100→30 means 20 cancels.
            assert!(c
                .on_trade_tick(&trade("up", Side::Sell, 0.60, 50.0))
                .is_empty());
            c.on_orderbook(&book("up", vec![(0.60, 30.0)], vec![(0.62, 100.0)]));
            c.orders.get("a").unwrap().q_ahead
        };
        assert!((remaining(0.0) - 30.0).abs() < 1e-9, "fixed af=1");
        assert!((remaining(0.5) - 35.0).abs() < 1e-9, "half blend af=.75");
        assert!((remaining(1.0) - 40.0).abs() < 1e-9, "dynamic af=.50");
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
    fn maker_order_audit_attributes_book_through_fill_separately() {
        let mut c = core();
        c.configure_maker_order_audit(true);
        c.configure_book_through(1.0);
        c.on_orderbook(&book("up", vec![(0.55, 100.0)], vec![(0.57, 100.0)]));
        let accepted = c.submit_order(
            &order("a", "up", Side::Buy, 0.55, 10.0, true, OrderType::Limit),
            1,
        );
        assert_eq!(accepted.status, OrderStatus::Accepted);
        assert!(c.on_trade_tick(&trade("up", Side::Sell, 0.55, 10.0)).is_empty());
        let fills = c.on_orderbook(&book("up", vec![(0.55, 100.0)], vec![(0.54, 200.0)]));
        assert_eq!(fills.len(), 1);

        let rows = c.maker_order_audit_rows();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.candidate_qty, 0.0);
        assert_eq!(row.book_through_candidate_qty, 10.0);
        assert_eq!(row.book_through_fill_qty, 10.0);
        assert_eq!(row.fill_qty, 10.0);
        assert_eq!(row.q_ahead_final, 0.0);
        assert_eq!(row.remaining_final, 0.0);
    }

    #[test]
    fn unexplained_depletion_is_result_neutral_without_self_depth_credit() {
        let mut c = core();
        c.configure(Some(1.0), 2_000_000_000);
        c.configure_unexplained_depletion_execution(1.0);
        c.on_orderbook(&book("up", vec![(0.55, 10.0)], vec![(0.57, 100.0)]));
        let accepted = c.submit_order(
            &order("a", "up", Side::Buy, 0.55, 5.0, true, OrderType::Limit),
            1,
        );
        assert_eq!(accepted.status, OrderStatus::Accepted);

        // The ten-share depletion only consumes the ten-share public queue.
        // With no leave-one-out credit there is no causal overflow to fill us.
        let fills = c.on_orderbook(&book(
            "up",
            vec![(0.54, 10.0)],
            vec![(0.56, 100.0)],
        ));
        assert!(fills.is_empty());
        assert_eq!(c.orders["a"].q_ahead, 0.0);
        assert_eq!(c.orders["a"].remaining, 5.0);
    }

    #[test]
    fn replay_self_depth_and_hidden_depletion_produce_bounded_partial_fill() {
        let mut c = core();
        c.configure(Some(1.0), 2_000_000_000);
        c.configure_maker_order_audit(true);
        c.configure_replay_self_depth(1.0);
        c.configure_unexplained_depletion_execution(0.25);
        c.on_orderbook(&book("up", vec![(0.55, 10.0)], vec![(0.57, 100.0)]));
        let accepted = c.submit_order(
            &order("a", "up", Side::Buy, 0.55, 5.0, true, OrderType::Limit),
            1,
        );
        assert_eq!(accepted.status, OrderStatus::Accepted);

        // q_init = raw 10 - own-tape credit 5 = 5. The new book moves the mid
        // one tick against this bid, fully opening the causal adverse gate. The
        // raw ten-share depletion therefore contains 2.5 hidden-execution
        // shares and 7.5 cancels. At ahead_frac=1 the total reaches past q by
        // five, but the fill remains capped by the 2.5 execution shares.
        let fills = c.on_orderbook(&book(
            "up",
            vec![(0.54, 10.0)],
            vec![(0.56, 100.0)],
        ));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].status, OrderStatus::PartiallyFilled);
        assert_eq!(fills[0].filled_quantity, 2.5);
        assert_eq!(fills[0].remaining_quantity, 2.5);
        assert_eq!(c.orders["a"].q_ahead, 0.0);
        assert_eq!(c.orders["a"].replay_self_depth_credit, 0.0);

        let row = &c.maker_order_audit_rows()[0];
        assert_eq!(row.replay_self_depth_credit, 5.0);
        assert_eq!(row.depletion_observed_qty, 10.0);
        assert_eq!(row.depletion_exec_qty, 2.5);
        assert_eq!(row.depletion_cancel_advance_qty, 7.5);
        assert_eq!(row.depletion_candidate_qty, 2.5);
        assert_eq!(row.depletion_fill_qty, 2.5);
        assert_eq!(row.fill_qty, 2.5);
    }

    #[test]
    fn replay_self_depth_without_execution_only_advances_queue() {
        let mut c = core();
        c.configure(Some(1.0), 2_000_000_000);
        c.configure_replay_self_depth(1.0);
        c.on_orderbook(&book("up", vec![(0.55, 10.0)], vec![(0.57, 100.0)]));
        let accepted = c.submit_order(
            &order("a", "up", Side::Buy, 0.55, 5.0, true, OrderType::Limit),
            1,
        );
        assert_eq!(accepted.status, OrderStatus::Accepted);
        assert_eq!(c.orders["a"].q_ahead, 5.0);

        let fills = c.on_orderbook(&book("up", vec![], vec![(0.57, 100.0)]));
        assert!(fills.is_empty());
        assert_eq!(c.orders["a"].q_ahead, 0.0);
        assert_eq!(c.orders["a"].remaining, 5.0);
        assert_eq!(c.orders["a"].replay_self_depth_credit, 0.0);
    }

    #[test]
    fn favorable_depletion_remains_cancel_only() {
        let mut c = core();
        c.configure(Some(1.0), 2_000_000_000);
        c.configure_replay_self_depth(1.0);
        c.configure_unexplained_depletion_execution(1.0);
        c.on_orderbook(&book("up", vec![(0.55, 10.0)], vec![(0.57, 100.0)]));
        let accepted = c.submit_order(
            &order("a", "up", Side::Buy, 0.55, 5.0, true, OrderType::Limit),
            1,
        );
        assert_eq!(accepted.status, OrderStatus::Accepted);

        // The old bid level disappears while the mid moves up, in our favor.
        // This is consistent with cancellation/repricing, not an adverse maker
        // execution, so it may advance q but must not fill.
        let fills = c.on_orderbook(&book(
            "up",
            vec![(0.56, 10.0)],
            vec![(0.57, 100.0)],
        ));
        assert!(fills.is_empty());
        assert_eq!(c.orders["a"].q_ahead, 0.0);
        assert_eq!(c.orders["a"].remaining, 5.0);
        assert_eq!(c.orders["a"].replay_self_depth_credit, 0.0);
    }

    #[test]
    fn depletion_fill_then_cancel_replays_the_same_fill_idempotently() {
        let mut c = core();
        c.configure(Some(1.0), 2_000_000_000);
        c.configure_maker_order_audit(true);
        c.configure_replay_self_depth(1.0);
        c.configure_unexplained_depletion_execution(1.0);
        c.on_orderbook(&book_ts(
            "up",
            vec![(0.55, 5.0)],
            vec![(0.57, 100.0)],
            10,
        ));
        let accepted = c.submit_order(
            &order("a", "up", Side::Buy, 0.55, 5.0, true, OrderType::Limit),
            11,
        );
        assert_eq!(accepted.status, OrderStatus::Accepted);

        let fills = c.on_orderbook(&book_ts(
            "up",
            vec![(0.54, 10.0)],
            vec![(0.56, 100.0)],
            100,
        ));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].status, OrderStatus::Filled);
        let trade_id = fills[0].trade_id.clone();

        let cancel = c.cancel_order(Exchange::Polymarket, "a", 150);
        assert_eq!(cancel.status, OrderStatus::Filled);
        assert_eq!(cancel.trade_id, trade_id);
        assert_eq!(cancel.filled_quantity, 5.0);
        assert_eq!(c.matched_cant_cancel, 1);
        assert_eq!(c.maker_order_audit_rows()[0].cancel_result, "matched_before_cancel");
    }

    #[test]
    fn inferred_depletion_fill_retains_dust_across_aggregate_public_trade() {
        let mut c = core();
        c.configure(Some(1.0), 2_000_000_000);
        c.configure_maker_order_audit(true);
        c.configure_replay_self_depth(1.0);
        c.configure_unexplained_depletion_execution(1.0);
        c.configure_inferred_maker_residual(1.0, 0.001);
        c.on_orderbook(&book_ts(
            "up",
            vec![(0.55, 5.0)],
            vec![(0.57, 100.0)],
            10,
        ));
        assert_eq!(
            c.submit_order(
                &order("dust", "up", Side::Buy, 0.55, 5.0, true, OrderType::Limit),
                11,
            )
            .status,
            OrderStatus::Accepted
        );

        let inferred = c.on_orderbook(&book_ts(
            "up",
            vec![(0.54, 10.0)],
            vec![(0.56, 100.0)],
            100,
        ));
        assert_eq!(inferred.len(), 1);
        assert_eq!(inferred[0].status, OrderStatus::PartiallyFilled);
        assert!((inferred[0].filled_quantity - 4.995).abs() < EPS);
        assert!((inferred[0].remaining_quantity - 0.005).abs() < EPS);
        assert!((c.orders["dust"].remaining - 0.005).abs() < EPS);
        assert_eq!(c.inferred_maker_residual_orders_n, 1);
        assert!((c.inferred_maker_residual_qty - 0.005).abs() < EPS);

        // An aggregate public print cannot prove that this individual dust
        // allocation completed, so it remains physically resting.
        let explicit = c.on_trade_tick(&trade_ts(
            "up",
            Side::Sell,
            0.55,
            1.0,
            150,
        ));
        assert!(explicit.is_empty());
        assert!((c.orders["dust"].remaining - 0.005).abs() < EPS);
        assert_eq!(c.inferred_maker_residual_orders_n, 1);
        assert!((c.inferred_maker_residual_qty - 0.005).abs() < EPS);

        let cancelled = c.cancel_order(Exchange::Polymarket, "dust", 200);
        assert_eq!(cancelled.status, OrderStatus::Cancelled);
        assert!(!c.orders.contains_key("dust"));

        let row = &c.maker_order_audit_rows()[0];
        assert!((row.inferred_residual_floor - 0.005).abs() < EPS);
        assert!((row.inferred_residual_suppressed_qty - 0.005).abs() < EPS);
        assert!((row.fill_qty - 4.995).abs() < EPS);
    }

    #[test]
    fn depletion_fill_is_isolated_to_the_owning_instance() {
        let mut balances = HashMap::new();
        balances.insert("iid-a".to_string(), 100.0);
        balances.insert("iid-b".to_string(), 100.0);
        let mut c = SimExchangeV2::new(500_000_000, balances, HashMap::new());
        c.on_instrument(&binary_instrument());
        c.configure(Some(1.0), 2_000_000_000);
        c.configure_replay_self_depth(1.0);
        c.configure_unexplained_depletion_execution(1.0);
        c.on_orderbook(&book(
            "up",
            vec![(0.55, 5.0), (0.54, 5.0)],
            vec![(0.57, 100.0)],
        ));

        let mut a = order("a", "up", Side::Buy, 0.55, 5.0, true, OrderType::Limit);
        a.instance_id = "iid-a".into();
        let mut b = order("b", "up", Side::Buy, 0.54, 5.0, true, OrderType::Limit);
        b.instance_id = "iid-b".into();
        assert_eq!(c.submit_order(&a, 1).status, OrderStatus::Accepted);
        assert_eq!(c.submit_order(&b, 1).status, OrderStatus::Accepted);

        let fills = c.on_orderbook(&book(
            "up",
            vec![(0.54, 5.0)],
            vec![(0.56, 100.0)],
        ));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].client_order_id, "a");
        assert_eq!(c.wallet_usdc_raw("iid-a"), Some(97.25));
        assert_eq!(c.wallet_usdc_raw("iid-b"), Some(100.0));
        assert!(!c.orders.contains_key("a"));
        assert_eq!(c.orders["b"].remaining, 5.0);
        assert_eq!(c.orders["b"].q_ahead, 0.0);
    }

    #[test]
    fn same_level_depletion_execution_budget_fills_once_in_arrival_order() {
        let mut c = core();
        c.configure(Some(1.0), 2_000_000_000);
        c.configure_maker_order_audit(true);
        c.configure_replay_self_depth(1.0);
        c.configure_unexplained_depletion_execution(1.0);
        c.on_orderbook(&book(
            "up",
            vec![(0.55, 5.0)],
            vec![(0.57, 100.0)],
        ));

        // Reverse lexical coids prove allocation follows exchange arrival,
        // not BTreeMap/coid order. Both approximate leave-one-out queues reach
        // zero, but the public level supplied only five execution shares.
        assert_eq!(
            c.submit_order(
                &order("z-older", "up", Side::Buy, 0.55, 5.0, true, OrderType::Limit),
                1,
            )
            .status,
            OrderStatus::Accepted
        );
        assert_eq!(
            c.submit_order(
                &order("a-newer", "up", Side::Buy, 0.55, 5.0, true, OrderType::Limit),
                2,
            )
            .status,
            OrderStatus::Accepted
        );

        let fills = c.on_orderbook(&book(
            "up",
            vec![(0.54, 10.0)],
            vec![(0.56, 100.0)],
        ));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].client_order_id, "z-older");
        assert_eq!(fills[0].filled_quantity, 5.0);
        assert!(!c.orders.contains_key("z-older"));
        assert_eq!(c.orders["a-newer"].remaining, 5.0);
        assert_eq!(c.orders["a-newer"].q_ahead, 0.0);

        let newer = c
            .maker_order_audit_rows()
            .into_iter()
            .find(|row| row.coid == "a-newer")
            .unwrap();
        assert_eq!(newer.depletion_candidate_qty, 5.0);
        assert_eq!(newer.depletion_budget_suppressed_qty, 5.0);
        assert_eq!(newer.depletion_fill_qty, 0.0);
    }

    /// Focused hot-section evidence for changes to `resync_queues`. Run with:
    /// `cargo test --release -p hexagent-exchange benchmark_orderbook_resync_latency -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn benchmark_orderbook_resync_latency() {
        fn sample(rate: f64, n: usize) -> Vec<u64> {
            let mut c = core();
            c.configure(Some(1.0), 2_000_000_000);
            c.configure_unexplained_depletion_execution(rate);
            c.on_orderbook(&book_ts(
                "up",
                vec![(0.55, 10.0)],
                vec![(0.57, 100.0)],
                1,
            ));
            assert_eq!(
                c.submit_order(
                    &order("a", "up", Side::Buy, 0.55, 5.0, true, OrderType::Limit),
                    2,
                )
                .status,
                OrderStatus::Accepted
            );

            let mut samples = Vec::with_capacity(n);
            for i in 0..(n + 2_000) {
                // Restore the same ten-share public queue before every measured
                // adverse level removal. All setup/allocation stays outside the
                // timed section; no fill is possible because q_before equals the
                // entire causal depletion.
                let base_ts = 3 + (i as u64) * 2;
                let down = book_ts(
                    "up",
                    vec![(0.54, 10.0)],
                    vec![(0.56, 100.0)],
                    base_ts,
                );
                let restored = book_ts(
                    "up",
                    vec![(0.55, 10.0)],
                    vec![(0.57, 100.0)],
                    base_ts + 1,
                );
                let resting = c.orders.get_mut("a").expect("benchmark order remains resting");
                resting.q_ahead = 10.0;
                resting.level_qty_at_sync = 10.0;
                resting.mid_at_sync = 0.56;
                resting.traded_since_sync = 0.0;

                let start = std::time::Instant::now();
                let updates = c.on_orderbook(&down);
                let elapsed = start.elapsed().as_nanos() as u64;
                std::hint::black_box(updates);
                std::hint::black_box(c.on_orderbook(&restored));
                if i >= 2_000 {
                    samples.push(elapsed);
                }
            }
            samples
        }

        fn describe(label: &str, mut samples: Vec<u64>) {
            samples.sort_unstable();
            let at = |fraction: f64| {
                samples[((samples.len() - 1) as f64 * fraction) as usize]
            };
            println!(
                "SIMV2_RESYNC_BENCH profile={label} n={} median_ns={} p99_ns={} p999_ns={} max_ns={}",
                samples.len(),
                at(0.5),
                at(0.99),
                at(0.999),
                samples[samples.len() - 1]
            );
        }

        const N: usize = 100_000;
        describe("disabled", sample(0.0, N));
        describe("adverse_depletion", sample(0.1, N));
    }

    /// Focused evidence for the two new matching paths. Boundaries:
    /// - trade: `on_trade_tick` entry through returned strategy updates;
    /// - queue lifecycle: two place admissions plus FIFO-head cancel and tail
    ///   cancel;
    /// - book markout: `on_orderbook_fwd` through returned strategy updates;
    /// - taker self-clean: `submit_order` through its returned update.
    /// Active order depth is one or two as reported; no message queue is used.
    /// Run with `cargo test --release -p hexagent-exchange benchmark_causal_maker_models_latency -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn benchmark_causal_maker_models_latency() {
        fn describe(label: &str, mut samples: Vec<u64>, max_depth: usize) {
            samples.sort_unstable();
            let at = |fraction: f64| {
                samples[((samples.len() - 1) as f64 * fraction) as usize]
            };
            println!(
                "SIMV2_CAUSAL_BENCH profile={label} n={} median_ns={} p99_ns={} p999_ns={} max_ns={} max_order_depth={} overflow=0 boundary={}",
                samples.len(),
                at(0.5),
                at(0.99),
                at(0.999),
                samples[samples.len() - 1],
                max_depth,
                if label.starts_with("trade") {
                    "on_trade_tick_to_updates"
                } else if label.starts_with("queue") {
                    "two_places_and_two_cancels"
                } else if label.starts_with("book") {
                    "on_orderbook_fwd_to_updates"
                } else {
                    "submit_taker_to_update"
                }
            );
        }

        fn sample_trade(toxicity: f64, n: usize) -> Vec<u64> {
            let mut c = core();
            c.configure_maker_toxicity(toxicity, 1.0);
            c.on_orderbook(&book("up", vec![(0.60, 10.0)], vec![(0.62, 80.0)]));
            assert_eq!(
                c.submit_order(
                    &order("maker", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit),
                    1,
                )
                .status,
                OrderStatus::Accepted
            );
            // One favorable tick since immutable entry_mid.
            c.on_orderbook(&book("up", vec![(0.60, 10.0)], vec![(0.64, 80.0)]));
            let t = trade("up", Side::Sell, 0.60, 1.0);
            let mut samples = Vec::with_capacity(n);
            for i in 0..(n + 2_000) {
                let resting = c.orders.get_mut("maker").unwrap();
                resting.q_ahead = 0.0;
                resting.own_q_ahead = 0.0;
                resting.remaining = 5.0;
                resting.locked_usdc = 3.0;
                let start = std::time::Instant::now();
                let updates = c.on_trade_tick(&t);
                let elapsed = start.elapsed().as_nanos() as u64;
                std::hint::black_box(updates);
                if i >= 2_000 {
                    samples.push(elapsed);
                }
            }
            samples
        }

        fn sample_queue_lifecycle(strength: f64, n: usize) -> Vec<u64> {
            let mut c = core();
            c.configure_order_queue_position(strength);
            c.on_orderbook(&book("up", vec![(0.60, 10.0)], vec![(0.62, 80.0)]));
            let older = order("older", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit);
            let later = order("later", "up", Side::Buy, 0.60, 5.0, true, OrderType::Limit);
            let mut samples = Vec::with_capacity(n);
            for i in 0..(n + 2_000) {
                let start = std::time::Instant::now();
                std::hint::black_box(c.submit_order(&older, 1));
                std::hint::black_box(c.submit_order(&later, 2));
                std::hint::black_box(c.cancel_order(Exchange::Polymarket, "older", 3));
                std::hint::black_box(c.cancel_order(Exchange::Polymarket, "later", 4));
                let elapsed = start.elapsed().as_nanos() as u64;
                debug_assert!(c.orders.is_empty());
                if i >= 2_000 {
                    samples.push(elapsed);
                }
            }
            samples
        }

        fn sample_book_markout(strength: f64, n: usize) -> Vec<u64> {
            let mut c = core();
            c.configure(Some(1.0), 2_000_000_000);
            c.configure_replay_self_depth(1.0);
            c.configure_unexplained_depletion_execution(1.0);
            c.configure_book_fill_markout_vn(strength);
            c.on_orderbook(&book("up", vec![(0.55, 5.0)], vec![(0.57, 100.0)]));
            assert_eq!(
                c.submit_order(
                    &order("maker", "up", Side::Buy, 0.55, 10.0, true, OrderType::Limit),
                    1,
                )
                .status,
                OrderStatus::Accepted
            );
            let depleted = book("up", vec![(0.54, 10.0)], vec![(0.56, 100.0)]);
            let mut samples = Vec::with_capacity(n);
            for i in 0..(n + 2_000) {
                let start = std::time::Instant::now();
                let updates = c.on_orderbook_fwd(&depleted, Some(0.57));
                let elapsed = start.elapsed().as_nanos() as u64;
                std::hint::black_box(updates);
                if i >= 2_000 {
                    samples.push(elapsed);
                }

                // Reset outside the measured boundary. The order remains
                // active because each synthetic depletion fills only half.
                c.books.update(
                    "up",
                    vec![PriceLevel { price: 0.55, quantity: 5.0 }],
                    vec![PriceLevel { price: 0.57, quantity: 100.0 }],
                );
                let resting = c.orders.get_mut("maker").unwrap();
                resting.remaining = 10.0;
                resting.locked_usdc = 5.5;
                resting.q_ahead = 0.0;
                resting.own_q_ahead = 0.0;
                resting.level_qty_at_sync = 5.0;
                resting.mid_at_sync = 0.56;
                resting.traded_since_sync = 0.0;
                resting.replay_self_depth_credit = 0.0;
            }
            samples
        }

        fn sample_taker_self_clean(strength: f64, n: usize) -> Vec<u64> {
            let mut c = core();
            c.configure_replay_self_taker_depth(strength);
            c.on_orderbook(&book("up", vec![(0.60, 80.0)], vec![(0.62, 80.0)]));
            assert_eq!(
                c.submit_order(
                    &order("maker", "up", Side::Sell, 0.62, 10.0, true, OrderType::Limit),
                    1,
                )
                .status,
                OrderStatus::Accepted
            );
            let taker = order("taker", "up", Side::Buy, 0.62, 5.0, false, OrderType::Limit);
            let mut samples = Vec::with_capacity(n);
            for i in 0..(n + 2_000) {
                let start = std::time::Instant::now();
                let update = c.submit_order(&taker, i as u64 + 2);
                let elapsed = start.elapsed().as_nanos() as u64;
                std::hint::black_box(update);
                if i >= 2_000 {
                    samples.push(elapsed);
                }
            }
            samples
        }

        const N: usize = 50_000;
        describe("trade_disabled", sample_trade(0.0, N), 1);
        describe("trade_toxicity_0p5", sample_trade(0.5, N), 1);
        describe("queue_disabled", sample_queue_lifecycle(0.0, N), 2);
        describe("queue_fifo_1p0", sample_queue_lifecycle(1.0, N), 2);
        describe("book_markout_disabled", sample_book_markout(0.0, N), 1);
        describe("book_markout_0p78", sample_book_markout(0.78, N), 1);
        describe("taker_self_clean_disabled", sample_taker_self_clean(0.0, N), 1);
        describe("taker_self_clean_1p0", sample_taker_self_clean(1.0, N), 1);
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

    #[test]
    fn book_fill_markout_reprices_depletion_without_changing_quantity() {
        let mut c = core();
        c.configure(Some(1.0), 2_000_000_000);
        c.configure_maker_order_audit(true);
        c.configure_replay_self_depth(1.0);
        c.configure_unexplained_depletion_execution(0.25);
        c.configure_book_fill_markout_vn(1.0);
        c.on_orderbook(&book("up", vec![(0.55, 10.0)], vec![(0.57, 100.0)]));
        assert_eq!(
            c.submit_order(
                &order("a", "up", Side::Buy, 0.55, 5.0, true, OrderType::Limit),
                1,
            )
            .status,
            OrderStatus::Accepted
        );

        // Same causal 2.5-share hidden-depletion fill as the disabled model.
        // The independent future mid is two cents favorable, so vn=1 settles
        // the fragment at 0.57 while leaving quantity and real resting limit
        // state unchanged.
        let fills = c.on_orderbook_fwd(
            &book("up", vec![(0.54, 10.0)], vec![(0.56, 100.0)]),
            Some(0.57),
        );
        assert_eq!(fills.len(), 1);
        assert!((fills[0].filled_quantity - 2.5).abs() < 1e-9);
        assert!((fills[0].remaining_quantity - 2.5).abs() < 1e-9);
        assert!((fills[0].avg_fill_price - 0.57).abs() < 1e-9);
        assert!((c.orders["a"].request.price.unwrap() - 0.55).abs() < 1e-9);

        assert_eq!(c.book_fill_haircut_n, 1);
        assert!((c.book_fill_haircut_qty - 2.5).abs() < 1e-9);
        assert!((c.book_fill_haircut_cost_usdc - 0.05).abs() < 1e-9);
        let row = &c.maker_order_audit_rows()[0];
        assert!((row.book_markout_qty - 2.5).abs() < 1e-9);
        assert!((row.book_markout_cost_usdc - 0.05).abs() < 1e-9);
    }

    #[test]
    fn book_fill_markout_covers_book_through_and_disabled_path() {
        let probe = |strength: f64, fwd_mid: Option<f64>| {
            let mut c = core();
            c.configure_book_through(1.0);
            c.configure_book_fill_markout_vn(strength);
            c.on_orderbook(&book("up", vec![(0.55, 100.0)], vec![(0.57, 100.0)]));
            assert_eq!(
                c.submit_order(
                    &order("a", "up", Side::Buy, 0.55, 10.0, true, OrderType::Limit),
                    1,
                )
                .status,
                OrderStatus::Accepted
            );
            assert!(c.on_trade_tick(&trade("up", Side::Sell, 0.55, 10.0)).is_empty());
            let fills = c.on_orderbook_fwd(
                &book("up", vec![(0.55, 100.0)], vec![(0.54, 200.0)]),
                fwd_mid,
            );
            (c, fills)
        };

        let (enabled, fills) = probe(1.0, Some(0.57));
        assert_eq!(fills.len(), 1);
        assert!((fills[0].filled_quantity - 10.0).abs() < 1e-9);
        assert!((fills[0].avg_fill_price - 0.57).abs() < 1e-9);
        assert_eq!(enabled.book_fill_haircut_n, 1);
        assert!((enabled.book_fill_haircut_cost_usdc - 0.20).abs() < 1e-9);

        let (disabled, fills) = probe(0.0, Some(0.57));
        assert!((fills[0].avg_fill_price - 0.55).abs() < 1e-9);
        assert_eq!(disabled.book_fill_haircut_n, 0);
        assert_eq!(disabled.book_fill_haircut_qty, 0.0);

        let (_, adverse) = probe(1.0, Some(0.54));
        assert!((adverse[0].avg_fill_price - 0.55).abs() < 1e-9);
    }

    #[test]
    fn maker_markout_reprice_preserves_folded_penalty_direction() {
        // Original DOWN sell @0.40 is canonical UP buy @0.60. A favorable UP
        // move to 0.61 must reduce the original sell settlement to 0.39.
        let (price, penalty) = maker_markout_reprice(
            0.40,
            Side::Sell,
            Side::Buy,
            0.60,
            Some(0.61),
            1.0,
        );
        assert!((price - 0.39).abs() < 1e-9);
        assert!((penalty - 0.01).abs() < 1e-9);
    }
}
