use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use log;

use crate::types::{Exchange, OrderRequest, OrderStatus, OrderType, OrderUpdate, Side, Signal};

/// Global auto-increment counter for client_order_id (unique across all OrderManagers).
/// Initialized from millisecond timestamp to avoid collisions across restarts.
static GLOBAL_ORDER_ID: AtomicU64 = AtomicU64::new(0);

/// Initialize the global order ID counter from current timestamp (call once at startup).
pub fn init_global_order_id() {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    GLOBAL_ORDER_ID.store(now_ms, Ordering::Relaxed);
}

/// Deterministically (re)seed the global order-id counter — backtest only.
///
/// Live uses `init_global_order_id` (wall-clock ms) so coids never collide
/// across process restarts. A backtest instead needs BYTE-IDENTICAL coids
/// across runs: client_order_ids key the sim's per-order state (resting-order
/// and recent-fill maps), so a wall-clock coid base reshuffles run-to-run →
/// edge/vol noise that swamps calibration. Call once at the start of the
/// backtest with a fixed seed; vary the seed to generate independent
/// reproducible replicates.
pub fn init_global_order_id_seeded(seed: u64) {
    GLOBAL_ORDER_ID.store(seed, Ordering::Relaxed);
}

/// Local order status tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalOrderStatus {
    /// Order submitted, awaiting exchange acknowledgement.
    Submitted,
    /// Exchange accepted or partially filled — order is live.
    Active,
    /// Cancel request sent, awaiting confirmation.
    Cancelling,
    /// Exchange explicitly rejected the submission (not an HTTP / timeout
    /// error — see `OrderStatus::Rejected`). Kept in the map so subsequent
    /// `refresh()` cycles don't try to cancel it; filtered out of all
    /// active-order / locked-qty queries.
    Rejected,
}

/// Outcome of [`OrderManager::on_signal_dropped`] — tells the caller how the
/// executor-dropped (stale) signal was classified so it can keep its own
/// pending-order / lock ledger (e.g. `PositionManager::pending_orders`)
/// consistent with the OM's view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DroppedSignalOutcome {
    /// The coid was a `Submitted` placement whose NewOrder signal never
    /// reached the exchange — it has been removed from tracking. The order
    /// does NOT exist on the book; the caller should release its lock.
    PlaceRemoved,
    /// The coid was a `Cancelling` order whose CancelOrder signal never
    /// reached the exchange — the order is STILL LIVE on the book, so it
    /// has been reverted to `Active` and the next `refresh` will re-emit
    /// the cancel. The caller MUST keep its pending lock (the order rests).
    CancelReverted,
    /// The coid is not tracked by this OM (wrong OM, already terminal, or
    /// the OM was torn down). Caller falls back to its own heuristic.
    NotTracked,
}

/// A locally tracked order.
#[derive(Debug, Clone)]
pub struct LocalOrder {
    pub client_order_id: String,
    pub symbol: String,
    pub side: Side,
    pub price: f64,
    pub quantity: f64,
    pub status: LocalOrderStatus,
    /// Wall-clock timestamp (ns) when this order was inserted into the
    /// local tracker. Used by the diagnostic log in `refresh()` to show
    /// order age when the open-count exceeds the expected-healthy
    /// threshold — helps distinguish stale accumulation (old ages) from
    /// a legitimate burst of fresh orders.
    pub created_ns: u64,
    /// Cumulative filled qty, keyed by `trade_id` for dedup across the
    /// MATCHED → MINED → CONFIRMED push sequence (each carries the same
    /// `matched_amount`). Summing values gives true cumulative fills; the
    /// order is removed from tracking once this reaches `quantity`, so
    /// `refresh()` won't emit a doomed cancel against an order the server
    /// already fully matched.
    pub filled_by_trade: HashMap<String, f64>,
}

/// Manages local order state for a single outcome (symbol) and generates
/// signals to converge toward desired quotes.
///
/// Tracks all open orders locally and compares against the desired quote list
/// on each `refresh()` call. Only emits cancel/new signals for orders that
/// actually need to change — orders at the same price+qty are kept as-is.
pub struct OrderManager {
    exchange: Exchange,
    symbol: String,
    tick_size: f64,
    instance_id: String,
    /// Fee rate in basis points (Polymarket market-specific).
    fee_rate_bps: u32,
    /// Exchange-enforced minimum order size in shares. Desired levels with
    /// `quantity < min_order_size` are silently dropped in `reconcile_side`
    /// instead of being sent (Polymarket returns a 400 "Size (X) lower than
    /// the minimum: Y" otherwise — observed 151× in 2026-05-04 live when
    /// inventory-cap math left a 4-share remainder against a 5-share min).
    /// 0.0 means no filter (default for exchanges/strategies that don't
    /// configure a minimum).
    min_order_size: f64,
    /// Exchange-enforced minimum NOTIONAL (price × qty in quote currency)
    /// for marketable BUY orders. Polymarket rejects marketable BUYs (i.e.
    /// FAK or non-post-only Limits that cross the book) with notional
    /// below $1 — observed 14× in the 2026-05-06 8h22m live run when the
    /// strategy's per-tick notional gate (`MIN_MARKETABLE_NOTIONAL` in
    /// the routing logic) used the quoter's ORIGINAL price but the price
    /// was subsequently lowered by `adjust_buy` to maintain post-only,
    /// dropping notional under $1. Defense-in-depth: this filter at the
    /// reconcile boundary catches any leak regardless of how / where the
    /// strategy adjusted price downstream of its own gate.
    /// 0.0 means no filter (default).
    min_marketable_notional: f64,
    /// Requote hysteresis band, in **ticks**. In `reconcile_side`, a resting
    /// order is kept (no cancel+replace) while the desired quote price is
    /// within this band of it. `0.0` (default) reduces the match to exact
    /// tick-grid equality (`price_eq`) — BIT-IDENTICAL to the behaviour before
    /// this knob existed. `N ≥ 2` suppresses sub-N-tick requote churn: at
    /// ~150 ms cadence the myindex feed wiggles the binary's fair value
    /// ~2 ticks/tick (mostly the unpredictable bulk of its variance), so
    /// chasing every wiggle cancels orders before they rest long enough to
    /// earn a maker fill. The band edge sits at `(N − 0.5)` ticks so an
    /// exactly-N-tick move requotes while an (N−1)-tick move is kept — robust
    /// to float error on the price grid.
    requote_min_ticks: f64,
    /// Local active-order ledger keyed by client_order_id.
    /// **`BTreeMap`** for deterministic iteration order — `active_order`
    /// uses `.find()`, `cancel_orders_by_side` collects in iter order,
    /// `reconcile_side`'s active-list ordering all leak HashMap's
    /// per-process random hash seed into BT outcomes if the underlying
    /// map were a `HashMap`. Same memory profile, O(log N) lookups
    /// (N is at most a handful of resting orders per OM).
    orders: std::collections::BTreeMap<String, LocalOrder>,
}

fn price_eq(a: f64, b: f64, tick_size: f64) -> bool {
    (a - b).abs() < tick_size / 2.0
}

/// A desired quote level: price + quantity on a given side.
#[derive(Debug, Clone)]
pub struct QuoteLevel {
    pub side: Side,
    pub price: f64,
    pub quantity: f64,
    /// If true, order is post-only (maker). If false, may cross spread (taker).
    pub post_only: bool,
    /// Polymarket order-type. Default `Limit` (= GTC on the wire).
    /// Use `Fak` for Fill-and-Kill taker-only orders (emitted when a
    /// crossing quote uses `taker_cross_use_fak`) — wire body uses
    /// `orderType="FAK"` so any unfilled portion is cancelled rather
    /// than resting. Read by `reconcile_side`'s marketable-notional
    /// filter (FAK BUYs are always marketable regardless of `post_only`).
    pub order_type: OrderType,
}

impl OrderManager {
    pub fn new(exchange: Exchange, symbol: String, tick_size: f64, instance_id: String) -> Self {
        Self {
            exchange,
            symbol,
            tick_size,
            instance_id,
            fee_rate_bps: 0,
            min_order_size: 0.0,
            min_marketable_notional: 0.0,
            requote_min_ticks: 0.0,
            orders: std::collections::BTreeMap::new(),
        }
    }

    /// Set fee rate in basis points (from instrument data).
    pub fn set_fee_rate_bps(&mut self, fee_rate_bps: u32) {
        self.fee_rate_bps = fee_rate_bps;
    }

    /// Set the exchange-enforced minimum order size (shares). Desired levels
    /// below this threshold will be silently dropped instead of submitted.
    /// Pass `0.0` (the default) to disable the filter.
    pub fn set_min_order_size(&mut self, min_order_size: f64) {
        self.min_order_size = min_order_size.max(0.0);
    }

    /// Set the exchange-enforced minimum NOTIONAL for marketable BUY orders
    /// (in quote currency, e.g. USDC for Polymarket). Marketable BUYs whose
    /// `price × quantity` falls below this threshold are silently dropped
    /// in `reconcile_side`. A BUY is "marketable" when it would (or could)
    /// cross the book — concretely: `!post_only` OR `order_type == Fak`.
    /// Resting (post-only Limit) BUYs are NOT subject to this filter
    /// because Polymarket only enforces the $1 minimum on marketable
    /// orders. Pass `0.0` (the default) to disable the filter.
    pub fn set_min_marketable_notional(&mut self, usdc: f64) {
        self.min_marketable_notional = usdc.max(0.0);
    }

    /// Set the requote hysteresis band in ticks (see field doc). `0.0` (the
    /// default) keeps exact tick-grid price matching — byte-identical to the
    /// pre-knob behaviour. Negative inputs are clamped to `0.0`.
    pub fn set_requote_min_ticks(&mut self, ticks: f64) {
        self.requote_min_ticks = ticks.max(0.0);
    }

    /// Inject an existing open order from the exchange (for startup sync).
    pub fn inject_open_order(&mut self, client_order_id: String, side: Side, price: f64, quantity: f64) {
        self.orders.insert(client_order_id.clone(), LocalOrder {
            client_order_id,
            symbol: self.symbol.clone(),
            side,
            price,
            quantity,
            status: LocalOrderStatus::Active,
            created_ns: crate::types::now_ns(),
            filled_by_trade: HashMap::new(),
        });
    }

    pub fn set_tick_size(&mut self, tick_size: f64) {
        self.tick_size = tick_size;
    }

    fn next_id(&self) -> String {
        GLOBAL_ORDER_ID.fetch_add(1, Ordering::Relaxed).to_string()
    }

    /// Update local order state from an exchange OrderUpdate.
    pub fn on_order_update(&mut self, update: &OrderUpdate) {
        let Some(order) = self.orders.get_mut(&update.client_order_id) else {
            return;
        };
        match update.status {
            OrderStatus::Accepted => {
                order.status = LocalOrderStatus::Active;
            }
            OrderStatus::PartiallyFilled => {
                order.status = LocalOrderStatus::Active;
                // Polymarket's trade push goes MATCHED → MINED → CONFIRMED
                // with the same `matched_amount` each time; we only get
                // the terminal `Filled` status on the CONFIRMED leg, which
                // can lag by seconds. In the meantime the order is already
                // fully matched at the server — trying to cancel it
                // triggers "matched orders can't be canceled" 4xx and
                // burns a round trip.
                //
                // Track cumulative fills here keyed by `trade_id` so
                // repeat events for the same trade don't double-count,
                // and eagerly remove the order as soon as fills reach
                // the original quantity.
                if let Some(tid) = update.trade_id.clone() {
                    order.filled_by_trade.insert(tid, update.filled_quantity);
                }
                let cumulative: f64 = order.filled_by_trade.values().sum();
                // Tolerance: Polymarket fills our maker leg at the *taker's*
                // price-improved level (e.g. ask 0.40 + price-improvement
                // → fill at 0.4000006), so the wire-level matched_amount
                // for our 5-share maker order can come back as
                // 1.66 + 3.333332 = 4.993332 — a 0.7-share-cent gap that
                // a `1e-9` epsilon never closes. The 2026-05-04 live run
                // had 9/49 cancel-after-matched cases driven by this:
                // the order was effectively fully matched on the server
                // but OM kept it as Active, so the strategy's next quote
                // tick re-emitted a Cancel that the server rejected with
                // "matched orders can't be canceled" — wasted RTT and a
                // misleading log. Use max(0.01 share, 0.5% of qty) to
                // close the rounding gap without prematurely retiring
                // genuine partial fills (smallest legitimate residual
                // we'd want to keep alive >> 0.01 share at typical
                // base_qty values).
                let tolerance = (order.quantity * 0.005).max(0.01);
                // Also respect a single-event "full fill" signal when
                // there's no trade_id to dedup by (best-effort fallback).
                let single_event_full = update.trade_id.is_none()
                    && update.filled_quantity >= order.quantity - tolerance;
                if cumulative >= order.quantity - tolerance || single_event_full {
                    log::debug!(
                        "[OrderManager] {} {} {} fully filled at MATCHED ({}/{} tol={:.4}); removing before CONFIRMED",
                        self.symbol, update.client_order_id, order.side,
                        cumulative.max(update.filled_quantity), order.quantity, tolerance,
                    );
                    self.orders.remove(&update.client_order_id);
                }
            }
            OrderStatus::Rejected => {
                // Server explicitly rejected the submission (insufficient
                // balance, invalid price, etc.). Mark as Rejected and keep
                // in the map — it's already not on the server, and leaving
                // it here ensures `refresh()` won't re-emit a cancel signal
                // for it if a race delivers the update after a quote cycle.
                log::info!(
                    "[OrderManager] {} {} {} rejected (kept as Rejected; no cancel)",
                    self.symbol, update.client_order_id, order.side
                );
                order.status = LocalOrderStatus::Rejected;
            }
            OrderStatus::Filled | OrderStatus::Cancelled => {
                log::debug!(
                    "[OrderManager] {} {} {} removed ({:?})",
                    self.symbol, update.client_order_id, order.side, update.status
                );
                self.orders.remove(&update.client_order_id);
            }
            _ => {}
        }
    }

    /// Reconcile local state after the executor DROPPED a signal for `coid`
    /// without sending it (the stale-signal guard in `execute_fallback_signal`
    /// returns `OrderStatus::ExecutorRejected` for both legs of a dropped
    /// `BatchUpdateOrders`).
    ///
    /// Because the request never reached the exchange, server state is exactly
    /// what it was *before* the dropped signal — so the correct action depends
    /// entirely on the local status, which is ground truth here:
    ///   * `Submitted` → the placement never landed → remove it (it does not
    ///     exist on the book; releasing it lets the caller free the lock).
    ///   * `Cancelling` → the cancel never landed → the order is STILL resting
    ///     on the book. Revert it to `Active` so the next `refresh` re-emits
    ///     the cancel. Without this the order is stuck `Cancelling` forever
    ///     (excluded from the active set, so `reconcile_side` never re-cancels
    ///     it) and silently rests until the market resolves — the "forgotten
    ///     order on the book" bug observed in the 2026-06-18 live run, where a
    ///     stale-dropped reprice batch's cancel leg was mis-handled as a
    ///     dropped placement.
    ///
    /// Returns a [`DroppedSignalOutcome`] so the caller can keep its own
    /// pending-order / lock ledger in lockstep. Any other status (or an
    /// unknown coid) is a no-op returning `NotTracked`.
    pub fn on_signal_dropped(&mut self, coid: &str) -> DroppedSignalOutcome {
        match self.orders.get_mut(coid) {
            Some(o) if o.status == LocalOrderStatus::Submitted => {
                self.orders.remove(coid);
                DroppedSignalOutcome::PlaceRemoved
            }
            Some(o) if o.status == LocalOrderStatus::Cancelling => {
                o.status = LocalOrderStatus::Active;
                DroppedSignalOutcome::CancelReverted
            }
            _ => DroppedSignalOutcome::NotTracked,
        }
    }

    /// Find the active (Submitted or Active) order on a given side.
    pub fn active_order(&self, side: Side) -> Option<&LocalOrder> {
        self.orders.values().find(|o| {
            o.side == side && matches!(o.status, LocalOrderStatus::Submitted | LocalOrderStatus::Active)
        })
    }

    pub fn active_bid(&self) -> Option<&LocalOrder> {
        self.active_order(Side::Buy)
    }

    pub fn active_ask(&self) -> Option<&LocalOrder> {
        self.active_order(Side::Sell)
    }

    /// All active (non-Cancelling) orders.
    pub fn active_orders(&self) -> Vec<&LocalOrder> {
        self.orders.values().filter(|o| {
            matches!(o.status, LocalOrderStatus::Submitted | LocalOrderStatus::Active)
        }).collect()
    }

    /// All client_order_ids currently tracked, in any state. Used by
    /// the polymaker strategy on event eviction to scrub the orphan
    /// reconciler of coids tied to the just-evicted event.
    pub fn active_coids(&self) -> Vec<String> {
        self.orders.keys().cloned().collect()
    }

    /// Emit Cancel signals for every Submitted/Active order on `side`,
    /// marking each locally as `Cancelling` so the next `refresh()` won't
    /// try to reconcile them again. Used by the hard-position-cap enforcer
    /// in the strategy: when a fill pushes |net| past the intended cap,
    /// we yank the entire "adding" side from the book in one pass so no
    /// further passive fill can drive inventory deeper.
    pub fn cancel_orders_by_side(&mut self, side: Side, ts_event: u64) -> Vec<Signal> {
        let coids: Vec<String> = self.orders.values()
            .filter(|o| o.side == side
                && matches!(o.status, LocalOrderStatus::Submitted | LocalOrderStatus::Active))
            .map(|o| o.client_order_id.clone())
            .collect();
        let mut signals = Vec::with_capacity(coids.len());
        for coid in coids {
            if let Some(o) = self.orders.get_mut(&coid) {
                o.status = LocalOrderStatus::Cancelling;
            }
            signals.push(Signal::CancelOrder {
                exchange: self.exchange,
                client_order_id: coid,
                instance_id: self.instance_id.clone(),
                timestamp_ns: ts_event,
            });
        }
        signals
    }

    /// Total quantity locked by active sell orders. `Cancelling` is
    /// INCLUDED: a cancel-in-flight order is still resting on the server
    /// (shares stay reserved by the exchange until the cancel CONFIRMS),
    /// so excluding it made `available` over-count during a SELL reprice
    /// → the routing's feasibility gate passed → an oversized SELL was
    /// placed → `insufficient shares (rest sell)` reject (scales with
    /// cadence). Counting it keeps the local view in sync with the
    /// exchange lock-until-confirm, so the SELL→BUY-down fallback fires.
    pub fn locked_sell_qty(&self) -> f64 {
        self.orders.values()
            .filter(|o| o.side == Side::Sell && matches!(o.status, LocalOrderStatus::Submitted | LocalOrderStatus::Active | LocalOrderStatus::Cancelling))
            .map(|o| o.quantity)
            .sum()
    }

    /// Total cost locked by active buy orders (sum of price * qty).
    /// `Cancelling` INCLUDED — same lock-until-confirm reasoning as
    /// [`locked_sell_qty`].
    pub fn locked_buy_cost(&self) -> f64 {
        self.orders.values()
            .filter(|o| o.side == Side::Buy && matches!(o.status, LocalOrderStatus::Submitted | LocalOrderStatus::Active | LocalOrderStatus::Cancelling))
            .map(|o| o.price * o.quantity)
            .sum()
    }

    /// Number of orders currently tracked by this OrderManager — includes
    /// terminal-but-kept states like Rejected / Cancelling that stick
    /// around in the map for race-prevention reasons. Use this for "how
    /// much state is this OM holding" queries. For "how many orders are
    /// live on the server right now" use [`active_count`].
    pub fn open_count(&self) -> usize {
        self.orders.len()
    }

    /// Number of orders whose server-side state is (believed to be) live:
    /// filtered to `Submitted | Active`, matching the same predicate
    /// `refresh()` uses to build `active_bids` / `active_asks`. This is
    /// the count operators should watch — it's what `reconcile_side`
    /// treats as "orders that need reconciling against desired quotes".
    /// Rejected / Cancelling / Cancelled entries sitting in the map are
    /// NOT counted here.
    pub fn active_count(&self) -> usize {
        self.orders.values()
            .filter(|o| matches!(o.status, LocalOrderStatus::Submitted | LocalOrderStatus::Active))
            .count()
    }

    /// Number of IN-FLIGHT orders on `side` — `Submitted` (placing), `Active`
    /// (resting) OR `Cancelling` (cancel sent, order STILL on the book until the
    /// exchange confirms). This is the count the strategy's per-leg replace gate
    /// keys off: 0 → place, 1 → keep/replace, ≥2 → a reprice is already in
    /// flight, so DON'T place a second one (the gate then cancels the stale
    /// active but suppresses the replacement until the in-flight cancel drains).
    /// `Cancelling` IS counted on purpose: a cancel that hasn't confirmed leaves
    /// a real order resting on the exchange, so stacking another reprice on top
    /// is what builds up the orphan / not-enough-balance pile-up. Terminal
    /// `Rejected` is excluded (already off the book).
    pub fn live_count(&self, side: Side) -> usize {
        self.orders.values()
            .filter(|o| o.side == side
                && matches!(o.status,
                    LocalOrderStatus::Submitted | LocalOrderStatus::Active | LocalOrderStatus::Cancelling))
            .count()
    }

    /// Refresh orders to match the desired quote levels.
    ///
    /// Compares each desired level against existing active orders:
    /// - If an active order exists at the same price+qty → keep it (no action).
    /// - If an active order exists at a different price → cancel old + submit new.
    /// - If no active order exists for a desired level → submit new.
    /// - Any active orders not matched by a desired level → cancel.
    ///
    /// Returns signals to execute (CancelOrder / NewOrder).
    pub fn refresh(&mut self, desired: &[QuoteLevel], ts_event: u64) -> Vec<Signal> {
        // Default: reconcile both sides (byte-identical to pre-gate behaviour).
        self.refresh_gated(desired, ts_event, false, false, false, false)
    }

    /// `refresh`, but with per-side gating. When `block_buy` (resp.
    /// `block_sell`) is true the BUY (resp. SELL) side is left completely
    /// untouched — no place, no cancel, resting orders kept — so the
    /// strategy's per-leg in-flight gate can pause one exposure leg
    /// (e.g. BUY-Up / SELL-Down) while the other keeps quoting. With both
    /// flags false this is identical to the original `refresh`.
    /// `block_place_buy` / `block_place_sell`: SIDE is reconciled normally for
    /// KEEP + CANCEL (a stale resting order out of the requote band is still
    /// cancelled), but placing a NEW order for an unmatched desired level is
    /// SUPPRESSED. Used by the per-leg in-flight gate: when a reprice is already
    /// in flight on a leg (`live_count` incl. Cancelling ≥ 2), don't stack a
    /// second placement — cancel the stale active if needed, but wait for the
    /// in-flight cancel to drain before placing again. Distinct from
    /// `block_buy`/`block_sell` which skip the side ENTIRELY (no cancel either).
    pub fn refresh_gated(
        &mut self,
        desired: &[QuoteLevel],
        ts_event: u64,
        block_buy: bool,
        block_sell: bool,
        block_place_buy: bool,
        block_place_sell: bool,
    ) -> Vec<Signal> {
        let _t = crate::latency::TimedStage::new("order_manager.refresh");
        let mut signals = Vec::new();

        // Collect active orders by side
        let active_bids: Vec<LocalOrder> = self.orders.values()
            .filter(|o| o.side == Side::Buy && matches!(o.status, LocalOrderStatus::Submitted | LocalOrderStatus::Active))
            .cloned()
            .collect();
        let active_asks: Vec<LocalOrder> = self.orders.values()
            .filter(|o| o.side == Side::Sell && matches!(o.status, LocalOrderStatus::Submitted | LocalOrderStatus::Active))
            .cloned()
            .collect();

        // Health diagnostic: under normal operation each side has ≤1 order
        // (one bid + one ask). When we see a cluster of active orders on
        // one side the cause is either (a) ladder quoting — shouldn't
        // happen with this quoter, (b) stale Submitted-state leaks where
        // an ack never arrived, or (c) Rejected/Cancelled updates never
        // routed back to this OM. Log once per (side, refresh tick) when
        // the count exceeds the threshold so we can diagnose from log
        // alone — each active order's {coid, price, status, age_ms} is
        // dumped so price-ladder vs same-price-leak is visible.
        const HEALTHY_MAX_PER_SIDE: usize = 2;
        let now = crate::types::now_ns();
        for (side_name, active) in [("BID", &active_bids), ("ASK", &active_asks)] {
            if active.len() > HEALTHY_MAX_PER_SIDE {
                let details: Vec<String> = active.iter().map(|o| {
                    let age_ms = now.saturating_sub(o.created_ns) / 1_000_000;
                    format!("{{coid={} @{:.4} qty={} status={:?} age={}ms}}",
                        o.client_order_id, o.price, o.quantity, o.status, age_ms)
                }).collect();
                log::warn!(
                    "[OrderManager] {} {} side has {} active orders (healthy≤{}): [{}]",
                    self.symbol, side_name, active.len(), HEALTHY_MAX_PER_SIDE,
                    details.join(", "),
                );
            }
        }

        let desired_bids: Vec<&QuoteLevel> = desired.iter().filter(|q| q.side == Side::Buy).collect();
        let desired_asks: Vec<&QuoteLevel> = desired.iter().filter(|q| q.side == Side::Sell).collect();

        // A blocked side is skipped entirely — resting orders are left in
        // place and no new place/cancel is emitted for it this tick.
        if !block_buy {
            self.reconcile_side(&active_bids, &desired_bids, Side::Buy, ts_event, block_place_buy, &mut signals);
        }
        if !block_sell {
            self.reconcile_side(&active_asks, &desired_asks, Side::Sell, ts_event, block_place_sell, &mut signals);
        }

        signals
    }

    /// Simple single bid/ask interface (backward compatible).
    pub fn get_signals(
        &mut self,
        bid_price: Option<f64>,
        ask_price: Option<f64>,
        quantity: f64,
        ts_event: u64,
    ) -> Vec<Signal> {
        let mut desired = Vec::new();
        if let Some(p) = bid_price {
            desired.push(QuoteLevel { side: Side::Buy, price: p, quantity, post_only: true, order_type: OrderType::Limit });
        }
        if let Some(p) = ask_price {
            desired.push(QuoteLevel { side: Side::Sell, price: p, quantity, post_only: true, order_type: OrderType::Limit });
        }
        self.refresh(&desired, ts_event)
    }

    /// Reconcile active orders on one side against desired levels.
    /// Whether a resting order at price `active` should be kept against a
    /// desired quote at price `desired` (vs cancelled + replaced). With
    /// `requote_min_ticks <= 1` this is exact tick-grid equality
    /// ([`price_eq`]); the default (`0.0`) is therefore bit-identical to the
    /// pre-knob behaviour. With `requote_min_ticks = N (≥ 2)` a hysteresis
    /// band of `(N − 0.5)` ticks retains the order until the desired price
    /// drifts ≥ N ticks away. See the `requote_min_ticks` field doc.
    fn keep_resting_price(&self, active: f64, desired: f64) -> bool {
        if self.requote_min_ticks <= 1.0 {
            price_eq(active, desired, self.tick_size)
        } else {
            (active - desired).abs() < (self.requote_min_ticks - 0.5) * self.tick_size
        }
    }

    fn reconcile_side(
        &mut self,
        active: &[LocalOrder],
        desired: &[&QuoteLevel],
        side: Side,
        ts_event: u64,
        block_place: bool,
        signals: &mut Vec<Signal>,
    ) {
        // Track which active orders are "matched" (should be kept)
        let mut matched_active: Vec<bool> = vec![false; active.len()];

        // For each desired level, try to find a matching active order
        for desired_level in desired {
            let mut found = false;
            for (i, order) in active.iter().enumerate() {
                if matched_active[i] {
                    continue; // already matched to another desired level
                }
                if self.keep_resting_price(order.price, desired_level.price) {
                    // Same price (within the requote hysteresis band) → keep
                    matched_active[i] = true;
                    found = true;
                    break;
                }
            }
            if !found {
                // Drop orders below the exchange's minimum size before
                // they hit the wire. Polymarket otherwise responds with
                // `400 "Size (4) lower than the minimum: 5"`, which costs
                // an HTTP RTT and floods the log — observed 151× in the
                // 2026-05-04 49-min live run when hard-cap remainder left
                // 4 shares against a 5-share min. Filter is opt-in (0.0
                // default = no filter), set via `set_min_order_size` from
                // the instrument's `order_min_size`.
                if self.min_order_size > 0.0
                    && desired_level.quantity < self.min_order_size
                {
                    log::debug!(
                        "[OrderManager] {} drop new {} @ {:.4} qty={} below min_order_size={}",
                        self.symbol, side, desired_level.price,
                        desired_level.quantity, self.min_order_size,
                    );
                    continue;
                }
                // Drop marketable BUYs whose notional (price × qty) falls
                // below the exchange's marketable-min. Polymarket otherwise
                // returns a 400 "invalid amount for a marketable BUY order
                // ($X), min size: $1" — observed 14× in the 2026-05-06 8h22m
                // live run when `adjust_buy` lowered the price below the
                // notional gate that the strategy's per-tick logic had
                // checked at the original price. Filter is opt-in (0.0 =
                // disabled), set via `set_min_marketable_notional`.
                //
                // A BUY is "marketable" iff `!post_only` (server allows it
                // to cross) OR `order_type == Fak` (FAK always crosses).
                // Resting post-only Limit BUYs at low notional are still
                // legal and stay through the filter.
                if self.min_marketable_notional > 0.0
                    && side == Side::Buy
                    && (!desired_level.post_only
                        || matches!(desired_level.order_type, OrderType::Fak))
                {
                    let notional = desired_level.price * desired_level.quantity;
                    if notional < self.min_marketable_notional {
                        log::debug!(
                            "[OrderManager] {} drop marketable {} @ {:.4} qty={} notional={:.4} below min_marketable_notional={}",
                            self.symbol, side, desired_level.price,
                            desired_level.quantity, notional, self.min_marketable_notional,
                        );
                        continue;
                    }
                }
                // No matching active order → would submit new. But when this
                // side's placement is gated (a reprice is already in flight on
                // the leg), suppress the placement — the stale active above is
                // still cancelled by the loop below, we just don't stack a
                // second order until the in-flight cancel drains.
                if block_place {
                    continue;
                }
                // No matching active order → submit new
                let client_order_id = self.next_id();
                log::debug!(
                    "[OrderManager] {} new {} {} @ {:.4} qty={}",
                    self.symbol, client_order_id, side, desired_level.price, desired_level.quantity,
                );
                let order = LocalOrder {
                    client_order_id: client_order_id.clone(),
                    symbol: self.symbol.clone(),
                    side,
                    price: desired_level.price,
                    quantity: desired_level.quantity,
                    status: LocalOrderStatus::Submitted,
                    created_ns: crate::types::now_ns(),
                    filled_by_trade: HashMap::new(),
                };
                self.orders.insert(client_order_id.clone(), order);
                signals.push(Signal::NewOrder(OrderRequest {
                    client_order_id,
                    exchange: self.exchange,
                    symbol: self.symbol.clone(),
                    side,
                    order_type: desired_level.order_type,
                    price: Some(desired_level.price),
                    quantity: desired_level.quantity,
                    timestamp_ns: ts_event,
                    instance_id: self.instance_id.clone(),
                    fee_rate_bps: self.fee_rate_bps,
                    post_only: desired_level.post_only,
                    outcome_label: String::new(),
                }));
            }
        }

        // Cancel any active orders that weren't matched to a desired level
        for (i, order) in active.iter().enumerate() {
            if !matched_active[i] {
                log::debug!(
                    "[OrderManager] {} cancel {} {} @ {:.4} (no longer desired)",
                    self.symbol, order.client_order_id, side, order.price,
                );
                signals.push(Signal::CancelOrder {
                    exchange: self.exchange,
                    client_order_id: order.client_order_id.clone(),
                    instance_id: self.instance_id.clone(),
                    timestamp_ns: ts_event,
                });
                if let Some(o) = self.orders.get_mut(&order.client_order_id) {
                    o.status = LocalOrderStatus::Cancelling;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lvl(side: Side, price: f64) -> QuoteLevel {
        QuoteLevel { side, price, quantity: 5.0, post_only: true, order_type: OrderType::Limit }
    }
    fn om() -> OrderManager {
        OrderManager::new(Exchange::Polymarket, "TOK".into(), 0.001, "iid".into())
    }
    fn has_cancel(sigs: &[Signal], coid: &str) -> bool {
        sigs.iter().any(|s| matches!(s, Signal::CancelOrder { client_order_id, .. } if client_order_id == coid))
    }
    fn has_new(sigs: &[Signal], side: Side) -> bool {
        sigs.iter().any(|s| matches!(s, Signal::NewOrder(o) if o.side == side))
    }
    fn upd(coid: &str, side: Side, status: OrderStatus) -> OrderUpdate {
        OrderUpdate {
            client_order_id: coid.into(),
            exchange: Exchange::Polymarket,
            symbol: "TOK".into(),
            side,
            exchange_order_id: None,
            status,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: 0.0,
            avg_fill_price: 0.0,
            timestamp_ns: 0,
            trade_id: None,
            error: None,
        }
    }

    // live_count counts Submitted|Active|Cancelling per side — an in-flight
    // cancel leaves a real order on the book, so it MUST gate further placement
    // (the per-leg gate keys off it: 0 → place, 1 → keep/replace, ≥2 → don't
    // stack a second reprice).
    #[test]
    fn live_count_includes_cancelling() {
        let mut m = om();
        m.inject_open_order("b".into(), Side::Buy, 0.40, 5.0); // Active
        assert_eq!(m.live_count(Side::Buy), 1);
        // Reprice → cancel b (→ Cancelling) + place new (→ Submitted): BOTH count.
        let _ = m.refresh(&[lvl(Side::Buy, 0.42)], 1);
        assert_eq!(m.live_count(Side::Buy), 2, "Cancelling old + Submitted new = 2");
        // A Cancelled update on b removes it → back to 1 (just the new order).
        m.on_order_update(&upd("b", Side::Buy, OrderStatus::Cancelled));
        assert_eq!(m.live_count(Side::Buy), 1, "Cancelled removes b → back to 1");
    }

    // block_place: a saturated leg still CANCELS a stale (out-of-band) active,
    // but SUPPRESSES the replacement placement (cancel-stale-but-don't-place).
    #[test]
    fn refresh_gated_block_place_cancels_stale_but_does_not_place() {
        let mut m = om();
        m.inject_open_order("b".into(), Side::Buy, 0.40, 5.0); // Active
        // Desired BUY drifts to 0.41 (out of band) with BUY placement gated.
        let sigs = m.refresh_gated(&[lvl(Side::Buy, 0.41)], 1, false, false, true, false);
        assert!(has_cancel(&sigs, "b"), "stale active must still be cancelled");
        assert!(!has_new(&sigs, Side::Buy), "placement must be suppressed");
    }

    // block_place: an in-band active is simply KEPT (no cancel, no place) when
    // its leg is gated.
    #[test]
    fn refresh_gated_block_place_keeps_in_band() {
        let mut m = om();
        m.inject_open_order("b".into(), Side::Buy, 0.40, 5.0); // Active
        // Desired == current price → in band → kept regardless of the gate.
        let sigs = m.refresh_gated(&[lvl(Side::Buy, 0.40)], 1, false, false, true, false);
        assert!(!has_cancel(&sigs, "b"), "in-band active must be kept (no cancel)");
        assert!(!has_new(&sigs, Side::Buy), "no placement");
    }

    // A blocked side is left completely untouched (no cancel, no place),
    // while the unblocked side reprices normally.
    #[test]
    fn refresh_gated_skips_blocked_side_only() {
        let mut m = om();
        m.inject_open_order("b".into(), Side::Buy, 0.40, 5.0);
        m.inject_open_order("s".into(), Side::Sell, 0.60, 5.0);
        // Reprice both, but block the BUY side.
        let sigs = m.refresh_gated(&[lvl(Side::Buy, 0.41), lvl(Side::Sell, 0.59)], 1, true, false, false, false);
        assert!(!has_cancel(&sigs, "b"), "blocked BUY must not be cancelled");
        assert!(!has_new(&sigs, Side::Buy), "blocked BUY must not place");
        assert!(has_cancel(&sigs, "s"), "unblocked SELL reprices: cancel old");
        assert!(has_new(&sigs, Side::Sell), "unblocked SELL reprices: place new");
    }

    // Both unblocked = identical to the plain `refresh` (reprice = cancel+place).
    #[test]
    fn refresh_gated_unblocked_equals_refresh() {
        let mut m = om();
        m.inject_open_order("b".into(), Side::Buy, 0.40, 5.0);
        let sigs = m.refresh_gated(&[lvl(Side::Buy, 0.41)], 1, false, false, false, false);
        assert!(has_new(&sigs, Side::Buy));
        assert!(has_cancel(&sigs, "b"));
        // refresh() delegates to refresh_gated(.., false, false, false, false) → same.
        let mut m2 = om();
        m2.inject_open_order("b".into(), Side::Buy, 0.40, 5.0);
        let sigs2 = m2.refresh(&[lvl(Side::Buy, 0.41)], 1);
        assert_eq!(sigs.len(), sigs2.len());
    }

    // A dropped PLACE (Submitted) is removed entirely — the order never
    // reached the exchange, so nothing rests.
    #[test]
    fn on_signal_dropped_removes_submitted_placement() {
        let mut m = om();
        // A freshly-placed order is Submitted (reconcile_side inserts it so).
        let sigs = m.refresh(&[lvl(Side::Buy, 0.41)], 1);
        let coid = match sigs.iter().find_map(|s| match s {
            Signal::NewOrder(o) => Some(o.client_order_id.clone()), _ => None,
        }) { Some(c) => c, None => panic!("expected a NewOrder") };
        assert_eq!(m.active_count(), 1);
        assert_eq!(m.on_signal_dropped(&coid), DroppedSignalOutcome::PlaceRemoved);
        assert_eq!(m.open_count(), 0, "dropped placement must be removed");
        assert!(m.active_bid().is_none());
    }

    // A dropped CANCEL (Cancelling) is reverted to Active — the order is
    // STILL live on the exchange, so the next refresh must re-cancel it.
    #[test]
    fn on_signal_dropped_reverts_cancelling_to_active() {
        let mut m = om();
        m.inject_open_order("C".into(), Side::Buy, 0.40, 5.0);
        // Reprice to a new price → cancel C (→ Cancelling) + place new P.
        let _ = m.refresh(&[lvl(Side::Buy, 0.41)], 1);
        // C is now Cancelling (excluded from active set); P is the new active.
        assert!(m.active_bid().map(|o| o.client_order_id != "C").unwrap_or(false));

        // The reprice batch is dropped as stale: both legs come back as
        // ExecutorRejected. Place leg P → removed; cancel leg C → reverted.
        let p_coid = m.active_bid().unwrap().client_order_id.clone();
        assert_eq!(m.on_signal_dropped(&p_coid), DroppedSignalOutcome::PlaceRemoved);
        assert_eq!(m.on_signal_dropped("C"), DroppedSignalOutcome::CancelReverted);

        // C is back to Active and is the only live order — invariant restored.
        assert_eq!(m.active_count(), 1);
        assert_eq!(m.active_bid().unwrap().client_order_id, "C");

        // REGRESSION: the next refresh re-cancels C (pre-fix it was stuck
        // Cancelling forever and silently rested until settlement).
        let sigs = m.refresh(&[lvl(Side::Buy, 0.41)], 2);
        assert!(has_cancel(&sigs, "C"), "reverted order must be re-cancelled");
    }

    // An unknown coid (wrong OM / already terminal) is a no-op.
    #[test]
    fn on_signal_dropped_unknown_coid_is_nottracked() {
        let mut m = om();
        m.inject_open_order("C".into(), Side::Buy, 0.40, 5.0);
        assert_eq!(m.on_signal_dropped("nope"), DroppedSignalOutcome::NotTracked);
        // An already-Active (not Cancelling/Submitted) order is also untracked
        // by this path — leave it alone.
        assert_eq!(m.on_signal_dropped("C"), DroppedSignalOutcome::NotTracked);
        assert_eq!(m.active_count(), 1);
    }
}
