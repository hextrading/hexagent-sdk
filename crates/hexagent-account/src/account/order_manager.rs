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
    /// Cancel confirmed by the exchange — order is OFF the book. Kept in the
    /// map (NOT removed) so a late `Accepted` for the same coid can RESURRECT
    /// it: the pending/delayed cancel/placement race lands the placement on
    /// the book ~tens of ms AFTER the cancel committed, and the strategy does
    /// receive that `Accepted` — keeping the Cancelled order here lets
    /// `on_order_update` flip it back to `Active` instead of dropping the
    /// (now-live) order to settlement (live.log 2026-06-25: 120/121 forgotten
    /// orders were this race). Filtered out of ALL active/live/locked queries
    /// exactly like `Rejected` (they whitelist `Submitted|Active|Cancelling`);
    /// the whole map is freed when the OM is torn down at event expiry, so
    /// kept-Cancelled entries are bounded to one event.
    Cancelled,
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
    /// When true (and `instance_id` is non-empty), `next_id()` prefixes every
    /// client_order_id with `"{instance_id}-"`. This makes a coid
    /// self-describing: the multi-instance strategy router can recover the
    /// PLACING instance from the coid alone — even for late synthetic updates
    /// (place-timeout orphans, settlement fills, reconcile results) that arrive
    /// AFTER the coid's `coid_owner` registry entry was freed on its first
    /// terminal status, which would otherwise hit the broadcast fallback and
    /// fan a single instance's order out to every instance's PositionManager /
    /// orphan_reconciler. Default `false` → byte-identical numeric coids
    /// (backtest determinism + single-instance/tests untouched); the polymaker
    /// strategy enables it for live/paper only.
    coid_prefix: bool,
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
    /// Cancel requests for placements still awaiting their first authoritative
    /// exchange update. Sending DELETE while the POST is still being processed
    /// produces the `pending/delayed` cancel-before-ack race; instead we park
    /// the coid here and emit its cancel immediately after Accepted /
    /// PartiallyFilled. A NewOrderTimeout also flushes the intent so an order
    /// that may have landed is still pulled and handed to orphan reconcile.
    cancel_intents: std::collections::BTreeSet<String>,
    /// Monotonic per-manager telemetry counter. Incremented only when a fresh
    /// cancel intent is parked behind a still-Submitted placement ACK.
    cancel_before_ack_count: u64,
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
            coid_prefix: false,
            fee_rate_bps: 0,
            min_order_size: 0.0,
            min_marketable_notional: 0.0,
            requote_min_ticks: 0.0,
            orders: std::collections::BTreeMap::new(),
            cancel_intents: std::collections::BTreeSet::new(),
            cancel_before_ack_count: 0,
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

    /// Enable/disable the `"{instance_id}-"` coid prefix (see field doc).
    /// Pass `false` (the default) for backtest/single-instance to keep
    /// byte-identical numeric coids; `true` for live/paper multi-account so
    /// the router can route late updates back to the placing instance.
    pub fn set_coid_prefix(&mut self, enabled: bool) {
        self.coid_prefix = enabled;
    }

    fn next_id(&self) -> String {
        let n = GLOBAL_ORDER_ID.fetch_add(1, Ordering::Relaxed);
        if self.coid_prefix && !self.instance_id.is_empty() {
            // "{instance_id}-{counter}" — the counter suffix is the unique,
            // monotonic part (never contains '-'); routers recover the
            // instance via `rsplit_once('-')`.
            format!("{}-{}", self.instance_id, n)
        } else {
            n.to_string()
        }
    }

    fn cancel_signal(&self, client_order_id: &str, timestamp_ns: u64) -> Signal {
        Signal::CancelOrder {
            exchange: self.exchange,
            client_order_id: client_order_id.to_string(),
            instance_id: self.instance_id.clone(),
            timestamp_ns,
        }
    }

    /// Request cancellation without racing a placement that is still awaiting
    /// its first exchange result. Active orders emit immediately; Submitted
    /// orders retain a cancel intent until ACK/partial-fill/place-timeout.
    fn request_cancel(&mut self, client_order_id: &str, timestamp_ns: u64) -> Option<Signal> {
        let status = self.orders.get(client_order_id).map(|o| o.status)?;
        match status {
            LocalOrderStatus::Submitted => {
                if self.cancel_intents.insert(client_order_id.to_string()) {
                    self.cancel_before_ack_count = self.cancel_before_ack_count.saturating_add(1);
                    log::info!(
                        "[orphan_metric] cancel_before_ack=1 cancel_before_ack_total={} symbol={} coid={} state=Submitted",
                        self.cancel_before_ack_count, self.symbol, client_order_id,
                    );
                }
                None
            }
            LocalOrderStatus::Active => {
                self.cancel_intents.remove(client_order_id);
                if let Some(order) = self.orders.get_mut(client_order_id) {
                    order.status = LocalOrderStatus::Cancelling;
                }
                Some(self.cancel_signal(client_order_id, timestamp_ns))
            }
            LocalOrderStatus::Cancelling
            | LocalOrderStatus::Rejected
            | LocalOrderStatus::Cancelled => None,
        }
    }

    /// Flush a parked cancel intent after the placement request can no longer
    /// be overtaken by a normal ACK. Accepted/PartiallyFilled are authoritative
    /// evidence that the order exists; NewOrderTimeout is still unknown, so we
    /// send the cancel defensively and let the orphan lifecycle retain locks.
    fn flush_cancel_intent(&mut self, update: &OrderUpdate) -> Option<Signal> {
        if !matches!(
            update.status,
            OrderStatus::Accepted
                | OrderStatus::PartiallyFilled
                | OrderStatus::NewOrderTimeout
        ) || !self.cancel_intents.remove(&update.client_order_id)
        {
            return None;
        }
        let Some(order) = self.orders.get_mut(&update.client_order_id) else {
            return None;
        };
        order.status = LocalOrderStatus::Cancelling;
        log::info!(
            "[OrderManager] {} cancel-intent released coid={} after {:?}",
            self.symbol, update.client_order_id, update.status,
        );
        Some(self.cancel_signal(&update.client_order_id, update.timestamp_ns))
    }

    /// Whether `coid` has a cancel parked behind its placement ACK.
    pub fn has_cancel_intent(&self, coid: &str) -> bool {
        self.cancel_intents.contains(coid)
    }

    /// Per-manager count exposed for diagnostics and regression tests.
    pub fn cancel_before_ack_count(&self) -> u64 {
        self.cancel_before_ack_count
    }

    /// Update local order state from an exchange OrderUpdate. Returns a
    /// deferred `CancelOrder` when this update releases a cancel intent.
    pub fn on_order_update(&mut self, update: &OrderUpdate) -> Option<Signal> {
        let Some(order) = self.orders.get_mut(&update.client_order_id) else {
            self.cancel_intents.remove(&update.client_order_id);
            return None;
        };
        match update.status {
            OrderStatus::Accepted => {
                // RESURRECTION: an Accepted for an order we'd marked Cancelled
                // means the placement landed on the book AFTER an intervening
                // cancel committed (the pending/delayed cancel/placement race).
                // Flip it back to live so `refresh()` reprices it and
                // `cancel_all()` cancels it at expiry — instead of leaving the
                // (now-live) order forgotten to settlement.
                if order.status == LocalOrderStatus::Cancelled {
                    log::warn!(
                        "[OrderManager] {} {} {} RESURRECTED — Accepted landed after Cancelled (cancel/placement race); re-activating @ {:.4} x {}",
                        self.symbol, update.client_order_id, order.side, order.price, order.quantity,
                    );
                }
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
                    self.cancel_intents.remove(&update.client_order_id);
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
                self.cancel_intents.remove(&update.client_order_id);
            }
            OrderStatus::Filled => {
                // Terminal & done — a Filled order never receives a later
                // Accepted, so there's nothing to resurrect; remove it.
                log::debug!(
                    "[OrderManager] {} {} {} removed (Filled)",
                    self.symbol, update.client_order_id, order.side
                );
                self.orders.remove(&update.client_order_id);
                self.cancel_intents.remove(&update.client_order_id);
            }
            OrderStatus::Cancelled => {
                // Do NOT remove — keep as `Cancelled` so a late `Accepted`
                // (pending/delayed race) can resurrect it via the Accepted arm
                // above. Excluded from all active/live/locked queries; freed
                // with the OM at event teardown. (See LocalOrderStatus::Cancelled.)
                log::debug!(
                    "[OrderManager] {} {} {} → Cancelled (kept for possible resurrection)",
                    self.symbol, update.client_order_id, order.side
                );
                order.status = LocalOrderStatus::Cancelled;
                self.cancel_intents.remove(&update.client_order_id);
            }
            _ => {}
        }
        self.flush_cancel_intent(update)
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
                self.cancel_intents.remove(coid);
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

    /// Request cancellation for every Submitted/Active order on `side`.
    /// Active orders emit immediately and become `Cancelling`; Submitted
    /// orders park a cancel intent until their first exchange result, avoiding
    /// DELETE overtaking POST. Used by the hard-position-cap enforcer.
    pub fn cancel_orders_by_side(&mut self, side: Side, ts_event: u64) -> Vec<Signal> {
        let coids: Vec<String> = self.orders.values()
            .filter(|o| o.side == side
                && matches!(o.status, LocalOrderStatus::Submitted | LocalOrderStatus::Active))
            .map(|o| o.client_order_id.clone())
            .collect();
        let mut signals = Vec::with_capacity(coids.len());
        for coid in coids {
            if let Some(signal) = self.request_cancel(&coid, ts_event) {
                signals.push(signal);
            }
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

    /// The token/symbol this OM manages — for strategy-layer reconcile logging.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Cancel every tracked active order (both sides), marking each
    /// `Cancelling`. Byte-identical to the legacy `get_signals(None, None, ..)`
    /// drain (BUY cancels then SELL cancels, BTreeMap order). Used by polymaker
    /// to pull all resting orders at event expiry / settlement.
    pub fn cancel_all(&mut self, ts_event: u64) -> Vec<Signal> {
        let mut signals = self.cancel_orders_by_side(Side::Buy, ts_event);
        signals.extend(self.cancel_orders_by_side(Side::Sell, ts_event));
        signals
    }

    /// Snapshot the maker-policy knobs for the reconcile primitive. Lets the
    /// per-side reconcile policy live outside the OM (strategy layer) while the
    /// OM stays the order/book store.
    pub fn reconcile_cfg(&self) -> ReconcileCfg {
        ReconcileCfg {
            tick_size: self.tick_size,
            requote_min_ticks: self.requote_min_ticks,
            min_order_size: self.min_order_size,
            min_marketable_notional: self.min_marketable_notional,
        }
    }

    /// Active (Submitted | Active) orders on one side — the snapshot the
    /// reconcile primitive diffs against desired levels.
    pub fn active_by_side(&self, side: Side) -> Vec<LocalOrder> {
        self.orders.values()
            .filter(|o| o.side == side
                && matches!(o.status, LocalOrderStatus::Submitted | LocalOrderStatus::Active))
            .cloned()
            .collect()
    }

    /// Apply reconcile decisions to OM state, emitting the matching signals.
    /// `Place` assigns a fresh monotonic coid, inserts a `Submitted` local
    /// order, and emits `NewOrder`; `Cancel` emits immediately for Active or
    /// records an intent for Submitted. When a decision would replace a still-
    /// Submitted order on the same side, the new place is deferred as well —
    /// the next quote can place it after ACK → intent cancel has started.
    pub fn apply_reconcile(&mut self, actions: Vec<ReconcileAction>, ts_event: u64) -> Vec<Signal> {
        let defer_buy_place = actions.iter().any(|action| match action {
            ReconcileAction::Cancel { client_order_id } => self.orders
                .get(client_order_id)
                .is_some_and(|o| o.side == Side::Buy && o.status == LocalOrderStatus::Submitted),
            _ => false,
        });
        let defer_sell_place = actions.iter().any(|action| match action {
            ReconcileAction::Cancel { client_order_id } => self.orders
                .get(client_order_id)
                .is_some_and(|o| o.side == Side::Sell && o.status == LocalOrderStatus::Submitted),
            _ => false,
        });
        let mut signals = Vec::with_capacity(actions.len());
        for action in actions {
            match action {
                ReconcileAction::Place { side, price, quantity, order_type, post_only } => {
                    if (side == Side::Buy && defer_buy_place)
                        || (side == Side::Sell && defer_sell_place)
                    {
                        log::debug!(
                            "[OrderManager] {} defer new {} @ {:.4}: same-side Submitted cancel-intent awaiting ACK",
                            self.symbol, side, price,
                        );
                        continue;
                    }
                    let client_order_id = self.next_id();
                    log::debug!(
                        "[OrderManager] {} new {} {} @ {:.4} qty={}",
                        self.symbol, client_order_id, side, price, quantity,
                    );
                    self.orders.insert(client_order_id.clone(), LocalOrder {
                        client_order_id: client_order_id.clone(),
                        symbol: self.symbol.clone(),
                        side,
                        price,
                        quantity,
                        status: LocalOrderStatus::Submitted,
                        created_ns: crate::types::now_ns(),
                        filled_by_trade: HashMap::new(),
                    });
                    signals.push(Signal::NewOrder(OrderRequest {
                        client_order_id,
                        exchange: self.exchange,
                        symbol: self.symbol.clone(),
                        side,
                        order_type,
                        price: Some(price),
                        quantity,
                        timestamp_ns: ts_event,
                        instance_id: self.instance_id.clone(),
                        fee_rate_bps: self.fee_rate_bps,
                        post_only,
                        reduce_only: false,
                        outcome_label: String::new(),
                    }));
                }
                ReconcileAction::Cancel { client_order_id } => {
                    if let Some(o) = self.orders.get(&client_order_id) {
                        log::debug!(
                            "[OrderManager] {} cancel {} {} @ {:.4} (no longer desired)",
                            self.symbol, client_order_id, o.side, o.price,
                        );
                    }
                    if let Some(signal) = self.request_cancel(&client_order_id, ts_event) {
                        signals.push(signal);
                    }
                }
            }
        }
        signals
    }

}

/// Maker-policy knobs for `reconcile_side_decide`, snapshotted from an
/// `OrderManager` via `reconcile_cfg`. Lets the per-side reconcile policy live
/// in the strategy layer while the OM stays the order/book store.
#[derive(Clone, Copy, Debug)]
pub struct ReconcileCfg {
    pub tick_size: f64,
    pub requote_min_ticks: f64,
    pub min_order_size: f64,
    pub min_marketable_notional: f64,
}

/// One reconcile decision for a side. Produced by `reconcile_side_decide`
/// (pure policy), applied to OM state by `OrderManager::apply_reconcile`.
#[derive(Clone, Debug)]
pub enum ReconcileAction {
    /// Submit a new order at this level — coid is assigned at apply time.
    Place { side: Side, price: f64, quantity: f64, order_type: OrderType, post_only: bool },
    /// Cancel a resting / placing order by coid.
    Cancel { client_order_id: String },
}


#[cfg(test)]
mod tests {
    use super::*;

    fn om() -> OrderManager {
        OrderManager::new(Exchange::Polymarket, "TOK".into(), 0.001, "iid".into())
    }
    /// Place a resting order via the OM service; returns its coid. (Reconcile
    /// policy now lives in the strategy layer; the OM only applies decisions.)
    fn place(m: &mut OrderManager, side: Side, price: f64, qty: f64) -> String {
        let sigs = m.apply_reconcile(vec![ReconcileAction::Place {
            side, price, quantity: qty, order_type: OrderType::Limit, post_only: true,
        }], 1);
        sigs.into_iter().find_map(|s| match s {
            Signal::NewOrder(o) => Some(o.client_order_id),
            _ => None,
        }).expect("expected a NewOrder")
    }
    /// Cancel an order via the OM service (marks it Cancelling).
    fn cancel(m: &mut OrderManager, coid: &str) {
        let _ = m.apply_reconcile(vec![ReconcileAction::Cancel { client_order_id: coid.into() }], 1);
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
        cancel(&mut m, "b");
        let _ = place(&mut m, Side::Buy, 0.42, 5.0);
        assert_eq!(m.live_count(Side::Buy), 2, "Cancelling old + Submitted new = 2");
        // A Cancelled update on b excludes it from live_count → back to 1
        // (b is now KEPT as Cancelled but filtered out of live/active queries).
        let _ = m.on_order_update(&upd("b", Side::Buy, OrderStatus::Cancelled));
        assert_eq!(m.live_count(Side::Buy), 1, "Cancelled excludes b → live back to 1");
    }

    /// A DELETE-uncertain update must leave the local order Cancelling/live so
    /// leg admission and worst-case collateral accounting remain blocked until
    /// an authoritative terminal update arrives.
    #[test]
    fn cancel_timeout_keeps_order_live_and_locked() {
        let mut m = om();
        m.inject_open_order("x".into(), Side::Buy, 0.40, 5.0);
        cancel(&mut m, "x");
        assert_eq!(m.live_count(Side::Buy), 1);
        assert!((m.locked_buy_cost() - 2.0).abs() < 1e-9);

        let _ = m.on_order_update(&upd("x", Side::Buy, OrderStatus::CancelOrderTimeout));
        assert_eq!(m.live_count(Side::Buy), 1, "uncertain cancel remains live");
        assert!((m.locked_buy_cost() - 2.0).abs() < 1e-9,
            "uncertain cancel keeps worst-case cash locked");

        let _ = m.on_order_update(&upd("x", Side::Buy, OrderStatus::Cancelled));
        assert_eq!(m.live_count(Side::Buy), 0, "authoritative cancel releases slot");
        assert_eq!(m.locked_buy_cost(), 0.0, "authoritative cancel releases lock");
    }

    // Cancelled is KEPT in the map (not removed) and excluded from live/active
    // queries; a late `Accepted` (the pending/delayed cancel/placement race)
    // RESURRECTS it to Active, retaining the original price/qty — no
    // reconstruction needed. Fixes the forgotten-order leak (live.log
    // 2026-06-25: 120/121 forgotten orders).
    #[test]
    fn cancelled_is_kept_and_accepted_resurrects() {
        let mut m = om();
        m.inject_open_order("x".into(), Side::Buy, 0.40, 5.0); // live on book
        assert_eq!(m.live_count(Side::Buy), 1);
        assert_eq!(m.open_count(), 1);
        // Cancelled: kept in map but excluded from live/active.
        let _ = m.on_order_update(&upd("x", Side::Buy, OrderStatus::Cancelled));
        assert_eq!(m.live_count(Side::Buy), 0, "Cancelled excluded from live_count");
        assert!(m.active_bid().is_none(), "Cancelled excluded from active_bid");
        assert_eq!(m.open_count(), 1, "but KEPT in the map for resurrection");
        // Late Accepted resurrects → Active, retaining original price/qty.
        let _ = m.on_order_update(&upd("x", Side::Buy, OrderStatus::Accepted));
        assert_eq!(m.live_count(Side::Buy), 1, "Accepted resurrects Cancelled → Active");
        let bid = m.active_bid().expect("resurrected as the active bid");
        assert_eq!(bid.client_order_id, "x");
        assert_eq!(bid.status, LocalOrderStatus::Active);
        assert!((bid.price - 0.40).abs() < 1e-9, "retains original price");
        assert_eq!(bid.quantity, 5.0, "retains original quantity");
    }

    // Filled is terminal & done (never gets a later Accepted) → still removed,
    // never resurrected.
    #[test]
    fn filled_is_removed_not_kept() {
        let mut m = om();
        m.inject_open_order("x".into(), Side::Buy, 0.40, 5.0);
        let _ = m.on_order_update(&upd("x", Side::Buy, OrderStatus::Filled));
        assert_eq!(m.open_count(), 0, "Filled is removed");
    }

    #[test]
    fn cancel_before_ack_parks_intent_and_defers_same_side_replacement() {
        let mut m = om();
        let coid = place(&mut m, Side::Buy, 0.40, 5.0);

        // Normal reprice decision order is Place then Cancel. Because the old
        // order is still Submitted, neither DELETE nor the replacement may be
        // sent before its ACK.
        let signals = m.apply_reconcile(vec![
            ReconcileAction::Place {
                side: Side::Buy,
                price: 0.42,
                quantity: 5.0,
                order_type: OrderType::Limit,
                post_only: true,
            },
            ReconcileAction::Cancel { client_order_id: coid.clone() },
        ], 2);
        assert!(signals.is_empty(), "place and cancel both wait for the ACK");
        assert!(m.has_cancel_intent(&coid));
        assert_eq!(m.cancel_before_ack_count(), 1);
        assert_eq!(m.orders.len(), 1, "replacement was not stacked pre-ACK");
        assert_eq!(m.orders[&coid].status, LocalOrderStatus::Submitted);

        let signal = m.on_order_update(&upd(&coid, Side::Buy, OrderStatus::Accepted))
            .expect("Accepted releases the deferred cancel");
        assert!(matches!(signal, Signal::CancelOrder { ref client_order_id, .. } if client_order_id == &coid));
        assert!(!m.has_cancel_intent(&coid));
        assert_eq!(m.orders[&coid].status, LocalOrderStatus::Cancelling);
    }

    #[test]
    fn cancel_intent_flushes_after_place_timeout_with_locks_held() {
        let mut m = om();
        let coid = place(&mut m, Side::Sell, 0.60, 5.0);
        let signals = m.apply_reconcile(
            vec![ReconcileAction::Cancel { client_order_id: coid.clone() }],
            2,
        );
        assert!(signals.is_empty());
        assert!(m.has_cancel_intent(&coid));

        let signal = m.on_order_update(&upd(&coid, Side::Sell, OrderStatus::NewOrderTimeout))
            .expect("unknown placement still needs a defensive cancel");
        assert!(matches!(signal, Signal::CancelOrder { .. }));
        assert_eq!(m.orders[&coid].status, LocalOrderStatus::Cancelling);
        assert_eq!(m.live_count(Side::Sell), 1);
        assert!((m.locked_sell_qty() - 5.0).abs() < 1e-9);
    }

    #[test]
    fn terminal_place_result_clears_cancel_intent_without_delete() {
        let mut m = om();
        let coid = place(&mut m, Side::Buy, 0.40, 5.0);
        let signals = m.apply_reconcile(
            vec![ReconcileAction::Cancel { client_order_id: coid.clone() }],
            2,
        );
        assert!(signals.is_empty());
        assert!(m.has_cancel_intent(&coid));

        let signal = m.on_order_update(&upd(&coid, Side::Buy, OrderStatus::Rejected));
        assert!(signal.is_none(), "rejected placement never needs DELETE");
        assert!(!m.has_cancel_intent(&coid));
        assert_eq!(m.orders[&coid].status, LocalOrderStatus::Rejected);
    }

    // (The per-side reconcile policy tests — block_place / in-band keep /
    // skip-side — moved with the policy into the strategy layer:
    // polymaker `leg_manager.rs` + hexmaker. The OM only tests its order
    // ledger / state-machine / service below.)

    // A dropped PLACE (Submitted) is removed entirely — the order never
    // reached the exchange, so nothing rests.
    #[test]
    fn on_signal_dropped_removes_submitted_placement() {
        let mut m = om();
        // A freshly-placed order is Submitted.
        let coid = place(&mut m, Side::Buy, 0.41, 5.0);
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
        // Reprice → cancel C (→ Cancelling) + place new P.
        cancel(&mut m, "C");
        let p_coid = place(&mut m, Side::Buy, 0.41, 5.0);
        // C is now Cancelling (excluded from active set); P is the new active.
        assert!(m.active_bid().map(|o| o.client_order_id != "C").unwrap_or(false));

        // The reprice batch is dropped as stale: both legs come back as
        // ExecutorRejected. Place leg P → removed; cancel leg C → reverted.
        assert_eq!(m.on_signal_dropped(&p_coid), DroppedSignalOutcome::PlaceRemoved);
        assert_eq!(m.on_signal_dropped("C"), DroppedSignalOutcome::CancelReverted);

        // C is back to Active and is the only live order — invariant restored.
        // (The "next reconcile re-cancels the reverted order" regression now
        // lives with the reconcile policy in the strategy layer.)
        assert_eq!(m.active_count(), 1);
        assert_eq!(m.active_bid().unwrap().client_order_id, "C");
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

    // coid prefix: default off → bare numeric (BT/single byte-identical);
    // enabled → "{instance_id}-{counter}" so the router can route by prefix.
    #[test]
    fn coid_prefix_gates_on_instance_id() {
        // Default: no prefix.
        let mut m = om();
        let c = place(&mut m, Side::Buy, 0.40, 5.0);
        assert!(c.parse::<u64>().is_ok(), "default coid is bare numeric, got {c}");

        // Enabled with a non-empty instance_id → "iid-<digits>".
        let mut m = om();
        m.set_coid_prefix(true);
        let c = place(&mut m, Side::Buy, 0.40, 5.0);
        let (iid, n) = c.rsplit_once('-').expect("prefixed coid has a '-'");
        assert_eq!(iid, "iid");
        assert!(n.parse::<u64>().is_ok(), "suffix is the numeric counter, got {n}");

        // Enabled but empty instance_id → still bare numeric (no leading '-').
        let mut m = OrderManager::new(Exchange::Polymarket, "TOK".into(), 0.001, String::new());
        m.set_coid_prefix(true);
        let c = place(&mut m, Side::Buy, 0.40, 5.0);
        assert!(c.parse::<u64>().is_ok(), "empty instance_id → no prefix, got {c}");
    }
}
