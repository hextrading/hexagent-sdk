use std::collections::HashMap;

use log::info;

use crate::account::orderbook::OrderbookManager;
use crate::types::{Liquidity, OrderStatus, OrderUpdate, Side};

/// A single position snapshot for a symbol. Produced on demand from the
/// underlying trade ledger by `PositionManager::positions()` / `get()`,
/// or from an external wallet query (live-mode API fetch).
#[derive(Debug, Clone, serde::Serialize)]
pub struct Position {
    /// Net quantity: positive = long, negative = short.
    pub quantity: f64,
    /// Volume-weighted average entry price (BUY fills only, ignoring fees).
    pub avg_price: f64,
    /// Mark-to-market value (USDC) of this position at snapshot time.
    /// Populated by `fetch_positions` from the Polymarket data-api's
    /// `currentValue` field (market mid × qty for active events, or
    /// settle × qty for settled events). Default 0 for ledger-derived
    /// positions (which don't have an external mark).
    #[serde(default)]
    pub current_value: f64,
}

/// Lifecycle status of a single fill. Mirrors Polymarket's trade push states
/// but is also used for sim/non-live fills (which land as `Confirmed` right away).
///
/// Rules (same as `LivePositionManager::update_trade`):
/// - `Confirmed` and `Failed` are terminal — subsequent updates are ignored.
/// - `Retrying` is not modeled here; callers should not call `upsert_trade`
///   with a retry status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeStatus {
    Matched,
    Mined,
    Confirmed,
    Failed,
}

impl TradeStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, TradeStatus::Confirmed | TradeStatus::Failed)
    }

    /// Lifecycle stage rank for monotonic dedup (idempotent re-pushes / gap
    /// replays): the ledger advances only when an incoming status outranks
    /// the stored one. Matched(1) → Mined(2) → Confirmed/Failed(3, terminal).
    /// NB: `OrderStatus::PartiallyFilled` collapses both MATCHED and MINED to
    /// `Matched` here (this enum's `from_order_status` never yields `Mined`),
    /// so at the PositionManager level the effective ranks are 1 and 3.
    pub fn rank(&self) -> u8 {
        match self {
            TradeStatus::Matched => 1,
            TradeStatus::Mined => 2,
            TradeStatus::Confirmed | TradeStatus::Failed => 3,
        }
    }

    /// Best-effort mapping from an `OrderStatus` carried on a fill OrderUpdate.
    /// Anything that isn't a real fill (Accepted/Rejected/Cancelled) returns None.
    /// `Failed` maps to `TradeStatus::Failed` so on-chain reverts can be
    /// reversed out of the ledger and downstream accumulators.
    pub fn from_order_status(status: OrderStatus) -> Option<Self> {
        match status {
            OrderStatus::Filled => Some(TradeStatus::Confirmed),
            OrderStatus::PartiallyFilled => Some(TradeStatus::Matched),
            OrderStatus::Failed => Some(TradeStatus::Failed),
            _ => None,
        }
    }
}

/// Outcome of `PositionManager::upsert_trade`. Replaces the prior `bool`
/// return so callers can distinguish "newly recorded fill" from "lifecycle
/// transition" from "fill reversal" — required to drive per-event
/// accumulators (volume / cashflow / fees) that the ledger itself doesn't
/// carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct UpsertResult {
    /// Ledger was inserted or transitioned to a new state.
    pub applied: bool,
    /// Sign for downstream per-fill accumulators:
    ///   +1 → trade is newly recorded with non-Failed status (add)
    ///   -1 → trade transitioned from non-Failed to Failed (reverse)
    ///    0 → status update on an already-tracked trade (no-op), or a
    ///        fresh insert that lands directly as Failed
    pub accumulator_sign: i8,
}

impl UpsertResult {
    pub const NOOP: Self = Self { applied: false, accumulator_sign: 0 };
    pub fn add() -> Self { Self { applied: true, accumulator_sign: 1 } }
    pub fn reverse() -> Self { Self { applied: true, accumulator_sign: -1 } }
    pub fn update_only() -> Self { Self { applied: true, accumulator_sign: 0 } }
}

/// A single fill in the PositionManager ledger.
#[derive(Debug, Clone)]
pub struct TradeRecord {
    pub trade_id: String,
    pub asset_id: String,
    pub side: Side,
    pub size: f64,
    pub price: f64,
    pub status: TradeStatus,
    pub is_maker: bool,
    /// USDC fee deducted from balance (TAKER SELL only). 0 otherwise.
    pub usdc_fee: f64,
    /// Shares fee deducted from acquired shares (TAKER BUY only). 0 otherwise.
    pub shares_fee: f64,
}

/// An in-flight order not yet reflected in the trade ledger. Kept alongside
/// trades so that `available_cash` / `available_inventory` can be derived
/// entirely inside `PositionManager` without the caller having to plumb
/// `locked_buy_cost` / `locked_sell_qty` from some other tracker.
#[derive(Debug, Clone)]
pub struct PendingOrder {
    pub client_order_id: String,
    pub symbol: String,
    pub side: Side,
    pub price: f64,
    /// Remaining (unfilled) quantity. Decrements on PartiallyFilled updates
    /// and the entry is removed entirely on Filled / Cancelled / Rejected.
    pub remaining_quantity: f64,
}

/// Ledger-backed position / balance tracker.
///
/// The canonical state is the trade list keyed by `trade_id`. Positions and
/// balance are computed by iterating that ledger, so status transitions
/// (Matched → Mined → Confirmed / Failed) automatically flow through to
/// downstream queries without any direct mutation of cached quantities.
///
/// `init_balance` and `init_positions` represent pre-existing state at
/// bootstrap (seeded from an API snapshot, or from a prior event's
/// settlement). Post-bootstrap flows are always expressed as trades.
pub struct PositionManager {
    init_balance: f64,
    init_positions: HashMap<String, f64>,
    // BTreeMap for deterministic iteration — `available_*` and
    // `pending_orders().values()` callers fold over these in BT;
    // the float-summation order can produce ULP-level drift run-to-
    // run with HashMap's randomized hash seed, and any side-effecty
    // iteration (logging, conditional state mutation per-item)
    // diverges outright.
    trades: std::collections::BTreeMap<String, TradeRecord>,
    /// In-flight orders keyed by client_order_id. Drives locked-cost and
    /// locked-quantity computation for `available_*` queries.
    pending_orders: std::collections::BTreeMap<String, PendingOrder>,
    /// Monotonic counter used to synthesize `trade_id` when the caller
    /// didn't supply one (e.g. hexmarket fills without a per-fill id).
    synthetic_counter: u64,
    maker_volume: f64,
    taker_volume: f64,
}

impl PositionManager {
    pub fn new() -> Self {
        Self {
            init_balance: 0.0,
            init_positions: HashMap::new(),
            trades: std::collections::BTreeMap::new(),
            pending_orders: std::collections::BTreeMap::new(),
            synthetic_counter: 0,
            maker_volume: 0.0,
            taker_volume: 0.0,
        }
    }

    /// Create seeded with starting quantities (pre-existing positions) and balance.
    pub fn with_initial_quantities(initial: HashMap<String, f64>, balance: f64) -> Self {
        Self {
            init_balance: balance,
            init_positions: initial,
            trades: std::collections::BTreeMap::new(),
            pending_orders: std::collections::BTreeMap::new(),
            synthetic_counter: 0,
            maker_volume: 0.0,
            taker_volume: 0.0,
        }
    }

    /// Create seeded with full `Position` records (quantity + avg_price). Only
    /// quantity is preserved — avg_price is reconstructed from subsequent fills.
    pub fn with_positions(positions: HashMap<String, Position>, balance: f64) -> Self {
        let initial = positions.into_iter()
            .map(|(sym, p)| (sym, p.quantity))
            .collect();
        Self::with_initial_quantities(initial, balance)
    }

    // ════════════════════════════════════════════════════════════════
    // Ledger mutations
    // ════════════════════════════════════════════════════════════════

    /// Upsert a trade in the ledger. Returns an [`UpsertResult`] that tells
    /// the caller whether downstream per-fill accumulators (volume /
    /// cashflow / fees that the ledger doesn't itself maintain) should
    /// **add** (`+1`), **reverse** (`-1`), or stay put (`0`).
    ///
    /// Lifecycle rules:
    /// - First sighting with non-Failed status → `add` (sign +1).
    /// - First sighting that already lands as `Failed` (e.g. WS dropped the
    ///   earlier states): inserted into the ledger for completeness, but
    ///   sign is 0 — there was nothing to reverse, and a Failed fill
    ///   never contributes to position/cashflow/volume.
    /// - Existing non-Failed → non-Failed transition (Matched → Confirmed
    ///   etc.): `update_only` (sign 0). Position is derived from the
    ///   record so size/price changes flow through automatically; the
    ///   triple-counted accumulator bug in strategy.rs lived right here
    ///   when sign was implicit.
    /// - Existing non-Failed → Failed: `reverse` (sign -1). The on-chain
    ///   settlement reverted; back the original add out of the
    ///   accumulators using the fresh values (Polymarket repeats the same
    ///   size/price on the FAILED push, so caller can reuse them).
    /// - Existing terminal (Confirmed | Failed) and incoming repeat:
    ///   rejected (`NOOP`, applied=false).
    pub fn upsert_trade(
        &mut self,
        trade_id: &str,
        asset_id: &str,
        side: Side,
        size: f64,
        price: f64,
        status: TradeStatus,
        is_maker: bool,
        usdc_fee: f64,
        shares_fee: f64,
        // Revert / failure reason from the upstream `OrderUpdate.error`
        // field (Polymarket relayer surface: `"INSUFFICIENT_BALANCE"`,
        // `"INVALID_NONCE"`, etc.). Logged on the per-trade info line
        // when present — without it `[PositionManager] rev …` events
        // are silent on the actual revert cause, making FAILED-cascade
        // diagnosis impossible. Pass `None` when the caller has no
        // error context (e.g. legacy code paths or non-Failed status
        // pushes that arrive without error metadata).
        error: Option<&str>,
    ) -> UpsertResult {
        if size <= 0.0 || trade_id.is_empty() {
            return UpsertResult::NOOP;
        }

        let prev_status = self.trades.get(trade_id).map(|r| r.status);

        let outcome = match prev_status {
            // Already terminal (Confirmed or Failed) — ignore re-pushes.
            Some(s) if s.is_terminal() => return UpsertResult::NOOP,
            // First sighting + lands as Failed → record but don't accumulate.
            None if status == TradeStatus::Failed => UpsertResult::update_only(),
            // First sighting with a live fill → caller should add.
            None => UpsertResult::add(),
            // Existing non-terminal: only act on a strictly-later stage.
            // Same/earlier rank → NOOP (dedups repeated gap-replay / WS
            // pushes, and blocks a stale earlier state from reversing a
            // later one). Advance to Failed → reverse the original add;
            // any other advance (Matched → Mined → Confirmed) → status-only.
            Some(prev) => {
                if status.rank() <= prev.rank() {
                    return UpsertResult::NOOP;
                }
                if status == TradeStatus::Failed {
                    UpsertResult::reverse()
                } else {
                    UpsertResult::update_only()
                }
            }
        };

        self.trades.insert(trade_id.to_string(), TradeRecord {
            trade_id: trade_id.to_string(),
            asset_id: asset_id.to_string(),
            side,
            size,
            price,
            status,
            is_maker,
            usdc_fee,
            shares_fee,
        });

        // Mirror the same sign onto the per-asset volume tracker so it
        // stays consistent with the strategy-side accumulators.
        if outcome.accumulator_sign != 0 {
            let notional = size * price * outcome.accumulator_sign as f64;
            if is_maker { self.maker_volume += notional; }
            else { self.taker_volume += notional; }
        }

        // Tail-append `error="..."` ONLY when the upstream supplied a
        // non-empty reason. Keeps the happy-path log shape identical
        // for downstream parsers that key on the legacy fields.
        let error_part = match error {
            Some(s) if !s.is_empty() => format!(" error=\"{}\"", s),
            _ => String::new(),
        };
        info!(
            "[PositionManager] {} {} {} {:.4} @ {:.4} ({}) status={:?} fee_usdc={:.4} fee_shares={:.4} sign={:+}{}",
            match outcome.accumulator_sign {
                1 => "new",
                -1 => "rev",
                _ => "upd",
            },
            asset_id, side, size, price,
            if is_maker { "maker" } else { "taker" },
            status, usdc_fee, shares_fee, outcome.accumulator_sign,
            error_part,
        );

        outcome
    }

    /// Ingest a fill OrderUpdate with no fee information. Fees are left at 0
    /// (callers that need fee-aware accounting — i.e. polymaker — should use
    /// `upsert_trade` directly).
    ///
    /// Also reconciles `pending_orders`:
    /// - PartiallyFilled → decrement the open order's remaining_quantity
    /// - Filled / Cancelled / Rejected → remove from pending_orders entirely
    pub fn on_order_update(&mut self, update: &OrderUpdate) {
        self.sync_pending_from_update(update);

        let Some(status) = TradeStatus::from_order_status(update.status) else {
            return;
        };
        if update.filled_quantity <= 0.0 {
            return;
        }
        let is_maker = matches!(update.liquidity, Some(Liquidity::Maker));

        // Prefer the authoritative trade_id; fall back to synthetic.
        let trade_id: String = match &update.trade_id {
            Some(id) if !id.is_empty() => id.clone(),
            _ => {
                self.synthetic_counter += 1;
                format!("synth-{}-{}", update.client_order_id, self.synthetic_counter)
            }
        };

        self.upsert_trade(
            &trade_id,
            &update.symbol,
            update.side,
            update.filled_quantity,
            update.avg_fill_price,
            status,
            is_maker,
            0.0, 0.0,
            update.error.as_deref(),
        );
    }

    /// Reconcile the `pending_orders` map against an OrderUpdate status.
    /// Called automatically by `on_order_update`; strategies that drive the
    /// ledger via `upsert_trade` directly should call this before/after to
    /// keep locks in sync.
    pub fn sync_pending_from_update(&mut self, update: &OrderUpdate) {
        let coid = &update.client_order_id;
        if coid.is_empty() { return; }
        match update.status {
            OrderStatus::PartiallyFilled => {
                if let Some(po) = self.pending_orders.get_mut(coid) {
                    po.remaining_quantity =
                        (po.remaining_quantity - update.filled_quantity).max(0.0);
                    if po.remaining_quantity <= 0.0 {
                        self.pending_orders.remove(coid);
                    }
                }
            }
            OrderStatus::Filled
            | OrderStatus::Cancelled
            | OrderStatus::Rejected => {
                self.pending_orders.remove(coid);
            }
            _ => {}
        }
    }

    /// Shift `init_balance` by `delta`. Used for event-level adjustments
    /// such as settlement PnL injection.
    pub fn adjust_balance(&mut self, delta: f64) {
        self.init_balance += delta;
    }

    /// Shift the seeded `init_positions` entry for `symbol` by `delta`.
    /// Leaves the trade ledger untouched. Used for in-kind adjustments that
    /// shouldn't be modelled as a fill (e.g. manual rebalancing in tests).
    pub fn adjust_quantity(&mut self, symbol: &str, delta: f64) {
        let entry = self.init_positions.entry(symbol.to_string()).or_insert(0.0);
        *entry += delta;
    }

    // ════════════════════════════════════════════════════════════════
    // Derived state
    // ════════════════════════════════════════════════════════════════

    /// **Total** cash balance (USDC) — includes all non-FAILED trades
    /// (Confirmed + Matched + Mined on both sides). Used by the quoter where
    /// full balance visibility is desired.
    pub fn balance(&self) -> f64 {
        let mut b = self.init_balance;
        for t in self.trades.values() {
            if t.status == TradeStatus::Failed { continue; }
            match t.side {
                Side::Buy => b -= t.size * t.price,
                Side::Sell => b += t.size * t.price - t.usdc_fee,
            }
        }
        b
    }

    /// **Total** net quantity for `symbol` — includes all non-FAILED fills
    /// (Confirmed + pending Matched/Mined). Used by the quoter to compute
    /// inventory-based reservation prices against the fullest view.
    pub fn get_quantity(&self, symbol: &str) -> f64 {
        let mut q = self.init_positions.get(symbol).copied().unwrap_or(0.0);
        for t in self.trades.values() {
            if t.status == TradeStatus::Failed || t.asset_id != symbol { continue; }
            match t.side {
                Side::Buy  => q += t.size - t.shares_fee,
                Side::Sell => q -= t.size,
            }
        }
        q
    }

    /// Snapshot of a single symbol. VWAP averages BUY fills (size pre-fee).
    pub fn get(&self, symbol: &str) -> Option<Position> {
        let mut qty = self.init_positions.get(symbol).copied().unwrap_or(0.0);
        let mut buy_qty = 0.0_f64;
        let mut buy_notional = 0.0_f64;
        let mut touched = self.init_positions.contains_key(symbol);
        for t in self.trades.values() {
            if t.status == TradeStatus::Failed || t.asset_id != symbol { continue; }
            touched = true;
            match t.side {
                Side::Buy => {
                    qty += t.size - t.shares_fee;
                    buy_qty += t.size;
                    buy_notional += t.size * t.price;
                }
                Side::Sell => {
                    qty -= t.size;
                }
            }
        }
        if !touched { return None; }
        let avg_price = if buy_qty > 0.0 { buy_notional / buy_qty } else { 0.0 };
        Some(Position { quantity: qty, avg_price, current_value: 0.0 })
    }

    pub fn inventory(&self, outcome_id: &str) -> f64 {
        self.get_quantity(outcome_id).max(0.0)
    }

    /// Sum of (price × remaining_quantity) across all pending BUY orders.
    pub fn locked_buy_cost(&self) -> f64 {
        self.pending_orders.values()
            .filter(|o| o.side == Side::Buy)
            .map(|o| o.price * o.remaining_quantity)
            .sum()
    }

    /// Sum of remaining_quantity across pending SELL orders for `symbol`.
    pub fn locked_sell_qty(&self, symbol: &str) -> f64 {
        self.pending_orders.values()
            .filter(|o| o.side == Side::Sell && o.symbol == symbol)
            .map(|o| o.remaining_quantity)
            .sum()
    }

    /// **Available** USDC for placing new BUYs. Conservative — unconfirmed
    /// SELL proceeds are NOT counted; unconfirmed BUYs (money already out)
    /// ARE deducted; open BUY order locks are subtracted.
    ///
    /// ```text
    /// available_cash = init_balance
    ///                + Σ (confirmed + pending) BUY trades' −(size × price)
    ///                + Σ confirmed SELL trades' +(size × price − usdc_fee)
    ///                − Σ open BUY orders' (price × remaining_qty)
    /// ```
    pub fn available_cash(&self) -> f64 {
        let mut b = self.init_balance;
        for t in self.trades.values() {
            match t.status {
                TradeStatus::Failed => continue,
                _ => {}
            }
            match t.side {
                // BUY: money goes out regardless of confirmation state.
                Side::Buy => b -= t.size * t.price,
                // SELL: only credit proceeds once Confirmed. Matched/Mined
                // pending SELLs don't count toward available cash.
                Side::Sell => {
                    if t.status == TradeStatus::Confirmed {
                        b += t.size * t.price - t.usdc_fee;
                    }
                }
            }
        }
        (b - self.locked_buy_cost()).max(0.0)
    }

    /// **Available** inventory for `outcome_id` to cover new SELLs. Conservative —
    /// unconfirmed BUY purchases are NOT counted; unconfirmed SELLs (shares
    /// already out) ARE deducted; open SELL order locks are subtracted.
    ///
    /// ```text
    /// available_inventory = init_position
    ///                     + Σ confirmed BUY trades' +(size − shares_fee)
    ///                     + Σ (confirmed + pending) SELL trades' −size
    ///                     − Σ open SELL orders' remaining_qty
    /// ```
    pub fn available_inventory(&self, outcome_id: &str) -> f64 {
        let mut q = self.init_positions.get(outcome_id).copied().unwrap_or(0.0);
        for t in self.trades.values() {
            if t.status == TradeStatus::Failed || t.asset_id != outcome_id { continue; }
            match t.side {
                // BUY: only credit shares once Confirmed. Matched/Mined
                // pending BUYs don't count toward available inventory.
                Side::Buy => {
                    if t.status == TradeStatus::Confirmed {
                        q += t.size - t.shares_fee;
                    }
                }
                // SELL: shares go out regardless of confirmation state.
                Side::Sell => q -= t.size,
            }
        }
        (q - self.locked_sell_qty(outcome_id)).max(0.0)
    }

    /// Register / overwrite an in-flight order. Strategies should call this
    /// whenever a `Signal::NewOrder` is emitted (or on an `Accepted` OrderUpdate
    /// if they prefer lifecycle-driven registration).
    pub fn register_pending_order(
        &mut self,
        client_order_id: &str,
        symbol: &str,
        side: Side,
        price: f64,
        quantity: f64,
    ) {
        if client_order_id.is_empty() || quantity <= 0.0 { return; }
        self.pending_orders.insert(client_order_id.to_string(), PendingOrder {
            client_order_id: client_order_id.to_string(),
            symbol: symbol.to_string(),
            side,
            price,
            remaining_quantity: quantity,
        });
    }

    pub fn remove_pending_order(&mut self, client_order_id: &str) {
        self.pending_orders.remove(client_order_id);
    }

    pub fn pending_orders(&self) -> &std::collections::BTreeMap<String, PendingOrder> {
        &self.pending_orders
    }

    pub fn maker_volume(&self) -> f64 { self.maker_volume }
    pub fn taker_volume(&self) -> f64 { self.taker_volume }

    /// All positions as a `HashMap<symbol, Position>`, built on demand from
    /// the ledger. Symbols with 0 quantity are included if they ever had a
    /// fill or a seeded init entry.
    pub fn positions(&self) -> HashMap<String, Position> {
        let mut syms: std::collections::HashSet<String> =
            self.init_positions.keys().cloned().collect();
        for t in self.trades.values() {
            if t.status == TradeStatus::Failed { continue; }
            syms.insert(t.asset_id.clone());
        }
        let mut out = HashMap::with_capacity(syms.len());
        for sym in syms {
            if let Some(p) = self.get(&sym) {
                out.insert(sym, p);
            }
        }
        out
    }

    /// Trade ledger (read-only).
    pub fn trades(&self) -> &std::collections::BTreeMap<String, TradeRecord> {
        &self.trades
    }

    /// Total assets = cash balance + Σ(position × mid_price) from the orderbook.
    pub fn total_assets(&self, orderbook_manager: &OrderbookManager) -> f64 {
        let mut total = self.balance();
        for (symbol, pos) in &self.positions() {
            if let Some(mid) = orderbook_manager.mid_price(symbol) {
                total += pos.quantity * mid;
            }
        }
        total
    }

    pub fn log_positions(&self) {
        info!("[PositionManager] balance={:.4} maker_vol={:.4} taker_vol={:.4} trades={}",
            self.balance(), self.maker_volume, self.taker_volume, self.trades.len());
        let positions = self.positions();
        if positions.is_empty() {
            info!("[PositionManager] No positions");
            return;
        }
        for (symbol, pos) in &positions {
            info!(
                "[PositionManager] {} qty={:.4} avg_price={:.4}",
                symbol, pos.quantity, pos.avg_price,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn upsert(pm: &mut PositionManager, id: &str, status: TradeStatus) -> UpsertResult {
        pm.upsert_trade(id, "TOKEN", Side::Buy, 5.0, 0.4, status, true, 0.0, 0.0, None)
    }

    #[test]
    fn first_sighting_returns_add_sign() {
        let mut pm = PositionManager::new();
        let r = upsert(&mut pm, "t1", TradeStatus::Matched);
        assert!(r.applied);
        assert_eq!(r.accumulator_sign, 1);
        assert!((pm.maker_volume - 2.0).abs() < 1e-9);
    }

    #[test]
    fn lifecycle_transitions_dont_reaccumulate() {
        // MATCHED → MINED → CONFIRMED on the same trade_id should add
        // exactly once, not three times. This is the bug strategy.rs:4155
        // had before the refactor.
        let mut pm = PositionManager::new();
        let a = upsert(&mut pm, "t1", TradeStatus::Matched);
        let b = upsert(&mut pm, "t1", TradeStatus::Mined);
        let c = upsert(&mut pm, "t1", TradeStatus::Confirmed);
        assert_eq!(a.accumulator_sign, 1);
        assert_eq!(b.accumulator_sign, 0);
        assert_eq!(c.accumulator_sign, 0);
        assert!((pm.maker_volume - 2.0).abs() < 1e-9, "got {}", pm.maker_volume);
    }

    #[test]
    fn repeated_same_status_is_noop() {
        // Idempotency for periodic gap-replay: re-pushing the same status
        // must not re-accumulate or re-log.
        let mut pm = PositionManager::new();
        assert_eq!(upsert(&mut pm, "t1", TradeStatus::Matched).accumulator_sign, 1);
        let dup = upsert(&mut pm, "t1", TradeStatus::Matched);
        assert!(!dup.applied, "same-status re-push should be NOOP");
        assert_eq!(dup.accumulator_sign, 0);
    }

    #[test]
    fn earlier_status_does_not_reverse() {
        // An out-of-order earlier stage (Matched after Mined) must be rejected
        // so it can't roll the ledger backwards.
        let mut pm = PositionManager::new();
        upsert(&mut pm, "t1", TradeStatus::Matched);
        upsert(&mut pm, "t1", TradeStatus::Mined);
        assert!(!upsert(&mut pm, "t1", TradeStatus::Matched).applied);
    }

    #[test]
    fn terminal_is_immutable() {
        let mut pm = PositionManager::new();
        assert_eq!(upsert(&mut pm, "t1", TradeStatus::Confirmed).accumulator_sign, 1);
        assert!(!upsert(&mut pm, "t1", TradeStatus::Matched).applied);
        assert!(!upsert(&mut pm, "t1", TradeStatus::Failed).applied);
    }

    #[test]
    fn failed_after_match_reverses() {
        let mut pm = PositionManager::new();
        let a = upsert(&mut pm, "t1", TradeStatus::Matched);
        let b = upsert(&mut pm, "t1", TradeStatus::Failed);
        assert_eq!(a.accumulator_sign, 1);
        assert_eq!(b.accumulator_sign, -1);
        assert!(pm.maker_volume.abs() < 1e-9, "got {}", pm.maker_volume);
        // Position derivation already excludes Failed trades.
        let q = pm.get_quantity("TOKEN");
        assert!(q.abs() < 1e-9, "expected 0, got {}", q);
    }

    #[test]
    fn fresh_failed_doesnt_accumulate() {
        // First sighting that already lands as Failed (e.g. ws missed
        // the earlier states) should record the trade for completeness
        // but never affect accumulators or position.
        let mut pm = PositionManager::new();
        let r = upsert(&mut pm, "t1", TradeStatus::Failed);
        assert!(r.applied);
        assert_eq!(r.accumulator_sign, 0);
        assert!(pm.maker_volume.abs() < 1e-9);
        assert!(pm.get_quantity("TOKEN").abs() < 1e-9);
    }

    #[test]
    fn terminal_repush_is_noop() {
        let mut pm = PositionManager::new();
        let _ = upsert(&mut pm, "t1", TradeStatus::Confirmed);
        let r = upsert(&mut pm, "t1", TradeStatus::Failed);
        assert!(!r.applied, "Confirmed→Failed re-push must be rejected");
        assert_eq!(r.accumulator_sign, 0);
    }
}
