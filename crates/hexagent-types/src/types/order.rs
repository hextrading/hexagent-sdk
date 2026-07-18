use serde::{Deserialize, Serialize};

use super::market::{Exchange, Side};

/// Marker attached to a terminal `OrderUpdate` produced by an explicit orphan
/// reconciliation GET. Consumers use it to distinguish a fresh server audit
/// from a possibly delayed private-stream lifecycle update.
pub const ORPHAN_RECONCILE_AUTHORITATIVE_TERMINAL: &str =
    "orphan_reconcile_authoritative_terminal";

fn default_true_fn() -> bool { true }

/// Order type
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Market,
    Limit,
    LimitMaker,
    /// Fill-and-Kill: cross the book for what's available, cancel the
    /// rest. Polymarket wire value `FAK`. Taker-only by definition —
    /// any maker portion is rejected. Emitted by polymaker when a
    /// crossing quote uses `taker_cross_use_fak`.
    Fak,
    /// Fill-or-Kill: cross the book for the entire size or cancel.
    /// Polymarket wire value `FOK`. Reserved for future use; not
    /// currently emitted by any strategy.
    Fok,
}

/// Whether the fill was maker or taker.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Liquidity {
    Maker,
    Taker,
}

/// Order lifecycle status
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Pending,
    Accepted,
    PartiallyFilled,
    Filled,
    /// On-chain settlement of a previously-matched fill reverted (Polymarket
    /// emits `FAILED` over the user WS feed). The fill must be reversed in
    /// the ledger and any per-event accumulators (volume/cashflow/fees).
    /// Distinct from `Rejected` (= order placement was refused) and
    /// `Cancelled` (= resting order taken off the book without filling).
    Failed,
    Cancelled,
    Rejected,
    /// Executor dropped the signal before sending because it was too stale
    /// (queue-congestion guard). Semantics depend on the original operation:
    /// - placement → treat like Cancelled (never reached the exchange)
    /// - cancel → no-op (the resting order is still live on the exchange;
    ///   retry on the next cycle, same as an HTTP error)
    ExecutorRejected,
    /// HTTP POST /order timed out. Outcome unknown — strategy should
    /// reconcile against the exchange's open-order set.
    NewOrderTimeout,
    /// HTTP DELETE /order or /orders timed out. Outcome unknown — strategy
    /// should re-query the specific order_id's status.
    CancelOrderTimeout,
}

/// Request to place a new order
#[derive(
    Debug, Clone, Serialize, Deserialize,
)]
pub struct OrderRequest {
    pub client_order_id: String,
    pub exchange: Exchange,
    pub symbol: String,
    pub side: Side,
    pub order_type: OrderType,
    pub price: Option<f64>,
    pub quantity: f64,
    pub timestamp_ns: u64,
    /// Strategy instance ID for routing to the correct executor/wallet.
    #[serde(default)]
    pub instance_id: String,
    /// Fee rate in basis points (Polymarket market-specific, 0 = use default).
    #[serde(default)]
    pub fee_rate_bps: u32,
    /// If true, order is post-only (maker only, rejected if it would cross spread).
    #[serde(default = "default_true_fn")]
    pub post_only: bool,
    /// If true, the order may only reduce (never increase or flip) the current
    /// position — the venue caps/rejects the portion that would open. Used for
    /// flatten / close-only quotes so they can't overshoot into the opposite
    /// side. Venues that don't support it ignore the flag. Default false.
    #[serde(default)]
    pub reduce_only: bool,
    /// Optional human-readable label for the outcome / token this order
    /// targets (e.g. "Up", "Down"). Populated by the strategy before
    /// emission; empty for exchanges / strategies that don't need it.
    /// Used only for logging — no business logic depends on this.
    #[serde(default)]
    pub outcome_label: String,
}

/// Exact metadata from an authenticated, order-specific reconciliation GET.
///
/// Quantities remain strings because the exchange API is fixed-point; this
/// preserves terminal dust exactly for reconciliation diagnostics.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeOrderAudit {
    #[serde(default)]
    pub original_size: Option<String>,
    #[serde(default)]
    pub size_matched: Option<String>,
    /// Base Polymarket trade IDs associated with this order. Maker private
    /// updates use `<trade_id>:<order_id>` as their ledger key.
    #[serde(default)]
    pub associate_trades: Vec<String>,
}

/// Update on an existing order
#[derive(
    Debug, Clone, Serialize, Deserialize,
)]
pub struct OrderUpdate {
    pub client_order_id: String,
    pub exchange: Exchange,
    pub symbol: String,
    pub side: Side,
    pub exchange_order_id: Option<String>,
    pub status: OrderStatus,
    pub liquidity: Option<Liquidity>,
    pub filled_quantity: f64,
    pub remaining_quantity: f64,
    pub avg_fill_price: f64,
    pub timestamp_ns: u64,
    /// Stable identifier for a single fill. Populated on trade-push events
    /// (Polymarket WebSocket "trade"); None on order-lifecycle updates
    /// (placement/update/cancel). The `PositionManager` uses it as the
    /// primary key for its trade ledger so that status transitions
    /// (Matched → Mined → Confirmed / Failed) update the same record instead
    /// of double-counting.
    #[serde(default)]
    pub trade_id: Option<String>,
    /// Present only on an authoritative order-specific GET result.
    #[serde(default)]
    pub order_audit: Option<AuthoritativeOrderAudit>,
    /// Server-provided error string for rejected orders. Strategies use this
    /// to distinguish rejection causes — e.g. "invalid post-only order: order
    /// crosses book" lets the strategy refresh its inferred top of book. A
    /// terminal update emitted by an explicit orphan-reconcile GET uses
    /// [`ORPHAN_RECONCILE_AUTHORITATIVE_TERMINAL`] as an origin marker; normal
    /// private-stream lifecycle updates leave this field empty.
    #[serde(default)]
    pub error: Option<String>,
}

/// A single fill record for backtest results (serialized to JSON).
#[derive(Debug, Clone, Serialize)]
pub struct BacktestFill {
    pub event_id: String,
    pub condition_id: String,
    pub timestamp_ns: u64,
    pub symbol_id: String,
    pub symbol_outcome: String,
    pub side: Side,
    pub price: f64,
    pub quantity: f64,
    pub client_order_id: String,
}

impl OrderRequest {
    pub fn new_limit(
        exchange: Exchange,
        symbol: String,
        side: Side,
        price: f64,
        quantity: f64,
    ) -> Self {
        Self {
            client_order_id: uuid::Uuid::new_v4().to_string(),
            exchange,
            symbol,
            side,
            order_type: OrderType::Limit,
            price: Some(price),
            quantity,
            timestamp_ns: crate::types::now_ns(),
            instance_id: String::new(),
            fee_rate_bps: 0,
            post_only: true,
            reduce_only: false,
            outcome_label: String::new(),
        }
    }

    pub fn new_market(
        exchange: Exchange,
        symbol: String,
        side: Side,
        quantity: f64,
    ) -> Self {
        Self {
            client_order_id: uuid::Uuid::new_v4().to_string(),
            exchange,
            symbol,
            side,
            order_type: OrderType::Market,
            price: None,
            quantity,
            timestamp_ns: crate::types::now_ns(),
            instance_id: String::new(),
            fee_rate_bps: 0,
            post_only: false, // market orders are not post-only
            reduce_only: false,
            outcome_label: String::new(),
        }
    }
}
