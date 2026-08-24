use serde::{Deserialize, Serialize};
use std::fmt;

use super::market::{Exchange, Side};

/// Marker attached to a terminal `OrderUpdate` produced by an explicit orphan
/// reconciliation GET. Consumers use it to distinguish a fresh server audit
/// from a possibly delayed private-stream lifecycle update.
pub const ORPHAN_RECONCILE_AUTHORITATIVE_TERMINAL: &str = "orphan_reconcile_authoritative_terminal";

/// Marker attached to an `ExecutorRejected` update when the dedicated
/// Polymarket reconcile pool had no free permit and therefore sent no HTTP
/// request. Strategies must treat this as admission feedback only: clear the
/// reconcile in-flight/backoff state, but do not mutate the ordinary order
/// lifecycle as if a placement or cancel signal had been rejected.
pub const ORPHAN_RECONCILE_DEFERRED: &str = "orphan_reconcile_deferred";

/// Prefix on an ambiguous cancel-reconcile update whose next authoritative
/// GET has an executor-owned backoff. The strategy mirrors this deadline so
/// it does not mark the coid in-flight and then wait its unrelated 4.5-second
/// lost-callback TTL. Optional diagnostic fields follow after `;`.
pub const ORPHAN_RECONCILE_RETRY_AFTER_MS_PREFIX: &str = "orphan_reconcile_retry_after_ms=";

pub fn orphan_reconcile_retry_after_ms(error: Option<&str>) -> Option<u64> {
    error?
        .strip_prefix(ORPHAN_RECONCILE_RETRY_AFTER_MS_PREFIX)?
        .split(';')
        .next()?
        .parse::<u64>()
        .ok()
}

/// Prefix attached to the synthetic executor update that proves a
/// market-scoped Polymarket expiry cancel reached order/trade finality.
/// `client_order_id` carries the owning strategy instance and `symbol` carries
/// the condition id; this update is control-plane feedback, not an order
/// lifecycle transition.
pub const POLYMARKET_MARKET_CANCEL_FINALITY_CONFIRMED: &str =
    "polymarket_market_cancel_finality_confirmed";

/// Prefix attached when the market-scoped expiry cancel or its follow-up
/// order/trade audit is still unavailable. The strategy must remain
/// unsettled and retry; details may follow this prefix after `: `.
pub const POLYMARKET_MARKET_CANCEL_FINALITY_PENDING: &str =
    "polymarket_market_cancel_finality_pending";

fn default_true_fn() -> bool {
    true
}

#[cfg(test)]
mod orphan_reconcile_diagnostic_tests {
    use super::*;

    #[test]
    fn parses_retry_delay_without_consuming_diagnostic_suffix() {
        assert_eq!(
            orphan_reconcile_retry_after_ms(Some(
                "orphan_reconcile_retry_after_ms=582;evidence=unavailable;attempt=1",
            )),
            Some(582),
        );
        assert_eq!(orphan_reconcile_retry_after_ms(Some("other")), None);
        assert_eq!(orphan_reconcile_retry_after_ms(None), None);
    }
}

/// Order type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Liquidity {
    Maker,
    Taker,
}

/// Order lifecycle status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
    /// DELETE /order got a fast, healthy reply whose wording is ambiguous
    /// (canonically "order can't be found - already canceled or matched"),
    /// or reconcile evidence that is not yet authoritative. The order is no
    /// longer cancelable but Cancelled-vs-Filled is undecided. Consumers
    /// MUST handle this exactly like [`OrderStatus::CancelOrderTimeout`]
    /// (orphan reconcile, worst-case reservation kept). The split exists so
    /// transport timeouts (2 s cap-hits) and healthy-but-ambiguous replies
    /// stay separable in logs, metrics, and sim latency calibration —
    /// 2026-07-31 live: 91% of "CancelOrderTimeout" were actually ~50 ms
    /// not-found replies.
    CancelUncertain,
}

/// Allocation-free quote origin carried across strategy → executor → exchange.
/// Symbol/event identity already lives in `OrderRequest`, so the hot path only
/// needs this compact discriminator instead of formatting a descriptive String.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuoteTriggerSource {
    #[default]
    Unknown,
    StrategyCallback,
    OrderBook(Exchange),
    OrderUpdateRequote,
    CancelAckLegRequote,
    ClobHealthRecovery,
}

impl QuoteTriggerSource {
    pub fn from_callback_reason(reason: &str) -> Self {
        match reason {
            "order_update_requote" => Self::OrderUpdateRequote,
            "cancel_ack_leg_requote" => Self::CancelAckLegRequote,
            "clob_health_recovery" => Self::ClobHealthRecovery,
            _ => Self::StrategyCallback,
        }
    }

    pub fn is_unknown(self) -> bool {
        self == Self::Unknown
    }
}

impl fmt::Display for QuoteTriggerSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => f.write_str("unknown"),
            Self::StrategyCallback => f.write_str("strategy_callback"),
            Self::OrderBook(exchange) => write!(f, "orderbook:{exchange}"),
            Self::OrderUpdateRequote => f.write_str("order_update_requote"),
            Self::CancelAckLegRequote => f.write_str("cancel_ack_leg_requote"),
            Self::ClobHealthRecovery => f.write_str("clob_health_recovery"),
        }
    }
}

/// Owner-local, numeric order routing slot. Zero-based values address a fixed
/// strategy table directly; `UNASSIGNED` marks legacy/private sources that
/// have not yet published a numeric identity and must fail closed or use a
/// cold-path reconciliation lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OrderSlot(u16);

impl OrderSlot {
    pub const UNASSIGNED: Self = Self(u16::MAX);

    #[inline]
    pub const fn new(index: u16) -> Self {
        Self(index)
    }

    #[inline]
    pub const fn index(self) -> Option<usize> {
        if self.0 == u16::MAX {
            None
        } else {
            Some(self.0 as usize)
        }
    }

    #[inline]
    pub const fn is_assigned(self) -> bool {
        self.0 != u16::MAX
    }
}

impl Default for OrderSlot {
    fn default() -> Self {
        Self::UNASSIGNED
    }
}

/// Request to place a new order
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    /// Fixed owner-local routing identity, echoed by execution and lifecycle.
    #[serde(default)]
    pub order_slot: OrderSlot,
    pub client_order_id: String,
    pub exchange: Exchange,
    pub symbol: String,
    pub side: Side,
    pub order_type: OrderType,
    pub price: Option<f64>,
    pub quantity: f64,
    /// Exchange timestamp of the market-data event that triggered this quote.
    /// Kept separate from `timestamp_ns`, which is the strategy's wall-clock
    /// emission time and is used by executor staleness admission.
    #[serde(default)]
    pub quote_trigger_exchange_timestamp_ns: u64,
    /// Local receive timestamp of the market-data event that triggered this
    /// quote. This is the preferred origin for end-to-end latency because it
    /// uses the same host clock as every downstream lifecycle stage.
    #[serde(default)]
    pub quote_trigger_local_timestamp_ns: u64,
    /// Stable event/market identifier supplied by the strategy (for example a
    /// Polymarket condition id). Logging-only; never used for order routing.
    #[serde(default)]
    pub quote_event_id: String,
    /// Compact allocation-free origin. The order symbol/event fields provide
    /// the remaining human-readable correlation.
    #[serde(default)]
    pub quote_trigger_source: QuoteTriggerSource,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderUpdate {
    /// Fixed owner-local routing identity copied from the originating request.
    /// Authenticated private events may be unassigned until replay/reconcile
    /// resolves them outside the quote path.
    #[serde(default)]
    pub order_slot: OrderSlot,
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
    /// Venue-supplied wall-clock timestamp for the originating private event.
    /// `timestamp_ns` remains the local producer clock used for internal queue
    /// latency; this optional field measures exchange-event-to-owner apply.
    #[serde(default)]
    pub exchange_event_timestamp_ns: Option<u64>,
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
            order_slot: OrderSlot::UNASSIGNED,
            client_order_id: uuid::Uuid::new_v4().to_string(),
            exchange,
            symbol,
            side,
            order_type: OrderType::Limit,
            price: Some(price),
            quantity,
            quote_trigger_exchange_timestamp_ns: 0,
            quote_trigger_local_timestamp_ns: 0,
            quote_event_id: String::new(),
            quote_trigger_source: QuoteTriggerSource::Unknown,
            timestamp_ns: crate::types::now_ns(),
            instance_id: String::new(),
            fee_rate_bps: 0,
            post_only: true,
            reduce_only: false,
            outcome_label: String::new(),
        }
    }

    pub fn new_market(exchange: Exchange, symbol: String, side: Side, quantity: f64) -> Self {
        Self {
            order_slot: OrderSlot::UNASSIGNED,
            client_order_id: uuid::Uuid::new_v4().to_string(),
            exchange,
            symbol,
            side,
            order_type: OrderType::Market,
            price: None,
            quantity,
            quote_trigger_exchange_timestamp_ns: 0,
            quote_trigger_local_timestamp_ns: 0,
            quote_event_id: String::new(),
            quote_trigger_source: QuoteTriggerSource::Unknown,
            timestamp_ns: crate::types::now_ns(),
            instance_id: String::new(),
            fee_rate_bps: 0,
            post_only: false, // market orders are not post-only
            reduce_only: false,
            outcome_label: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_origin_fields_are_backward_compatible() {
        let request = OrderRequest::new_limit(
            Exchange::Polymarket,
            "token".to_string(),
            Side::Buy,
            0.5,
            10.0,
        );
        let mut value = serde_json::to_value(request).unwrap();
        let object = value.as_object_mut().unwrap();
        object.remove("quote_trigger_exchange_timestamp_ns");
        object.remove("quote_trigger_local_timestamp_ns");
        object.remove("quote_event_id");
        object.remove("quote_trigger_source");

        let restored: OrderRequest = serde_json::from_value(value).unwrap();
        assert_eq!(restored.quote_trigger_exchange_timestamp_ns, 0);
        assert_eq!(restored.quote_trigger_local_timestamp_ns, 0);
        assert!(restored.quote_event_id.is_empty());
        assert!(restored.quote_trigger_source.is_unknown());
    }

    #[test]
    fn numeric_order_slot_round_trips_and_defaults_unassigned() {
        let mut request = OrderRequest::new_limit(
            Exchange::Polymarket,
            "token".to_string(),
            Side::Buy,
            0.5,
            10.0,
        );
        request.order_slot = OrderSlot::new(37);
        let encoded = serde_json::to_value(&request).unwrap();
        let restored: OrderRequest = serde_json::from_value(encoded).unwrap();
        assert_eq!(restored.order_slot, OrderSlot::new(37));

        let mut legacy = serde_json::to_value(request).unwrap();
        legacy.as_object_mut().unwrap().remove("order_slot");
        let restored: OrderRequest = serde_json::from_value(legacy).unwrap();
        assert_eq!(restored.order_slot, OrderSlot::UNASSIGNED);
    }
}
