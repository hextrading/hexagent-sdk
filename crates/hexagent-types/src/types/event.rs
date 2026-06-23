use serde::{Deserialize, Serialize};

use super::instrument::Instrument;
use super::market::{BarData, Exchange, OrderBookSnapshot, QuoteTick, Side, SpotPrice, TickSizeChange, TradeTick};
use super::order::OrderRequest;

/// Events flowing from market data sources to the strategy engine
#[derive(
    Debug, Clone, Serialize, Deserialize,
)]
pub enum MarketEvent {
    OrderBook(OrderBookSnapshot),
    Trade(TradeTick),
    Quote(QuoteTick),
    Bar(BarData),
    TickSizeChange(TickSizeChange),
    SpotPrice(SpotPrice),
    Instrument(Instrument),
    Connected { exchange: Exchange },
    Disconnected { exchange: Exchange, reason: String },
    /// Signals the start of a new event for continuous recording (e.g. Polymarket series rotation).
    EventStart {
        exchange: Exchange,
        symbol: String,
        event_id: String,
        event_start_ns: u64,
    },
    Exit,
}

/// Signals from strategy to execution (internal only, no serialization needed)
#[derive(Debug, Clone)]
pub enum Signal {
    NewOrder(OrderRequest),
    CancelOrder {
        exchange: Exchange,
        client_order_id: String,
        #[allow(dead_code)]
        instance_id: String,
        /// Strategy-side emission time (ns). Executor drops the request if
        /// the queue lag exceeds `stale_signal_threshold_ms`, returning
        /// `OrderStatus::ExecutorRejected`.
        timestamp_ns: u64,
    },
    CancelAll {
        exchange: Exchange,
        symbol: String,
        instance_id: String,
        timestamp_ns: u64,
    },
    /// Batch place orders for the same market (single API call).
    BatchNewOrders {
        exchange: Exchange,
        market_id: String,
        orders: Vec<OrderRequest>,
        /// Strategy instance ID for routing to the correct per-account
        /// LiveRouter / SharedState. Set explicitly even though the
        /// per-`OrderRequest` field carries the same value, so the
        /// extractor can still resolve the id when `orders` is empty
        /// (e.g. when cancel-only batches funnel through the same
        /// dispatch path).
        instance_id: String,
    },
    /// Batch cancel orders for the same market (single API call).
    BatchCancelOrders {
        exchange: Exchange,
        market_id: String,
        client_order_ids: Vec<String>,
        instance_id: String,
        timestamp_ns: u64,
    },
    /// Batch update: cancel + place in a single atomic request.
    BatchUpdateOrders {
        exchange: Exchange,
        market_id: String,
        cancel_client_order_ids: Vec<String>,
        place_orders: Vec<OrderRequest>,
        timestamp_ns: u64,
        /// Strategy instance ID — see [`Signal::BatchNewOrders.instance_id`].
        instance_id: String,
    },
    /// Request the executor to reconcile orphan Polymarket orders whose
    /// placement or cancel HTTP timed out.
    ///   - `pending_places`: (coid, symbol, side, price, order_hash) where
    ///     `order_hash` is the pre-computed EIP-712 hash == Polymarket
    ///     server `orderID`. When present, reconcile queries the order
    ///     directly by ID (`GET /data/order/{id}`) for a deterministic
    ///     LIVE / MATCHED / CANCELED / 404 answer. When `None`, fall back
    ///     to matching against the snapshot by (asset_id, side, price).
    ///   - `pending_cancels`: (coid, server order_id) — query that specific
    ///     order's status and emit the resolved OrderUpdate.
    ReconcilePolymarket {
        pending_places: Vec<(String, String, Side, f64, Option<String>)>,
        pending_cancels: Vec<(String, String)>,
        /// Strategy instance ID — reconcile-by-orderID hits the
        /// per-account `/data/order/{id}` endpoint, so the executor
        /// must route this to the matching SharedState's auth.
        instance_id: String,
    },
    /// Emergency wipe: cancel **every** active maker order across all
    /// markets via Polymarket's `DELETE /cancel-all` endpoint, then
    /// clear local executor tracking (open_orders / coid maps). Used
    /// by polymaker when accumulated orphan count exceeds
    /// `max_orphans` — at that point local <-> server state has
    /// diverged enough that the safe move is to wipe the slate and
    /// let the next quote tick rebuild fresh quotes.
    PolymarketCancelAllOrders {
        reason: String,
        /// Strategy instance ID — `DELETE /cancel-all` is per-account,
        /// so the executor routes to the matching SharedState.
        instance_id: String,
    },
    Exit,
}

impl MarketEvent {
    pub fn timestamp_ns(&self) -> u64 {
        match self {
            MarketEvent::OrderBook(ob) => ob.local_timestamp_ns,
            MarketEvent::Trade(t) => t.local_timestamp_ns,
            MarketEvent::Quote(q) => q.local_timestamp_ns,
            MarketEvent::Bar(b) => b.local_timestamp_ns,
            MarketEvent::TickSizeChange(ts) => ts.local_timestamp_ns,
            MarketEvent::SpotPrice(sp) => sp.local_timestamp_ns,
            MarketEvent::EventStart { event_start_ns, .. } => *event_start_ns,
            MarketEvent::Instrument(_)
            | MarketEvent::Connected { .. }
            | MarketEvent::Disconnected { .. }
            | MarketEvent::Exit => crate::types::now_ns(),
        }
    }

    /// Server-side / exchange timestamp (for SimExchange ordering).
    pub fn exchange_timestamp_ns(&self) -> u64 {
        match self {
            MarketEvent::OrderBook(ob) => ob.exchange_timestamp_ns,
            MarketEvent::Trade(t) => t.exchange_timestamp_ns,
            MarketEvent::Quote(q) => q.exchange_timestamp_ns,
            MarketEvent::Bar(b) => b.exchange_timestamp_ns,
            MarketEvent::TickSizeChange(ts) => ts.local_timestamp_ns, // no separate exchange ts
            MarketEvent::SpotPrice(sp) => sp.timestamp_ns,
            // Instrument, EventStart, etc. — use local timestamp (same as timestamp_ns)
            _ => self.timestamp_ns(),
        }
    }

    pub fn exchange(&self) -> Exchange {
        match self {
            MarketEvent::OrderBook(ob) => ob.exchange,
            MarketEvent::Trade(t) => t.exchange,
            MarketEvent::Quote(q) => q.exchange,
            MarketEvent::Bar(b) => b.exchange,
            MarketEvent::TickSizeChange(ts) => ts.exchange,
            MarketEvent::SpotPrice(_) => Exchange::Polymarket,
            MarketEvent::Instrument(inst) => match inst {
                Instrument::Spot(s) => s.exchange,
                Instrument::BinaryOption(bo) => bo.exchange,
            },
            MarketEvent::Connected { exchange }
            | MarketEvent::Disconnected { exchange, .. }
            | MarketEvent::EventStart { exchange, .. } => *exchange,
            MarketEvent::Exit => Exchange::Binance, // placeholder, never used meaningfully
        }
    }
}
