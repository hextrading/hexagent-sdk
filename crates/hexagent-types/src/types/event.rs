use serde::{Deserialize, Serialize};

use super::instrument::Instrument;
use super::market::{AssetCtxTick, BarData, Exchange, OrderBookSnapshot, QuoteTick, Side, SpotPrice, TickSizeChange, TradeTick};
use super::order::OrderRequest;

/// Condition-scoped public market-data health.  Unlike `Connected` /
/// `Disconnected`, this does not describe the whole exchange transport: one
/// Polymarket condition can settle or repair its L2 while unrelated markets
/// remain fully tradeable.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MarketDataHealthState {
    Healthy,
    Settling,
    Repairing,
    Degraded,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketDataHealth {
    pub exchange: Exchange,
    /// Venue market identifier (Polymarket condition_id).
    pub market_id: String,
    /// Canonical routing symbol (the Up token for a binary market).
    pub symbol: String,
    pub state: MarketDataHealthState,
    pub passive_ready: bool,
    pub taker_ready: bool,
    pub reason: String,
    pub local_timestamp_ns: u64,
}

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
    /// Perp asset context (mark/oracle px, funding, OI) — e.g. Hyperliquid
    /// `activeAssetCtx`, ~1 msg/s per coin.
    AssetCtx(AssetCtxTick),
    Instrument(Instrument),
    MarketDataHealth(MarketDataHealth),
    Connected { exchange: Exchange },
    Disconnected { exchange: Exchange, reason: String },
    /// Signals the start of a new event for continuous recording (e.g. Polymarket series rotation).
    EventStart {
        exchange: Exchange,
        symbol: String,
        event_id: String,
        event_start_ns: u64,
    },
    /// Signals that one rotating event has retired and its dynamic market
    /// symbols will no longer produce authoritative public data. Consumers
    /// may reclaim symbol-scoped routing/coalescing state before the following
    /// `EventStart` installs the next event.
    EventEnd {
        exchange: Exchange,
        symbol: String,
        event_id: String,
        retired_symbols: Vec<String>,
        event_end_ns: u64,
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
    /// Replace order(s) — a reprice: cancel + place dispatched as one
    /// operation. First-class peer of `NewOrder` (place) / `CancelOrder`
    /// (cancel). Live venues dispatch the cancels and places fully
    /// concurrently on disjoint connection pools (no ordering between
    /// them — see `ExchangeTrade::replace_order`). Same field shape as
    /// `BatchUpdateOrders`; processed identically by the sim fill path.
    ReplaceOrder {
        exchange: Exchange,
        market_id: String,
        cancel_client_order_ids: Vec<String>,
        place_orders: Vec<OrderRequest>,
        timestamp_ns: u64,
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
        /// Base trade IDs named by an authoritative order audit but not yet
        /// observed in the strategy ledger. The executor fetches and replays
        /// them through the normal private-feed parser.
        pending_trade_ids: Vec<String>,
        /// Strategy instance ID — reconcile-by-orderID hits the
        /// per-account `/data/order/{id}` endpoint, so the executor
        /// must route this to the matching SharedState's auth.
        instance_id: String,
    },
    /// Cancel resting Polymarket orders server-side, independent of the
    /// executor's local order tracking — so it catches "forgotten" orders
    /// that were wrongly dropped from tracking (e.g. a `pending/delayed`
    /// cancel race or a `matched`-then-FAILED trade) and would otherwise
    /// rest unmanaged to settlement.
    ///
    /// * `market: Some(condition_id)` → `DELETE /cancel-market-orders`
    ///   scoped to that ONE market. The endpoint requires both `market`
    ///   AND `asset_id`, so the executor calls it once per `asset_ids`
    ///   entry (a binary market = both outcome tokens). Used at event
    ///   expiry: an account may trade several markets concurrently, so a
    ///   single event ending must NOT wipe the other markets' orders.
    /// * `market: None` → `DELETE /cancel-all` (whole account; `asset_ids`
    ///   ignored). Emergency wipe when accumulated orphan count exceeds
    ///   `max_orphans` and local <-> server state has diverged enough to
    ///   rebuild from scratch.
    ///
    /// Both clear the matching local executor tracking afterwards.
    PolymarketCancelAllOrders {
        reason: String,
        /// `Some(condition_id)` → market-scoped cancel; `None` → whole
        /// account. See variant docs.
        market: Option<String>,
        /// Outcome token_ids for the market-scoped cancel (the endpoint
        /// requires `asset_id`; one call per entry). Ignored when
        /// `market` is `None`.
        asset_ids: Vec<String>,
        /// Strategy instance ID — these endpoints are per-account, so the
        /// executor routes to the matching SharedState.
        instance_id: String,
    },
    /// Register one strategy instance's settled-FIFO ownership of an event's
    /// executor/account audit history. Registration is idempotent across
    /// restart: the corresponding retire only removes this instance's
    /// reference, and global cleanup waits for every sibling reference.
    RetainPolymarketEventAudit {
        condition_id: String,
        asset_ids: Vec<String>,
        instance_id: String,
    },
    /// Retire executor/account audit history only after the strategy's
    /// settled-event FIFO has evicted the corresponding event. Expiry cancel
    /// must not perform this cleanup: late trade lifecycle revisions remain
    /// routable for the whole settled-ledger retention window.
    RetirePolymarketEventAudit {
        condition_id: String,
        asset_ids: Vec<String>,
        instance_id: String,
    },
    /// Begin coordinated shutdown. The executor stops order-producing work,
    /// cancels/audits orders to finality, and emits all resulting updates
    /// before acknowledging the strategy.
    BeginShutdown,
    /// Terminal executor stop after the coordinated barrier and final report.
    Exit,
}

impl MarketEvent {
    /// Reject NaN/±Infinity before public market data crosses the SDK boundary.
    /// String-valued exchange fields can parse these as valid `f64`s even
    /// though JSON numbers cannot encode them.
    pub fn has_finite_market_values(&self) -> bool {
        match self {
            MarketEvent::OrderBook(ob) => ob.bids.iter().chain(ob.asks.iter())
                .all(|level| level.price.is_finite() && level.quantity.is_finite()),
            MarketEvent::Trade(trade) => trade.price.is_finite() && trade.quantity.is_finite(),
            MarketEvent::Quote(quote) => [quote.bid_price, quote.bid_qty, quote.ask_price, quote.ask_qty]
                .into_iter().all(f64::is_finite),
            MarketEvent::Bar(bar) => [bar.open, bar.high, bar.low, bar.close, bar.volume,
                bar.taker_buy_base, bar.quote_volume].into_iter().all(f64::is_finite),
            MarketEvent::TickSizeChange(change) => {
                change.old_tick_size.is_finite() && change.new_tick_size.is_finite()
            }
            MarketEvent::SpotPrice(spot) => spot.price.is_finite(),
            MarketEvent::AssetCtx(ctx) => [ctx.mark_px, ctx.oracle_px, ctx.mid_px, ctx.funding,
                ctx.open_interest, ctx.premium, ctx.impact_bid_px, ctx.impact_ask_px,
                ctx.day_ntl_vlm, ctx.prev_day_px].into_iter().all(f64::is_finite),
            MarketEvent::Instrument(_)
            | MarketEvent::MarketDataHealth(_)
            | MarketEvent::Connected { .. }
            | MarketEvent::Disconnected { .. }
            | MarketEvent::EventStart { .. }
            | MarketEvent::EventEnd { .. }
            | MarketEvent::Exit => true,
        }
    }

    pub fn timestamp_ns(&self) -> u64 {
        match self {
            MarketEvent::OrderBook(ob) => ob.local_timestamp_ns,
            MarketEvent::Trade(t) => t.local_timestamp_ns,
            MarketEvent::Quote(q) => q.local_timestamp_ns,
            MarketEvent::Bar(b) => b.local_timestamp_ns,
            MarketEvent::TickSizeChange(ts) => ts.local_timestamp_ns,
            MarketEvent::SpotPrice(sp) => sp.local_timestamp_ns,
            MarketEvent::AssetCtx(ac) => ac.local_timestamp_ns,
            MarketEvent::MarketDataHealth(health) => health.local_timestamp_ns,
            MarketEvent::EventStart { event_start_ns, .. } => *event_start_ns,
            MarketEvent::EventEnd { event_end_ns, .. } => *event_end_ns,
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
            MarketEvent::AssetCtx(ac) => ac.exchange,
            MarketEvent::Instrument(inst) => match inst {
                Instrument::Spot(s) => s.exchange,
                Instrument::BinaryOption(bo) => bo.exchange,
            },
            MarketEvent::MarketDataHealth(health) => health.exchange,
            MarketEvent::Connected { exchange }
            | MarketEvent::Disconnected { exchange, .. }
            | MarketEvent::EventStart { exchange, .. }
            | MarketEvent::EventEnd { exchange, .. } => *exchange,
            MarketEvent::Exit => Exchange::Binance, // placeholder, never used meaningfully
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PriceLevel;

    #[test]
    fn market_event_finite_validation_covers_string_parsed_payload_shapes() {
        let orderbook = MarketEvent::OrderBook(OrderBookSnapshot {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_string(),
            bids: vec![PriceLevel { price: f64::NAN, quantity: 1.0 }],
            asks: vec![PriceLevel { price: 100.0, quantity: 1.0 }],
            exchange_timestamp_ns: 1,
            local_timestamp_ns: 1,
        });
        assert!(!orderbook.has_finite_market_values());

        let trade = MarketEvent::Trade(TradeTick {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_string(),
            exchange_trade_id: None,
            price: 100.0,
            quantity: f64::INFINITY,
            side: Side::Buy,
            exchange_timestamp_ns: 1,
            local_timestamp_ns: 1,
        });
        assert!(!trade.has_finite_market_values());

        let valid = MarketEvent::SpotPrice(SpotPrice {
            source: "chainlink".to_string(),
            symbol: "btc/usd".to_string(),
            price: 100.0,
            timestamp_ns: 1,
            local_timestamp_ns: 1,
        });
        assert!(valid.has_finite_market_values());
    }
}
