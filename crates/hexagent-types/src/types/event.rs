use arrayvec::ArrayVec;
use serde::{Deserialize, Serialize};

use super::instrument::Instrument;
use super::market::{
    AssetCtxTick, BarData, Exchange, OrderBookSnapshot, QuoteTick, Side, SpotPrice, TickSizeChange,
    TradeTick,
};
use super::order::{OrderRequest, OrderUpdate};

/// Hard upper bound for one exchange batch. Keeping the payload inline makes
/// the strategy-to-execution handoff allocation-free after the strings owned
/// by the individual orders have been constructed.
pub const ORDER_BATCH_CAPACITY: usize = 16;

/// Maximum number of strategy signals produced by one callback. The storage
/// is inline and reused by the strategy owner, so steady-state quote and
/// lifecycle dispatch cannot grow a heap-backed `Vec`.
pub const SIGNAL_BATCH_CAPACITY: usize = 64;

/// Maximum lifecycle results produced by one bounded execution command.
/// Batch place/update is already capped at 16+16 entries; the wider bound
/// leaves room for venue-specific control acknowledgements without growing a
/// `Vec` on the connection-owner thread.
pub const ORDER_UPDATE_BATCH_CAPACITY: usize = 64;

/// Fixed-capacity place payload used by all batch/replace signals.
pub type OrderBatch = ArrayVec<OrderRequest, ORDER_BATCH_CAPACITY>;

/// Fixed-capacity client-order-id payload used by cancel/update signals.
pub type OrderIdBatch = ArrayVec<String, ORDER_BATCH_CAPACITY>;

/// Fixed callback output shared by quote, lifecycle, health and watchdog
/// dispatch. Producers must surface `try_push` failure; silently truncating a
/// cancel or risk-control signal is not permitted.
pub type SignalBatch = ArrayVec<Signal, SIGNAL_BATCH_CAPACITY>;

/// Reusable fixed output buffer for execution/lifecycle adapters.
pub type OrderUpdateBatch = ArrayVec<OrderUpdate, ORDER_UPDATE_BATCH_CAPACITY>;

#[derive(Debug)]
pub struct SignalBatchOverflow {
    pub signal: Signal,
}

#[derive(Debug)]
pub struct OrderUpdateBatchOverflow {
    pub update: OrderUpdate,
}

#[inline]
pub fn push_order_update(
    out: &mut OrderUpdateBatch,
    update: OrderUpdate,
) -> Result<(), OrderUpdateBatchOverflow> {
    out.try_push(update)
        .map_err(|error| OrderUpdateBatchOverflow {
            update: error.element(),
        })
}

#[inline]
pub fn extend_order_update_batch(
    out: &mut OrderUpdateBatch,
    updates: impl IntoIterator<Item = OrderUpdate>,
) -> Result<(), OrderUpdateBatchOverflow> {
    for update in updates {
        push_order_update(out, update)?;
    }
    Ok(())
}

#[inline]
pub fn extend_signal_batch(
    out: &mut SignalBatch,
    signals: impl IntoIterator<Item = Signal>,
) -> Result<(), SignalBatchOverflow> {
    for signal in signals {
        out.try_push(signal).map_err(|error| SignalBatchOverflow {
            signal: error.element(),
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod fixed_batch_tests {
    use super::*;

    #[test]
    fn fixed_order_id_batch_rejects_capacity_plus_one() {
        let mut batch = OrderIdBatch::new();
        for index in 0..ORDER_BATCH_CAPACITY {
            batch.try_push(index.to_string()).unwrap();
        }
        assert_eq!(batch.len(), ORDER_BATCH_CAPACITY);
        assert!(batch.try_push("overflow".to_string()).is_err());
    }

    #[test]
    fn fixed_signal_batch_returns_the_first_unqueued_signal() {
        let mut batch = SignalBatch::new();
        let signals = (0..=SIGNAL_BATCH_CAPACITY).map(|index| Signal::CancelAll {
            exchange: Exchange::Polymarket,
            symbol: index.to_string(),
            instance_id: "maker".to_string(),
            timestamp_ns: 1,
        });
        let overflow = extend_signal_batch(&mut batch, signals).unwrap_err();
        assert_eq!(batch.len(), SIGNAL_BATCH_CAPACITY);
        match overflow.signal {
            Signal::CancelAll { symbol, .. } => {
                assert_eq!(symbol, SIGNAL_BATCH_CAPACITY.to_string())
            }
            other => panic!("unexpected overflow signal: {other:?}"),
        }
    }
    #[test]
    fn fixed_order_update_batch_returns_the_first_unqueued_update() {
        let mut batch = OrderUpdateBatch::new();
        for sequence in 0..ORDER_UPDATE_BATCH_CAPACITY {
            push_order_update(&mut batch, test_order_update(sequence)).unwrap();
        }
        let overflow = push_order_update(
            &mut batch,
            test_order_update(ORDER_UPDATE_BATCH_CAPACITY),
        )
        .unwrap_err();
        assert_eq!(batch.len(), ORDER_UPDATE_BATCH_CAPACITY);
        assert_eq!(
            overflow.update.client_order_id,
            ORDER_UPDATE_BATCH_CAPACITY.to_string()
        );
    }

    fn test_order_update(sequence: usize) -> OrderUpdate {
        OrderUpdate {
            order_slot: super::super::order::OrderSlot::UNASSIGNED,
            client_order_id: sequence.to_string(),
            exchange: Exchange::Polymarket,
            symbol: String::new(),
            side: Side::Buy,
            exchange_order_id: None,
            status: super::super::order::OrderStatus::Accepted,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: 0.0,
            avg_fill_price: 0.0,
            timestamp_ns: 0,
            exchange_event_timestamp_ns: None,
            trade_id: None,
            order_audit: None,
            error: None,
        }
    }
}

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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    Connected {
        exchange: Exchange,
    },
    Disconnected {
        exchange: Exchange,
        reason: String,
    },
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
        orders: OrderBatch,
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
        client_order_ids: OrderIdBatch,
        instance_id: String,
        timestamp_ns: u64,
    },
    /// Batch update: cancel + place in a single atomic request.
    BatchUpdateOrders {
        exchange: Exchange,
        market_id: String,
        cancel_client_order_ids: OrderIdBatch,
        place_orders: OrderBatch,
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
        cancel_client_order_ids: OrderIdBatch,
        place_orders: OrderBatch,
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
            MarketEvent::OrderBook(ob) => ob
                .bids
                .iter()
                .chain(ob.asks.iter())
                .all(|level| level.price.is_finite() && level.quantity.is_finite()),
            MarketEvent::Trade(trade) => trade.price.is_finite() && trade.quantity.is_finite(),
            MarketEvent::Quote(quote) => [
                quote.bid_price,
                quote.bid_qty,
                quote.ask_price,
                quote.ask_qty,
            ]
            .into_iter()
            .all(f64::is_finite),
            MarketEvent::Bar(bar) => [
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
                bar.taker_buy_base,
                bar.quote_volume,
            ]
            .into_iter()
            .all(f64::is_finite),
            MarketEvent::TickSizeChange(change) => {
                change.old_tick_size.is_finite() && change.new_tick_size.is_finite()
            }
            MarketEvent::SpotPrice(spot) => spot.price.is_finite(),
            MarketEvent::AssetCtx(ctx) => [
                ctx.mark_px,
                ctx.oracle_px,
                ctx.mid_px,
                ctx.funding,
                ctx.open_interest,
                ctx.premium,
                ctx.impact_bid_px,
                ctx.impact_ask_px,
                ctx.day_ntl_vlm,
                ctx.prev_day_px,
            ]
            .into_iter()
            .all(f64::is_finite),
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
            bids: vec![PriceLevel {
                price: f64::NAN,
                quantity: 1.0,
            }],
            asks: vec![PriceLevel {
                price: 100.0,
                quantity: 1.0,
            }],
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
