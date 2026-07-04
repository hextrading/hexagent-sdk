pub mod binance;
pub mod bitget;
pub mod bybit;
pub mod chainlink;
pub mod coinbase;
pub mod gate;
pub mod hexmarket;
pub mod hyperliquid;
pub mod kucoin;
pub mod mexc;
pub mod kraken;
pub mod okx;
pub mod polymarket;
pub mod pyth;
pub mod paper;
pub mod sim;
pub mod sim_v2;

use std::time::Duration;

use crate::types::MarketEvent;
use crate::types::{Exchange, OrderRequest, OrderUpdate};
use anyhow::Result;

/// Exponential backoff with jitter for reconnection.
///
/// - First retry: `base_ms` (e.g. 100ms) for quick recovery from transient failures.
/// - Retries 1-3: fast ramp (< 2s).
/// - Retries 4-10: exponential growth up to `max_ms`.
/// - Beyond 10: constant `max_ms` interval (low-energy guard mode).
/// - Jitter: ±50% randomization to avoid thundering herd.
pub struct ReconnectBackoff {
    base_ms: u64,
    max_ms: u64,
    attempt: u32,
}

impl ReconnectBackoff {
    pub fn new(base_ms: u64, max_ms: u64) -> Self {
        Self { base_ms, max_ms, attempt: 0 }
    }

    /// Reset attempt counter (call on successful connection).
    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Compute next sleep duration and increment attempt counter.
    pub fn next_delay(&mut self) -> Duration {
        let wait = if self.attempt == 0 {
            self.base_ms
        } else {
            let exp = self.base_ms.saturating_mul(1u64 << self.attempt.min(15));
            exp.min(self.max_ms)
        };
        self.attempt = self.attempt.saturating_add(1);

        // Jitter: random(0.5, 1.5) × wait
        let jitter = 0.5 + rand_f64() * 1.0; // [0.5, 1.5)
        Duration::from_millis((wait as f64 * jitter) as u64)
    }
}

/// Simple pseudo-random f64 in [0, 1) using thread-local state.
fn rand_f64() -> f64 {
    use std::cell::Cell;
    thread_local! {
        static STATE: Cell<u64> = Cell::new(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64
        );
    }
    STATE.with(|s| {
        // xorshift64
        let mut x = s.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        (x >> 11) as f64 / (1u64 << 53) as f64
    })
}

/// Trait for market data feed sources.
/// Each implementation runs blocking I/O in its own thread and produces MarketEvents.
pub trait ExchangeMarket: Send {
    /// Connect to the exchange
    fn connect(&mut self) -> Result<()>;

    /// Subscribe to symbols (call before connect for feeds that encode symbols in URL)
    fn subscribe(&mut self, symbols: &[String]) -> Result<()>;

    /// Blocking read of the next market event.
    /// Returns Ok(None) on clean disconnect or read timeout.
    fn next_event(&mut self) -> Result<Option<MarketEvent>>;

    /// Disconnect from the exchange
    fn disconnect(&mut self);

    /// Name of this feed
    fn name(&self) -> &str;

    /// Whether the feed currently has an active subscription that should
    /// be producing data. Used by the engine's data-timeout watchdog to
    /// avoid futile reconnect storms when the feed is intentionally idle
    /// (e.g. Polymarket has no currently-trading event in the series).
    /// Default `true` preserves prior behavior for all other exchanges.
    fn has_active_subscription(&self) -> bool { true }
}

/// Trait for order execution backends.
pub trait ExchangeTrade: Send {
    /// Submit a new order
    fn submit_order(&mut self, order: &OrderRequest) -> Result<OrderUpdate>;

    /// Cancel an existing order
    fn cancel_order(&mut self, exchange: Exchange, client_order_id: &str) -> Result<OrderUpdate>;

    /// Cancel all orders for a symbol on an exchange
    fn cancel_all(&mut self, exchange: Exchange, symbol: &str) -> Result<Vec<OrderUpdate>>;

    /// Batch submit orders for the same market (default: submit one by one)
    fn batch_submit_orders(&mut self, _market_id: &str, orders: &[OrderRequest]) -> Result<Vec<OrderUpdate>> {
        let mut updates = Vec::new();
        for order in orders {
            updates.push(self.submit_order(order)?);
        }
        Ok(updates)
    }

    /// Batch cancel orders for the same market (default: cancel one by one)
    fn batch_cancel_orders(&mut self, exchange: Exchange, _market_id: &str, client_order_ids: &[String]) -> Result<Vec<OrderUpdate>> {
        let mut updates = Vec::new();
        for id in client_order_ids {
            updates.push(self.cancel_order(exchange, id)?);
        }
        Ok(updates)
    }

    /// Batch update: cancel + place in a single request (default: cancel then place separately)
    fn batch_update_orders(
        &mut self,
        exchange: Exchange,
        market_id: &str,
        cancel_client_order_ids: &[String],
        place_orders: &[OrderRequest],
    ) -> Result<Vec<OrderUpdate>> {
        let mut updates = Vec::new();
        if !cancel_client_order_ids.is_empty() {
            updates.extend(self.batch_cancel_orders(exchange, market_id, cancel_client_order_ids)?);
        }
        if !place_orders.is_empty() {
            updates.extend(self.batch_submit_orders(market_id, place_orders)?);
        }
        Ok(updates)
    }

    /// Replace order(s) — a reprice dispatched as one operation, parallel to
    /// `submit_order` (place) and `cancel_order` (cancel). For Polymarket in
    /// single-endpoint mode this is the serial "all cancels then all places on
    /// ONE connection, no ack wait between them" path (cancel→place arrival
    /// order, closing the place-before-cancel double-commit window) that the
    /// per-leg replace relies on. The default delegates to
    /// `batch_update_orders`, so behaviour is identical to the pre-existing
    /// replace path (poly's `batch_update_orders` already routes a
    /// both-cancels-and-places batch to `dispatch_single_endpoint_serial`).
    fn replace_order(
        &mut self,
        exchange: Exchange,
        market_id: &str,
        cancel_client_order_ids: &[String],
        place_orders: &[OrderRequest],
    ) -> Result<Vec<OrderUpdate>> {
        self.batch_update_orders(exchange, market_id, cancel_client_order_ids, place_orders)
    }

    /// Name of this executor
    fn name(&self) -> &str;
}
