pub mod aster;
pub mod binance;
pub mod bitget;
pub mod bybit;
pub mod chainlink;
pub mod coinbase;
pub mod gate;
pub mod hexmarket;
pub mod hyperliquid;
pub mod kucoin;
pub mod lighter;
pub mod mexc;
pub mod kraken;
pub mod okx;
pub mod polymarket;
pub mod pyth;
pub mod paper;
pub mod sim;
pub mod sim_v2;

use std::time::{Duration, Instant};

use crate::types::MarketEvent;
use crate::types::{Exchange, OrderRequest, OrderUpdate};
use anyhow::Result;

/// Heartbeat cadence for the Polymarket CLOB feed. Each tick sends both its
/// application-level text heartbeat and a WebSocket protocol Ping frame.
pub(crate) const POLYMARKET_WS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// RTDS heartbeat: lowercase application text `ping` every five seconds,
/// accompanied by a WebSocket protocol Ping frame.
pub(crate) const POLYMARKET_RTDS_PING_INTERVAL: Duration = Duration::from_secs(5);
pub(crate) const POLYMARKET_RTDS_PING_PAYLOAD: &str = "ping";
pub(crate) const POLYMARKET_WS_HEALTH_LOG_INTERVAL: Duration = Duration::from_secs(30);

/// Layered WebSocket liveness timestamps.
///
/// Keeping these clocks separate lets an incident log distinguish:
/// - no PONG: heartbeat response absent (diagnostic only);
/// - PONG and raw frames, but no topic frame: subscription silence;
/// - topic frames, but no BTC price: a single-symbol data gap.
pub(crate) struct WsHealth {
    connected_at: Instant,
    last_pong: Option<Instant>,
    last_raw_frame: Option<Instant>,
    last_topic_frame: Option<Instant>,
    last_btc_price: Option<Instant>,
}

impl WsHealth {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            connected_at: now,
            last_pong: None,
            last_raw_frame: None,
            last_topic_frame: None,
            last_btc_price: None,
        }
    }

    pub(crate) fn record_raw_frame(&mut self, now: Instant) {
        self.last_raw_frame = Some(now);
    }

    pub(crate) fn record_pong(&mut self, now: Instant) {
        self.last_pong = Some(now);
    }

    pub(crate) fn record_topic_frame(&mut self, now: Instant) {
        self.last_topic_frame = Some(now);
    }

    pub(crate) fn record_btc_price(&mut self, now: Instant) {
        self.last_btc_price = Some(now);
    }

    pub(crate) fn topic_is_stale(&self, now: Instant, threshold: Duration) -> bool {
        self.age(self.last_topic_frame, now) >= threshold
    }

    pub(crate) fn btc_price_is_stale(&self, now: Instant, threshold: Duration) -> bool {
        self.age(self.last_btc_price, now) >= threshold
    }

    pub(crate) fn transport_summary(&self, now: Instant) -> String {
        format!(
            "last_pong={} last_raw_frame={} last_topic_frame={}",
            self.age_label(self.last_pong, now),
            self.age_label(self.last_raw_frame, now),
            self.age_label(self.last_topic_frame, now),
        )
    }

    pub(crate) fn rtds_summary(&self, now: Instant) -> String {
        format!(
            "{} last_btc_price={}",
            self.transport_summary(now),
            self.age_label(self.last_btc_price, now),
        )
    }

    fn age(&self, last: Option<Instant>, now: Instant) -> Duration {
        elapsed(now, last.unwrap_or(self.connected_at))
    }

    fn age_label(&self, last: Option<Instant>, now: Instant) -> String {
        match last {
            Some(at) => format!("{:.1}s_ago", elapsed(now, at).as_secs_f64()),
            None => format!(
                "never({:.1}s_since_connect)",
                elapsed(now, self.connected_at).as_secs_f64(),
            ),
        }
    }
}

fn elapsed(now: Instant, then: Instant) -> Duration {
    now.checked_duration_since(then).unwrap_or_default()
}

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
    /// `submit_order` (place) and `cancel_order` (cancel). The default
    /// delegates to `batch_update_orders`; for Polymarket that is the fully
    /// concurrent cancel+place dispatch (cancels on the CANCEL pool, places
    /// on the FAST pool, no ordering — see the history note in poly's
    /// `batch_update_orders` for the retired serial-replace path).
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rtds_text_heartbeat_uses_lowercase_ping_every_five_seconds() {
        assert_eq!(POLYMARKET_RTDS_PING_INTERVAL, Duration::from_secs(5));
        assert_eq!(POLYMARKET_RTDS_PING_PAYLOAD, "ping");
    }

    #[test]
    fn ws_health_keeps_transport_topic_and_btc_clocks_separate() {
        let start = Instant::now();
        let mut health = WsHealth::new(start);

        health.record_raw_frame(start + Duration::from_secs(1));
        health.record_pong(start + Duration::from_secs(2));
        health.record_topic_frame(start + Duration::from_secs(3));
        health.record_btc_price(start + Duration::from_secs(4));

        let now = start + Duration::from_secs(10);
        let summary = health.rtds_summary(now);
        assert!(summary.contains("last_pong=8.0s_ago"));
        assert!(summary.contains("last_raw_frame=9.0s_ago"));
        assert!(summary.contains("last_topic_frame=7.0s_ago"));
        assert!(summary.contains("last_btc_price=6.0s_ago"));
    }
}
