pub mod aster;
pub mod binance;
pub mod bitget;
pub mod bybit;
pub mod chainlink;
pub mod coinbase;
pub mod gate;
pub mod hexmarket;
pub mod hyperliquid;
pub mod kraken;
pub mod kucoin;
pub mod lighter;
pub mod mexc;
pub mod okx;
pub mod paper;
pub mod polymarket;
pub mod pyth;
pub mod sim;
pub mod sim_v2;

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::types::MarketEvent;
use crate::types::{Exchange, OrderRequest, OrderUpdate};
use anyhow::Result;

/// Capacity of the parser-task -> synchronous feed-owner lane used by public
/// market adapters. The producer never blocks: replaceable observations are
/// overwritten oldest-first on overflow and ordered events force the feed
/// generation to reconnect. This bounds stale backlog and memory independently
/// per venue.
pub const PUBLIC_MARKET_ADAPTER_LANE_CAPACITY: usize = 4096;
const PUBLIC_MARKET_ORDERED_BURST: u8 = 8;

#[inline]
fn replaceable_market_event(event: &MarketEvent) -> bool {
    matches!(
        event,
        MarketEvent::OrderBook(_)
            | MarketEvent::Quote(_)
            | MarketEvent::SpotPrice(_)
            | MarketEvent::AssetCtx(_)
    )
}

/// Multi-producer half of one venue adapter's public-data mailbox.
///
/// Ordered events use a bounded FIFO and fail the feed generation when full.
/// Replaceable observations use a fixed lock-free overwrite-oldest queue, so
/// parser tasks preserve freshness without blocking or growing the heap.
#[derive(Clone)]
pub(crate) struct PublicMarketPublisher {
    ordered: crossbeam_channel::Sender<MarketEvent>,
    latest: Arc<crossbeam_queue::ArrayQueue<MarketEvent>>,
    consumer_alive: Arc<AtomicBool>,
}

/// Single-consumer half owned by the synchronous venue feed thread. Ordered
/// traffic has priority, bounded to a short burst so current snapshots cannot
/// starve during a sustained public-trade stream.
pub(crate) struct PublicMarketReceiver {
    ordered: crossbeam_channel::Receiver<MarketEvent>,
    latest: Arc<crossbeam_queue::ArrayQueue<MarketEvent>>,
    consumer_alive: Arc<AtomicBool>,
    ordered_burst: AtomicU8,
}

impl PublicMarketReceiver {
    pub(crate) fn try_recv(
        &self,
    ) -> std::result::Result<MarketEvent, crossbeam_channel::TryRecvError> {
        if self.ordered_burst.load(Ordering::Relaxed) >= PUBLIC_MARKET_ORDERED_BURST {
            if let Some(event) = self.latest.pop() {
                self.ordered_burst.store(0, Ordering::Relaxed);
                return Ok(event);
            }
        }
        match self.ordered.try_recv() {
            Ok(event) => {
                if self.ordered_burst.load(Ordering::Relaxed) < PUBLIC_MARKET_ORDERED_BURST {
                    self.ordered_burst.fetch_add(1, Ordering::Relaxed);
                }
                Ok(event)
            }
            Err(crossbeam_channel::TryRecvError::Empty) => {
                if let Some(event) = self.latest.pop() {
                    self.ordered_burst.store(0, Ordering::Relaxed);
                    Ok(event)
                } else {
                    Err(crossbeam_channel::TryRecvError::Empty)
                }
            }
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                if let Some(event) = self.latest.pop() {
                    self.ordered_burst.store(0, Ordering::Relaxed);
                    Ok(event)
                } else {
                    Err(crossbeam_channel::TryRecvError::Disconnected)
                }
            }
        }
    }
}

impl Drop for PublicMarketReceiver {
    fn drop(&mut self) {
        self.consumer_alive.store(false, Ordering::Release);
    }
}

pub(crate) fn public_market_channel() -> (PublicMarketPublisher, PublicMarketReceiver) {
    let (ordered_tx, ordered_rx) =
        crossbeam_channel::bounded(PUBLIC_MARKET_ADAPTER_LANE_CAPACITY);
    let latest = Arc::new(crossbeam_queue::ArrayQueue::new(
        PUBLIC_MARKET_ADAPTER_LANE_CAPACITY,
    ));
    let consumer_alive = Arc::new(AtomicBool::new(true));
    (
        PublicMarketPublisher {
            ordered: ordered_tx,
            latest: Arc::clone(&latest),
            consumer_alive: Arc::clone(&consumer_alive),
        },
        PublicMarketReceiver {
            ordered: ordered_rx,
            latest,
            consumer_alive,
            ordered_burst: AtomicU8::new(0),
        },
    )
}

/// Fixed capacity of a venue-private order/fill lane owned by one strategy
/// worker. The producer is one authenticated user-feed task; the consumer is
/// the strategy owner thread. Updates are FIFO and never replaceable.
pub const PRIVATE_UPDATE_LANE_CAPACITY: usize = 4096;

/// Replaceable control state for a [`PrivateUpdateLane`]. Connectivity changes
/// are informational. `Overflow` is terminal for the current feed generation:
/// at least one non-replayable lifecycle event could not be admitted, so the
/// strategy owner must fail closed until an authoritative reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivateFeedControl {
    Connected(Exchange),
    Disconnected(Exchange),
    Overflow(Exchange),
}

/// Consumer half of a bounded private-event lane.
///
/// Ownership and recovery semantics:
/// - one async authenticated feed task publishes FIFO `OrderUpdate`s;
/// - exactly one strategy thread consumes and mutates its private account;
/// - `updates` is lossless while capacity is available and never blocks Tokio;
/// - on capacity exhaustion the producer publishes `Overflow`, stops, and the
///   owner must cancel/fail closed because these venues have no gap replay;
/// - dropping the lane requests feed shutdown.
pub struct PrivateUpdateLane {
    pub updates: crossbeam_channel::Receiver<OrderUpdate>,
    pub control: crossbeam_channel::Receiver<PrivateFeedControl>,
    shutdown: Arc<AtomicBool>,
}

impl PrivateUpdateLane {
    pub fn shutdown_token(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown)
    }
}

impl Drop for PrivateUpdateLane {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

/// Producer half retained only by the authenticated feed task. The control
/// receiver clone lets the sole producer replace stale connectivity notices so
/// an overflow notice can always occupy the one-slot latest-value mailbox.
pub(crate) struct PrivateUpdatePublisher {
    exchange: Exchange,
    updates: crossbeam_channel::Sender<OrderUpdate>,
    control: crossbeam_channel::Sender<PrivateFeedControl>,
    control_replace: crossbeam_channel::Receiver<PrivateFeedControl>,
}

impl PrivateUpdatePublisher {
    pub(crate) fn publish(&self, update: OrderUpdate) -> bool {
        match self.updates.try_send(update) {
            Ok(()) => true,
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => false,
            Err(crossbeam_channel::TrySendError::Full(_)) => {
                self.publish_control(PrivateFeedControl::Overflow(self.exchange));
                false
            }
        }
    }

    pub(crate) fn connected(&self) {
        self.publish_control(PrivateFeedControl::Connected(self.exchange));
    }

    pub(crate) fn disconnected(&self) {
        self.publish_control(PrivateFeedControl::Disconnected(self.exchange));
    }

    fn publish_control(&self, control: PrivateFeedControl) {
        match self.control.try_send(control) {
            Ok(()) | Err(crossbeam_channel::TrySendError::Disconnected(_)) => {}
            Err(crossbeam_channel::TrySendError::Full(control)) => {
                let _ = self.control_replace.try_recv();
                let _ = self.control.try_send(control);
            }
        }
    }
}

pub(crate) fn private_update_lane(
    exchange: Exchange,
) -> (PrivateUpdatePublisher, PrivateUpdateLane) {
    let (update_tx, update_rx) = crossbeam_channel::bounded(PRIVATE_UPDATE_LANE_CAPACITY);
    // Connectivity is replaceable latest state. Overflow replaces an older
    // connectivity notification and makes this generation terminal.
    let (control_tx, control_rx) = crossbeam_channel::bounded(1);
    let shutdown = Arc::new(AtomicBool::new(false));
    (
        PrivateUpdatePublisher {
            exchange,
            updates: update_tx,
            control: control_tx,
            control_replace: control_rx.clone(),
        },
        PrivateUpdateLane {
            updates: update_rx,
            control: control_rx,
            shutdown,
        },
    )
}

static PUBLIC_MARKET_OVERFLOW_DROPS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static PUBLIC_MARKET_OVERFLOW_REPLACEMENTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub trait MarketEventPublisher {
    fn try_publish(
        &self,
        event: MarketEvent,
    ) -> std::result::Result<(), crossbeam_channel::SendError<MarketEvent>>;
}

impl<T: MarketEventPublisher + ?Sized> MarketEventPublisher for &T {
    fn try_publish(
        &self,
        event: MarketEvent,
    ) -> std::result::Result<(), crossbeam_channel::SendError<MarketEvent>> {
        (**self).try_publish(event)
    }
}

impl MarketEventPublisher for crossbeam_channel::Sender<MarketEvent> {
    fn try_publish(
        &self,
        event: MarketEvent,
    ) -> std::result::Result<(), crossbeam_channel::SendError<MarketEvent>> {
        match self.try_send(event) {
            Ok(()) => Ok(()),
            Err(crossbeam_channel::TrySendError::Full(event)) => {
                if replaceable_market_event(&event) {
                    PUBLIC_MARKET_OVERFLOW_DROPS.fetch_add(1, Ordering::Relaxed);
                    hexagent_runtime::latency::record_ns("market.root_overflow_drop", 1);
                    Ok(())
                } else {
                    Err(crossbeam_channel::SendError(event))
                }
            }
            Err(crossbeam_channel::TrySendError::Disconnected(event)) => {
                Err(crossbeam_channel::SendError(event))
            }
        }
    }
}

impl MarketEventPublisher for PublicMarketPublisher {
    fn try_publish(
        &self,
        event: MarketEvent,
    ) -> std::result::Result<(), crossbeam_channel::SendError<MarketEvent>> {
        if !self.consumer_alive.load(Ordering::Acquire) {
            return Err(crossbeam_channel::SendError(event));
        }
        if replaceable_market_event(&event) {
            if self.latest.force_push(event).is_some() {
                PUBLIC_MARKET_OVERFLOW_REPLACEMENTS.fetch_add(1, Ordering::Relaxed);
            }
            return Ok(());
        }
        self.ordered.try_send(event).map_err(|error| match error {
            crossbeam_channel::TrySendError::Full(event)
            | crossbeam_channel::TrySendError::Disconnected(event) => {
                crossbeam_channel::SendError(event)
            }
        })
    }
}

/// Publish parsed public data without ever parking an async socket/parser task
/// behind the synchronous root router. Quote/book/latest-value observations
/// are replaceable; capacity exhaustion drops this observation and records the
/// loss. Trades, bars, lifecycle, instrument and health messages are ordered:
/// a full lane returns an error so the producing feed fails closed/reconnects
/// instead of silently losing a non-replaceable transition.
pub fn publish_market_event<P: MarketEventPublisher + ?Sized>(
    tx: &P,
    event: MarketEvent,
) -> Result<(), crossbeam_channel::SendError<MarketEvent>> {
    tx.try_publish(event)
}

pub fn public_market_overflow_drops() -> u64 {
    PUBLIC_MARKET_OVERFLOW_DROPS.load(Ordering::Relaxed)
}

pub fn public_market_overflow_replacements() -> u64 {
    PUBLIC_MARKET_OVERFLOW_REPLACEMENTS.load(Ordering::Relaxed)
}

/// Heartbeat cadence for the Polymarket CLOB feed. Each tick sends both its
/// application-level text heartbeat and a WebSocket protocol Ping frame.
pub(crate) const POLYMARKET_WS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// RTDS heartbeat: lowercase application text `ping` every five seconds,
/// accompanied by a WebSocket protocol Ping frame.
pub(crate) const POLYMARKET_RTDS_PING_INTERVAL: Duration = Duration::from_secs(5);
pub(crate) const POLYMARKET_RTDS_PING_PAYLOAD: &str = "ping";
pub(crate) const POLYMARKET_WS_HEALTH_LOG_INTERVAL: Duration = Duration::from_secs(30);

/// Bound on a single outbound WebSocket write.
///
/// `SinkExt::send` is `feed` + `flush`, and `flush` on a TCP sink awaits until
/// the kernel accepts the bytes. If the peer stops reading (zero-window) or the
/// link goes half-open, it never returns. That await lives INSIDE a
/// `tokio::select!` branch body — not among the futures the select polls — so a
/// single hung write suspends the entire task: the read-stall watchdog and the
/// health tick stop being polled, and nothing in-process can recover it. The
/// engine's own data-timeout cannot help either; it can only mark the feed
/// stale, it cannot unstick the task.
///
/// Observed twice in 2116 h of recorded Chainlink RTDS (2026-05-01..07-28):
/// 2026-06-15 18:59:59Z for 41.1 min and 2026-07-23 18:01:35Z for 46.8 min.
/// In the second, all three RTDS symbols stopped on the same minute while
/// Binance, Coinbase, Hyperliquid AND the Polymarket CLOB — same process, and
/// in the CLOB's case the same vendor — recorded continuously throughout. One
/// task frozen, everything around it healthy.
///
/// 10 s is 2x the 5 s heartbeat cadence; a healthy write completes in
/// microseconds, so this only ever fires on a genuinely stuck sink.
pub(crate) const WS_SEND_TIMEOUT: Duration = Duration::from_secs(10);

/// Bound on connect + TLS handshake + WS upgrade. `connect_async` is equally
/// unbounded: the OS caps the TCP connect (~75 s), but nothing caps the TLS
/// handshake that follows, and this await sits in the reconnect loop where a
/// hang is indistinguishable from the send hang above. The reconnect backoff
/// caps at 6.4 s, so a *failing* connect cannot explain a 40-minute silence —
/// only a *hanging* one can.
pub(crate) const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

/// Send one WebSocket message under [`WS_SEND_TIMEOUT`].
///
/// Returns the reason as a string on failure so callers can log it and break to
/// their reconnect path; a stalled sink is reported distinctly from a sink that
/// returned an error, because the two mean different things in an incident log.
pub(crate) async fn ws_send<S>(
    sink: &mut S,
    msg: tokio_tungstenite::tungstenite::Message,
) -> std::result::Result<(), String>
where
    S: futures_util::SinkExt<tokio_tungstenite::tungstenite::Message> + Unpin,
    <S as futures_util::Sink<tokio_tungstenite::tungstenite::Message>>::Error: std::fmt::Display,
{
    ws_send_within(sink, msg, WS_SEND_TIMEOUT).await
}

/// [`ws_send`] with the bound supplied by the caller, so the timeout path is
/// testable without a multi-second test.
pub(crate) async fn ws_send_within<S>(
    sink: &mut S,
    msg: tokio_tungstenite::tungstenite::Message,
    bound: Duration,
) -> std::result::Result<(), String>
where
    S: futures_util::SinkExt<tokio_tungstenite::tungstenite::Message> + Unpin,
    <S as futures_util::Sink<tokio_tungstenite::tungstenite::Message>>::Error: std::fmt::Display,
{
    match tokio::time::timeout(bound, sink.send(msg)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err(format!(
            "write stalled >{:.1}s (sink never flushed)",
            bound.as_secs_f64()
        )),
    }
}

/// Layered WebSocket liveness timestamps.
///
/// Keeping these clocks separate lets an incident log distinguish:
/// - no PONG: heartbeat response absent (diagnostic only);
/// - PONG and raw frames, but no topic frame: subscription silence;
/// - topic frames, but no usable book: decode/state-application lag;
/// - topic frames, but no BTC price: a single-symbol RTDS data gap.
pub(crate) struct WsHealth {
    connected_at: Instant,
    last_pong: Option<Instant>,
    last_raw_frame: Option<Instant>,
    last_topic_frame: Option<Instant>,
    /// Last order-book snapshot or two-sided L1 that downstream strategy
    /// code could actually consume. This remains separate from the topic
    /// clock because an unseeded delta still proves the subscription is live.
    last_usable_book: Option<Instant>,
    last_btc_price: Option<Instant>,
}

impl WsHealth {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            connected_at: now,
            last_pong: None,
            last_raw_frame: None,
            last_topic_frame: None,
            last_usable_book: None,
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

    pub(crate) fn record_usable_book(&mut self, now: Instant) {
        self.last_usable_book = Some(now);
    }

    pub(crate) fn record_btc_price(&mut self, now: Instant) {
        self.last_btc_price = Some(now);
    }

    pub(crate) fn topic_is_stale(&self, now: Instant, threshold: Duration) -> bool {
        self.age(self.last_topic_frame, now) >= threshold
    }

    pub(crate) fn usable_book_is_stale(&self, now: Instant, threshold: Duration) -> bool {
        self.age(self.last_usable_book, now) >= threshold
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

    pub(crate) fn clob_summary(&self, now: Instant) -> String {
        format!(
            "{} last_usable_book={}",
            self.transport_summary(now),
            self.age_label(self.last_usable_book, now),
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
        Self {
            base_ms,
            max_ms,
            attempt: 0,
        }
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
    fn has_active_subscription(&self) -> bool {
        true
    }
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
    fn batch_submit_orders(
        &mut self,
        _market_id: &str,
        orders: &[OrderRequest],
    ) -> Result<Vec<OrderUpdate>> {
        let mut updates = Vec::new();
        for order in orders {
            updates.push(self.submit_order(order)?);
        }
        Ok(updates)
    }

    /// Batch cancel orders for the same market (default: cancel one by one)
    fn batch_cancel_orders(
        &mut self,
        exchange: Exchange,
        _market_id: &str,
        client_order_ids: &[String],
    ) -> Result<Vec<OrderUpdate>> {
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
            updates.extend(self.batch_cancel_orders(
                exchange,
                market_id,
                cancel_client_order_ids,
            )?);
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

    fn private_update(sequence: usize) -> OrderUpdate {
        OrderUpdate {
            client_order_id: format!("private-{sequence}"),
            exchange: Exchange::Hyperliquid,
            symbol: "BTC".into(),
            side: crate::types::Side::Buy,
            exchange_order_id: None,
            status: crate::types::OrderStatus::Filled,
            liquidity: None,
            filled_quantity: 1.0,
            remaining_quantity: 0.0,
            avg_fill_price: 1.0,
            timestamp_ns: sequence as u64,
            exchange_event_timestamp_ns: None,
            trade_id: Some(format!("trade-{sequence}")),
            order_audit: None,
            error: None,
        }
    }

    #[test]
    fn private_lane_is_fifo_and_overflow_is_terminal_control_state() {
        let (publisher, lane) = private_update_lane(Exchange::Hyperliquid);
        publisher.connected();
        for sequence in 0..PRIVATE_UPDATE_LANE_CAPACITY {
            assert!(publisher.publish(private_update(sequence)));
        }
        assert!(!publisher.publish(private_update(PRIVATE_UPDATE_LANE_CAPACITY)));
        assert_eq!(
            lane.control.try_recv(),
            Ok(PrivateFeedControl::Overflow(Exchange::Hyperliquid)),
        );
        for sequence in 0..PRIVATE_UPDATE_LANE_CAPACITY {
            assert_eq!(
                lane.updates.try_recv().unwrap().client_order_id,
                format!("private-{sequence}"),
            );
        }
        assert!(lane.updates.try_recv().is_err());
    }

    #[test]
    fn dropping_private_lane_requests_feed_shutdown() {
        let (_publisher, lane) = private_update_lane(Exchange::Aster);
        let shutdown = lane.shutdown_token();
        assert!(!shutdown.load(Ordering::Acquire));
        drop(lane);
        assert!(shutdown.load(Ordering::Acquire));
    }

    #[test]
    fn public_market_publish_never_blocks_and_lifecycle_fails_closed() {
        let (tx, rx) = crossbeam_channel::bounded(1);
        tx.try_send(MarketEvent::SpotPrice(crate::types::SpotPrice {
            source: "test".into(),
            symbol: "btc/usd".into(),
            price: 1.0,
            timestamp_ns: 1,
            local_timestamp_ns: 1,
        }))
        .unwrap();
        let before = public_market_overflow_drops();
        assert!(publish_market_event(
            &tx,
            MarketEvent::SpotPrice(crate::types::SpotPrice {
                source: "test".into(),
                symbol: "btc/usd".into(),
                price: 2.0,
                timestamp_ns: 2,
                local_timestamp_ns: 2,
            }),
        )
        .is_ok());
        assert!(public_market_overflow_drops() > before);
        assert!(publish_market_event(
            &tx,
            MarketEvent::Connected {
                exchange: Exchange::Binance,
            },
        )
        .is_err());
        assert_eq!(rx.len(), 1);
    }

    #[test]
    fn adapter_mailbox_overwrites_oldest_replaceable_observation() {
        let (publisher, receiver) = public_market_channel();
        let before = public_market_overflow_replacements();
        for sequence in 0..=PUBLIC_MARKET_ADAPTER_LANE_CAPACITY {
            publish_market_event(
                &publisher,
                MarketEvent::SpotPrice(crate::types::SpotPrice {
                    source: "test".into(),
                    symbol: "btc/usd".into(),
                    price: sequence as f64,
                    timestamp_ns: sequence as u64,
                    local_timestamp_ns: sequence as u64,
                }),
            )
            .unwrap();
        }
        assert!(public_market_overflow_replacements() > before);

        let mut timestamps = Vec::with_capacity(PUBLIC_MARKET_ADAPTER_LANE_CAPACITY);
        while let Ok(MarketEvent::SpotPrice(spot)) = receiver.try_recv() {
            timestamps.push(spot.timestamp_ns);
        }
        assert_eq!(timestamps.len(), PUBLIC_MARKET_ADAPTER_LANE_CAPACITY);
        assert_eq!(timestamps.first(), Some(&1));
        assert_eq!(
            timestamps.last(),
            Some(&(PUBLIC_MARKET_ADAPTER_LANE_CAPACITY as u64)),
        );
    }

    #[test]
    fn adapter_mailbox_prioritizes_ordered_control_over_snapshot() {
        let (publisher, receiver) = public_market_channel();
        publish_market_event(
            &publisher,
            MarketEvent::SpotPrice(crate::types::SpotPrice {
                source: "test".into(),
                symbol: "btc/usd".into(),
                price: 1.0,
                timestamp_ns: 1,
                local_timestamp_ns: 1,
            }),
        )
        .unwrap();
        publish_market_event(
            &publisher,
            MarketEvent::Connected {
                exchange: Exchange::Binance,
            },
        )
        .unwrap();
        assert!(matches!(
            receiver.try_recv(),
            Ok(MarketEvent::Connected {
                exchange: Exchange::Binance
            })
        ));
        assert!(matches!(
            receiver.try_recv(),
            Ok(MarketEvent::SpotPrice(_))
        ));
    }

    #[test]
    fn adapter_mailbox_bounds_snapshot_starvation_during_ordered_burst() {
        let (publisher, receiver) = public_market_channel();
        for _ in 0..(PUBLIC_MARKET_ORDERED_BURST as usize + 4) {
            publish_market_event(
                &publisher,
                MarketEvent::Connected {
                    exchange: Exchange::Binance,
                },
            )
            .unwrap();
        }
        publish_market_event(
            &publisher,
            MarketEvent::SpotPrice(crate::types::SpotPrice {
                source: "test".into(),
                symbol: "btc/usd".into(),
                price: 1.0,
                timestamp_ns: 1,
                local_timestamp_ns: 1,
            }),
        )
        .unwrap();

        for _ in 0..PUBLIC_MARKET_ORDERED_BURST {
            assert!(matches!(
                receiver.try_recv(),
                Ok(MarketEvent::Connected { .. })
            ));
        }
        assert!(matches!(
            receiver.try_recv(),
            Ok(MarketEvent::SpotPrice(_))
        ));
    }

    #[test]
    #[ignore = "manual public-mailbox latency benchmark; run with --release --ignored"]
    fn adapter_mailbox_publish_latency_benchmark() {
        const EVENTS: usize = 100_000;
        let events: Vec<MarketEvent> = (0..EVENTS)
            .map(|sequence| {
                MarketEvent::SpotPrice(crate::types::SpotPrice {
                    source: "benchmark".into(),
                    symbol: "btc/usd".into(),
                    price: sequence as f64,
                    timestamp_ns: sequence as u64,
                    local_timestamp_ns: sequence as u64,
                })
            })
            .collect();
        let (publisher, receiver) = public_market_channel();
        let consumer = std::thread::spawn(move || {
            let mut consumed = 0usize;
            loop {
                match receiver.try_recv() {
                    Ok(_) => consumed += 1,
                    Err(crossbeam_channel::TryRecvError::Empty) => std::hint::spin_loop(),
                    Err(crossbeam_channel::TryRecvError::Disconnected) => break,
                }
            }
            consumed
        });
        let replacements_before = public_market_overflow_replacements();
        let mut samples = Vec::with_capacity(EVENTS);
        let mut peak_depth = 0usize;
        for event in events {
            let started = std::time::Instant::now();
            publish_market_event(&publisher, event).unwrap();
            samples.push(started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
            peak_depth = peak_depth.max(publisher.latest.len() + publisher.ordered.len());
        }
        drop(publisher);
        let consumed = consumer.join().unwrap();
        samples.sort_unstable();
        let percentile = |numerator: usize, denominator: usize| {
            samples[((samples.len() - 1) * numerator) / denominator]
        };
        println!(
            "adapter_mailbox events={} boundary=prebuilt_event_publish_entry_to_return unit=ns consumed={} p50={} p99={} p999={} max={} peak_depth={} replacements={}",
            EVENTS,
            consumed,
            percentile(50, 100),
            percentile(99, 100),
            percentile(999, 1000),
            samples[samples.len() - 1],
            peak_depth,
            public_market_overflow_replacements().saturating_sub(replacements_before),
        );
        assert!(consumed > 0);
    }

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

    #[test]
    fn clob_health_keeps_raw_topic_and_usable_book_clocks_separate() {
        let start = Instant::now();
        let mut health = WsHealth::new(start);
        health.record_raw_frame(start + Duration::from_secs(8));
        health.record_topic_frame(start + Duration::from_secs(9));

        let now = start + Duration::from_secs(10);
        assert!(!health.topic_is_stale(now, Duration::from_secs(2)));
        assert!(health.usable_book_is_stale(now, Duration::from_secs(2)));
        assert!(health
            .clob_summary(now)
            .contains("last_usable_book=never(10.0s_since_connect)"));

        health.record_usable_book(now);
        assert!(!health.usable_book_is_stale(now, Duration::from_secs(2)));
        assert!(health
            .clob_summary(now)
            .contains("last_usable_book=0.0s_ago"));
    }

    /// A sink whose flush never completes must not be able to hold the task
    /// forever. This is the 2026-06-15 / 2026-07-23 RTDS freeze shape: every
    /// timer in the task lives in a `tokio::select!`, and a `write.send(...)`
    /// inside a branch body is NOT one of the futures that select polls — so
    /// while it is pending, the read-stall watchdog and the health tick are
    /// never polled again and nothing in-process can recover.
    #[tokio::test]
    async fn ws_send_gives_up_on_a_sink_that_never_flushes() {
        use futures_util::Sink;
        use std::pin::Pin;
        use std::task::{Context, Poll};
        use tokio_tungstenite::tungstenite::Message;

        /// Accepts the message, then never finishes flushing it — a TCP sink
        /// whose peer has stopped reading.
        struct NeverFlushes;
        impl Sink<Message> for NeverFlushes {
            type Error = std::io::Error;
            fn poll_ready(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }
            fn start_send(self: Pin<&mut Self>, _: Message) -> Result<(), Self::Error> {
                Ok(())
            }
            fn poll_flush(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<Result<(), Self::Error>> {
                Poll::Pending
            }
            fn poll_close(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<Result<(), Self::Error>> {
                Poll::Pending
            }
        }

        let mut sink = NeverFlushes;
        let err = ws_send_within(
            &mut sink,
            Message::Text("ping".to_string()),
            Duration::from_millis(50),
        )
        .await
        .expect_err("a sink that never flushes must time out, not hang");
        assert!(err.contains("stalled"), "unexpected reason: {err}");
    }

    /// A heartbeat responder that outlives the market-data feed must NOT read
    /// as liveness. This is the exact shape of the 2026-06-24 Polymarket CLOB
    /// freeze: `PONG` kept arriving every 5 s (so the raw-frame clock never
    /// aged past its 90 s bound) while no book or trade frame landed for 37
    /// minutes. Only the topic clock can see that, which is why the CLOB task
    /// reconnects on `topic_is_stale` and not on raw-frame silence alone.
    #[test]
    fn pongs_do_not_mask_a_topic_stall() {
        let start = Instant::now();
        let mut health = WsHealth::new(start);
        health.record_topic_frame(start);

        // 5 s heartbeat answered for 10 minutes; no topic frame after t=0.
        let mut now = start;
        for _ in 0..120 {
            now += Duration::from_secs(5);
            health.record_raw_frame(now);
            health.record_pong(now);
        }

        // Raw-frame clock is fresh — a read-timeout watchdog sees nothing wrong.
        assert_eq!(health.age(health.last_raw_frame, now), Duration::ZERO);
        // Topic clock is 10 minutes old — the stall is visible here and only here.
        assert!(health.topic_is_stale(now, Duration::from_secs(90)));
        assert!(!health.topic_is_stale(start + Duration::from_secs(89), Duration::from_secs(90)));
    }
}
