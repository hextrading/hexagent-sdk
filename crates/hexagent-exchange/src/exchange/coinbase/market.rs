//! Coinbase WebSocket market feed — level2 orderbook + matches (trades).
//! Endpoint: wss://ws-feed.exchange.coinbase.com
//! Channels: level2_batch (batched depth updates, no auth required), matches (trades).
//! Note: the `level2` channel now requires authentication; `level2_batch` is the
//!       unauthenticated equivalent (server-side alias: `level2_50`).
//! Symbols: "BTC-USD" format (dash-separated).

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

use crate::exchange::ExchangeMarket;
use crate::types::*;

const COINBASE_WS_URL: &str = "wss://ws-feed.exchange.coinbase.com";
/// Max depth levels to emit in OrderBook events.
const MAX_DEPTH: usize = 5;
const PING_INTERVAL: Duration = Duration::from_secs(30);
/// Per-task read-side stall watchdog: force reconnect if no message
/// arrives within this window. Defends against TCP zombies that
/// `read.next().await` doesn't surface as an error. Coinbase L2 +
/// matches push at multiple Hz combined, so 30 s is generous.
const STALE_THRESHOLD: Duration = Duration::from_secs(30);

/// Ordered f64 wrapper for BTreeMap keys (bid descending, ask ascending).
#[derive(Clone, Copy, PartialEq)]
struct OrdF64(f64);
impl Eq for OrdF64 {}
impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}
impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Local orderbook maintained from level2 snapshot + updates.
struct LocalBook {
    /// price → quantity (ascending order)
    bids: BTreeMap<OrdF64, f64>,
    asks: BTreeMap<OrdF64, f64>,
    ready: bool,
}

impl LocalBook {
    fn new() -> Self {
        Self { bids: BTreeMap::new(), asks: BTreeMap::new(), ready: false }
    }

    fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.ready = false;
    }

    fn update(&mut self, side: &str, price: f64, qty: f64) {
        let map = if side == "buy" { &mut self.bids } else { &mut self.asks };
        if qty == 0.0 {
            map.remove(&OrdF64(price));
        } else {
            map.insert(OrdF64(price), qty);
        }
    }

    /// Top N bids (highest first) and asks (lowest first).
    fn snapshot(&self, depth: usize) -> (Vec<PriceLevel>, Vec<PriceLevel>) {
        let bids: Vec<PriceLevel> = self.bids.iter().rev()
            .take(depth)
            .map(|(p, q)| PriceLevel { price: p.0, quantity: *q })
            .collect();
        let asks: Vec<PriceLevel> = self.asks.iter()
            .take(depth)
            .map(|(p, q)| PriceLevel { price: p.0, quantity: *q })
            .collect();
        (bids, asks)
    }
}

pub struct CoinbaseMarket {
    symbols: Vec<String>,
    event_rx: Option<crossbeam_channel::Receiver<MarketEvent>>,
    ws_shutdown: Arc<AtomicBool>,
}

impl CoinbaseMarket {
    pub fn new() -> Self {
        Self {
            symbols: Vec::new(),
            event_rx: None,
            ws_shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

/// Number of trades missed between `prev` (highest trade_id seen so far, if
/// any) and a newly received `tid`. Returns 0 for the baseline (first trade,
/// `prev == None`), contiguous trades (`tid == prev + 1`), and duplicate /
/// out-of-order (`tid <= prev`). Coinbase `trade_id` is a dense per-product
/// counter (+1 per trade), so a forward jump of more than 1 means messages
/// were dropped to us.
fn trade_id_gap(prev: Option<u64>, tid: u64) -> u64 {
    match prev {
        Some(p) if tid > p + 1 => tid - p - 1,
        _ => 0,
    }
}

async fn coinbase_ws_task(
    symbols: Vec<String>,
    event_tx: crossbeam_channel::Sender<MarketEvent>,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = crate::exchange::ReconnectBackoff::new(200, 30_000);

    loop {
        if shutdown.load(Ordering::Relaxed) { break; }

        info!("[Coinbase] Connecting to {}", COINBASE_WS_URL);
        let stream = match tokio_tungstenite::connect_async(COINBASE_WS_URL).await {
            Ok((s, _)) => s,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[Coinbase] WS connect failed: {}, retry in {:.1}s", e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        backoff.reset();
        let (mut write, mut read) = stream.split();

        let sub = serde_json::json!({
            "type": "subscribe",
            "channels": [
                { "name": "level2_batch", "product_ids": symbols },
                { "name": "matches", "product_ids": symbols },
                // heartbeat (1/s per product) — liveness + carries
                // last_trade_id so we can detect dropped messages even
                // when matches stop flowing to us. See gap detection below.
                { "name": "heartbeat", "product_ids": symbols },
            ],
        });
        if let Err(e) = write.send(Message::Text(sub.to_string())).await {
            warn!("[Coinbase] subscribe failed: {}", e);
            continue;
        }
        info!("[Coinbase] Connected, subscribed to {:?}", symbols);

        let mut books: std::collections::HashMap<String, LocalBook> = std::collections::HashMap::new();
        for s in &symbols {
            books.insert(s.clone(), LocalBook::new());
        }

        // Gap detection state — recreated per connection (so a reconnect's
        // fresh `snapshot` rebuilds the book AND resets these). level2_batch
        // carries no sequence number, so we detect connection-wide message
        // loss via the matches `trade_id` (dense +1 per trade per product)
        // and the heartbeat `last_trade_id`. A gap implies level2 updates
        // may have been dropped too → break to reconnect → re-snapshot.
        let mut last_trade_id: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
        let mut hb_prev_trade_id: std::collections::HashMap<String, u64> = std::collections::HashMap::new();

        let mut ping_interval = tokio::time::interval(PING_INTERVAL);
        ping_interval.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = ping_interval.tick() => {
                    if let Err(e) = write.send(Message::Ping(Vec::new())).await {
                        warn!("[Coinbase] Ping send failed: {}", e);
                        break;
                    }
                }
                read_result = tokio::time::timeout(STALE_THRESHOLD, read.next()) => {
                    let msg = match read_result {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => { warn!("[Coinbase] WS read error: {}", e); break; }
                        Ok(None) => { warn!("[Coinbase] WS closed"); break; }
                        Err(_elapsed) => {
                            warn!("[Coinbase] No message for {:.0}s (stall watchdog) — reconnecting",
                                STALE_THRESHOLD.as_secs_f64());
                            break;
                        }
                    };
                    match msg {
                        Message::Text(text) => {
                            // simd-json drop-in for SIMD parse speedup.
                            let mut buf = text.as_bytes().to_vec();
                            let data: serde_json::Value = match simd_json::serde::from_slice(&mut buf) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                            let msg_type = data.get("type").and_then(|v| v.as_str()).unwrap_or("");

                            // Trades
                            if msg_type == "match" || msg_type == "last_match" {
                                let product_id = match data.get("product_id").and_then(|v| v.as_str()) {
                                    Some(s) => s,
                                    None => continue,
                                };
                                // Gap detection: trade_id is a dense per-product
                                // counter (+1 per trade). A forward jump means
                                // messages were dropped to us — level2_batch
                                // updates (no sequence) were likely dropped too
                                // — so reconnect to re-snapshot the book.
                                if let Some(tid) = data.get("trade_id").and_then(|v| v.as_u64()) {
                                    let prev = last_trade_id.get(product_id).copied();
                                    let missed = trade_id_gap(prev, tid);
                                    if missed > 0 {
                                        warn!("[Coinbase] {} trade_id gap {}→{} ({} missed) — reconnecting to re-snapshot",
                                            product_id, prev.unwrap_or(0), tid, missed);
                                        break;
                                    }
                                    last_trade_id.entry(product_id.to_string())
                                        .and_modify(|v| if tid > *v { *v = tid; })
                                        .or_insert(tid);
                                }
                                let price: f64 = data.get("price").and_then(|v| v.as_str())
                                    .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                let quantity: f64 = data.get("size").and_then(|v| v.as_str())
                                    .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                if price <= 0.0 { continue; }
                                let side = match data.get("side").and_then(|v| v.as_str()) {
                                    Some("buy") => Side::Buy,
                                    _ => Side::Sell,
                                };
                                let exchange_ts = data.get("time").and_then(|v| v.as_str())
                                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                                    .map(|dt| dt.timestamp_nanos_opt().unwrap_or(0) as u64)
                                    .unwrap_or_else(now_ns);
                                let event = MarketEvent::Trade(TradeTick {
                                    exchange: Exchange::Coinbase,
                                    symbol: product_id.to_string(),
                                    price,
                                    quantity,
                                    side,
                                    exchange_timestamp_ns: exchange_ts,
                                    local_timestamp_ns: now_ns(),
                                });
                                if event_tx.send(event).is_err() { return; }
                                continue;
                            }

                            // Heartbeat (1/s): cross-check that we haven't
                            // fallen behind the server's last_trade_id. We
                            // compare against the PREVIOUS heartbeat (≥1s old)
                            // so an in-flight match isn't a false positive: if
                            // our highest received trade_id is still below what
                            // the server reported a full interval ago, matches
                            // (and likely level2 updates) were dropped to us.
                            if msg_type == "heartbeat" {
                                let product_id = match data.get("product_id").and_then(|v| v.as_str()) {
                                    Some(s) if !s.is_empty() => s,
                                    _ => continue,
                                };
                                let hb_tid = data.get("last_trade_id").and_then(|v| v.as_u64()).unwrap_or(0);
                                if let (Some(&prev_hb), Some(&seen)) =
                                    (hb_prev_trade_id.get(product_id), last_trade_id.get(product_id))
                                {
                                    if seen < prev_hb {
                                        warn!("[Coinbase] {} heartbeat gap: trade_id {} < server {} (1 interval ago) — reconnecting to re-snapshot",
                                            product_id, seen, prev_hb);
                                        break;
                                    }
                                }
                                hb_prev_trade_id.insert(product_id.to_string(), hb_tid);
                                continue;
                            }

                            // Level2 snapshot
                            if msg_type == "snapshot" {
                                let product_id = match data.get("product_id").and_then(|v| v.as_str()) {
                                    Some(s) => s,
                                    None => continue,
                                };
                                let book = books.entry(product_id.to_string()).or_insert_with(LocalBook::new);
                                book.clear();
                                for (key, side) in [("bids", "buy"), ("asks", "sell")] {
                                    if let Some(arr) = data.get(key).and_then(|v| v.as_array()) {
                                        for level in arr {
                                            if let Some(a) = level.as_array() {
                                                let price: f64 = a.first().and_then(|v| v.as_str())
                                                    .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                                let qty: f64 = a.get(1).and_then(|v| v.as_str())
                                                    .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                                book.update(side, price, qty);
                                            }
                                        }
                                    }
                                }
                                book.ready = true;
                                let (bids, asks) = book.snapshot(MAX_DEPTH);
                                if bids.is_empty() || asks.is_empty() { continue; }
                                let event = MarketEvent::OrderBook(OrderBookSnapshot {
                                    exchange: Exchange::Coinbase,
                                    symbol: product_id.to_string(),
                                    bids,
                                    asks,
                                    exchange_timestamp_ns: now_ns(),
                                    local_timestamp_ns: now_ns(),
                                });
                                if event_tx.send(event).is_err() { return; }
                                continue;
                            }

                            // Level2 update
                            if msg_type == "l2update" {
                                let product_id = match data.get("product_id").and_then(|v| v.as_str()) {
                                    Some(s) => s,
                                    None => continue,
                                };
                                let book = match books.get_mut(product_id) {
                                    Some(b) if b.ready => b,
                                    _ => continue,
                                };
                                if let Some(changes) = data.get("changes").and_then(|v| v.as_array()) {
                                    for change in changes {
                                        if let Some(a) = change.as_array() {
                                            let side = a.first().and_then(|v| v.as_str()).unwrap_or("");
                                            let price: f64 = a.get(1).and_then(|v| v.as_str())
                                                .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                            let qty: f64 = a.get(2).and_then(|v| v.as_str())
                                                .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                            book.update(side, price, qty);
                                        }
                                    }
                                }
                                let exchange_ts = data.get("time").and_then(|v| v.as_str())
                                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                                    .map(|dt| dt.timestamp_nanos_opt().unwrap_or(0) as u64)
                                    .unwrap_or_else(now_ns);
                                let (bids, asks) = book.snapshot(MAX_DEPTH);
                                if bids.is_empty() || asks.is_empty() { continue; }
                                let event = MarketEvent::OrderBook(OrderBookSnapshot {
                                    exchange: Exchange::Coinbase,
                                    symbol: product_id.to_string(),
                                    bids,
                                    asks,
                                    exchange_timestamp_ns: exchange_ts,
                                    local_timestamp_ns: now_ns(),
                                });
                                if event_tx.send(event).is_err() { return; }
                            }
                        }
                        Message::Ping(payload) => {
                            let _ = write.send(Message::Pong(payload)).await;
                        }
                        Message::Close(_) => {
                            warn!("[Coinbase] WebSocket closed");
                            break;
                        }
                        _ => {}
                    }
                }
            }
            if shutdown.load(Ordering::Relaxed) { return; }
        }

        if shutdown.load(Ordering::Relaxed) { break; }
        let delay = backoff.next_delay();
        tokio::time::sleep(delay).await;
    }
    info!("[Coinbase] WS task exiting");
}

impl ExchangeMarket for CoinbaseMarket {
    fn connect(&mut self) -> Result<()> {
        let (event_tx, event_rx) = crossbeam_channel::unbounded::<MarketEvent>();
        self.event_rx = Some(event_rx);
        // Per-task shutdown Arc — see binance/market.rs commentary.
        let shutdown = Arc::new(AtomicBool::new(false));
        self.ws_shutdown = shutdown.clone();
        let symbols = self.symbols.clone();

        crate::async_rt::handle().spawn(coinbase_ws_task(symbols, event_tx, shutdown));
        Ok(())
    }

    fn subscribe(&mut self, symbols: &[String]) -> Result<()> {
        self.symbols = symbols.to_vec();
        Ok(())
    }

    fn next_event(&mut self) -> Result<Option<MarketEvent>> {
        let rx = self.event_rx.as_ref().ok_or_else(|| anyhow!("Not connected"))?;
        match rx.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(crossbeam_channel::TryRecvError::Empty) => Ok(None),
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                Err(anyhow!("Coinbase WS task ended unexpectedly"))
            }
        }
    }

    fn disconnect(&mut self) {
        self.ws_shutdown.store(true, Ordering::Relaxed);
        self.event_rx = None;
        info!("[Coinbase] Disconnected");
    }

    fn name(&self) -> &str { "coinbase" }
}

#[cfg(test)]
mod gap_tests {
    use super::trade_id_gap;

    #[test]
    fn baseline_first_trade_is_not_a_gap() {
        assert_eq!(trade_id_gap(None, 100), 0);
    }

    #[test]
    fn contiguous_trades_are_not_a_gap() {
        assert_eq!(trade_id_gap(Some(100), 101), 0);
    }

    #[test]
    fn forward_jump_reports_missed_count() {
        assert_eq!(trade_id_gap(Some(100), 103), 2); // missed 101, 102
        assert_eq!(trade_id_gap(Some(100), 102), 1); // missed 101
    }

    #[test]
    fn duplicate_or_out_of_order_is_not_a_gap() {
        assert_eq!(trade_id_gap(Some(100), 100), 0); // duplicate
        assert_eq!(trade_id_gap(Some(100), 99), 0);  // reorder (won't happen on Coinbase, but safe)
    }
}
