//! Lighter market-data feed — the `ExchangeMarket` (WS) impl.
//!
//! Subscribes `order_book/{market_id}` + `trade/{market_id}` per symbol.
//! `subscribed/order_book` delivers a full snapshot; `update/order_book`
//! messages are **deltas** (size "0.00000" removes a level), so a local book
//! is maintained per market and re-emitted as a top-N `OrderBookSnapshot`
//! on every update. Continuity is tracked via `begin_nonce`/`nonce`: a gap
//! (`begin_nonce > last_nonce + 1`) forces a reconnect + fresh snapshot.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use tokio_tungstenite::tungstenite::Message;

use crate::types::{
    now_ns, Exchange, MarketEvent, OrderBookSnapshot, PriceLevel, Side, TradeTick,
};

use super::info::LighterMeta;

const PING_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
const STALE_THRESHOLD: std::time::Duration = std::time::Duration::from_secs(60);
const MAX_DEPTH: usize = 20;

pub struct LighterMarket {
    symbols: Vec<String>,
    ws_url: String,
    meta: LighterMeta,
    event_rx: Option<crossbeam_channel::Receiver<MarketEvent>>,
    ws_shutdown: Arc<AtomicBool>,
}

impl LighterMarket {
    /// `ws_url` empty → mainnet default (the engine passes the resolved URL).
    /// `meta` maps symbol ↔ market_id (fetched via `info::fetch_meta`).
    pub fn new(ws_url: &str, meta: LighterMeta) -> Self {
        Self {
            symbols: Vec::new(),
            ws_url: ws_url.to_string(),
            meta,
            event_rx: None,
            ws_shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

/// Per-market local book state. Keys are the IEEE-754 bit patterns of the
/// (strictly positive) prices — bit order == numeric order for positive
/// floats, which gives a total-ordered map without an ordered-float dep.
#[derive(Default)]
struct LocalBook {
    /// price bits → size; bids iterated in reverse (best = highest).
    bids: BTreeMap<u64, f64>,
    asks: BTreeMap<u64, f64>,
    last_nonce: u64,
    synced: bool,
}

impl LocalBook {
    fn apply_side(side: &mut BTreeMap<u64, f64>, levels: &[serde_json::Value]) {
        for lvl in levels {
            let (Some(px), Some(sz)) = (
                lvl.get("price").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()),
                lvl.get("size").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()),
            ) else {
                continue;
            };
            if px <= 0.0 {
                continue;
            }
            if sz <= 0.0 {
                side.remove(&px.to_bits());
            } else {
                side.insert(px.to_bits(), sz);
            }
        }
    }

    fn top_levels(&self) -> (Vec<PriceLevel>, Vec<PriceLevel>) {
        let bids = self
            .bids
            .iter()
            .rev()
            .take(MAX_DEPTH)
            .map(|(p, q)| PriceLevel { price: f64::from_bits(*p), quantity: *q })
            .collect();
        let asks = self
            .asks
            .iter()
            .take(MAX_DEPTH)
            .map(|(p, q)| PriceLevel { price: f64::from_bits(*p), quantity: *q })
            .collect();
        (bids, asks)
    }
}

/// Extract the market id from a response channel string ("order_book:1").
fn channel_market_id(channel: &str) -> Option<i16> {
    channel.rsplit(&[':', '/'][..]).next()?.parse().ok()
}

async fn lighter_ws_task(
    market_ids: HashMap<i16, String>, // market_id → symbol
    ws_url: String,
    event_tx: crossbeam_channel::Sender<MarketEvent>,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = crate::exchange::ReconnectBackoff::new(200, 30_000);

    'reconnect: loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        info!("[Lighter] Connecting to {}", ws_url);
        let stream = match tokio_tungstenite::connect_async(&ws_url).await {
            Ok((s, _)) => s,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[Lighter] WS connect failed: {}, retry in {:.1}s", e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        backoff.reset();
        let (mut write, mut read) = stream.split();

        // Subscribe order_book + trade per market.
        for (mid, _) in &market_ids {
            for ch in ["order_book", "trade"] {
                let sub = serde_json::json!({
                    "type": "subscribe",
                    "channel": format!("{}/{}", ch, mid),
                });
                if let Err(e) = write.send(Message::Text(sub.to_string())).await {
                    warn!("[Lighter] subscribe {}/{} failed: {}", ch, mid, e);
                    continue 'reconnect;
                }
            }
        }
        info!("[Lighter] Connected, subscribed order_book+trade for {:?}", market_ids);

        let mut books: HashMap<i16, LocalBook> = HashMap::new();
        let mut ping_interval = tokio::time::interval(PING_INTERVAL);
        ping_interval.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = ping_interval.tick() => {
                    // Server requires at least one client frame per 2 minutes.
                    let ping = serde_json::json!({ "type": "ping" }).to_string();
                    if let Err(e) = write.send(Message::Text(ping)).await {
                        warn!("[Lighter] Ping send failed: {}", e);
                        break;
                    }
                }
                read_result = tokio::time::timeout(STALE_THRESHOLD, read.next()) => {
                    let msg = match read_result {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => { warn!("[Lighter] WS read error: {}", e); break; }
                        Ok(None) => { warn!("[Lighter] WS closed"); break; }
                        Err(_elapsed) => {
                            warn!("[Lighter] No message for {:.0}s (stall watchdog) — reconnecting",
                                STALE_THRESHOLD.as_secs_f64());
                            break;
                        }
                    };
                    match msg {
                        Message::Text(text) => {
                            let mut buf = text.as_bytes().to_vec();
                            let data: serde_json::Value = match simd_json::serde::from_slice(&mut buf) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                            let msg_type = data.get("type").and_then(|v| v.as_str()).unwrap_or("");
                            match msg_type {
                                "ping" => {
                                    let pong = serde_json::json!({ "type": "pong" }).to_string();
                                    let _ = write.send(Message::Text(pong)).await;
                                }
                                "subscribed/order_book" | "update/order_book" => {
                                    let channel = data.get("channel").and_then(|v| v.as_str()).unwrap_or("");
                                    let Some(mid) = channel_market_id(channel) else { continue };
                                    let Some(symbol) = market_ids.get(&mid) else { continue };
                                    let Some(ob) = data.get("order_book") else { continue };
                                    let book = books.entry(mid).or_default();

                                    let is_snapshot = msg_type == "subscribed/order_book";
                                    if is_snapshot {
                                        book.bids.clear();
                                        book.asks.clear();
                                        book.synced = true;
                                    } else if book.synced {
                                        // Continuity: deltas carry [begin_nonce, nonce];
                                        // a hole means missed updates → resync.
                                        let begin = ob.get("begin_nonce").and_then(|v| v.as_u64()).unwrap_or(0);
                                        if book.last_nonce > 0 && begin > book.last_nonce + 1 {
                                            warn!("[Lighter] order_book nonce gap on {} ({} > {}+1) — resyncing",
                                                symbol, begin, book.last_nonce);
                                            continue 'reconnect;
                                        }
                                    } else {
                                        continue; // delta before snapshot — ignore
                                    }
                                    if let Some(n) = ob.get("nonce").and_then(|v| v.as_u64()) {
                                        book.last_nonce = n;
                                    }

                                    if let Some(bids) = ob.get("bids").and_then(|v| v.as_array()) {
                                        LocalBook::apply_side(&mut book.bids, bids);
                                    }
                                    if let Some(asks) = ob.get("asks").and_then(|v| v.as_array()) {
                                        LocalBook::apply_side(&mut book.asks, asks);
                                    }

                                    let (bids, asks) = book.top_levels();
                                    if bids.is_empty() || asks.is_empty() { continue; }
                                    // last_updated_at is µs since epoch.
                                    let ts = data.get("last_updated_at").and_then(|v| v.as_u64())
                                        .map(|us| us * 1_000).unwrap_or_else(now_ns);
                                    let event = MarketEvent::OrderBook(OrderBookSnapshot {
                                        exchange: Exchange::Lighter,
                                        symbol: symbol.clone(),
                                        bids,
                                        asks,
                                        exchange_timestamp_ns: ts,
                                        local_timestamp_ns: now_ns(),
                                    });
                                    if event_tx.send(event).is_err() { return; }
                                }
                                "update/trade" => {
                                    let channel = data.get("channel").and_then(|v| v.as_str()).unwrap_or("");
                                    let Some(mid) = channel_market_id(channel) else { continue };
                                    let Some(symbol) = market_ids.get(&mid) else { continue };
                                    let Some(trades) = data.get("trades").and_then(|v| v.as_array()) else { continue };
                                    for t in trades {
                                        let price: f64 = t.get("price").and_then(|v| v.as_str())
                                            .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                        let quantity: f64 = t.get("size").and_then(|v| v.as_str())
                                            .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                        if price <= 0.0 { continue; }
                                        // is_maker_ask=true → taker lifted the ask? No:
                                        // maker was the ask → the taker BOUGHT.
                                        let side = match t.get("is_maker_ask").and_then(|v| v.as_bool()) {
                                            Some(true) => Side::Buy,
                                            _ => Side::Sell,
                                        };
                                        let ts = t.get("timestamp").and_then(|v| v.as_u64())
                                            .map(|ms| ms * 1_000_000).unwrap_or_else(now_ns);
                                        let event = MarketEvent::Trade(TradeTick {
                                            exchange: Exchange::Lighter,
                                            symbol: symbol.clone(),
                                            price,
                                            quantity,
                                            side,
                                            exchange_timestamp_ns: ts,
                                            local_timestamp_ns: now_ns(),
                                        });
                                        if event_tx.send(event).is_err() { return; }
                                    }
                                }
                                // connected / subscribed/trade (history) / pong — ignore.
                                _ => {}
                            }
                        }
                        Message::Ping(payload) => {
                            let _ = write.send(Message::Pong(payload)).await;
                        }
                        Message::Close(_) => {
                            warn!("[Lighter] WebSocket closed");
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
    info!("[Lighter] WS task exiting");
}

impl super::super::ExchangeMarket for LighterMarket {
    fn connect(&mut self) -> Result<()> {
        let (event_tx, event_rx) = crossbeam_channel::unbounded::<MarketEvent>();
        self.event_rx = Some(event_rx);
        let shutdown = Arc::new(AtomicBool::new(false));
        self.ws_shutdown = shutdown.clone();

        let mut market_ids = HashMap::new();
        for sym in &self.symbols {
            match self.meta.market_index(sym) {
                Some(mid) => {
                    market_ids.insert(mid, sym.clone());
                }
                None => {
                    return Err(anyhow!("lighter: unknown symbol `{}` (not in orderBookDetails)", sym));
                }
            }
        }

        let ws_url = if self.ws_url.is_empty() {
            "wss://mainnet.zklighter.elliot.ai/stream".to_string()
        } else {
            self.ws_url.clone()
        };
        crate::async_rt::handle().spawn(lighter_ws_task(market_ids, ws_url, event_tx, shutdown));
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
                Err(anyhow!("Lighter WS task ended unexpectedly"))
            }
        }
    }

    fn disconnect(&mut self) {
        self.ws_shutdown.store(true, Ordering::Relaxed);
        self.event_rx = None;
        info!("[Lighter] Disconnected");
    }

    fn name(&self) -> &str {
        "lighter"
    }

    fn has_active_subscription(&self) -> bool {
        !self.symbols.is_empty()
    }
}
