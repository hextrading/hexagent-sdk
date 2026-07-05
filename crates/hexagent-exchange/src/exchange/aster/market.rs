//! Aster market-data feed — the `ExchangeMarket` (WS) impl.
//!
//! Subscribes the **partial book depth** stream (`<sym>@depth20@100ms`) +
//! **aggTrade** + **markPrice@1s** per symbol over one combined-stream
//! connection. Partial depth pushes the absolute top-20 book each message —
//! like Hyperliquid's `l2Book`, there is no local incremental book to
//! maintain; each message maps straight to an `OrderBookSnapshot`.
//!
//! `markPrice@1s` doubles as a **liveness heartbeat**: depth/aggTrade only
//! push when the book/tape changes, so on a quiet market a healthy
//! connection can be silent long enough to trip both this adapter's stall
//! watchdog and the engine's no-data watchdog into pointless reconnect
//! churn. Mark price pushes every second regardless of trading activity;
//! it's forwarded as a `SpotPrice` event (source `"aster_mark"`) which
//! downstream index code ignores (unknown source) but which keeps the
//! watchdogs fed.
//!
//! Server behaviour (V3 docs): ping frame every 5 min (must pong within
//! 15 min), 24 h connection lifetime — the reconnect loop covers both.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use tokio_tungstenite::tungstenite::Message;

use crate::types::{
    now_ns, Exchange, MarketEvent, OrderBookSnapshot, PriceLevel, Side, SpotPrice, TradeTick,
};

/// Book pushes arrive every 100 ms; anything quieter than this means the
/// connection is dead even if TCP hasn't noticed.
const STALE_THRESHOLD: std::time::Duration = std::time::Duration::from_secs(30);

pub struct AsterMarket {
    symbols: Vec<String>,
    /// WS base, e.g. `wss://fstream.asterdex.com` (no path).
    ws_base: String,
    event_rx: Option<crossbeam_channel::Receiver<MarketEvent>>,
    ws_shutdown: Arc<AtomicBool>,
}

impl AsterMarket {
    /// `ws_base` empty → mainnet default (the engine passes the resolved
    /// base from the exchange config / network field).
    pub fn new(ws_base: &str) -> Self {
        Self {
            symbols: Vec::new(),
            ws_base: ws_base.trim_end_matches('/').to_string(),
            event_rx: None,
            ws_shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

fn parse_levels(arr: Option<&serde_json::Value>) -> Vec<PriceLevel> {
    arr.and_then(|v| v.as_array())
        .map(|levels| {
            levels
                .iter()
                .filter_map(|lvl| {
                    let pair = lvl.as_array()?;
                    let px: f64 = pair.first()?.as_str()?.parse().ok()?;
                    let sz: f64 = pair.get(1)?.as_str()?.parse().ok()?;
                    Some(PriceLevel { price: px, quantity: sz })
                })
                .collect()
        })
        .unwrap_or_default()
}

async fn aster_ws_task(
    symbols: Vec<String>,
    ws_base: String,
    event_tx: crossbeam_channel::Sender<MarketEvent>,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = crate::exchange::ReconnectBackoff::new(200, 30_000);

    // Combined stream: /stream?streams=btcusdt@depth20@100ms/btcusdt@aggTrade
    let streams: Vec<String> = symbols
        .iter()
        .flat_map(|s| {
            let ls = s.to_lowercase();
            [
                format!("{}@depth20@100ms", ls),
                format!("{}@aggTrade", ls),
                format!("{}@markPrice@1s", ls), // liveness heartbeat (see module docs)
            ]
        })
        .collect();
    let url = format!("{}/stream?streams={}", ws_base, streams.join("/"));

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        info!("[Aster] Connecting to {}", url);
        let stream = match tokio_tungstenite::connect_async(&url).await {
            Ok((s, _)) => s,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[Aster] WS connect failed: {}, retry in {:.1}s", e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        backoff.reset();
        let (mut write, mut read) = stream.split();
        info!("[Aster] Connected, streaming {:?}", streams);

        loop {
            let read_result = tokio::time::timeout(STALE_THRESHOLD, read.next()).await;
            let msg = match read_result {
                Ok(Some(Ok(m))) => m,
                Ok(Some(Err(e))) => { warn!("[Aster] WS read error: {}", e); break; }
                Ok(None) => { warn!("[Aster] WS closed"); break; }
                Err(_elapsed) => {
                    warn!("[Aster] No message for {:.0}s (stall watchdog) — reconnecting",
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
                    // Combined-stream wrapper: {"stream":"…","data":{…}}
                    let payload = data.get("data").unwrap_or(&data);
                    let event_type = payload.get("e").and_then(|v| v.as_str()).unwrap_or("");
                    match event_type {
                        "depthUpdate" => {
                            let symbol = payload.get("s").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let bids = parse_levels(payload.get("b"));
                            let asks = parse_levels(payload.get("a"));
                            if bids.is_empty() || asks.is_empty() { continue; }
                            let ts = payload.get("E").and_then(|v| v.as_u64())
                                .map(|ms| ms * 1_000_000).unwrap_or_else(now_ns);
                            let event = MarketEvent::OrderBook(OrderBookSnapshot {
                                exchange: Exchange::Aster,
                                symbol,
                                bids,
                                asks,
                                exchange_timestamp_ns: ts,
                                local_timestamp_ns: now_ns(),
                            });
                            if event_tx.send(event).is_err() { return; }
                        }
                        "aggTrade" => {
                            let symbol = payload.get("s").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let price: f64 = payload.get("p").and_then(|v| v.as_str())
                                .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            let quantity: f64 = payload.get("q").and_then(|v| v.as_str())
                                .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            if price <= 0.0 { continue; }
                            // `m` = buyer is maker → aggressor was the SELLER.
                            let side = match payload.get("m").and_then(|v| v.as_bool()) {
                                Some(true) => Side::Sell,
                                _ => Side::Buy,
                            };
                            let ts = payload.get("T").and_then(|v| v.as_u64())
                                .map(|ms| ms * 1_000_000).unwrap_or_else(now_ns);
                            let event = MarketEvent::Trade(TradeTick {
                                exchange: Exchange::Aster,
                                symbol,
                                price,
                                quantity,
                                side,
                                exchange_timestamp_ns: ts,
                                local_timestamp_ns: now_ns(),
                            });
                            if event_tx.send(event).is_err() { return; }
                        }
                        "markPriceUpdate" => {
                            let symbol = payload.get("s").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let price: f64 = payload.get("p").and_then(|v| v.as_str())
                                .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            if price <= 0.0 { continue; }
                            let ts = payload.get("E").and_then(|v| v.as_u64())
                                .map(|ms| ms * 1_000_000).unwrap_or_else(now_ns);
                            let event = MarketEvent::SpotPrice(SpotPrice {
                                source: "aster_mark".to_string(),
                                symbol,
                                price,
                                timestamp_ns: ts,
                                local_timestamp_ns: now_ns(),
                            });
                            if event_tx.send(event).is_err() { return; }
                        }
                        _ => {}
                    }
                }
                Message::Ping(payload) => {
                    let _ = write.send(Message::Pong(payload)).await;
                }
                Message::Close(_) => {
                    warn!("[Aster] WebSocket closed");
                    break;
                }
                _ => {}
            }
            if shutdown.load(Ordering::Relaxed) { return; }
        }

        if shutdown.load(Ordering::Relaxed) { break; }
        let delay = backoff.next_delay();
        tokio::time::sleep(delay).await;
    }
    info!("[Aster] WS task exiting");
}

impl super::super::ExchangeMarket for AsterMarket {
    fn connect(&mut self) -> Result<()> {
        let (event_tx, event_rx) = crossbeam_channel::unbounded::<MarketEvent>();
        self.event_rx = Some(event_rx);
        let shutdown = Arc::new(AtomicBool::new(false));
        self.ws_shutdown = shutdown.clone();
        let symbols = self.symbols.clone();
        let ws_base = if self.ws_base.is_empty() {
            super::auth::Network::Mainnet.ws_base().to_string()
        } else {
            self.ws_base.clone()
        };
        crate::async_rt::handle().spawn(aster_ws_task(symbols, ws_base, event_tx, shutdown));
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
                Err(anyhow!("Aster WS task ended unexpectedly"))
            }
        }
    }

    fn disconnect(&mut self) {
        self.ws_shutdown.store(true, Ordering::Relaxed);
        self.event_rx = None;
        info!("[Aster] Disconnected");
    }

    fn name(&self) -> &str {
        "aster"
    }

    fn has_active_subscription(&self) -> bool {
        !self.symbols.is_empty()
    }
}
