//! Bitget WebSocket market feed — 5-level orderbook depth (books5 channel).
//! Endpoint: wss://ws.bitget.com/v2/ws/public
//! Channel: books5 — pushes top 5 bid/ask levels as snapshots.
//! Symbols: "BTCUSDT" format.

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

use crate::exchange::ExchangeMarket;
use crate::types::*;

const BITGET_WS_URL: &str = "wss://ws.bitget.com/v2/ws/public";
const PING_INTERVAL: Duration = Duration::from_secs(25);
/// Per-task read-side stall watchdog — see binance/market.rs.
const STALE_THRESHOLD: Duration = Duration::from_secs(30);

pub struct BitgetMarket {
    symbols: Vec<String>,
    event_rx: Option<crossbeam_channel::Receiver<MarketEvent>>,
    ws_shutdown: Arc<AtomicBool>,
}

impl BitgetMarket {
    pub fn new() -> Self {
        Self {
            symbols: Vec::new(),
            event_rx: None,
            ws_shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

async fn bitget_ws_task(
    symbols: Vec<String>,
    event_tx: crossbeam_channel::Sender<MarketEvent>,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = crate::exchange::ReconnectBackoff::new(200, 30_000);

    loop {
        if shutdown.load(Ordering::Relaxed) { break; }

        info!("[Bitget] Connecting to {}", BITGET_WS_URL);
        let stream = match tokio_tungstenite::connect_async(BITGET_WS_URL).await {
            Ok((s, _)) => s,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[Bitget] WS connect failed: {}, retry in {:.1}s", e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        backoff.reset();
        let (mut write, mut read) = stream.split();

        let args: Vec<serde_json::Value> = symbols.iter()
            .flat_map(|s| vec![
                serde_json::json!({"instType": "SPOT", "channel": "books5", "instId": s}),
                serde_json::json!({"instType": "SPOT", "channel": "trade", "instId": s}),
            ])
            .collect();
        let sub = serde_json::json!({"op": "subscribe", "args": args});
        if let Err(e) = write.send(Message::Text(sub.to_string())).await {
            warn!("[Bitget] subscribe failed: {}", e);
            continue;
        }
        info!("[Bitget] Connected, subscribed to {:?}", symbols);

        let mut ping_interval = tokio::time::interval(PING_INTERVAL);
        ping_interval.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = ping_interval.tick() => {
                    if let Err(e) = write.send(Message::Text("ping".to_string())).await {
                        warn!("[Bitget] Ping send failed: {}", e);
                        break;
                    }
                }
                read_result = tokio::time::timeout(STALE_THRESHOLD, read.next()) => {
                    let msg = match read_result {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => { warn!("[Bitget] WS read error: {}", e); break; }
                        Ok(None) => { warn!("[Bitget] WS closed"); break; }
                        Err(_elapsed) => {
                            warn!("[Bitget] No message for {:.0}s (stall watchdog) — reconnecting",
                                STALE_THRESHOLD.as_secs_f64());
                            break;
                        }
                    };
                    match msg {
                        Message::Text(text) => {
                            if text == "pong" { continue; }
                            // simd-json drop-in for SIMD parse speedup.
                            let mut buf = text.as_bytes().to_vec();
                            let data: serde_json::Value = match simd_json::serde::from_slice(&mut buf) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                            let arg = match data.get("arg") {
                                Some(a) => a.clone(),
                                None => continue,
                            };
                            let channel = arg.get("channel").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let inst_id = match arg.get("instId").and_then(|v| v.as_str()) {
                                Some(s) => s.to_string(),
                                None => continue,
                            };
                            let items = match data.get("data").and_then(|v| v.as_array()) {
                                Some(arr) => arr.clone(),
                                None => continue,
                            };
                            for item in &items {
                                if channel == "trade" {
                                    let price: f64 = item.get("price").and_then(|v| v.as_str())
                                        .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                    let quantity: f64 = item.get("size").and_then(|v| v.as_str())
                                        .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                    if price <= 0.0 { continue; }
                                    let side = match item.get("side").and_then(|v| v.as_str()) {
                                        Some("Buy") | Some("buy") => Side::Buy,
                                        _ => Side::Sell,
                                    };
                                    let ts_ms = item.get("ts")
                                        .and_then(|v| v.as_str().and_then(|s| s.parse::<u64>().ok()).or_else(|| v.as_u64()))
                                        .unwrap_or(0);
                                    let event = MarketEvent::Trade(TradeTick {
                                        exchange: Exchange::Bitget,
                                        symbol: inst_id.clone(),
                                        price,
                                        quantity,
                                        side,
                                        exchange_timestamp_ns: ts_ms * 1_000_000,
                                        local_timestamp_ns: now_ns(),
                                    });
                                    if event_tx.send(event).is_err() { return; }
                                } else if channel == "books5" {
                                    let parse_levels = |key: &str| -> Vec<PriceLevel> {
                                        item.get(key)
                                            .and_then(|v| v.as_array())
                                            .map(|arr| {
                                                arr.iter()
                                                    .filter_map(|level| {
                                                        let a = level.as_array()?;
                                                        Some(PriceLevel {
                                                            price: a.first()?.as_str()?.parse().ok()?,
                                                            quantity: a.get(1)?.as_str()?.parse().ok()?,
                                                        })
                                                    })
                                                    .collect()
                                            })
                                            .unwrap_or_default()
                                    };
                                    let bids = parse_levels("bids");
                                    let asks = parse_levels("asks");
                                    if bids.is_empty() || asks.is_empty() { continue; }
                                    let ts_ms = item.get("ts")
                                        .and_then(|v| v.as_str().and_then(|s| s.parse::<u64>().ok()).or_else(|| v.as_u64()))
                                        .unwrap_or(0);
                                    let event = MarketEvent::OrderBook(OrderBookSnapshot {
                                        exchange: Exchange::Bitget,
                                        symbol: inst_id.clone(),
                                        bids,
                                        asks,
                                        exchange_timestamp_ns: ts_ms * 1_000_000,
                                        local_timestamp_ns: now_ns(),
                                    });
                                    if event_tx.send(event).is_err() { return; }
                                }
                            }
                        }
                        Message::Ping(payload) => {
                            let _ = write.send(Message::Pong(payload)).await;
                        }
                        Message::Close(_) => {
                            warn!("[Bitget] WebSocket closed");
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
    info!("[Bitget] WS task exiting");
}

impl ExchangeMarket for BitgetMarket {
    fn connect(&mut self) -> Result<()> {
        let (event_tx, event_rx) = crossbeam_channel::unbounded::<MarketEvent>();
        self.event_rx = Some(event_rx);
        // Per-task shutdown Arc — see binance/market.rs.
        let shutdown = Arc::new(AtomicBool::new(false));
        self.ws_shutdown = shutdown.clone();
        let symbols = self.symbols.clone();
        crate::async_rt::handle().spawn(bitget_ws_task(symbols, event_tx, shutdown));
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
                Err(anyhow!("Bitget WS task ended unexpectedly"))
            }
        }
    }

    fn disconnect(&mut self) {
        self.ws_shutdown.store(true, Ordering::Relaxed);
        self.event_rx = None;
        info!("[Bitget] Disconnected");
    }

    fn name(&self) -> &str { "bitget" }
}
