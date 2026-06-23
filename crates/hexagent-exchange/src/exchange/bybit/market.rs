//! Bybit WebSocket market feed — 50-level orderbook depth via orderbook.50 (20ms).
//! Endpoint: wss://stream.bybit.com/v5/public/spot
//! Topic: orderbook.50.{SYMBOL} — pushes top 50 bid/ask levels.
//! Spot supports depths: 1, 50, 200, 1000 (NOT 5/10/20).
//! Symbols: "BTCUSDT" format (USDT-denominated).

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

use crate::exchange::ExchangeMarket;
use crate::types::*;

const BYBIT_WS_URL: &str = "wss://stream.bybit.com/v5/public/spot";
const PING_INTERVAL: Duration = Duration::from_secs(15);
/// Per-task read-side stall watchdog — see binance/market.rs.
const STALE_THRESHOLD: Duration = Duration::from_secs(30);

pub struct BybitMarket {
    symbols: Vec<String>,
    event_rx: Option<crossbeam_channel::Receiver<MarketEvent>>,
    ws_shutdown: Arc<AtomicBool>,
}

impl BybitMarket {
    pub fn new() -> Self {
        Self {
            symbols: Vec::new(),
            event_rx: None,
            ws_shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

async fn bybit_ws_task(
    symbols: Vec<String>,
    event_tx: crossbeam_channel::Sender<MarketEvent>,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = crate::exchange::ReconnectBackoff::new(200, 30_000);

    loop {
        if shutdown.load(Ordering::Relaxed) { break; }

        info!("[Bybit] Connecting to {}", BYBIT_WS_URL);
        let stream = match tokio_tungstenite::connect_async(BYBIT_WS_URL).await {
            Ok((s, _)) => s,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[Bybit] WS connect failed: {}, retry in {:.1}s", e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        backoff.reset();
        let (mut write, mut read) = stream.split();

        let args: Vec<String> = symbols.iter()
            .flat_map(|s| vec![
                format!("orderbook.50.{}", s),
                format!("publicTrade.{}", s),
            ])
            .collect();
        let sub = serde_json::json!({"op": "subscribe", "args": args});
        if let Err(e) = write.send(Message::Text(sub.to_string())).await {
            warn!("[Bybit] subscribe failed: {}", e);
            continue;
        }
        info!("[Bybit] Connected, subscribed to {:?}", symbols);

        let mut ping_interval = tokio::time::interval(PING_INTERVAL);
        ping_interval.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = ping_interval.tick() => {
                    if let Err(e) = write.send(Message::Text(r#"{"op":"ping"}"#.to_string())).await {
                        warn!("[Bybit] Ping send failed: {}", e);
                        break;
                    }
                }
                read_result = tokio::time::timeout(STALE_THRESHOLD, read.next()) => {
                    let msg = match read_result {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => { warn!("[Bybit] WS read error: {}", e); break; }
                        Ok(None) => { warn!("[Bybit] WS closed"); break; }
                        Err(_elapsed) => {
                            warn!("[Bybit] No message for {:.0}s (stall watchdog) — reconnecting",
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

                            let topic = match data.get("topic").and_then(|v| v.as_str()) {
                                Some(t) => t.to_string(),
                                _ => continue,
                            };

                            if topic.starts_with("publicTrade.") {
                                let trades = match data.get("data").and_then(|v| v.as_array()) {
                                    Some(arr) => arr.clone(),
                                    None => continue,
                                };
                                for t in &trades {
                                    let symbol = t.get("s").and_then(|v| v.as_str()).unwrap_or("");
                                    let price: f64 = t.get("p").and_then(|v| v.as_str())
                                        .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                    let quantity: f64 = t.get("v").and_then(|v| v.as_str())
                                        .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                    if price <= 0.0 { continue; }
                                    let side = match t.get("S").and_then(|v| v.as_str()) {
                                        Some("Buy") => Side::Buy,
                                        _ => Side::Sell,
                                    };
                                    let ts_ms = t.get("T").and_then(|v| v.as_u64()).unwrap_or(0);
                                    let event = MarketEvent::Trade(TradeTick {
                                        exchange: Exchange::Bybit,
                                        symbol: symbol.to_string(),
                                        price,
                                        quantity,
                                        side,
                                        exchange_timestamp_ns: ts_ms * 1_000_000,
                                        local_timestamp_ns: now_ns(),
                                    });
                                    if event_tx.send(event).is_err() { return; }
                                }
                                continue;
                            }

                            if !topic.starts_with("orderbook.") { continue; }

                            let item = match data.get("data") {
                                Some(d) => d.clone(),
                                None => continue,
                            };

                            let symbol = item.get("s").and_then(|v| v.as_str())
                                .map(|s| s.to_string())
                                .unwrap_or_else(|| topic.rsplit('.').next().unwrap_or(&topic).to_string());

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

                            let bids = parse_levels("b");
                            let asks = parse_levels("a");
                            if bids.is_empty() || asks.is_empty() { continue; }

                            let ts_ms = data.get("cts")
                                .or_else(|| data.get("ts"))
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0);

                            let event = MarketEvent::OrderBook(OrderBookSnapshot {
                                exchange: Exchange::Bybit,
                                symbol,
                                bids,
                                asks,
                                exchange_timestamp_ns: ts_ms * 1_000_000,
                                local_timestamp_ns: now_ns(),
                            });
                            if event_tx.send(event).is_err() { return; }
                        }
                        Message::Ping(payload) => {
                            let _ = write.send(Message::Pong(payload)).await;
                        }
                        Message::Close(_) => {
                            warn!("[Bybit] WebSocket closed");
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
    info!("[Bybit] WS task exiting");
}

impl ExchangeMarket for BybitMarket {
    fn connect(&mut self) -> Result<()> {
        let (event_tx, event_rx) = crossbeam_channel::unbounded::<MarketEvent>();
        self.event_rx = Some(event_rx);
        // Per-task shutdown Arc — see binance/market.rs.
        let shutdown = Arc::new(AtomicBool::new(false));
        self.ws_shutdown = shutdown.clone();
        let symbols = self.symbols.clone();
        crate::async_rt::handle().spawn(bybit_ws_task(symbols, event_tx, shutdown));
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
                Err(anyhow!("Bybit WS task ended unexpectedly"))
            }
        }
    }

    fn disconnect(&mut self) {
        self.ws_shutdown.store(true, Ordering::Relaxed);
        self.event_rx = None;
        info!("[Bybit] Disconnected");
    }

    fn name(&self) -> &str { "bybit" }
}
