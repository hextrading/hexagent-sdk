//! Gate.io WebSocket market feed — 5-level orderbook depth (spot.order_book, 100ms).
//! Endpoint: wss://api.gateio.ws/ws/v4/
//! Channel: spot.order_book — pushes top N bid/ask levels as snapshots.
//! Symbols: "BTC_USDT" format (underscore-separated).

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

use crate::exchange::ExchangeMarket;
use crate::types::*;

const GATE_WS_URL: &str = "wss://api.gateio.ws/ws/v4/";
const PING_INTERVAL: Duration = Duration::from_secs(20);
/// Per-task read-side stall watchdog — see binance/market.rs.
const STALE_THRESHOLD: Duration = Duration::from_secs(30);

pub struct GateMarket {
    symbols: Vec<String>,
    event_rx: Option<crossbeam_channel::Receiver<MarketEvent>>,
    ws_shutdown: Arc<AtomicBool>,
}

impl GateMarket {
    pub fn new() -> Self {
        Self {
            symbols: Vec::new(),
            event_rx: None,
            ws_shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

async fn gate_ws_task(
    symbols: Vec<String>,
    event_tx: crossbeam_channel::Sender<MarketEvent>,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = crate::exchange::ReconnectBackoff::new(200, 30_000);

    loop {
        if shutdown.load(Ordering::Relaxed) { break; }

        info!("[Gate] Connecting to {}", GATE_WS_URL);
        let stream = match tokio_tungstenite::connect_async(GATE_WS_URL).await {
            Ok((s, _)) => s,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[Gate] WS connect failed: {}, retry in {:.1}s", e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        backoff.reset();
        let (mut write, mut read) = stream.split();

        let mut sub_failed = false;
        for symbol in &symbols {
            let msg = serde_json::json!({
                "time": chrono::Utc::now().timestamp(),
                "channel": "spot.order_book",
                "event": "subscribe",
                "payload": [symbol, "5", "100ms"],
            });
            if write.send(Message::Text(msg.to_string())).await.is_err() { sub_failed = true; break; }

            let msg = serde_json::json!({
                "time": chrono::Utc::now().timestamp(),
                "channel": "spot.trades",
                "event": "subscribe",
                "payload": [symbol],
            });
            if write.send(Message::Text(msg.to_string())).await.is_err() { sub_failed = true; break; }
        }
        if sub_failed { continue; }

        info!("[Gate] Connected, subscribed to {:?}", symbols);

        let mut ping_interval = tokio::time::interval(PING_INTERVAL);
        ping_interval.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = ping_interval.tick() => {
                    let ping = serde_json::json!({
                        "time": chrono::Utc::now().timestamp(),
                        "channel": "spot.ping",
                    });
                    if let Err(e) = write.send(Message::Text(ping.to_string())).await {
                        warn!("[Gate] Ping send failed: {}", e);
                        break;
                    }
                }
                read_result = tokio::time::timeout(STALE_THRESHOLD, read.next()) => {
                    let msg = match read_result {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => { warn!("[Gate] WS read error: {}", e); break; }
                        Ok(None) => { warn!("[Gate] WS closed"); break; }
                        Err(_elapsed) => {
                            warn!("[Gate] No message for {:.0}s (stall watchdog) — reconnecting",
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
                            let channel = match data.get("channel").and_then(|v| v.as_str()) {
                                Some(c) => c.to_string(),
                                None => continue,
                            };
                            let event_str = data.get("event").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            if event_str != "update" { continue; }

                            if channel == "spot.trades" {
                                let result = match data.get("result") {
                                    Some(r) => r.clone(),
                                    None => continue,
                                };
                                let items: Vec<serde_json::Value> = if let Some(arr) = result.as_array() {
                                    arr.clone()
                                } else if result.is_object() {
                                    vec![result]
                                } else {
                                    continue;
                                };
                                for t in &items {
                                    let price: f64 = t.get("price").and_then(|v| v.as_str())
                                        .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                    let quantity: f64 = t.get("amount").and_then(|v| v.as_str())
                                        .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                    if price <= 0.0 { continue; }
                                    let side = match t.get("side").and_then(|v| v.as_str()) {
                                        Some("buy") => Side::Buy,
                                        _ => Side::Sell,
                                    };
                                    let ts_ns = t.get("create_time_ms")
                                        .and_then(|v| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                                        .map(|ms| (ms * 1_000_000.0) as u64)
                                        .or_else(|| t.get("create_time").and_then(|v| v.as_u64()).map(|s| s * 1_000_000_000))
                                        .unwrap_or(0);
                                    let symbol = t.get("currency_pair").and_then(|v| v.as_str()).unwrap_or("");
                                    let event = MarketEvent::Trade(TradeTick {
                                        exchange: Exchange::Gate,
                                        symbol: symbol.to_string(),
                                        price,
                                        quantity,
                                        side,
                                        exchange_timestamp_ns: ts_ns,
                                        local_timestamp_ns: now_ns(),
                                    });
                                    if event_tx.send(event).is_err() { return; }
                                }
                                continue;
                            }

                            if channel != "spot.order_book" { continue; }
                            let result = match data.get("result") {
                                Some(r) => r.clone(),
                                None => continue,
                            };
                            let symbol = match result.get("s").and_then(|v| v.as_str()) {
                                Some(s) => s.to_string(),
                                None => continue,
                            };
                            let parse_levels = |key: &str| -> Vec<PriceLevel> {
                                result.get(key)
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
                            let ts_ms = result.get("t").and_then(|v| v.as_u64()).unwrap_or(0);
                            let event = MarketEvent::OrderBook(OrderBookSnapshot {
                                exchange: Exchange::Gate,
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
                            warn!("[Gate] WebSocket closed");
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
    info!("[Gate] WS task exiting");
}

impl ExchangeMarket for GateMarket {
    fn connect(&mut self) -> Result<()> {
        let (event_tx, event_rx) = crossbeam_channel::unbounded::<MarketEvent>();
        self.event_rx = Some(event_rx);
        // Per-task shutdown Arc — see binance/market.rs.
        let shutdown = Arc::new(AtomicBool::new(false));
        self.ws_shutdown = shutdown.clone();
        let symbols = self.symbols.clone();
        crate::async_rt::handle().spawn(gate_ws_task(symbols, event_tx, shutdown));
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
                Err(anyhow!("Gate WS task ended unexpectedly"))
            }
        }
    }

    fn disconnect(&mut self) {
        self.ws_shutdown.store(true, Ordering::Relaxed);
        self.event_rx = None;
        info!("[Gate] Disconnected");
    }

    fn name(&self) -> &str { "gate" }
}
