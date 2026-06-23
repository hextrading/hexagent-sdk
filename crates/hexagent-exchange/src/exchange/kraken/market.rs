//! Kraken WebSocket v2 market feed — best bid/ask (ticker channel).
//! Endpoint: wss://ws.kraken.com/v2

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

use crate::exchange::ExchangeMarket;
use crate::types::*;

const KRAKEN_WS_URL: &str = "wss://ws.kraken.com/v2";
const PING_INTERVAL: Duration = Duration::from_secs(30);
/// Per-task read-side stall watchdog — see binance/market.rs.
/// Wider than 30 s here because PING_INTERVAL itself is 30 s, so we
/// need a margin above the typical heartbeat-only round-trip.
const STALE_THRESHOLD: Duration = Duration::from_secs(60);

pub struct KrakenMarket {
    symbols: Vec<String>,
    event_rx: Option<crossbeam_channel::Receiver<MarketEvent>>,
    ws_shutdown: Arc<AtomicBool>,
}

impl KrakenMarket {
    pub fn new() -> Self {
        Self {
            symbols: Vec::new(),
            event_rx: None,
            ws_shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

async fn kraken_ws_task(
    symbols: Vec<String>,
    event_tx: crossbeam_channel::Sender<MarketEvent>,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = crate::exchange::ReconnectBackoff::new(200, 30_000);

    loop {
        if shutdown.load(Ordering::Relaxed) { break; }

        info!("[Kraken] Connecting to {}", KRAKEN_WS_URL);
        let stream = match tokio_tungstenite::connect_async(KRAKEN_WS_URL).await {
            Ok((s, _)) => s,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[Kraken] WS connect failed: {}, retry in {:.1}s", e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        backoff.reset();
        let (mut write, mut read) = stream.split();

        let sub = serde_json::json!({
            "method": "subscribe",
            "params": {
                "channel": "ticker",
                "symbol": symbols,
            },
        });
        if let Err(e) = write.send(Message::Text(sub.to_string())).await {
            warn!("[Kraken] subscribe failed: {}", e);
            continue;
        }
        info!("[Kraken] Connected, subscribed to {:?}", symbols);

        let mut ping_interval = tokio::time::interval(PING_INTERVAL);
        ping_interval.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = ping_interval.tick() => {
                    if let Err(e) = write.send(Message::Ping(Vec::new())).await {
                        warn!("[Kraken] Ping send failed: {}", e);
                        break;
                    }
                }
                read_result = tokio::time::timeout(STALE_THRESHOLD, read.next()) => {
                    let msg = match read_result {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => { warn!("[Kraken] WS read error: {}", e); break; }
                        Ok(None) => { warn!("[Kraken] WS closed"); break; }
                        Err(_elapsed) => {
                            warn!("[Kraken] No message for {:.0}s (stall watchdog) — reconnecting",
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
                            let channel = data.get("channel").and_then(|v| v.as_str()).unwrap_or("");
                            if channel != "ticker" { continue; }
                            let items = match data.get("data").and_then(|v| v.as_array()) {
                                Some(arr) => arr.clone(),
                                None => continue,
                            };
                            for item in &items {
                                let symbol = match item.get("symbol").and_then(|v| v.as_str()) {
                                    Some(s) => s.to_string(),
                                    None => continue,
                                };
                                let bid = item.get("bid").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                let ask = item.get("ask").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                let bid_qty = item.get("bid_qty").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                let ask_qty = item.get("ask_qty").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                let event = MarketEvent::Quote(QuoteTick {
                                    exchange: Exchange::Kraken,
                                    symbol,
                                    bid_price: bid,
                                    bid_qty,
                                    ask_price: ask,
                                    ask_qty,
                                    exchange_timestamp_ns: now_ns(),
                                    local_timestamp_ns: now_ns(),
                                });
                                if event_tx.send(event).is_err() { return; }
                            }
                        }
                        Message::Ping(payload) => {
                            let _ = write.send(Message::Pong(payload)).await;
                        }
                        Message::Close(_) => {
                            warn!("[Kraken] WebSocket closed");
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
    info!("[Kraken] WS task exiting");
}

impl ExchangeMarket for KrakenMarket {
    fn connect(&mut self) -> Result<()> {
        let (event_tx, event_rx) = crossbeam_channel::unbounded::<MarketEvent>();
        self.event_rx = Some(event_rx);
        // Per-task shutdown Arc — see binance/market.rs.
        let shutdown = Arc::new(AtomicBool::new(false));
        self.ws_shutdown = shutdown.clone();
        let symbols = self.symbols.clone();
        crate::async_rt::handle().spawn(kraken_ws_task(symbols, event_tx, shutdown));
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
                Err(anyhow!("Kraken WS task ended unexpectedly"))
            }
        }
    }

    fn disconnect(&mut self) {
        self.ws_shutdown.store(true, Ordering::Relaxed);
        self.event_rx = None;
        info!("[Kraken] Disconnected");
    }

    fn name(&self) -> &str { "kraken" }
}
