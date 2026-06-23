//! KuCoin WebSocket market feed — 5-level orderbook depth.
//! Token endpoint: POST https://api.kucoin.com/api/v1/bullet-public (no auth required)
//! Topic: /spotMarket/level2Depth5:{SYMBOL} — pushes top 5 bid/ask levels (~100ms).
//! Symbols: "BTC-USDT" format (dash-separated).

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

/// Per-task read-side stall watchdog — see binance/market.rs.
const STALE_THRESHOLD: Duration = Duration::from_secs(30);

use crate::exchange::ExchangeMarket;
use crate::types::*;

pub struct KucoinMarket {
    symbols: Vec<String>,
    event_rx: Option<crossbeam_channel::Receiver<MarketEvent>>,
    ws_shutdown: Arc<AtomicBool>,
}

impl KucoinMarket {
    pub fn new() -> Self {
        Self {
            symbols: Vec::new(),
            event_rx: None,
            ws_shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

/// Fetch public WebSocket token and endpoint from KuCoin REST API.
async fn get_ws_endpoint() -> Result<(String, String, u64)> {
    let client = crate::async_rt::http_client();
    let resp = client
        .post("https://api.kucoin.com/api/v1/bullet-public")
        .send()
        .await
        .map_err(|e| anyhow!("bullet-public POST failed: {}", e))?;
    let text = resp.text().await.map_err(|e| anyhow!("bullet-public body: {}", e))?;
    let resp: serde_json::Value = serde_json::from_str(&text)?;
    let data = resp.get("data").ok_or_else(|| anyhow!("Missing data in bullet-public response"))?;
    let token = data.get("token").and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("Missing token"))?.to_string();
    let server = data.get("instanceServers").and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .ok_or_else(|| anyhow!("No instance servers"))?;
    let endpoint = server.get("endpoint").and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("Missing endpoint"))?.to_string();
    let ping_interval = server.get("pingInterval").and_then(|v| v.as_u64()).unwrap_or(18000);
    Ok((endpoint, token, ping_interval))
}

async fn kucoin_ws_task(
    symbols: Vec<String>,
    event_tx: crossbeam_channel::Sender<MarketEvent>,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = crate::exchange::ReconnectBackoff::new(200, 30_000);

    loop {
        if shutdown.load(Ordering::Relaxed) { break; }

        info!("[KuCoin] Fetching public WebSocket token...");
        let (endpoint, token, ping_interval_ms) = match get_ws_endpoint().await {
            Ok(v) => v,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[KuCoin] token fetch failed: {}, retry in {:.1}s", e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
                continue;
            }
        };

        let connect_id = chrono::Utc::now().timestamp_millis();
        let url = format!("{}?token={}&connectId={}", endpoint, token, connect_id);
        info!("[KuCoin] Connecting to {}", endpoint);
        let stream = match tokio_tungstenite::connect_async(&url).await {
            Ok((s, _)) => s,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[KuCoin] WS connect failed: {}, retry in {:.1}s", e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        backoff.reset();
        let (mut write, mut read) = stream.split();

        let ob_topics: Vec<String> = symbols.iter()
            .map(|s| format!("/spotMarket/level2Depth5:{}", s))
            .collect();
        let msg = serde_json::json!({
            "id": connect_id,
            "type": "subscribe",
            "topic": ob_topics.join(","),
            "response": true,
        });
        if write.send(Message::Text(msg.to_string())).await.is_err() { continue; }

        let trade_topics: Vec<String> = symbols.iter()
            .map(|s| format!("/market/match:{}", s))
            .collect();
        let msg = serde_json::json!({
            "id": connect_id + 1,
            "type": "subscribe",
            "topic": trade_topics.join(","),
            "response": true,
        });
        if write.send(Message::Text(msg.to_string())).await.is_err() { continue; }

        info!("[KuCoin] Connected, subscribed to {:?}", symbols);

        let ping_period = Duration::from_millis(ping_interval_ms.saturating_sub(2000));
        let mut ping_interval = tokio::time::interval(ping_period);
        ping_interval.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = ping_interval.tick() => {
                    let ping = serde_json::json!({
                        "id": chrono::Utc::now().timestamp_millis(),
                        "type": "ping",
                    });
                    if let Err(e) = write.send(Message::Text(ping.to_string())).await {
                        warn!("[KuCoin] Ping send failed: {}", e);
                        break;
                    }
                }
                read_result = tokio::time::timeout(STALE_THRESHOLD, read.next()) => {
                    let msg = match read_result {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => { warn!("[KuCoin] WS read error: {}", e); break; }
                        Ok(None) => { warn!("[KuCoin] WS closed"); break; }
                        Err(_elapsed) => {
                            warn!("[KuCoin] No message for {:.0}s (stall watchdog) — reconnecting",
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
                            let item = match data.get("data") {
                                Some(d) => d.clone(),
                                None => continue,
                            };

                            if topic.starts_with("/market/match:") {
                                let symbol = item.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
                                let price: f64 = item.get("price").and_then(|v| v.as_str())
                                    .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                let quantity: f64 = item.get("size").and_then(|v| v.as_str())
                                    .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                if price <= 0.0 { continue; }
                                let side = match item.get("side").and_then(|v| v.as_str()) {
                                    Some("buy") => Side::Buy,
                                    _ => Side::Sell,
                                };
                                let ts_ns = item.get("time").and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
                                let event = MarketEvent::Trade(TradeTick {
                                    exchange: Exchange::Kucoin,
                                    symbol: symbol.to_string(),
                                    price,
                                    quantity,
                                    side,
                                    exchange_timestamp_ns: ts_ns,
                                    local_timestamp_ns: now_ns(),
                                });
                                if event_tx.send(event).is_err() { return; }
                                continue;
                            }

                            if !topic.starts_with("/spotMarket/level2Depth5:") { continue; }

                            let symbol = topic.split(':').nth(1).unwrap_or(&topic).to_string();

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
                            let ts_ms = item.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0);
                            let event = MarketEvent::OrderBook(OrderBookSnapshot {
                                exchange: Exchange::Kucoin,
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
                            warn!("[KuCoin] WebSocket closed");
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
    info!("[KuCoin] WS task exiting");
}

impl ExchangeMarket for KucoinMarket {
    fn connect(&mut self) -> Result<()> {
        let (event_tx, event_rx) = crossbeam_channel::unbounded::<MarketEvent>();
        self.event_rx = Some(event_rx);
        // Per-task shutdown Arc — see binance/market.rs.
        let shutdown = Arc::new(AtomicBool::new(false));
        self.ws_shutdown = shutdown.clone();
        let symbols = self.symbols.clone();
        crate::async_rt::handle().spawn(kucoin_ws_task(symbols, event_tx, shutdown));
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
                Err(anyhow!("KuCoin WS task ended unexpectedly"))
            }
        }
    }

    fn disconnect(&mut self) {
        self.ws_shutdown.store(true, Ordering::Relaxed);
        self.event_rx = None;
        info!("[KuCoin] Disconnected");
    }

    fn name(&self) -> &str { "kucoin" }
}
