//! Chainlink price feed via Polymarket RTDS WebSocket.
//!
//! Endpoint: wss://ws-live-data.polymarket.com
//! Topic: crypto_prices_chainlink
//! Symbols: "btc/usd", "eth/usd", "sol/usd", etc.
//!
//! Emits MarketEvent::SpotPrice with source = "chainlink".

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

use crate::exchange::ExchangeMarket;
use crate::types::*;

const RTDS_URL: &str = "wss://ws-live-data.polymarket.com";
const PING_INTERVAL: Duration = Duration::from_secs(4);
/// Per-task read-side stall watchdog — see binance/market.rs.
const STALE_THRESHOLD: Duration = Duration::from_secs(60);

pub struct ChainlinkMarket {
    symbols: Vec<String>,
    event_rx: Option<crossbeam_channel::Receiver<MarketEvent>>,
    ws_shutdown: Arc<AtomicBool>,
    pending: VecDeque<MarketEvent>,
}

impl ChainlinkMarket {
    pub fn new() -> Self {
        Self {
            symbols: Vec::new(),
            event_rx: None,
            ws_shutdown: Arc::new(AtomicBool::new(false)),
            pending: VecDeque::new(),
        }
    }
}

async fn chainlink_ws_task(
    symbols: Vec<String>,
    event_tx: crossbeam_channel::Sender<MarketEvent>,
    shutdown: Arc<AtomicBool>,
) {
    // base 0.1s, cap 6.4s → 0.1→0.2→0.4→0.8→1.6→3.2→6.4s (±50% jitter) on
    // consecutive failures. See the stability-gated reset below.
    let mut backoff = crate::exchange::ReconnectBackoff::new(100, 6_400);

    loop {
        if shutdown.load(Ordering::Relaxed) { break; }

        info!("[Chainlink] Connecting to {}", RTDS_URL);
        let stream = match tokio_tungstenite::connect_async(RTDS_URL).await {
            Ok((s, _)) => s,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[Chainlink] WS connect failed: {}, retry in {:.1}s", e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        // Do NOT reset the backoff on connect: a successful `connect_async`
        // only means the WS upgrade was accepted, not that the connection is
        // healthy. Resetting here let a connect-then-immediate-drop (server-side
        // rate-limit / 429 flap) hammer reconnect every ~base_ms and never back
        // off. Reset only after the connection proves stable (≥30s), below.
        let connected_at = std::time::Instant::now();
        let (mut write, mut read) = stream.split();

        // RTDS honors only ONE subscription per topic per connection: with
        // several per-symbol filtered entries the server silently keeps the
        // FIRST and drops the rest (observed 2026-07-11: btc/eth/sol
        // subscribed → only btc pushed). A filters ARRAY is rejected
        // outright. So: single symbol → keep the server-side filter;
        // multiple → subscribe the whole topic unfiltered (only ~7 symbols
        // at ~1 msg/s each) and rely on the client-side symbol filter in
        // the read loop below.
        let subs: Vec<serde_json::Value> = if symbols.len() == 1 {
            let filters_json = serde_json::json!({"symbol": symbols[0]}).to_string();
            vec![serde_json::json!({
                "topic": "crypto_prices_chainlink",
                "type": "*",
                "filters": filters_json,
            })]
        } else {
            vec![serde_json::json!({
                "topic": "crypto_prices_chainlink",
                "type": "*",
            })]
        };

        let sub_msg = serde_json::json!({
            "action": "subscribe",
            "subscriptions": subs,
        });
        info!("[Chainlink] Subscribe: {}", sub_msg);
        if let Err(e) = write.send(Message::Text(sub_msg.to_string())).await {
            warn!("[Chainlink] subscribe failed: {}", e);
            continue;
        }
        info!("[Chainlink] Connected, subscribed to {:?}", symbols);

        let mut ping_interval = tokio::time::interval(PING_INTERVAL);
        ping_interval.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = ping_interval.tick() => {
                    // RTDS keepalive = the literal text "ping" (NOT JSON), per the
                    // official client (Polymarket/real-time-data-client sends
                    // `ws.send("ping")` every 5s; docs: "send PING every 5s"). The
                    // server replies with a pong frame, which resets the read-side
                    // stall watchdog below — giving a liveness signal independent of
                    // price ticks. The old JSON `{"action":"ping"}` was not a
                    // recognised keepalive (→ server-initiated closes).
                    if let Err(e) = write.send(Message::Text("ping".to_string())).await {
                        warn!("[Chainlink] Ping send failed: {}", e);
                        break;
                    }
                }
                read_result = tokio::time::timeout(STALE_THRESHOLD, read.next()) => {
                    let msg = match read_result {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => { warn!("[Chainlink] WS read error: {}", e); break; }
                        Ok(None) => { warn!("[Chainlink] WS closed"); break; }
                        Err(_elapsed) => {
                            warn!("[Chainlink] No message for {:.0}s (stall watchdog) — reconnecting",
                                STALE_THRESHOLD.as_secs_f64());
                            break;
                        }
                    };
                    match msg {
                        Message::Ping(payload) => {
                            let _ = write.send(Message::Pong(payload)).await;
                        }
                        Message::Close(reason) => {
                            // 1000 "Normal" / 1001 "Going away" = Polymarket RTDS
                            // recycles the connection ~every 2h (server-side lifecycle
                            // cap, not a keepalive failure) — log at INFO. Abnormal
                            // close codes stay WARN. The reconnect below is ~330ms.
                            let code: Option<u16> = reason.as_ref().map(|c| c.code.into());
                            if matches!(code, Some(1000) | Some(1001)) {
                                info!("[Chainlink] Server closed (expected recycle): {:?}", reason);
                            } else {
                                warn!("[Chainlink] Server closed: {:?}", reason);
                            }
                            break;
                        }
                        Message::Text(text) => {
                            if text.is_empty() { continue; }
                            // simd-json drop-in for SIMD parse speedup.
                            let mut buf = text.as_bytes().to_vec();
                            let data: serde_json::Value = match simd_json::serde::from_slice(&mut buf) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                            let topic = match data.get("topic").and_then(|v| v.as_str()) {
                                Some(t) if t == "crypto_prices_chainlink" => t.to_string(),
                                _ => continue,
                            };
                            let payload = match data.get("payload") {
                                Some(p) => p.clone(),
                                None => continue,
                            };
                            let symbol = match payload.get("symbol").and_then(|v| v.as_str()) {
                                Some(s) => s.to_string(),
                                None => continue,
                            };
                            let price = match payload.get("value").and_then(|v| v.as_f64()) {
                                Some(p) => p,
                                None => continue,
                            };
                            let server_ts_ms = data.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0);

                            if !symbols.iter().any(|f| f.eq_ignore_ascii_case(&symbol)) {
                                log::trace!("[Chainlink] Filtered out: topic={} symbol={} price={}", topic, symbol, price);
                                continue;
                            }

                            let event = MarketEvent::SpotPrice(SpotPrice {
                                source: "chainlink".to_string(),
                                symbol,
                                price,
                                timestamp_ns: server_ts_ms * 1_000_000,
                                local_timestamp_ns: now_ns(),
                            });
                            if event_tx.send(event).is_err() { return; }
                        }
                        _ => {}
                    }
                }
            }
            if shutdown.load(Ordering::Relaxed) { return; }
        }

        if shutdown.load(Ordering::Relaxed) { break; }
        // Reset the exponential backoff only when the connection was stable for
        // ≥30s; otherwise keep escalating 0.1→0.2→…→6.4s so consecutive fast
        // drops back off instead of storming the endpoint into HTTP 429.
        if connected_at.elapsed().as_secs() >= 30 { backoff.reset(); }
        let delay = backoff.next_delay();
        tokio::time::sleep(delay).await;
    }
    info!("[Chainlink] WS task exiting");
}

impl ExchangeMarket for ChainlinkMarket {
    fn connect(&mut self) -> Result<()> {
        let (event_tx, event_rx) = crossbeam_channel::unbounded::<MarketEvent>();
        self.event_rx = Some(event_rx);
        // Per-task shutdown Arc — see binance/market.rs.
        let shutdown = Arc::new(AtomicBool::new(false));
        self.ws_shutdown = shutdown.clone();
        let symbols = self.symbols.clone();
        crate::async_rt::handle().spawn(chainlink_ws_task(symbols, event_tx, shutdown));
        Ok(())
    }

    fn subscribe(&mut self, symbols: &[String]) -> Result<()> {
        self.symbols = symbols.to_vec();
        Ok(())
    }

    fn next_event(&mut self) -> Result<Option<MarketEvent>> {
        if let Some(event) = self.pending.pop_front() {
            return Ok(Some(event));
        }
        let rx = self.event_rx.as_ref().ok_or_else(|| anyhow!("Not connected"))?;
        match rx.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(crossbeam_channel::TryRecvError::Empty) => Ok(None),
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                Err(anyhow!("Chainlink WS task ended unexpectedly"))
            }
        }
    }

    fn disconnect(&mut self) {
        self.ws_shutdown.store(true, Ordering::Relaxed);
        self.event_rx = None;
        info!("[Chainlink] Disconnected");
    }

    fn name(&self) -> &str { "chainlink" }
}
