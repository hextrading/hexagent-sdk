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
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

use crate::exchange::{
    ExchangeMarket, WsHealth, POLYMARKET_RTDS_PING_INTERVAL,
    POLYMARKET_RTDS_PING_PAYLOAD, POLYMARKET_WS_HEALTH_LOG_INTERVAL,
};
use crate::types::*;

const RTDS_URL: &str = "wss://ws-live-data.polymarket.com";
/// Per-task read-side stall watchdog — see binance/market.rs.
const STALE_THRESHOLD: Duration = Duration::from_secs(60);
const TOPIC_STALE_WARNING_THRESHOLD: Duration = Duration::from_secs(30);

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
        let connected_at = Instant::now();
        let mut health = WsHealth::new(connected_at);
        let (mut write, mut read) = stream.split();

        // ALWAYS subscribe the whole topic unfiltered and filter client-side
        // (read loop below). Server-side `filters` is a trap, twice over
        // (all observed 2026-07-11):
        //   1. Only ONE subscription per topic is honored — per-symbol
        //      filtered entries silently keep the FIRST and drop the rest
        //      (btc/eth/sol subscribed → only btc pushed). A JSON-array
        //      filters string `[{"symbol":..},{"symbol":..}]` IS accepted
        //      when the filter path works; comma-joined symbols are not.
        //   2. The filtered path itself is FLAKY: in several test windows
        //      every filtered form (single or array) went silently dead —
        //      connection healthy, zero pushes — while the unfiltered
        //      subscription delivered normally in the same window. A 24/7
        //      recorder reconnecting into such a window would record
        //      nothing while looking connected.
        // The unfiltered topic is tiny (~7 symbols × ~1 msg/s), so the
        // client-side filter costs nothing.
        let subs: Vec<serde_json::Value> = vec![serde_json::json!({
            "topic": "crypto_prices_chainlink",
            "type": "*",
        })];

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

        let mut ping_interval = tokio::time::interval(POLYMARKET_RTDS_PING_INTERVAL);
        ping_interval.tick().await;
        let mut health_interval =
            tokio::time::interval(POLYMARKET_WS_HEALTH_LOG_INTERVAL);
        health_interval.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = ping_interval.tick() => {
                    let now = Instant::now();
                    if let Err(e) = write
                        .send(Message::Text(POLYMARKET_RTDS_PING_PAYLOAD.to_string()))
                        .await
                    {
                        warn!(
                            "[Chainlink] RTDS ping send failed: {}; {}",
                            e,
                            health.rtds_summary(now),
                        );
                        break;
                    }
                    if let Err(e) = write.send(Message::Ping(Vec::new())).await {
                        warn!(
                            "[Chainlink] RTDS frame Ping send failed: {}; {}",
                            e,
                            health.rtds_summary(now),
                        );
                        break;
                    }
                }
                _ = health_interval.tick() => {
                    let now = Instant::now();
                    if health.topic_is_stale(now, TOPIC_STALE_WARNING_THRESHOLD) {
                        warn!(
                            "[Chainlink] RTDS subscription silent; {}",
                            health.rtds_summary(now),
                        );
                    } else if health
                        .btc_price_is_stale(now, TOPIC_STALE_WARNING_THRESHOLD)
                    {
                        warn!(
                            "[Chainlink] RTDS BTC price gap; {}",
                            health.rtds_summary(now),
                        );
                    }
                }
                read_result = tokio::time::timeout(STALE_THRESHOLD, read.next()) => {
                    let msg = match read_result {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => {
                            let now = Instant::now();
                            warn!(
                                "[Chainlink] WS read error: {}; {}",
                                e,
                                health.rtds_summary(now),
                            );
                            break;
                        }
                        Ok(None) => {
                            let now = Instant::now();
                            warn!(
                                "[Chainlink] WS closed; {}",
                                health.rtds_summary(now),
                            );
                            break;
                        }
                        Err(_elapsed) => {
                            let now = Instant::now();
                            warn!(
                                "[Chainlink] No raw frame for {:.0}s (stall watchdog) — reconnecting; {}",
                                STALE_THRESHOLD.as_secs_f64(),
                                health.rtds_summary(now),
                            );
                            break;
                        }
                    };
                    let received_at = Instant::now();
                    health.record_raw_frame(received_at);
                    match msg {
                        Message::Ping(payload) => {
                            let _ = write.send(Message::Pong(payload)).await;
                        }
                        Message::Pong(_) => {
                            health.record_pong(received_at);
                        }
                        Message::Close(reason) => {
                            // 1000 "Normal" / 1001 "Going away" = Polymarket RTDS
                            // recycles the connection ~every 2h (server-side lifecycle
                            // cap, not a keepalive failure) — log at INFO. Abnormal
                            // close codes stay WARN. The reconnect below is ~330ms.
                            let code: Option<u16> = reason.as_ref().map(|c| c.code.into());
                            if matches!(code, Some(1000) | Some(1001)) {
                                info!(
                                    "[Chainlink] Server closed (expected recycle): {:?}; {}",
                                    reason,
                                    health.rtds_summary(received_at),
                                );
                            } else {
                                warn!(
                                    "[Chainlink] Server closed: {:?}; {}",
                                    reason,
                                    health.rtds_summary(received_at),
                                );
                            }
                            break;
                        }
                        Message::Text(text) => {
                            if text.is_empty() { continue; }
                            let body = text.trim();
                            if body.eq_ignore_ascii_case("PONG") {
                                health.record_pong(received_at);
                                continue;
                            }
                            if body.eq_ignore_ascii_case("PING") {
                                let _ = write.send(Message::Text("PONG".to_string())).await;
                                continue;
                            }
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
                            health.record_topic_frame(received_at);
                            let payload = match data.get("payload") {
                                Some(p) => p.clone(),
                                None => continue,
                            };
                            let symbol = match payload.get("symbol").and_then(|v| v.as_str()) {
                                Some(s) => s.to_string(),
                                None => continue,
                            };
                            let price = match payload.get("value").and_then(json_f64) {
                                Some(p) => p,
                                None => continue,
                            };
                            let server_ts_ms = data.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0);

                            if symbol.eq_ignore_ascii_case("btc/usd") {
                                health.record_btc_price(received_at);
                            }

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

fn json_f64(value: &serde_json::Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
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
