//! Chainlink price feed via Polymarket RTDS WebSocket.
//!
//! Endpoint: wss://ws-live-data.polymarket.com
//! Topics: crypto_prices_chainlink (spot), crypto_prices_twap_sixty (60s TWAP)
//! Symbols: "btc/usd", "eth/usd", "sol/usd", etc.
//!
//! Emits MarketEvent::SpotPrice with source = "chainlink".

use anyhow::{anyhow, Result};
use futures_util::StreamExt;
use log::{info, warn};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

use crate::exchange::{
    ws_send, ExchangeMarket, WsHealth, POLYMARKET_RTDS_PING_INTERVAL, POLYMARKET_RTDS_PING_PAYLOAD,
    POLYMARKET_WS_HEALTH_LOG_INTERVAL, WS_CONNECT_TIMEOUT,
};
use crate::types::*;

const RTDS_URL: &str = "wss://ws-live-data.polymarket.com";
/// Per-task read-side stall watchdog — see binance/market.rs.
///
/// `STALE_THRESHOLD` bounds the RAW-frame read and is defeated the same way the
/// CLOB one is: the server answers our 5 s `ping` with `PONG`, `record_raw_frame`
/// runs before the `PONG` branch, so a heartbeat responder outliving the price
/// feed keeps this timer green forever. `RTDS_TOPIC_STALL_THRESHOLD` bounds the
/// age of the last `crypto_prices_chainlink` frame instead.
///
/// 90 s, measured over 2026-07-21..07-28 (1,692,788 frames, btc/eth/sol merged,
/// `scripts/cmp_rtds_topic_gaps.py` in the hexbot repo):
///
///   p50 0.11s · p90 1.04s · p99 1.66s · p99.9 2.42s · p99.99 8.59s
///   gaps >= 20s: 139 total — 1 in [20,30), 8 in [30,45), 2 in [45,60),
///                5 in [60,90), **0 in [90,115)**, 122 in [121.4,124.7], 1 huge
///
/// The [90,115) bucket is EMPTY, and the 122-strong cluster at ~122.4 s is the
/// engine's own 120 s chainlink data-timeout reconnecting (120 s + ~2.4 s to
/// re-establish). Read together: on this feed, silence that reaches 90 s has
/// never once recovered on its own — it recovers only when someone reconnects.
/// So a 90 s topic watchdog costs zero false trips on that week and recovers
/// each incident ~30 s sooner than the engine backstop does.
///
/// Unlike the CLOB watchdog this one needs NO subscription gate: RTDS has no
/// between-events quiet period to protect (p99.99 is 8.6 s), so any 90 s of
/// silence is anomalous by construction.
const STALE_THRESHOLD: Duration = Duration::from_secs(60);
const RTDS_TOPIC_STALL_THRESHOLD: Duration = Duration::from_secs(90);
const TOPIC_STALE_WARNING_THRESHOLD: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
struct RtdsPriceLiveness {
    last_fresh: Instant,
    source_ns: u64,
}

impl RtdsPriceLiveness {
    fn new(now: Instant) -> Self {
        Self {
            last_fresh: now,
            source_ns: 0,
        }
    }

    fn observe(&mut self, source_ns: u64, now: Instant) {
        // Replayed/duplicate observations cannot disguise a frozen oracle.
        if source_ns > self.source_ns {
            self.source_ns = source_ns;
            self.last_fresh = now;
        }
    }

    fn is_stale(&self, now: Instant) -> bool {
        now.duration_since(self.last_fresh) >= RTDS_TOPIC_STALL_THRESHOLD
    }
}

pub struct ChainlinkMarket {
    tls: Option<crate::exchange::ws_tls::WsTlsConfig>,
    symbols: Vec<String>,
    event_rx: Option<crate::exchange::PublicMarketReceiver>,
    ws_shutdown: Arc<AtomicBool>,
    pending: VecDeque<MarketEvent>,
}

impl ChainlinkMarket {
    pub fn new() -> Self {
        Self {
            tls: None,
            symbols: Vec::new(),
            event_rx: None,
            ws_shutdown: Arc::new(AtomicBool::new(false)),
            pending: VecDeque::new(),
        }
    }
}

async fn chainlink_ws_task(
    symbols: Vec<String>,
    event_tx: crate::exchange::PublicMarketPublisher,
    shutdown: Arc<AtomicBool>,
    tls: crate::exchange::ws_tls::WsTlsConfig,
) {
    // base 0.1s, cap 6.4s → 0.1→0.2→0.4→0.8→1.6→3.2→6.4s (±50% jitter) on
    // consecutive failures. See the stability-gated reset below.
    let mut backoff = crate::exchange::ReconnectBackoff::new(100, 6_400);

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        info!("[Chainlink] Connecting to {}", RTDS_URL);
        let stream = match tokio::time::timeout(
            WS_CONNECT_TIMEOUT,
            tokio_tungstenite::connect_async_tls_with_config(
                RTDS_URL,
                None,
                false,
                Some(tls.connector()),
            ),
        )
        .await
        {
            Ok(Ok((s, _))) => s,
            Ok(Err(e)) => {
                let delay = backoff.next_delay();
                warn!(
                    "[Chainlink] WS connect failed: {}, retry in {:.1}s",
                    e,
                    delay.as_secs_f64()
                );
                tokio::time::sleep(delay).await;
                continue;
            }
            Err(_) => {
                let delay = backoff.next_delay();
                warn!(
                    "[Chainlink] WS connect stalled >{:.0}s (TLS handshake never \
                     completed), retry in {:.1}s",
                    WS_CONNECT_TIMEOUT.as_secs_f64(),
                    delay.as_secs_f64(),
                );
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
        // Task-local, fixed length for this connection. No steady-state growth.
        let mut requested_prices = vec![RtdsPriceLiveness::new(connected_at); symbols.len()];
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
        let mut subs: Vec<serde_json::Value> = vec![serde_json::json!({
            "topic": "crypto_prices_chainlink",
            "type": "*",
        })];

        let needs_twap = symbols.iter().any(|s| s.ends_with("/twap/60s"));
        if needs_twap {
            subs.push(serde_json::json!({"topic": "crypto_prices_twap_sixty", "type": "*"}));
        }
        let mut last_spot_frame = Instant::now();
        let mut last_twap_frame = Instant::now();
        let sub_msg = serde_json::json!({
            "action": "subscribe",
            "subscriptions": subs,
        });
        info!("[Chainlink] Subscribe: {}", sub_msg);
        if let Err(e) = ws_send(&mut write, Message::Text(sub_msg.to_string())).await {
            warn!("[Chainlink] subscribe failed: {}", e);
            continue;
        }
        info!("[Chainlink] Connected, subscribed to {:?}", symbols);

        let mut ping_interval = tokio::time::interval(POLYMARKET_RTDS_PING_INTERVAL);
        ping_interval.tick().await;
        let mut health_interval = tokio::time::interval(POLYMARKET_WS_HEALTH_LOG_INTERVAL);
        health_interval.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = ping_interval.tick() => {
                    let now = Instant::now();
                    if let Err(e) = ws_send(
                        &mut write,
                        Message::Text(POLYMARKET_RTDS_PING_PAYLOAD.to_string()),
                    ).await
                    {
                        warn!(
                            "[Chainlink] RTDS ping send failed: {}; {}",
                            e,
                            health.rtds_summary(now),
                        );
                        break;
                    }
                    if let Err(e) = ws_send(&mut write, Message::Ping(Vec::new())).await {
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
                    // Topic-level stall watchdog. No subscription gate — see
                    // RTDS_TOPIC_STALL_THRESHOLD: this feed has no legitimate
                    // 90 s quiet period, and silence that deep has never been
                    // observed to self-heal.
                    if now.duration_since(last_spot_frame) >= RTDS_TOPIC_STALL_THRESHOLD
                        || (needs_twap && now.duration_since(last_twap_frame) >= RTDS_TOPIC_STALL_THRESHOLD)
                        || requested_prices.iter().any(|price| price.is_stale(now))
                    {
                        warn!(
                            "[Chainlink] RTDS no topic frame for {:.0}s (topic stall watchdog) \
                             — reconnecting; {}",
                            RTDS_TOPIC_STALL_THRESHOLD.as_secs_f64(),
                            health.rtds_summary(now),
                        );
                        break;
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
                            let _ = ws_send(&mut write, Message::Pong(payload)).await;
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
                                let _ = ws_send(&mut write, Message::Text("PONG".to_string())).await;
                                continue;
                            }
                            // simd-json drop-in for SIMD parse speedup.
                            let mut buf = text.as_bytes().to_vec();
                            let data: serde_json::Value = match simd_json::serde::from_slice(&mut buf) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                            let Some(topic) = data.get("topic").and_then(|v| v.as_str()) else { continue; };
                            match topic {
                                "crypto_prices_chainlink" => last_spot_frame = received_at,
                                "crypto_prices_twap_sixty" if needs_twap => last_twap_frame = received_at,
                                _ => continue,
                            }
                            health.record_topic_frame(received_at);
                            if data.get("type").and_then(|v| v.as_str()) != Some("update") { continue; }
                            let Some(payload) = data.get("payload") else { continue; };
                            let Some(tick) = decode_rtds_tick(topic, payload, now_ns()) else { continue; };
                            if tick.symbol == "btc/usd" {
                                health.record_btc_price(received_at);
                            }
                            let Some(index) = symbols.iter().position(|f| f.eq_ignore_ascii_case(&tick.symbol)) else { continue; };
                            requested_prices[index].observe(tick.timestamp_ns, received_at);
                            let event = MarketEvent::SpotPrice(tick);
                            if crate::exchange::publish_market_event(&event_tx, event).is_err() { return; }
                        }
                        _ => {}
                    }
                }
            }
            if shutdown.load(Ordering::Relaxed) {
                return;
            }
        }

        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        // Reset the exponential backoff only when the connection was stable for
        // ≥30s; otherwise keep escalating 0.1→0.2→…→6.4s so consecutive fast
        // drops back off instead of storming the endpoint into HTTP 429.
        if connected_at.elapsed().as_secs() >= 30 {
            backoff.reset();
        }
        let delay = backoff.next_delay();
        tokio::time::sleep(delay).await;
    }
    info!("[Chainlink] WS task exiting");
}

/// Decode only live updates, never replay a subscription snapshot into the live
/// oracle cache. RTDS's envelope timestamp is publication time; the payload
/// timestamp is the Chainlink observation used for event boundary selection.
fn decode_rtds_tick(
    topic: &str,
    payload: &serde_json::Value,
    received_ns: u64,
) -> Option<SpotPrice> {
    let twap = match topic {
        "crypto_prices_chainlink" => false,
        "crypto_prices_twap_sixty" => true,
        _ => return None,
    };
    if twap && payload.get("window_s")?.as_u64()? != 60 {
        return None;
    }
    let symbol = payload.get("symbol")?.as_str()?;
    if !symbol.ends_with("/usd") {
        return None;
    }
    let timestamp_ms = payload.get("timestamp")?.as_u64()?;
    if timestamp_ms == 0 {
        return None;
    }
    let price = match payload.get("full_accuracy_value") {
        Some(value) => value.as_str()?.parse::<f64>().ok()? / 1e18,
        None => json_f64(payload.get("value")?)?,
    };
    if !price.is_finite() || price <= 0.0 {
        return None;
    }
    Some(SpotPrice {
        source: "chainlink".to_owned(),
        symbol: if twap {
            format!("{symbol}/twap/60s")
        } else {
            symbol.to_owned()
        },
        price,
        timestamp_ns: timestamp_ms.checked_mul(1_000_000)?,
        local_timestamp_ns: received_ns,
    })
}

fn json_f64(value: &serde_json::Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

impl ExchangeMarket for ChainlinkMarket {
    fn connect(&mut self) -> Result<()> {
        // Prepare native trust before the feed future reaches the reactor.
        // Each adapter owns an immutable configuration reused across reconnects.
        if self.tls.is_none() {
            self.tls = Some(crate::exchange::ws_tls::WsTlsConfig::load_native()?);
        }
        let tls = self.tls.as_ref().expect("TLS prepared").clone();
        let (event_tx, event_rx) = crate::exchange::public_market_channel();
        self.event_rx = Some(event_rx);
        // Per-task shutdown Arc — see binance/market.rs.
        let shutdown = Arc::new(AtomicBool::new(false));
        self.ws_shutdown = shutdown.clone();
        let symbols = self.symbols.clone();
        crate::async_rt::handle().spawn(chainlink_ws_task(symbols, event_tx, shutdown, tls));
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
        let rx = self
            .event_rx
            .as_ref()
            .ok_or_else(|| anyhow!("Not connected"))?;
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

    fn name(&self) -> &str {
        "chainlink"
    }
}

#[cfg(test)]
mod rtds_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn requested_prices_cannot_mask_each_other_or_refresh_from_replay() {
        let start = Instant::now();
        let mut prices = [RtdsPriceLiveness::new(start); 3]; // BTC, BTC TWAP, ETH
        prices[0].observe(100, start);
        prices[0].observe(100, start + Duration::from_secs(80));
        prices[0].observe(99, start + Duration::from_secs(85));
        prices[1].observe(200, start + Duration::from_secs(80));
        prices[2].observe(300, start + Duration::from_secs(89));
        let now = start + RTDS_TOPIC_STALL_THRESHOLD;
        assert!(prices[0].is_stale(now));
        assert!(!prices[1].is_stale(now));
        assert!(!prices[2].is_stale(now));
        prices[0].observe(101, now);
        assert!(!prices[0].is_stale(now));
        // A reconnect gets a new grace interval and accepts its first source ID.
        prices[0] = RtdsPriceLiveness::new(now);
        prices[0].observe(1, now);
        assert_eq!(prices[0].source_ns, 1);
    }

    #[test]
    fn observation_clock_and_twap_are_not_spot_or_publication_clock() {
        let payload = json!({"symbol":"btc/usd","timestamp":1793679200000u64,
            "full_accuracy_value":"76763196801204162134016","value":1,"window_s":60});
        let tick =
            decode_rtds_tick("crypto_prices_twap_sixty", &payload, 1793679201900000000).unwrap();
        assert_eq!(tick.symbol, "btc/usd/twap/60s");
        assert_eq!(tick.timestamp_ns, 1793679200000000000);
        assert_eq!(tick.local_timestamp_ns, 1793679201900000000);
        assert!((tick.price - 76763.19680120416).abs() < 1e-9);
        let spot = decode_rtds_tick("crypto_prices_chainlink", &payload, 1).unwrap();
        assert_eq!(spot.symbol, "btc/usd");
    }

    #[test]
    fn malformed_or_unsupported_updates_do_not_become_prices() {
        let base = json!({"symbol":"btc/usd","timestamp":1000,"value":123,"window_s":60});
        assert!(decode_rtds_tick("other", &base, 1).is_none());
        for (key, value) in [
            ("timestamp", json!(0)),
            ("timestamp", json!(u64::MAX)),
            ("window_s", json!(30)),
            ("value", json!(-1)),
            ("full_accuracy_value", json!("bad")),
        ] {
            let mut p = base.clone();
            p[key] = value;
            assert!(decode_rtds_tick("crypto_prices_twap_sixty", &p, 1).is_none());
        }
        assert!(decode_rtds_tick(
            "crypto_prices_chainlink",
            &json!({"symbol":"btc/usd","data":[]}),
            1
        )
        .is_none());
    }
}
