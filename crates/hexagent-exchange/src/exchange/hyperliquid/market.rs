//! Hyperliquid market-data feed — the `ExchangeMarket` (WS) impl.
//!
//! Subscribes `l2Book` + `trades` + `activeAssetCtx` per coin. `l2Book` is
//! a full best-first snapshot each message (`levels = [bids, asks]`), so —
//! unlike Coinbase's incremental `level2` — there's no local book to
//! maintain: each message maps straight to an `OrderBookSnapshot`.
//!
//! `l2Book` is subscribed with `fast: true`: since the 2026-06 network
//! upgrade the standard feed is throttled to a 20-level snapshot every
//! ~5s; `fast` restores ~0.5s cadence at 5 levels. `activeAssetCtx`
//! (mark/oracle px, funding, OI) pushes ~1 msg/s per coin.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use tokio_tungstenite::tungstenite::Message;

use crate::types::{
    now_ns, AssetCtxTick, Exchange, MarketEvent, OrderBookSnapshot, PriceLevel, Side, TradeTick,
};

const PING_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
const STALE_THRESHOLD: std::time::Duration = std::time::Duration::from_secs(60);
const MAX_DEPTH: usize = 20;

pub struct HyperliquidMarket {
    coins: Vec<String>,
    ws_url: String,
    event_rx: Option<crossbeam_channel::Receiver<MarketEvent>>,
    ws_shutdown: Arc<AtomicBool>,
}

impl HyperliquidMarket {
    /// `ws_url` empty → the network default is used (mainnet/testnet resolved
    /// by the engine from the exchange config).
    pub fn new(ws_url: &str) -> Self {
        Self {
            coins: Vec::new(),
            ws_url: ws_url.to_string(),
            event_rx: None,
            ws_shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

fn parse_levels(arr: &serde_json::Value) -> Vec<PriceLevel> {
    arr.as_array()
        .map(|levels| {
            levels
                .iter()
                .filter_map(|lvl| {
                    let px: f64 = lvl.get("px")?.as_str()?.parse().ok()?;
                    let sz: f64 = lvl.get("sz")?.as_str()?.parse().ok()?;
                    Some(PriceLevel { price: px, quantity: sz })
                })
                .take(MAX_DEPTH)
                .collect()
        })
        .unwrap_or_default()
}

async fn hyperliquid_ws_task(
    coins: Vec<String>,
    ws_url: String,
    event_tx: crossbeam_channel::Sender<MarketEvent>,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = crate::exchange::ReconnectBackoff::new(200, 30_000);

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        info!("[Hyperliquid] Connecting to {}", ws_url);
        let stream = match tokio_tungstenite::connect_async(&ws_url).await {
            Ok((s, _)) => s,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[Hyperliquid] WS connect failed: {}, retry in {:.1}s", e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        backoff.reset();
        let (mut write, mut read) = stream.split();

        // Subscribe l2Book (fast: 5 levels / ~0.5s vs throttled 20 levels /
        // ~5s default) + trades + activeAssetCtx per coin.
        let mut sub_ok = true;
        for coin in &coins {
            for ch in ["l2Book", "trades", "activeAssetCtx"] {
                let mut sub_body = serde_json::json!({ "type": ch, "coin": coin });
                if ch == "l2Book" {
                    sub_body["fast"] = serde_json::Value::Bool(true);
                }
                let sub = serde_json::json!({
                    "method": "subscribe",
                    "subscription": sub_body,
                });
                if let Err(e) = write.send(Message::Text(sub.to_string())).await {
                    warn!("[Hyperliquid] subscribe {} {} failed: {}", ch, coin, e);
                    sub_ok = false;
                    break;
                }
            }
        }
        if !sub_ok {
            continue;
        }
        info!("[Hyperliquid] Connected, subscribed l2Book(fast)+trades+activeAssetCtx for {:?}", coins);

        let mut ping_interval = tokio::time::interval(PING_INTERVAL);
        ping_interval.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = ping_interval.tick() => {
                    // HL heartbeat is an application-level text ping.
                    let ping = serde_json::json!({ "method": "ping" }).to_string();
                    if let Err(e) = write.send(Message::Text(ping)).await {
                        warn!("[Hyperliquid] Ping send failed: {}", e);
                        break;
                    }
                }
                read_result = tokio::time::timeout(STALE_THRESHOLD, read.next()) => {
                    let msg = match read_result {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => { warn!("[Hyperliquid] WS read error: {}", e); break; }
                        Ok(None) => { warn!("[Hyperliquid] WS closed"); break; }
                        Err(_elapsed) => {
                            warn!("[Hyperliquid] No message for {:.0}s (stall watchdog) — reconnecting",
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
                            let channel = data.get("channel").and_then(|v| v.as_str()).unwrap_or("");
                            match channel {
                                "l2Book" => {
                                    let d = match data.get("data") { Some(d) => d, None => continue };
                                    let coin = d.get("coin").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                    let levels = d.get("levels").and_then(|v| v.as_array());
                                    let (bids, asks) = match levels {
                                        Some(l) if l.len() == 2 => (parse_levels(&l[0]), parse_levels(&l[1])),
                                        _ => continue,
                                    };
                                    if bids.is_empty() || asks.is_empty() { continue; }
                                    let ts = d.get("time").and_then(|v| v.as_u64())
                                        .map(|ms| ms * 1_000_000).unwrap_or_else(now_ns);
                                    let event = MarketEvent::OrderBook(OrderBookSnapshot {
                                        exchange: Exchange::Hyperliquid,
                                        symbol: coin,
                                        bids,
                                        asks,
                                        exchange_timestamp_ns: ts,
                                        local_timestamp_ns: now_ns(),
                                    });
                                    if event_tx.send(event).is_err() { return; }
                                }
                                "trades" => {
                                    let trades = match data.get("data").and_then(|v| v.as_array()) {
                                        Some(t) => t,
                                        None => continue,
                                    };
                                    for t in trades {
                                        let coin = t.get("coin").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                        let price: f64 = t.get("px").and_then(|v| v.as_str())
                                            .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                        let quantity: f64 = t.get("sz").and_then(|v| v.as_str())
                                            .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                        if price <= 0.0 { continue; }
                                        // HL side: "B" = buy aggressor, "A" = sell aggressor.
                                        let side = match t.get("side").and_then(|v| v.as_str()) {
                                            Some("B") => Side::Buy,
                                            _ => Side::Sell,
                                        };
                                        let ts = t.get("time").and_then(|v| v.as_u64())
                                            .map(|ms| ms * 1_000_000).unwrap_or_else(now_ns);
                                        let event = MarketEvent::Trade(TradeTick {
                                            exchange: Exchange::Hyperliquid,
                                            symbol: coin,
                                            price,
                                            quantity,
                                            side,
                                            exchange_timestamp_ns: ts,
                                            local_timestamp_ns: now_ns(),
                                        });
                                        if event_tx.send(event).is_err() { return; }
                                    }
                                }
                                "activeAssetCtx" => {
                                    let d = match data.get("data") { Some(d) => d, None => continue };
                                    let coin = d.get("coin").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                    let ctx = match d.get("ctx") { Some(c) => c, None => continue };
                                    let f = |key: &str| -> f64 {
                                        ctx.get(key).and_then(|v| v.as_str())
                                            .and_then(|s| s.parse().ok()).unwrap_or(0.0)
                                    };
                                    // impactPxs = [bid, ask]; midPx can be
                                    // absent on an empty book.
                                    let (impact_bid, impact_ask) = ctx.get("impactPxs")
                                        .and_then(|v| v.as_array())
                                        .map(|a| {
                                            let p = |i: usize| a.get(i).and_then(|v| v.as_str())
                                                .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                            (p(0), p(1))
                                        })
                                        .unwrap_or((0.0, 0.0));
                                    let event = MarketEvent::AssetCtx(AssetCtxTick {
                                        exchange: Exchange::Hyperliquid,
                                        symbol: coin,
                                        mark_px: f("markPx"),
                                        oracle_px: f("oraclePx"),
                                        mid_px: f("midPx"),
                                        funding: f("funding"),
                                        open_interest: f("openInterest"),
                                        premium: f("premium"),
                                        impact_bid_px: impact_bid,
                                        impact_ask_px: impact_ask,
                                        day_ntl_vlm: f("dayNtlVlm"),
                                        prev_day_px: f("prevDayPx"),
                                        local_timestamp_ns: now_ns(),
                                    });
                                    if event_tx.send(event).is_err() { return; }
                                }
                                // subscriptionResponse / pong / other — ignore.
                                _ => {}
                            }
                        }
                        Message::Ping(payload) => {
                            let _ = write.send(Message::Pong(payload)).await;
                        }
                        Message::Close(_) => {
                            warn!("[Hyperliquid] WebSocket closed");
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
    info!("[Hyperliquid] WS task exiting");
}

impl super::super::ExchangeMarket for HyperliquidMarket {
    fn connect(&mut self) -> Result<()> {
        let (event_tx, event_rx) = crossbeam_channel::unbounded::<MarketEvent>();
        self.event_rx = Some(event_rx);
        let shutdown = Arc::new(AtomicBool::new(false));
        self.ws_shutdown = shutdown.clone();
        let coins = self.coins.clone();
        let ws_url = if self.ws_url.is_empty() {
            // Default to mainnet host; the engine passes the resolved URL, so
            // this only bites if constructed without one.
            "wss://api.hyperliquid.xyz/ws".to_string()
        } else {
            self.ws_url.clone()
        };
        crate::async_rt::handle().spawn(hyperliquid_ws_task(coins, ws_url, event_tx, shutdown));
        Ok(())
    }

    fn subscribe(&mut self, symbols: &[String]) -> Result<()> {
        self.coins = symbols.to_vec();
        Ok(())
    }

    fn next_event(&mut self) -> Result<Option<MarketEvent>> {
        let rx = self.event_rx.as_ref().ok_or_else(|| anyhow!("Not connected"))?;
        match rx.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(crossbeam_channel::TryRecvError::Empty) => Ok(None),
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                Err(anyhow!("Hyperliquid WS task ended unexpectedly"))
            }
        }
    }

    fn disconnect(&mut self) {
        self.ws_shutdown.store(true, Ordering::Relaxed);
        self.event_rx = None;
        info!("[Hyperliquid] Disconnected");
    }

    fn name(&self) -> &str {
        "hyperliquid"
    }

    fn has_active_subscription(&self) -> bool {
        !self.coins.is_empty()
    }
}
