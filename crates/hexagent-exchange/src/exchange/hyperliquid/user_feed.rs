//! Hyperliquid user feed — async WS `userFills` / `orderUpdates` → `OrderUpdate`.
//!
//! Keyed purely by the account address (no signing needed), so this is a
//! standalone spawnable task: [`spawn_user_feed`] returns a
//! `crossbeam_channel::Receiver<OrderUpdate>` plus a shutdown flag. The
//! hypermaker strategy drains the receiver each quote tick to keep its net
//! inventory in sync with maker fills (which arrive asynchronously, not on the
//! synchronous place path). This avoids adding a venue-specific user-feed
//! subsystem to the engine.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use tokio_tungstenite::tungstenite::Message;

use crate::types::{now_ns, Exchange, Liquidity, OrderStatus, OrderUpdate, Side};

const PING_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
const STALE_THRESHOLD: std::time::Duration = std::time::Duration::from_secs(60);

/// Spawn the user-feed task. Returns the fill receiver and a shutdown flag
/// (set it `true` to stop the task).
pub fn spawn_user_feed(
    ws_url: &str,
    account_address: &str,
) -> (crossbeam_channel::Receiver<OrderUpdate>, Arc<AtomicBool>) {
    let (tx, rx) = crossbeam_channel::unbounded::<OrderUpdate>();
    let shutdown = Arc::new(AtomicBool::new(false));
    let ws_url = if ws_url.is_empty() {
        "wss://api.hyperliquid.xyz/ws".to_string()
    } else {
        ws_url.to_string()
    };
    let account = account_address.to_string();
    let sd = shutdown.clone();
    crate::async_rt::handle().spawn(user_feed_task(ws_url, account, tx, sd));
    (rx, shutdown)
}

async fn user_feed_task(
    ws_url: String,
    account: String,
    tx: crossbeam_channel::Sender<OrderUpdate>,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = crate::exchange::ReconnectBackoff::new(200, 30_000);

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        info!("[Hyperliquid] user-feed connecting to {} for {}", ws_url, account);
        let stream = match tokio_tungstenite::connect_async(&ws_url).await {
            Ok((s, _)) => s,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[Hyperliquid] user-feed connect failed: {}, retry in {:.1}s", e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        backoff.reset();
        let (mut write, mut read) = stream.split();

        // Subscribe fills (userFills carries the definitive fill records).
        let sub = serde_json::json!({
            "method": "subscribe",
            "subscription": { "type": "userFills", "user": account },
        });
        if let Err(e) = write.send(Message::Text(sub.to_string())).await {
            warn!("[Hyperliquid] user-feed subscribe failed: {}", e);
            continue;
        }

        let mut ping_interval = tokio::time::interval(PING_INTERVAL);
        ping_interval.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = ping_interval.tick() => {
                    let ping = serde_json::json!({ "method": "ping" }).to_string();
                    if write.send(Message::Text(ping)).await.is_err() { break; }
                }
                read_result = tokio::time::timeout(STALE_THRESHOLD, read.next()) => {
                    let msg = match read_result {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => { warn!("[Hyperliquid] user-feed read error: {}", e); break; }
                        Ok(None) => { warn!("[Hyperliquid] user-feed WS closed"); break; }
                        Err(_) => { warn!("[Hyperliquid] user-feed stall — reconnecting"); break; }
                    };
                    match msg {
                        Message::Text(text) => {
                            let mut buf = text.as_bytes().to_vec();
                            let data: serde_json::Value = match simd_json::serde::from_slice(&mut buf) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                            if data.get("channel").and_then(|v| v.as_str()) != Some("userFills") {
                                continue;
                            }
                            // data.data = { isSnapshot, user, fills: [...] }
                            let d = match data.get("data") { Some(d) => d, None => continue };
                            let is_snapshot = d.get("isSnapshot").and_then(|v| v.as_bool()).unwrap_or(false);
                            // Skip the initial snapshot: those are historical fills
                            // already reflected in the position poll; only stream
                            // live incremental fills into inventory.
                            if is_snapshot { continue; }
                            let fills = match d.get("fills").and_then(|v| v.as_array()) {
                                Some(f) => f,
                                None => continue,
                            };
                            for f in fills {
                                if let Some(u) = parse_fill(f) {
                                    if tx.send(u).is_err() { return; }
                                }
                            }
                        }
                        Message::Ping(p) => { let _ = write.send(Message::Pong(p)).await; }
                        Message::Close(_) => { warn!("[Hyperliquid] user-feed closed"); break; }
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
    info!("[Hyperliquid] user-feed task exiting");
}

/// Parse one `userFills` fill record into an `OrderUpdate`.
fn parse_fill(f: &serde_json::Value) -> Option<OrderUpdate> {
    let coin = f.get("coin")?.as_str()?.to_string();
    let px: f64 = f.get("px")?.as_str()?.parse().ok()?;
    let sz: f64 = f.get("sz")?.as_str()?.parse().ok()?;
    // HL fill side: "B" = buy, "A" = sell.
    let side = match f.get("side").and_then(|v| v.as_str()) {
        Some("B") => Side::Buy,
        _ => Side::Sell,
    };
    let crossed = f.get("crossed").and_then(|v| v.as_bool()).unwrap_or(false);
    let liquidity = if crossed { Liquidity::Taker } else { Liquidity::Maker };
    let oid = f.get("oid").and_then(|v| v.as_u64()).map(|o| o.to_string());
    let tid = f.get("tid").and_then(|v| v.as_u64()).map(|t| t.to_string());
    let cloid = f.get("cloid").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let ts = f.get("time").and_then(|v| v.as_u64()).map(|ms| ms * 1_000_000).unwrap_or_else(now_ns);

    Some(OrderUpdate {
        client_order_id: cloid,
        exchange: Exchange::Hyperliquid,
        symbol: coin,
        side,
        exchange_order_id: oid,
        status: OrderStatus::Filled, // one discrete fill; strategy accumulates
        liquidity: Some(liquidity),
        filled_quantity: sz,
        remaining_quantity: 0.0,
        avg_fill_price: px,
        timestamp_ns: ts,
        trade_id: tid,
        error: None,
    })
}
