//! HexMarket User WebSocket feed — receives real-time order fill/cancel
//! notifications.
//!
//! Uses `tokio-tungstenite` on the shared async runtime. The existing
//! synchronous engine API is preserved: `spawn_user_feed` still returns a
//! `std::thread::JoinHandle<()>` — internally it launches a tokio task and
//! a short joiner thread that awaits it, mirroring the pattern used by
//! polymarket/user_feed.rs.

use anyhow::{anyhow, Result};
use crossbeam_channel::Sender;
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

use super::sdk::ApiCredentials;
use crate::types::*;

/// Ping interval — server's user feed drops idle sockets after ~60s.
const PING_INTERVAL: Duration = Duration::from_secs(25);
/// Per-task read-side stall watchdog — see binance/market.rs. User
/// feed is event-driven (fills, balance updates) and naturally has
/// long quiet periods, so 90 s is conservative.
const STALE_THRESHOLD: Duration = Duration::from_secs(90);
/// How long to wait between reconnects on transient failures.
const RECONNECT_MIN_MS: u64 = 200;
const RECONNECT_MAX_MS: u64 = 30_000;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UserAuthMessage<'a> {
    auth: UserAuthPayload<'a>,
    #[serde(rename = "type")]
    msg_type: &'static str,
    markets: &'a [String],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UserAuthPayload<'a> {
    api_key: &'a str,
    secret: &'a str,
    passphrase: &'a str,
}

/// Parse a HexUserWs event JSON into an OrderUpdate.
fn parse_user_event(data: &serde_json::Value) -> Option<OrderUpdate> {
    let event_type = data.get("event_type")?.as_str()?;

    match event_type {
        "order_fill" | "trade" => {
            let client_order_id = data.get("client_order_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if client_order_id.is_empty() {
                return None;
            }

            let outcome_id = data.get("outcome_id")
                .or_else(|| data.get("asset_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let side_str = data.get("side").and_then(|v| v.as_str()).unwrap_or("buy");
            let side = if side_str == "sell" { Side::Sell } else { Side::Buy };

            let filled_qty = data.get("filled_quantity")
                .or_else(|| data.get("quantity"))
                .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                .unwrap_or(0.0);
            let remaining_qty = data.get("remaining_quantity")
                .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                .unwrap_or(0.0);
            let avg_price = data.get("price")
                .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                .unwrap_or(0.0);

            let status = if remaining_qty <= 0.0 {
                OrderStatus::Filled
            } else {
                OrderStatus::PartiallyFilled
            };

            let exchange_order_id = data.get("order_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            let liquidity = data.get("liquidity")
                .and_then(|v| v.as_str())
                .and_then(|s| match s {
                    "maker" => Some(Liquidity::Maker),
                    "taker" => Some(Liquidity::Taker),
                    _ => None,
                });

            Some(OrderUpdate {
                client_order_id,
                exchange: Exchange::Hexmarket,
                symbol: outcome_id,
                side,
                exchange_order_id,
                status,
                liquidity,
                filled_quantity: filled_qty,
                remaining_quantity: remaining_qty,
                avg_fill_price: avg_price,
                timestamp_ns: now_ns(),
                trade_id: None,
                error: None,
            })
        }
        "order_cancelled" | "order_cancel" => {
            let client_order_id = data.get("client_order_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if client_order_id.is_empty() {
                return None;
            }

            let outcome_id = data.get("outcome_id")
                .or_else(|| data.get("asset_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let side_str = data.get("side").and_then(|v| v.as_str()).unwrap_or("buy");
            let side = if side_str == "sell" { Side::Sell } else { Side::Buy };

            let exchange_order_id = data.get("order_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            Some(OrderUpdate {
                client_order_id,
                exchange: Exchange::Hexmarket,
                symbol: outcome_id,
                side,
                exchange_order_id,
                status: OrderStatus::Cancelled,
                liquidity: None,
                filled_quantity: 0.0,
                remaining_quantity: 0.0,
                avg_fill_price: 0.0,
                timestamp_ns: now_ns(),
                trade_id: None,
                error: None,
            })
        }
        _ => None,
    }
}

/// Main async loop for the user WS. Reconnects with backoff on any error.
async fn user_ws_task(
    url: String,
    credentials: ApiCredentials,
    update_tx: Sender<OrderUpdate>,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = crate::exchange::ReconnectBackoff::new(RECONNECT_MIN_MS, RECONNECT_MAX_MS);

    loop {
        if shutdown.load(Ordering::Relaxed) { break; }

        info!("[HexUserFeed] Connecting to {}", url);
        let stream = match tokio_tungstenite::connect_async(&url).await {
            Ok((s, _)) => s,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[HexUserFeed] connect failed: {}, retry in {:.1}s", e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        backoff.reset();
        let (mut write, mut read) = stream.split();

        // Send auth + empty markets subscription immediately — the server
        // treats this message as both authentication and the initial
        // market list. Passing an empty Vec means "all markets for this
        // pubkey", which mirrors what the old SDK did (`vec![]`).
        let auth_msg = match serde_json::to_string(&UserAuthMessage {
            auth: UserAuthPayload {
                api_key: &credentials.api_key,
                secret: &credentials.secret,
                passphrase: &credentials.passphrase,
            },
            msg_type: "user",
            markets: &[],
        }) {
            Ok(s) => s,
            Err(e) => {
                warn!("[HexUserFeed] serialize auth failed: {}", e);
                break;
            }
        };
        if let Err(e) = write.send(Message::Text(auth_msg)).await {
            warn!("[HexUserFeed] send auth failed: {}", e);
            continue;
        }

        info!("[HexUserFeed] Connected and authenticated");

        let mut ping_interval = tokio::time::interval(PING_INTERVAL);
        ping_interval.tick().await;

        loop {
            if shutdown.load(Ordering::Relaxed) { return; }

            tokio::select! {
                biased;
                _ = ping_interval.tick() => {
                    if let Err(e) = write.send(Message::Text("PING".to_string())).await {
                        warn!("[HexUserFeed] ping send failed: {}", e);
                        break;
                    }
                }
                read_result = tokio::time::timeout(STALE_THRESHOLD, read.next()) => {
                    let msg = match read_result {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => { warn!("[HexUserFeed] read error: {}", e); break; }
                        Ok(None) => { warn!("[HexUserFeed] stream closed"); break; }
                        Err(_elapsed) => {
                            warn!("[HexUserFeed] No message for {:.0}s (stall watchdog) — reconnecting",
                                STALE_THRESHOLD.as_secs_f64());
                            break;
                        }
                    };
                    match msg {
                        Message::Text(text) => {
                            if text == "PONG" { continue; }
                            let t_parse = crate::latency::Instant::now();
                            // simd-json drop-in for SIMD parse speedup.
                            let mut buf = text.as_bytes().to_vec();
                            let data: serde_json::Value = match simd_json::serde::from_slice(&mut buf) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                            if let Some(update) = parse_user_event(&data) {
                                info!(
                                    "[HexUserFeed] {} coid={} {} {:?} filled={} remaining={} price={}",
                                    update.symbol, update.client_order_id,
                                    update.side, update.status,
                                    update.filled_quantity, update.remaining_quantity,
                                    update.avg_fill_price,
                                );
                                if update_tx.send(update).is_err() {
                                    return;
                                }
                            } else {
                                let json = serde_json::to_string(&data).unwrap_or_default();
                                if json != "\"PONG\"" {
                                    info!("[HexUserFeed] Unhandled: {}", &json[..json.len().min(500)]);
                                }
                            }
                            crate::latency::record("hexmarket.user.event_parse", t_parse);
                        }
                        Message::Ping(payload) => {
                            let _ = write.send(Message::Pong(payload)).await;
                        }
                        Message::Close(_) => {
                            warn!("[HexUserFeed] server closed WS");
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        if shutdown.load(Ordering::Relaxed) { break; }
        let delay = backoff.next_delay();
        tokio::time::sleep(delay).await;
    }

    info!("[HexUserFeed] Stopped");
}

/// Spawn a WS listener that forwards fill/cancel events as `OrderUpdate`s
/// through `update_tx`. Returns a `JoinHandle<()>` (sync thread wrapping
/// the tokio task) so the engine can join at shutdown like before.
pub fn spawn_user_feed(
    wss_url: &str,
    credentials: ApiCredentials,
    update_tx: Sender<OrderUpdate>,
    shutdown: Arc<AtomicBool>,
) -> Result<std::thread::JoinHandle<()>> {
    let url = format!("{}/user", wss_url.trim_end_matches('/'));

    // Spawn the async task on the shared runtime and grab its JoinHandle.
    let join_handle = crate::async_rt::handle().spawn(
        user_ws_task(url, credentials, update_tx, shutdown),
    );

    // Wrap in a sync thread that awaits the tokio task. Uses the shared
    // `block_on_runtime` helper (oneshot + blocking_recv) so this joiner
    // thread doesn't consume runtime scheduler cycles while it parks.
    let handle = std::thread::Builder::new()
        .name("hex-user-feed".into())
        .spawn(move || {
            crate::os_tune::pin_background("hex-user-feed");
            crate::async_rt::block_on_runtime(async move {
                let _ = join_handle.await;
            });
        })
        .map_err(|e| anyhow!("spawn hex-user-feed joiner: {}", e))?;

    Ok(handle)
}
