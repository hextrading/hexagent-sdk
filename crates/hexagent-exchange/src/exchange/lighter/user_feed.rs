//! Lighter user feed — async WS `account_all_trades` / `account_all_orders`
//! → `OrderUpdate`.
//!
//! Same shape as the Hyperliquid user feed: [`spawn_user_feed`] returns a
//! `crossbeam_channel::Receiver<OrderUpdate>` plus a shutdown flag; the
//! litmaker strategy drains it each quote tick to keep net inventory in sync
//! with maker fills. Fills (from the trades channel, `trade_id` set) drive
//! inventory; order-status pushes (orders channel, `trade_id` None,
//! `filled_quantity` 0) are informational.
//!
//! Both channels need an auth token; tokens are minted per connection with a
//! ~1 h deadline by the shared [`super::signer::LighterSigner`], so a
//! reconnect always carries a fresh token.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use tokio_tungstenite::tungstenite::Message;

use crate::types::{now_ns, Exchange, Liquidity, OrderStatus, OrderUpdate, Side};

use super::signer::LighterSigner;

const PING_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
const STALE_THRESHOLD: std::time::Duration = std::time::Duration::from_secs(120);
/// Auth-token validity per connection (server max is 8 h; reconnects mint
/// fresh tokens well before expiry via the stale watchdog).
const AUTH_TOKEN_TTL_SECS: i64 = 60 * 60;

/// Spawn the user-feed task. `market_symbols` maps market_id → symbol so
/// updates carry the strategy-facing symbol ("BTC").
pub fn spawn_user_feed(
    ws_url: &str,
    signer: Arc<LighterSigner>,
    market_symbols: std::collections::HashMap<i16, String>,
) -> (crossbeam_channel::Receiver<OrderUpdate>, Arc<AtomicBool>) {
    let (tx, rx) = crossbeam_channel::unbounded::<OrderUpdate>();
    let shutdown = Arc::new(AtomicBool::new(false));
    let ws_url = if ws_url.is_empty() {
        "wss://mainnet.zklighter.elliot.ai/stream".to_string()
    } else {
        ws_url.to_string()
    };
    let sd = shutdown.clone();
    crate::async_rt::handle().spawn(user_feed_task(ws_url, signer, market_symbols, tx, sd));
    (rx, shutdown)
}

async fn user_feed_task(
    ws_url: String,
    signer: Arc<LighterSigner>,
    market_symbols: std::collections::HashMap<i16, String>,
    tx: crossbeam_channel::Sender<OrderUpdate>,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = crate::exchange::ReconnectBackoff::new(200, 30_000);
    let account_index = signer.account_index;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        info!("[Lighter] user-feed connecting to {} for account {}", ws_url, account_index);
        let stream = match tokio_tungstenite::connect_async(&ws_url).await {
            Ok((s, _)) => s,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[Lighter] user-feed connect failed: {}, retry in {:.1}s", e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        backoff.reset();
        let (mut write, mut read) = stream.split();

        // Fresh auth token per connection.
        let deadline = (now_ns() / 1_000_000_000) as i64 + AUTH_TOKEN_TTL_SECS;
        let auth_token = signer.create_auth_token(deadline);

        let mut sub_ok = true;
        for ch in ["account_all_orders", "account_all_trades"] {
            let sub = serde_json::json!({
                "type": "subscribe",
                "channel": format!("{}/{}", ch, account_index),
                "auth": auth_token,
            });
            if let Err(e) = write.send(Message::Text(sub.to_string())).await {
                warn!("[Lighter] user-feed subscribe {} failed: {}", ch, e);
                sub_ok = false;
                break;
            }
        }
        if !sub_ok {
            continue;
        }

        let mut ping_interval = tokio::time::interval(PING_INTERVAL);
        ping_interval.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = ping_interval.tick() => {
                    let ping = serde_json::json!({ "type": "ping" }).to_string();
                    if write.send(Message::Text(ping)).await.is_err() { break; }
                }
                read_result = tokio::time::timeout(STALE_THRESHOLD, read.next()) => {
                    let msg = match read_result {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => { warn!("[Lighter] user-feed read error: {}", e); break; }
                        Ok(None) => { warn!("[Lighter] user-feed WS closed"); break; }
                        Err(_) => { warn!("[Lighter] user-feed stall — reconnecting"); break; }
                    };
                    match msg {
                        Message::Text(text) => {
                            let mut buf = text.as_bytes().to_vec();
                            let data: serde_json::Value = match simd_json::serde::from_slice(&mut buf) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                            let msg_type = data.get("type").and_then(|v| v.as_str()).unwrap_or("");
                            match msg_type {
                                "ping" => {
                                    let pong = serde_json::json!({ "type": "pong" }).to_string();
                                    let _ = write.send(Message::Text(pong)).await;
                                }
                                // Live fills. The subscribed/* snapshot is
                                // history (already in the position poll) —
                                // only stream incremental updates.
                                "update/account_all_trades" => {
                                    for u in parse_trades(&data, account_index, &market_symbols) {
                                        if tx.send(u).is_err() { return; }
                                    }
                                }
                                "update/account_all_orders" => {
                                    for u in parse_orders(&data, &market_symbols) {
                                        if tx.send(u).is_err() { return; }
                                    }
                                }
                                _ => {}
                            }
                        }
                        Message::Ping(p) => { let _ = write.send(Message::Pong(p)).await; }
                        Message::Close(frame) => {
                            warn!("[Lighter] user-feed closed: {:?}", frame);
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
    info!("[Lighter] user-feed task exiting");
}

/// `trades` arrives keyed by market id: `{"trades": {"1": [Trade, ...]}}`.
fn parse_trades(
    data: &serde_json::Value,
    account_index: i64,
    market_symbols: &std::collections::HashMap<i16, String>,
) -> Vec<OrderUpdate> {
    let mut out = Vec::new();
    let Some(by_market) = data.get("trades").and_then(|v| v.as_object()) else {
        return out;
    };
    for (mid_str, arr) in by_market {
        let Ok(mid) = mid_str.parse::<i16>() else { continue };
        let symbol = match market_symbols.get(&mid) {
            Some(s) => s.clone(),
            None => continue, // not a market we trade
        };
        let Some(trades) = arr.as_array() else { continue };
        for t in trades {
            let price: f64 = t.get("price").and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let size: f64 = t.get("size").and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()).unwrap_or(0.0);
            if price <= 0.0 || size <= 0.0 {
                continue;
            }
            let ask_acct = t.get("ask_account_id").and_then(|v| v.as_i64()).unwrap_or(-1);
            let bid_acct = t.get("bid_account_id").and_then(|v| v.as_i64()).unwrap_or(-1);
            // Our side of the trade. (Self-trades can't happen — STP.)
            let side = if bid_acct == account_index {
                Side::Buy
            } else if ask_acct == account_index {
                Side::Sell
            } else {
                continue; // not ours (shouldn't happen on an account channel)
            };
            let is_maker_ask = t.get("is_maker_ask").and_then(|v| v.as_bool()).unwrap_or(false);
            let we_are_maker = (is_maker_ask && side == Side::Sell)
                || (!is_maker_ask && side == Side::Buy);
            let coid = match side {
                Side::Buy => t.get("bid_client_id"),
                Side::Sell => t.get("ask_client_id"),
            }
            .and_then(|v| v.as_i64())
            .map(|v| v.to_string())
            .unwrap_or_default();
            let tid = t
                .get("trade_id")
                .and_then(|v| v.as_i64())
                .map(|v| v.to_string());
            let ts = t.get("timestamp").and_then(|v| v.as_u64())
                .map(|ms| ms * 1_000_000).unwrap_or_else(now_ns);
            out.push(OrderUpdate {
                client_order_id: coid,
                exchange: Exchange::Lighter,
                symbol: symbol.clone(),
                side,
                exchange_order_id: None,
                status: OrderStatus::Filled, // one discrete fill; strategy accumulates
                liquidity: Some(if we_are_maker { Liquidity::Maker } else { Liquidity::Taker }),
                filled_quantity: size,
                remaining_quantity: 0.0,
                avg_fill_price: price,
                timestamp_ns: ts,
                trade_id: tid,
                error: None,
            });
        }
    }
    out
}

/// `orders` arrives keyed by market id: `{"orders": {"1": [Order, ...]}}`.
/// Status-only pushes (`filled_quantity = 0`) — fills come from the trades
/// channel.
fn parse_orders(
    data: &serde_json::Value,
    market_symbols: &std::collections::HashMap<i16, String>,
) -> Vec<OrderUpdate> {
    let mut out = Vec::new();
    let Some(by_market) = data.get("orders").and_then(|v| v.as_object()) else {
        return out;
    };
    for (mid_str, arr) in by_market {
        let Ok(mid) = mid_str.parse::<i16>() else { continue };
        let symbol = match market_symbols.get(&mid) {
            Some(s) => s.clone(),
            None => continue,
        };
        let Some(orders) = arr.as_array() else { continue };
        for o in orders {
            let status_raw = o.get("status").and_then(|v| v.as_str()).unwrap_or("");
            let status = match status_raw {
                "open" | "pending" | "in-progress" => OrderStatus::Accepted,
                "filled" => OrderStatus::Filled,
                s if s.contains("cancel") => OrderStatus::Cancelled,
                s if s.contains("expir") => OrderStatus::Cancelled,
                _ => continue, // unknown lifecycle state — ignore
            };
            let is_ask = o.get("is_ask").and_then(|v| v.as_bool())
                .or_else(|| o.get("is_ask").and_then(|v| v.as_i64()).map(|n| n != 0))
                .unwrap_or(false);
            let coid = o.get("client_order_id")
                .map(|v| match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .unwrap_or_default();
            let oid = o.get("order_index").and_then(|v| v.as_i64()).map(|v| v.to_string());
            let ts = o.get("timestamp").and_then(|v| v.as_u64())
                .map(|ms| ms * 1_000_000).unwrap_or_else(now_ns);
            out.push(OrderUpdate {
                client_order_id: coid,
                exchange: Exchange::Lighter,
                symbol: symbol.clone(),
                side: if is_ask { Side::Sell } else { Side::Buy },
                exchange_order_id: oid,
                status,
                liquidity: None,
                filled_quantity: 0.0, // status-only — inventory comes from trades
                remaining_quantity: 0.0,
                avg_fill_price: 0.0,
                timestamp_ns: ts,
                trade_id: None,
                error: None,
            });
        }
    }
    out
}
