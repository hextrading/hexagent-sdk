//! Aster user feed — listenKey WS `ORDER_TRADE_UPDATE` → `OrderUpdate`.
//!
//! Same shape as the Hyperliquid user feed: [`spawn_user_feed`] returns a
//! `crossbeam_channel::Receiver<OrderUpdate>` plus a shutdown flag, and the
//! astermaker strategy drains the receiver each quote tick to keep its net
//! inventory in sync with maker fills (which arrive asynchronously, not on
//! the synchronous place path).
//!
//! Lifecycle per the V3 docs: `POST /fapi/v3/listenKey` (signed) creates a
//! key valid 60 min; `PUT` extends it; the stream lives at
//! `wss://…/ws/<listenKey>`; the server pushes `listenKeyExpired` when the
//! key lapses. We keepalive every 30 min and reconnect (with a fresh key)
//! on expiry, close, or stall.
//!
//! Event semantics (mirrors HL): entries with execution type `TRADE` are
//! **fills** — `trade_id` set, `filled_quantity` = the *incremental* fill
//! size (`o.l`), the strategy accumulates. Everything else is a status-only
//! update with `filled_quantity = 0` so it never double-counts.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use tokio_tungstenite::tungstenite::Message;

use crate::types::{now_ns, Exchange, Liquidity, OrderStatus, OrderUpdate, Side};

use super::auth::AsterAuth;
use super::signer::signed_query;

/// Server pings every 5 min; two missed pings + slack = dead connection.
const STALE_THRESHOLD: std::time::Duration = std::time::Duration::from_secs(630);
/// listenKey is valid 60 min; refresh at half-life.
const KEEPALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// Spawn the user-feed task. Returns the fill receiver and a shutdown flag
/// (set it `true` to stop the task).
pub fn spawn_user_feed(auth: &AsterAuth) -> (crossbeam_channel::Receiver<OrderUpdate>, Arc<AtomicBool>) {
    let (tx, rx) = crossbeam_channel::unbounded::<OrderUpdate>();
    let shutdown = Arc::new(AtomicBool::new(false));
    let sd = shutdown.clone();
    let auth = auth.clone();
    crate::async_rt::handle().spawn(user_feed_task(auth, tx, sd));
    (rx, shutdown)
}

/// Async signed request against `/fapi/v3/listenKey` (POST create / PUT
/// keepalive). Runs inside the feed task, so plain async — no block_on.
async fn listen_key_request(auth: &AsterAuth, method: &str) -> anyhow::Result<String> {
    let query = signed_query(auth, Vec::new())?;
    let url = format!("{}?{}", auth.fapi_url("listenKey"), query);
    // Query role, h1.1 pool — Aster's h2 frontend mishandles signed
    // requests (see info.rs); listenKey is not latency-critical.
    let client = crate::http1_pool::client(crate::http1_pool::Role::Query);
    let req = match method {
        "POST" => client.post(&url),
        "PUT" => client.put(&url),
        m => return Err(anyhow::anyhow!("listenKey: unsupported method {}", m)),
    };
    let resp = req
        .header("Content-Type", "application/x-www-form-urlencoded")
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("{} {}: {}", method, url, e))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow::anyhow!("{} listenKey -> {}: {}", method, status, text));
    }
    Ok(text)
}

async fn user_feed_task(
    auth: AsterAuth,
    tx: crossbeam_channel::Sender<OrderUpdate>,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = crate::exchange::ReconnectBackoff::new(200, 30_000);

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // 1. create (or refresh) the listenKey.
        let listen_key = match listen_key_request(&auth, "POST").await {
            Ok(text) => {
                match serde_json::from_str::<serde_json::Value>(&text)
                    .ok()
                    .and_then(|v| v.get("listenKey").and_then(|k| k.as_str()).map(String::from))
                {
                    Some(k) => k,
                    None => {
                        warn!("[Aster] user-feed: listenKey missing in response: {}", text);
                        tokio::time::sleep(backoff.next_delay()).await;
                        continue;
                    }
                }
            }
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[Aster] user-feed listenKey failed: {}, retry in {:.1}s", e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
                continue;
            }
        };

        // 2. connect the stream.
        let url = format!("{}/ws/{}", auth.ws_base(), listen_key);
        info!("[Aster] user-feed connecting for signer {}", auth.signer_address);
        let stream = match tokio_tungstenite::connect_async(&url).await {
            Ok((s, _)) => s,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[Aster] user-feed connect failed: {}, retry in {:.1}s", e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        backoff.reset();
        let (mut write, mut read) = stream.split();

        let mut keepalive = tokio::time::interval(KEEPALIVE_INTERVAL);
        keepalive.tick().await; // consume the immediate first tick

        loop {
            tokio::select! {
                biased;
                _ = keepalive.tick() => {
                    if let Err(e) = listen_key_request(&auth, "PUT").await {
                        warn!("[Aster] user-feed keepalive failed: {} — reconnecting", e);
                        break;
                    }
                }
                read_result = tokio::time::timeout(STALE_THRESHOLD, read.next()) => {
                    let msg = match read_result {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => { warn!("[Aster] user-feed read error: {}", e); break; }
                        Ok(None) => { warn!("[Aster] user-feed WS closed"); break; }
                        Err(_) => { warn!("[Aster] user-feed stall — reconnecting"); break; }
                    };
                    match msg {
                        Message::Text(text) => {
                            let mut buf = text.as_bytes().to_vec();
                            let data: serde_json::Value = match simd_json::serde::from_slice(&mut buf) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                            match data.get("e").and_then(|v| v.as_str()).unwrap_or("") {
                                "ORDER_TRADE_UPDATE" => {
                                    if let Some(u) = parse_order_trade_update(&data) {
                                        if tx.send(u).is_err() { return; }
                                    }
                                }
                                "listenKeyExpired" => {
                                    warn!("[Aster] user-feed listenKey expired — reconnecting");
                                    break;
                                }
                                // ACCOUNT_UPDATE / MARGIN_CALL / … — inventory
                                // is driven by fills; balance events ignored.
                                _ => {}
                            }
                        }
                        Message::Ping(p) => { let _ = write.send(Message::Pong(p)).await; }
                        Message::Close(_) => { warn!("[Aster] user-feed closed"); break; }
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
    info!("[Aster] user-feed task exiting");
}

/// Parse one `ORDER_TRADE_UPDATE` event.
///
/// Execution type (`o.x`) `TRADE` → a fill: `trade_id` = `o.t`,
/// `filled_quantity` = `o.l` (incremental last-fill size), price = `o.L`,
/// liquidity from `o.m` (is-maker). Other execution types (NEW / CANCELED /
/// EXPIRED / CALCULATED) → status-only, `filled_quantity = 0`.
fn parse_order_trade_update(data: &serde_json::Value) -> Option<OrderUpdate> {
    let o = data.get("o")?;
    let symbol = o.get("s")?.as_str()?.to_string();
    let cloid = o.get("c").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let side = match o.get("S").and_then(|v| v.as_str()) {
        Some("BUY") => Side::Buy,
        _ => Side::Sell,
    };
    let oid = o.get("i").and_then(|v| v.as_u64()).map(|n| n.to_string());
    let exec_type = o.get("x").and_then(|v| v.as_str()).unwrap_or("");
    let status_raw = o.get("X").and_then(|v| v.as_str()).unwrap_or("");
    let ts = data.get("E").and_then(|v| v.as_u64()).map(|ms| ms * 1_000_000).unwrap_or_else(now_ns);

    if exec_type == "TRADE" {
        let last_qty: f64 = o.get("l").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let last_px: f64 = o.get("L").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
        if last_qty <= 0.0 {
            return None;
        }
        let is_maker = o.get("m").and_then(|v| v.as_bool()).unwrap_or(false);
        let tid = o.get("t").and_then(|v| v.as_u64()).map(|t| t.to_string());
        let status = if status_raw == "FILLED" { OrderStatus::Filled } else { OrderStatus::PartiallyFilled };
        return Some(OrderUpdate {
            client_order_id: cloid,
            exchange: Exchange::Aster,
            symbol,
            side,
            exchange_order_id: oid,
            status,
            liquidity: Some(if is_maker { Liquidity::Maker } else { Liquidity::Taker }),
            filled_quantity: last_qty, // incremental; strategy accumulates
            remaining_quantity: 0.0,
            avg_fill_price: last_px,
            timestamp_ns: ts,
            trade_id: tid,
            error: None,
        });
    }

    // Status-only lifecycle push.
    let status = match status_raw {
        "NEW" => OrderStatus::Accepted,
        "CANCELED" | "EXPIRED" => OrderStatus::Cancelled,
        "FILLED" => OrderStatus::Filled,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        _ => return None, // NEW_INSURANCE / NEW_ADL / unknown — ignore
    };
    Some(OrderUpdate {
        client_order_id: cloid,
        exchange: Exchange::Aster,
        symbol,
        side,
        exchange_order_id: oid,
        status,
        liquidity: None,
        filled_quantity: 0.0, // status-only — inventory comes from TRADE events
        remaining_quantity: 0.0,
        avg_fill_price: 0.0,
        timestamp_ns: ts,
        trade_id: None,
        error: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_fill_event() {
        let ev: serde_json::Value = serde_json::from_str(r#"{
            "e":"ORDER_TRADE_UPDATE","E":1568879465651,"T":1568879465650,
            "o":{"s":"BTCUSDT","c":"550e8400-e29b-41d4-a716-446655440000",
                 "S":"SELL","o":"LIMIT","f":"GTX","q":"0.001","p":"50000",
                 "ap":"50000","x":"TRADE","X":"FILLED","i":8886774,
                 "l":"0.001","z":"0.001","L":"50000","n":"0.005","N":"USDT",
                 "T":1568879465651,"t":12345,"m":true,"R":false,"ps":"BOTH","rp":"0"}
        }"#).unwrap();
        let u = parse_order_trade_update(&ev).unwrap();
        assert_eq!(u.status, OrderStatus::Filled);
        assert_eq!(u.filled_quantity, 0.001);
        assert_eq!(u.avg_fill_price, 50000.0);
        assert_eq!(u.trade_id.as_deref(), Some("12345"));
        assert_eq!(u.liquidity, Some(Liquidity::Maker));
        assert_eq!(u.side, Side::Sell);
    }

    #[test]
    fn parse_status_event_no_inventory_effect() {
        let ev: serde_json::Value = serde_json::from_str(r#"{
            "e":"ORDER_TRADE_UPDATE","E":1568879465651,
            "o":{"s":"BTCUSDT","c":"abc","S":"BUY","x":"CANCELED","X":"CANCELED",
                 "i":1,"l":"0","z":"0","L":"0","t":0}
        }"#).unwrap();
        let u = parse_order_trade_update(&ev).unwrap();
        assert_eq!(u.status, OrderStatus::Cancelled);
        assert_eq!(u.filled_quantity, 0.0);
        assert!(u.trade_id.is_none());
    }
}
