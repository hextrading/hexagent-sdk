//! Polymarket User WebSocket feed — receives real-time order/trade notifications.
//!
//! Async implementation (tokio + tokio-tungstenite). The public API returns
//! a `std::thread::JoinHandle` so the engine shutdown path is unchanged,
//! but under the hood the WS read loop runs as a tokio task on the shared
//! async runtime.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossbeam_channel::Sender;
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;

use crate::async_rt;
use crate::types::*;
use super::live_position::TradeStatus;
use super::trade::SharedState;

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/user";
const CLOB_BASE_URL: &str = "https://clob.polymarket.com";
const PING_INTERVAL: Duration = Duration::from_secs(10);
const READ_TIMEOUT: Duration = Duration::from_secs(2);
const STALE_TIMEOUT: Duration = Duration::from_secs(30);

/// Parse a Polymarket user WebSocket event into zero-or-more OrderUpdates.
/// A single "trade" push from a MAKER perspective may expand into multiple
/// OrderUpdates (one per matching `maker_orders[]` entry owned by us).
fn parse_user_event(data: &serde_json::Value, shared: &SharedState) -> Vec<OrderUpdate> {
    // Determine event type from the payload structure
    let event_type = match data.get("event_type")
        .or_else(|| data.get("type"))
        .and_then(|v| v.as_str()) {
        Some(s) => s,
        None => return Vec::new(),
    };

    // Top-level ID resolution for the TAKER branch.
    //
    // Two historical pitfalls, both addressed here:
    //
    //   (1) `id` is the TRADE UUID (e.g. "390303b7-a..."), NOT the
    //       order hash. Earlier code fell back to it and caused every
    //       TAKER fill to register as `<unmapped>` because lookup_coid
    //       keys by the 0x-prefixed EIP-712 digest we register at
    //       submit time, not by trade UUID. So we do NOT include `id`
    //       in this fallback chain.
    //
    //   (2) For TAKER fills on the `user` WebSocket channel, the
    //       submitted order's hash lives under `taker_order_id` —
    //       confirmed against:
    //         * Polymarket official docs sample payload at
    //           https://docs.polymarket.com/market-data/websocket/user-channel
    //           (verbatim: `"taker_order_id": "0x06bc63e346..."`)
    //         * Nautilus-trader's `PolymarketUserTrade` msgspec struct
    //           which declares `taker_order_id: str` as required and
    //           returns `[self.taker_order_id]` from
    //           `get_filled_user_order_ids()` when trader_side==TAKER
    //         * py-clob-client / wallet.rs's REST `/trades` parser
    //           (shared schema between the WS and REST endpoints)
    //       The top-level `order_id` / `orderID` keys exist on some
    //       legacy/order-lifecycle payloads but are NOT present on
    //       trade events for TAKER fills — kept as fallbacks so any
    //       future schema variant (or non-trade event type taking
    //       this code path) still gets a chance to map.
    let order_id = data.get("order_id")
        .or_else(|| data.get("orderID"))
        .or_else(|| data.get("taker_order_id"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    match event_type {
        "order" => {
            // Order lifecycle event (placement, cancel) — we already track
            // these locally via the submit/cancel path. Keep as a silent
            // ack (no OrderUpdate) to avoid double-counting.
            if !order_id.is_empty() {
                if let Some(_coid) = shared.lookup_coid(order_id) {
                    log::debug!("[PolyUserFeed] order event ack: orderID={}", order_id);
                }
            }
            Vec::new()
        }
        "trade" => {
            let asset_id = data.get("asset_id")
                .or_else(|| data.get("token_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("").to_string();

            // trader_side / role determines whether we look up by
            // top-level fields (TAKER) or walk `maker_orders[]` (MAKER).
            // Legacy payloads used `type: "trade"` with top-level order_id
            // pointing to a specific side; the modern server sends both
            // `trader_side` and a populated `maker_orders` array — we
            // handle both via the `is_maker` check below.
            //
            // IMPORTANT: Polymarket emits one `trade` push per status
            // transition (MATCHED → MINED → CONFIRMED); each carries the
            // full trade object. We re-emit on every transition so the
            // strategy's ledger can lift records from Matched → Confirmed.
            //
            // Fee fields come from the server under `fee_bps` / `fee_rate_bps`;
            // we ignore them here because the strategy computes fee locally
            // (the server may not populate these consistently).

            let side = data.get("side").and_then(|v| v.as_str()).unwrap_or("BUY");
            let side = if side.eq_ignore_ascii_case("SELL") { Side::Sell } else { Side::Buy };

            // trade id (from top-level `id` / `trade_id`) + maker_order_id
            // (from `maker_orders[]`) form the ledger key. For TAKER we
            // use trade_id alone; for MAKER we build `{trade_id}:{maker_order_id}`
            // so each of our maker legs on this trade gets a distinct ledger row.
            let trade_id = data.get("id")
                .or_else(|| data.get("trade_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("");

            let trader_side = data.get("trader_side")
                .or_else(|| data.get("role"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            // maker/taker is decided by whether `maker_orders[]` carries OUR
            // funder leg — NOT the server's `trader_side` field. Verified
            // 100% consistent across 968 live trades, but the address-based
            // rule is the robust source of truth: if `trader_side` were ever
            // wrong/empty for a maker fill, the old check routed it to the
            // taker branch and silently dropped it. Mirrors the reconciler
            // (fetch_server_trades) classification.
            let is_maker = data.get("maker_orders")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().any(|mo| mo.get("maker_address")
                    .and_then(|v| v.as_str())
                    .map_or(false, |a| a.eq_ignore_ascii_case(&shared.order_maker_address))))
                .unwrap_or(false);
            let liquidity = match trader_side {
                "MAKER" | "maker" => Some(Liquidity::Maker),
                "TAKER" | "taker" => Some(Liquidity::Taker),
                _ => None,
            };

            let status_str = data.get("status").and_then(|v| v.as_str()).unwrap_or("MATCHED");

            let match_time_secs: u64 = data.get("match_time")
                .and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_u64()))
                .unwrap_or(0);
            if match_time_secs > 0 {
                shared.live_position.lock().unwrap().touch_match_time(match_time_secs);
            }

            let status = match status_str {
                "MATCHED" | "MINED" => OrderStatus::PartiallyFilled,
                "CONFIRMED" => OrderStatus::Filled,
                // FAILED = on-chain settlement reverted; downstream must
                // reverse the fill out of position/cashflow/volume/fees.
                // RETRYING is transient (chain settlement still pending) —
                // keep dropping so we don't churn the ledger before the
                // resolved CONFIRMED / FAILED arrives.
                "FAILED" => OrderStatus::Failed,
                "RETRYING" => return Vec::new(),
                _ => OrderStatus::PartiallyFilled,
            };

            let parse_f = |v: Option<&serde_json::Value>| -> f64 {
                match v {
                    Some(serde_json::Value::String(s)) => s.parse().unwrap_or(0.0),
                    Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
                    _ => 0.0,
                }
            };

            // Extract the FAILED / status-transition reason from
            // whichever field Polymarket happens to surface it under.
            // The server has used several names across versions; check
            // them in priority order, return the first non-empty.
            // When status is FAILED but no known field is populated,
            // log the raw data payload at warn so the operator can
            // identify the actual field name post-hoc.
            let extract_reason = |d: &serde_json::Value| -> Option<String> {
                for k in &[
                    "error",
                    "reason",
                    "failure_reason",
                    "revert_reason",
                    "last_status_reason",
                    "last_update_reason",
                    "status_reason",
                    "error_message",
                    "errorMsg",
                ] {
                    if let Some(s) = d.get(*k).and_then(|v| v.as_str()) {
                        if !s.is_empty() { return Some(s.to_string()); }
                    }
                }
                None
            };
            let failure_reason: Option<String> = if status == OrderStatus::Failed {
                let r = extract_reason(data);
                if r.is_none() {
                    // Surface the full payload so we learn the real
                    // field name. Limited to FAILED so we don't spam
                    // on the happy path.
                    warn!("[PolyUserFeed] FAILED trade {} carries no known \
                          reason field; raw payload: {}",
                          trade_id, data);
                }
                r
            } else {
                extract_reason(data)
            };
            let reason_ref: Option<&str> = failure_reason.as_deref();

            let mut updates: Vec<OrderUpdate> = Vec::new();

            if is_maker {
                let funder = &shared.order_maker_address;
                let Some(arr) = data.get("maker_orders").and_then(|v| v.as_array()) else {
                    return Vec::new();
                };

                for mo in arr {
                    let mo_addr = mo.get("maker_address").and_then(|v| v.as_str()).unwrap_or("");
                    if !mo_addr.eq_ignore_ascii_case(funder) { continue; }

                    let mo_asset_id = mo.get("asset_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let mo_side_str = mo.get("side").and_then(|v| v.as_str()).unwrap_or("BUY");
                    let mo_side = if mo_side_str.eq_ignore_ascii_case("SELL") { Side::Sell } else { Side::Buy };
                    let mo_size: f64 = parse_f(mo.get("matched_amount"));
                    let mo_price: f64 = parse_f(mo.get("price"));
                    let mo_order_id = mo.get("order_id").and_then(|v| v.as_str()).unwrap_or("");

                    let leg_id = if mo_order_id.is_empty() {
                        trade_id.to_string()
                    } else {
                        format!("{}:{}", trade_id, mo_order_id)
                    };

                    if let Some(trade_status) = TradeStatus::from_str(status_str) {
                        if !leg_id.is_empty() && mo_size > 0.0 {
                            shared.live_position.lock().unwrap().update_trade(
                                &leg_id, trade_status, &mo_asset_id, mo_side,
                                mo_size, mo_price, true, reason_ref,
                            );
                        }
                    }

                    let coid = shared.lookup_coid(mo_order_id).unwrap_or_default();

                    updates.push(OrderUpdate {
                        client_order_id: coid,
                        exchange: Exchange::Polymarket,
                        symbol: mo_asset_id,
                        side: mo_side,
                        exchange_order_id: if mo_order_id.is_empty() { None } else { Some(mo_order_id.to_string()) },
                        status,
                        liquidity,
                        filled_quantity: mo_size,
                        remaining_quantity: 0.0,
                        avg_fill_price: mo_price,
                        timestamp_ns: now_ns(),
                        trade_id: if leg_id.is_empty() { None } else { Some(leg_id) },
                        error: failure_reason.clone(),
                    });
                }
            } else {
                let matched_amount: f64 = parse_f(data.get("size").or_else(|| data.get("matched_amount")));
                let price: f64 = parse_f(data.get("price"));

                if let Some(trade_status) = TradeStatus::from_str(status_str) {
                    if !trade_id.is_empty() && matched_amount > 0.0 {
                        shared.live_position.lock().unwrap().update_trade(
                            trade_id, trade_status, &asset_id, side,
                            matched_amount, price, false, reason_ref,
                        );
                    }
                }

                // Vacate the taker-matched accelerator buffer for this fill:
                // the authoritative WS push has arrived (this `OrderUpdate` is
                // pushed below and booked into PositionManager by the
                // strategy), so the HTTP-sourced placeholder must stop
                // contributing. Runs BEFORE the OrderUpdate is delivered, so
                // the strategy never double-counts.
                shared.taker_matched.on_ws_trade(trade_id);

                let coid = shared.lookup_coid(order_id).unwrap_or_default();

                updates.push(OrderUpdate {
                    client_order_id: coid,
                    exchange: Exchange::Polymarket,
                    symbol: asset_id,
                    side,
                    exchange_order_id: if order_id.is_empty() { None } else { Some(order_id.to_string()) },
                    status,
                    liquidity,
                    filled_quantity: matched_amount,
                    remaining_quantity: 0.0,
                    avg_fill_price: price,
                    timestamp_ns: now_ns(),
                    trade_id: if trade_id.is_empty() { None } else { Some(trade_id.to_string()) },
                    error: failure_reason.clone(),
                });
            }

            updates
        }
        _ => Vec::new(),
    }
}

/// Fetch trades newer than `after_secs` from the authenticated CLOB `/trades`
/// endpoint and replay them through the update channel. Async version —
/// uses the shared reqwest HTTP/2 client.
async fn replay_missed_trades(
    shared: &SharedState,
    update_tx: &Sender<OrderUpdate>,
    market: &str,
    after_secs: u64,
) -> usize {
    // Scope the catch-up to the event currently being traded
    // (`?market=<condition_id>`). L2 auth already restricts `/trades` to this
    // account, so dropping the `maker_address` filter just widens coverage to
    // BOTH our maker and taker legs in this market. Empty market = no active
    // event yet → nothing to replay.
    if market.is_empty() { return 0; }
    let mut cursor = String::new();
    let mut count = 0usize;
    let client = async_rt::http_client();

    // Roll back 1 s on the boundary so trades sharing the same second as
    // `last_match_time` aren't excluded by Polymarket's strict-`>`
    // semantics on `?after=T`. The overlap is harmless — `trade_id`
    // dedup in `PositionManager::upsert_trade` short-circuits any trade
    // already in the ledger (terminal-state guard, position.rs:171).
    let after_param = after_secs.saturating_sub(1);

    // Up to 50 pages × ~50 trades/page ≈ 2,500 trades of catch-up.
    // Covers ~30-minute disconnects at the busiest observed fill rate
    // (≈80 fills/min). Bumped from 20 (~1,000 cap) so a longer outage
    // — e.g. WS down through a fee-rate change or a region failover —
    // doesn't silently truncate the replay.
    const MAX_PAGES: usize = 50;
    let mut truncated = false;
    for page in 0..MAX_PAGES {
        let headers = shared.auth.sign_request("GET", "/trades", "");
        let url = if cursor.is_empty() {
            format!("{}/trades?market={}&after={}", CLOB_BASE_URL, market, after_param)
        } else {
            format!("{}/trades?market={}&after={}&next_cursor={}",
                CLOB_BASE_URL, market, after_param, cursor)
        };
        let mut req = client.get(&url);
        for (k, v) in headers.as_pairs() {
            req = req.header(k, v);
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                warn!("[PolyUserFeed] Gap-fetch /trades error: {}", e);
                break;
            }
        };
        if !resp.status().is_success() {
            let code = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            warn!("[PolyUserFeed] Gap-fetch /trades {}: {}", code, body);
            break;
        }
        let json: serde_json::Value = match resp.json().await {
            Ok(j) => j,
            Err(e) => { warn!("[PolyUserFeed] Gap-fetch parse: {}", e); break; }
        };

        let (records, next) = if let Some(arr) = json.as_array() {
            (arr.clone(), String::new())
        } else {
            let data = json.get("data").and_then(|v| v.as_array()).cloned().unwrap_or_default();
            let next = json.get("next_cursor").and_then(|v| v.as_str()).unwrap_or("").to_string();
            (data, next)
        };

        for mut rec in records {
            if let Some(obj) = rec.as_object_mut() {
                obj.entry("event_type".to_string())
                    .or_insert(serde_json::Value::String("trade".to_string()));
            }
            for update in parse_user_event(&rec, shared) {
                let _ = update_tx.send(update);
            }
            count += 1;
        }

        if next.is_empty() || next == "LTE=" { break; }
        cursor = next;
        // Hit the page cap with a cursor still pending → there are more
        // missed trades than we can replay. We may have PERMANENTLY missed
        // fills, so the current event's inventory is unknowable.
        if page == MAX_PAGES - 1 { truncated = true; }
    }

    if truncated {
        warn!(
            "[PolyUserFeed] Gap replay TRUNCATED at {} pages (~{} trades) with more pending — \
             inventory may be incomplete; flagging current event inventory-uncertain (will stop \
             quoting/trading it and ride to settlement)",
            MAX_PAGES, count,
        );
        shared.user_feed_health.set_inventory_uncertain(true);
    }

    count
}

/// Async WebSocket loop. Spawned as a tokio task on the shared runtime.
async fn user_feed_loop(
    api_key: String,
    api_secret: String,
    passphrase: String,
    shared: Arc<SharedState>,
    update_tx: Sender<OrderUpdate>,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = crate::exchange::ReconnectBackoff::new(100, 30_000);
    // First connect is also treated as "recovering" so the strategy stays
    // paused until the first batch of state (and gap replay) is in.
    shared.user_feed_health.set_recovering(true);
    {
        let mut lp = shared.live_position.lock().unwrap();
        if lp.last_match_time_secs() == 0 {
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs()).unwrap_or(0);
            lp.touch_match_time(now_secs);
        }
    }

    // Periodic gap-replay task — independent of the WS read loop so its HTTP
    // call never pauses WS reads, and it keeps recovering fills *across*
    // reconnects (including while the main loop is reconnecting). Cadence and
    // rewind window are config-driven (`gap_replay.interval_ms` /
    // `periodic_rewind_ms`; defaults 2s / 5s). The status dedup in
    // upsert_trade / update_trade makes already-seen fills no-ops, so only
    // genuinely-dropped ones reach the ledger. A rewind larger than the
    // cadence means a fill is covered by ≥2 sweeps even with match_time
    // second-quantization jitter.
    //
    // When the active event changes (new condition_id, incl. the first seed
    // after startup), the very next sweep does a one-shot now−300s DEEP
    // catch-up of that market so a mid-event (re)start recovers all of its
    // fills — then reverts to the configured rewind window.
    {
        let shared = shared.clone();
        let update_tx = update_tx.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let interval = Duration::from_millis(shared.gap_replay.interval_ms.max(1));
            let rewind_ms = shared.gap_replay.periodic_rewind_ms;
            let mut last_market = String::new();
            loop {
                sleep(interval).await;
                if shutdown.load(Ordering::Relaxed) { break; }
                let market = shared.current_market.get();
                if market.is_empty() { continue; }   // no active event yet
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64).unwrap_or(0);
                let after = if market != last_market {
                    last_market = market.clone();
                    (now_ms / 1000).saturating_sub(300)            // new event → deep catch-up
                } else {
                    now_ms.saturating_sub(rewind_ms) / 1000        // rewind (ms) → floor to sec
                };
                replay_missed_trades(&shared, &update_tx, &market, after).await;
            }
        });
    }

    loop {
        if shutdown.load(Ordering::Relaxed) { break; }

        // Connect
        let ws_stream = match tokio_tungstenite::connect_async(WS_URL).await {
            Ok((ws, _)) => ws,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[PolyUserFeed] Connect failed: {}, retrying in {:.1}s", e, delay.as_secs_f64());
                sleep(delay).await;
                continue;
            }
        };

        let (mut sink, mut stream) = ws_stream.split();

        // Authenticate
        let auth_msg = serde_json::json!({
            "auth": {
                "apiKey": api_key,
                "secret": api_secret,
                "passphrase": passphrase,
            },
            "type": "user"
        });
        if let Err(e) = sink.send(Message::Text(auth_msg.to_string())).await {
            warn!("[PolyUserFeed] Auth send failed: {}", e);
            continue;
        }

        info!("[PolyUserFeed] Connected and authenticated (async)");
        backoff.reset();

        // Gap recovery on (re)connect — scope to the active market and rewind
        // `gap_replay.reconnect_rewind_ms` (default 5s, quantised up to whole
        // seconds) before the last-seen match_time so a fill that landed right
        // around the disconnect edge isn't skipped by an exact `after=`
        // boundary. Idempotent via the upsert_trade / update_trade status
        // dedup. Skips when no event is active yet (replay_missed_trades
        // returns 0 on empty market) — the periodic loop's now−300s deep
        // catch-up covers the first seed after startup.
        let market = shared.current_market.get();
        let rewind_secs = shared.gap_replay.reconnect_rewind_ms.div_ceil(1000);
        let after_secs = shared.live_position.lock().unwrap()
            .last_match_time_secs().saturating_sub(rewind_secs);
        let replayed = replay_missed_trades(&shared, &update_tx, &market, after_secs).await;
        info!("[PolyUserFeed] Gap recovery market={} after={} replayed={} trades",
            if market.is_empty() { "<none>" } else { &market }, after_secs, replayed);
        shared.user_feed_health.set_recovering(false);

        let mut last_ping = Instant::now();
        let mut last_data = Instant::now();

        // Event loop
        loop {
            if shutdown.load(Ordering::Relaxed) { break; }

            // Periodic PING (application-level — CLOB uses plaintext "PING"/"PONG"
            // strings, not WS-frame pings).
            if last_ping.elapsed() >= PING_INTERVAL {
                if sink.send(Message::Text("PING".to_string())).await.is_err() {
                    break;
                }
                last_ping = Instant::now();
            }

            // Await the next message with a short read timeout so we can
            // tick the PING / staleness loops without blocking forever.
            match timeout(READ_TIMEOUT, stream.next()).await {
                Ok(Some(Ok(msg))) => {
                    match msg {
                        Message::Text(text) => {
                            last_data = Instant::now();
                            if text == "PONG" || text.is_empty() { continue; }

                            let t_parse = crate::latency::Instant::now();
                            // simd-json drop-in for SIMD parse speedup.
                            let mut buf = text.as_bytes().to_vec();
                            if let Ok(data) = simd_json::serde::from_slice::<serde_json::Value>(&mut buf) {
                                let events = if data.is_array() {
                                    data.as_array().cloned().unwrap_or_default()
                                } else {
                                    vec![data]
                                };

                                for event in &events {
                                    for update in parse_user_event(event, &shared) {
                                        let coid_str = if update.client_order_id.is_empty() {
                                            match update.exchange_order_id.as_deref() {
                                                Some(oid) if !oid.is_empty() => {
                                                    let n = oid.len().min(10);
                                                    format!("<unmapped:orderID={}..>", &oid[..n])
                                                }
                                                _ => "<unmapped>".to_string(),
                                            }
                                        } else {
                                            update.client_order_id.clone()
                                        };
                                        info!(
                                            "[PolyUserFeed] {} coid={} {} {:?} filled={} price={}",
                                            update.symbol, coid_str,
                                            update.side, update.status,
                                            update.filled_quantity, update.avg_fill_price,
                                        );
                                        if update_tx.send(update).is_err() {
                                            return; // Channel closed
                                        }
                                    }
                                }
                            }
                            // Full frame parse + dispatch latency: wall
                            // time from text arrival to last OrderUpdate
                            // forwarded to the engine.
                            crate::latency::record("polymarket.user.event_parse", t_parse);
                        }
                        Message::Ping(payload) => {
                            last_data = Instant::now();
                            let _ = sink.send(Message::Pong(payload)).await;
                        }
                        Message::Close(_) => {
                            warn!("[PolyUserFeed] Server closed connection");
                            break;
                        }
                        _ => {}
                    }
                }
                Ok(Some(Err(e))) => {
                    warn!("[PolyUserFeed] Read error: {}", e);
                    break;
                }
                Ok(None) => {
                    warn!("[PolyUserFeed] Stream ended");
                    break;
                }
                Err(_) => {
                    // Timeout — no message in READ_TIMEOUT. Check staleness.
                    if last_data.elapsed() > STALE_TIMEOUT {
                        warn!("[PolyUserFeed] No data for 30s, reconnecting");
                        break;
                    }
                }
            }
        }

        // Disconnected
        info!("[PolyUserFeed] Disconnected, will reconcile on reconnect");
        shared.user_feed_health.set_recovering(true);
        if !shutdown.load(Ordering::Relaxed) {
            let delay = backoff.next_delay();
            warn!("[PolyUserFeed] Reconnecting in {:.1}s", delay.as_secs_f64());
            sleep(delay).await;
        }
    }

    info!("[PolyUserFeed] Stopped");
}

/// Spawn the Polymarket User WebSocket feed. The returned JoinHandle's
/// thread just awaits the underlying tokio task on the shared runtime.
pub fn spawn_user_feed(
    api_key: &str,
    api_secret: &str,
    passphrase: &str,
    shared: Arc<SharedState>,
    update_tx: Sender<OrderUpdate>,
    shutdown: Arc<AtomicBool>,
) -> Result<std::thread::JoinHandle<()>> {
    let api_key = api_key.to_string();
    let api_secret = api_secret.to_string();
    let passphrase = passphrase.to_string();

    let task_handle = async_rt::handle().spawn(user_feed_loop(
        api_key, api_secret, passphrase, shared, update_tx, shutdown,
    ));

    let handle = std::thread::Builder::new()
        .name("poly-user-feed-join".into())
        .spawn(move || {
            crate::os_tune::pin_background("poly-user-feed-join");
            async_rt::block_on_runtime(async move { let _ = task_handle.await; });
        })?;

    Ok(handle)
}
