//! Polymarket User WebSocket feed — receives real-time order/trade notifications.
//!
//! Async implementation (tokio + tokio-tungstenite). The public API returns
//! a `std::thread::JoinHandle` so the engine shutdown path is unchanged,
//! but under the hood the WS read loop runs as a tokio task on the shared
//! async runtime.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use crossbeam_channel::Sender;
use futures_util::{SinkExt, StreamExt};
use log::{debug, info, warn};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;

use crate::async_rt;
use crate::types::*;
use super::live_position::{LivePositionManager, TradeStatus};
use super::trade::SharedState;

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/user";
const CLOB_BASE_URL: &str = "https://clob.polymarket.com";
const PING_INTERVAL: Duration = Duration::from_secs(10);
const READ_TIMEOUT: Duration = Duration::from_secs(2);
const STALE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GapReplayOutcome {
    Complete { records: usize },
    Truncated { records: usize },
}

impl GapReplayOutcome {
    fn records(self) -> usize {
        match self {
            Self::Complete { records } | Self::Truncated { records } => records,
        }
    }
}

/// Apply a successful reconnect replay to feed health. Failed REST attempts
/// never call this helper, so `recovering` stays asserted and quoting remains
/// paused until the same recovery window has been fetched completely.
///
/// A truncated replay is transport-complete but inventory-incomplete: resume
/// consuming the WS while keeping trading stopped via `inventory_uncertain`.
fn accept_reconnect_replay(
    health: &super::live_position::UserFeedHealth,
    outcome: GapReplayOutcome,
) {
    if matches!(outcome, GapReplayOutcome::Truncated { .. }) {
        health.set_inventory_uncertain(true);
    }
    health.set_recovering(false);
}

/// Pin the beginning of one reconnect-recovery episode. REST replay updates
/// `last_match_time_secs` as it parses each page, including before a later page
/// fails, so recomputing this value on every attempt could skip the failed gap.
fn recovery_window_start(
    current: &mut Option<u64>,
    last_match_time_secs: u64,
    rewind_secs: u64,
) -> u64 {
    *current.get_or_insert_with(|| last_match_time_secs.saturating_sub(rewind_secs))
}

/// Record one trade-lifecycle edge and tell the caller whether it is new.
/// The live ledger owns the terminal/monotonic rules; gating at the feed
/// boundary prevents replayed terminal trades from reaching reconciliation
/// or inventory accounting again.
fn record_trade_transition(
    live_position: &Mutex<LivePositionManager>,
    trade_key: &str,
    status_str: &str,
    asset_id: &str,
    side: Side,
    size: f64,
    price: f64,
    is_maker: bool,
    reason: Option<&str>,
) -> bool {
    let Some(status) = TradeStatus::from_str(status_str) else {
        return false;
    };
    if trade_key.is_empty() || size <= 0.0 {
        return false;
    }
    live_position.lock().unwrap().update_trade(
        trade_key, status, asset_id, side, size, price, is_maker, reason,
    )
}

/// Parse a Polymarket user WebSocket event into zero-or-more OrderUpdates.
/// A single "trade" push from a MAKER perspective may expand into multiple
/// OrderUpdates (one per matching `maker_orders[]` entry owned by us).
pub(crate) fn parse_user_event(data: &serde_json::Value, shared: &SharedState) -> Vec<OrderUpdate> {
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
            // transition (MATCHED → MINED → CONFIRMED/FAILED); each carries
            // the full trade object. Gap replay can repeat the same object,
            // so only an edge accepted by `update_trade` is forwarded.
            // FAILED is terminal: the first edge is forwarded for inventory
            // reversal; later FAILED or stale earlier states are dropped.
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

            let status_raw = data.get("status").and_then(|v| v.as_str()).unwrap_or("MATCHED");
            let status_str = status_raw
                .strip_prefix("TRADE_STATUS_")
                .unwrap_or(status_raw);

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
            let failure_reason: Option<String> = extract_reason(data);
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

                    let lifecycle_advanced = record_trade_transition(
                        &shared.live_position,
                        &leg_id,
                        status_str,
                        &mo_asset_id,
                        mo_side,
                        mo_size,
                        mo_price,
                        true,
                        reason_ref,
                    );
                    if !lifecycle_advanced {
                        continue;
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
                        order_audit: None,
                        error: failure_reason.clone(),
                    });
                }
            } else {
                let matched_amount: f64 = parse_f(data.get("size").or_else(|| data.get("matched_amount")));
                let price: f64 = parse_f(data.get("price"));

                let lifecycle_advanced = record_trade_transition(
                    &shared.live_position,
                    trade_id,
                    status_str,
                    &asset_id,
                    side,
                    matched_amount,
                    price,
                    false,
                    reason_ref,
                );

                // Vacate the taker-matched accelerator buffer for this fill:
                // the authoritative WS push has arrived (this `OrderUpdate` is
                // pushed below and booked into PositionManager by the
                // strategy), so the HTTP-sourced placeholder must stop
                // contributing. Runs BEFORE the OrderUpdate is delivered, so
                // the strategy never double-counts.
                shared.taker_matched.on_ws_trade(trade_id);

                if !lifecycle_advanced {
                    return Vec::new();
                }

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
                    order_audit: None,
                    error: failure_reason.clone(),
                });
            }

            if status == OrderStatus::Failed
                && failure_reason.is_none()
                && !updates.is_empty()
            {
                // Warn only for the accepted terminal edge. Periodic REST
                // replay can return the same FAILED trade indefinitely.
                warn!("[PolyUserFeed] FAILED trade {} carries no known \
                      reason field; raw payload: {}",
                      trade_id, data);
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
    after_secs: u64,
) -> Result<GapReplayOutcome> {
    // Whole-wallet catch-up: L2 auth already restricts `/trades` to this
    // account, so we fetch ALL of the wallet's trades since `after` (no
    // `?market=` narrowing). This is multi-market correct — two instances
    // sharing one wallet both recover via the same sweep — and `upsert_trade`
    // dedups by trade_id + routes by asset_id, so cross-market rows are
    // harmless. (Previously scoped to a single `CurrentMarket` condition_id,
    // which a sibling instance could clobber → wrong-market replay.)
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
            format!("{}/trades?after={}", CLOB_BASE_URL, after_param)
        } else {
            format!("{}/trades?after={}&next_cursor={}",
                CLOB_BASE_URL, after_param, cursor)
        };
        let mut req = client.get(&url);
        for (k, v) in headers.as_pairs() {
            req = req.header(k, v);
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                return Err(anyhow!(
                    "Gap-fetch /trades request failed after {} records: {}",
                    count,
                    e,
                ));
            }
        };
        if !resp.status().is_success() {
            let code = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Gap-fetch /trades HTTP {} after {} records: {}",
                code,
                count,
                body,
            ));
        }
        let json: serde_json::Value = match resp.json().await {
            Ok(j) => j,
            Err(e) => {
                return Err(anyhow!(
                    "Gap-fetch /trades parse failed after {} records: {}",
                    count,
                    e,
                ));
            }
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
        Ok(GapReplayOutcome::Truncated { records: count })
    } else {
        Ok(GapReplayOutcome::Complete { records: count })
    }
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
    let reconnect_rewind_secs = shared.gap_replay.reconnect_rewind_ms.div_ceil(1000);
    // Fixed lower bound for the current recovery episode. A partial REST
    // attempt may advance `last_match_time_secs`; recomputing from that cursor
    // would then skip an earlier page that failed. Retain this floor across
    // reconnect attempts and clear it only after a successful replay.
    let mut recovery_after_secs: Option<u64> = None;

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
        // New task → won't inherit the loop's span; re-attach the same
        // per-account span so gap-recovery logs are tagged too.
        let gap_span = tracing::info_span!("user_feed", acct = %shared.instance_id);
        tokio::spawn(tracing::Instrument::instrument(async move {
            let interval = Duration::from_millis(shared.gap_replay.interval_ms.max(1));
            let rewind_ms = shared.gap_replay.periodic_rewind_ms;
            // One-shot deep (now−300s) catch-up on the first sweep so a
            // mid-event (re)start recovers all in-flight fills across EVERY
            // active market on this wallet at once; subsequent sweeps use the
            // small rewind. (Was keyed on per-event `CurrentMarket` change,
            // which a sibling instance sharing the wallet could clobber.)
            let mut did_startup_deep = false;
            loop {
                sleep(interval).await;
                if shutdown.load(Ordering::Relaxed) { break; }
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64).unwrap_or(0);
                let after = if !did_startup_deep {
                    did_startup_deep = true;
                    (now_ms / 1000).saturating_sub(300)            // startup → deep catch-up
                } else {
                    now_ms.saturating_sub(rewind_ms) / 1000        // rewind (ms) → floor to sec
                };
                match replay_missed_trades(&shared, &update_tx, after).await {
                    Ok(GapReplayOutcome::Complete { .. }) => {}
                    Ok(outcome @ GapReplayOutcome::Truncated { .. }) => {
                        shared.user_feed_health.set_inventory_uncertain(true);
                        warn!(
                            "[PolyUserFeed] Periodic gap replay incomplete: {} records fetched; \
                             inventory remains uncertain",
                            outcome.records(),
                        );
                    }
                    Err(e) => {
                        warn!("[PolyUserFeed] Periodic gap replay failed: {}", e);
                    }
                }
            }
        }, gap_span));
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

        // Gap recovery on (re)connect — whole-wallet, rewind
        // `gap_replay.reconnect_rewind_ms` (default 5s, quantised up to whole
        // seconds) before the last-seen match_time so a fill that landed right
        // around the disconnect edge isn't skipped by an exact `after=`
        // boundary. Idempotent via the upsert_trade / update_trade status
        // dedup. Covers ALL active markets on this wallet at once.
        let last_match_time_secs =
            shared.live_position.lock().unwrap().last_match_time_secs();
        let after_secs = recovery_window_start(
            &mut recovery_after_secs,
            last_match_time_secs,
            reconnect_rewind_secs,
        );
        match replay_missed_trades(&shared, &update_tx, after_secs).await {
            Ok(outcome) => {
                match outcome {
                    GapReplayOutcome::Complete { records } => {
                        info!(
                            "[PolyUserFeed] Gap recovery after={} replayed={} trades (complete)",
                            after_secs,
                            records,
                        );
                    }
                    GapReplayOutcome::Truncated { records } => {
                        warn!(
                            "[PolyUserFeed] Gap recovery after={} replayed={} trades but was \
                             truncated; WS consumption will continue with trading stopped",
                            after_secs,
                            records,
                        );
                    }
                }
                accept_reconnect_replay(&shared.user_feed_health, outcome);
                recovery_after_secs = None;
                backoff.reset();
            }
            Err(e) => {
                // Do not enter the WS read loop with an unverified gap. Drop
                // this socket, retain `recovery_after_secs`, and retry the
                // complete original window after reconnect.
                shared.user_feed_health.set_recovering(true);
                let delay = backoff.next_delay();
                warn!(
                    "[PolyUserFeed] Gap recovery after={} failed: {}; keeping quoting paused and \
                     reconnecting in {:.1}s",
                    after_secs,
                    e,
                    delay.as_secs_f64(),
                );
                if !shutdown.load(Ordering::Relaxed) {
                    sleep(delay).await;
                }
                continue;
            }
        }

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
                                        // RTT-probe traffic: the probe's synthetic
                                        // resting orders have no coid mapping, so
                                        // their placement / cancellation pushes
                                        // would log as `<unmapped>` (an ops signal
                                        // expected to stay at zero) and broadcast
                                        // to every instance. Identify them by
                                        // orderID and swallow: DEBUG only.
                                        if update.client_order_id.is_empty() {
                                            if let Some(oid) = update.exchange_order_id.as_deref() {
                                                let is_probe = shared
                                                    .probe_order_ids
                                                    .lock()
                                                    .unwrap_or_else(|p| p.into_inner())
                                                    .iter()
                                                    .any(|p| p == oid);
                                                if is_probe {
                                                    debug!(
                                                        "[PolyUserFeed] probe order push muted: {} {:?} oid={}..",
                                                        update.symbol, update.status,
                                                        &oid[..oid.len().min(10)],
                                                    );
                                                    continue;
                                                }
                                            }
                                        }
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
        let last_match_time_secs =
            shared.live_position.lock().unwrap().last_match_time_secs();
        recovery_window_start(
            &mut recovery_after_secs,
            last_match_time_secs,
            reconnect_rewind_secs,
        );
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

    // Tag every `[PolyUserFeed]` line with the ACCOUNT this feed serves
    // (`user_feed{acct=<account_id>}:`). The feed is per-account (one
    // authenticated stream per wallet, shared by all instances on it), so
    // account is the correct grain — per-fill instance routing happens
    // downstream via coid→instance. `SharedState.instance_id` holds the
    // account_id. Async task → `.instrument()` (NOT `.entered()` across
    // await).
    use tracing::Instrument as _;
    let acct = shared.instance_id.clone();
    let task_handle = async_rt::handle().spawn(
        user_feed_loop(api_key, api_secret, passphrase, shared, update_tx, shutdown)
            .instrument(tracing::info_span!("user_feed", acct = %acct)),
    );

    let handle = std::thread::Builder::new()
        .name("poly-user-feed-join".into())
        .spawn(move || {
            crate::os_tune::pin_background("poly-user-feed-join");
            async_rt::block_on_runtime(async move { let _ = task_handle.await; });
        })?;

    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(manager: &Mutex<LivePositionManager>, status: &str) -> bool {
        record_trade_transition(
            manager,
            "1651e74c-6358-41d1-b9df-5c5b38bd981e:0xmaker-order",
            status,
            "TOKEN",
            Side::Sell,
            10.0,
            0.58,
            true,
            None,
        )
    }

    #[test]
    fn failed_is_forwarded_once_then_replay_and_regression_are_dropped() {
        let manager = Mutex::new(LivePositionManager::new());

        assert!(record(&manager, "MATCHED"));
        assert!(record(&manager, "FAILED"));

        // Mirrors the live 118-push replay storm: only the first FAILED edge
        // may reach downstream accounting and reverse MATCHED inventory.
        for _ in 1..118 {
            assert!(!record(&manager, "FAILED"));
        }
        assert!(!record(&manager, "MATCHED"), "FAILED is terminal");
        assert!(!record(&manager, "MINED"), "FAILED cannot regress");
        assert!(!record(&manager, "CONFIRMED"), "FAILED cannot flip terminal");
    }

    #[test]
    fn first_sighting_failed_is_forwarded_once_for_tombstoning() {
        let manager = Mutex::new(LivePositionManager::new());

        assert!(record(&manager, "FAILED"));
        assert!(!record(&manager, "FAILED"));
    }

    #[test]
    fn reconnect_health_clears_only_after_a_successful_replay() {
        let health = super::super::live_position::UserFeedHealth::new();
        assert!(health.is_recovering());

        // A failed REST result never reaches `accept_reconnect_replay`.
        let failed: Result<GapReplayOutcome> = Err(anyhow!("temporary REST failure"));
        if let Ok(outcome) = failed {
            accept_reconnect_replay(&health, outcome);
        }
        assert!(
            health.is_recovering(),
            "REST failure must keep quoting paused",
        );

        accept_reconnect_replay(
            &health,
            GapReplayOutcome::Complete { records: 3 },
        );
        assert!(!health.is_recovering());
        assert!(!health.inventory_uncertain());
    }

    #[test]
    fn truncated_replay_keeps_trading_stopped_via_inventory_uncertain() {
        let health = super::super::live_position::UserFeedHealth::new();

        accept_reconnect_replay(
            &health,
            GapReplayOutcome::Truncated { records: 2_500 },
        );

        assert!(!health.is_recovering(), "WS consumption may resume");
        assert!(
            health.inventory_uncertain(),
            "quoting must remain stopped after an incomplete replay",
        );
    }

    #[test]
    fn reconnect_retries_keep_the_original_recovery_window() {
        let mut recovery_after = None;

        assert_eq!(recovery_window_start(&mut recovery_after, 1_000, 3), 997);
        // A partial first attempt may have advanced the observed match time,
        // but its failed later page still requires the original lower bound.
        assert_eq!(recovery_window_start(&mut recovery_after, 1_100, 3), 997);

        recovery_after = None;
        assert_eq!(recovery_window_start(&mut recovery_after, 1_100, 3), 1_097);
    }
}
