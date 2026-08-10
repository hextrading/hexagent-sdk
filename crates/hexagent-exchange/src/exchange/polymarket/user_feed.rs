//! Polymarket User WebSocket feed — receives real-time order/trade notifications.
//!
//! Async implementation (tokio + tokio-tungstenite). The public API returns
//! a `std::thread::JoinHandle` so the engine shutdown path is unchanged,
//! but under the hood the WS read loop runs as a tokio task on the shared
//! async runtime.

use std::error::Error as _;
use std::collections::HashSet;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use crossbeam_channel::Sender;
use futures_util::{SinkExt, StreamExt};
use hexagent_account::account::shared_account::normalize_order_id;
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
const GAP_REPLAY_DEGRADED_AFTER_FAILURES: u32 = 3;
const GAP_USER_AGENT: &str = "hexbot-gap-replay/1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GapReplayOutcome {
    Complete { records: usize },
}

/// In-memory progress for one authenticated `/trades` pagination sweep.
/// Retaining this value across a transient failure makes the retry request the
/// exact failed page instead of starting again at the original `after`.
#[derive(Debug, Clone)]
struct GapReplayCheckpoint {
    after_secs: u64,
    cursor: String,
    seen_cursors: HashSet<String>,
    records: usize,
    pages: usize,
}

impl GapReplayCheckpoint {
    fn new(after_secs: u64) -> Self {
        Self {
            after_secs,
            cursor: String::new(),
            seen_cursors: HashSet::new(),
            records: 0,
            pages: 0,
        }
    }
}

impl GapReplayOutcome {
    fn records(self) -> usize {
        match self {
            Self::Complete { records } => records,
        }
    }
}

/// Apply a successful reconnect replay to feed health. Failed REST attempts
/// never call this helper, so `recovering` stays asserted and quoting remains
/// paused until the same recovery window has been fetched completely.
///
fn accept_reconnect_replay(
    health: &super::live_position::UserFeedHealth,
    _outcome: GapReplayOutcome,
) {
    health.set_recovering(false);
}

fn advance_gap_cursor(
    cursor: &mut String,
    seen: &mut HashSet<String>,
    next: String,
) -> Result<bool> {
    if next.is_empty() || next == "LTE=" {
        return Ok(false);
    }
    if !seen.insert(next.clone()) {
        return Err(anyhow!("Gap-fetch /trades returned repeated cursor `{next}`"));
    }
    *cursor = next;
    Ok(true)
}

#[derive(Debug, Clone)]
struct GapSendFailure {
    slot: usize,
    generation: u64,
    kind: &'static str,
    detail: String,
}

impl GapSendFailure {
    fn new(slot: usize, generation: u64, kind: &'static str, detail: String) -> Self {
        Self { slot, generation, kind, detail }
    }

    fn from_reqwest(slot: usize, generation: u64, error: &reqwest::Error) -> Self {
        let kind = if error.is_timeout() {
            "timeout"
        } else if error.is_connect() {
            "connect"
        } else if error.is_body() {
            "body"
        } else if error.is_request() {
            "request"
        } else if error.is_decode() {
            "decode"
        } else {
            "other"
        };
        let mut detail = error.to_string();
        let mut source = error.source();
        while let Some(cause) = source {
            detail.push_str(": ");
            detail.push_str(&cause.to_string());
            source = cause.source();
        }
        Self {
            slot,
            generation,
            kind,
            detail,
        }
    }

    fn is_transport(&self) -> bool {
        matches!(self.kind, "timeout" | "connect" | "body" | "request")
    }
}

impl fmt::Display for GapSendFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.kind == "pool_busy" {
            return write!(f, "kind={}: {}", self.kind, self.detail);
        }
        write!(
            f,
            "slot={} generation={} kind={}: {}",
            self.slot,
            self.generation,
            self.kind,
            self.detail,
        )
    }
}

struct GapHttpResponse {
    response: reqwest::Response,
    permit: crate::http1_pool::Permit,
}

struct GapAttemptFailure {
    failure: GapSendFailure,
    _permit: Option<crate::http1_pool::Permit>,
}

/// Facade over one account's physically isolated GapReplay pool.
///
/// The account pool owns clients, admission, counters, generations and rebuild
/// state. This facade owns endpoint auth and primary-to-peer retry policy.
struct GapReplayTransport {
    account_id: String,
}

impl GapReplayTransport {
    fn new(account_id: impl Into<String>) -> Self {
        Self {
            account_id: account_id.into(),
        }
    }

    async fn prewarm(&self) -> crate::http1_pool::PrewarmReport {
        let probe_url = format!("{}/time", CLOB_BASE_URL);
        crate::http1_pool::prewarm_account_gap_replay(&self.account_id, &probe_url).await
    }

    async fn send_once(
        &self,
        shared: &SharedState,
        url: &str,
    ) -> std::result::Result<GapHttpResponse, GapAttemptFailure> {
        let permit = match crate::http1_pool::try_acquire_account(
            &self.account_id,
            crate::http1_pool::Role::GapReplay,
        ) {
            Some(permit) => permit,
            None => return Err(GapAttemptFailure {
                failure: GapSendFailure::new(
                    usize::MAX, 0, "pool_busy",
                    "no GapReplay warm slot available".to_string(),
                ),
                _permit: None,
            }),
        };
        let slot = permit.slot();
        let generation = permit.generation();
        let client = permit.pooled_client();
        let headers = shared.auth.sign_request("GET", "/trades", "");
        let mut request = client.client().get(url).header("User-Agent", GAP_USER_AGENT);
        for (key, value) in headers.as_pairs() {
            request = request.header(key, value);
        }
        match request.send().await {
            Ok(response) => Ok(GapHttpResponse { response, permit }),
            Err(error) => {
                let failure = GapSendFailure::from_reqwest(slot, generation, &error);
                if failure.is_transport() {
                    client.note_transport_failure(format!("{}/time", CLOB_BASE_URL));
                }
                Err(GapAttemptFailure {
                    failure,
                    _permit: Some(permit),
                })
            }
        }
    }

    async fn get(
        &self,
        shared: &SharedState,
        url: &str,
    ) -> std::result::Result<GapHttpResponse, String> {
        match self.send_once(shared, url).await {
            Ok(result) => Ok(result),
            Err(first) => {
                // Keep the failed primary permit while acquiring the fallback,
                // guaranteeing that the retry binds to a different slot.
                match self.send_once(shared, url).await {
                    Ok(result) => {
                        warn!(
                            "[PolyUserFeed] GapReplay transport failover succeeded: failed [{}], \
                             fallback_slot={} generation={}",
                            first.failure,
                            result.permit.slot(),
                            result.permit.generation(),
                        );
                        Ok(result)
                    }
                    Err(second) => {
                        Err(format!(
                            "primary [{}]; fallback [{}]",
                            first.failure,
                            second.failure,
                        ))
                    }
                }
            }
        }
    }

    async fn report_body_failure(
        &self,
        permit: crate::http1_pool::Permit,
        failure: GapSendFailure,
    ) {
        if failure.is_transport() {
            permit
                .pooled_client()
                .note_transport_failure(format!("{}/time", CLOB_BASE_URL));
        }
    }
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
    if trade_key.is_empty()
        || !size.is_finite()
        || size <= 0.0
        || !price.is_finite()
        || price <= 0.0
        || price > 1.0 + f64::EPSILON
    {
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

    // Top-level ID resolution for order-lifecycle events. Trade events use a
    // role-specific order id below: TAKER must prefer `taker_order_id`, while
    // MAKER must use each matching `maker_orders[].order_id`.
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
    //       legacy/order-lifecycle payloads. They are handled separately so
    //       they can never override an authoritative `taker_order_id` when a
    //       schema variant contains both.
    let order_id = data.get("order_id")
        .or_else(|| data.get("orderID"))
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
            let taker_order_id = data
                .get("taker_order_id")
                .and_then(|v| v.as_str())
                .filter(|value| !value.is_empty())
                .or_else(|| data.get("order_id").and_then(|v| v.as_str()))
                .filter(|value| !value.is_empty())
                .or_else(|| data.get("orderID").and_then(|v| v.as_str()))
                .unwrap_or("");
            let asset_id = data.get("asset_id")
                .or_else(|| data.get("token_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("").to_string();

            // The authenticated maker address determines whether we use
            // top-level fields (TAKER) or walk `maker_orders[]` (MAKER).
            // `trader_side` is not authoritative and has been empty/wrong in
            // otherwise valid payloads.
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
                    let normalized_mo_order_id = normalize_order_id(mo_order_id);

                    let leg_id = if normalized_mo_order_id.is_empty() {
                        trade_id.to_string()
                    } else {
                        format!("{}:{}", trade_id, normalized_mo_order_id)
                    };

                    if TradeStatus::from_str(status_str).is_none()
                        || leg_id.is_empty()
                        || mo_size <= 0.0
                    {
                        continue;
                    }

                    let runtime_coid = shared.lookup_coid(mo_order_id).unwrap_or_default();
                    let Some(ownership) = shared.account_state.apply_trade_transition(
                        &leg_id,
                        status_str,
                        &runtime_coid,
                        mo_order_id,
                        &mo_asset_id,
                        mo_side,
                        mo_size,
                        mo_price,
                    ) else {
                        // Never broadcast an unowned private trade. The account
                        // ledger has already entered uncertain with the exact
                        // oid/trade reason; fanning an empty coid to every
                        // same-token strategy would book the fill N times.
                        continue;
                    };
                    let coid = ownership.client_order_id;
                    // Only advance the feed-level dedupe after ownership was
                    // successfully resolved. An unowned event must remain
                    // replayable after its order mapping arrives later.
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
                    let _ = shared.account_state.apply_configured_trade_fee(
                        &leg_id,
                        status,
                        true,
                    );
                    shared.finish_filled_order_if_audited(&coid);

                    updates.push(OrderUpdate {
                        client_order_id: coid,
                        exchange: Exchange::Polymarket,
                        symbol: mo_asset_id,
                        side: mo_side,
                        exchange_order_id: if mo_order_id.is_empty() { None } else { Some(mo_order_id.to_string()) },
                        status,
                        liquidity: Some(Liquidity::Maker),
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

                if TradeStatus::from_str(status_str).is_none()
                    || trade_id.is_empty()
                    || matched_amount <= 0.0
                {
                    return Vec::new();
                }

                let runtime_coid = shared.lookup_coid(taker_order_id).unwrap_or_default();
                let Some(ownership) = shared.account_state.apply_trade_transition(
                    trade_id,
                    status_str,
                    &runtime_coid,
                    taker_order_id,
                    &asset_id,
                    side,
                    matched_amount,
                    price,
                ) else {
                    return Vec::new();
                };
                let coid = ownership.client_order_id;
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
                if !lifecycle_advanced {
                    return Vec::new();
                }
                let _ = shared.account_state.apply_configured_trade_fee(
                    trade_id,
                    status,
                    false,
                );
                shared.finish_filled_order_if_audited(&coid);

                updates.push(OrderUpdate {
                    client_order_id: coid,
                    exchange: Exchange::Polymarket,
                    symbol: asset_id,
                    side,
                    exchange_order_id: if taker_order_id.is_empty() {
                        None
                    } else {
                        Some(taker_order_id.to_string())
                    },
                    status,
                    liquidity: Some(Liquidity::Taker),
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
/// endpoint and replay them through the update channel.
async fn replay_missed_trades(
    shared: &SharedState,
    update_tx: &Sender<OrderUpdate>,
    checkpoint: &mut GapReplayCheckpoint,
    transport: &mut GapReplayTransport,
) -> Result<GapReplayOutcome> {
    let pages_before_attempt = checkpoint.pages;
    let result = replay_missed_trades_inner(shared, update_tx, checkpoint, transport).await;
    shared.account_state.record_gap_replay_pages(
        checkpoint.pages.saturating_sub(pages_before_attempt),
    );
    result
}

async fn replay_missed_trades_inner(
    shared: &SharedState,
    update_tx: &Sender<OrderUpdate>,
    checkpoint: &mut GapReplayCheckpoint,
    transport: &mut GapReplayTransport,
) -> Result<GapReplayOutcome> {
    // Whole-wallet catch-up: L2 auth already restricts `/trades` to this
    // account, so we fetch ALL of the wallet's trades since `after` (no
    // `?market=` narrowing). This is multi-market correct — two instances
    // sharing one wallet both recover via the same sweep — and `upsert_trade`
    // dedups by trade_id + routes by asset_id, so cross-market rows are
    // harmless. (Previously scoped to a single `CurrentMarket` condition_id,
    // which a sibling instance could clobber → wrong-market replay.)
    // Roll back 1 s on the boundary so trades sharing the same second as
    // `last_match_time` aren't excluded by Polymarket's strict-`>`
    // semantics on `?after=T`. The overlap is harmless — `trade_id`
    // dedup in `PositionManager::upsert_trade` short-circuits any trade
    // already in the ledger (terminal-state guard, position.rs:171).
    let after_param = checkpoint.after_secs.saturating_sub(1);

    // Never abandon a valid next_cursor merely because a fixed page budget
    // was reached. Long disconnects are replayed to completion through the
    // same account-level connection slot. Yield periodically so the runtime
    // can service the live user feed and other accounts between batches.
    const PAGES_PER_YIELD: usize = 50;
    let mut attempt_pages = 0usize;
    loop {
        let url = if checkpoint.cursor.is_empty() {
            format!("{}/trades?after={}", CLOB_BASE_URL, after_param)
        } else {
            format!("{}/trades?after={}&next_cursor={}",
                CLOB_BASE_URL, after_param, checkpoint.cursor)
        };
        let gap_response = match transport.get(shared, &url).await {
            Ok(response) => response,
            Err(error) => {
                return Err(anyhow!(
                    "Gap-fetch /trades request failed after {} records: {}",
                    checkpoint.records,
                    error,
                ));
            }
        };
        let GapHttpResponse {
            response: resp,
            permit,
        } = gap_response;
        let response_slot = permit.slot();
        let response_generation = permit.generation();
        if !resp.status().is_success() {
            let code = resp.status().as_u16();
            let body = match resp.text().await {
                Ok(body) => {
                    permit.pooled_client().note_transport_success();
                    body
                }
                Err(error) => {
                    let failure = GapSendFailure::from_reqwest(
                        response_slot,
                        response_generation,
                        &error,
                    );
                    let detail = format!("<response body read failed: {}>", failure);
                    transport.report_body_failure(permit, failure).await;
                    return Err(anyhow!(
                        "Gap-fetch /trades HTTP {} after {} records: {}",
                        code,
                        checkpoint.records,
                        detail,
                    ));
                }
            };
            return Err(anyhow!(
                "Gap-fetch /trades HTTP {} after {} records: {}",
                code,
                checkpoint.records,
                body,
            ));
        }
        let json: serde_json::Value = match resp.json().await {
            Ok(j) => {
                permit.pooled_client().note_transport_success();
                j
            }
            Err(error) => {
                let failure = GapSendFailure::from_reqwest(
                    response_slot,
                    response_generation,
                    &error,
                );
                if failure.is_transport() {
                    let detail = failure.to_string();
                    transport.report_body_failure(permit, failure).await;
                    return Err(anyhow!(
                        "Gap-fetch /trades parse failed after {} records: {}",
                        checkpoint.records,
                        detail,
                    ));
                }
                permit.pooled_client().note_transport_success();
                return Err(anyhow!(
                    "Gap-fetch /trades parse failed after {} records: {}",
                    checkpoint.records,
                    failure,
                ));
            }
        };
        // The response body is fully consumed; release the exclusive global
        // slot before routing/deduplicating records.
        drop(permit);

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
            checkpoint.records += 1;
        }

        checkpoint.pages += 1;
        attempt_pages += 1;
        if !advance_gap_cursor(
            &mut checkpoint.cursor,
            &mut checkpoint.seen_cursors,
            next,
        )? {
            break;
        }
        if attempt_pages % PAGES_PER_YIELD == 0 {
            tokio::task::yield_now().await;
        }
    }

    Ok(GapReplayOutcome::Complete { records: checkpoint.records })
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
    let account_id = shared.account_state.account_id().to_string();
    let transport = GapReplayTransport::new(account_id.clone());
    // Warm exactly this account's two replay slots before its first reconnect
    // or periodic replay. Other accounts own independent replay capacity.
    let prewarm = transport.prewarm().await;
    if prewarm.ok < prewarm.total {
        shared.user_feed_health.set_recovering(true);
        warn!(
            "[PolyUserFeed] account={} GapReplay prewarm incomplete: {}/{} slots ready; \
             first_error={}; keeping recovery asserted until catch-up succeeds",
            account_id,
            prewarm.ok,
            prewarm.total,
            prewarm.first_error.as_deref().unwrap_or("<unknown>"),
        );
    } else {
        info!(
            "[PolyUserFeed] account={} GapReplay prewarmed all {} slots",
            account_id,
            prewarm.total,
        );
    }
    let gap_transport = Arc::new(tokio::sync::Mutex::new(transport));
    // Retain both the lower bound and exact next_cursor across reconnects.
    // A transient failure therefore resumes the failed page without either
    // skipping the original window or redownloading its completed prefix.
    let mut recovery_checkpoint: Option<GapReplayCheckpoint> = None;

    // Periodic gap-replay task — independent of the WS read loop so its HTTP
    // call never pauses WS reads, and it keeps recovering fills *across*
    // reconnects (including while the main loop is reconnecting). Cadence and
    // rewind window are config-driven (`gap_replay.interval_ms` /
    // `periodic_rewind_ms`; defaults 2s / 10s — the rewind is a FLOOR,
    // the sweep also always reaches back to the last server-timestamped
    // trade seen, so longer WS gaps stay covered). The status dedup in
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
        let gap_transport = gap_transport.clone();
        // New task → won't inherit the loop's span; re-attach the same
        // per-account span so gap-recovery logs are tagged too.
        let gap_span = tracing::info_span!("user_feed", acct = %account_id);
        tokio::spawn(tracing::Instrument::instrument(async move {
            let interval = Duration::from_millis(shared.gap_replay.interval_ms.max(1));
            let rewind_ms = shared.gap_replay.periodic_rewind_ms;
            // One-shot deep (now−300s) catch-up on the first sweep so a
            // mid-event (re)start recovers all in-flight fills across EVERY
            // active market on this wallet at once; subsequent sweeps use the
            // small rewind. (Was keyed on per-event `CurrentMarket` change,
            // which a sibling instance sharing the wallet could clobber.)
            let mut did_startup_deep = false;
            let mut periodic_checkpoint: Option<GapReplayCheckpoint> = None;
            let mut consecutive_failures = 0u32;
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
                    // Dynamic rewind: max(configured floor, now − last trade
                    // the feed actually delivered, on the SERVER match_time
                    // axis) — i.e. `after = min(now − floor, last_trade − 1)`.
                    // A WS drop longer than the floor is then still covered:
                    // the window always reaches back to the last fill we have
                    // seen. −1 s guards Polymarket's strict-`>` semantics on
                    // `?after=T`; the overlap is deduped by trade_id.
                    let floor_after = now_ms.saturating_sub(rewind_ms) / 1000; // rewind (ms) → floor to sec
                    let last_trade_secs =
                        shared.live_position.lock().unwrap().last_match_time_secs();
                    if last_trade_secs > 0 {
                        floor_after.min(last_trade_secs.saturating_sub(1))
                    } else {
                        floor_after
                    }
                };
                let checkpoint = periodic_checkpoint
                    .get_or_insert_with(|| GapReplayCheckpoint::new(after));
                let after = checkpoint.after_secs;
                let replay_result = {
                    let mut transport = gap_transport.lock().await;
                    replay_missed_trades(&shared, &update_tx, checkpoint, &mut transport).await
                };
                match replay_result {
                    Ok(outcome @ GapReplayOutcome::Complete { .. }) => {
                        if consecutive_failures > 0 {
                            info!(
                                "[PolyUserFeed] Periodic gap replay recovered: after={} attempts={} \
                                 records={}",
                                after,
                                consecutive_failures.saturating_add(1),
                                outcome.records(),
                            );
                        }
                        consecutive_failures = 0;
                        periodic_checkpoint = None;
                        shared.user_feed_health.set_gap_replay_degraded(false);
                    }
                    Err(e) => {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        if consecutive_failures >= GAP_REPLAY_DEGRADED_AFTER_FAILURES {
                            let newly_degraded = !shared
                                .user_feed_health
                                .gap_replay_degraded();
                            shared.user_feed_health.set_gap_replay_degraded(true);
                            if newly_degraded {
                                let (acquires, skips, busy, slots) =
                                    crate::http1_pool::gap_replay_stats(&account_id)
                                        .unwrap_or((0, 0, 0, Vec::new()));
                                warn!(
                                    "[PolyUserFeed] Periodic gap replay DEGRADED after {} \
                                     consecutive failures; after={} remains pinned and quoting \
                                     will pause until catch-up succeeds; \
                                     account={} GapReplay pool slots={:?} acquires={} skips={} busy={}",
                                    consecutive_failures,
                                    after,
                                    account_id,
                                    slots,
                                    acquires,
                                    skips,
                                    busy,
                                );
                            }
                        }
                        warn!(
                            "[PolyUserFeed] Periodic gap replay failed: {} (attempt={} pinned_after={})",
                            e,
                            consecutive_failures,
                            after,
                        );
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
        let checkpoint = recovery_checkpoint.get_or_insert_with(|| {
            GapReplayCheckpoint::new(last_match_time_secs.saturating_sub(reconnect_rewind_secs))
        });
        let after_secs = checkpoint.after_secs;
        let replay_result = {
            let mut transport = gap_transport.lock().await;
            replay_missed_trades(
                &shared,
                &update_tx,
                checkpoint,
                &mut transport,
            )
            .await
        };
        match replay_result {
            Ok(outcome) => {
                match outcome {
                    GapReplayOutcome::Complete { records } => {
                        info!(
                            "[PolyUserFeed] Gap recovery after={} replayed={} trades (complete)",
                            after_secs,
                            records,
                        );
                    }
                }
                accept_reconnect_replay(&shared.user_feed_health, outcome);
                recovery_checkpoint = None;
                backoff.reset();
            }
            Err(e) => {
                // Do not enter the WS read loop with an unverified gap. Drop
                // this socket, retain the exact failed cursor, and retry that
                // page after reconnect.
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
            // strings). Also send a WebSocket protocol Ping frame.
            if last_ping.elapsed() >= PING_INTERVAL {
                if let Err(e) = sink.send(Message::Text("PING".to_string())).await {
                    warn!("[PolyUserFeed] Text PING send failed: {}", e);
                    break;
                }
                if let Err(e) = sink.send(Message::Ping(Vec::new())).await {
                    warn!("[PolyUserFeed] Frame Ping send failed: {}", e);
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
                        Message::Pong(_) => {
                            last_data = Instant::now();
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
        recovery_checkpoint.get_or_insert_with(|| {
            GapReplayCheckpoint::new(last_match_time_secs.saturating_sub(reconnect_rewind_secs))
        });
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
    use std::collections::HashMap;

    fn test_shared() -> Arc<SharedState> {
        super::super::trade::PolymarketTrade::new(
            "api-key",
            "c2VjcmV0",
            "passphrase",
            "0x0000000000000000000000000000000000000000000000000000000000000001",
            false,
            10,
            super::super::signer::SignatureType::Eoa,
        )
        .unwrap()
        .shared_state()
    }

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
    fn unowned_trade_remains_replayable_after_order_mapping_is_repaired() {
        let trade = super::super::trade::PolymarketTrade::new(
            "api-key",
            "c2VjcmV0",
            "passphrase",
            "0x0000000000000000000000000000000000000000000000000000000000000001",
            false,
            10,
            super::super::signer::SignatureType::Eoa,
        )
        .unwrap();
        let shared = trade.shared_state();
        shared.account_state.register_instance("owner", 1.0);
        shared
            .account_state
            .apply_physical_snapshot(100.0, HashMap::new());
        shared
            .account_state
            .register_token_fee_config(&["TOKEN".to_string()], 0.0, 1.0)
            .unwrap();
        shared
            .account_state
            .reserve_order(
                "owner",
                "owner-1",
                "oid-temporary",
                "TOKEN",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();
        let event = serde_json::json!({
            "event_type": "trade",
            "id": "trade-replayable",
            "status": "MATCHED",
            "asset_id": "TOKEN",
            "side": "BUY",
            "size": "10",
            "price": "0.5",
            "taker_order_id": "oid-final",
            "maker_orders": [],
        });

        assert!(parse_user_event(&event, &shared).is_empty());
        assert!(shared.account_state.is_uncertain());

        shared.account_state.rebind_order_id("owner-1", "oid-final");
        shared.register_order_id("owner-1", "oid-final", "TOKEN");
        let updates = parse_user_event(&event, &shared);
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].client_order_id, "owner-1");
        assert!(!shared.account_state.is_uncertain());
    }

    #[test]
    fn taker_prefers_taker_order_id_over_conflicting_legacy_order_id() {
        let shared = test_shared();
        shared.account_state.register_instance("taker-owner", 1.0);
        shared.account_state.register_instance("legacy-owner", 1.0);
        shared
            .account_state
            .apply_physical_snapshot(200.0, HashMap::new());
        shared
            .account_state
            .register_token_fee_config(&["TOKEN".to_string()], 0.0, 1.0)
            .unwrap();
        shared
            .account_state
            .reserve_order(
                "taker-owner",
                "taker-coid",
                "0xAaBb",
                "TOKEN",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();
        shared
            .account_state
            .reserve_order(
                "legacy-owner",
                "legacy-coid",
                "0xDeAd",
                "TOKEN",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();
        shared.register_order_id("taker-coid", "0xAaBb", "TOKEN");
        shared.register_order_id("legacy-coid", "0xDeAd", "TOKEN");

        let updates = parse_user_event(
            &serde_json::json!({
                "event_type": "trade",
                "id": "trade-taker-field-priority",
                "status": "MATCHED",
                "asset_id": "TOKEN",
                "side": "BUY",
                "size": "10",
                "price": "0.5",
                "taker_order_id": "AABB",
                "order_id": "0xdead",
                "maker_orders": [],
            }),
            &shared,
        );

        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].client_order_id, "taker-coid");
        assert_eq!(updates[0].liquidity, Some(Liquidity::Taker));
        assert_eq!(updates[0].exchange_order_id.as_deref(), Some("AABB"));
        assert_eq!(
            shared
                .account_state
                .order("legacy-coid")
                .unwrap()
                .filled_quantity,
            0.0,
        );
    }

    #[test]
    fn one_trade_routes_each_owned_maker_leg_to_its_instance() {
        let shared = test_shared();
        shared.account_state.register_instance("maker-a", 1.0);
        shared.account_state.register_instance("maker-b", 1.0);
        shared
            .account_state
            .apply_physical_snapshot(200.0, HashMap::new());
        shared
            .account_state
            .reserve_order(
                "maker-a",
                "maker-a-coid",
                "0xAa01",
                "TOKEN",
                Side::Buy,
                5.0,
                0.4,
                0,
            )
            .unwrap();
        shared
            .account_state
            .reserve_order(
                "maker-b",
                "maker-b-coid",
                "0xAa02",
                "TOKEN",
                Side::Buy,
                6.0,
                0.4,
                0,
            )
            .unwrap();
        shared.register_order_id("maker-a-coid", "0xAa01", "TOKEN");
        shared.register_order_id("maker-b-coid", "0xAa02", "TOKEN");
        let maker = shared.order_maker_address.clone();

        let mut updates = parse_user_event(
            &serde_json::json!({
                "event_type": "trade",
                "id": "trade-two-maker-legs",
                "status": "MATCHED",
                "asset_id": "OTHER",
                "side": "SELL",
                "size": "11",
                "price": "0.6",
                "taker_order_id": "0xsomeone-else",
                "maker_orders": [
                    {
                        "maker_address": maker,
                        "asset_id": "TOKEN",
                        "side": "BUY",
                        "matched_amount": "5",
                        "price": "0.4",
                        "order_id": "AA01"
                    },
                    {
                        "maker_address": shared.order_maker_address.clone(),
                        "asset_id": "TOKEN",
                        "side": "BUY",
                        "matched_amount": "6",
                        "price": "0.4",
                        "order_id": "0xaa02"
                    }
                ]
            }),
            &shared,
        );
        updates.sort_by(|a, b| a.client_order_id.cmp(&b.client_order_id));

        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].client_order_id, "maker-a-coid");
        assert_eq!(updates[1].client_order_id, "maker-b-coid");
        assert!(updates
            .iter()
            .all(|update| update.liquidity == Some(Liquidity::Maker)));
        assert_eq!(
            updates[0].trade_id.as_deref(),
            Some("trade-two-maker-legs:aa01")
        );
        assert_eq!(
            updates[1].trade_id.as_deref(),
            Some("trade-two-maker-legs:aa02")
        );
        assert_eq!(
            shared
                .account_state
                .order("maker-a-coid")
                .unwrap()
                .filled_quantity,
            5.0,
        );
        assert_eq!(
            shared
                .account_state
                .order("maker-b-coid")
                .unwrap()
                .filled_quantity,
            6.0,
        );

        let before_a = shared.account_state.instance_snapshot("maker-a").unwrap();
        let before_b = shared.account_state.instance_snapshot("maker-b").unwrap();
        let replay_updates = parse_user_event(
            &serde_json::json!({
                "event_type": "trade",
                "id": "trade-two-maker-legs",
                "status": "MINED",
                "asset_id": "OTHER",
                "side": "SELL",
                "size": "11",
                "price": "0.6",
                "taker_order_id": "0xsomeone-else",
                "maker_orders": [
                    {
                        "maker_address": shared.order_maker_address.clone(),
                        "asset_id": "TOKEN",
                        "side": "BUY",
                        "matched_amount": "5",
                        "price": "0.4",
                        "order_id": "0xaa01"
                    },
                    {
                        "maker_address": shared.order_maker_address.clone(),
                        "asset_id": "TOKEN",
                        "side": "BUY",
                        "matched_amount": "6",
                        "price": "0.4",
                        "order_id": "AA02"
                    }
                ]
            }),
            &shared,
        );
        assert_eq!(replay_updates.len(), 2);
        assert_eq!(
            shared.account_state.instance_snapshot("maker-a").unwrap().cash,
            before_a.cash,
            "a casing/prefix-only lifecycle replay must not book twice",
        );
        assert_eq!(
            shared.account_state.instance_snapshot("maker-b").unwrap().cash,
            before_b.cash,
            "a casing/prefix-only lifecycle replay must not book twice",
        );
    }

    #[test]
    fn same_token_maker_and_taker_trades_stay_with_their_instances() {
        let shared = test_shared();
        shared.account_state.register_instance("maker-owner", 1.0);
        shared.account_state.register_instance("taker-owner", 1.0);
        shared
            .account_state
            .apply_physical_snapshot(200.0, HashMap::new());
        shared
            .account_state
            .register_token_fee_config(&["TOKEN".to_string()], 0.0, 1.0)
            .unwrap();
        for (instance, coid, oid) in [
            ("maker-owner", "maker-coid", "0xB001"),
            ("taker-owner", "taker-coid", "0xB002"),
        ] {
            shared
                .account_state
                .reserve_order(instance, coid, oid, "TOKEN", Side::Buy, 4.0, 0.5, 0)
                .unwrap();
            shared.register_order_id(coid, oid, "TOKEN");
        }

        let maker_updates = parse_user_event(
            &serde_json::json!({
                "event_type": "trade",
                "id": "trade-maker-owner",
                "status": "MATCHED",
                "asset_id": "OTHER",
                "side": "SELL",
                "size": "4",
                "price": "0.5",
                "taker_order_id": "other-taker",
                "maker_orders": [{
                    "maker_address": shared.order_maker_address.clone(),
                    "asset_id": "TOKEN",
                    "side": "BUY",
                    "matched_amount": "4",
                    "price": "0.5",
                    "order_id": "b001"
                }]
            }),
            &shared,
        );
        let taker_updates = parse_user_event(
            &serde_json::json!({
                "event_type": "trade",
                "id": "trade-taker-owner",
                "status": "MATCHED",
                "asset_id": "TOKEN",
                "side": "BUY",
                "size": "4",
                "price": "0.5",
                "taker_order_id": "B002",
                "maker_orders": [{
                    "maker_address": "0x0000000000000000000000000000000000000002",
                    "asset_id": "OTHER",
                    "side": "SELL",
                    "matched_amount": "4",
                    "price": "0.5",
                    "order_id": "other-maker"
                }]
            }),
            &shared,
        );

        assert_eq!(maker_updates.len(), 1);
        assert_eq!(maker_updates[0].client_order_id, "maker-coid");
        assert_eq!(maker_updates[0].liquidity, Some(Liquidity::Maker));
        assert_eq!(taker_updates.len(), 1);
        assert_eq!(taker_updates[0].client_order_id, "taker-coid");
        assert_eq!(taker_updates[0].liquidity, Some(Liquidity::Taker));
    }

    #[test]
    fn filled_order_ack_holds_reservation_until_private_trade_audits_it() {
        let trade = super::super::trade::PolymarketTrade::new(
            "api-key",
            "c2VjcmV0",
            "passphrase",
            "0x0000000000000000000000000000000000000000000000000000000000000001",
            false,
            10,
            super::super::signer::SignatureType::Eoa,
        )
        .unwrap();
        let shared = trade.shared_state();
        shared.account_state.register_instance("owner", 1.0);
        shared
            .account_state
            .apply_physical_snapshot(100.0, HashMap::new());
        shared
            .account_state
            .register_token_fee_config(&["TOKEN".to_string()], 0.0, 1.0)
            .unwrap();
        shared
            .account_state
            .reserve_order(
                "owner",
                "owner-1",
                "oid-final",
                "TOKEN",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();
        shared.open_orders.lock().unwrap().insert(
            "owner-1".to_string(),
            super::super::trade::TrackedOrder {
                symbol: "TOKEN".to_string(),
                side: Side::Buy,
                instance_id: "owner".to_string(),
            },
        );
        shared.register_order_id("owner-1", "oid-final", "TOKEN");

        shared.remove_order_as("owner-1", OrderStatus::Filled);
        assert!(shared.open_orders.lock().unwrap().contains_key("owner-1"));
        assert_eq!(
            shared.account_state.instance_snapshot("owner").unwrap().reserved_cash,
            5.0,
        );
        assert_eq!(shared.account_state.monitoring_snapshot().recovery_pending_orders, 1);

        let updates = parse_user_event(
            &serde_json::json!({
                "event_type": "trade",
                "id": "trade-filled-audit",
                "status": "MATCHED",
                "asset_id": "TOKEN",
                "side": "BUY",
                "size": "10",
                "price": "0.5",
                "taker_order_id": "oid-final",
                "maker_orders": [],
            }),
            &shared,
        );
        assert_eq!(updates.len(), 1);
        assert!(!shared.open_orders.lock().unwrap().contains_key("owner-1"));
        assert_eq!(
            shared.account_state.instance_snapshot("owner").unwrap().reserved_cash,
            0.0,
        );
        assert_eq!(shared.account_state.monitoring_snapshot().recovery_pending_orders, 0);
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
    fn gap_cursor_continues_past_batch_boundaries_and_rejects_loops() {
        let mut cursor = String::new();
        let mut seen = HashSet::new();

        for page in 1..=75 {
            assert!(advance_gap_cursor(
                &mut cursor,
                &mut seen,
                format!("cursor-{page}"),
            )
            .unwrap());
        }
        assert_eq!(cursor, "cursor-75");
        assert!(advance_gap_cursor(
            &mut cursor,
            &mut seen,
            "cursor-75".to_string(),
        )
        .is_err());
        assert!(!advance_gap_cursor(
            &mut cursor,
            &mut seen,
            "LTE=".to_string(),
        )
        .unwrap());
    }

    #[test]
    fn gap_replay_checkpoint_keeps_window_cursor_and_progress_for_retry() {
        let mut checkpoint = GapReplayCheckpoint::new(997);
        assert!(advance_gap_cursor(
            &mut checkpoint.cursor,
            &mut checkpoint.seen_cursors,
            "page-2".to_string(),
        ).unwrap());
        checkpoint.records = 100;
        checkpoint.pages = 1;

        // A later retry uses this same object, so wall time / last trade
        // changes cannot move the lower bound and page 1 is not fetched again.
        assert_eq!(checkpoint.after_secs, 997);
        assert_eq!(checkpoint.cursor, "page-2");
        assert_eq!(checkpoint.records, 100);
        assert_eq!(checkpoint.pages, 1);

        assert!(advance_gap_cursor(
            &mut checkpoint.cursor,
            &mut checkpoint.seen_cursors,
            "page-3".to_string(),
        ).unwrap());
        assert_eq!(checkpoint.cursor, "page-3");
    }

}
