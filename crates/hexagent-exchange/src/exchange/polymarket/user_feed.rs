//! Polymarket User WebSocket feed — receives real-time order/trade notifications.
//!
//! Async implementation (tokio + tokio-tungstenite). The public API returns
//! a `std::thread::JoinHandle` so the engine shutdown path is unchanged,
//! but under the hood the WS read loop runs as a tokio task on the shared
//! async runtime.

use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error as _;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use crossbeam_channel::Sender;
use futures_util::{SinkExt, StreamExt};
use hexagent_account::account::shared_account::normalize_order_id;
use log::{debug, info, warn};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;

use super::live_position::{LivePositionManager, TradeStatus};
use super::trade::{PolymarketTrade, SharedState};
use crate::async_rt;
use crate::types::*;

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/user";
const CLOB_BASE_URL: &str = "https://clob.polymarket.com";
const PING_INTERVAL: Duration = Duration::from_secs(10);
const READ_TIMEOUT: Duration = Duration::from_secs(2);
const STALE_TIMEOUT: Duration = Duration::from_secs(30);
const RECOVERY_DELIVERY_TIMEOUT: Duration = Duration::from_secs(5);
const GAP_USER_AGENT: &str = "hexbot-gap-replay/1";
const FAILED_TRADE_DIAGNOSTIC_CAPACITY: usize = 4096;
const TERMINAL_GAP_REPLAY_DEDUPE_CAPACITY: usize = 32_768;
const PERIODIC_GAP_RETRY_MAX_MS: u64 = 30_000;
const HEALTHY_GAP_REPLAY_ACTIVE_MS: u64 = 10_000;
const HEALTHY_GAP_REPLAY_IDLE_MS: u64 = 30_000;
const HEALTHY_GAP_REPLAY_ACTIVITY_WINDOW_NS: u64 = 30_000_000_000;
const PRIVATE_APPLY_QUEUE_CAPACITY: usize = 1024;
// A replay command executes as one uninterrupted account-owner turn. Keep the
// cold batch at one record so a long REST catch-up cannot hold the owner across
// many persistence/account transactions; the awaiting producer yields before
// it submits the next record, which gives owner-control snapshots and live
// lifecycle work an admission point between every historical event.
/// Bound one replay handoff so the private owner can service live traffic
/// between chunks. Durable high-water rows complete before reaching the cold
/// account actor; genuinely advancing rows retain ordered single-writer apply.
const GAP_APPLY_BATCH_SIZE: usize = 8;

fn periodic_gap_retry_delay(base: Duration, failures: u32) -> Duration {
    let base_ms = u64::try_from(base.as_millis()).unwrap_or(u64::MAX).max(1);
    let multiplier = 1u64.checked_shl(failures.min(63)).unwrap_or(u64::MAX);
    Duration::from_millis(
        base_ms
            .saturating_mul(multiplier)
            .min(PERIODIC_GAP_RETRY_MAX_MS),
    )
}

fn periodic_gap_failure_reminder(attempt: u32) -> bool {
    attempt >= 4 && attempt.is_power_of_two()
}

fn adaptive_healthy_gap_replay_delay(
    configured_fast: Duration,
    now_ns: u64,
    last_business_event_ns: u64,
) -> Duration {
    let recent_activity = last_business_event_ns > 0
        && now_ns.saturating_sub(last_business_event_ns) <= HEALTHY_GAP_REPLAY_ACTIVITY_WINDOW_NS;
    let healthy_floor_ms = if recent_activity {
        HEALTHY_GAP_REPLAY_ACTIVE_MS
    } else {
        HEALTHY_GAP_REPLAY_IDLE_MS
    };
    configured_fast.max(Duration::from_millis(healthy_floor_ms))
}

#[derive(Debug)]
struct FailedTradeDiagnosticDedupe {
    capacity: usize,
    seen: HashSet<String>,
    order: VecDeque<String>,
}

impl FailedTradeDiagnosticDedupe {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            seen: HashSet::new(),
            order: VecDeque::new(),
        }
    }

    fn admit(&mut self, venue_trade_id: &str) -> bool {
        if venue_trade_id.is_empty() || !self.seen.insert(venue_trade_id.to_string()) {
            return false;
        }
        self.order.push_back(venue_trade_id.to_string());
        while self.order.len() > self.capacity {
            if let Some(expired) = self.order.pop_front() {
                self.seen.remove(&expired);
            }
        }
        true
    }
}

#[derive(Debug)]
struct GapReplayLifecycleDedupe {
    capacity: usize,
    ranks: HashMap<String, u8>,
    order: VecDeque<(String, u8)>,
}

impl GapReplayLifecycleDedupe {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            ranks: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn seen(&self, trade_id: &str, rank: u8) -> bool {
        self.ranks
            .get(trade_id)
            .is_some_and(|existing| *existing >= rank)
    }

    fn remember(&mut self, trade_id: &str, rank: u8) {
        if trade_id.is_empty() || self.seen(trade_id, rank) {
            return;
        }
        self.ranks.insert(trade_id.to_string(), rank);
        self.order.push_back((trade_id.to_string(), rank));
        while self.order.len() > self.capacity {
            if let Some((expired, expired_rank)) = self.order.pop_front() {
                if self.ranks.get(&expired) == Some(&expired_rank) {
                    self.ranks.remove(&expired);
                }
            }
        }
    }
}

fn trade_lifecycle_identity(payload: &serde_json::Value) -> Option<(&str, u8)> {
    let trade_id = payload
        .get("id")
        .or_else(|| payload.get("trade_id"))
        .and_then(serde_json::Value::as_str)?
        .trim();
    if trade_id.is_empty() {
        return None;
    }
    let status = payload
        .get("status")
        .and_then(serde_json::Value::as_str)?
        .trim_start_matches("TRADE_STATUS_");
    let rank = match status {
        "MATCHED" | "MATCHED_NOT_BROADCASTED" => 1,
        "MINED" => 2,
        "CONFIRMED" => 3,
        "FAILED" => 4,
        _ => return None,
    };
    Some((trade_id, rank))
}

/// Mutable replay and diagnostic suppression owned exclusively by the
/// physical account owner thread. No process-global map or mutex participates
/// in authenticated private lifecycle processing.
pub(crate) struct PrivateReplayOwner {
    failed_diagnostics: FailedTradeDiagnosticDedupe,
    terminal: FailedTradeDiagnosticDedupe,
    lifecycle: GapReplayLifecycleDedupe,
}

impl PrivateReplayOwner {
    pub(crate) fn new() -> Self {
        Self {
            failed_diagnostics: FailedTradeDiagnosticDedupe::new(FAILED_TRADE_DIAGNOSTIC_CAPACITY),
            terminal: FailedTradeDiagnosticDedupe::new(TERMINAL_GAP_REPLAY_DEDUPE_CAPACITY),
            lifecycle: GapReplayLifecycleDedupe::new(TERMINAL_GAP_REPLAY_DEDUPE_CAPACITY),
        }
    }

    fn admit_failed_diagnostic(&mut self, venue_trade_id: &str) -> bool {
        self.failed_diagnostics.admit(venue_trade_id)
    }

    fn terminal_seen(&self, key: &str) -> bool {
        self.terminal.seen.contains(key)
    }

    fn remember_terminal(&mut self, key: &str) {
        self.terminal.admit(key);
    }

    fn lifecycle_seen(&self, payload: &serde_json::Value) -> bool {
        trade_lifecycle_identity(payload)
            .is_some_and(|(trade_id, rank)| self.lifecycle.seen(trade_id, rank))
    }

    fn remember_lifecycle(&mut self, payload: &serde_json::Value) {
        if let Some((trade_id, rank)) = trade_lifecycle_identity(payload) {
            self.lifecycle.remember(trade_id, rank);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GapReplayOutcome {
    Complete { records: usize },
}

/// In-memory progress for one authenticated `/data/trades` pagination sweep.
/// Retaining this value across a transient failure makes the retry request the
/// exact failed page instead of starting again at the original `after`.
#[derive(Debug, Clone)]
struct GapReplayCheckpoint {
    after_secs: u64,
    cursor: String,
    seen_cursors: HashSet<String>,
    /// Decoded remainder of the current REST page. It is retained across a
    /// reconnect/apply failure so the retry resumes at the exact failed record
    /// rather than fetching and replaying the already-committed page prefix.
    pending_page: VecDeque<PrivateEventDelta>,
    pending_page_records: usize,
    pending_next_cursor: Option<String>,
    records: usize,
    pages: usize,
    cursor_resets: usize,
    durable_fast_skips: usize,
}

impl GapReplayCheckpoint {
    fn new(after_secs: u64) -> Self {
        Self {
            after_secs,
            cursor: String::new(),
            seen_cursors: HashSet::new(),
            pending_page: VecDeque::new(),
            pending_page_records: 0,
            pending_next_cursor: None,
            records: 0,
            pages: 0,
            cursor_resets: 0,
            durable_fast_skips: 0,
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
fn accept_reconnect_replay(shared: &SharedState, _outcome: GapReplayOutcome) {
    shared.user_feed_health.set_recovering(false);
    if !shared.account_state.is_uncertain() {
        shared.user_feed_health.set_inventory_uncertain(false);
    }
}

fn enqueue_recovery_update(
    shared: &SharedState,
    update_tx: &Sender<RoutedOrderUpdate>,
    generation: u64,
    update: OrderUpdate,
) -> Result<()> {
    let owner = shared
        .account_state
        .order_owner_by_coid(&update.client_order_id)
        .ok_or_else(|| {
            anyhow!(
                "recovery update coid={} has no durable owner",
                update.client_order_id,
            )
        })?;
    let numeric_owner = shared
        .strategy_owner(&owner)
        .ok_or_else(|| anyhow!("recovery update instance={owner} has no numeric strategy owner"))?;
    let buffered = shared
        .user_feed_health
        .register_recovery_update(generation, &owner, &update)
        .map_err(|error| anyhow!(error))?;
    if buffered {
        Ok(())
    } else {
        update_tx
            .send(RoutedOrderUpdate {
                owner: numeric_owner,
                update,
            })
            .map_err(|_| anyhow!("order update channel closed during reconnect recovery"))
    }
}

async fn wait_for_recovery_delivery(
    shared: &SharedState,
    generation: u64,
    update_tx: &Sender<RoutedOrderUpdate>,
    shutdown: &AtomicBool,
) -> Result<()> {
    let mut ready_started = None;
    let mut startup_buffer_drained = false;
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Err(anyhow!(
                "shutdown while waiting for reconnect updates to drain"
            ));
        }
        let consumer_ready = shared.user_feed_health.strategy_consumer_ready();
        if consumer_ready {
            if ready_started.is_none() {
                ready_started = Some(Instant::now());
            }
            if !startup_buffer_drained {
                let buffered = shared
                    .user_feed_health
                    .take_startup_recovery_updates(generation)
                    .map_err(|error| anyhow!(error))?;
                let buffered_count = buffered.len();
                for update in buffered {
                    let instance_id = shared
                        .account_state
                        .order_owner_by_coid(&update.client_order_id)
                        .ok_or_else(|| {
                            anyhow!(
                                "startup recovery update coid={} has no durable owner",
                                update.client_order_id,
                            )
                        })?;
                    let owner = shared.strategy_owner(&instance_id).ok_or_else(|| {
                        anyhow!(
                            "startup recovery instance={instance_id} has no numeric strategy owner"
                        )
                    })?;
                    update_tx
                        .send(RoutedOrderUpdate { owner, update })
                        .map_err(|_| {
                            anyhow!("order update channel closed while draining startup recovery")
                        })?;
                }
                if buffered_count > 0 {
                    info!(
                        "[PolyUserFeed] released {} bounded startup replay update(s) to strategy consumer",
                        buffered_count,
                    );
                }
                startup_buffer_drained = true;
            }
        }
        let Some((enrollment_finished, pending)) = shared
            .user_feed_health
            .recovery_delivery_progress(generation)
        else {
            return Err(anyhow!(
                "reconnect delivery generation {generation} was superseded",
            ));
        };
        if consumer_ready && enrollment_finished && pending == 0 {
            return Ok(());
        }
        if ready_started.is_some_and(|started| started.elapsed() >= RECOVERY_DELIVERY_TIMEOUT) {
            return Err(anyhow!(
                "timed out after {}ms waiting for {pending} replay update(s) to be processed",
                RECOVERY_DELIVERY_TIMEOUT.as_millis(),
            ));
        }
        if consumer_ready {
            sleep(Duration::from_millis(1)).await;
        } else {
            // Predictor/APV2 construction can take tens of seconds. Sleep on
            // the one-shot publication edge instead of waking the private
            // runtime every millisecond throughout startup; retain a bounded
            // timeout only so shutdown is still observed promptly.
            tokio::select! {
                _ = shared.user_feed_health.wait_for_strategy_consumer_ready() => {}
                _ = sleep(Duration::from_millis(100)) => {}
            }
        }
    }
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
        return Err(anyhow!(
            "Gap-fetch /data/trades returned repeated cursor `{next}`"
        ));
    }
    *cursor = next;
    Ok(true)
}

fn replay_match_time_anchor(shared: &SharedState, committed_secs: u64) -> u64 {
    match shared.account_state.earliest_unresolved_trade_match_time() {
        Some(unresolved) if committed_secs > 0 => committed_secs.min(unresolved),
        Some(unresolved) => unresolved,
        None => committed_secs,
    }
}

fn update_trade_finality_replay_anchor(
    shared: &SharedState,
    trade_key: &str,
    status: &str,
    match_time_secs: u64,
) {
    let started = crate::latency::Instant::now();
    let normalized = status
        .trim_start_matches("TRADE_STATUS_")
        .to_ascii_uppercase();
    if matches!(normalized.as_str(), "MATCHED" | "MATCHED_NOT_BROADCASTED") {
        shared
            .account_state
            .mark_unresolved_trade_match_time(trade_key, match_time_secs);
    } else {
        shared
            .account_state
            .resolve_unresolved_trade_match_time(trade_key);
    }
    crate::latency::record("polymarket.user.trade_replay_anchor_apply", started);
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
        Self {
            slot,
            generation,
            kind,
            detail,
        }
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
            self.slot, self.generation, self.kind, self.detail,
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
            None => {
                return Err(GapAttemptFailure {
                    failure: GapSendFailure::new(
                        usize::MAX,
                        0,
                        "pool_busy",
                        "no GapReplay warm slot available".to_string(),
                    ),
                    _permit: None,
                })
            }
        };
        let slot = permit.slot();
        let generation = permit.generation();
        let client = permit.pooled_client();
        let headers = shared.auth.sign_request("GET", "/data/trades", "");
        let mut request = client
            .client()
            .get(url)
            .header("User-Agent", GAP_USER_AGENT);
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
                    Err(second) => Err(format!(
                        "primary [{}]; fallback [{}]",
                        first.failure, second.failure,
                    )),
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
    live_position: &mut LivePositionManager,
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
        || price > 1.0 + 1e-8
    {
        return false;
    }
    live_position.update_trade(
        trade_key, status, asset_id, side, size, price, is_maker, reason,
    )
}

fn required_string<'a>(
    data: &'a serde_json::Value,
    keys: &[&str],
    field: &str,
) -> std::result::Result<&'a str, String> {
    keys.iter()
        .find_map(|key| {
            data.get(*key)
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
        })
        .ok_or_else(|| format!("trade field `{field}` is missing or empty"))
}

fn strict_number(
    value: Option<&serde_json::Value>,
    field: &str,
) -> std::result::Result<f64, String> {
    let parsed = match value {
        Some(serde_json::Value::String(value)) => value
            .trim()
            .parse::<f64>()
            .map_err(|_| format!("trade field `{field}` is not a finite number"))?,
        Some(serde_json::Value::Number(value)) => value
            .as_f64()
            .ok_or_else(|| format!("trade field `{field}` is not representable as f64"))?,
        _ => return Err(format!("trade field `{field}` is missing or not numeric")),
    };
    if !parsed.is_finite() {
        return Err(format!("trade field `{field}` is not finite"));
    }
    Ok(parsed)
}

fn strict_side(value: &str, field: &str) -> std::result::Result<Side, String> {
    match value.trim().to_ascii_uppercase().as_str() {
        "BUY" => Ok(Side::Buy),
        "SELL" => Ok(Side::Sell),
        _ => Err(format!("trade field `{field}` has invalid side `{value}`")),
    }
}

fn validate_quantity_price(
    quantity: f64,
    price: f64,
    quantity_field: &str,
    price_field: &str,
) -> std::result::Result<(), String> {
    if quantity <= 0.0 {
        return Err(format!("trade field `{quantity_field}` must be positive"));
    }
    let tolerance = 1e-10_f64.max(price.abs() * 1e-8);
    if price <= 0.0 || price > 1.0 + tolerance {
        return Err(format!("trade field `{price_field}` is outside (0, 1]"));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PrivateTradeRole {
    Maker,
    Taker,
}

fn taker_order_id(data: &serde_json::Value) -> Option<&str> {
    data.get("taker_order_id")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            data.get("order_id")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
        })
        .or_else(|| {
            data.get("orderID")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
        })
}

fn top_level_maker_matches_account(data: &serde_json::Value, shared: &SharedState) -> bool {
    data.get("maker_address")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|address| address.eq_ignore_ascii_case(&shared.order_maker_address))
}

fn classify_private_trade_role(
    data: &serde_json::Value,
    shared: &SharedState,
) -> std::result::Result<PrivateTradeRole, String> {
    let maker_orders = match data.get("maker_orders") {
        None => None,
        Some(serde_json::Value::Array(orders)) => Some(orders),
        Some(_) => return Err("trade field `maker_orders` must be an array".to_string()),
    };
    if maker_orders.is_some_and(|orders| orders.iter().any(|order| !order.is_object())) {
        return Err("trade field `maker_orders` contains a non-object leg".to_string());
    }
    let has_owned_maker_leg = maker_orders.is_some_and(|orders| {
        orders.iter().any(|order| {
            order
                .get("maker_address")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|address| address.eq_ignore_ascii_case(&shared.order_maker_address))
        })
    });
    let taker_id = taker_order_id(data);
    let trade_id = data
        .get("id")
        .or_else(|| data.get("trade_id"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let has_owned_taker_order = top_level_maker_matches_account(data, shared)
        || taker_id.is_some_and(|order_id| {
            // The live owner route uses the OID ownership publication only.
            // A durable trade lookup takes an instance lifecycle lock and is
            // needed solely for a compacted historical taker replay. In
            // particular, never perform that cold lookup for a maker event:
            // its top-level taker order normally belongs to another account.
            let owned_order = if has_owned_maker_leg {
                shared.has_runtime_order_ownership(order_id)
            } else {
                shared.lookup_order_ownership(order_id).is_some()
            };
            owned_order
                || (!has_owned_maker_leg
                    && trade_id
                        .and_then(|trade_key| {
                            let started = crate::latency::Instant::now();
                            let ownership = shared.account_state.trade_ownership(trade_key);
                            crate::latency::record(
                                "polymarket.user.owner_durable_fallback",
                                started,
                            );
                            ownership
                        })
                        .is_some_and(|ownership| {
                            normalize_order_id(&ownership.order_id) == normalize_order_id(order_id)
                        }))
        });
    match (has_owned_maker_leg, has_owned_taker_order) {
        (true, false) => Ok(PrivateTradeRole::Maker),
        (false, true) => Ok(PrivateTradeRole::Taker),
        (true, true) => Err(
            "trade payload is ambiguous: both maker leg and taker order belong to this account"
                .to_string(),
        ),
        (false, false) => Err(format!(
            "trade maker/taker role is unknown: no owned maker leg and taker order `{}` is not owned",
            taker_id.unwrap_or("<missing>"),
        )),
    }
}

/// Validate every role-specific field before booking any maker leg, avoiding a
/// partially-applied multi-leg trade when a later leg is malformed.
fn validate_trade_event(
    data: &serde_json::Value,
    shared: &SharedState,
) -> std::result::Result<PrivateTradeRole, String> {
    required_string(data, &["id", "trade_id"], "id")?;
    let status = required_string(data, &["status"], "status")?;
    let status = status.strip_prefix("TRADE_STATUS_").unwrap_or(status);
    if !matches!(
        status,
        "MATCHED" | "MATCHED_NOT_BROADCASTED" | "MINED" | "CONFIRMED" | "FAILED" | "RETRYING"
    ) {
        return Err(format!(
            "trade field `status` has unsupported value `{status}`"
        ));
    }
    let role = classify_private_trade_role(data, shared)?;
    match role {
        PrivateTradeRole::Taker => {
            strict_side(required_string(data, &["side"], "side")?, "side")?;
            required_string(data, &["asset_id", "token_id"], "asset_id")?;
            let quantity = strict_number(
                data.get("size").or_else(|| data.get("matched_amount")),
                "quantity",
            )?;
            let price = strict_number(data.get("price"), "price")?;
            validate_quantity_price(quantity, price, "quantity", "price")?;
            taker_order_id(data)
                .ok_or_else(|| "trade field `order_id` is missing or empty".to_string())?;
        }
        PrivateTradeRole::Maker => {
            for (index, order) in data
                .get("maker_orders")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter(|order| {
                    order
                        .get("maker_address")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|address| {
                            address.eq_ignore_ascii_case(&shared.order_maker_address)
                        })
                })
                .enumerate()
            {
                // Keep success-path validation allocation-free. The leg index
                // is added only when malformed input needs an error string.
                let validate = || -> std::result::Result<(), String> {
                    required_string(order, &["order_id"], "maker order_id")?;
                    required_string(order, &["asset_id"], "maker asset_id")?;
                    strict_side(
                        required_string(order, &["side"], "maker side")?,
                        "maker side",
                    )?;
                    let quantity =
                        strict_number(order.get("matched_amount"), "maker matched_amount")?;
                    let price = strict_number(order.get("price"), "maker price")?;
                    validate_quantity_price(
                        quantity,
                        price,
                        "maker matched_amount",
                        "maker price",
                    )?;
                    Ok(())
                };
                validate().map_err(|error| format!("maker_orders[{index}]: {error}"))?;
            }
        }
    }
    Ok(role)
}

fn close_enough(left: f64, right: f64) -> bool {
    (left - right).abs() <= 1e-9_f64.max(left.abs().max(right.abs()) * 1e-8)
}

fn parse_order_event(
    data: &serde_json::Value,
    shared: &SharedState,
) -> std::result::Result<Vec<OrderUpdate>, String> {
    let order_id = required_string(data, &["order_id", "orderID", "id"], "order_id")?;
    if shared.probe_order_ids.contains(order_id) {
        debug!("[PolyUserFeed] probe order lifecycle push muted: oid={order_id}");
        return Ok(Vec::new());
    }
    let asset_id = required_string(data, &["asset_id", "token_id"], "asset_id")?;
    let side = strict_side(required_string(data, &["side"], "side")?, "side")?;
    let price = strict_number(data.get("price"), "price")?;
    let original_size = strict_number(
        data.get("original_size").or_else(|| data.get("size")),
        "original_size",
    )?;
    let size_matched = strict_number(data.get("size_matched"), "size_matched")?;
    let tolerance = 1e-9_f64.max(original_size.abs() * 1e-8);
    if original_size <= 0.0
        || size_matched < 0.0
        || size_matched > original_size + tolerance
        || price <= 0.0
        || price > 1.0 + 1e-8
    {
        return Err(format!(
            "invalid order lifecycle economics order_id={order_id}"
        ));
    }
    let retired_audit_started = crate::latency::Instant::now();
    let retired_audit_covers =
        shared
            .account_state
            .retired_order_audit_covers(order_id, original_size, size_matched);
    crate::latency::record(
        "polymarket.user.retired_order_audit_lookup",
        retired_audit_started,
    );
    if retired_audit_covers {
        shared
            .account_state
            .resolve_private_event_anomaly(&format!("order:{}", normalize_order_id(order_id)));
        debug!("[PolyUserFeed] retired zero-fill order lifecycle replay ignored: oid={order_id}");
        return Ok(Vec::new());
    }
    let coid = shared.lookup_coid(order_id).ok_or_else(|| {
        format!("unowned private order lifecycle event for order_id `{order_id}`")
    })?;
    // Cold account application needs the ledger's advancing lifecycle status;
    // the fast owner route intentionally uses the immutable runtime mirror.
    let reconcile_started = crate::latency::Instant::now();
    let ownership = shared
        .account_state
        .reconcile_order_route(&coid, order_id)
        .or_else(|| shared.lookup_order_ownership(order_id))
        .ok_or_else(|| {
            format!("order lifecycle event has runtime mapping but no ledger row coid `{coid}`")
        })?;
    crate::latency::record(
        "polymarket.user.account_order_reconcile_route",
        reconcile_started,
    );
    if normalize_order_id(&ownership.order_id) != normalize_order_id(order_id)
        || ownership.token_id != asset_id
        || ownership.side != side
        || !close_enough(ownership.quantity, original_size)
        || !close_enough(ownership.price, price)
    {
        return Err(format!(
            "order lifecycle invariant mismatch coid={coid} order_id={order_id}"
        ));
    }
    let lifecycle = required_string(data, &["type"], "type")?.to_ascii_uppercase();
    let status = match lifecycle.as_str() {
        "PLACEMENT" => OrderStatus::Accepted,
        "UPDATE" if size_matched + tolerance >= original_size => OrderStatus::Filled,
        "UPDATE" if size_matched > tolerance => OrderStatus::PartiallyFilled,
        "UPDATE" => OrderStatus::Accepted,
        "CANCELLATION" | "CANCELLED" | "CANCELED" => OrderStatus::Cancelled,
        _ => return Err(format!("unsupported order lifecycle type `{lifecycle}`")),
    };
    if ownership.status == OrderStatus::Failed
        || (ownership.status == OrderStatus::Filled
            && matches!(status, OrderStatus::Accepted | OrderStatus::PartiallyFilled))
        || (ownership.status == OrderStatus::Cancelled && status == OrderStatus::PartiallyFilled)
        || (ownership.status == OrderStatus::PartiallyFilled && status == OrderStatus::Accepted)
    {
        debug!(
            "[PolyUserFeed] stale order lifecycle regression ignored: coid={} current={:?} incoming={:?}",
            coid, ownership.status, status,
        );
        return Ok(Vec::new());
    }
    let associate_trades = match data.get("associate_trades") {
        None => Vec::new(),
        Some(value) => {
            let values = value.as_array().ok_or_else(|| {
                format!("order lifecycle associate_trades is not an array order_id={order_id}")
            })?;
            let mut seen = std::collections::HashSet::with_capacity(values.len());
            let mut trades = Vec::with_capacity(values.len());
            for value in values {
                let trade_id = value
                    .as_str()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| format!(
                        "order lifecycle associate_trades contains a non-string/empty id order_id={order_id}"
                    ))?;
                if !seen.insert(trade_id.to_string()) {
                    return Err(format!(
                        "order lifecycle associate_trades contains duplicate id `{trade_id}` order_id={order_id}"
                    ));
                }
                trades.push(trade_id.to_string());
            }
            trades
        }
    };
    if size_matched > tolerance && associate_trades.is_empty() {
        return Err(format!(
            "matched order lifecycle is missing associate_trades order_id={order_id} size_matched={size_matched}"
        ));
    }
    let order_audit = AuthoritativeOrderAudit {
        original_size: Some(original_size.to_string()),
        size_matched: Some(size_matched.to_string()),
        associate_trades,
    };
    let lifecycle_started = crate::latency::Instant::now();
    match status {
        OrderStatus::Filled | OrderStatus::Cancelled => {
            shared.commit_authoritative_terminal_audit(&coid, status, &order_audit);
        }
        OrderStatus::Accepted | OrderStatus::PartiallyFilled => {
            shared.mark_order_live(
                &coid,
                shared
                    .execution_snapshot()
                    .open_orders
                    .get(&coid)
                    .map(|tracked| tracked.order_slot)
                    .unwrap_or_default(),
                &ownership.token_id,
                ownership.side,
                &ownership.instance_id,
                status,
            );
            // A strict order lifecycle row is authoritative enough to clear
            // the recovery gate installed for a malformed push.
            shared.account_state.finish_order_recovery(&coid);
        }
        _ => shared.account_state.mark_order_status(&coid, status),
    }
    crate::latency::record(
        if matches!(status, OrderStatus::Filled | OrderStatus::Cancelled) {
            "polymarket.user.account_order_lifecycle_commit.terminal"
        } else {
            "polymarket.user.account_order_lifecycle_commit.live"
        },
        lifecycle_started,
    );
    crate::latency::record(
        "polymarket.user.account_order_lifecycle_commit",
        lifecycle_started,
    );
    let update = OrderUpdate {
        order_slot: ownership.order_slot,
        client_order_id: coid,
        exchange: Exchange::Polymarket,
        symbol: asset_id.to_string(),
        side,
        exchange_order_id: Some(order_id.to_string()),
        status,
        liquidity: None,
        // Inventory remains exclusively trade-driven.
        filled_quantity: 0.0,
        remaining_quantity: (original_size - size_matched).max(0.0),
        avg_fill_price: price,
        timestamp_ns: now_ns(),
        exchange_event_timestamp_ns: None,
        trade_id: None,
        order_audit: Some(order_audit),
        error: None,
    };
    let log_started = crate::latency::Instant::now();
    shared.log_order_lifecycle(
        &update.client_order_id,
        "private_order",
        update.exchange_order_id.as_deref(),
        Some(update.status),
        None,
    );
    crate::latency::record("polymarket.user.account_order_log", log_started);
    Ok(vec![update])
}

fn invalid_payload_key(data: &serde_json::Value) -> String {
    let is_trade = data
        .get("event_type")
        .or_else(|| data.get("type"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|kind| kind == "trade");
    if is_trade {
        if let Some(id) = data
            .get("trade_id")
            .or_else(|| data.get("id"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|id| !id.is_empty())
        {
            return format!("trade:{id}");
        }
    }
    if let Some(id) = data
        .get("order_id")
        .or_else(|| data.get("orderID"))
        .or_else(|| data.get("id"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|id| !id.is_empty())
    {
        return format!("order:{}", normalize_order_id(id));
    }
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in data.to_string().bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("invalid-payload:{hash:016x}")
}

fn invalid_replay_anchor_key(data: &serde_json::Value) -> String {
    let is_trade = data
        .get("event_type")
        .or_else(|| data.get("type"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|kind| kind == "trade");
    if is_trade {
        if let Some(id) = data
            .get("trade_id")
            .or_else(|| data.get("id"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|id| !id.is_empty())
        {
            // Successful taker attribution clears the raw trade id, and maker
            // attribution creates more-specific `{trade_id}:{order_id}` keys.
            return id.to_string();
        }
    }
    if let Some(id) = data
        .get("order_id")
        .or_else(|| data.get("orderID"))
        .or_else(|| data.get("id"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|id| !id.is_empty())
    {
        return format!("private-order:{}", normalize_order_id(id));
    }
    invalid_payload_key(data)
}

fn receipt_time_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(1)
        .max(1)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EffectiveMatchTime {
    /// Upstream business timestamp retained in the durable trade audit.
    business_secs: u64,
    /// Conservative REST replay watermark. A future business timestamp must
    /// never move the `after=` lower bound beyond receipt time.
    replay_watermark_secs: u64,
}

fn effective_match_time(data: &serde_json::Value, trade_id: &str) -> EffectiveMatchTime {
    let now = receipt_time_secs();
    let parsed = data
        .get("match_time")
        .and_then(|value| {
            value
                .as_str()
                .and_then(|text| text.parse::<u64>().ok())
                .or_else(|| value.as_u64())
        })
        .filter(|value| *value > 0);
    if parsed.is_none() {
        warn!("[PolyUserFeed] trade={} has missing/invalid match_time; pinning replay at receipt_time={}", trade_id, now);
    } else if parsed.is_some_and(|value| value > now) {
        warn!(
            "[PolyUserFeed] trade={} has future business match_time={}; retaining audit timestamp but capping replay watermark at receipt_time={}",
            trade_id, parsed.unwrap_or(now), now,
        );
    }
    let business_secs = parsed.unwrap_or(now);
    EffectiveMatchTime {
        business_secs,
        replay_watermark_secs: business_secs.min(now),
    }
}

fn exchange_event_timestamp_ns(data: &serde_json::Value) -> Option<u64> {
    let raw = ["timestamp", "last_update", "match_time"]
        .into_iter()
        .find_map(|key| {
            data.get(key).and_then(|value| {
                value
                    .as_u64()
                    .or_else(|| value.as_str().and_then(|text| text.parse::<u64>().ok()))
            })
        })?;
    let timestamp_ns = match raw {
        100_000_000_000_000_000.. => raw,
        100_000_000_000_000.. => raw.saturating_mul(1_000),
        100_000_000_000.. => raw.saturating_mul(1_000_000),
        1_000_000_000.. => raw.saturating_mul(1_000_000_000),
        _ => return None,
    };
    let receipt_ns = now_ns();
    let maximum_future_skew_ns = Duration::from_secs(300).as_nanos() as u64;
    (timestamp_ns <= receipt_ns.saturating_add(maximum_future_skew_ns)).then_some(timestamp_ns)
}

fn flag_invalid_private_event(data: &serde_json::Value, shared: &SharedState, error: &str) {
    let key = invalid_payload_key(data);
    let reason = format!("invalid Polymarket private event `{key}`: {error}");
    let event_type = data
        .get("event_type")
        .or_else(|| data.get("type"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    if event_type == "trade" {
        let replay_key = invalid_replay_anchor_key(data);
        let match_time = effective_match_time(data, &key);
        shared
            .account_state
            .mark_unresolved_trade_match_time(&replay_key, match_time.replay_watermark_secs);
    } else if event_type == "order" {
        if let Some(order_id) = data
            .get("order_id")
            .or_else(|| data.get("orderID"))
            .or_else(|| data.get("id"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|order_id| !order_id.is_empty())
        {
            let coid = shared.lookup_coid(order_id);
            if let Some(coid) = coid.as_deref() {
                shared
                    .account_state
                    .begin_order_recovery(std::iter::once(coid));
            }
            let token_id = data
                .get("asset_id")
                .or_else(|| data.get("token_id"))
                .and_then(serde_json::Value::as_str);
            shared
                .account_state
                .mark_private_order_event_anomaly_with_token(
                    order_id,
                    coid.as_deref(),
                    token_id,
                    reason,
                );
            shared.user_feed_health.set_inventory_uncertain(true);
            warn!("[PolyUserFeed] rejecting invalid private event: {error}; raw={data}");
            return;
        }
    }
    shared
        .account_state
        .mark_private_event_anomaly(&key, reason);
    shared.user_feed_health.set_inventory_uncertain(true);
    warn!("[PolyUserFeed] rejecting invalid private event: {error}; raw={data}");
}

fn parse_user_event_checked(
    data: &serde_json::Value,
    shared: &SharedState,
    live_position: &mut LivePositionManager,
    replay: &mut PrivateReplayOwner,
) -> std::result::Result<Vec<OrderUpdate>, String> {
    let event_type = data
        .get("event_type")
        .or_else(|| data.get("type"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    match event_type {
        "order" => parse_order_event(data, shared),
        "trade" => {
            validate_trade_event(data, shared)?;
            let updates = parse_user_event_validated(data, shared, live_position, replay);
            Ok(updates)
        }
        _ => Ok(Vec::new()),
    }
}

#[derive(Debug)]
pub(crate) struct ParsedPrivateEvent {
    pub(crate) updates: Vec<OrderUpdate>,
    pub(crate) valid_business_event: bool,
    pub(crate) invalid_business_event: bool,
    /// Exact schema/ownership/invariant failure returned by the checked
    /// parser. `None` with a valid event and zero updates is an idempotent or
    /// non-advancing replay, not a parser rejection.
    pub(crate) rejection_reason: Option<String>,
}

#[derive(Debug, Clone)]
enum PrivateEventDelta {
    Order(serde_json::Value),
    Trade(serde_json::Value),
}

impl PrivateEventDelta {
    fn classify(payload: serde_json::Value) -> Option<Self> {
        match payload
            .get("event_type")
            .or_else(|| payload.get("type"))
            .and_then(serde_json::Value::as_str)
        {
            Some("order") => Some(Self::Order(payload)),
            Some("trade") => Some(Self::Trade(payload)),
            _ => None,
        }
    }

    fn payload(&self) -> &serde_json::Value {
        match self {
            Self::Order(payload) | Self::Trade(payload) => payload,
        }
    }

    /// Synthetic RTT order pushes are operational measurements, not strategy
    /// lifecycle events.  Remove them on the websocket parser thread before
    /// they consume either the lossless owner lane or the cold account lane.
    fn is_registered_probe_order(&self, shared: &SharedState) -> bool {
        let Self::Order(payload) = self else {
            return false;
        };
        let Some(order_id) = payload
            .get("order_id")
            .or_else(|| payload.get("orderID"))
            .or_else(|| payload.get("id"))
            .and_then(serde_json::Value::as_str)
        else {
            return false;
        };
        shared.probe_order_ids.contains(order_id)
    }

    fn dedupe_key(&self) -> String {
        // REST pages can repeat exact rows at cursor boundaries. Include the
        // lifecycle status/type in addition to the venue id so MATCHED →
        // MINED → CONFIRMED edges within one page remain distinct.
        let payload = self.payload();
        let id = invalid_payload_key(payload);
        let status = payload
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        let lifecycle = payload
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        let timestamp = payload
            .get("timestamp")
            .or_else(|| payload.get("match_time"))
            .map(serde_json::Value::to_string)
            .unwrap_or_default();
        format!("{id}|{status}|{lifecycle}|{timestamp}")
    }

    fn terminal_trade_key(&self) -> Option<String> {
        let Self::Trade(payload) = self else {
            return None;
        };
        let status = payload
            .get("status")
            .and_then(serde_json::Value::as_str)?
            .trim_start_matches("TRADE_STATUS_");
        let status = match status {
            "CONFIRMED" | "FAILED" => status,
            _ => return None,
        };
        let trade_id = payload
            .get("id")
            .or_else(|| payload.get("trade_id"))
            .and_then(serde_json::Value::as_str)?
            .trim();
        (!trade_id.is_empty()).then(|| format!("{trade_id}|{status}"))
    }
}

fn trade_lifecycle_is_durably_covered(payload: &serde_json::Value, shared: &SharedState) -> bool {
    let status = payload
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .trim_start_matches("TRADE_STATUS_");
    if status.is_empty() || status == "RETRYING" {
        return false;
    }
    let trade_id = payload
        .get("id")
        .or_else(|| payload.get("trade_id"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|trade_id| !trade_id.is_empty());
    let Some(trade_id) = trade_id else {
        return false;
    };
    let mut owned_maker_leg = false;
    for order in payload
        .get("maker_orders")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|order| {
            order
                .get("maker_address")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|address| address.eq_ignore_ascii_case(&shared.order_maker_address))
        })
    {
        owned_maker_leg = true;
        let Some(order_id) = order
            .get("order_id")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|order_id| !order_id.is_empty())
        else {
            return false;
        };
        let Some(token_id) = order.get("asset_id").and_then(serde_json::Value::as_str) else {
            return false;
        };
        let Some(side) = order
            .get("side")
            .and_then(serde_json::Value::as_str)
            .and_then(|side| strict_side(side, "maker side").ok())
        else {
            return false;
        };
        let Some(quantity) = strict_number(order.get("matched_amount"), "matched_amount").ok()
        else {
            return false;
        };
        let Some(price) = strict_number(order.get("price"), "price").ok() else {
            return false;
        };
        let key = format!("{}:{}", trade_id, normalize_order_id(order_id));
        if !shared.account_state.trade_lifecycle_covers_nonblocking(
            &key, status, order_id, token_id, side, quantity, price, true,
        ) {
            return false;
        }
    }
    if owned_maker_leg {
        return true;
    }
    let Some(order_id) = taker_order_id(payload)
        .map(str::trim)
        .filter(|id| !id.is_empty())
    else {
        return false;
    };
    let Some(token_id) = payload
        .get("asset_id")
        .or_else(|| payload.get("token_id"))
        .and_then(serde_json::Value::as_str)
    else {
        return false;
    };
    let Some(side) = payload
        .get("side")
        .and_then(serde_json::Value::as_str)
        .and_then(|side| strict_side(side, "side").ok())
    else {
        return false;
    };
    let Some(quantity) = strict_number(
        payload
            .get("size")
            .or_else(|| payload.get("matched_amount")),
        "size",
    )
    .ok() else {
        return false;
    };
    let Some(price) = strict_number(payload.get("price"), "price").ok() else {
        return false;
    };
    shared.account_state.trade_lifecycle_covers_nonblocking(
        trade_id, status, order_id, token_id, side, quantity, price, false,
    )
}

#[derive(Debug, Clone, Copy, Default)]
struct ReplayApplySummary {
    applied: usize,
    durable_skips: usize,
}

enum PrivateApplyCommand {
    Live {
        events: Vec<PrivateEventDelta>,
        enqueued_at: crate::latency::Instant,
    },
    Replay {
        events: Vec<PrivateEventDelta>,
        recovery_generation: Option<u64>,
        completion: tokio::sync::oneshot::Sender<std::result::Result<ReplayApplySummary, String>>,
    },
}

/// Ownership has already been validated and every advancing strategy update
/// has crossed the high-priority delivery edge before this command is
/// published.  One cold-account worker is the sole consumer, preserving the
/// venue order of account/audit/persistence transitions without holding up the
/// private owner route.
pub(crate) struct PrivateColdCommand {
    events: Vec<PrivateEventDelta>,
    /// Lifecycle identities durably covered when this whole command commits.
    /// They return to the single owner over a bounded advisory ack lane; only
    /// then may another cold-ledger replay be suppressed.
    identities: Vec<PrivateRouteIdentity>,
    durable_skips: usize,
    recovery_generation: Option<u64>,
    completion:
        Option<tokio::sync::oneshot::Sender<std::result::Result<ReplayApplySummary, String>>>,
    routed_at: crate::latency::Instant,
    feedback: PrivateColdFeedback,
}

#[derive(Clone)]
struct PrivateColdFeedback {
    ack_tx: crossbeam_channel::Sender<Vec<PrivateRouteIdentity>>,
    reconnect_generation: Arc<AtomicU64>,
    reconnect_notify: Arc<tokio::sync::Notify>,
}

#[derive(Clone)]
struct PrivateApplyLane {
    live_tx: crossbeam_channel::Sender<PrivateApplyCommand>,
    replay_tx: crossbeam_channel::Sender<PrivateApplyCommand>,
    reconnect_generation: Arc<AtomicU64>,
    reconnect_notify: Arc<tokio::sync::Notify>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum PrivateRouteIdentity {
    TradeLifecycle { fingerprint: u128, rank: u8 },
}

#[derive(Debug)]
struct FastPrivateUpdate {
    owner: u16,
    update: OrderUpdate,
    identity: PrivateRouteIdentity,
}

#[derive(Debug)]
struct PrivateRouteDedupe {
    capacity: usize,
    ranks: HashMap<u128, u8>,
    order: VecDeque<(u128, u8)>,
}

struct RoutedPrivateBatch {
    events: Vec<PrivateEventDelta>,
    identities: Vec<PrivateRouteIdentity>,
    durable_skips: usize,
}

impl PrivateRouteDedupe {
    fn new() -> Self {
        Self {
            capacity: TERMINAL_GAP_REPLAY_DEDUPE_CAPACITY,
            ranks: HashMap::with_capacity(TERMINAL_GAP_REPLAY_DEDUPE_CAPACITY),
            order: VecDeque::with_capacity(TERMINAL_GAP_REPLAY_DEDUPE_CAPACITY),
        }
    }

    fn seen(&self, identity: &PrivateRouteIdentity) -> bool {
        match identity {
            PrivateRouteIdentity::TradeLifecycle { fingerprint, rank } => self
                .ranks
                .get(fingerprint)
                .is_some_and(|existing| *existing >= *rank),
        }
    }

    fn remember(&mut self, identity: PrivateRouteIdentity) {
        match identity {
            PrivateRouteIdentity::TradeLifecycle { fingerprint, rank } => {
                if self
                    .ranks
                    .get(&fingerprint)
                    .is_some_and(|existing| *existing >= rank)
                {
                    return;
                }
                self.ranks.insert(fingerprint, rank);
                self.order.push_back((fingerprint, rank));
                while self.order.len() > self.capacity {
                    if let Some((expired, expired_rank)) = self.order.pop_front() {
                        if self.ranks.get(&expired) == Some(&expired_rank) {
                            self.ranks.remove(&expired);
                        }
                    }
                }
            }
        }
    }
}

fn private_route_fingerprint(kind: u8, parts: &[&str]) -> u128 {
    let mut left = 0xcbf29ce484222325_u64 ^ u64::from(kind);
    let mut right = 0x9e3779b97f4a7c15_u64 ^ u64::from(kind);
    for part in parts {
        for byte in part.bytes().map(|byte| byte.to_ascii_lowercase()) {
            left ^= u64::from(byte);
            left = left.wrapping_mul(0x100000001b3);
            right ^= u64::from(byte).wrapping_add(0x9d);
            right = right.rotate_left(7).wrapping_mul(0x9e3779b185ebca87);
        }
        left ^= 0xff;
        right ^= 0xa5;
    }
    (u128::from(left) << 64) | u128::from(right)
}

fn private_status(data: &serde_json::Value) -> (&str, OrderStatus, u8) {
    let raw = data
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("MATCHED")
        .trim_start_matches("TRADE_STATUS_");
    let normalized = if raw == "MATCHED_NOT_BROADCASTED" {
        "MATCHED"
    } else {
        raw
    };
    match normalized {
        "MATCHED" => (normalized, OrderStatus::PartiallyFilled, 1),
        "MINED" => (normalized, OrderStatus::PartiallyFilled, 2),
        "CONFIRMED" => (normalized, OrderStatus::Filled, 3),
        "FAILED" => (normalized, OrderStatus::Failed, 4),
        _ => (normalized, OrderStatus::PartiallyFilled, 0),
    }
}

fn private_failure_reason(data: &serde_json::Value) -> Option<String> {
    [
        "error",
        "reason",
        "failure_reason",
        "revert_reason",
        "last_status_reason",
        "last_update_reason",
        "status_reason",
        "error_message",
        "errorMsg",
    ]
    .into_iter()
    .find_map(|key| {
        data.get(key)
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    })
}

/// Validate and attribute a private event using the route-local ownership
/// mirror only. No shared account transition, audit, logging, or persistence
/// occurs here; the account actor can deliver these updates to StrategyAccount
/// before beginning cold bookkeeping.
fn route_private_event_fast(
    event: &PrivateEventDelta,
    shared: &SharedState,
) -> std::result::Result<Vec<FastPrivateUpdate>, String> {
    let data = event.payload();
    match event {
        PrivateEventDelta::Order(_) => {
            let order_id = required_string(data, &["order_id", "orderID", "id"], "order_id")?;
            let asset_id = required_string(data, &["asset_id", "token_id"], "asset_id")?;
            let side = strict_side(required_string(data, &["side"], "side")?, "side")?;
            let price = strict_number(data.get("price"), "price")?;
            let original_size = strict_number(
                data.get("original_size").or_else(|| data.get("size")),
                "original_size",
            )?;
            let size_matched = strict_number(data.get("size_matched"), "size_matched")?;
            let tolerance = 1e-9_f64.max(original_size.abs() * 1e-8);
            if original_size <= 0.0
                || size_matched < 0.0
                || size_matched > original_size + tolerance
                || price <= 0.0
                || price > 1.0 + 1e-8
            {
                return Err(format!(
                    "invalid order lifecycle economics order_id={order_id}"
                ));
            }
            let ownership = shared.lookup_order_ownership(order_id).ok_or_else(|| {
                format!("unowned private order lifecycle event for order_id `{order_id}`")
            })?;
            if ownership.token_id != asset_id
                || ownership.side != side
                || !close_enough(ownership.quantity, original_size)
                || !close_enough(ownership.price, price)
            {
                return Err(format!(
                    "order lifecycle invariant mismatch coid={} order_id={order_id}",
                    ownership.client_order_id,
                ));
            }
            let lifecycle = required_string(data, &["type"], "type")?;
            let status = if lifecycle.eq_ignore_ascii_case("PLACEMENT") {
                OrderStatus::Accepted
            } else if lifecycle.eq_ignore_ascii_case("UPDATE") {
                if size_matched + tolerance >= original_size {
                    OrderStatus::Filled
                } else if size_matched > tolerance {
                    OrderStatus::PartiallyFilled
                } else {
                    OrderStatus::Accepted
                }
            } else if lifecycle.eq_ignore_ascii_case("CANCELLATION")
                || lifecycle.eq_ignore_ascii_case("CANCELLED")
                || lifecycle.eq_ignore_ascii_case("CANCELED")
            {
                OrderStatus::Cancelled
            } else {
                return Err(format!("unsupported order lifecycle type `{lifecycle}`"));
            };
            let associate_trades = match data.get("associate_trades") {
                None => Vec::new(),
                Some(value) => {
                    let values = value.as_array().ok_or_else(|| {
                        "order lifecycle associate_trades is not an array".to_string()
                    })?;
                    let mut trades = Vec::with_capacity(values.len());
                    for value in values {
                        let trade_id = value
                            .as_str()
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .ok_or_else(|| {
                                "order lifecycle associate_trades contains invalid id".to_string()
                            })?;
                        if trades.iter().any(|existing| existing == trade_id) {
                            return Err(format!(
                                "order lifecycle associate_trades contains duplicate id `{trade_id}`"
                            ));
                        }
                        trades.push(trade_id.to_string());
                    }
                    trades
                }
            };
            if size_matched > tolerance && associate_trades.is_empty() {
                return Err(format!(
                    "matched order lifecycle is missing associate_trades order_id={order_id}"
                ));
            }
            let rank = match status {
                OrderStatus::Accepted => 1,
                OrderStatus::PartiallyFilled => 2,
                OrderStatus::Filled | OrderStatus::Cancelled => 3,
                _ => 0,
            };
            let identity = PrivateRouteIdentity::TradeLifecycle {
                fingerprint: private_route_fingerprint(b'o', &[order_id]),
                rank,
            };
            Ok(vec![FastPrivateUpdate {
                owner: shared
                    .strategy_owner(&ownership.instance_id)
                    .ok_or_else(|| {
                        format!(
                            "private lifecycle instance={} has no numeric strategy owner",
                            ownership.instance_id,
                        )
                    })?,
                update: OrderUpdate {
                    order_slot: ownership.order_slot,
                    client_order_id: ownership.client_order_id,
                    exchange: Exchange::Polymarket,
                    symbol: asset_id.to_string(),
                    side,
                    exchange_order_id: Some(order_id.to_string()),
                    status,
                    liquidity: None,
                    filled_quantity: 0.0,
                    remaining_quantity: (original_size - size_matched).max(0.0),
                    avg_fill_price: price,
                    timestamp_ns: now_ns(),
                    exchange_event_timestamp_ns: None,
                    trade_id: None,
                    order_audit: Some(AuthoritativeOrderAudit {
                        original_size: Some(original_size.to_string()),
                        size_matched: Some(size_matched.to_string()),
                        associate_trades,
                    }),
                    error: None,
                },
                identity,
            }])
        }
        PrivateEventDelta::Trade(_) => {
            let (status_name, status, rank) = private_status(data);
            // REST history commonly returns MATCHED even after the durable
            // ledger has advanced the same trade to MINED/CONFIRMED. Treat a
            // later durable edge as the high-water mark so reconnect replay
            // cannot regress StrategyAccount or enter the cold aggregate path.
            if rank > 0 && status_name != "RETRYING" {
                let high_water_started = crate::latency::Instant::now();
                let covered = trade_lifecycle_is_durably_covered(data, shared);
                crate::latency::record(
                    "polymarket.user.trade_lifecycle_high_water",
                    high_water_started,
                );
                if covered {
                    return Ok(Vec::new());
                }
            }
            let fields_started = crate::latency::Instant::now();
            let role = validate_trade_event(data, shared)?;
            crate::latency::record("polymarket.user.validate_trade_fields", fields_started);
            if rank == 0 || status_name == "RETRYING" {
                return Ok(Vec::new());
            }
            let trade_id = required_string(data, &["id", "trade_id"], "trade_id")?;
            let exchange_timestamp_ns = exchange_event_timestamp_ns(data);
            let failure_reason = private_failure_reason(data);
            let mut routed = Vec::new();
            match role {
                PrivateTradeRole::Maker => {
                    for maker in data
                        .get("maker_orders")
                        .and_then(serde_json::Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter(|maker| {
                            maker
                                .get("maker_address")
                                .and_then(serde_json::Value::as_str)
                                .is_some_and(|address| {
                                    address.eq_ignore_ascii_case(&shared.order_maker_address)
                                })
                        })
                    {
                        let order_id = required_string(maker, &["order_id"], "maker order_id")?;
                        let ownership = match shared.lookup_order_ownership(order_id) {
                            Some(ownership) => ownership,
                            None if matches!(status_name, "CONFIRMED" | "FAILED") => {
                                // Do not reject an authenticated historical
                                // terminal leg before the cold account writer
                                // can inspect it. No strategy update is emitted
                                // here. The cold path still requires either an
                                // exact durable trade tombstone or settlement
                                // proof plus one unique historical token owner;
                                // ambiguity therefore remains fail-closed and
                                // keeps the replay cursor pinned.
                                continue;
                            }
                            None => {
                                return Err(format!(
                                    "unowned maker trade `{trade_id}` order_id `{order_id}`"
                                ));
                            }
                        };
                        let symbol = required_string(maker, &["asset_id"], "maker asset_id")?;
                        let side = strict_side(
                            required_string(maker, &["side"], "maker side")?,
                            "maker side",
                        )?;
                        let quantity =
                            strict_number(maker.get("matched_amount"), "matched_amount")?;
                        let price = strict_number(maker.get("price"), "price")?;
                        if ownership.token_id != symbol || ownership.side != side {
                            return Err(format!(
                                "maker trade ownership mismatch trade={trade_id} order_id={order_id}"
                            ));
                        }
                        let key = format!("{}:{}", trade_id, normalize_order_id(order_id));
                        routed.push(FastPrivateUpdate {
                            owner: shared.strategy_owner(&ownership.instance_id).ok_or_else(
                                || {
                                    format!(
                                    "private maker trade instance={} has no numeric strategy owner",
                                    ownership.instance_id,
                                )
                                },
                            )?,
                            update: OrderUpdate {
                                order_slot: ownership.order_slot,
                                client_order_id: ownership.client_order_id,
                                exchange: Exchange::Polymarket,
                                symbol: symbol.to_string(),
                                side,
                                exchange_order_id: Some(order_id.to_string()),
                                status,
                                liquidity: Some(Liquidity::Maker),
                                filled_quantity: quantity,
                                remaining_quantity: 0.0,
                                avg_fill_price: price,
                                timestamp_ns: now_ns(),
                                exchange_event_timestamp_ns: exchange_timestamp_ns,
                                trade_id: Some(key.clone()),
                                order_audit: None,
                                error: failure_reason.clone(),
                            },
                            identity: PrivateRouteIdentity::TradeLifecycle {
                                fingerprint: private_route_fingerprint(b'm', &[trade_id, order_id]),
                                rank,
                            },
                        });
                    }
                }
                PrivateTradeRole::Taker => {
                    let order_id = taker_order_id(data).unwrap_or("");
                    let ownership = match shared.lookup_order_ownership(order_id) {
                        Some(ownership) => ownership,
                        None if matches!(status_name, "CONFIRMED" | "FAILED") => {
                            // `classify_private_trade_role` authenticated this
                            // account as the taker. Defer the strict historical
                            // ownership proof to the cold single writer without
                            // broadcasting an unowned fill to any strategy.
                            return Ok(Vec::new());
                        }
                        None => {
                            return Err(format!(
                                "unowned taker trade `{trade_id}` order_id `{order_id}`"
                            ));
                        }
                    };
                    let symbol = required_string(data, &["asset_id", "token_id"], "asset_id")?;
                    let side = strict_side(required_string(data, &["side"], "side")?, "side")?;
                    let quantity = strict_number(
                        data.get("size").or_else(|| data.get("matched_amount")),
                        "size",
                    )?;
                    let price = strict_number(data.get("price"), "price")?;
                    if ownership.token_id != symbol || ownership.side != side {
                        return Err(format!(
                            "taker trade ownership mismatch trade={trade_id} order_id={order_id}"
                        ));
                    }
                    routed.push(FastPrivateUpdate {
                        owner: shared
                            .strategy_owner(&ownership.instance_id)
                            .ok_or_else(|| {
                                format!(
                                    "private taker trade instance={} has no numeric strategy owner",
                                    ownership.instance_id,
                                )
                            })?,
                        update: OrderUpdate {
                            order_slot: ownership.order_slot,
                            client_order_id: ownership.client_order_id,
                            exchange: Exchange::Polymarket,
                            symbol: symbol.to_string(),
                            side,
                            exchange_order_id: Some(order_id.to_string()),
                            status,
                            liquidity: Some(Liquidity::Taker),
                            filled_quantity: quantity,
                            remaining_quantity: 0.0,
                            avg_fill_price: price,
                            timestamp_ns: now_ns(),
                            exchange_event_timestamp_ns: exchange_timestamp_ns,
                            trade_id: Some(trade_id.to_string()),
                            order_audit: None,
                            error: failure_reason,
                        },
                        identity: PrivateRouteIdentity::TradeLifecycle {
                            fingerprint: private_route_fingerprint(b't', &[trade_id]),
                            rank,
                        },
                    });
                }
            }
            Ok(routed)
        }
    }
}

impl PrivateApplyLane {
    fn dispatch_live(&self, events: Vec<PrivateEventDelta>) -> std::result::Result<(), String> {
        if events.is_empty() {
            return Ok(());
        }
        self.live_tx
            .try_send(PrivateApplyCommand::Live {
                events,
                enqueued_at: crate::latency::Instant::now(),
            })
            .map_err(|error| format!("private account-apply queue unavailable: {error}"))
    }

    async fn apply_replay_batch(
        &self,
        events: Vec<PrivateEventDelta>,
        recovery_generation: Option<u64>,
    ) -> std::result::Result<ReplayApplySummary, String> {
        if events.is_empty() {
            return Ok(ReplayApplySummary::default());
        }
        let (completion, done) = tokio::sync::oneshot::channel();
        let mut command = PrivateApplyCommand::Replay {
            events,
            recovery_generation,
            completion,
        };
        loop {
            match self.replay_tx.try_send(command) {
                Ok(()) => break,
                Err(crossbeam_channel::TrySendError::Full(returned)) => {
                    command = returned;
                    tokio::task::yield_now().await;
                }
                Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                    return Err("private account-apply worker stopped".to_string());
                }
            }
        }
        done.await
            .map_err(|_| "private account-apply completion dropped".to_string())?
    }
}

fn dispatch_private_update(
    shared: &SharedState,
    update_tx: &Sender<RoutedOrderUpdate>,
    recovery_generation: Option<u64>,
    routed: RoutedOrderUpdate,
) -> std::result::Result<(), String> {
    let update = routed.update;
    if update.client_order_id.is_empty() {
        if let Some(oid) = update.exchange_order_id.as_deref() {
            let is_probe = shared.probe_order_ids.contains(oid);
            if is_probe {
                debug!(
                    "[PolyUserFeed] probe order push muted: {} {:?} oid={}..",
                    update.symbol,
                    update.status,
                    &oid[..oid.len().min(10)],
                );
                return Ok(());
            }
        }
    }
    if log::log_enabled!(log::Level::Debug) {
        let coid = if update.client_order_id.is_empty() {
            match update.exchange_order_id.as_deref() {
                Some(oid) if !oid.is_empty() => {
                    format!("<unmapped:orderID={}..>", &oid[..oid.len().min(10)])
                }
                _ => "<unmapped>".to_string(),
            }
        } else {
            update.client_order_id.clone()
        };
        debug!(
            "[PolyUserFeed] {} coid={} {} {:?} filled={} price={}",
            update.symbol,
            coid,
            update.side,
            update.status,
            update.filled_quantity,
            update.avg_fill_price,
        );
    }
    let dispatch_started = crate::latency::Instant::now();
    let result = if let Some(generation) = recovery_generation {
        enqueue_recovery_update(shared, update_tx, generation, update)
            .map_err(|error| error.to_string())
    } else {
        update_tx
            .try_send(RoutedOrderUpdate {
                owner: routed.owner,
                update,
            })
            .map_err(|error| match error {
                crossbeam_channel::TrySendError::Full(_) => {
                    "root order-update queue saturated; private event remains replayable"
                        .to_string()
                }
                crossbeam_channel::TrySendError::Disconnected(_) => {
                    "order update channel closed".to_string()
                }
            })
    };
    crate::latency::record("polymarket.user.dispatch", dispatch_started);
    result
}

fn route_private_batch(
    shared: &SharedState,
    update_tx: &Sender<RoutedOrderUpdate>,
    events: Vec<PrivateEventDelta>,
    recovery_generation: Option<u64>,
    route_dedupe: &mut PrivateRouteDedupe,
    cold_committed: Option<&PrivateRouteDedupe>,
) -> std::result::Result<RoutedPrivateBatch, String> {
    let mut cold_events = Vec::with_capacity(events.len());
    let mut cold_identities = Vec::with_capacity(events.len());
    let mut durable_skips = 0usize;
    for event in events {
        let payload = event.payload();
        let route_started = crate::latency::Instant::now();
        let validate_started = crate::latency::Instant::now();
        let durably_covered = matches!(event, PrivateEventDelta::Trade(_))
            && trade_lifecycle_is_durably_covered(payload, shared);
        let routed = match if durably_covered {
            Ok(Vec::new())
        } else {
            route_private_event_fast(&event, shared)
        } {
            Ok(routed) => routed,
            Err(error) => {
                flag_invalid_private_event(payload, shared, &error);
                return Err(error);
            }
        };
        crate::latency::record("polymarket.user.validate_route", validate_started);
        let cold_already_committed = !routed.is_empty()
            && cold_committed
                .is_some_and(|dedupe| routed.iter().all(|routed| dedupe.seen(&routed.identity)));
        for routed in routed {
            if !cold_already_committed {
                cold_identities.push(routed.identity);
            }
            if route_dedupe.seen(&routed.identity) {
                continue;
            }
            dispatch_private_update(
                shared,
                update_tx,
                recovery_generation,
                RoutedOrderUpdate {
                    owner: routed.owner,
                    update: routed.update,
                },
            )?;
            // Delivery is the commit edge for actor-local replay suppression.
            route_dedupe.remember(routed.identity);
        }
        // Fast validation succeeded, so duplicate lifecycle traffic remains a
        // valid private-feed heartbeat even when an acknowledged cold commit
        // makes another shared-ledger transition unnecessary.
        shared
            .user_feed_health
            .record_valid_business_event(now_ns());
        if durably_covered {
            durable_skips = durable_skips.saturating_add(1);
            crate::latency::record_ns("polymarket.user.durable_lifecycle_skip", 1);
        } else if cold_already_committed {
            crate::latency::record_ns("polymarket.user.cold_committed_skip", 1);
        } else {
            // An empty identity set is intentionally not suppressible. Rare
            // retired/historical terminal events still receive durable cold
            // validation after a process restart.
            cold_events.push(event);
        }
        crate::latency::record("polymarket.user.validate_route_dispatch", route_started);
    }
    Ok(RoutedPrivateBatch {
        events: cold_events,
        identities: cold_identities,
        durable_skips,
    })
}

fn apply_private_cold_batch_owned(
    shared: &SharedState,
    live_position: &mut LivePositionManager,
    replay: &mut PrivateReplayOwner,
    events: &[PrivateEventDelta],
    recovery_generation: Option<u64>,
) -> std::result::Result<usize, String> {
    let mut applied = 0usize;
    for event in events {
        let payload = event.payload();
        let is_trade = matches!(event, PrivateEventDelta::Trade(_));
        if (is_trade && replay.lifecycle_seen(payload))
            || event
                .terminal_trade_key()
                .is_some_and(|key| replay.terminal_seen(&key))
        {
            crate::latency::record_ns("polymarket.user.owner_replay_skip", 1);
            continue;
        }
        let account_apply_started = crate::latency::Instant::now();
        let parsed = parse_user_event_with_health_owned(payload, shared, live_position, replay);
        crate::latency::record(
            if is_trade {
                "polymarket.user.account_apply.trade"
            } else {
                "polymarket.user.account_apply.order"
            },
            account_apply_started,
        );
        crate::latency::record("polymarket.user.account_apply", account_apply_started);
        let health_apply_started = crate::latency::Instant::now();
        if parsed.valid_business_event {
            shared
                .user_feed_health
                .record_valid_business_event(now_ns());
        }
        crate::latency::record("polymarket.user.health_apply", health_apply_started);
        if parsed.invalid_business_event {
            return Err(parsed
                .rejection_reason
                .unwrap_or_else(|| "invalid private business event".to_string()));
        }
        let terminal_to_remember = event
            .terminal_trade_key()
            .filter(|_| trade_lifecycle_is_durably_covered(event.payload(), shared));
        let terminal_tokens = terminal_to_remember
            .is_some()
            .then(|| {
                parsed
                    .updates
                    .iter()
                    .filter(|update| !update.symbol.is_empty())
                    .map(|update| update.symbol.clone())
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();
        // `parsed.updates` are the cold ledger's view of the same transitions;
        // the owner-directed delivery was already committed above.
        if is_trade {
            // Advance the cross-round replay cache only after every derived
            // strategy update was delivered. A delivery failure must remain
            // replayable even though the account ledger is idempotent.
            replay.remember_lifecycle(event.payload());
        }
        if let Some(key) = terminal_to_remember {
            // Only a newly durable terminal edge can make a zero-reference
            // settled event collectible. MATCHED/MINED pushes and replayed
            // no-ops used to wake the cold account GC for every raw trade,
            // creating the 30-100ms account-apply/Cancel tail seen in live.
            for token in terminal_tokens {
                shared.request_settled_gc_for_token(&token);
            }
            replay.remember_terminal(&key);
        }
        applied = applied.saturating_add(1);
        if recovery_generation.is_some() {
            // Let the independent live worker acquire an instance shard
            // between replay records instead of allowing a REST page to win
            // the lifecycle mutex repeatedly.
            std::thread::yield_now();
        }
    }
    Ok(applied)
}

#[cfg(test)]
fn apply_private_cold_batch(
    shared: &SharedState,
    events: &[PrivateEventDelta],
    recovery_generation: Option<u64>,
) -> std::result::Result<usize, String> {
    shared.with_test_live_position(|live_position| {
        let mut replay = PrivateReplayOwner::new();
        let result = apply_private_cold_batch_owned(
            shared,
            live_position,
            &mut replay,
            events,
            recovery_generation,
        );
        shared.publish_live_position_watermark(live_position.last_match_time_secs());
        result
    })
}

/// Apply one private batch on the account actor. Execution lifecycle and
/// authenticated private lifecycle therefore share exactly one writer and one
/// ordered high-priority lane per physical account.
pub(crate) fn apply_private_cold_command(
    shared: &SharedState,
    live_position: &mut LivePositionManager,
    replay: &mut PrivateReplayOwner,
    command: PrivateColdCommand,
) {
    let PrivateColdCommand {
        events,
        identities,
        durable_skips,
        recovery_generation,
        completion,
        routed_at,
        feedback,
    } = command;
    crate::latency::record("polymarket.user.fast_route_to_account_owner", routed_at);
    let result =
        apply_private_cold_batch_owned(shared, live_position, replay, &events, recovery_generation);
    shared.publish_live_position_watermark(live_position.last_match_time_secs());
    if let Err(error) = &result {
        shared.user_feed_health.set_recovering(true);
        shared.enqueue_private_apply_failure_diagnostic(error);
        feedback.reconnect_generation.fetch_add(1, Ordering::AcqRel);
        feedback.reconnect_notify.notify_one();
    }
    if result.is_ok() && !identities.is_empty() && feedback.ack_tx.try_send(identities).is_err() {
        crate::latency::record_ns("polymarket.user.cold_commit_ack_overflow", 1);
    }
    if let Some(completion) = completion {
        let _ = completion.send(result.map(|applied| ReplayApplySummary {
            applied,
            durable_skips,
        }));
    }
}

fn spawn_private_apply_worker(
    shared: Arc<SharedState>,
    update_tx: Sender<RoutedOrderUpdate>,
    shutdown: Arc<AtomicBool>,
) -> Result<(PrivateApplyLane, Vec<std::thread::JoinHandle<()>>)> {
    let (live_tx, live_rx) = crossbeam_channel::bounded(PRIVATE_APPLY_QUEUE_CAPACITY);
    let (replay_tx, replay_rx) = crossbeam_channel::bounded(PRIVATE_APPLY_QUEUE_CAPACITY);
    // Account actor -> private owner, SPSC and bounded. Overflow drops only the
    // duplicate-suppression hint: the owner safely re-enqueues the idempotent
    // event, so no private lifecycle or recovery edge can be lost.
    let (cold_ack_tx, cold_ack_rx) =
        crossbeam_channel::bounded::<Vec<PrivateRouteIdentity>>(PRIVATE_APPLY_QUEUE_CAPACITY);
    let reconnect_generation = Arc::new(AtomicU64::new(0));
    let reconnect_notify = Arc::new(tokio::sync::Notify::new());
    let lane = PrivateApplyLane {
        live_tx,
        replay_tx,
        reconnect_generation: Arc::clone(&reconnect_generation),
        reconnect_notify: Arc::clone(&reconnect_notify),
    };
    let account_id = shared.account_state.account_id().to_string();
    let cold_feedback = PrivateColdFeedback {
        ack_tx: cold_ack_tx,
        reconnect_generation: Arc::clone(&reconnect_generation),
        reconnect_notify: Arc::clone(&reconnect_notify),
    };
    let worker = std::thread::Builder::new()
        .name(format!("poly-private-owner-{}", account_id))
        .spawn(move || {
            crate::os_tune::pin_private_account_apply(
                "polymarket-private-owner",
                &account_id,
            );
            crate::latency::prepare_polymarket_private_stages();
            let mut route_dedupe = PrivateRouteDedupe::new();
            let mut cold_committed = PrivateRouteDedupe::new();
            while !shutdown.load(Ordering::Relaxed) {
                crossbeam_channel::select_biased! {
                    recv(cold_ack_rx) -> ack => match ack {
                        Ok(identities) => {
                            for identity in identities {
                                cold_committed.remember(identity);
                            }
                        }
                        Err(_) => break,
                    },
                    recv(live_rx) -> command => match command {
                        Ok(PrivateApplyCommand::Live { events, enqueued_at }) => {
                            crate::latency::record(
                                "polymarket.user.ws_enqueue_to_owner_dequeue",
                                enqueued_at,
                            );
                            let routed = match route_private_batch(
                                &shared,
                                &update_tx,
                                events,
                                None,
                                &mut route_dedupe,
                                Some(&cold_committed),
                            ) {
                                Ok(routed) => routed,
                                Err(error) => {
                                    shared.user_feed_health.set_recovering(true);
                                    warn!(
                                        "[PolyUserFeed] invalid private business event; forcing reconnect for authoritative trade/order audit: {}",
                                        error,
                                    );
                                    reconnect_generation.fetch_add(1, Ordering::AcqRel);
                                    reconnect_notify.notify_one();
                                    continue;
                                }
                            };
                            if routed.events.is_empty() {
                                continue;
                            }
                            let cold = PrivateColdCommand {
                                events: routed.events,
                                identities: routed.identities,
                                durable_skips: routed.durable_skips,
                                recovery_generation: None,
                                completion: None,
                                routed_at: crate::latency::Instant::now(),
                                feedback: cold_feedback.clone(),
                            };
                            if shared.enqueue_private_cold(cold).is_err() {
                                shared.user_feed_health.set_recovering(true);
                                warn!(
                                    "[PolyUserFeed] cold private-account queue saturated; forcing reconnect/replay"
                                );
                                reconnect_generation.fetch_add(1, Ordering::AcqRel);
                                reconnect_notify.notify_one();
                            }
                        }
                        Ok(PrivateApplyCommand::Replay { completion, .. }) => {
                            let _ = completion.send(Err(
                                "replay command reached private live lane".to_string()
                            ));
                        }
                        Err(_) => break,
                    },
                    recv(replay_rx) -> command => match command {
                        Ok(PrivateApplyCommand::Replay {
                            events,
                            recovery_generation,
                            completion,
                        }) => {
                            let routed = route_private_batch(
                                &shared,
                                &update_tx,
                                events,
                                recovery_generation,
                                &mut route_dedupe,
                                None,
                            );
                            match routed {
                                Err(error) => {
                                    let _ = completion.send(Err(error));
                                }
                                Ok(routed) => {
                                    if routed.events.is_empty() {
                                        let _ = completion.send(Ok(ReplayApplySummary {
                                            applied: 0,
                                            durable_skips: routed.durable_skips,
                                        }));
                                        continue;
                                    }
                                    let cold = PrivateColdCommand {
                                        events: routed.events,
                                        identities: routed.identities,
                                        durable_skips: routed.durable_skips,
                                        recovery_generation,
                                        completion: Some(completion),
                                        routed_at: crate::latency::Instant::now(),
                                        feedback: cold_feedback.clone(),
                                    };
                                    if let Err(error) = shared.enqueue_private_cold(cold) {
                                        if let PrivateColdCommand {
                                            completion: Some(completion),
                                            ..
                                        } = error.into_inner()
                                        {
                                            let _ = completion.send(Err(
                                                "cold private-account queue saturated".to_string(),
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                        Ok(PrivateApplyCommand::Live { .. }) => {
                            warn!("[PolyUserFeed] live command reached replay lane");
                        }
                        Err(_) => break,
                    },
                    default(Duration::from_millis(100)) => {}
                }
            }
        })?;
    Ok((lane, vec![worker]))
}

fn parse_user_event_with_health_owned(
    data: &serde_json::Value,
    shared: &SharedState,
    live_position: &mut LivePositionManager,
    replay: &mut PrivateReplayOwner,
) -> ParsedPrivateEvent {
    let recognized = data
        .get("event_type")
        .or_else(|| data.get("type"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|event_type| matches!(event_type, "order" | "trade"));
    if !recognized {
        return ParsedPrivateEvent {
            updates: Vec::new(),
            valid_business_event: false,
            invalid_business_event: false,
            rejection_reason: None,
        };
    }
    match parse_user_event_checked(data, shared, live_position, replay) {
        Ok(updates) => {
            let anomaly_started = crate::latency::Instant::now();
            resolve_valid_private_event_anomaly(data, shared);
            crate::latency::record("polymarket.user.account_resolve_anomaly", anomaly_started);
            ParsedPrivateEvent {
                updates,
                valid_business_event: true,
                invalid_business_event: false,
                rejection_reason: None,
            }
        }
        Err(error) => {
            flag_invalid_private_event(data, shared, &error);
            ParsedPrivateEvent {
                updates: Vec::new(),
                valid_business_event: false,
                invalid_business_event: true,
                rejection_reason: Some(error),
            }
        }
    }
}

#[cfg(test)]
fn parse_user_event_with_health(
    data: &serde_json::Value,
    shared: &SharedState,
) -> ParsedPrivateEvent {
    shared.with_test_live_position(|live_position| {
        let mut replay = PrivateReplayOwner::new();
        let parsed = parse_user_event_with_health_owned(data, shared, live_position, &mut replay);
        shared.publish_live_position_watermark(live_position.last_match_time_secs());
        parsed
    })
}

fn resolve_valid_private_event_anomaly(data: &serde_json::Value, shared: &SharedState) {
    let key = invalid_payload_key(data);
    let replay_key = invalid_replay_anchor_key(data);
    let event_type = data
        .get("event_type")
        .or_else(|| data.get("type"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    // Successful trade attribution already clears its exact replay anchor in
    // `parse_user_event_validated`. Do not clear it generically here: for a
    // valid but still-unowned taker trade the exact key is also `trade:{id}`.
    // Maker failures use per-leg anchors (`trade:{id}:{maker_order_id}`), so
    // the payload-level syntax anchor can safely be cleared after validation.
    if event_type == "trade"
        && data
            .get("maker_orders")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|orders| {
                orders.iter().any(|order| {
                    order
                        .get("maker_address")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|address| {
                            address.eq_ignore_ascii_case(&shared.order_maker_address)
                        })
                })
            })
    {
        shared
            .account_state
            .resolve_unresolved_trade_match_time(&replay_key);
    }
    shared.account_state.resolve_private_event_anomaly(&key);
    if !shared.account_state.is_uncertain() {
        shared.user_feed_health.set_inventory_uncertain(false);
    }
}

/// Parse a Polymarket user WebSocket event into zero-or-more OrderUpdates.
/// A single "trade" push from a MAKER perspective may expand into multiple
/// OrderUpdates (one per matching `maker_orders[]` entry owned by us).
#[cfg(test)]
pub(crate) fn parse_user_event(data: &serde_json::Value, shared: &SharedState) -> Vec<OrderUpdate> {
    parse_user_event_with_health(data, shared).updates
}

/// Checked parser outcome for terminal order-audit backfill. Callers must not
/// infer "parser rejected" from an empty update vector: valid lifecycle
/// duplicates and already-applied records intentionally produce no update.
pub(crate) fn parse_user_event_diagnosed_owned(
    data: &serde_json::Value,
    shared: &SharedState,
    live_position: &mut LivePositionManager,
    replay: &mut PrivateReplayOwner,
) -> ParsedPrivateEvent {
    parse_user_event_with_health_owned(data, shared, live_position, replay)
}

#[cfg(test)]
pub(crate) fn parse_user_event_diagnosed(
    data: &serde_json::Value,
    shared: &SharedState,
) -> ParsedPrivateEvent {
    parse_user_event_with_health(data, shared)
}

fn parse_user_event_validated(
    data: &serde_json::Value,
    shared: &SharedState,
    live_position: &mut LivePositionManager,
    replay: &mut PrivateReplayOwner,
) -> Vec<OrderUpdate> {
    // Determine event type from the payload structure
    let event_type = match data
        .get("event_type")
        .or_else(|| data.get("type"))
        .and_then(|v| v.as_str())
    {
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
    //         * py-clob-client / wallet.rs's REST `/data/trades` parser
    //           (shared schema between the WS and REST endpoints)
    //       The top-level `order_id` / `orderID` keys exist on some
    //       legacy/order-lifecycle payloads. They are handled separately so
    //       they can never override an authoritative `taker_order_id` when a
    //       schema variant contains both.
    match event_type {
        "order" => Vec::new(),
        "trade" => {
            let taker_order_id = taker_order_id(data).unwrap_or("");
            let asset_id = data
                .get("asset_id")
                .or_else(|| data.get("token_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

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
            let side = if side.eq_ignore_ascii_case("SELL") {
                Side::Sell
            } else {
                Side::Buy
            };

            // trade id (from top-level `id` / `trade_id`) + maker_order_id
            // (from `maker_orders[]`) form the ledger key. For TAKER we
            // use trade_id alone; for MAKER we build `{trade_id}:{maker_order_id}`
            // so each of our maker legs on this trade gets a distinct ledger row.
            let trade_id = data
                .get("id")
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
            let role = match classify_private_trade_role(data, shared) {
                Ok(role) => role,
                Err(_) => return Vec::new(),
            };
            let status_raw = data
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("MATCHED");
            let status_str = status_raw
                .strip_prefix("TRADE_STATUS_")
                .unwrap_or(status_raw);
            let status_str = if status_str == "MATCHED_NOT_BROADCASTED" {
                "MATCHED"
            } else {
                status_str
            };

            let match_time = effective_match_time(data, trade_id);

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
                        if !s.is_empty() {
                            return Some(s.to_string());
                        }
                    }
                }
                None
            };
            let failure_reason: Option<String> = extract_reason(data);
            let reason_ref: Option<&str> = failure_reason.as_deref();

            let mut updates: Vec<OrderUpdate> = Vec::new();

            if role == PrivateTradeRole::Maker {
                let funder = &shared.order_maker_address;
                let Some(arr) = data.get("maker_orders").and_then(|v| v.as_array()) else {
                    return Vec::new();
                };

                for mo in arr {
                    let mo_addr = mo
                        .get("maker_address")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    if !mo_addr.eq_ignore_ascii_case(funder) {
                        continue;
                    }

                    let mo_asset_id = mo
                        .get("asset_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let mo_side_str = mo.get("side").and_then(|v| v.as_str()).unwrap_or("BUY");
                    let mo_side = if mo_side_str.eq_ignore_ascii_case("SELL") {
                        Side::Sell
                    } else {
                        Side::Buy
                    };
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
                    let transition_started = crate::latency::Instant::now();
                    let transition = shared.account_state.apply_trade_transition_with_context(
                        &leg_id,
                        status_str,
                        &runtime_coid,
                        mo_order_id,
                        &mo_asset_id,
                        mo_side,
                        mo_size,
                        mo_price,
                        true,
                        match_time.business_secs,
                    );
                    crate::latency::record(
                        "polymarket.user.account_trade_transition",
                        transition_started,
                    );
                    let transition = if transition.ownership().is_none()
                        && matches!(status_str, "CONFIRMED" | "FAILED")
                    {
                        shared
                            .account_state
                            .record_authenticated_terminal_trade_noop(
                                &leg_id,
                                status_str,
                                mo_order_id,
                                &mo_asset_id,
                                mo_side,
                                mo_size,
                                mo_price,
                                true,
                            )
                    } else {
                        transition
                    };
                    let Some(ownership) = transition.ownership().cloned() else {
                        // Never broadcast an unowned private trade. The account
                        // ledger has already entered uncertain with the exact
                        // oid/trade reason; fanning an empty coid to every
                        // same-token strategy would book the fill N times.
                        shared.account_state.mark_unresolved_trade_match_time(
                            &leg_id,
                            match_time.replay_watermark_secs,
                        );
                        continue;
                    };
                    if transition.persistence_pending() {
                        warn!(
                            "[PolymarketUserFeed] owned maker trade {} is applied with persistence pending; broadcasting fill while account admission stays blocked",
                            leg_id,
                        );
                    }
                    update_trade_finality_replay_anchor(
                        shared,
                        &leg_id,
                        status_str,
                        match_time.replay_watermark_secs,
                    );
                    if match_time.replay_watermark_secs > 0 {
                        live_position.touch_match_time(match_time.replay_watermark_secs);
                    }
                    if transition.owned_noop() {
                        continue;
                    }
                    let coid = ownership.client_order_id;
                    // Only advance the feed-level dedupe after ownership was
                    // successfully resolved. An unowned event must remain
                    // replayable after its order mapping arrives later.
                    let live_position_started = crate::latency::Instant::now();
                    let lifecycle_advanced = record_trade_transition(
                        live_position,
                        &leg_id,
                        status_str,
                        &mo_asset_id,
                        mo_side,
                        mo_size,
                        mo_price,
                        true,
                        reason_ref,
                    );
                    crate::latency::record(
                        "polymarket.user.account_live_position",
                        live_position_started,
                    );
                    if !lifecycle_advanced {
                        continue;
                    }
                    if status != OrderStatus::Failed {
                        shared.request_filled_order_cleanup(&coid);
                    }

                    let update = OrderUpdate {
                        order_slot: ownership.order_slot,
                        client_order_id: coid,
                        exchange: Exchange::Polymarket,
                        symbol: mo_asset_id,
                        side: mo_side,
                        exchange_order_id: if mo_order_id.is_empty() {
                            None
                        } else {
                            Some(mo_order_id.to_string())
                        },
                        status,
                        liquidity: Some(Liquidity::Maker),
                        filled_quantity: mo_size,
                        remaining_quantity: 0.0,
                        avg_fill_price: mo_price,
                        timestamp_ns: now_ns(),
                        exchange_event_timestamp_ns: None,
                        trade_id: if leg_id.is_empty() {
                            None
                        } else {
                            Some(leg_id)
                        },
                        order_audit: None,
                        error: failure_reason.clone(),
                    };
                    shared.log_order_lifecycle(
                        &update.client_order_id,
                        "private_trade",
                        update.exchange_order_id.as_deref(),
                        Some(update.status),
                        update.trade_id.as_deref(),
                    );
                    updates.push(update);
                }
            } else {
                let matched_amount: f64 =
                    parse_f(data.get("size").or_else(|| data.get("matched_amount")));
                let price: f64 = parse_f(data.get("price"));

                if TradeStatus::from_str(status_str).is_none()
                    || trade_id.is_empty()
                    || matched_amount <= 0.0
                {
                    return Vec::new();
                }

                let runtime_coid = shared.lookup_coid(taker_order_id).unwrap_or_default();
                let transition_started = crate::latency::Instant::now();
                let transition = shared.account_state.apply_trade_transition_with_context(
                    trade_id,
                    status_str,
                    &runtime_coid,
                    taker_order_id,
                    &asset_id,
                    side,
                    matched_amount,
                    price,
                    false,
                    match_time.business_secs,
                );
                crate::latency::record(
                    "polymarket.user.account_trade_transition",
                    transition_started,
                );
                let transition = if transition.ownership().is_none()
                    && matches!(status_str, "CONFIRMED" | "FAILED")
                    && top_level_maker_matches_account(data, shared)
                {
                    shared
                        .account_state
                        .record_authenticated_terminal_trade_noop(
                            trade_id,
                            status_str,
                            taker_order_id,
                            &asset_id,
                            side,
                            matched_amount,
                            price,
                            false,
                        )
                } else {
                    transition
                };
                let Some(ownership) = transition.ownership().cloned() else {
                    shared.account_state.mark_unresolved_trade_match_time(
                        trade_id,
                        match_time.replay_watermark_secs,
                    );
                    return Vec::new();
                };
                if transition.persistence_pending() {
                    warn!(
                        "[PolymarketUserFeed] owned taker trade {} is applied with persistence pending; broadcasting fill while account admission stays blocked",
                        trade_id,
                    );
                }
                update_trade_finality_replay_anchor(
                    shared,
                    trade_id,
                    status_str,
                    match_time.replay_watermark_secs,
                );
                if match_time.replay_watermark_secs > 0 {
                    live_position.touch_match_time(match_time.replay_watermark_secs);
                }
                if transition.owned_noop() {
                    return Vec::new();
                }
                let coid = ownership.client_order_id;
                let live_position_started = crate::latency::Instant::now();
                let lifecycle_advanced = record_trade_transition(
                    live_position,
                    trade_id,
                    status_str,
                    &asset_id,
                    side,
                    matched_amount,
                    price,
                    false,
                    reason_ref,
                );
                crate::latency::record(
                    "polymarket.user.account_live_position",
                    live_position_started,
                );
                if !lifecycle_advanced {
                    return Vec::new();
                }
                if status != OrderStatus::Failed {
                    shared.request_filled_order_cleanup(&coid);
                }

                let update = OrderUpdate {
                    order_slot: ownership.order_slot,
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
                    exchange_event_timestamp_ns: None,
                    trade_id: if trade_id.is_empty() {
                        None
                    } else {
                        Some(trade_id.to_string())
                    },
                    order_audit: None,
                    error: failure_reason.clone(),
                };
                shared.log_order_lifecycle(
                    &update.client_order_id,
                    "private_trade",
                    update.exchange_order_id.as_deref(),
                    Some(update.status),
                    update.trade_id.as_deref(),
                );
                updates.push(update);
            }

            if status == OrderStatus::Failed
                && failure_reason.is_none()
                && !updates.is_empty()
                && replay.admit_failed_diagnostic(trade_id)
            {
                // A venue trade can legitimately contain maker legs owned by
                // several configured accounts. Keep every account-scoped
                // OrderUpdate above, but emit the venue-level missing-reason
                // diagnostic only once across all account feeds.
                let tx_hash = data
                    .get("transaction_hash")
                    .or_else(|| data.get("transactionHash"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("<missing>");
                let maker_legs = data
                    .get("maker_orders")
                    .and_then(serde_json::Value::as_array)
                    .map_or(0, Vec::len);
                shared.enqueue_private_diagnostic(trade_id, tx_hash, maker_legs);
            }

            updates
        }
        _ => Vec::new(),
    }
}

/// Fetch trades newer than `after_secs` from the authenticated CLOB `/data/trades`
/// endpoint and replay them through the update channel.
async fn replay_missed_trades(
    shared: &SharedState,
    apply_lane: &PrivateApplyLane,
    checkpoint: &mut GapReplayCheckpoint,
    transport: &GapReplayTransport,
    recovery_generation: Option<u64>,
) -> Result<GapReplayOutcome> {
    let pages_before_attempt = checkpoint.pages;
    let result = replay_missed_trades_inner(
        shared,
        apply_lane,
        checkpoint,
        transport,
        recovery_generation,
    )
    .await;
    shared
        .account_state
        .record_gap_replay_pages(checkpoint.pages.saturating_sub(pages_before_attempt));
    result
}

async fn replay_missed_trades_inner(
    shared: &SharedState,
    apply_lane: &PrivateApplyLane,
    checkpoint: &mut GapReplayCheckpoint,
    transport: &GapReplayTransport,
    recovery_generation: Option<u64>,
) -> Result<GapReplayOutcome> {
    // Whole-wallet catch-up: L2 auth already restricts `/data/trades` to this
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
    // One page can contain enough JSON/account work to monopolise the
    // current-thread general runtime. Yield after every page so the private
    // user socket and other control-plane tasks remain responsive. The public
    // CLOB reader is additionally isolated on its own runtime.
    const PAGES_PER_YIELD: usize = 1;
    let mut attempt_pages = 0usize;
    loop {
        if checkpoint.pending_next_cursor.is_some() {
            let apply_stage = crate::latency::TimedStage::new("polymarket.gap_replay.apply_page");
            while !checkpoint.pending_page.is_empty() {
                let batch_len = checkpoint.pending_page.len().min(GAP_APPLY_BATCH_SIZE);
                // Retain the exact batch until the account owner acknowledges
                // it. A rejected/closed completion is therefore retryable at
                // this record boundary without refetching the page.
                let owned: Vec<_> = checkpoint
                    .pending_page
                    .iter()
                    .take(batch_len)
                    .cloned()
                    .collect();
                let owner_turn_started = crate::latency::Instant::now();
                let apply_result = apply_lane
                    .apply_replay_batch(owned, recovery_generation)
                    .await;
                crate::latency::record(
                    "polymarket.gap_replay.owner_turn_roundtrip",
                    owner_turn_started,
                );
                let summary = apply_result.map_err(|error| {
                    anyhow!(
                        "Gap-fetch /data/trades rejected private batch after {} records: {}",
                        checkpoint.records,
                        error,
                    )
                })?;
                checkpoint.durable_fast_skips = checkpoint
                    .durable_fast_skips
                    .saturating_add(summary.durable_skips);
                debug_assert!(summary.applied.saturating_add(summary.durable_skips) <= batch_len);
                checkpoint.pending_page.drain(..batch_len);
                tokio::task::yield_now().await;
            }
            let next = checkpoint
                .pending_next_cursor
                .as_ref()
                .cloned()
                .ok_or_else(|| {
                    anyhow!(
                        "Gap-fetch /data/trades lost its pending cursor after {} records",
                        checkpoint.records,
                    )
                })?;
            let has_next =
                advance_gap_cursor(&mut checkpoint.cursor, &mut checkpoint.seen_cursors, next)?;
            checkpoint.pending_next_cursor = None;
            checkpoint.records = checkpoint
                .records
                .saturating_add(checkpoint.pending_page_records);
            checkpoint.pending_page_records = 0;
            checkpoint.pages += 1;
            attempt_pages += 1;
            if !has_next {
                drop(apply_stage);
                break;
            }
            drop(apply_stage);
            if attempt_pages % PAGES_PER_YIELD == 0 {
                tokio::task::yield_now().await;
            }
            continue;
        }

        let page_stage = crate::latency::TimedStage::new("polymarket.gap_replay.page_total");
        let url = if checkpoint.cursor.is_empty() {
            format!("{}/data/trades?after={}", CLOB_BASE_URL, after_param)
        } else {
            format!(
                "{}/data/trades?after={}&next_cursor={}",
                CLOB_BASE_URL, after_param, checkpoint.cursor
            )
        };
        let gap_response = match transport.get(shared, &url).await {
            Ok(response) => response,
            Err(error) => {
                return Err(anyhow!(
                    "Gap-fetch /data/trades request failed after {} records: {}",
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
                    let failure =
                        GapSendFailure::from_reqwest(response_slot, response_generation, &error);
                    let detail = format!("<response body read failed: {}>", failure);
                    transport.report_body_failure(permit, failure).await;
                    return Err(anyhow!(
                        "Gap-fetch /data/trades HTTP {} after {} records: {}",
                        code,
                        checkpoint.records,
                        detail,
                    ));
                }
            };
            let invalid_cursor = !checkpoint.cursor.is_empty()
                && checkpoint.cursor_resets == 0
                && matches!(code, 400 | 422)
                && body.to_ascii_lowercase().contains("cursor");
            if invalid_cursor {
                warn!(
                    "[PolyUserFeed] GapReplay cursor rejected with HTTP {}; restarting pinned after={} once",
                    code,
                    checkpoint.after_secs,
                );
                checkpoint.cursor.clear();
                checkpoint.seen_cursors.clear();
                checkpoint.cursor_resets = 1;
                continue;
            }
            return Err(anyhow!(
                "Gap-fetch /data/trades HTTP {} after {} records: {}",
                code,
                checkpoint.records,
                body,
            ));
        }
        let body_started = crate::latency::Instant::now();
        let body = match resp.bytes().await {
            Ok(body) => {
                crate::latency::record("polymarket.gap_replay.http_body", body_started);
                permit.pooled_client().note_transport_success();
                body
            }
            Err(error) => {
                crate::latency::record("polymarket.gap_replay.http_body", body_started);
                let failure =
                    GapSendFailure::from_reqwest(response_slot, response_generation, &error);
                if failure.is_transport() {
                    let detail = failure.to_string();
                    transport.report_body_failure(permit, failure).await;
                    return Err(anyhow!(
                        "Gap-fetch /data/trades parse failed after {} records: {}",
                        checkpoint.records,
                        detail,
                    ));
                }
                permit.pooled_client().note_transport_success();
                return Err(anyhow!(
                    "Gap-fetch /data/trades parse failed after {} records: {}",
                    checkpoint.records,
                    failure,
                ));
            }
        };
        let json_started = crate::latency::Instant::now();
        let json_result = serde_json::from_slice::<serde_json::Value>(&body);
        crate::latency::record("polymarket.gap_replay.json_decode", json_started);
        let json = json_result.map_err(|error| {
            anyhow!(
                "Gap-fetch /data/trades JSON decode failed after {} records: {}",
                checkpoint.records,
                error,
            )
        })?;
        // The response body is fully consumed; release the exclusive global
        // slot before routing/deduplicating records.
        drop(permit);

        let (records, next) = if let Some(arr) = json.as_array() {
            (arr.clone(), String::new())
        } else if let Some(object) = json.as_object() {
            let data = object.get("data").and_then(serde_json::Value::as_array).cloned()
                .ok_or_else(|| anyhow!(
                    "Gap-fetch /data/trades returned object without an array `data` field after {} records",
                    checkpoint.records,
                ))?;
            let next = match object.get("next_cursor") {
                None | Some(serde_json::Value::Null) => String::new(),
                Some(serde_json::Value::String(next)) => next.clone(),
                Some(_) => {
                    return Err(anyhow!(
                        "Gap-fetch /data/trades returned non-string `next_cursor` after {} records",
                        checkpoint.records,
                    ))
                }
            };
            (data, next)
        } else {
            return Err(anyhow!(
                "Gap-fetch /data/trades returned neither an array nor a paginated object after {} records",
                checkpoint.records,
            ));
        };

        let record_count = records.len();
        let mut dedupe = HashSet::with_capacity(record_count);
        let mut deltas = Vec::with_capacity(record_count);
        for mut rec in records {
            if let Some(obj) = rec.as_object_mut() {
                obj.entry("event_type".to_string())
                    .or_insert(serde_json::Value::String("trade".to_string()));
            }
            let Some(delta) = PrivateEventDelta::classify(rec) else {
                continue;
            };
            if dedupe.insert(delta.dedupe_key()) {
                deltas.push(delta);
            }
        }
        debug_assert!(checkpoint.pending_page.is_empty());
        debug_assert!(checkpoint.pending_next_cursor.is_none());
        checkpoint.pending_page = deltas.into();
        checkpoint.pending_page_records = record_count;
        checkpoint.pending_next_cursor = Some(next);
        drop(page_stage);
        // Apply through the staged-page branch above. Keeping fetch and apply
        // as separate turns also lets the live socket run after JSON decode.
        tokio::task::yield_now().await;
    }

    Ok(GapReplayOutcome::Complete {
        records: checkpoint.records,
    })
}

/// Async WebSocket loop. Spawned as a tokio task on the shared runtime.
async fn user_feed_loop(
    api_key: String,
    api_secret: String,
    passphrase: String,
    shared: Arc<SharedState>,
    update_tx: Sender<RoutedOrderUpdate>,
    apply_lane: PrivateApplyLane,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = crate::exchange::ReconnectBackoff::new(100, 30_000);
    // First connect is also treated as "recovering" so the strategy stays
    // paused until the first batch of state (and gap replay) is in.
    shared.user_feed_health.set_recovering(true);
    if shared.live_position_last_match_secs() == 0 {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0);
        shared.publish_live_position_watermark(now_secs);
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
            account_id, prewarm.total,
        );
    }
    // The transport is stateless; the account pool itself owns its two slot
    // permits. Let periodic and reconnect replay compete for separate slots
    // instead of serializing both behind an unrelated process-local mutex.
    let gap_transport = Arc::new(transport);
    // Retain both the lower bound and exact next_cursor across reconnects.
    // A transient failure therefore resumes the failed page without either
    // skipping the original window or redownloading its completed prefix.
    let mut recovery_checkpoint: Option<GapReplayCheckpoint> = None;

    // Periodic gap-replay task — independent of the WS read loop so its HTTP
    // call never pauses WS reads. While reconnect recovery is active it yields
    // to that recovery pass, so no untracked periodic update can race the
    // worker-delivery barrier. Cadence and
    // rewind window are config-driven (`gap_replay.interval_ms` /
    // `periodic_rewind_ms`; defaults 2s / 10s — the rewind is a FLOOR,
    // the sweep also always reaches back to the last server-timestamped
    // trade seen, so longer WS gaps stay covered). The status dedup in
    // The durable ledger and update_trade both dedupe lifecycle transitions;
    // unchanged durable replays skip persistence/fsync, while genuinely
    // dropped transitions still reach the owning strategy. A rewind larger
    // than the cadence means a fill is covered by ≥2 sweeps even with
    // match_time second-quantization jitter.
    //
    // When the active event changes (new condition_id, incl. the first seed
    // after startup), the very next sweep does a one-shot now−300s DEEP
    // catch-up of that market so a mid-event (re)start recovers all of its
    // fills — then reverts to the configured rewind window.
    {
        let shared = shared.clone();
        let apply_lane = apply_lane.clone();
        let shutdown = shutdown.clone();
        let gap_transport = gap_transport.clone();
        // New task → won't inherit the loop's span; re-attach the same
        // per-account span so gap-recovery logs are tagged too.
        let gap_span = tracing::info_span!("user_feed", acct = %account_id);
        tokio::spawn(tracing::Instrument::instrument(
            async move {
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
                let mut next_delay = interval;
                loop {
                    sleep(next_delay).await;
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    if shared.user_feed_health.is_recovering() {
                        continue;
                    }
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    let floor_after = if !did_startup_deep {
                        did_startup_deep = true;
                        (now_ms / 1000).saturating_sub(300) // startup → deep catch-up
                    } else {
                        // Dynamic rewind: max(configured floor, now − last trade
                        // the feed actually delivered, on the SERVER match_time
                        // axis) — i.e. `after = min(now − floor, last_trade − 1)`.
                        // A WS drop longer than the floor is then still covered:
                        // the window always reaches back to the last fill we have
                        // seen. −1 s guards Polymarket's strict-`>` semantics on
                        // `?after=T`; the overlap is deduped by trade_id.
                        now_ms.saturating_sub(rewind_ms) / 1000 // rewind (ms) → floor to sec
                    };
                    let committed_secs = shared.live_position_last_match_secs();
                    let replay_anchor = replay_match_time_anchor(&shared, committed_secs);
                    let after = if replay_anchor > 0 {
                        floor_after.min(replay_anchor.saturating_sub(1))
                    } else {
                        floor_after
                    };
                    let checkpoint =
                        periodic_checkpoint.get_or_insert_with(|| GapReplayCheckpoint::new(after));
                    let after = checkpoint.after_secs;
                    let replay_result = replay_missed_trades(
                        &shared,
                        &apply_lane,
                        checkpoint,
                        &gap_transport,
                        None,
                    )
                    .await;
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
                            let (_, last_business_event_ns) =
                                shared.user_feed_health.activity_timestamps_ns();
                            next_delay = adaptive_healthy_gap_replay_delay(
                                interval,
                                now_ns(),
                                last_business_event_ns,
                            );
                            periodic_checkpoint = None;
                            shared.user_feed_health.set_gap_replay_degraded(false);
                        }
                        Err(e) => {
                            consecutive_failures = consecutive_failures.saturating_add(1);
                            next_delay = periodic_gap_retry_delay(interval, consecutive_failures);
                            {
                                let newly_degraded = !shared.user_feed_health.gap_replay_degraded();
                                shared.user_feed_health.set_gap_replay_degraded(true);
                                if newly_degraded {
                                    let (acquires, skips, busy, slots) =
                                        crate::http1_pool::gap_replay_stats(&account_id)
                                            .unwrap_or((0, 0, 0, Vec::new()));
                                    warn!(
                                    "[PolyUserFeed] Periodic gap replay DEGRADED after {} \
                                     consecutive failures; after={} remains pinned; live WS \
                                     inventory stays authoritative and quoting continues while \
                                     background catch-up retries in {}ms; error={}; \
                                     account={} GapReplay pool slots={:?} acquires={} skips={} busy={}",
                                    consecutive_failures,
                                    after,
                                    next_delay.as_millis(),
                                    e,
                                    account_id,
                                    slots,
                                    acquires,
                                    skips,
                                    busy,
                                );
                                } else if periodic_gap_failure_reminder(consecutive_failures) {
                                    warn!(
                                        "[PolyUserFeed] Periodic gap replay still degraded: attempt={} pinned_after={} next_retry_ms={} error={}",
                                        consecutive_failures,
                                        after,
                                        next_delay.as_millis(),
                                        e,
                                    );
                                } else {
                                    debug!(
                                        "[PolyUserFeed] Periodic gap replay retry failed: attempt={} pinned_after={} next_retry_ms={} error={}",
                                        consecutive_failures,
                                        after,
                                        next_delay.as_millis(),
                                        e,
                                    );
                                }
                            }
                        }
                    }
                }
            },
            gap_span,
        ));
    }

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // Connect
        let ws_stream = match tokio_tungstenite::connect_async(WS_URL).await {
            Ok((ws, _)) => ws,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!(
                    "[PolyUserFeed] Connect failed: {}, retrying in {:.1}s",
                    e,
                    delay.as_secs_f64()
                );
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
        let recovery_generation = shared.user_feed_health.begin_recovery_delivery();

        // Gap recovery on (re)connect — whole-wallet, rewind
        // `gap_replay.reconnect_rewind_ms` (default 5s, quantised up to whole
        // seconds) before the last-seen match_time so a fill that landed right
        // around the disconnect edge isn't skipped by an exact `after=`
        // boundary. Idempotent via the upsert_trade / update_trade status
        // dedup. Covers ALL active markets on this wallet at once.
        let last_match_time_secs = shared.live_position_last_match_secs();
        let replay_anchor = replay_match_time_anchor(&shared, last_match_time_secs);
        let checkpoint = recovery_checkpoint.get_or_insert_with(|| {
            GapReplayCheckpoint::new(replay_anchor.saturating_sub(reconnect_rewind_secs))
        });
        let after_secs = checkpoint.after_secs;
        let replay_result = replay_missed_trades(
            &shared,
            &apply_lane,
            checkpoint,
            &gap_transport,
            Some(recovery_generation),
        )
        .await;
        let durable_fast_skips = checkpoint.durable_fast_skips;
        match replay_result {
            Ok(outcome) => {
                match outcome {
                    GapReplayOutcome::Complete { records } => {
                        info!(
                            "[PolyUserFeed] Gap recovery after={} replayed={} trades durable_fast_skips={} (complete)",
                            after_secs, records, durable_fast_skips,
                        );
                    }
                }
                recovery_checkpoint = None;
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

        let recovery_executor = PolymarketTrade::from_shared(shared.clone(), "", "");
        let open_order_recovery = tokio::task::spawn_blocking(move || {
            recovery_executor.reconcile_runtime_open_orders_with_updates()
        })
        .await;
        match open_order_recovery {
            Ok(Ok(updates)) => {
                let mut enqueue_error = None;
                for update in updates {
                    if let Err(error) =
                        enqueue_recovery_update(&shared, &update_tx, recovery_generation, update)
                    {
                        enqueue_error = Some(error);
                        break;
                    }
                }
                if let Some(error) = enqueue_error {
                    shared.user_feed_health.set_recovering(true);
                    let delay = backoff.next_delay();
                    warn!(
                        "[PolyUserFeed] reconnect update delivery failed: {}; keeping quoting paused and reconnecting in {:.1}s",
                        error,
                        delay.as_secs_f64(),
                    );
                    if !shutdown.load(Ordering::Relaxed) {
                        sleep(delay).await;
                    }
                    continue;
                }
                if !shared
                    .user_feed_health
                    .finish_recovery_delivery_enrollment(recovery_generation)
                {
                    warn!(
                        "[PolyUserFeed] reconnect delivery generation={} was superseded; keeping recovery asserted",
                        recovery_generation,
                    );
                    continue;
                }
                if let Err(error) =
                    wait_for_recovery_delivery(&shared, recovery_generation, &update_tx, &shutdown)
                        .await
                {
                    shared.user_feed_health.set_recovering(true);
                    let delay = backoff.next_delay();
                    warn!(
                        "[PolyUserFeed] reconnect updates were not fully processed: {}; keeping quoting paused and reconnecting in {:.1}s",
                        error,
                        delay.as_secs_f64(),
                    );
                    if !shutdown.load(Ordering::Relaxed) {
                        sleep(delay).await;
                    }
                    continue;
                }
                accept_reconnect_replay(&shared, GapReplayOutcome::Complete { records: 0 });
                backoff.reset();
            }
            Ok(Err(error)) => {
                shared.user_feed_health.set_recovering(true);
                let delay = backoff.next_delay();
                warn!("[PolyUserFeed] Open-order recovery failed: {}; keeping quoting paused and reconnecting in {:.1}s", error, delay.as_secs_f64());
                if !shutdown.load(Ordering::Relaxed) {
                    sleep(delay).await;
                }
                continue;
            }
            Err(error) => {
                shared.user_feed_health.set_recovering(true);
                let delay = backoff.next_delay();
                warn!("[PolyUserFeed] Open-order recovery task failed: {}; keeping quoting paused and reconnecting in {:.1}s", error, delay.as_secs_f64());
                if !shutdown.load(Ordering::Relaxed) {
                    sleep(delay).await;
                }
                continue;
            }
        }

        let mut last_ping = Instant::now();
        // Transport heartbeats prove only that the socket is alive. They must
        // not masquerade as validated private order/trade traffic.
        let mut last_transport = Instant::now();
        let apply_reconnect_generation = apply_lane.reconnect_generation.load(Ordering::Acquire);

        // Event loop
        loop {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }

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
            let read_result = tokio::select! {
                _ = apply_lane.reconnect_notify.notified() => {
                    if apply_lane.reconnect_generation.load(Ordering::Acquire)
                        != apply_reconnect_generation
                    {
                        break;
                    }
                    continue;
                }
                result = timeout(READ_TIMEOUT, stream.next()) => result,
            };
            match read_result {
                Ok(Some(Ok(msg))) => {
                    match msg {
                        Message::Text(text) => {
                            last_transport = Instant::now();
                            shared.user_feed_health.record_transport_activity(now_ns());
                            let body = text.trim();
                            if body.eq_ignore_ascii_case("PONG") || body.is_empty() {
                                continue;
                            }
                            if body.eq_ignore_ascii_case("PING") {
                                let _ = sink.send(Message::Text("PONG".to_string())).await;
                                continue;
                            }

                            let frame_started = crate::latency::Instant::now();
                            let json_started = crate::latency::Instant::now();
                            // simd-json drop-in for SIMD parse speedup.
                            let mut buf = text.as_bytes().to_vec();
                            let data = match simd_json::serde::from_slice::<serde_json::Value>(
                                &mut buf,
                            ) {
                                Ok(data) => {
                                    crate::latency::record(
                                        "polymarket.user.json_parse",
                                        json_started,
                                    );
                                    data
                                }
                                Err(error) => {
                                    crate::latency::record(
                                        "polymarket.user.json_parse",
                                        json_started,
                                    );
                                    shared.user_feed_health.set_recovering(true);
                                    shared.user_feed_health.set_inventory_uncertain(true);
                                    let raw: String = text.chars().take(256).collect();
                                    let (_, last_valid_ns) =
                                        shared.user_feed_health.activity_timestamps_ns();
                                    let valid_age_s = if last_valid_ns == 0 {
                                        -1.0
                                    } else {
                                        now_ns().saturating_sub(last_valid_ns) as f64 / 1e9
                                    };
                                    warn!(
                                        "[PolyUserFeed] invalid private WS JSON after {:.3}s without a validated business event: {}; forcing reconnect; raw={}",
                                        valid_age_s, error, raw,
                                    );
                                    break;
                                }
                            };
                            let events = if data.is_array() {
                                data.as_array().cloned().unwrap_or_default()
                            } else {
                                vec![data]
                            }
                            .into_iter()
                            .filter_map(PrivateEventDelta::classify)
                            .filter(|event| !event.is_registered_probe_order(&shared))
                            .collect();
                            let apply_enqueue_started = crate::latency::Instant::now();
                            if let Err(error) = apply_lane.dispatch_live(events) {
                                shared.user_feed_health.set_recovering(true);
                                shared.user_feed_health.set_inventory_uncertain(true);
                                warn!(
                                    "[PolyUserFeed] {}; forcing reconnect before private updates can be lost",
                                    error,
                                );
                                crate::latency::record(
                                    "polymarket.user.apply_enqueue",
                                    apply_enqueue_started,
                                );
                                crate::latency::record(
                                    "polymarket.user.frame_total",
                                    frame_started,
                                );
                                crate::latency::record(
                                    "polymarket.user.event_parse",
                                    frame_started,
                                );
                                break;
                            }
                            crate::latency::record(
                                "polymarket.user.apply_enqueue",
                                apply_enqueue_started,
                            );
                            crate::latency::record("polymarket.user.frame_total", frame_started);
                            // Compatibility aggregate for existing live
                            // dashboards; the four stage metrics above are the
                            // actionable breakdown.
                            crate::latency::record("polymarket.user.event_parse", frame_started);
                        }
                        Message::Ping(payload) => {
                            last_transport = Instant::now();
                            shared.user_feed_health.record_transport_activity(now_ns());
                            let _ = sink.send(Message::Pong(payload)).await;
                        }
                        Message::Pong(_) => {
                            last_transport = Instant::now();
                            shared.user_feed_health.record_transport_activity(now_ns());
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
                    if last_transport.elapsed() > STALE_TIMEOUT {
                        warn!("[PolyUserFeed] No data for 30s, reconnecting");
                        break;
                    }
                }
            }
        }

        // Disconnected
        info!("[PolyUserFeed] Disconnected, will reconcile on reconnect");
        shared.user_feed_health.set_recovering(true);
        let last_match_time_secs = shared.live_position_last_match_secs();
        let replay_anchor = replay_match_time_anchor(&shared, last_match_time_secs);
        recovery_checkpoint.get_or_insert_with(|| {
            GapReplayCheckpoint::new(replay_anchor.saturating_sub(reconnect_rewind_secs))
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
    update_tx: Sender<RoutedOrderUpdate>,
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
    let (apply_lane, apply_workers) = spawn_private_apply_worker(
        Arc::clone(&shared),
        update_tx.clone(),
        Arc::clone(&shutdown),
    )?;
    let task_handle = async_rt::handle().spawn(
        user_feed_loop(
            api_key, api_secret, passphrase, shared, update_tx, apply_lane, shutdown,
        )
        .instrument(tracing::info_span!("user_feed", acct = %acct)),
    );

    let handle = std::thread::Builder::new()
        .name("poly-user-feed-join".into())
        .spawn(move || {
            crate::os_tune::pin_background("poly-user-feed-join");
            async_rt::block_on_runtime(async move {
                let _ = task_handle.await;
            });
            for worker in apply_workers {
                let _ = worker.join();
            }
        })?;

    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn exchange_event_timestamp_normalizes_seconds_millis_micros_and_nanos() {
        let seconds = receipt_time_secs().saturating_sub(1);
        assert_eq!(
            exchange_event_timestamp_ns(&serde_json::json!({"match_time": seconds})),
            Some(seconds * 1_000_000_000),
        );
        assert_eq!(
            exchange_event_timestamp_ns(&serde_json::json!({"timestamp": seconds * 1_000})),
            Some(seconds * 1_000_000_000),
        );
        assert_eq!(
            exchange_event_timestamp_ns(&serde_json::json!({"timestamp": seconds * 1_000_000})),
            Some(seconds * 1_000_000_000),
        );
        assert_eq!(
            exchange_event_timestamp_ns(&serde_json::json!({"timestamp": seconds * 1_000_000_000})),
            Some(seconds * 1_000_000_000),
        );
    }

    #[test]
    fn private_route_fingerprint_is_case_stable_and_role_scoped() {
        let maker = private_route_fingerprint(b'm', &["Trade-1", "0xAbCd"]);
        assert_eq!(
            maker,
            private_route_fingerprint(b'm', &["trade-1", "0xabcd"])
        );
        assert_ne!(
            maker,
            private_route_fingerprint(b't', &["trade-1", "0xabcd"])
        );
    }

    #[test]
    fn replay_lifecycle_dedupe_advances_rank_and_keeps_failed_correction() {
        let mut dedupe = GapReplayLifecycleDedupe::new(8);
        assert!(!dedupe.seen("trade-1", 1));
        dedupe.remember("trade-1", 1);
        assert!(dedupe.seen("trade-1", 1));
        assert!(!dedupe.seen("trade-1", 2));
        dedupe.remember("trade-1", 3);
        assert!(dedupe.seen("trade-1", 2));
        assert!(!dedupe.seen("trade-1", 4));
        dedupe.remember("trade-1", 4);
        assert!(dedupe.seen("trade-1", 4));
    }

    #[test]
    fn private_delta_classification_preserves_lifecycle_edges_for_dedupe() {
        let matched = PrivateEventDelta::classify(serde_json::json!({
            "event_type": "trade",
            "id": "trade-1",
            "status": "MATCHED",
            "match_time": 123,
        }))
        .unwrap();
        let duplicate = PrivateEventDelta::classify(serde_json::json!({
            "event_type": "trade",
            "id": "trade-1",
            "status": "MATCHED",
            "match_time": 123,
        }))
        .unwrap();
        let confirmed = PrivateEventDelta::classify(serde_json::json!({
            "event_type": "trade",
            "id": "trade-1",
            "status": "CONFIRMED",
            "match_time": 123,
        }))
        .unwrap();

        assert!(matches!(matched, PrivateEventDelta::Trade(_)));
        assert_eq!(matched.dedupe_key(), duplicate.dedupe_key());
        assert_ne!(matched.dedupe_key(), confirmed.dedupe_key());
        assert!(PrivateEventDelta::classify(serde_json::json!({
            "event_type": "heartbeat"
        }))
        .is_none());
    }

    #[test]
    fn failed_trade_diagnostic_is_venue_scoped_bounded_and_non_business() {
        let mut dedupe = FailedTradeDiagnosticDedupe::new(2);
        assert!(dedupe.admit("venue-trade-a"));
        assert!(!dedupe.admit("venue-trade-a"));
        assert!(dedupe.admit("venue-trade-b"));
        assert!(dedupe.admit("venue-trade-c"));
        assert!(
            dedupe.admit("venue-trade-a"),
            "old diagnostics may be evicted"
        );
    }

    #[test]
    fn periodic_gap_retry_uses_bounded_exponential_backoff_and_sparse_warns() {
        let base = Duration::from_secs(2);
        let delays: Vec<_> = (0..6)
            .map(|failures| periodic_gap_retry_delay(base, failures))
            .collect();
        assert_eq!(
            delays,
            vec![
                Duration::from_secs(2),
                Duration::from_secs(4),
                Duration::from_secs(8),
                Duration::from_secs(16),
                Duration::from_secs(30),
                Duration::from_secs(30),
            ],
        );
        let reminders: Vec<_> = (1..=20)
            .filter(|attempt| periodic_gap_failure_reminder(*attempt))
            .collect();
        assert_eq!(reminders, vec![4, 8, 16]);
    }

    #[test]
    fn healthy_gap_replay_adapts_to_private_activity_without_ignoring_config() {
        let now = 100_000_000_000;
        assert_eq!(
            adaptive_healthy_gap_replay_delay(Duration::from_secs(2), now, now - 1_000_000_000,),
            Duration::from_secs(10),
        );
        assert_eq!(
            adaptive_healthy_gap_replay_delay(Duration::from_secs(2), now, now - 31_000_000_000,),
            Duration::from_secs(30),
        );
        assert_eq!(
            adaptive_healthy_gap_replay_delay(Duration::from_secs(45), now, now),
            Duration::from_secs(45),
        );
    }

    fn test_shared() -> Arc<SharedState> {
        let shared = super::super::trade::PolymarketTrade::new(
            "api-key",
            "c2VjcmV0",
            "passphrase",
            "0x0000000000000000000000000000000000000000000000000000000000000001",
            false,
            10,
            super::super::signer::SignatureType::Eoa,
        )
        .unwrap()
        .shared_state();
        shared.install_strategy_owner_routes(HashMap::from([
            ("owner-1".to_string(), 0),
            ("owner".to_string(), 1),
            ("taker-owner".to_string(), 2),
            ("legacy-owner".to_string(), 3),
            ("maker-a".to_string(), 4),
            ("maker-b".to_string(), 5),
            ("maker-owner".to_string(), 6),
            ("maker".to_string(), 7),
            ("historical-owner".to_string(), 8),
        ]));
        shared
    }

    #[test]
    fn registered_probe_order_is_filtered_before_private_owner_lane() {
        let shared = test_shared();
        shared.probe_order_ids.insert("0xAbC123");
        let probe = PrivateEventDelta::classify(serde_json::json!({
            "event_type": "order",
            "order_id": "0xabc123",
        }))
        .unwrap();
        assert!(probe.is_registered_probe_order(&shared));

        let real_order = PrivateEventDelta::classify(serde_json::json!({
            "event_type": "order",
            "order_id": "0xdef456",
        }))
        .unwrap();
        assert!(!real_order.is_registered_probe_order(&shared));

        let trade = PrivateEventDelta::classify(serde_json::json!({
            "event_type": "trade",
            "id": "0xabc123",
        }))
        .unwrap();
        assert!(!trade.is_registered_probe_order(&shared));

        for index in 0..=64 {
            shared.probe_order_ids.insert(&format!("0x{index:064x}"));
        }
        assert!(!shared.probe_order_ids.contains("0xAbC123"));
        assert!(shared.probe_order_ids.contains(&format!("0x{:064x}", 64)));
    }

    #[test]
    fn private_actor_routes_owned_trade_before_cold_account_apply() {
        let shared = test_shared();
        shared.account_state.register_instance("owner-1", 1.0);
        shared
            .account_state
            .apply_physical_snapshot(100.0, HashMap::new())
            .unwrap();
        shared
            .account_state
            .register_token_fee_config(&["TOKEN".to_string()], 0.0, 1.0)
            .unwrap();
        shared
            .account_state
            .reserve_order(
                "owner-1",
                "owner-1-1",
                "0xabc1",
                "TOKEN",
                Side::Buy,
                5.0,
                0.5,
                0,
            )
            .unwrap();
        shared.register_order_id("owner-1-1", "0xabc1", "TOKEN");
        let event = PrivateEventDelta::classify(serde_json::json!({
            "event_type": "trade",
            "id": "fast-route-trade",
            "status": "MATCHED",
            "asset_id": "TOKEN",
            "side": "BUY",
            "size": "2",
            "price": "0.5",
            "taker_order_id": "0xabc1",
            "maker_orders": []
        }))
        .unwrap();

        let before = shared.account_state.order("owner-1-1").unwrap();
        let routed = route_private_event_fast(&event, &shared).unwrap();
        assert_eq!(routed.len(), 1);
        assert_eq!(routed[0].update.client_order_id, "owner-1-1");
        assert_eq!(
            shared
                .account_state
                .order("owner-1-1")
                .unwrap()
                .filled_quantity,
            before.filled_quantity,
            "fast validation/routing must not synchronously mutate the shared ledger",
        );

        let cold = parse_user_event_with_health(event.payload(), &shared);
        assert!(cold.valid_business_event);
        assert!(
            shared
                .account_state
                .order("owner-1-1")
                .unwrap()
                .filled_quantity
                > before.filled_quantity
        );
    }

    #[test]
    fn cold_duplicate_is_suppressed_only_after_commit_ack() {
        let shared = test_shared();
        shared.account_state.register_instance("owner-1", 1.0);
        shared
            .account_state
            .apply_physical_snapshot(100.0, HashMap::new())
            .unwrap();
        shared
            .account_state
            .register_token_fee_config(&["TOKEN".to_string()], 0.0, 1.0)
            .unwrap();
        shared
            .account_state
            .reserve_order(
                "owner-1",
                "owner-1-1",
                "0xabc1",
                "TOKEN",
                Side::Buy,
                5.0,
                0.5,
                0,
            )
            .unwrap();
        shared.register_order_id("owner-1-1", "0xabc1", "TOKEN");
        let event = || {
            PrivateEventDelta::classify(serde_json::json!({
                "event_type": "trade",
                "id": "cold-ack-trade",
                "status": "MATCHED",
                "asset_id": "TOKEN",
                "side": "BUY",
                "size": "2",
                "price": "0.5",
                "taker_order_id": "0xabc1",
                "maker_orders": []
            }))
            .unwrap()
        };
        let (update_tx, _update_rx) = crossbeam_channel::bounded(8);
        let mut route_dedupe = PrivateRouteDedupe::new();
        let mut cold_committed = PrivateRouteDedupe::new();

        let first = route_private_batch(
            &shared,
            &update_tx,
            vec![event()],
            None,
            &mut route_dedupe,
            Some(&cold_committed),
        )
        .unwrap();
        assert_eq!(first.events.len(), 1);
        assert_eq!(first.identities.len(), 1);

        apply_private_cold_batch(&shared, &first.events, None).unwrap();
        for identity in first.identities {
            cold_committed.remember(identity);
        }

        let duplicate = route_private_batch(
            &shared,
            &update_tx,
            vec![event()],
            None,
            &mut route_dedupe,
            Some(&cold_committed),
        )
        .unwrap();
        assert!(duplicate.events.is_empty());
        assert!(duplicate.identities.is_empty());
    }

    #[test]
    fn retired_confirmed_trade_skips_historical_matched_cold_apply() {
        let shared = test_shared();
        shared.account_state.register_instance("owner-1", 1.0);
        shared
            .account_state
            .apply_physical_snapshot(100.0, HashMap::new())
            .unwrap();
        shared
            .account_state
            .register_token_fee_config(&["TOKEN".to_string()], 0.0, 1.0)
            .unwrap();
        shared
            .account_state
            .reserve_order(
                "owner-1",
                "owner-1-retired",
                "oid-retired",
                "TOKEN",
                Side::Buy,
                2.0,
                0.5,
                0,
            )
            .unwrap();
        assert!(shared
            .account_state
            .apply_trade_transition_with_context(
                "retired-gap-trade",
                "CONFIRMED",
                "owner-1-retired",
                "oid-retired",
                "TOKEN",
                Side::Buy,
                2.0,
                0.5,
                false,
                123,
            )
            .ownership()
            .is_some());
        shared
            .account_state
            .release_order("owner-1-retired", OrderStatus::Filled);
        assert_eq!(
            shared
                .account_state
                .prune_terminal_history(&HashSet::from(["TOKEN".to_string()])),
            (1, 1),
        );

        let event = PrivateEventDelta::classify(serde_json::json!({
            "event_type": "trade",
            "id": "retired-gap-trade",
            "status": "MATCHED",
            "asset_id": "TOKEN",
            "side": "BUY",
            "size": "2",
            "price": "0.5",
            "taker_order_id": "oid-retired",
            "maker_orders": []
        }))
        .unwrap();
        let (update_tx, update_rx) = crossbeam_channel::bounded(8);
        let mut route_dedupe = PrivateRouteDedupe::new();
        let routed = route_private_batch(
            &shared,
            &update_tx,
            vec![event.clone()],
            Some(1),
            &mut route_dedupe,
            None,
        )
        .unwrap();

        assert!(routed.events.is_empty());
        assert!(routed.identities.is_empty());
        assert_eq!(routed.durable_skips, 1);
        assert!(update_rx.is_empty());

        const BATCHES: usize = 128;
        let account_locks_before = shared
            .account_state
            .monitoring_snapshot_fast()
            .account_lock_acquisitions;
        let mut samples = Vec::with_capacity(BATCHES);
        for _ in 0..BATCHES {
            let started = std::time::Instant::now();
            let routed = route_private_batch(
                &shared,
                &update_tx,
                (0..GAP_APPLY_BATCH_SIZE).map(|_| event.clone()).collect(),
                Some(1),
                &mut route_dedupe,
                None,
            )
            .unwrap();
            assert_eq!(routed.durable_skips, GAP_APPLY_BATCH_SIZE);
            assert!(routed.events.is_empty());
            samples.push(started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        }
        let account_locks_after = shared
            .account_state
            .monitoring_snapshot_fast()
            .account_lock_acquisitions;
        samples.sort_unstable();
        let percentile = |numerator: usize, denominator: usize| {
            samples[(samples.len() - 1) * numerator / denominator]
        };
        let p50 = percentile(1, 2);
        let p99 = percentile(99, 100);
        let p999 = percentile(999, 1_000);
        let max = *samples.last().unwrap();
        eprintln!(
            "durable gap replay: boundary=route_batch_to_cold_bypass records={} batches={} batch_size={} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth={} overflow=0 account_lock_acquisitions={}",
            BATCHES * GAP_APPLY_BATCH_SIZE,
            BATCHES,
            GAP_APPLY_BATCH_SIZE,
            p50,
            p99,
            p999,
            max,
            update_rx.len(),
            account_locks_after.saturating_sub(account_locks_before),
        );
        assert_eq!(account_locks_after, account_locks_before);
        assert!(
            p99 < 100_000_000,
            "eight-record durable replay P99 exceeded 100ms: {p99}ns",
        );
    }

    #[test]
    fn gap_replay_handoff_is_bounded_and_amortized() {
        assert!(GAP_APPLY_BATCH_SIZE > 1);
        assert!(GAP_APPLY_BATCH_SIZE <= 8);
    }

    fn record(manager: &Mutex<LivePositionManager>, status: &str) -> bool {
        let mut manager = manager.lock().unwrap();
        record_trade_transition(
            &mut manager,
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
        assert!(
            !record(&manager, "CONFIRMED"),
            "FAILED cannot flip terminal"
        );
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
            "match_time": "123",
            "taker_order_id": "oid-final",
            "maker_orders": [],
        });

        assert!(parse_user_event(&event, &shared).is_empty());
        assert!(shared.account_state.is_uncertain());
        assert_eq!(
            shared.account_state.earliest_unresolved_trade_match_time(),
            Some(123),
        );
        assert_eq!(shared.live_position_last_match_secs(), 0);

        shared.account_state.rebind_order_id("owner-1", "oid-final");
        shared.register_order_id("owner-1", "oid-final", "TOKEN");
        let updates = parse_user_event(&event, &shared);
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].client_order_id, "owner-1");
        assert!(!shared.account_state.is_uncertain());
        assert_eq!(
            shared.account_state.earliest_unresolved_trade_match_time(),
            Some(123),
            "an owned MATCHED trade must keep gap replay pinned until finality",
        );
        assert_eq!(shared.live_position_last_match_secs(), 123);

        let mut confirmed = event;
        confirmed["status"] = serde_json::json!("CONFIRMED");
        let updates = parse_user_event(&confirmed, &shared);
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].client_order_id, "owner-1");
        assert_eq!(
            shared.account_state.earliest_unresolved_trade_match_time(),
            None,
            "terminal finality releases the replay anchor",
        );
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
            shared
                .account_state
                .instance_snapshot("maker-a")
                .unwrap()
                .cash,
            before_a.cash,
            "a casing/prefix-only lifecycle replay must not book twice",
        );
        assert_eq!(
            shared
                .account_state
                .instance_snapshot("maker-b")
                .unwrap()
                .cash,
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
        shared
            .track_open_order(
                "owner-1",
                super::super::trade::TrackedOrder {
                    order_slot: Default::default(),
                    symbol: "TOKEN".to_string(),
                    side: Side::Buy,
                    instance_id: "owner".to_string(),
                },
            )
            .unwrap();
        shared.register_order_id("owner-1", "oid-final", "TOKEN");
        shared.flush_execution_state_for_test();

        shared.remove_order_as("owner-1", OrderStatus::Filled);
        shared.flush_execution_state_for_test();
        assert!(shared
            .execution_snapshot()
            .open_orders
            .contains_key("owner-1"));
        assert_eq!(
            shared
                .account_state
                .instance_snapshot("owner")
                .unwrap()
                .reserved_cash,
            5.0,
        );
        assert_eq!(
            shared
                .account_state
                .monitoring_snapshot()
                .recovery_pending_orders,
            1
        );

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
        let cleanup_deadline = Instant::now() + Duration::from_millis(100);
        while shared
            .execution_snapshot()
            .open_orders
            .contains_key("owner-1")
            && Instant::now() < cleanup_deadline
        {
            std::thread::yield_now();
        }
        assert!(!shared
            .execution_snapshot()
            .open_orders
            .contains_key("owner-1"));
        assert_eq!(
            shared
                .account_state
                .instance_snapshot("owner")
                .unwrap()
                .reserved_cash,
            0.0,
        );
        assert_eq!(
            shared
                .account_state
                .monitoring_snapshot()
                .recovery_pending_orders,
            0
        );
    }

    #[test]
    fn reconnect_health_clears_only_after_a_successful_replay() {
        let shared = test_shared();
        assert!(shared.user_feed_health.is_recovering());

        // A failed REST result never reaches `accept_reconnect_replay`.
        let failed: Result<GapReplayOutcome> = Err(anyhow!("temporary REST failure"));
        if let Ok(outcome) = failed {
            accept_reconnect_replay(&shared, outcome);
        }
        assert!(
            shared.user_feed_health.is_recovering(),
            "REST failure must keep quoting paused",
        );

        accept_reconnect_replay(&shared, GapReplayOutcome::Complete { records: 3 });
        assert!(!shared.user_feed_health.is_recovering());
        assert!(!shared.user_feed_health.inventory_uncertain());
    }

    #[test]
    fn gap_cursor_continues_past_batch_boundaries_and_rejects_loops() {
        let mut cursor = String::new();
        let mut seen = HashSet::new();

        for page in 1..=75 {
            assert!(advance_gap_cursor(&mut cursor, &mut seen, format!("cursor-{page}"),).unwrap());
        }
        assert_eq!(cursor, "cursor-75");
        assert!(advance_gap_cursor(&mut cursor, &mut seen, "cursor-75".to_string(),).is_err());
        assert!(!advance_gap_cursor(&mut cursor, &mut seen, "LTE=".to_string(),).unwrap());
    }

    #[test]
    fn gap_replay_checkpoint_keeps_window_cursor_and_progress_for_retry() {
        let mut checkpoint = GapReplayCheckpoint::new(997);
        assert!(advance_gap_cursor(
            &mut checkpoint.cursor,
            &mut checkpoint.seen_cursors,
            "page-2".to_string(),
        )
        .unwrap());
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
        )
        .unwrap());
        assert_eq!(checkpoint.cursor, "page-3");
    }

    #[test]
    fn gap_replay_checkpoint_retains_exact_failed_record_before_cursor_commit() {
        let mut checkpoint = GapReplayCheckpoint::new(997);
        checkpoint.pending_page = VecDeque::from([
            PrivateEventDelta::Trade(serde_json::json!({
                "event_type": "trade", "id": "already-applied"
            })),
            PrivateEventDelta::Trade(serde_json::json!({
                "event_type": "trade", "id": "failed-record"
            })),
        ]);
        checkpoint.pending_page_records = 2;
        checkpoint.pending_next_cursor = Some("page-2".to_string());

        // Model the first owner acknowledgement, then an error on the next
        // record. The failed record and old page cursor remain intact.
        checkpoint.pending_page.pop_front();
        assert_eq!(checkpoint.pending_page.len(), 1);
        assert_eq!(
            checkpoint.pending_page.front().unwrap().dedupe_key(),
            "trade:failed-record|||",
        );
        assert_eq!(checkpoint.cursor, "");
        assert_eq!(checkpoint.pending_next_cursor.as_deref(), Some("page-2"));

        // Only a fully acknowledged page may advance the server cursor.
        checkpoint.pending_page.pop_front();
        let next = checkpoint.pending_next_cursor.take().unwrap();
        assert!(
            advance_gap_cursor(&mut checkpoint.cursor, &mut checkpoint.seen_cursors, next,)
                .unwrap()
        );
        assert_eq!(checkpoint.cursor, "page-2");
    }

    fn owned_taker_shared(limit_price: f64) -> Arc<SharedState> {
        let shared = test_shared();
        shared.account_state.register_instance("owner", 1.0);
        shared
            .account_state
            .apply_physical_snapshot(100.0, HashMap::new())
            .unwrap();
        shared
            .account_state
            .register_token_fee_config(&["TOKEN".to_string()], 0.0, 1.0)
            .unwrap();
        shared
            .account_state
            .reserve_order(
                "owner",
                "owner-1",
                "oid-1",
                "TOKEN",
                Side::Buy,
                10.0,
                limit_price,
                0,
            )
            .unwrap();
        shared.register_order_id("owner-1", "oid-1", "TOKEN");
        shared
            .track_open_order(
                "owner-1",
                super::super::trade::TrackedOrder {
                    order_slot: Default::default(),
                    symbol: "TOKEN".to_string(),
                    side: Side::Buy,
                    instance_id: "owner".to_string(),
                },
            )
            .unwrap();
        shared.flush_execution_state_for_test();
        shared
    }

    fn valid_taker_event() -> serde_json::Value {
        serde_json::json!({
            "event_type": "trade", "id": "trade-strict", "status": "MATCHED",
            "asset_id": "TOKEN", "side": "BUY", "size": "10", "price": "0.5",
            "match_time": "123", "taker_order_id": "oid-1", "maker_orders": [],
        })
    }

    #[test]
    fn trade_identity_lifecycle_and_economics_are_all_strictly_required() {
        let shared = owned_taker_shared(0.5);
        for field in [
            "id",
            "status",
            "side",
            "asset_id",
            "size",
            "price",
            "taker_order_id",
        ] {
            let mut event = valid_taker_event();
            event.as_object_mut().unwrap().remove(field);
            assert!(
                validate_trade_event(&event, &shared).is_err(),
                "missing `{field}`"
            );
        }
        for (field, invalid) in [
            ("status", serde_json::json!("UNKNOWN")),
            ("side", serde_json::json!("HOLD")),
            ("size", serde_json::json!("NaN")),
            ("price", serde_json::json!("Infinity")),
        ] {
            let mut event = valid_taker_event();
            event[field] = invalid;
            assert!(
                validate_trade_event(&event, &shared).is_err(),
                "invalid `{field}`"
            );
        }
    }

    #[test]
    fn trade_float_validation_allows_only_relative_rounding_noise() {
        let shared = owned_taker_shared(1.0);
        let mut event = valid_taker_event();
        event["price"] = serde_json::json!("1.000000005");
        let updates = parse_user_event(&event, &shared);
        assert_eq!(updates.len(), 1);
        assert!((updates[0].avg_fill_price - 1.000000005).abs() < 1e-12);
        let mut invalid = valid_taker_event();
        invalid["price"] = serde_json::json!("1.0001");
        assert!(validate_trade_event(&invalid, &owned_taker_shared(1.0)).is_err());
    }

    #[test]
    fn private_trade_without_owned_maker_or_taker_evidence_is_rejected() {
        let shared = test_shared();
        let event = serde_json::json!({
            "event_type": "trade",
            "id": "unknown-role",
            "status": "MATCHED",
            "asset_id": "TOKEN",
            "side": "BUY",
            "size": "1",
            "price": "0.5",
            "taker_order_id": "somebody-elses-order",
            "maker_orders": [],
        });
        let error = validate_trade_event(&event, &shared).unwrap_err();
        assert!(error.contains("maker/taker role is unknown"), "{error}");
        assert!(parse_user_event(&event, &shared).is_empty());
        assert!(shared.account_state.is_uncertain());
    }

    #[test]
    fn owned_maker_leg_does_not_require_untrusted_taker_economics() {
        let shared = test_shared();
        shared.account_state.register_instance("maker", 1.0);
        shared
            .account_state
            .apply_physical_snapshot(100.0, HashMap::new())
            .unwrap();
        shared
            .account_state
            .reserve_order(
                "maker",
                "maker-coid",
                "maker-oid",
                "TOKEN",
                Side::Buy,
                2.0,
                0.4,
                0,
            )
            .unwrap();
        shared.register_order_id("maker-coid", "maker-oid", "TOKEN");
        let event = serde_json::json!({
            "event_type": "trade",
            "id": "maker-without-taker-fields",
            "status": "MATCHED",
            "maker_orders": [{
                "maker_address": shared.order_maker_address.clone(),
                "asset_id": "TOKEN",
                "side": "BUY",
                "matched_amount": "2",
                "price": "0.4",
                "order_id": "maker-oid"
            }]
        });
        assert!(validate_trade_event(&event, &shared).is_ok());
        let updates = parse_user_event(&event, &shared);
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].liquidity, Some(Liquidity::Maker));
    }

    #[test]
    fn order_lifecycle_updates_release_on_cancel_and_restore_on_late_placement() {
        let shared = owned_taker_shared(0.5);
        let placement = serde_json::json!({
            "event_type": "order", "type": "PLACEMENT", "id": "oid-1",
            "asset_id": "TOKEN", "side": "BUY", "price": "0.5",
            "original_size": "10", "size_matched": "0",
        });
        let updates = parse_user_event(&placement, &shared);
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].status, OrderStatus::Accepted);
        assert_eq!(updates[0].filled_quantity, 0.0);
        let mut cancellation = placement;
        cancellation["type"] = serde_json::json!("CANCELLATION");
        assert_eq!(
            parse_user_event(&cancellation, &shared)[0].status,
            OrderStatus::Cancelled
        );
        shared.flush_execution_state_for_test();
        assert_eq!(
            shared
                .account_state
                .instance_snapshot("owner")
                .unwrap()
                .reserved_cash,
            0.0
        );
        assert!(!shared
            .execution_snapshot()
            .open_orders
            .contains_key("owner-1"));

        let resurrection = serde_json::json!({
            "event_type": "order", "type": "PLACEMENT", "id": "oid-1",
            "asset_id": "TOKEN", "side": "BUY", "price": "0.5",
            "original_size": "10", "size_matched": "0",
        });
        assert_eq!(
            parse_user_event(&resurrection, &shared)[0].status,
            OrderStatus::Accepted,
        );
        shared.flush_execution_state_for_test();
        assert_eq!(
            shared
                .account_state
                .instance_snapshot("owner")
                .unwrap()
                .reserved_cash,
            5.0
        );
        assert!(shared
            .execution_snapshot()
            .open_orders
            .contains_key("owner-1"));

        let shared = owned_taker_shared(0.5);
        let mut update = serde_json::json!({
            "event_type": "order", "type": "UPDATE", "id": "oid-1",
            "asset_id": "TOKEN", "side": "BUY", "price": "0.5",
            "original_size": "10", "size_matched": "4",
            "associate_trades": ["trade-partial"],
        });
        assert_eq!(
            parse_user_event(&update, &shared)[0].status,
            OrderStatus::PartiallyFilled
        );
        shared.flush_execution_state_for_test();
        assert_eq!(
            shared
                .account_state
                .instance_snapshot("owner")
                .unwrap()
                .reserved_cash,
            5.0,
            "order lifecycle must not book or release trade-driven inventory"
        );
        update["size_matched"] = serde_json::json!("10");
        assert_eq!(
            parse_user_event(&update, &shared)[0].status,
            OrderStatus::Filled
        );
        shared.flush_execution_state_for_test();
        assert_eq!(
            shared
                .account_state
                .instance_snapshot("owner")
                .unwrap()
                .reserved_cash,
            5.0,
            "whole-order Filled holds the lock until trade rows arrive"
        );
        assert_eq!(
            shared
                .account_state
                .monitoring_snapshot()
                .recovery_pending_orders,
            1
        );
    }

    #[test]
    fn late_placement_lifecycle_cannot_regress_partial_fill() {
        let shared = owned_taker_shared(0.5);
        let partial = serde_json::json!({
            "event_type": "order", "type": "UPDATE", "id": "oid-1",
            "asset_id": "TOKEN", "side": "BUY", "price": "0.5",
            "original_size": "10", "size_matched": "4",
            "associate_trades": ["trade-partial"],
        });
        assert_eq!(
            parse_user_event(&partial, &shared)[0].status,
            OrderStatus::PartiallyFilled
        );
        shared.flush_execution_state_for_test();

        let placement = serde_json::json!({
            "event_type": "order", "type": "PLACEMENT", "id": "oid-1",
            "asset_id": "TOKEN", "side": "BUY", "price": "0.5",
            "original_size": "10", "size_matched": "0",
        });
        assert!(parse_user_event(&placement, &shared).is_empty());
        shared.flush_execution_state_for_test();
        assert_eq!(
            shared.account_state.order("owner-1").unwrap().status,
            OrderStatus::PartiallyFilled,
        );
    }

    #[test]
    fn partial_fill_cancellation_retains_only_unreplayed_matched_reservation() {
        let shared = owned_taker_shared(0.5);
        let cancellation = serde_json::json!({
            "event_type": "order", "type": "CANCELLATION", "id": "oid-1",
            "asset_id": "TOKEN", "side": "BUY", "price": "0.5",
            "original_size": "10", "size_matched": "4",
            "associate_trades": ["trade-strict"],
        });
        let updates = parse_user_event(&cancellation, &shared);
        assert_eq!(updates[0].status, OrderStatus::Cancelled);
        shared.flush_execution_state_for_test();
        assert_eq!(
            shared
                .account_state
                .instance_snapshot("owner")
                .unwrap()
                .reserved_cash,
            2.0
        );
        assert_eq!(
            shared
                .account_state
                .monitoring_snapshot()
                .recovery_pending_orders,
            1
        );

        let mut trade = valid_taker_event();
        trade["size"] = serde_json::json!("4");
        assert_eq!(parse_user_event(&trade, &shared).len(), 1);
        shared.flush_execution_state_for_test();
        assert_eq!(
            shared
                .account_state
                .instance_snapshot("owner")
                .unwrap()
                .reserved_cash,
            0.0
        );
        assert_eq!(
            shared
                .account_state
                .monitoring_snapshot()
                .recovery_pending_orders,
            0
        );
    }

    #[test]
    fn matched_order_lifecycle_requires_strict_associate_trade_ids() {
        for associate_trades in [
            serde_json::json!(null),
            serde_json::json!("trade-1"),
            serde_json::json!([]),
            serde_json::json!(["trade-1", 2]),
            serde_json::json!(["trade-1", "trade-1"]),
        ] {
            let shared = owned_taker_shared(0.5);
            let mut update = serde_json::json!({
                "event_type": "order", "type": "UPDATE", "id": "oid-1",
                "asset_id": "TOKEN", "side": "BUY", "price": "0.5",
                "original_size": "10", "size_matched": "4",
            });
            update["associate_trades"] = associate_trades;
            assert!(parse_user_event(&update, &shared).is_empty());
            assert_eq!(
                shared
                    .account_state
                    .order("owner-1")
                    .unwrap()
                    .filled_quantity,
                0.0
            );
            assert!(shared.account_state.is_uncertain());
        }

        let shared = owned_taker_shared(0.5);
        let unmatched = serde_json::json!({
            "event_type": "order", "type": "PLACEMENT", "id": "oid-1",
            "asset_id": "TOKEN", "side": "BUY", "price": "0.5",
            "original_size": "10", "size_matched": "0",
            "associate_trades": [],
        });
        assert_eq!(parse_user_event(&unmatched, &shared).len(), 1);
        assert!(!shared.account_state.is_uncertain());
    }

    #[test]
    fn malformed_later_maker_leg_cannot_partially_book_earlier_leg() {
        let shared = test_shared();
        shared.account_state.register_instance("maker-a", 1.0);
        shared.account_state.register_instance("maker-b", 1.0);
        shared
            .account_state
            .apply_physical_snapshot(200.0, HashMap::new());
        for (instance, coid, oid) in [
            ("maker-a", "maker-a-1", "oid-a"),
            ("maker-b", "maker-b-1", "oid-b"),
        ] {
            shared
                .account_state
                .reserve_order(instance, coid, oid, "TOKEN", Side::Buy, 5.0, 0.5, 0)
                .unwrap();
            shared.register_order_id(coid, oid, "TOKEN");
        }
        let event = serde_json::json!({
            "event_type": "trade", "id": "maker-atomic", "status": "MATCHED",
            "asset_id": "OTHER", "side": "SELL", "size": "10", "price": "0.5",
            "taker_order_id": "other", "maker_orders": [
                {"maker_address": shared.order_maker_address.clone(), "asset_id": "TOKEN",
                 "side": "BUY", "matched_amount": "5", "price": "0.5", "order_id": "oid-a"},
                {"maker_address": shared.order_maker_address.clone(), "asset_id": "TOKEN",
                 "side": "BUY", "matched_amount": "5", "price": "NaN", "order_id": "oid-b"}
            ]
        });
        assert!(parse_user_event(&event, &shared).is_empty());
        assert_eq!(
            shared
                .account_state
                .order("maker-a-1")
                .unwrap()
                .filled_quantity,
            0.0
        );
        assert_eq!(
            shared
                .account_state
                .order("maker-b-1")
                .unwrap()
                .filled_quantity,
            0.0
        );
        assert!(shared.account_state.is_uncertain());
    }

    #[test]
    fn failed_trade_is_terminal_but_does_not_terminalize_parent_order() {
        let shared = owned_taker_shared(0.5);
        let mut event = valid_taker_event();
        event["status"] = serde_json::json!("FAILED");
        let updates = parse_user_event(&event, &shared);
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].status, OrderStatus::Failed);
        shared.flush_execution_state_for_test();
        assert_eq!(
            shared
                .account_state
                .instance_snapshot("owner")
                .unwrap()
                .reserved_cash,
            5.0
        );
        assert_eq!(
            shared
                .account_state
                .monitoring_snapshot()
                .recovery_pending_orders,
            0
        );
        assert!(!shared.account_state.is_uncertain());
        shared.flush_execution_state_for_test();
        assert!(shared
            .execution_snapshot()
            .open_orders
            .contains_key("owner-1"));

        let stale_placement = serde_json::json!({
            "event_type": "order", "type": "PLACEMENT", "id": "oid-1",
            "asset_id": "TOKEN", "side": "BUY", "price": "0.5",
            "original_size": "10", "size_matched": "0",
        });
        assert_eq!(parse_user_event(&stale_placement, &shared).len(), 1);
        shared.flush_execution_state_for_test();
        assert_eq!(
            shared.account_state.order("owner-1").unwrap().status,
            OrderStatus::Accepted
        );
        assert_eq!(
            shared
                .account_state
                .instance_snapshot("owner")
                .unwrap()
                .reserved_cash,
            5.0
        );
    }

    #[test]
    fn unowned_trade_with_invalid_match_time_keeps_receipt_replay_anchor() {
        let shared = test_shared();
        let mut event = valid_taker_event();
        event["taker_order_id"] = serde_json::json!("unknown-order");
        event["match_time"] = serde_json::json!("not-a-time");
        assert!(parse_user_event(&event, &shared).is_empty());
        assert!(shared.account_state.is_uncertain());
        assert!(shared
            .account_state
            .earliest_unresolved_trade_match_time()
            .is_some_and(|anchor| anchor > 0));
    }

    #[test]
    fn future_business_match_time_is_capped_for_replay_only() {
        let receipt = receipt_time_secs();
        let event = serde_json::json!({
            "match_time": receipt.saturating_add(120),
        });
        let effective = effective_match_time(&event, "future-trade");
        assert_eq!(effective.business_secs, receipt.saturating_add(120));
        assert!(effective.replay_watermark_secs <= receipt_time_secs());
        assert!(effective.replay_watermark_secs < effective.business_secs);
    }

    #[test]
    fn unknown_private_event_cannot_clear_order_schema_anomaly() {
        let shared = test_shared();
        shared
            .account_state
            .mark_private_event_anomaly("order:oid-unknown", "test anomaly");
        let ignored = serde_json::json!({
            "event_type": "subscription_ack",
            "id": "oid-unknown",
        });
        let parsed = parse_user_event_with_health(&ignored, &shared);
        assert!(!parsed.valid_business_event);
        assert!(!parsed.invalid_business_event);
        assert!(shared.account_state.is_uncertain());
        assert!(shared
            .account_state
            .ownership_anomalies()
            .contains_key("private_event:order:oid-unknown"));
    }

    #[test]
    fn corrected_order_schema_event_clears_anomaly_and_recovery_gate() {
        let shared = owned_taker_shared(0.5);
        let mut update = serde_json::json!({
            "event_type": "order", "type": "UPDATE", "id": "oid-1",
            "asset_id": "TOKEN", "side": "BUY", "price": "0.5",
            "original_size": "10", "size_matched": "4",
        });
        let rejected = parse_user_event_with_health(&update, &shared);
        assert!(rejected.invalid_business_event);
        assert!(rejected
            .rejection_reason
            .as_deref()
            .is_some_and(|reason| { reason.contains("missing associate_trades") }));
        assert!(shared.account_state.is_uncertain());
        assert_eq!(
            shared
                .account_state
                .monitoring_snapshot()
                .recovery_pending_orders,
            1
        );

        update["associate_trades"] = serde_json::json!(["trade-partial"]);
        let corrected = parse_user_event_with_health(&update, &shared);
        assert!(corrected.valid_business_event);
        assert_eq!(corrected.updates.len(), 1);
        assert!(corrected.rejection_reason.is_none());
        assert!(!shared.account_state.is_uncertain());
        assert_eq!(
            shared
                .account_state
                .monitoring_snapshot()
                .recovery_pending_orders,
            0
        );
    }

    #[test]
    fn valid_duplicate_trade_is_a_noop_not_a_parser_rejection() {
        let shared = owned_taker_shared(0.5);
        let event = valid_taker_event();
        let first = parse_user_event_diagnosed(&event, &shared);
        assert!(first.valid_business_event);
        assert_eq!(first.updates.len(), 1);
        assert!(first.rejection_reason.is_none());

        let duplicate = parse_user_event_diagnosed(&event, &shared);
        assert!(duplicate.valid_business_event);
        assert!(!duplicate.invalid_business_event);
        assert!(duplicate.updates.is_empty());
        assert!(duplicate.rejection_reason.is_none());
    }

    #[test]
    fn retired_taker_trade_replay_uses_durable_ownership_and_emits_no_fill() {
        let shared = owned_taker_shared(0.5);
        let mut event = valid_taker_event();
        event["status"] = serde_json::json!("CONFIRMED");
        let first = parse_user_event_diagnosed(&event, &shared);
        assert!(first.valid_business_event);
        assert_eq!(first.updates.len(), 1);

        let before = shared.account_state.monitoring_snapshot();
        assert_eq!(
            shared
                .account_state
                .prune_terminal_history(&HashSet::from(["TOKEN".to_string()])),
            (1, 1),
        );
        shared.clear_execution_state_for_test();
        assert!(shared.lookup_coid("oid-1").is_none());
        assert!(shared.account_state.order_owner_by_oid("oid-1").is_none());

        shared.user_feed_health.set_inventory_uncertain(true);
        let replay = parse_user_event_diagnosed(&event, &shared);
        assert!(replay.valid_business_event);
        assert!(!replay.invalid_business_event);
        assert!(replay.updates.is_empty());
        assert!(replay.rejection_reason.is_none());
        assert!(!shared.account_state.is_uncertain());
        assert!(!shared.user_feed_health.inventory_uncertain());
        let after = shared.account_state.monitoring_snapshot();
        assert_eq!(after.physical_cash, before.physical_cash);
        assert_eq!(after.physical_positions, before.physical_positions);
        assert_eq!(after.retired_trade_ownership_tombstones, 1);
    }

    #[test]
    fn fast_route_accepts_retired_terminal_maker_trade_without_parent_order() {
        let shared = owned_taker_shared(0.5);
        let event = serde_json::json!({
            "event_type": "trade",
            "id": "retired-maker-trade",
            "status": "CONFIRMED",
            "asset_id": "COUNTERPARTY",
            "side": "SELL",
            "size": "10",
            "price": "0.5",
            "match_time": "123",
            "taker_order_id": "counterparty-order",
            "maker_orders": [{
                "maker_address": shared.order_maker_address.clone(),
                "asset_id": "TOKEN",
                "side": "BUY",
                "matched_amount": "10",
                "price": "0.5",
                "order_id": "oid-1"
            }]
        });
        let first = parse_user_event_diagnosed(&event, &shared);
        assert!(first.valid_business_event);
        assert_eq!(first.updates.len(), 1);
        assert_eq!(
            shared
                .account_state
                .prune_terminal_history(&HashSet::from(["TOKEN".to_string()])),
            (1, 1),
        );
        shared.clear_execution_state_for_test();

        let event = PrivateEventDelta::classify(event).unwrap();
        let routed = route_private_event_fast(&event, &shared).unwrap();
        assert!(routed.is_empty());

        // The cold path remains the durable validator and treats the replay
        // as an owned no-op rather than reopening or reapplying the fill.
        let cold = parse_user_event_with_health(event.payload(), &shared);
        assert!(cold.valid_business_event);
        assert!(!cold.invalid_business_event);
        assert!(cold.updates.is_empty());
    }

    #[test]
    fn fast_route_defers_unknown_authenticated_terminal_maker_to_cold_recovery() {
        let shared = test_shared();
        shared
            .account_state
            .register_instance("historical-owner", 1.0);
        shared
            .account_state
            .register_token_interest("historical-owner", "historical-condition", "TOKEN", "OTHER")
            .unwrap();
        shared
            .account_state
            .apply_physical_snapshot(100.0, HashMap::from([("TOKEN".to_string(), 0.0)]))
            .unwrap();
        shared
            .account_state
            .record_settled_token_values(&HashMap::from([("TOKEN".to_string(), 1.0)]));
        let payload = serde_json::json!({
            "event_type": "trade",
            "id": "unknown-historical-maker",
            "status": "CONFIRMED",
            "asset_id": "COUNTERPARTY",
            "side": "BUY",
            "size": "7",
            "price": "0.4",
            "match_time": "123",
            "taker_order_id": "counterparty-order",
            "maker_orders": [{
                "maker_address": shared.order_maker_address.clone(),
                "asset_id": "TOKEN",
                "side": "SELL",
                "matched_amount": "7",
                "price": "0.6",
                "order_id": "unknown-historical-oid"
            }]
        });

        let event = PrivateEventDelta::classify(payload).unwrap();
        let routed = route_private_event_fast(&event, &shared).unwrap();
        assert!(
            routed.is_empty(),
            "historical recovery must not emit a fill"
        );
        assert!(!shared.account_state.is_uncertain());

        let cold = parse_user_event_with_health(event.payload(), &shared);
        assert!(cold.valid_business_event);
        assert!(!cold.invalid_business_event);
        assert!(cold.updates.is_empty());
        assert!(cold.rejection_reason.is_none());
        assert_eq!(
            shared
                .account_state
                .trade_ownership("unknown-historical-maker:unknown-historical-oid")
                .unwrap()
                .instance_id,
            "historical-owner",
        );
    }

    #[test]
    fn fast_route_does_not_suppress_failed_after_matched_high_water() {
        let shared = owned_taker_shared(0.5);
        let matched = valid_taker_event();
        let first = parse_user_event_diagnosed(&matched, &shared);
        assert!(first.valid_business_event);
        assert_eq!(first.updates.len(), 1);

        let mut failed = matched;
        failed["status"] = serde_json::json!("FAILED");
        let failed = PrivateEventDelta::classify(failed).unwrap();
        let routed = route_private_event_fast(&failed, &shared).unwrap();
        assert_eq!(routed.len(), 1);
        assert_eq!(routed[0].update.status, OrderStatus::Failed);
    }

    #[test]
    fn authenticated_settled_historical_taker_recovers_as_noop_without_order_row() {
        let shared = test_shared();
        shared.account_state.register_instance("owner", 1.0);
        shared
            .account_state
            .apply_physical_snapshot(100.0, HashMap::from([("TOKEN".to_string(), 1.0)]))
            .unwrap();
        shared
            .account_state
            .record_settled_token_values(&HashMap::from([("TOKEN".to_string(), 1.0)]));
        let event = serde_json::json!({
            "event_type": "trade",
            "id": "historical-trade",
            "status": "CONFIRMED",
            "asset_id": "TOKEN",
            "side": "BUY",
            "size": "6.24",
            "price": "0.42",
            "match_time": "123",
            "taker_order_id": "historical-oid",
            "maker_address": shared.order_maker_address.clone(),
            "maker_orders": [{
                "maker_address": "0x0000000000000000000000000000000000000002",
                "asset_id": "TOKEN",
                "side": "SELL",
                "matched_amount": "6.24",
                "price": "0.42",
                "order_id": "counterparty-oid"
            }],
        });

        let before = shared.account_state.monitoring_snapshot();
        let recovered = parse_user_event_diagnosed(&event, &shared);
        assert!(recovered.valid_business_event);
        assert!(!recovered.invalid_business_event);
        assert!(recovered.updates.is_empty());
        assert!(recovered.rejection_reason.is_none());
        assert!(!shared.account_state.is_uncertain());
        assert!(shared.account_state.ownership_anomalies().is_empty());

        let replay = parse_user_event_diagnosed(&event, &shared);
        assert!(replay.valid_business_event);
        assert!(replay.updates.is_empty());
        let after = shared.account_state.monitoring_snapshot();
        assert_eq!(after.physical_cash, before.physical_cash);
        assert_eq!(after.physical_positions, before.physical_positions);
        assert_eq!(after.retired_trade_ownership_tombstones, 1);
    }

    #[test]
    fn corrected_trade_replay_replaces_receipt_anchor_with_business_anchor() {
        let shared = owned_taker_shared(0.5);
        let mut event = valid_taker_event();
        event["side"] = serde_json::json!("HOLD");
        event["match_time"] = serde_json::json!("not-a-time");
        assert!(parse_user_event(&event, &shared).is_empty());
        assert!(shared.account_state.is_uncertain());
        assert!(shared.user_feed_health.inventory_uncertain());
        assert!(shared
            .account_state
            .earliest_unresolved_trade_match_time()
            .is_some_and(|anchor| anchor > 0));

        event["side"] = serde_json::json!("BUY");
        event["match_time"] = serde_json::json!("123");
        let updates = parse_user_event(&event, &shared);
        assert_eq!(updates.len(), 1);
        assert!(!shared.account_state.is_uncertain());
        assert!(!shared.user_feed_health.inventory_uncertain());
        assert_eq!(
            shared.account_state.earliest_unresolved_trade_match_time(),
            Some(123),
            "a corrected MATCHED event clears the receipt-time syntax anchor but remains replayable from its authenticated business time",
        );
    }

    #[test]
    fn terminal_gap_replay_dedupe_survives_page_and_sweep_boundaries() {
        let mut replay = PrivateReplayOwner::new();
        let terminal = PrivateEventDelta::classify(serde_json::json!({
            "event_type": "trade",
            "id": "trade-terminal-dedupe",
            "status": "CONFIRMED"
        }))
        .unwrap();
        let key = terminal.terminal_trade_key().unwrap();
        assert!(!replay.terminal_seen(&key));
        replay.remember_terminal(&key);
        assert!(replay.terminal_seen(&key));
        let advancing = PrivateEventDelta::classify(serde_json::json!({
            "event_type": "trade",
            "id": "trade-terminal-dedupe",
            "status": "FAILED"
        }))
        .unwrap();
        assert_ne!(advancing.terminal_trade_key().unwrap(), key);
    }
}
