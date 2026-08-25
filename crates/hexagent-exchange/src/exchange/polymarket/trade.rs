//! Polymarket CLOB live order execution.
//!
//! Implements `ExchangeTrade` for submitting and canceling orders via the
//! Polymarket CLOB REST API, with EIP-712 order signing and HMAC request auth.

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hasher;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::{anyhow, Result};
use arc_swap::{ArcSwap, ArcSwapOption};
use bytes::{BufMut, Bytes, BytesMut};
use crossbeam_queue::ArrayQueue;
use hexagent_account::account::shared_account::{
    measure_deferred_lifecycle_stages, normalize_order_id, OrderOwnership,
};
use hexagent_runtime::shutdown::{ShutdownPhase, ShutdownToken};
use log::{debug, info, warn};

use super::auth::PolyAuth;
use super::live_position::LivePositionManager;
use super::signer::{validate_signing_inputs, OrderSigner, SignatureType};
use crate::async_rt;
use crate::exchange::ExchangeTrade;
use crate::types::*;

/// CLOB protocol version selector. Threaded through `SharedState` so
/// every signing / POST / auth path can dispatch at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClobVersion {
    V2,
}

impl ClobVersion {
    /// Parse a config string. Accepts "v1" / "v2" (case-insensitive).
    /// Default is V2 (Polymarket cut over 2026-04-28; v1 wire is dead):
    /// empty string and anything unrecognised resolve to V2. Only an
    /// explicit "v1" / "1" opts back into the legacy v1 path. NOTE: this
    /// `parse` is only reached from `build_poly_shared_states_map`
    /// (live/record), so the default flip cannot change backtests — the
    /// strategy reads the raw `clob_version` string directly, which stays
    /// "" (⇒ v1 behaviour) for any backtest config that doesn't set it.
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "v2" | "2" => Ok(ClobVersion::V2),
            value => Err(anyhow!(
                "unsupported Polymarket clob_version={value:?}; production supports v2 only",
            )),
        }
    }
    pub fn as_str(&self) -> &'static str {
        "v2"
    }
}

/// Default CLOB host when `api_url_prefix` is unset in config.
/// Post-2026-04-28 cutover this host serves the v2 schema directly;
/// the legacy `clob-v2.polymarket.com` staging hostname was folded
/// into the canonical name. Override via `api_url_prefix` only if
/// you need to point at a non-prod environment.
const DEFAULT_CLOB_BASE_URL: &str = "https://clob.polymarket.com";
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const AUTHENTICATED_TRADES_PATH: &str = "/data/trades";
const REQUEST_BUFFER_SLOTS: usize = 64;
const REQUEST_BUFFER_BYTES: usize = 2_048;
const RUNTIME_OWNERSHIP_CAPACITY: usize = 65_536;
const RUNTIME_OWNERSHIP_MAX_PROBES: usize = 128;

fn new_request_buffer_pool() -> Arc<ArrayQueue<BytesMut>> {
    let pool = Arc::new(ArrayQueue::new(REQUEST_BUFFER_SLOTS));
    for _ in 0..REQUEST_BUFFER_SLOTS {
        let _ = pool.push(BytesMut::with_capacity(REQUEST_BUFFER_BYTES));
    }
    pool
}

/// Read-mostly OID ownership publication used by the private owner actor.
/// New-order workers publish into a fixed open-addressed table, while a
/// private event performs one bounded read and never enters the aggregate
/// account lifecycle lock or grows a string map.
#[derive(Debug)]
struct RuntimeOwnershipIndex {
    slots: Box<[ArcSwapOption<RuntimeOwnershipEntry>]>,
}

#[derive(Debug)]
struct RuntimeOwnershipEntry {
    normalized_order_id: Arc<str>,
    ownership: OrderOwnership,
}

impl RuntimeOwnershipIndex {
    fn new() -> Self {
        Self {
            slots: (0..RUNTIME_OWNERSHIP_CAPACITY)
                .map(|_| ArcSwapOption::empty())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    fn start_index(order_id: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        for byte in runtime_order_id_view(order_id).bytes() {
            hasher.write_u8(byte.to_ascii_lowercase());
        }
        hasher.finish() as usize % RUNTIME_OWNERSHIP_CAPACITY
    }

    fn insert(&self, order_id: &str, ownership: OrderOwnership) -> Result<(), String> {
        let normalized = Arc::<str>::from(normalize_order_id(order_id));
        let entry = Arc::new(RuntimeOwnershipEntry {
            normalized_order_id: Arc::clone(&normalized),
            ownership,
        });
        let start = Self::start_index(&normalized);
        for offset in 0..RUNTIME_OWNERSHIP_MAX_PROBES {
            let slot = &self.slots[(start + offset) % self.slots.len()];
            slot.rcu(|current| match current {
                Some(existing) if existing.normalized_order_id.as_ref() != normalized.as_ref() => {
                    Some(Arc::clone(existing))
                }
                _ => Some(Arc::clone(&entry)),
            });
            if slot.load().as_ref().is_some_and(|published| {
                published.normalized_order_id.as_ref() == normalized.as_ref()
            }) {
                return Ok(());
            }
        }
        Err(format!(
            "fixed runtime ownership table probe budget exhausted capacity={} probes={}",
            RUNTIME_OWNERSHIP_CAPACITY, RUNTIME_OWNERSHIP_MAX_PROBES,
        ))
    }

    fn find_entry(&self, order_id: &str) -> Option<Arc<RuntimeOwnershipEntry>> {
        let normalized = runtime_order_id_view(order_id);
        let start = Self::start_index(order_id);
        (0..RUNTIME_OWNERSHIP_MAX_PROBES).find_map(|offset| {
            let entry = self.slots[(start + offset) % self.slots.len()].load();
            entry
                .as_ref()
                .filter(|entry| {
                    entry
                        .normalized_order_id
                        .as_ref()
                        .eq_ignore_ascii_case(normalized)
                })
                .map(Arc::clone)
        })
    }

    fn get(&self, order_id: &str) -> Option<OrderOwnership> {
        self.find_entry(order_id)
            .map(|entry| entry.ownership.clone())
    }

    fn contains(&self, order_id: &str) -> bool {
        let normalized = runtime_order_id_view(order_id);
        let start = Self::start_index(order_id);
        (0..RUNTIME_OWNERSHIP_MAX_PROBES).any(|offset| {
            self.slots[(start + offset) % self.slots.len()]
                .load()
                .as_ref()
                .is_some_and(|entry| {
                    entry
                        .normalized_order_id
                        .as_ref()
                        .eq_ignore_ascii_case(normalized)
                })
        })
    }

    fn client_order_id(&self, order_id: &str) -> Option<String> {
        self.find_entry(order_id)
            .map(|entry| entry.ownership.client_order_id.clone())
    }

    fn remove(&self, order_id: &str) {
        let normalized = runtime_order_id_view(order_id);
        let start = Self::start_index(order_id);
        for offset in 0..RUNTIME_OWNERSHIP_MAX_PROBES {
            let slot = &self.slots[(start + offset) % self.slots.len()];
            if slot.load().as_ref().is_some_and(|entry| {
                entry
                    .normalized_order_id
                    .as_ref()
                    .eq_ignore_ascii_case(normalized)
            }) {
                slot.rcu(|current| match current {
                    Some(entry)
                        if entry
                            .normalized_order_id
                            .as_ref()
                            .eq_ignore_ascii_case(normalized) =>
                    {
                        None
                    }
                    _ => current.clone(),
                });
                return;
            }
        }
    }

    fn remove_client_orders(&self, client_order_ids: &HashSet<String>) {
        for slot in &self.slots {
            slot.rcu(|current| match current {
                Some(entry) if client_order_ids.contains(&entry.ownership.client_order_id) => None,
                _ => current.clone(),
            });
        }
    }

    #[cfg(test)]
    fn clear(&self) {
        for slot in &self.slots {
            slot.store(None);
        }
    }
}

#[inline]
fn runtime_order_id_view(order_id: &str) -> &str {
    let trimmed = order_id.trim();
    trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed)
}

#[derive(Debug, Clone)]
struct FetchedOrder {
    status: String,
    audit: AuthoritativeOrderAudit,
    rebuild_identity: Option<FetchedOrderIdentity>,
}

#[derive(Debug, Clone)]
struct FetchedOrderIdentity {
    maker_address: String,
    asset_id: String,
    side: Side,
    price: f64,
    fee_rate_bps: Option<u32>,
}

struct RuntimeOrderAuditPass {
    updates: Vec<OrderUpdate>,
    errors: Vec<String>,
    not_found: Vec<RuntimeMissingOrder>,
    /// Order-specific not-found/null evidence which becomes terminal only
    /// after the expiry endpoint proves that every market token is retired.
    retired_market_absent: Vec<RuntimeMissingOrder>,
}

#[derive(Clone, Debug)]
struct RuntimeMissingOrder {
    client_order_id: String,
    tracked: TrackedOrder,
    order_id: String,
    evidence: String,
}

const RETIRED_MARKET_TERMINAL_ATTEMPTS: usize = 3;

fn retired_market_terminalization_allowed(
    consecutive_evidence_passes: usize,
    token_count: usize,
    terminal_token_responses: usize,
    audit_error_count: usize,
    absent_order_count: usize,
) -> bool {
    consecutive_evidence_passes >= RETIRED_MARKET_TERMINAL_ATTEMPTS
        && token_count > 0
        && terminal_token_responses == token_count
        // Unrelated transport/schema/ledger failures must break the chain.
        && audit_error_count == absent_order_count
}

fn retired_market_parallel_absence_fast_path_allowed(
    allow_expired_market_terminalization: bool,
    remote_clean: bool,
    audit_error_count: usize,
    absent_orders: &[RuntimeMissingOrder],
) -> bool {
    allow_expired_market_terminalization
        && remote_clean
        && !absent_orders.is_empty()
        && audit_error_count == absent_orders.len()
        && absent_orders
            .iter()
            .all(|missing| missing.evidence.starts_with("parallel_reconcile_absence"))
}

fn is_cancels_disabled_error(text: &str) -> bool {
    text.to_ascii_lowercase().contains("cancels are disabled")
}

fn is_retired_market_terminal_error(
    text: &str,
    allow_expired_market_terminalization: bool,
) -> bool {
    SharedState::is_invalid_token_error(text)
        || (allow_expired_market_terminalization && is_cancels_disabled_error(text))
}

/// Polymarket L2 HMAC signs the endpoint path only. Query parameters are sent
/// on the wire but deliberately excluded from the signing payload, matching
/// the official clients.
fn canonical_l2_auth_path(path: &str) -> &str {
    path.split_once('?').map_or(path, |(endpoint, _)| endpoint)
}

fn terminal_trade_lookup_path(trade_id: &str) -> String {
    format!("{}?id={}", AUTHENTICATED_TRADES_PATH, trade_id)
}

/// Normalize the terminal trade endpoint while preserving the distinction
/// between an absent record and a record rejected later by parsing/invariants.
fn terminal_trade_records(json: serde_json::Value, trade_id: &str) -> Vec<serde_json::Value> {
    let records = if let Some(records) = json.as_array() {
        records.clone()
    } else if let Some(records) = json.get("data").and_then(|value| value.as_array()) {
        records.clone()
    } else if json.get("id").and_then(|value| value.as_str()).is_some() {
        vec![json]
    } else {
        Vec::new()
    };
    records
        .into_iter()
        .filter(|record| record.get("id").and_then(|value| value.as_str()) == Some(trade_id))
        .collect()
}

/// One bounded market-expiry cancel attempt. `confirmed` is true only after
/// every DELETE response has the authoritative schema and the token-scoped
/// order/trade audit has no remaining live or recovery-pending rows.
pub struct MarketCancelFinality {
    pub confirmed: bool,
    pub updates: Vec<OrderUpdate>,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum OrphanOrderRepairFailure {
    LookupTimeout,
    LookupTransport,
    LookupHttp,
    LookupInvalidResponse,
    MissingClientOrderId,
    MissingHistoricalTimestamp,
    TradeAuditUnavailable,
    TradeAuditIncomplete,
    TradeAuditFoundFill,
    EventNotSettled,
    MissingRebuildIdentity,
    MakerMismatch,
    UnknownInstance,
    InvalidOriginalSize,
    OwnershipBackfillRejected,
    NonTerminalOrder,
    TerminalOrderHasFills,
    TombstoneRejected,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrphanOrderStartupRepair {
    pub examined: usize,
    pub rebuilt: usize,
    pub tombstoned: usize,
    pub unresolved: usize,
    pub failures: BTreeMap<OrphanOrderRepairFailure, usize>,
}

impl OrphanOrderStartupRepair {
    fn unresolved(&mut self, failure: OrphanOrderRepairFailure) {
        self.unresolved = self.unresolved.saturating_add(1);
        *self.failures.entry(failure).or_default() += 1;
    }
}

const ORPHAN_TRADE_AUDIT_MAX_PAGES: usize = 64;
const ORPHAN_TRADE_AUDIT_REWIND_SECS: u64 = 300;
const LEGACY_ORPHAN_COMPACTION_FINALITY_MS: u64 = 6 * 60 * 60 * 1_000;

#[derive(Debug)]
enum HistoricalOrderTradeAudit {
    CompleteNoFill { pages: usize, after_secs: u64 },
    FoundFill { records: usize },
    Incomplete { pages: usize },
    Unavailable(String),
}

fn coid_wall_clock_ms(coid: &str) -> Option<u64> {
    let value = coid.rsplit('-').next()?.parse::<u64>().ok()?;
    // Millisecond ids have been used since the first durable ledger format.
    // Bounds reject sequence suffixes and corrupt/future timestamps.
    let now_ms = now_ns() / 1_000_000;
    (value >= 1_500_000_000_000 && value <= now_ms.saturating_add(60_000)).then_some(value)
}

fn trade_record_references_order(record: &serde_json::Value, order_id: &str) -> bool {
    let expected = normalize_order_id(order_id);
    let same = |candidate: &str| normalize_order_id(candidate) == expected;
    record
        .get("taker_order_id")
        .or_else(|| record.get("order_id"))
        .or_else(|| record.get("orderID"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(same)
        || record
            .get("maker_orders")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|orders| {
                orders.iter().any(|order| {
                    order
                        .get("order_id")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(same)
                })
            })
}

fn authenticated_trade_page(
    json: serde_json::Value,
) -> Result<(Vec<serde_json::Value>, String), String> {
    if let Some(records) = json.as_array() {
        return Ok((records.clone(), String::new()));
    }
    let Some(object) = json.as_object() else {
        return Err("response is neither an array nor an object".to_string());
    };
    let records = object
        .get("data")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .ok_or_else(|| "response object has no array `data`".to_string())?;
    let next = match object.get("next_cursor") {
        None | Some(serde_json::Value::Null) => String::new(),
        Some(serde_json::Value::String(next)) => next.clone(),
        Some(_) => return Err("response has a non-string `next_cursor`".to_string()),
    };
    Ok((records, next))
}

/// Result of an orphan order lookup. Keep an explicit server not-found result
/// separate from transport/service/parse failures: only the former is
/// authoritative enough to advance the universal four-result placement
/// terminalization rule.
#[derive(Debug)]
enum FetchOrderResult {
    Found(FetchedOrder),
    NotFound(String),
    Unavailable(FetchUnavailable),
}

fn fetch_order_result_name(result: &FetchOrderResult) -> &'static str {
    match result {
        FetchOrderResult::Found(_) => "found",
        FetchOrderResult::NotFound(_) => "parallel_not_found",
        FetchOrderResult::Unavailable(kind) if kind.is_parallel_absence() => {
            "parallel_absence"
        }
        FetchOrderResult::Unavailable(_) => "unavailable",
    }
}

fn order_lookup_is_absent(result: &FetchOrderResult) -> bool {
    matches!(result, FetchOrderResult::NotFound(_))
        || matches!(result, FetchOrderResult::Unavailable(kind) if kind.is_json_null())
}

fn combine_parallel_order_lookups(
    primary: FetchOrderResult,
    secondary: FetchOrderResult,
    primary_location: (crate::http1_pool::Role, usize),
    secondary_location: (crate::http1_pool::Role, usize),
) -> FetchOrderResult {
    let primary = match primary {
        FetchOrderResult::Found(order) => return FetchOrderResult::Found(order),
        other => other,
    };
    let secondary = match secondary {
        FetchOrderResult::Found(order) => return FetchOrderResult::Found(order),
        other => other,
    };
    if order_lookup_is_absent(&primary) && order_lookup_is_absent(&secondary) {
        let evidence = format!(
            "parallel_reconcile_absence primary={:?}/{} secondary={:?}/{}",
            primary_location.0,
            primary_location.1,
            secondary_location.0,
            secondary_location.1,
        );
        return if matches!(primary, FetchOrderResult::NotFound(_))
            && matches!(secondary, FetchOrderResult::NotFound(_))
        {
            FetchOrderResult::NotFound(evidence)
        } else {
            FetchOrderResult::Unavailable(FetchUnavailable::InvalidResponse(evidence))
        };
    }
    // A single replica's absence cannot override the other slot's transport,
    // service or schema failure. Preserve the non-absence result so no local
    // reservation is released without cross-slot agreement.
    match (primary, secondary) {
        (FetchOrderResult::NotFound(_), other) => other,
        (other, FetchOrderResult::NotFound(_)) => other,
        (primary, _) => primary,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum FetchUnavailable {
    Timeout,
    Transport,
    Http(u16),
    InvalidResponse(String),
}

impl FetchUnavailable {
    /// Polymarket returns a literal JSON `null` for some orders after their
    /// event has closed. The general lookup path deliberately keeps that
    /// response unavailable; only durable-order recovery may terminalize it.
    fn is_json_null(&self) -> bool {
        matches!(self, Self::InvalidResponse(body) if body.trim() == "null" || body.starts_with("parallel_reconcile_absence"))
    }

    fn is_parallel_absence(&self) -> bool {
        matches!(self, Self::InvalidResponse(body) if body.starts_with("parallel_reconcile_absence"))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecoveredOrderCloseReason {
    EventEnded,
    JsonNull,
}

impl RecoveredOrderCloseReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::EventEnded => "event_end_recorded",
            Self::JsonNull => "single_order_lookup_json_null",
        }
    }
}

/// The exception is deliberately recovery-scoped. A literal null remains an
/// unavailable response for ordinary live/orphan lookups.
fn recovered_order_close_reason(
    is_recovered: bool,
    event_has_ended: bool,
    unavailable: Option<&FetchUnavailable>,
) -> Option<RecoveredOrderCloseReason> {
    is_recovered.then_some(())?;
    if event_has_ended {
        return Some(RecoveredOrderCloseReason::EventEnded);
    }
    unavailable
        .is_some_and(FetchUnavailable::is_json_null)
        .then_some(RecoveredOrderCloseReason::JsonNull)
}

/// Query-repair rows must receive an actual CLOB order lookup. An event-end
/// marker can close ordinary recovered orders before I/O, but is only fallback
/// evidence for query repair after the bounded lookup attempts complete.
fn prequery_recovered_order_close_reason(
    is_recovered: bool,
    event_has_ended: bool,
    requires_query_repair: bool,
) -> Option<RecoveredOrderCloseReason> {
    if requires_query_repair {
        None
    } else {
        recovered_order_close_reason(is_recovered, event_has_ended, None)
    }
}

/// Event-end evidence is only a pre-I/O shortcut for an order restored from
/// durable recovery. Runtime cancel audits must query the order itself, while
/// startup query-repair rows are explicitly required to do the same.
fn should_lookup_recovered_event_end(is_recovered: bool, requires_query_repair: bool) -> bool {
    is_recovered && !requires_query_repair
}

fn can_reuse_persisted_terminal_audit(
    terminal_trade_ids_authoritative: bool,
    requires_query_repair: bool,
) -> bool {
    terminal_trade_ids_authoritative && !requires_query_repair
}

/// The same bounded retry pass owns both crash-recovered orders and routine
/// post-cancel size-matched audits.  Returning when only the first counter is
/// zero restarts the outer background loop and resets every log to attempt=1.
fn startup_recovery_audits_complete(
    recovery_pending_orders: usize,
    routine_cancel_audits: usize,
) -> bool {
    recovery_pending_orders == 0 && routine_cancel_audits == 0
}

fn startup_recovery_warn_attempt(attempt: usize, attempts: usize) -> bool {
    attempt.saturating_add(1) == attempts
}

const EXPECTED_ORPHAN_WARN_INTERVAL_NS: u64 = 60_000_000_000;

/// Claim one operator-visible warning slot for a repetitive, fail-closed
/// orphan condition. The underlying retry/lock behavior is unchanged; only
/// duplicate WARN emission is suppressed until the next interval.
fn claim_rate_limited_warning(silent_until_ns: &std::sync::atomic::AtomicU64, now_ns: u64) -> bool {
    loop {
        let current = silent_until_ns.load(std::sync::atomic::Ordering::Relaxed);
        if now_ns < current {
            return false;
        }
        let next = now_ns.saturating_add(EXPECTED_ORPHAN_WARN_INTERVAL_NS);
        match silent_until_ns.compare_exchange_weak(
            current,
            next,
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
        ) {
            Ok(_) => return true,
            Err(observed) if now_ns < observed => return false,
            Err(_) => continue,
        }
    }
}

/// A literal JSON null is Polymarket's eventual-consistency response for an
/// order that is not visible on the queried replica. It remains unavailable
/// evidence (and therefore keeps every lock/reservation), but is not itself an
/// operator-actionable parse failure.
fn fetch_unavailable_should_warn(kind: &FetchUnavailable) -> bool {
    !kind.is_json_null()
}

impl FetchOrderResult {
    fn order(&self) -> Option<&FetchedOrder> {
        match self {
            Self::Found(order) => Some(order),
            Self::NotFound(_) | Self::Unavailable(_) => None,
        }
    }

    fn is_explicit_not_found(&self) -> bool {
        matches!(self, Self::NotFound(_))
    }

    fn has_parallel_not_found_evidence(&self) -> bool {
        matches!(self, Self::NotFound(evidence) if evidence.starts_with("parallel_reconcile_absence"))
    }
}

const ORDER_LOOKUP_EVIDENCE_MAX_CHARS: usize = 512;

fn compact_order_lookup_evidence_text(raw: &str) -> String {
    let mut chars = raw.chars();
    let compact: String = chars
        .by_ref()
        .take(ORDER_LOOKUP_EVIDENCE_MAX_CHARS)
        .collect();
    if chars.next().is_some() {
        format!("{compact}…")
    } else {
        compact
    }
}

fn compact_order_lookup_evidence(json: &serde_json::Value) -> String {
    let raw = serde_json::to_string(json).unwrap_or_else(|_| "<unserializable>".to_string());
    compact_order_lookup_evidence_text(&raw)
}

fn preflight_rejection_reason_code(reason: &str) -> &'static str {
    let reason = reason.trim().to_ascii_lowercase();
    if reason == "rate limited" {
        "rate_limit"
    } else if reason == "balance backoff" {
        "balance_backoff"
    } else if reason == "invalid token backoff" {
        "invalid_token_backoff"
    } else if reason.starts_with("body serialize:") {
        "serialization"
    } else if reason.contains("sign") || reason.contains("signature") {
        "signing"
    } else if reason.is_empty() {
        "unknown"
    } else {
        "validation"
    }
}

/// Polymarket may answer a singular `/data/order/{id}` lookup with HTTP 2xx
/// plus an error envelope instead of HTTP 404. Only recognized, order-specific
/// absence messages are authoritative; arbitrary error envelopes remain
/// unavailable evidence.
fn successful_lookup_not_found_evidence(json: &serde_json::Value) -> Option<String> {
    let object = json.as_object()?;
    let message = ["error", "errorMsg", "error_msg", "detail"]
        .iter()
        .find_map(|field| object.get(*field).and_then(serde_json::Value::as_str))?
        .trim()
        .to_ascii_lowercase();
    let authoritative = message == "order not found"
        || message.starts_with("order not found:")
        || message.starts_with("could not find order")
        || message.starts_with("no order found")
        || message.starts_with("order does not exist")
        || message.starts_with("order doesn't exist");
    authoritative.then(|| {
        format!(
            "http_2xx_error_envelope={}",
            compact_order_lookup_evidence(json)
        )
    })
}

fn parse_fetched_order(
    json: &serde_json::Value,
    expected_order_id: &str,
) -> std::result::Result<FetchedOrder, ()> {
    let object = json.as_object().ok_or(())?;
    if object
        .get("success")
        .is_some_and(|value| value.as_bool() == Some(false))
        || ["error", "errorMsg", "error_msg"]
            .iter()
            .any(|field| object.get(*field).is_some_and(|value| !value.is_null()))
    {
        return Err(());
    }
    let identities: Vec<&str> = ["id", "orderID", "order_id"]
        .iter()
        .filter_map(|field| object.get(*field).and_then(|value| value.as_str()))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect();
    if expected_order_id.trim().is_empty()
        || identities.is_empty()
        || identities
            .iter()
            .any(|identity| normalize_order_id(identity) != normalize_order_id(expected_order_id))
    {
        return Err(());
    }
    let raw_status = object
        .get("status")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(())?;
    let normalized = raw_status.to_ascii_uppercase();
    let normalized = normalized
        .strip_prefix("ORDER_STATUS_")
        .unwrap_or(&normalized);
    let status = match normalized {
        "LIVE" | "MATCHED" | "FILLED" | "INVALID" => normalized.to_string(),
        "MATCHED_NOT_BROADCASTED" => "MATCHED".to_string(),
        value if value.starts_with("CANCELED") || value.starts_with("CANCELLED") => {
            value.to_string()
        }
        _ => return Err(()),
    };
    let string_field = |name: &str| {
        object.get(name).and_then(|value| match value {
            serde_json::Value::String(value) => Some(value.clone()),
            serde_json::Value::Number(value) => Some(value.to_string()),
            _ => None,
        })
    };
    let original_size = string_field("original_size").ok_or(())?;
    let size_matched = string_field("size_matched").ok_or(())?;
    let original_quantity = original_size.parse::<f64>().map_err(|_| ())?;
    let matched_quantity = size_matched.parse::<f64>().map_err(|_| ())?;
    let tolerance = original_quantity.abs().max(1.0) * 1e-8;
    if !original_quantity.is_finite()
        || original_quantity <= 0.0
        || !matched_quantity.is_finite()
        || matched_quantity < -tolerance
        || matched_quantity > original_quantity + tolerance
        || matches!(status.as_str(), "MATCHED" | "FILLED") && matched_quantity <= 0.0
    {
        return Err(());
    }
    let associate_trades = object
        .get("associate_trades")
        .and_then(|value| value.as_array())
        .ok_or(())?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
        })
        .collect::<Option<Vec<_>>>()
        .ok_or(())?
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let unique_associate_trades: HashSet<&str> =
        associate_trades.iter().map(String::as_str).collect();
    if unique_associate_trades.len() != associate_trades.len()
        || (matched_quantity > tolerance && associate_trades.is_empty())
    {
        return Err(());
    }
    let rebuild_identity = (|| {
        let maker_address = ["maker_address", "maker"]
            .iter()
            .find_map(|field| object.get(*field).and_then(serde_json::Value::as_str))?
            .trim();
        let asset_id = ["asset_id", "token_id"]
            .iter()
            .find_map(|field| object.get(*field).and_then(serde_json::Value::as_str))?
            .trim();
        let side = match object
            .get("side")
            .and_then(serde_json::Value::as_str)?
            .trim()
            .to_ascii_uppercase()
            .as_str()
        {
            "BUY" => Side::Buy,
            "SELL" => Side::Sell,
            _ => return None,
        };
        let price = string_field("price")?.parse::<f64>().ok()?;
        let fee_rate_bps = string_field("fee_rate_bps")
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|value| *value <= 10_000);
        if maker_address.is_empty()
            || asset_id.is_empty()
            || !price.is_finite()
            || price <= 0.0
            || price > 1.0 + 1e-8
        {
            return None;
        }
        Some(FetchedOrderIdentity {
            maker_address: maker_address.to_string(),
            asset_id: asset_id.to_string(),
            side,
            price,
            fee_rate_bps,
        })
    })();
    Ok(FetchedOrder {
        status,
        audit: AuthoritativeOrderAudit {
            original_size: Some(original_size),
            size_matched: Some(size_matched),
            associate_trades,
        },
        rebuild_identity,
    })
}

/// Merge the exchange's cumulative match quantity with the durable local
/// ledger. A reconnect audit is observational: an omitted, malformed, or
/// smaller REST value must never erase fills already delivered by the private
/// feed. The boolean reports whether the REST value itself was authoritative.
fn effective_audited_match(
    reported: Option<&str>,
    order_quantity: f64,
    locally_filled: f64,
) -> (f64, bool) {
    let tolerance = 1e-8_f64.max(order_quantity.abs() * 1e-8);
    let reported = reported
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| {
            value.is_finite() && *value >= -tolerance && *value <= order_quantity + tolerance
        });
    let local = locally_filled.clamp(0.0, order_quantity.max(0.0));
    match reported {
        Some(value) => (value.clamp(0.0, order_quantity).max(local), true),
        None => (local, false),
    }
}

/// Reject every malformed numeric field before either v1 or v2 signing can
/// quantize it. Rust float-to-integer casts saturate, so merely checking the
/// resulting integer would silently turn NaN/overflow into a valid-looking
/// zero/MAX order.
fn validate_order_for_signing(order: &OrderRequest) -> Result<f64> {
    let price = order.price.ok_or_else(|| anyhow!("Missing order price"))?;
    validate_signing_inputs(&order.symbol, price, order.quantity)?;
    if order.fee_rate_bps > 10_000 {
        return Err(anyhow!("Invalid fee_rate_bps: {}", order.fee_rate_bps));
    }
    Ok(price)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlacementResponse {
    success: bool,
    order_id: String,
    status: String,
    error_msg: String,
}

/// Validate the minimum response envelope needed to make an irreversible
/// placement decision. HTTP 2xx only proves that an intermediary returned a
/// successful status; missing/wrongly-typed fields leave server-side order
/// state unknown and must be reconciled rather than released as Rejected.
fn parse_placement_response(
    value: &serde_json::Value,
) -> std::result::Result<PlacementResponse, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "placement response is not an object".to_string())?;
    let success = object
        .get("success")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| "placement response is missing boolean success".to_string())?;
    let order_id = object
        .get("orderID")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string();
    let status = object
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    let error_msg = object
        .get("errorMsg")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();

    if success && order_id.is_empty() {
        return Err("successful placement response is missing orderID".to_string());
    }
    if !success
        && object
            .get("errorMsg")
            .and_then(serde_json::Value::as_str)
            .is_none()
    {
        return Err("rejected placement response is missing string errorMsg".to_string());
    }

    Ok(PlacementResponse {
        success,
        order_id,
        status,
        error_msg,
    })
}

fn placement_response_status(
    success: bool,
    raw_status: &str,
    effective_account_status: Option<OrderStatus>,
) -> OrderStatus {
    if !success {
        return OrderStatus::Rejected;
    }
    if let Some(status @ (OrderStatus::Filled | OrderStatus::Failed)) = effective_account_status {
        return status;
    }
    if matches!(
        raw_status.to_ascii_uppercase().as_str(),
        "MATCHED" | "MATCHED_NOT_BROADCASTED"
    ) {
        OrderStatus::Filled
    } else if effective_account_status == Some(OrderStatus::PartiallyFilled) {
        OrderStatus::PartiallyFilled
    } else {
        OrderStatus::Accepted
    }
}

/// Classify a successful singular-order lookup. HTTP success proves only that
/// an intermediary returned 2xx; it does not prove that an order is absent.
/// Only the transport-level 404 branch in `fetch_order_by_id` produces
/// `NotFound`. Missing fields, error envelopes and unknown future status values
/// remain unavailable evidence so they cannot advance orphan terminalization.
fn classify_successful_order_lookup(
    json: &serde_json::Value,
    expected_order_id: &str,
) -> FetchOrderResult {
    if let Some(evidence) = successful_lookup_not_found_evidence(json) {
        return FetchOrderResult::NotFound(evidence);
    }
    match parse_fetched_order(json, expected_order_id) {
        Ok(order) => FetchOrderResult::Found(order),
        Err(()) => FetchOrderResult::Unavailable(FetchUnavailable::InvalidResponse(
            compact_order_lookup_evidence(json),
        )),
    }
}

/// Internal HTTP error discriminator for callers that need to map errors
/// to specific `OrderStatus` variants (Timeout vs server Status vs Other).
#[derive(Debug)]
pub(crate) enum HttpErr {
    Timeout,
    Status(u16, String),
    /// The request failed while reqwest was sending it or receiving the
    /// response, without an HTTP status. For an order-placement POST the
    /// server may still have accepted the signed order, so submit callers
    /// must reconcile it instead of treating it as a definitive rejection.
    Transport(String),
    /// An HTTP-success response was received, but its body could not be
    /// decoded into the documented JSON representation. Placement callers
    /// must treat this as ambiguous: the server may have committed the order
    /// before returning a truncated/proxy-corrupted body.
    InvalidResponse(String),
    Other(String),
}

impl std::fmt::Display for HttpErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpErr::Timeout => write!(f, "timeout"),
            HttpErr::Status(code, body) => write!(f, "status {} ({})", code, body),
            HttpErr::Transport(s) => write!(f, "transport: {}", s),
            HttpErr::InvalidResponse(s) => write!(f, "invalid response: {}", s),
            HttpErr::Other(s) => write!(f, "{}", s),
        }
    }
}

impl From<HttpErr> for anyhow::Error {
    fn from(e: HttpErr) -> Self {
        anyhow!("{}", e)
    }
}

impl HttpErr {
    fn is_http_425(&self) -> bool {
        matches!(self, HttpErr::Status(425, _))
    }

    fn is_explicit_not_found(&self) -> bool {
        matches!(self, HttpErr::Status(404, _))
    }

    fn fetch_unavailable(&self) -> FetchUnavailable {
        match self {
            HttpErr::Timeout => FetchUnavailable::Timeout,
            HttpErr::Transport(_) => FetchUnavailable::Transport,
            HttpErr::Status(code, _) => FetchUnavailable::Http(*code),
            HttpErr::InvalidResponse(message) | HttpErr::Other(message) => {
                FetchUnavailable::InvalidResponse(compact_order_lookup_evidence_text(message))
            }
        }
    }

    /// True when the server's response was either never received (timeout)
    /// or indicates server-side failure (HTTP 5xx). In both cases the order
    /// state is unknown — the server MAY have accepted/cancelled the order
    /// despite the error. Callers should emit timeout-equivalent statuses
    /// (NewOrderTimeout / CancelOrderTimeout) so the orphan reconciler can
    /// resolve state by re-querying.
    ///
    /// HTTP 4xx (other than 425) is a definitive server response. Local
    /// errors and response parse errors are also excluded here. A
    /// status-less transport error is deliberately handled by the separate
    /// placement classifier and the idempotent cancel hedge.
    ///
    /// **425 Too Early** is treated as unknown_state (transient server
    /// backpressure, NOT a definitive rejection). Polymarket emits 425 at
    /// service-level overload — observed 15,045× in 30 min during the
    /// 2026-05-06 21:00–21:35 outage — and the right response is to
    /// retry/reconcile, not mark Rejected. Routing through unknown_state
    /// also gates the call site's WARN behind the 425-storm dedup
    /// (see `should_warn_unknown_state`), preventing 15k+ near-identical
    /// log lines per outage.
    pub(crate) fn is_unknown_state(&self) -> bool {
        match self {
            HttpErr::Timeout => true,
            HttpErr::Status(code, _) => *code >= 500 || *code == 425,
            HttpErr::Transport(_) => false,
            HttpErr::InvalidResponse(_) => false,
            HttpErr::Other(_) => false,
        }
    }

    /// Placement-only unknown-state classification. A status-less reqwest
    /// failure happens after the signed POST has been handed to the HTTP
    /// stack, so the exchange may have accepted the order even though no
    /// response reached us. Keep this separate from `is_unknown_state` so
    /// adding the transport case does not change cancel/GET semantics.
    pub(crate) fn is_submit_unknown_state(&self) -> bool {
        (self.is_unknown_state() && !self.is_trading_disabled_rejection())
            || matches!(self, HttpErr::Transport(_) | HttpErr::InvalidResponse(_))
    }

    /// Polymarket returns this policy rejection as HTTP 503 even though the
    /// request is authoritative: no order was admitted while trading was
    /// disabled. It must not enter timeout/orphan reconciliation.
    fn is_trading_disabled_rejection(&self) -> bool {
        matches!(
            self,
            HttpErr::Status(_, body)
                if body.to_ascii_lowercase().contains("trading is disabled")
        )
    }

    /// A completed 4xx placement response is authoritative rejection evidence.
    /// 425 remains unknown-state because Polymarket uses it for transient
    /// service backpressure and may have accepted the signed request.
    fn is_definitive_submit_rejection(&self) -> bool {
        self.is_trading_disabled_rejection()
            || matches!(self, HttpErr::Status(code, _) if (400..500).contains(code) && *code != 425)
    }
}

/// Classify a reqwest HTTP-I/O error into our `HttpErr` taxonomy without
/// relying on its human-readable message.
fn map_reqwest_err(e: reqwest::Error) -> HttpErr {
    if e.is_timeout() {
        HttpErr::Timeout
    } else if e.is_connect() {
        // Preserve connect failures as transport evidence so the cluster
        // diagnostic distinguishes them from deadline expiry. Both kinds
        // share the same fail-closed placement gate.
        HttpErr::Transport(e.to_string())
    } else if let Some(status) = e.status() {
        HttpErr::Status(status.as_u16(), e.to_string())
    } else if e.is_request() || e.is_body() {
        // `is_request` is reqwest's structured category for a failure while
        // sending the request; `is_body` covers a response-body I/O failure.
        HttpErr::Transport(e.to_string())
    } else if e.is_builder() || e.is_redirect() || e.is_decode() {
        // These fail locally or while decoding a response; they are not the
        // status-less HTTP I/O failure covered by the placement fix.
        HttpErr::Other(e.to_string())
    } else {
        HttpErr::Other(e.to_string())
    }
}

/// Outcome of mapping a `not_canceled` reason returned by Polymarket
/// CLOB. Three categories: definite Cancelled, definite Filled, or
/// **Uncertain** (server's own wording is ambiguous — both states are
/// possible). Every caller maps Uncertain to `CancelUncertain`, keeping the
/// order in orphan-cancel state until an authoritative exchange response says
/// CANCELED / MATCHED / FILLED. The one bounded exception is the exact
/// "order can't be found - already canceled or matched" DELETE response:
/// three observations from reconciler-issued DELETE retries resolve the local
/// cancel lifecycle to Cancelled so a stale orphan cannot block an entire
/// event. The initial cancel response is deliberately excluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelReasonOutcome {
    /// Server explicitly says the order was cancelled.
    Cancelled,
    /// Server explicitly says the order matched. The accompanying
    /// trade will arrive on user-feed and update inventory.
    Filled,
    /// The order's terminal state is not yet decided — defer to a
    /// `GET /data/order/{oid}` reconcile. Two reasons route here:
    ///   * "order can't be found - already canceled or matched" — server
    ///     can't disambiguate between cancelled and matched.
    ///   * "can't be canceled because it is pending/delayed" — the cancel
    ///     raced ahead of the placement; the order is still being
    ///     processed and will shortly be LIVE (the reconcile then
    ///     re-issues the DELETE). Committing to Cancelled here would drop
    ///     tracking on a still-live order.
    Uncertain,
}

/// Map a `not_canceled` reason to a `CancelReasonOutcome`.
///
/// Reasons observed in 2026-04-27 live (74 min, 260 events):
///
///   * `"matched orders can't be canceled"` (159×) → **Filled**
///     A fill landed before our cancel reached the book. The order
///     is *done*, not cancelled; the trade message will arrive on
///     user-feed and update inventory. `Filled` here drops the
///     local order and tells `PositionManager` to release the
///     pending reservation — the trade stream is the authoritative
///     position delta.
///
///   * `"order can't be found - already canceled or matched"` (98×)
///     → **Uncertain**. Server's wording explicitly admits both
///     possibilities. Previous behaviour was to map to Cancelled
///     unconditionally, which prematurely released the
///     `pending_orders` lock when the order had actually matched —
///     during the brief window before the trade-push arrived,
///     `available_cash`/`available_inventory` over-credited and a
///     racing new BUY could trip a balance error. Routing to orphan
///     waits for `GET /data/order/{oid}` or the user feed to return an
///     authoritative MATCHED / FILLED / CANCELED state. A 404 remains
///     uncertain because the read replica may lag the write path.
///
///   * `"the order is already canceled"` (3×) → **Cancelled**
///     Server confirms cancelled, no ambiguity.
///
///   * `"can't be canceled because it is pending/delayed"` → **Uncertain**
///     The cancel raced ahead of the placement ack — the order is still
///     being processed and is neither cancelled nor matched. It becomes
///     LIVE moments later. Route to the orphan reconcile (GET → re-DELETE)
///     instead of dropping it. Previously fell through to the Cancelled
///     fallback, which abandoned a still-live order on the book → it rode
///     unmanaged to settlement (live.log 2026-06-24: 9 forgotten orders).
///
///   * Other / unrecognised → **Uncertain**. Unknown wording is not proof of a
///     terminal state, so keep the worst-case reservation until reconciliation.
/// True if a `not_canceled` reason means the cancel raced ahead of the
/// placement — the order is still being processed server-side and will
/// shortly become LIVE (NOT gone, NOT matched). Such an orphan is treated as
/// **Uncertain** (kept reconciling) rather than committed Cancelled: the
/// reconcile cancel not-found arm keeps re-GETting until it converges, so a
/// not-yet-indexed order isn't dropped (live.log 2026-06-25: 120/121 forgotten
/// orders had a pending/delayed cancel reject). Single source of truth, shared
/// by `cancel_not_canceled_outcome` and the cancel-reply classification sites.
fn is_pending_delayed_reason(reason: &str) -> bool {
    let r = reason.to_ascii_lowercase();
    r.contains("pending") || r.contains("delayed") || r.contains("processing")
}

/// Polymarket's exact ambiguous DELETE reply. A single observation is not
/// authoritative (the order may have matched), but repeatedly receiving this
/// response after re-GETing a LIVE order means the CLOB no longer has anything
/// cancelable under that order id. Count only DELETEs issued by the
/// cancel-orphan reconciler after GET returned LIVE: keep the first two
/// observations orphaned; the third is the bounded cancel-lifecycle terminal
/// requested by the live strategy.
fn is_cancel_not_found_already_canceled_or_matched(reason: &str) -> bool {
    reason
        .trim()
        .eq_ignore_ascii_case("order can't be found - already canceled or matched")
}

pub(crate) const CANCEL_NOT_FOUND_TERMINAL_LIMIT: u32 = 3;

#[cfg(test)]
fn record_cancel_not_found_observation(
    counts: &mut HashMap<String, u32>,
    coid: &str,
    reason: Option<&str>,
    outcome: CancelReasonOutcome,
) -> Option<u32> {
    if outcome != CancelReasonOutcome::Uncertain
        || coid.is_empty()
        || !reason.is_some_and(is_cancel_not_found_already_canceled_or_matched)
    {
        return None;
    }
    let count = counts.entry(coid.to_string()).or_insert(0);
    *count = count.saturating_add(1);
    Some(*count)
}

fn cancel_not_found_outcome_after_observation(
    outcome: CancelReasonOutcome,
    observation: Option<u32>,
) -> CancelReasonOutcome {
    if observation.is_some_and(|n| n >= CANCEL_NOT_FOUND_TERMINAL_LIMIT) {
        CancelReasonOutcome::Cancelled
    } else {
        outcome
    }
}

fn cancel_not_canceled_outcome(reason: &str) -> CancelReasonOutcome {
    let r = reason.to_ascii_lowercase();
    let not_found =
        r.contains("not found") || r.contains("can't be found") || r.contains("cant be found");
    let mentions_matched = r.contains("matched");

    // "matched orders can't be canceled" — definite (matched preceded "can't"
    // grammar, no ambiguity).
    if r.starts_with("matched") || (mentions_matched && !not_found) {
        return CancelReasonOutcome::Filled;
    }
    // "order can't be found - already canceled or matched" — server says
    // BOTH outcomes are possible; defer to reconcile.
    if not_found && mentions_matched {
        return CancelReasonOutcome::Uncertain;
    }
    // "not found" alone is not authoritative. The order write may not have
    // reached the read replica yet, or a fill push may still be in flight.
    if not_found {
        return CancelReasonOutcome::Uncertain;
    }
    // "can't be canceled because it is pending/delayed" — the cancel raced
    // ahead of the placement: the order is still being processed
    // server-side and is NOT yet cancelled and NOT matched. It will
    // shortly become LIVE on the book. Route to the orphan path (same as
    // Uncertain) so the reconciler GETs /data/order/{oid}, finds it LIVE,
    // and re-issues the DELETE. The previous behaviour fell through to the
    // Cancelled fallback below and dropped tracking on a still-live order,
    // leaving a forgotten resting order that rode to settlement
    // (live.log 2026-06-24: 9 such orders, all with this reason).
    if is_pending_delayed_reason(reason) {
        return CancelReasonOutcome::Uncertain;
    }
    // Only explicit already-cancelled wording is a terminal cancel. Unknown
    // wording remains orphaned: retry exhaustion is an operational fault, not
    // evidence that collateral can safely be released.
    if r.contains("already canceled") || r.contains("already cancelled") {
        return CancelReasonOutcome::Cancelled;
    }
    CancelReasonOutcome::Uncertain
}

/// Convert a DELETE `not_canceled` reason into the lifecycle status emitted to
/// the strategy. This mapping is deliberately identical for the initial
/// response and every reconcile retry: uncertainty always remains an orphan.
#[cfg(test)]
fn cancel_reason_order_status(reason: &str) -> OrderStatus {
    match cancel_not_canceled_outcome(reason) {
        CancelReasonOutcome::Cancelled => OrderStatus::Cancelled,
        CancelReasonOutcome::Filled => OrderStatus::Filled,
        CancelReasonOutcome::Uncertain => OrderStatus::CancelUncertain,
    }
}

/// Classify one order inside a successful DELETE response. HTTP success is not
/// order success: an omitted OID or contradictory canceled/not_canceled entry
/// remains uncertain and must retain its orphan reservation.
fn cancel_delete_response_outcome(
    response: &serde_json::Value,
    order_id: &str,
) -> CancelReasonOutcome {
    let explicitly_canceled = response
        .get("canceled")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str()).any(|id| id == order_id))
        .unwrap_or(false);
    let matching_reason = response
        .get("not_canceled")
        .and_then(|v| v.as_object())
        .and_then(|nc| nc.get(order_id))
        .and_then(|reason| reason.as_str());

    match (explicitly_canceled, matching_reason) {
        (true, None) => CancelReasonOutcome::Cancelled,
        (false, Some(reason)) => cancel_not_canceled_outcome(reason),
        // Both collections mentioning the same OID is contradictory; neither
        // collection mentioning it is an omission. Both remain uncertain.
        (true, Some(_)) | (false, None) => CancelReasonOutcome::Uncertain,
    }
}

fn validated_cancel_all_counts(json: &serde_json::Value) -> Option<(usize, usize)> {
    let object = json.as_object()?;
    if object
        .get("success")
        .is_some_and(|value| value.as_bool() == Some(false))
        || ["error", "errorMsg", "error_msg"]
            .iter()
            .any(|field| object.get(*field).is_some_and(|value| !value.is_null()))
    {
        return None;
    }
    let canceled_values = json.get("canceled")?.as_array()?;
    if canceled_values.iter().any(|value| {
        value
            .as_str()
            .is_none_or(|order_id| order_id.trim().is_empty())
    }) {
        return None;
    }
    let not_canceled_values = json.get("not_canceled")?.as_object()?;
    if not_canceled_values.iter().any(|(order_id, reason)| {
        order_id.trim().is_empty()
            || reason
                .as_str()
                .is_none_or(|reason| reason.trim().is_empty())
    }) {
        return None;
    }
    let canceled = canceled_values.len();
    let not_canceled = not_canceled_values.len();
    Some((canceled, not_canceled))
}

fn format_order_brief(o: &OrderRequest) -> String {
    let label: &str = if !o.outcome_label.is_empty() {
        &o.outcome_label
    } else {
        // Fallback: show a short symbol prefix if the caller didn't set a label.
        let n = o.symbol.len().min(10);
        &o.symbol[..n]
    };
    let po = if o.post_only { " po" } else { "" };
    format!(
        "coid={} {} {} @{:.3} qty={}{}",
        o.client_order_id,
        o.side,
        label,
        o.price.unwrap_or(0.0),
        o.quantity,
        po,
    )
}

// ════════════════════════════════════════════════════════════════
// Shared State (between trade executor and user_feed)
// ════════════════════════════════════════════════════════════════

/// Tracked order for state reconciliation.
#[derive(Debug, Clone)]
pub(crate) struct TrackedOrder {
    pub order_slot: OrderSlot,
    pub symbol: String,
    pub side: Side,
    /// Strategy instance that placed this order. Multiple instances may
    /// share one wallet (= one `SharedState`/`open_orders` map); this tags
    /// each row so an instance's bulk cancels (e.g. the balance-error
    /// USDC-pool sweep) only touch its OWN orders, never a sibling's.
    /// Empty for single-instance / CLI routes (every order carries the same
    /// value → filter is a no-op, byte-identical to legacy).
    pub instance_id: String,
}

/// Logging-only correlation retained for the same lifetime as the settled
/// event audit. Economic state remains authoritative in SharedAccount.
#[derive(Debug, Clone)]
pub(crate) struct OrderLifecycleTrace {
    pub instance_id: String,
    pub event_id: String,
    pub symbol: String,
    pub side: Side,
    pub trigger_source: QuoteTriggerSource,
    pub trigger_exchange_ns: u64,
    pub trigger_local_ns: u64,
    pub quote_emit_ns: u64,
    pub submit_prep_ns: u64,
    pub signed_ns: u64,
    pub account_reserved_ns: u64,
    pub http_dispatched_ns: u64,
    pub cancel_signal_ns: u64,
    pub cancel_prep_ns: u64,
    pub cancel_dispatched_ns: u64,
}

/// Immutable execution-state publication consumed by cancel/reconcile/private
/// workers. The account owner is the only writer; readers take one RCU guard
/// and never contend with another worker or observe a torn coid/oid update.
const EXECUTION_READ_SHARDS: usize = 64;

#[derive(Debug, Clone)]
pub(crate) struct ExecutionReadMap<V> {
    shards: Arc<[Arc<HashMap<String, V>>; EXECUTION_READ_SHARDS]>,
    len: usize,
}

impl<V> Default for ExecutionReadMap<V> {
    fn default() -> Self {
        Self {
            shards: Arc::new(std::array::from_fn(|_| Arc::new(HashMap::new()))),
            len: 0,
        }
    }
}

impl<V> ExecutionReadMap<V> {
    #[inline]
    fn shard_index(key: &str) -> usize {
        // Stable FNV-1a avoids RandomState construction and keeps the same key
        // on one shard across snapshot generations.
        let mut hash = 0xcbf29ce484222325u64;
        for byte in key.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash as usize % EXECUTION_READ_SHARDS
    }

    fn from_hash_map(values: HashMap<String, V>) -> Self {
        let len = values.len();
        let mut shards: [HashMap<String, V>; EXECUTION_READ_SHARDS] =
            std::array::from_fn(|_| HashMap::new());
        for (key, value) in values {
            shards[Self::shard_index(&key)].insert(key, value);
        }
        Self {
            shards: Arc::new(shards.map(Arc::new)),
            len,
        }
    }

    #[inline]
    pub(crate) fn get(&self, key: &str) -> Option<&V> {
        self.shards[Self::shard_index(key)].get(key)
    }

    #[inline]
    pub(crate) fn contains_key(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&String, &V)> {
        self.shards.iter().flat_map(|shard| shard.iter())
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = &String> {
        self.iter().map(|(key, _)| key)
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_, value)| value)
    }
}

impl<V: Clone> ExecutionReadMap<V> {
    fn with_insert(&self, key: String, value: V) -> Self {
        let index = Self::shard_index(&key);
        let mut shards = (*self.shards).clone();
        let mut shard = (*shards[index]).clone();
        let inserted = shard.insert(key, value).is_none();
        shards[index] = Arc::new(shard);
        Self {
            shards: Arc::new(shards),
            len: self.len + usize::from(inserted),
        }
    }

    fn with_remove(&self, key: &str) -> Self {
        let index = Self::shard_index(key);
        if !self.shards[index].contains_key(key) {
            return self.clone();
        }
        let mut shards = (*self.shards).clone();
        let mut shard = (*shards[index]).clone();
        shard.remove(key);
        shards[index] = Arc::new(shard);
        Self {
            shards: Arc::new(shards),
            len: self.len.saturating_sub(1),
        }
    }
}

impl<V> std::ops::Index<&str> for ExecutionReadMap<V> {
    type Output = V;

    fn index(&self, index: &str) -> &Self::Output {
        self.get(index).expect("execution read-map key missing")
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ExecutionStateSnapshot {
    pub(crate) open_orders: ExecutionReadMap<TrackedOrder>,
    pub(crate) coid_to_oid: ExecutionReadMap<String>,
    pub(crate) oid_to_coid: ExecutionReadMap<String>,
    pub(crate) coid_to_token: ExecutionReadMap<String>,
}

#[derive(Debug)]
enum ExecutionStateCommand {
    InstallIdentity {
        client_order_id: String,
        exchange_order_id: String,
        token: String,
    },
    TrackOpen {
        client_order_id: String,
        tracked: TrackedOrder,
    },
    RemoveOpen {
        client_order_id: String,
    },
    RetireMappings {
        asset_ids: Vec<String>,
        owned_coids: HashSet<String>,
    },
    #[cfg(test)]
    Clear,
    #[cfg(test)]
    BenchmarkPublishOpen {
        client_order_id: String,
        tracked: TrackedOrder,
        enqueued_ns: u64,
        completion: crossbeam_channel::Sender<u64>,
    },
}

struct ExecutionStateOwner {
    open_orders: HashMap<String, TrackedOrder>,
    coid_to_oid: HashMap<String, String>,
    oid_to_coid: HashMap<String, String>,
    coid_to_token: HashMap<String, String>,
}

impl ExecutionStateOwner {
    fn new(current: ExecutionStateSnapshot) -> Self {
        Self {
            open_orders: current
                .open_orders
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            coid_to_oid: current
                .coid_to_oid
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            oid_to_coid: current
                .oid_to_coid
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            coid_to_token: current
                .coid_to_token
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }

    fn apply(&mut self, shared: &SharedState, command: ExecutionStateCommand) {
        let mut next = (*shared.execution_state.load_full()).clone();
        match command {
            ExecutionStateCommand::InstallIdentity {
                client_order_id,
                exchange_order_id,
                token,
            } => {
                let previous = self
                    .coid_to_oid
                    .insert(client_order_id.clone(), exchange_order_id.clone());
                let normalized = normalize_order_id(&exchange_order_id);
                next.coid_to_oid = next
                    .coid_to_oid
                    .with_insert(client_order_id.clone(), exchange_order_id);
                if let Some(previous) = previous {
                    let previous = normalize_order_id(&previous);
                    if previous != normalized {
                        self.oid_to_coid.remove(&previous);
                        next.oid_to_coid = next.oid_to_coid.with_remove(&previous);
                    }
                }
                self.oid_to_coid
                    .insert(normalized.clone(), client_order_id.clone());
                next.oid_to_coid = next
                    .oid_to_coid
                    .with_insert(normalized, client_order_id.clone());
                if !token.is_empty() {
                    self.coid_to_token
                        .insert(client_order_id.clone(), token.clone());
                    next.coid_to_token = next.coid_to_token.with_insert(client_order_id, token);
                }
            }
            ExecutionStateCommand::TrackOpen {
                client_order_id,
                tracked,
            } => {
                self.open_orders
                    .insert(client_order_id.clone(), tracked.clone());
                next.open_orders = next.open_orders.with_insert(client_order_id, tracked);
            }
            ExecutionStateCommand::RemoveOpen { client_order_id } => {
                self.open_orders.remove(&client_order_id);
                next.open_orders = next.open_orders.with_remove(&client_order_id);
            }
            ExecutionStateCommand::RetireMappings {
                asset_ids,
                owned_coids,
            } => {
                reclaim_token_mappings(
                    &mut self.coid_to_oid,
                    &mut self.oid_to_coid,
                    &mut self.coid_to_token,
                    &asset_ids,
                    Some(&owned_coids),
                );
                shared
                    .runtime_order_ownership
                    .remove_client_orders(&owned_coids);
                // Settled-event retirement is cold and can remove many keys;
                // rebuild once instead of cloning one shard per deletion.
                next.coid_to_oid = ExecutionReadMap::from_hash_map(self.coid_to_oid.clone());
                next.oid_to_coid = ExecutionReadMap::from_hash_map(self.oid_to_coid.clone());
                next.coid_to_token = ExecutionReadMap::from_hash_map(self.coid_to_token.clone());
            }
            #[cfg(test)]
            ExecutionStateCommand::Clear => {
                self.open_orders.clear();
                self.coid_to_oid.clear();
                self.oid_to_coid.clear();
                self.coid_to_token.clear();
                shared.runtime_order_ownership.clear();
                next = ExecutionStateSnapshot::default();
            }
            #[cfg(test)]
            ExecutionStateCommand::BenchmarkPublishOpen {
                client_order_id,
                tracked,
                enqueued_ns,
                completion,
            } => {
                self.open_orders
                    .insert(client_order_id.clone(), tracked.clone());
                next.open_orders = next.open_orders.with_insert(client_order_id, tracked);
                shared.execution_state.store(Arc::new(next));
                let _ = completion.send(now_ns().saturating_sub(enqueued_ns));
                return;
            }
        }
        shared.execution_state.store(Arc::new(next));
    }

    fn publish(&self, shared: &SharedState, open_changed: bool, identity_changed: bool) {
        let mut next = (*shared.execution_state.load_full()).clone();
        if open_changed {
            next.open_orders = ExecutionReadMap::from_hash_map(self.open_orders.clone());
        }
        if identity_changed {
            next.coid_to_oid = ExecutionReadMap::from_hash_map(self.coid_to_oid.clone());
            next.oid_to_coid = ExecutionReadMap::from_hash_map(self.oid_to_coid.clone());
            next.coid_to_token = ExecutionReadMap::from_hash_map(self.coid_to_token.clone());
        }
        shared.execution_state.store(Arc::new(next));
    }
}

fn lifecycle_delta_ms(stage_ns: u64, origin_ns: u64) -> f64 {
    if origin_ns == 0 {
        return -1.0;
    }
    (stage_ns as i128 - origin_ns as i128) as f64 / 1_000_000.0
}

fn instance_owned_open_coids<'a>(
    open: impl Iterator<Item = (&'a String, &'a TrackedOrder)>,
    instance_id: &str,
) -> Vec<String> {
    let mut coids: Vec<String> = open
        .filter(|(_, tracked)| tracked.instance_id == instance_id)
        .map(|(coid, _)| coid.clone())
        .collect();
    coids.sort();
    coids
}

// ─── Async HTTP dispatch ────────────────────────────────────────────
// All Polymarket REST calls run on the dedicated order-I/O runtime
// (`async_rt::order_handle()`) via the shared `reqwest::Client`
// (h1.1, keepalive). No dedicated worker threads — tokio schedules the
// futures on that current_thread runtime, isolated from the WS-feed
// runtime so book bursts can't head-of-line-block order polls. Parallel
// cancel+place is realised by kicking off two `spawn` calls without
// waiting.

type HttpReply = std::result::Result<serde_json::Value, HttpErr>;

#[derive(Debug, Default)]
struct HttpCompletionTiming {
    response_ready_ns: AtomicU64,
    reply_enqueued_ns: AtomicU64,
}

#[derive(Debug)]
struct FilledCleanupJob {
    client_order_id: String,
    enqueued_ns: u64,
}

#[derive(Debug)]
struct DeferredLifecycleJob {
    client_order_id: String,
    status: OrderStatus,
    enqueued_ns: u64,
    #[cfg(test)]
    completion: Option<crossbeam_channel::Sender<u64>>,
}

#[derive(Debug)]
struct AttemptAuditJob {
    attempt: crate::http1_pool::AttemptTraceSnapshot,
    kind: &'static str,
    /// Execution-route owner, also present for cancels that have no placement
    /// tuple.  This keeps every audit row instance-scoped without inferring
    /// ownership from the coid.
    route_instance_id: String,
    /// Immutable placement inputs copied only after the HTTP reply reaches the
    /// completion worker.  Keeping these on the bounded audit lane makes a
    /// live order attempt reproducible without formatting or allocating on the
    /// strategy quote thread.  Cancels join back to their place by `coid` and
    /// therefore carry `None` here.
    order: Option<AttemptOrderReplica>,
    client_order_id: String,
    exchange_order_id: Option<String>,
    status: OrderStatus,
    error: Option<String>,
    completed_ns: u64,
    enqueued_ns: u64,
}

#[derive(Debug)]
enum PrivateDiagnosticJob {
    FailedTradeWithoutReason {
        trade_id: String,
        tx_hash: String,
        maker_legs: usize,
        enqueued_ns: u64,
    },
    ColdApplyFailure {
        detail: String,
        enqueued_ns: u64,
    },
}

#[derive(Debug)]
enum LifecycleTraceJob {
    Register {
        client_order_id: String,
        trace: OrderLifecycleTrace,
    },
    Forget {
        client_order_id: String,
    },
    ForgetMany {
        client_order_ids: HashSet<String>,
    },
    CancelSignal {
        client_order_id: String,
        signal_ns: u64,
    },
    Stage {
        client_order_id: String,
        stage: &'static str,
        exchange_order_id: Option<String>,
        status: Option<OrderStatus>,
        trade_id: Option<String>,
        reason_code: &'static str,
        reason: String,
        stage_ns: u64,
    },
}

#[derive(Debug)]
enum AuditJob {
    Attempt(AttemptAuditJob),
    Lifecycle(LifecycleTraceJob),
    PrivateDiagnostic(PrivateDiagnosticJob),
}

struct OrderAttemptRecorder {
    directory: PathBuf,
    hour: i64,
    writer: Option<BufWriter<std::fs::File>>,
}

impl OrderAttemptRecorder {
    fn new() -> Self {
        Self {
            directory: std::env::var_os("HEXBOT_ORDER_AUDIT_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("data/record/order_audit")),
            hour: -1,
            writer: None,
        }
    }

    fn ensure_writer(&mut self, hour: i64) -> std::io::Result<&mut BufWriter<std::fs::File>> {
        if self.writer.is_none() || self.hour != hour {
            std::fs::create_dir_all(&self.directory)?;
            let path = self.directory.join(format!("order-audit-{hour}.jsonl"));
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            self.writer = Some(BufWriter::with_capacity(64 * 1024, file));
            self.hour = hour;
        }
        Ok(self.writer.as_mut().expect("writer installed"))
    }

    fn write_attempt(&mut self, job: &AttemptAuditJob) -> std::io::Result<()> {
        let attempt = job.attempt;
        let order = job.order.as_ref();
        let record = serde_json::json!({
                "attempt_id": attempt.attempt_id,
                "kind": job.kind,
                "role": format!("{:?}", attempt.role),
                "slot": attempt.slot,
                "iid": order.map(|order| order.instance_id.as_str()).filter(|value| !value.is_empty()).unwrap_or(job.route_instance_id.as_str()),
                "event_id": order.map(|order| order.event_id.as_str()).filter(|value| !value.is_empty()),
                "token": order.map(|order| order.token.as_str()).filter(|value| !value.is_empty()),
                "side": order.map(|order| order.side.to_string()),
                "order_type": order.map(|order| format!("{:?}", order.order_type)),
                "price": order.and_then(|order| order.price),
                "quantity": order.map(|order| order.quantity),
                "post_only": order.map(|order| order.post_only),
                "reduce_only": order.map(|order| order.reduce_only),
                "fee_rate_bps": order.map(|order| order.fee_rate_bps),
                "trigger_source": order.map(|order| order.trigger_source.to_string()),
                "trigger_exchange_ns": order.map(|order| order.trigger_exchange_ns),
                "trigger_local_ns": order.map(|order| order.trigger_local_ns),
                "coid": job.client_order_id,
                "oid": job.exchange_order_id,
                "status": format!("{:?}", job.status),
                "error": job.error,
                "signal_ns": attempt.signal_ns,
                "prep_ns": attempt.prep_ns,
                "signed_ns": attempt.signed_ns,
                "account_recorded_ns": attempt.account_recorded_ns,
                "dispatched_ns": attempt.dispatched_ns,
                "completed_ns": job.completed_ns,
                "dispatch_to_done_ns": job.completed_ns.saturating_sub(attempt.dispatched_ns),
        });
        self.write_record(&record)
    }

    fn write_record(&mut self, record: &serde_json::Value) -> std::io::Result<()> {
        let hour = chrono::Utc::now().timestamp().div_euclid(3600);
        let writer = self.ensure_writer(hour)?;
        serde_json::to_writer(&mut *writer, record)?;
        writer.write_all(b"\n")
    }
}

/// Order-level inputs required to replay a live placement in sim_v2.
///
/// This is deliberately an audit payload, not mutable runtime authority.  It
/// crosses exactly one boundary: the order completion worker transfers it to
/// the single background audit writer through `attempt_audit_tx`.
#[derive(Debug, Clone, PartialEq)]
struct AttemptOrderReplica {
    instance_id: String,
    event_id: String,
    token: String,
    side: Side,
    order_type: crate::types::OrderType,
    price: Option<f64>,
    quantity: f64,
    post_only: bool,
    reduce_only: bool,
    fee_rate_bps: u32,
    trigger_source: QuoteTriggerSource,
    trigger_exchange_ns: u64,
    trigger_local_ns: u64,
}

impl AttemptOrderReplica {
    fn from_order(order: &OrderRequest, route_instance_id: &str) -> Self {
        // The executor route is the routing authority.  `OrderRequest` should
        // carry the same ID, but logging the route ID also covers legacy
        // callers whose request field is empty.
        Self {
            instance_id: route_instance_id.to_string(),
            event_id: order.quote_event_id.clone(),
            token: order.symbol.clone(),
            side: order.side,
            order_type: order.order_type,
            price: order.price,
            quantity: order.quantity,
            post_only: order.post_only,
            reduce_only: order.reduce_only,
            fee_rate_bps: order.fee_rate_bps,
            trigger_source: order.quote_trigger_source,
            trigger_exchange_ns: order.quote_trigger_exchange_timestamp_ns,
            trigger_local_ns: order.quote_trigger_local_timestamp_ns,
        }
    }
}

const EXECUTION_AUDIT_QUEUE_CAPACITY: usize = 4_096;

/// Typed, non-blocking messages consumed by the per-account owner. Commands
/// must never carry an HTTP future, connection permit, completion closure or
/// synchronous reply wait: cancel and completion pools remain separate so a
/// permit waiter cannot occupy the workers needed to release that permit.
enum AccountLifecycleJob {
    StartupBarrier(crossbeam_channel::Sender<()>),
    RegisterLocalOrder {
        command: ExecutionStateCommand,
        ownership: Option<OrderOwnership>,
    },
    ExecutionState(ExecutionStateCommand),
    RebindServerIdentity {
        client_order_id: String,
        exchange_order_id: String,
        token: String,
    },
    MarkLive {
        client_order_id: String,
        tracked: TrackedOrder,
        status: OrderStatus,
        enqueued_ns: u64,
    },
    DeferredLifecycle(DeferredLifecycleJob),
    PrivateCold(super::user_feed::PrivateColdCommand),
    DiagnosePrivate {
        record: serde_json::Value,
        completion: crossbeam_channel::Sender<super::user_feed::ParsedPrivateEvent>,
    },
}

#[derive(Debug)]
enum AccountMaintenanceJob {
    FilledCleanup(FilledCleanupJob),
}

async fn send_and_drain(
    request: reqwest::RequestBuilder,
) -> std::result::Result<u16, reqwest::Error> {
    let response = request.send().await?;
    let status = response.status().as_u16();
    response.bytes().await?;
    Ok(status)
}

const PREWARM_CONCURRENCY: usize = 4;
const PREWARM_STAGGER_MS: u64 = 25;

#[derive(Default)]
struct PrewarmSummary {
    total: usize,
    ok: usize,
    rate_limited: usize,
    first_error: Option<String>,
}

/// Warm a set of distinct reqwest pools without turning process startup into
/// a connection storm. Tasks are retained in a bounded lane and their starts
/// are staggered, so even a large per-instance pool cannot hit one host in a
/// single scheduler turn.
async fn prewarm_clients_staggered(
    label: &'static str,
    clients: Vec<Arc<reqwest::Client>>,
    url: String,
) -> PrewarmSummary {
    let total = clients.len();
    let semaphore = Arc::new(tokio::sync::Semaphore::new(PREWARM_CONCURRENCY));
    let mut tasks = tokio::task::JoinSet::new();
    for (idx, client) in clients.into_iter().enumerate() {
        let semaphore = semaphore.clone();
        let url = url.clone();
        tasks.spawn(async move {
            tokio::time::sleep(Duration::from_millis(
                PREWARM_STAGGER_MS.saturating_mul(idx as u64),
            ))
            .await;
            let _permit = semaphore
                .acquire_owned()
                .await
                .expect("prewarm semaphore open");
            send_and_drain(client.get(url)).await
        });
    }

    let mut summary = PrewarmSummary {
        total,
        ..Default::default()
    };
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(status)) if (200..400).contains(&status) => summary.ok += 1,
            Ok(Ok(429)) => {
                summary.rate_limited += 1;
                if summary.first_error.is_none() {
                    summary.first_error = Some("HTTP 429".into());
                }
            }
            Ok(Ok(status)) => {
                if summary.first_error.is_none() {
                    summary.first_error = Some(format!("HTTP {}", status));
                }
            }
            Ok(Err(error)) => {
                if summary.first_error.is_none() {
                    summary.first_error = Some(error.to_string());
                }
            }
            Err(error) => {
                if summary.first_error.is_none() {
                    summary.first_error = Some(error.to_string());
                }
            }
        }
    }
    if summary.ok == summary.total {
        info!(
            "[PolymarketTrade] Pre-warm {} {}/{} ok",
            label, summary.ok, summary.total,
        );
    } else {
        warn!(
            "[PolymarketTrade] Pre-warm {} {}/{} ok rate_limited={} first_error={}",
            label,
            summary.ok,
            summary.total,
            summary.rate_limited,
            summary.first_error.as_deref().unwrap_or("unknown"),
        );
    }
    summary
}

static TRANSPORT_PREWARMED: OnceLock<()> = OnceLock::new();

/// Account-scoped heartbeat loop. Each tick sends exactly one signed
/// `POST /heartbeats`; transport warming is handled separately at startup.
/// This avoids multiplying account heartbeat traffic by the number of HTTP
/// clients (and then again by the number of accounts).
///
/// Logging cadence:
///   - success (all pings ok): TRACE per tick
///   - any failure:           WARN per tick (with first error)
///   - every 30 ticks (5 min): INFO summary
async fn heartbeat_loop(
    auth: PolyAuth,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    base_url: String,
) {
    info!(
        "[PolyHeartbeat] Started (interval={}s, one signed request per account)",
        HEARTBEAT_INTERVAL.as_secs(),
    );
    const SUMMARY_TICKS: u32 = 30;
    let mut tick_ok = 0u32;
    let mut tick_err = 0u32;
    let mut ticks_since_summary = 0u32;
    loop {
        tokio::time::sleep(HEARTBEAT_INTERVAL).await;
        if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
        let start = std::time::Instant::now();
        let path = "/heartbeats";
        let headers = auth.sign_request("POST", path, "");
        let url = format!("{}{}", base_url, path);
        let prewarm_url = format!("{}/time", base_url.trim_end_matches('/'));

        let client = crate::http1_pool::pooled_client(crate::http1_pool::Role::Query);
        let mut request = client
            .client()
            .request(reqwest::Method::POST, &url)
            .header("Content-Type", "application/json")
            .body(String::new());
        for (name, value) in headers.as_pairs() {
            request = request.header(name, value);
        }
        let result = send_and_drain(request).await;
        let (err_n, first_err) = match result {
            Ok(status) if (200..400).contains(&status) => {
                client.note_transport_success();
                (0usize, None)
            }
            Ok(status) => {
                client.note_transport_success();
                (1, Some(format!("HTTP {}", status)))
            }
            Err(error) => {
                client.note_transport_failure(prewarm_url);
                (1, Some(error.to_string()))
            }
        };

        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        if err_n == 0 {
            log::trace!("[PolyHeartbeat] ok ({:.0}ms)", elapsed_ms);
            tick_ok += 1;
        } else {
            warn!(
                "[PolyHeartbeat] request failed ({:.0}ms): first_err={}",
                elapsed_ms,
                first_err.unwrap_or_default(),
            );
            tick_err += 1;
        }
        ticks_since_summary += 1;
        if ticks_since_summary >= SUMMARY_TICKS {
            info!(
                "[PolyHeartbeat] Summary: {} ticks OK, {} ticks had failures (last {} × {}s = {}s window)",
                tick_ok, tick_err,
                ticks_since_summary, HEARTBEAT_INTERVAL.as_secs(),
                ticks_since_summary as u64 * HEARTBEAT_INTERVAL.as_secs(),
            );
            tick_ok = 0;
            tick_err = 0;
            ticks_since_summary = 0;
        }
    }
    info!("[PolyHeartbeat] Stopped");
}

/// Pick a stable `&'static str` latency-histogram stage name for a CLOB
/// request. Buckets are coarse on purpose — we care about p99 of "place
/// order" vs "cancel all", not per-call-site breakdown.
fn http_stage(method: &str, path: &str) -> &'static str {
    match (method, path) {
        ("POST", "/order") => "polymarket.http.place_order",
        ("POST", "/orders") => "polymarket.http.place_orders_batch",
        ("DELETE", "/order") => "polymarket.http.cancel_order",
        ("DELETE", "/cancel-all") => "polymarket.http.cancel_all",
        ("DELETE", _) => "polymarket.http.cancel_other",
        ("POST", "/heartbeats") => "polymarket.http.heartbeat",
        ("POST", _) => "polymarket.http.post_other",
        ("GET", _) => "polymarket.http.get",
        _ => "polymarket.http.other",
    }
}

fn http_runtime_queue_stage(role: crate::http1_pool::Role) -> &'static str {
    match role {
        crate::http1_pool::Role::Fast => "polymarket.http.place.runtime_queue",
        crate::http1_pool::Role::Cancel => "polymarket.http.cancel.runtime_queue",
        crate::http1_pool::Role::Reconcile => "polymarket.http.reconcile.runtime_queue",
        crate::http1_pool::Role::GapReplay => "polymarket.http.gap_replay.runtime_queue",
        crate::http1_pool::Role::Query => "polymarket.http.query.runtime_queue",
    }
}

fn http_network_stage(role: crate::http1_pool::Role) -> &'static str {
    match role {
        crate::http1_pool::Role::Fast => "polymarket.http.place.network",
        crate::http1_pool::Role::Cancel => "polymarket.http.cancel.network",
        crate::http1_pool::Role::Reconcile => "polymarket.http.reconcile.network",
        crate::http1_pool::Role::GapReplay => "polymarket.http.gap_replay.network",
        crate::http1_pool::Role::Query => "polymarket.http.query.network",
    }
}

fn http_reply_enqueue_stage(role: crate::http1_pool::Role) -> &'static str {
    match role {
        crate::http1_pool::Role::Fast => "polymarket.http.place.reply_enqueue",
        crate::http1_pool::Role::Cancel => "polymarket.http.cancel.reply_enqueue",
        crate::http1_pool::Role::Reconcile => "polymarket.http.reconcile.reply_enqueue",
        crate::http1_pool::Role::GapReplay => "polymarket.http.gap_replay.reply_enqueue",
        crate::http1_pool::Role::Query => "polymarket.http.query.reply_enqueue",
    }
}

/// Classify a (method, path) as a place / cancel request for the
/// per-request latency CSV (`latency_record`). Returns `None` for
/// everything else (heartbeat, reconcile GET, …) so the record stays
/// focused on order placement + cancellation latency.
fn latency_record_kind(method: &str, path: &str) -> Option<crate::latency_record::RequestKind> {
    use crate::latency_record::RequestKind;
    match (method, path) {
        ("POST", "/order") | ("POST", "/orders") => Some(RequestKind::Place),
        ("DELETE", "/order") | ("DELETE", "/orders") | ("DELETE", "/cancel-all") => {
            Some(RequestKind::Cancel)
        }
        _ => None,
    }
}

/// Map an HTTP reply to the `status` column of the latency CSV.
fn latency_record_status(reply: &HttpReply) -> crate::latency_record::RequestStatus {
    use crate::latency_record::RequestStatus;
    match reply {
        Ok(_) => RequestStatus::Ok,
        Err(HttpErr::Timeout) => RequestStatus::Timeout,
        Err(HttpErr::Status(code, _)) => RequestStatus::Http(*code),
        Err(HttpErr::Transport(_)) => RequestStatus::TransportError,
        Err(HttpErr::InvalidResponse(_)) => RequestStatus::InvalidResponse,
        Err(HttpErr::Other(_)) => RequestStatus::Error,
    }
}

/// Classify a CLOB request by connection-pool role. Role isolation
/// ensures a slow query / heartbeat can't back-pressure the hot-path
/// submit via shared TCP receive windows — each role
/// owns a distinct TCP connection per host.
///
/// Routing table (all relative to CLOB_BASE_URL):
///   * POST /order, POST /orders        → fast (500 ms)
///   * DELETE /order, /orders, /cancel-all, DELETE *  → cancel (500 ms)
///   * GET /data/order/{id}              → reconcile (2000 ms)
///   * everything else (heartbeats, generic GET / POST) → query (5000 ms)
///
/// Gap replay bypasses this generic router and explicitly acquires its
/// account's `GapReplay` role so it cannot consume Query capacity.
fn request_role(method: &reqwest::Method, path: &str) -> crate::http1_pool::Role {
    match (method.as_str(), path) {
        ("POST", "/order") | ("POST", "/orders") => crate::http1_pool::Role::Fast,
        ("DELETE", _) => crate::http1_pool::Role::Cancel,
        ("GET", p) if p.starts_with("/data/order/") => crate::http1_pool::Role::Reconcile,
        _ => crate::http1_pool::Role::Query,
    }
}

/// Per-request timeout for `(method, path)`. Returns `Some(d)` for the
/// FAST + CANCEL paths so the session-of-day timeout takes effect and
/// for reconcile GETs (2 s, preserving the pre-merge reconcile pool
/// deadline); `None` for paths that keep the client-level timeout
/// (query 5 s — rare upstream stalls are absorbed by the ceiling).
fn per_request_timeout(method: &reqwest::Method, path: &str) -> Option<std::time::Duration> {
    match (method.as_str(), path) {
        ("POST", "/order") | ("POST", "/orders") => Some(async_rt::current_fast_timeout()),
        ("DELETE", _) => Some(async_rt::current_cancel_timeout()),
        // Reconcile order-state GETs keep their historical 2 s deadline:
        // the account reconcile pool's client-level timeout is the Query
        // ceiling (5 s), which would let a reconcile attempt hang
        // 2.5× longer than the retry ladder expects.
        ("GET", p) if p.starts_with("/data/order/") => Some(std::time::Duration::from_millis(2000)),
        _ => None,
    }
}

/// Dispatch on an explicit `client` — one of a role pool's connections,
/// typically the one reserved by an admission
/// [`http1_pool::Permit`]. Threading the exact connection (rather than a
/// fresh round-robin pick) is what lets the fire-and-track path guarantee a
/// request runs on its reserved warm connection instead of opening a cold one.
async fn execute_http_on(
    client: crate::http1_pool::PooledClient,
    attempt_id: u64,
    method: &reqwest::Method,
    url: &Arc<str>,
    path: &str,
    headers: &super::auth::AuthHeaders,
    body: Bytes,
) -> HttpReply {
    debug_assert_ne!(attempt_id, 0, "every HTTP dispatch requires an attempt ID");
    // Lazily derived — only the error branches need it, and parsing the
    // URL eagerly cost a full `Url::parse` + allocs on every request.
    let prewarm_url = |u: &str| {
        reqwest::Url::parse(u)
            .map(|mut parsed| {
                parsed.set_path("/time");
                parsed.set_query(None);
                parsed.set_fragment(None);
                parsed.to_string()
            })
            .unwrap_or_else(|_| "https://clob.polymarket.com/time".to_string())
    };
    let req_timeout = per_request_timeout(&method, &path);
    let mut req = client
        .client()
        .request(method.clone(), url.as_ref())
        .header("Content-Type", "application/json")
        .body(body);
    // Per-request timeout override (FAST / CANCEL paths only). The pool
    // client is built with a 2 s ceiling; this narrows it to the
    // configured flat value (default 1000 ms — see
    // `async_rt::init_http_timeout`).
    if let Some(t) = req_timeout {
        req = req.timeout(t);
    }
    // Attach Poly-Auth headers (POLY_ADDRESS / POLY_SIGNATURE /
    // POLY_TIMESTAMP / POLY_API_KEY / POLY_PASSPHRASE).
    for (k, v) in headers.as_pairs() {
        req = req.header(k, v);
    }
    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            client.note_transport_failure(prewarm_url(url.as_ref()));
            return Err(map_reqwest_err(e));
        }
    };
    let status = resp.status();
    if !status.is_success() {
        return match resp.text().await {
            Ok(body) => {
                client.note_transport_success();
                Err(HttpErr::Status(status.as_u16(), body))
            }
            Err(error) => {
                client.note_transport_failure(prewarm_url(url.as_ref()));
                Err(map_reqwest_err(error))
            }
        };
    }
    // Split response-body transport from JSON syntax/shape errors. Using
    // `Response::json` would wrap both in `reqwest::Error`, losing the exact
    // distinction needed by placement reconciliation.
    let bytes = match resp.bytes().await {
        Ok(bytes) => {
            client.note_transport_success();
            bytes
        }
        Err(error) => {
            client.note_transport_failure(prewarm_url(url.as_ref()));
            return Err(map_reqwest_err(error));
        }
    };
    serde_json::from_slice(&bytes).map_err(|error| {
        let raw_body = String::from_utf8_lossy(&bytes);
        HttpErr::InvalidResponse(compact_order_lookup_evidence_text(&format!(
            "json_parse_error={error} raw_body={raw_body}"
        )))
    })
}

/// DELETE is idempotent by order ID. If its primary connection times out or
/// fails at the transport layer (reset, broken pipe, TLS EOF, connect failure),
/// retry exactly once on a different account Cancel slot while new placement
/// admission remains cluster-gated. Place requests never enter this helper's
/// alternate-connection branch.
async fn execute_http_with_cancel_connection_failure_hedge(
    client: crate::http1_pool::PooledClient,
    attempt_id: u64,
    account_id: &str,
    method: &reqwest::Method,
    url: &Arc<str>,
    path: &str,
    headers: &super::auth::AuthHeaders,
    body: Bytes,
) -> HttpReply {
    let role = client.role();
    let slot = client.slot();
    let hedge_body = (method == reqwest::Method::DELETE).then(|| body.clone());
    let first = execute_http_on(client, attempt_id, method, url, path, headers, body).await;
    let failure_kind = match &first {
        Err(HttpErr::Timeout) => super::network_incident::ConnectionFailureKind::Timeout,
        Err(HttpErr::Transport(_)) => super::network_incident::ConnectionFailureKind::Transport,
        _ => return first,
    };
    super::network_incident::note_http_connection_failure(role, slot, failure_kind);
    let Some(hedge_body) = hedge_body else {
        return first;
    };

    let hedge_permit = crate::http1_pool::try_borrow_account(
        account_id,
        crate::http1_pool::Role::Cancel,
    );
    let hedge_client = hedge_permit
        .as_ref()
        .map(crate::http1_pool::Permit::pooled_client)
        .unwrap_or_else(|| crate::http1_pool::pooled_client(crate::http1_pool::Role::Cancel));
    let hedge_role = hedge_client.role();
    let hedge_slot = hedge_client.slot();
    let hedge_attempt = hedge_client.allocate_attempt_id();
    crate::latency::record_ns("polymarket.http.cancel.connection_failure_hedge", 1);
    let hedged = execute_http_on(
        hedge_client,
        hedge_attempt,
        method,
        url,
        path,
        headers,
        hedge_body,
    )
    .await;
    drop(hedge_permit);
    if let Some(kind) = match &hedged {
        Err(HttpErr::Timeout) => Some(super::network_incident::ConnectionFailureKind::Timeout),
        Err(HttpErr::Transport(_)) => {
            Some(super::network_incident::ConnectionFailureKind::Transport)
        }
        _ => None,
    } {
        super::network_incident::note_http_connection_failure(hedge_role, hedge_slot, kind);
    }
    hedged
}

/// Typed `POST /order` wire bodies. Serialized in one pass with
/// `serde_json::to_string` on the hot path (the legacy `json!{…}` +
/// `.to_string()` pair built and then re-walked a full `Value` tree per
/// order). Key NAMES must stay wire-exact; key order is irrelevant to
/// the server (JSON object).
#[derive(Debug, Clone, serde::Serialize)]
#[serde(untagged)]
pub(crate) enum PolyOrderBody {
    V2(WireBodyV2),
}

/// One-pass `DELETE /order` body for the hot cancel path.
#[derive(serde::Serialize)]
struct CancelBody<'a> {
    #[serde(rename = "orderID")]
    order_id: &'a str,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct WireBodyV2 {
    pub owner: String,
    #[serde(rename = "orderType")]
    pub order_type: &'static str,
    #[serde(rename = "postOnly")]
    pub post_only: bool,
    #[serde(rename = "deferExec")]
    pub defer_exec: bool,
    pub order: WireOrderV2,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct WireOrderV2 {
    pub salt: u64,
    pub maker: String,
    pub signer: String,
    pub taker: String,
    #[serde(rename = "tokenId")]
    pub token_id: String,
    #[serde(rename = "makerAmount")]
    pub maker_amount: String,
    #[serde(rename = "takerAmount")]
    pub taker_amount: String,
    pub side: &'static str,
    #[serde(rename = "signatureType")]
    pub signature_type: u8,
    pub timestamp: String,
    pub expiration: String,
    pub metadata: String,
    pub builder: String,
    pub signature: String,
}

/// User-feed gap-replay tuning (sourced from `exchanges[polymarket]`). All
/// values in milliseconds; the rewinds are quantised to whole seconds for the
/// second-granular `/data/trades?after=` API. `Default` matches the historical
/// hard-coded behaviour (2s cadence, 5s rewinds).
#[derive(Clone, Copy, Debug)]
pub struct GapReplayConfig {
    /// Periodic replay cadence (ms).
    pub interval_ms: u64,
    /// Periodic replay `?after=` rewind from now (ms).
    pub periodic_rewind_ms: u64,
    /// Reconnect replay `?after=` rewind from last match_time (ms).
    pub reconnect_rewind_ms: u64,
}

impl Default for GapReplayConfig {
    fn default() -> Self {
        Self {
            interval_ms: 2000,
            periodic_rewind_ms: 10_000,
            reconnect_rewind_ms: 5000,
        }
    }
}

/// Shared state between PolymarketTrade and the user_feed WebSocket thread.
pub struct SharedState {
    /// Strategy instance identifier (the `[poly.<id>]` key). Tags each
    /// row in the per-request latency CSV so a single file can hold
    /// multiple instances. `"cli"` for one-off CLI subcommands.
    pub(crate) instance_id: String,
    /// Account-wide physical/virtual ledger shared by every executor route.
    pub account_state: Arc<hexagent_account::account::shared_account::SharedAccount>,
    /// Message-only endpoint for strategy/cold-control producers.  Keeping it
    /// beside the read/snapshot account reference prevents those producers
    /// from invoking mutations on the live aggregate object.
    pub account_owner_handle: hexagent_account::account::shared_account::SharedAccountHandle,
    /// Account-owner-written execution identity and open-order state. Request,
    /// cancel, completion and private workers read immutable RCU snapshots.
    execution_state: ArcSwap<ExecutionStateSnapshot>,
    /// Complete ownership captured at the same publication edge as the
    /// runtime OID maps. It is a conservative repair root for the rare case
    /// where a cold snapshot replaces an instance lifecycle row before its
    /// asynchronous typed WAL delta is folded into the aggregate state.
    runtime_order_ownership: RuntimeOwnershipIndex,
    /// Startup-published instance → numeric strategy owner directory. Private
    /// producers resolve the durable instance identity here exactly once and
    /// publish an already-routed lifecycle message to the engine.
    strategy_owner_by_instance: ArcSwap<HashMap<String, u16>>,
    /// client_order_id → token_id (outcome asset). Written alongside the
    /// coid↔oid maps at registration and kept for the SAME lifetime, so the
    /// event-expiry sweep can purge an event's mappings by its outcome
    /// tokens. We deliberately KEEP coid↔oid mappings across order-lifecycle
    /// rejects/cancels (a "post-only crosses book" 400 or a cancel can still
    /// be followed by a real fill — the racy reject/cancel-then-fill case)
    /// so a late fill push still resolves its coid instead of arriving
    /// `<unmapped>` and broadcasting to every instance. This map is reclaimed
    /// only after the strategy's settled-event FIFO explicitly evicts the
    /// event, matching the full late-revision retention window.
    // `coid_to_token` is retained inside `execution_state` for the same
    // late-revision lifetime as the bidirectional order identity.
    /// Order IDs (== local EIP-712 order hashes) of the RTT probe's
    /// synthetic resting orders — ring of the most recent 64. The user
    /// feed consults this to identify probe placement / cancellation
    /// pushes: they carry no coid mapping, so without this they'd
    /// surface as `<unmapped>` (an ops anomaly signal expected to stay
    /// at zero) and broadcast to every instance. Identified probe
    /// events are logged at DEBUG and NOT forwarded.
    pub(crate) probe_order_ids: ProbeOrderIdRing,
    /// Authentication for REST requests
    pub auth: PolyAuth,
    /// EIP-712 order signer (v1 — pre-2026-04-28-cutover).
    pub signer: OrderSigner,
    /// EIP-712 order signer (v2 — post-cutover). `Some` iff this
    /// `SharedState` was initialised with `clob_version = "v2"`.
    pub signer_v2: Option<super::signer_v2::OrderSignerV2>,
    /// The address that actually owns the orders we place on-book — used
    /// to match incoming fills (WS maker-leg match + REST
    /// `/data/trades?maker_address=` gap recovery). For POLY_1271 (v2 deposit
    /// wallet) this is the funder/DW, which `with_funder` wrote into
    /// `signer_v2.maker_address`. `signer.maker_address` is the EOA
    /// (`derive_addresses` fixes POLY_1271 to the EOA), so matching fills
    /// against it silently dropped EVERY maker fill — the ledger never
    /// decremented and the strategy over-quoted SELL against phantom
    /// inventory (CLOB `not enough balance`).
    pub order_maker_address: String,
    /// Which CLOB protocol to use for order placement / signing.
    /// "v2" (default) = 2026-04-28 contract & schema; explicit "v1"
    /// opts back into the legacy path. Every `sign_and_build_body`
    /// and auth path dispatches on this.
    pub clob_version: ClobVersion,
    /// Whether the live router may use Polymarket's batch endpoints
    /// (`POST /orders`, `DELETE /orders`). When `false`, every place /
    /// cancel / update is routed through the single-order endpoints
    /// (`POST /order`, `DELETE /order`) dispatched **concurrently** —
    /// all requests are kicked off first via `http_call_async` (which
    /// returns immediately, with the HTTP work running on the shared
    /// async runtime; reqwest h2 multiplexes them onto a single TCP
    /// connection), then receivers are drained. Critical path =
    /// max single-RTT, not sum of singles. Surfaced from
    /// `exchanges[polymarket].use_batch_orders` (default true).
    pub use_batch_orders: bool,
    /// CLOB host used for all order / cancel / heartbeat requests.
    /// Populated from `exchanges[polymarket].api_url_prefix`, falling
    /// back to `DEFAULT_CLOB_BASE_URL` (= v1 host) when unset. MUST
    /// match `clob_version`: v2-signed orders require `clob-v2.polymarket.com`.
    pub clob_base_url: String,
    /// Startup-built hot single-order endpoint. Arc cloning is allocation-free
    /// when a fire worker hands the URL to the async order runtime.
    order_url: Arc<str>,
    /// Startup-filled, bounded request-body pool. Hot single-order requests
    /// take one buffer without blocking and return its allocation after the
    /// async HTTP future has dropped its body reference.
    request_buffers: Arc<ArrayQueue<BytesMut>>,
    /// Immutable-reader mirror of the account-owner's live-position replay
    /// watermark. The mutable trade ledger itself lives only on the account
    /// owner thread and is never exposed through a lock.
    live_position_last_match_secs: AtomicU64,
    #[cfg(test)]
    test_live_position: Mutex<LivePositionManager>,
    /// Narrow user-feed health handle shared with the strategy (pause-quoting
    /// signals). See [`UserFeedHealth`]. The user feed writes; the strategy
    /// reads (wired via `set_user_feed_health` in `build_strategies`).
    pub user_feed_health: std::sync::Arc<super::live_position::UserFeedHealth>,
    /// User-feed gap-replay cadence / rewind tuning (from config).
    pub gap_replay: GapReplayConfig,
    rate_limiter: crate::exchange::AtomicRateLimiter,
    /// Per-INSTANCE balance-error backoff deadlines (wall-clock ns), keyed
    /// by the placing `instance_id`. A future deadline means that instance's
    /// `submit_order` / `batch_submit_orders` / `batch_update_orders`
    /// pre-reject new placements so we stop hammering the server with doomed
    /// submits while a racing cancel releases the server-side allowance (a
    /// prior cancel timed out → orphan → server still reserves funds).
    /// Per-instance (not account-wide) so one strategy hitting `not enough
    /// balance` never pauses a shared-wallet sibling's submits. Absent or
    /// past = not in backoff.
    pub(crate) balance_backoff_until_ns: ArcSwap<HashMap<String, u64>>,
    /// Account-wide policy backoff after the CLOB explicitly responds
    /// `trading is disabled`. Unlike an ordinary 5xx this is a definitive
    /// rejection, so no orphan is created. The short circuit also prevents
    /// every strategy instance sharing this account from retrying at quote
    /// cadence while the market is administratively disabled.
    pub(crate) trading_disabled_backoff_until_ns: std::sync::atomic::AtomicU64,
    /// Per-token (asset_id) `invalid token id` backoff. The CLOB rejects an
    /// order with `invalid token id` when the token isn't registered on the
    /// orderbook — e.g. Gamma lists a 5-min event before its CLOB book is
    /// live. Retrying at quote cadence is a useless storm (live 2026-06-22
    /// 03:20: 4,746 rejects / 4 min, 0 fills). After `INVALID_TOKEN_STRIKES`
    /// consecutive rejects for a token we block its submits for
    /// `INVALID_TOKEN_BACKOFF_NS`, then allow one re-probe. Unlike the global
    /// balance backoff, this is PER-TOKEN: only the bad token is gated, other
    /// events quote normally. `token → (consecutive_strikes, block_until_ns)`;
    /// cleared on any accepted order for the token.
    pub(crate) invalid_token_backoff: ArcSwap<HashMap<String, (u32, u64)>>,
    /// Wall-clock ns until which HTTP 425 "service not ready" backpressure
    /// WARNs are suppressed across BOTH cancel and place paths. Set to
    /// `now + 5min` on the first 425 of each window; subsequent 425s are
    /// silently retried (the retry / reconcile machinery is unaffected).
    /// Non-425 errors always WARN. Read/write Relaxed.
    ///
    /// One shared window for cancel and place because a 425 storm hits
    /// both endpoints together (Polymarket service-level overload, not
    /// per-endpoint) — observed 2026-05-06 21:00–21:35 with 15,045 place
    /// 425s + cancel 425s in the same 30 min. One operator alert per
    /// 5 min is more useful than 15k near-identical log lines.
    pub(crate) http_425_warn_silent_until_ns: std::sync::atomic::AtomicU64,
    /// Per-coid wall-clock deadlines for HTTP 425 reconcile backoff. A 425
    /// parks only the affected orphan; unrelated orders continue reconciling.
    /// Entries are cleared on terminal removal or lazily after expiry.
    ///
    /// Rationale: HTTP 425 means "service is overloaded, retry later". The
    /// reconciler's 500 ms / 1.5 s loop converts that signal into a flood
    /// (live 2026-05-12 13:14–13:37: 1,975 retry log lines for one coid).
    /// During backoff we keep that orphan parked but skip its HTTP roundtrip
    /// — the reconciler naturally retries it after the deadline expires.
    /// This must never become an account-wide circuit breaker: one noisy
    /// order cannot delay terminal audits for every strategy sharing state.
    pub(crate) http_425_reconcile_backoff_until_ns: PublishedStringMap<u64>,
    /// Structured safety telemetry counters. These are per SharedState
    /// (account/instance route) and are emitted with every transition so the
    /// live log can be aggregated without a separate metrics dependency.
    pub(crate) http_425_circuit_entries_total: std::sync::atomic::AtomicU64,
    pub(crate) startup_recovery_lookup_warn_silent_until_ns: std::sync::atomic::AtomicU64,
    pub(crate) get_live_delete_uncertain_total: std::sync::atomic::AtomicU64,
    pub(crate) get_live_delete_uncertain_warn_silent_until_ns: std::sync::atomic::AtomicU64,
    pub(crate) get_live_delete_uncertain_suppressed: std::sync::atomic::AtomicU64,
    pub(crate) terminal_trade_backfill_missing_total: std::sync::atomic::AtomicU64,
    pub(crate) terminal_trade_backfill_missing_warn_silent_until_ns: std::sync::atomic::AtomicU64,
    pub(crate) terminal_trade_backfill_missing_suppressed: std::sync::atomic::AtomicU64,
    pub(crate) terminal_trade_backfill_noop_total: std::sync::atomic::AtomicU64,
    pub(crate) terminal_trade_backfill_noop_log_silent_until_ns: std::sync::atomic::AtomicU64,
    pub(crate) terminal_trade_backfill_noop_suppressed: std::sync::atomic::AtomicU64,
    /// Per-coid cumulative count of the exact ambiguous DELETE response
    /// "order can't be found - already canceled or matched" returned by
    /// DELETEs issued from the cancel-orphan reconciler. The initial cancel
    /// and ordinary single/batch cancel paths never advance this counter.
    /// The first two reconcile responses remain cancel orphans; the third
    /// resolves the local lifecycle to Cancelled. Cleared with the order on
    /// any terminal result.
    pub(crate) reconcile_cancel_not_found_counts: PublishedStringMap<u32>,
    /// Independent placement/cancel retry counters. Placement counts only
    /// uninterrupted explicit not-found responses; a coid can be in both
    /// orphan sets after cancel-before-ack, so cancel diagnostics must never
    /// exhaust the bounded placement retry budget.
    pub(crate) reconcile_attempts: ReconcileAttemptCounters,

    /// Per-coid **exponential backoff** gate for placement not-found GETs:
    /// wall-clock ns before which the next reconcile GET for this coid is
    /// skipped. Set after each not-found to `now + 0.5s · 2^(attempt-1)`
    /// (0.5s → 1s → 2s across the four observations). Without this the
    /// reconciler re-hammers the GET every ~0.5s re-emit; during a PM REST
    /// slowdown that pours ~5 GETs/orphan onto an already-struggling endpoint
    /// and prolongs the episode. Backing off costs only slower orphan
    /// cleanup — a real fill still lands via the WS user_feed independent of
    /// this GET. Cleared alongside the placement counter on any
    /// conclusive placement resolution.
    pub(crate) placement_reconcile_next_retry_ns: PublishedStringMap<u64>,
    /// Per-coid retry deadline for cancel-order GETs that returned an explicit
    /// not-found/empty success. Jittered exponential delay avoids synchronizing
    /// many orphans on the strategy quote cadence while preserving the
    /// worst-case reservation until a conclusive status or private fill arrives.
    pub(crate) cancel_reconcile_next_retry_ns: PublishedStringMap<u64>,

    /// Coids whose cancel was rejected with a `pending/delayed` reason — the
    /// cancel raced the placement, so the order is still being processed and
    /// will shortly be LIVE (not gone). The reconcile cancel-orphan `""`
    /// (not-found) arm treats these as **Uncertain** and keeps retrying the GET
    /// until the order converges (LIVE → re-DELETE / MATCHED → Filled /
    /// CANCELED → Cancelled), instead of committing Cancelled on a
    /// not-yet-indexed order. Inserted at the cancel-reply classification sites
    /// and cleared only on conclusive resolution via `remove_order`.
    pub(crate) pending_delayed_orphans: PublishedStringSet,
    /// Coalescing wakeup for account-wide settled-history GC.
    settled_gc_tx: crossbeam_channel::Sender<()>,
    /// Terminal executor bookkeeping can acquire several audit/runtime locks.
    /// Private account-apply only enqueues the coid; a background worker owns
    /// the readiness check and final teardown.
    account_maintenance_tx: crossbeam_channel::Sender<AccountMaintenanceJob>,
    /// Lossless, higher-priority account-owner lane (capacity 16,384). Order
    /// identity/open-state commands, execution replies and authenticated
    /// private events share FIFO producer ordering; pre-dispatch overflow
    /// rejects the order, while terminal overflow marks inventory uncertain.
    account_lifecycle_tx: crossbeam_channel::Sender<AccountLifecycleJob>,
    /// Multi-producer completion/lifecycle telemetry → one SCHED_OTHER audit
    /// owner. Lifecycle trace overflow is explicitly lossy and cannot delay
    /// account mutation or execution completion.
    /// FIFO capacity is [`EXECUTION_AUDIT_QUEUE_CAPACITY`]; overflow drops only
    /// the audit record and logs loudly, never an account/lifecycle mutation.
    /// This lane has no replay because attempt timing is operational telemetry.
    attempt_audit_tx: crossbeam_channel::Sender<AuditJob>,
    shutdown_token: ShutdownToken,
    background_worker_handles: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

/// Retry accounting for mixed placement/cancel orphans. Placement is a
/// bounded state machine; cancel attempts are unbounded diagnostics only.
#[derive(Debug)]
pub(crate) struct PublishedStringMap<V> {
    published: ArcSwap<HashMap<String, V>>,
}

/// Bounded, lock-free reader view of synthetic RTT-probe order identities.
///
/// The probe writer is operational/cold. Private websocket parsing is the
/// latency-sensitive reader, so it observes an immutable RCU snapshot and
/// never contends with probe placement or cancellation.
#[derive(Debug, Default)]
pub(crate) struct ProbeOrderIdRing {
    published: ArcSwap<std::collections::VecDeque<Arc<str>>>,
}

impl ProbeOrderIdRing {
    const CAPACITY: usize = 64;

    pub(crate) fn contains(&self, order_id: &str) -> bool {
        let normalized = normalize_order_id(order_id);
        self.published
            .load()
            .iter()
            .any(|probe| probe.as_ref() == normalized)
    }

    pub(crate) fn insert(&self, order_id: &str) {
        let normalized = Arc::<str>::from(normalize_order_id(order_id));
        self.published.rcu(|current| {
            let mut next = (**current).clone();
            if let Some(index) = next.iter().position(|probe| probe == &normalized) {
                next.remove(index);
            }
            if next.len() == Self::CAPACITY {
                next.pop_front();
            }
            next.push_back(Arc::clone(&normalized));
            Arc::new(next)
        });
    }
}

impl<V> Default for PublishedStringMap<V> {
    fn default() -> Self {
        Self {
            published: ArcSwap::from_pointee(HashMap::new()),
        }
    }
}

impl<V: Clone> PublishedStringMap<V> {
    fn get(&self, key: &str) -> Option<V> {
        self.published.load().get(key).cloned()
    }

    fn insert(&self, key: String, value: V) {
        self.published.rcu(|current| {
            let mut next = (**current).clone();
            next.insert(key.clone(), value.clone());
            Arc::new(next)
        });
    }

    fn remove(&self, key: &str) -> Option<V> {
        let previous = self.get(key);
        if previous.is_some() {
            self.published.rcu(|current| {
                let mut next = (**current).clone();
                next.remove(key);
                Arc::new(next)
            });
        }
        previous
    }

    fn update<F>(&self, key: &str, update: F)
    where
        F: Fn(Option<&V>) -> Option<V>,
    {
        self.published.rcu(|current| {
            let mut next = (**current).clone();
            match update(current.get(key)) {
                Some(value) => {
                    next.insert(key.to_string(), value);
                }
                None => {
                    next.remove(key);
                }
            }
            Arc::new(next)
        });
    }

    fn snapshot(&self) -> Arc<HashMap<String, V>> {
        self.published.load_full()
    }
}

#[derive(Debug, Default)]
pub(crate) struct PublishedStringSet {
    published: ArcSwap<HashSet<String>>,
}

impl PublishedStringSet {
    fn contains(&self, key: &str) -> bool {
        self.published.load().contains(key)
    }

    fn insert(&self, key: String) {
        self.published.rcu(|current| {
            let mut next = (**current).clone();
            next.insert(key.clone());
            Arc::new(next)
        });
    }

    fn remove(&self, key: &str) -> bool {
        let existed = self.contains(key);
        if existed {
            self.published.rcu(|current| {
                let mut next = (**current).clone();
                next.remove(key);
                Arc::new(next)
            });
        }
        existed
    }
}

#[derive(Default)]
pub(crate) struct ReconcileAttemptCounters {
    placement: PublishedStringMap<u32>,
    cancel: PublishedStringMap<u32>,
}

impl ReconcileAttemptCounters {
    #[cfg(test)]
    fn next_placement(&self, coid: &str) -> u32 {
        self.next_placement_with_evidence(coid, 1)
    }

    fn next_placement_with_evidence(&self, coid: &str, observations: u32) -> u32 {
        self.placement.update(coid, |attempt| {
            Some(
                attempt
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(observations.max(1)),
            )
        });
        self.placement.get(coid).unwrap_or(0)
    }

    fn clear_placement(&self, coid: &str) {
        self.placement.remove(coid);
    }

    fn next_cancel(&self, coid: &str) -> u32 {
        self.cancel.update(coid, |attempt| {
            Some(attempt.copied().unwrap_or(0).saturating_add(1))
        });
        self.cancel.get(coid).unwrap_or(0)
    }

    fn clear_cancel(&self, coid: &str) {
        self.cancel.remove(coid);
    }
}

/// A single-slot reconcile needs four consecutive explicit server not-found
/// observations. When two distinct account Reconcile slots independently
/// return 404 in the same pass, two passes contain the same four observations
/// and may converge without the extra exponential-backoff delay. Any
/// unavailable, found, or otherwise non-not-found lookup resets the run.
pub(crate) const RECONCILE_NOT_FOUND_RETRY_LIMIT: u32 = 4;

fn shutdown_absent_placement_phantom_is_terminal(status: OrderStatus, streak: u32) -> bool {
    status == OrderStatus::NewOrderTimeout && streak >= RECONCILE_NOT_FOUND_RETRY_LIMIT
}

/// Base interval for the placement not-found retry backoff. The gap before
/// the Nth GET doubles: 0.5s, 1s, 2s across attempts 2..4, so four
/// observations span ~3.5s. This keeps orphan resolution responsive while
/// preventing quote-cadence retries from amplifying a PM REST slowdown;
/// orphans that actually filled are booked via the WS user_feed independently.
pub(crate) const RECONCILE_BACKOFF_BASE_MS: u64 = 500;

fn placement_reconcile_backoff_ms(attempts: u32) -> u64 {
    RECONCILE_BACKOFF_BASE_MS.saturating_mul(1u64 << (attempts.saturating_sub(1)))
}

fn cancel_reconcile_backoff_ms(coid: &str, attempts: u32) -> u64 {
    let exponential = RECONCILE_BACKOFF_BASE_MS
        .saturating_mul(1u64 << attempts.saturating_sub(1).min(3))
        .min(4_000);
    let hash = coid.bytes().fold(0xcbf29ce484222325u64, |hash, byte| {
        hash.wrapping_mul(0x100000001b3)
            .wrapping_add(u64::from(byte))
    });
    exponential.saturating_add(hash.wrapping_add(u64::from(attempts)) % 251)
}

/// Backoff window applied to `reconcile_orphans` when a 425 "service not
/// ready" is observed. One second breaks the immediate retry loop while
/// allowing the orphan to enter the same four-consecutive-not-found proof
/// used by timeout, HTTP 5xx, and transport-origin placements. A repeated
/// 425 extends this per-coid deadline and still does not advance that proof.
pub(crate) const HTTP_425_BACKOFF_NS: u64 = 1_000_000_000;

/// Add or extend a 425 backoff for one orphan. Kept pure so tests can verify
/// isolation and monotonic deadlines without constructing authenticated
/// `SharedState`.
#[cfg(test)]
fn record_http_425_backoff(
    backoffs: &mut HashMap<String, u64>,
    client_order_id: &str,
    now_ns: u64,
) {
    if client_order_id.is_empty() {
        return;
    }
    let new_deadline = now_ns.saturating_add(HTTP_425_BACKOFF_NS);
    let deadline = backoffs.entry(client_order_id.to_string()).or_insert(0);
    *deadline = (*deadline).max(new_deadline);
}

/// Check one orphan's 425 backoff, removing an expired entry as part of the
/// lookup. No other coid can influence the result.
#[cfg(test)]
fn is_http_425_backoff_active(
    backoffs: &mut HashMap<String, u64>,
    client_order_id: &str,
    now_ns: u64,
) -> bool {
    match backoffs.get(client_order_id).copied() {
        Some(until) if now_ns < until => true,
        Some(_) => {
            backoffs.remove(client_order_id);
            false
        }
        None => false,
    }
}

/// Remove every owned coid↔oid / coid↔token entry whose token is in
/// `settling`, keeping sibling instances and all other events intact. Returns
/// the count reclaimed. Pure (maps passed in) so it's unit-testable without a
/// live `SharedState`. Callers hold all three map locks for the duration.
fn reclaim_token_mappings(
    coid_to_oid: &mut HashMap<String, String>,
    oid_to_coid: &mut HashMap<String, String>,
    coid_to_token: &mut HashMap<String, String>,
    settling: &[String],
    owned_coids: Option<&HashSet<String>>,
) -> usize {
    let settling: std::collections::HashSet<&str> = settling.iter().map(|s| s.as_str()).collect();
    let stale: Vec<String> = coid_to_token
        .iter()
        .filter(|(coid, tok)| {
            settling.contains(tok.as_str()) && owned_coids.is_none_or(|owned| owned.contains(*coid))
        })
        .map(|(coid, _)| coid.clone())
        .collect();
    for coid in &stale {
        if let Some(oid) = coid_to_oid.remove(coid) {
            oid_to_coid.remove(&normalize_order_id(&oid));
        }
        coid_to_token.remove(coid);
    }
    stale.len()
}

impl SharedState {
    /// Publish the immutable live strategy-owner directory before the private
    /// feed starts. Replacing it is a startup/reconfiguration operation; reads
    /// on the private owner lane are lock-free.
    pub fn install_strategy_owner_routes(&self, routes: HashMap<String, u16>) {
        self.strategy_owner_by_instance.store(Arc::new(routes));
    }

    #[inline]
    pub fn strategy_owner(&self, instance_id: &str) -> Option<u16> {
        self.strategy_owner_by_instance
            .load()
            .get(instance_id)
            .copied()
            .filter(|owner| *owner != SYSTEM_STRATEGY_OWNER)
    }

    pub(crate) fn live_position_last_match_secs(&self) -> u64 {
        self.live_position_last_match_secs.load(Ordering::Acquire)
    }

    pub(crate) fn publish_live_position_watermark(&self, match_time_secs: u64) {
        self.live_position_last_match_secs
            .fetch_max(match_time_secs, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn with_test_live_position<R>(
        &self,
        apply: impl FnOnce(&mut LivePositionManager) -> R,
    ) -> R {
        let mut live_position = self.test_live_position.lock().unwrap();
        apply(&mut live_position)
    }

    fn diagnose_private_record_on_owner(
        &self,
        record: serde_json::Value,
    ) -> std::result::Result<super::user_feed::ParsedPrivateEvent, String> {
        if self.account_state.is_account_owner_thread() {
            return Err("private reconcile diagnosis cannot wait on its own account owner".into());
        }
        let (completion, result) = crossbeam_channel::bounded(1);
        self.account_lifecycle_tx
            .send(AccountLifecycleJob::DiagnosePrivate { record, completion })
            .map_err(|_| "account owner lifecycle lane is closed".to_string())?;
        result
            .recv_timeout(Duration::from_secs(5))
            .map_err(|error| format!("account owner private diagnosis timed out: {error}"))
    }

    fn take_request_buffer(&self) -> Option<BytesMut> {
        let buffer = self.request_buffers.pop();
        if buffer.is_none() {
            crate::latency::record_ns("polymarket.order.request_buffer_pool_exhausted", 1);
        }
        buffer
    }

    fn recycle_request_buffer(&self, mut buffer: BytesMut) {
        buffer.clear();
        let _ = self.request_buffers.push(buffer);
    }

    pub(crate) fn request_settled_gc(&self) {
        if !self.account_state.has_settled_gc_candidates() {
            return;
        }
        self.account_state.note_settled_gc_activity();
        let _ = self.settled_gc_tx.try_send(());
    }

    pub(crate) fn request_settled_gc_for_token(&self, token_id: &str) {
        if !self
            .account_state
            .has_settled_gc_candidate_for_token(token_id)
        {
            return;
        }
        self.account_state.note_settled_gc_activity();
        let _ = self.settled_gc_tx.try_send(());
    }

    fn apply_account_lifecycle_job(
        &self,
        execution: &mut ExecutionStateOwner,
        live_position: &mut LivePositionManager,
        private_replay: &mut super::user_feed::PrivateReplayOwner,
        job: AccountLifecycleJob,
    ) {
        match job {
            AccountLifecycleJob::StartupBarrier(done) => {
                let _ = done.send(());
            }
            AccountLifecycleJob::RegisterLocalOrder { command, ownership } => {
                if let Some(ownership) = ownership {
                    if self
                        .account_state
                        .backfill_order_ownership(&ownership)
                        .is_none()
                    {
                        self.runtime_order_ownership.remove(&ownership.order_id);
                        self.user_feed_health.set_inventory_uncertain(true);
                        log::error!(
                            "[PolymarketTrade] async ownership persistence conflict coid={} oid={}",
                            ownership.client_order_id,
                            ownership.order_id,
                        );
                        return;
                    }
                }
                execution.apply(self, command);
            }
            AccountLifecycleJob::ExecutionState(command) => execution.apply(self, command),
            AccountLifecycleJob::RebindServerIdentity {
                client_order_id,
                exchange_order_id,
                token,
            } => {
                if !self
                    .account_state
                    .rebind_order_id(&client_order_id, &exchange_order_id)
                {
                    self.user_feed_health.set_inventory_uncertain(true);
                    log::error!(
                        "[PolymarketTrade] refused inconsistent server identity coid={} oid={}",
                        client_order_id,
                        exchange_order_id,
                    );
                    return;
                }
                let ownership = self.account_state.order(&client_order_id);
                if let Some(ownership) = ownership {
                    if let Err(error) = self
                        .runtime_order_ownership
                        .insert(&exchange_order_id, ownership)
                    {
                        self.user_feed_health.set_inventory_uncertain(true);
                        log::error!(
                            "[PolymarketTrade] fixed runtime identity publication failed coid={} oid={}: {}",
                            client_order_id,
                            exchange_order_id,
                            error,
                        );
                        return;
                    }
                }
                execution.apply(
                    self,
                    ExecutionStateCommand::InstallIdentity {
                        client_order_id,
                        exchange_order_id,
                        token,
                    },
                );
            }
            AccountLifecycleJob::MarkLive {
                client_order_id,
                tracked,
                status,
                enqueued_ns,
            } => {
                crate::latency::record_ns(
                    "polymarket.account.mark_live_queue",
                    now_ns().saturating_sub(enqueued_ns),
                );
                let effective = self
                    .account_state
                    .mark_order_status_effective(&client_order_id, status);
                if matches!(
                    effective,
                    Some(OrderStatus::Accepted | OrderStatus::PartiallyFilled)
                ) {
                    execution.apply(
                        self,
                        ExecutionStateCommand::TrackOpen {
                            client_order_id: client_order_id.clone(),
                            tracked,
                        },
                    );
                    self.pending_delayed_orphans.remove(&client_order_id);
                    self.reconcile_cancel_not_found_counts
                        .remove(&client_order_id);
                    self.cancel_reconcile_next_retry_ns.remove(&client_order_id);
                }
            }
            AccountLifecycleJob::DeferredLifecycle(job) => self.apply_deferred_lifecycle(job),
            AccountLifecycleJob::PrivateCold(command) => {
                super::user_feed::apply_private_cold_command(
                    self,
                    live_position,
                    private_replay,
                    command,
                );
            }
            AccountLifecycleJob::DiagnosePrivate { record, completion } => {
                let parsed = super::user_feed::parse_user_event_diagnosed_owned(
                    &record,
                    self,
                    live_position,
                    private_replay,
                );
                self.publish_live_position_watermark(live_position.last_match_time_secs());
                let _ = completion.send(parsed);
            }
        }
    }

    fn apply_account_maintenance_job(&self, job: AccountMaintenanceJob) {
        match job {
            AccountMaintenanceJob::FilledCleanup(job) => {
                crate::latency::record_ns(
                    "polymarket.user.filled_cleanup_queue",
                    now_ns().saturating_sub(job.enqueued_ns),
                );
                self.finish_filled_order_if_audited(&job.client_order_id);
            }
        }
    }

    fn run_settled_gc_pass(
        &self,
        execution: &mut ExecutionStateOwner,
        live_position: &mut LivePositionManager,
    ) {
        if !self.account_state.has_settled_gc_candidates() {
            return;
        }
        let started = crate::latency::Instant::now();
        let (retired_events, retired_live_trades) =
            self.finalize_ready_settled_audit_retirements(execution, live_position);
        crate::latency::record("polymarket.account.settled_gc", started);
        if retired_events > 0 {
            debug!(
                "[PolySettledGc] account={} retired_events={} retired_live_trades={}",
                self.account_state.account_id(),
                retired_events,
                retired_live_trades,
            );
        }
    }

    fn spawn_account_owner_workers(
        shared: &Arc<Self>,
        initial_execution_state: ExecutionStateSnapshot,
        initial_live_position: LivePositionManager,
        lifecycle_rx: crossbeam_channel::Receiver<AccountLifecycleJob>,
        account_owner: hexagent_account::account::shared_account::SharedAccountOwnerState,
        maintenance_rx: crossbeam_channel::Receiver<AccountMaintenanceJob>,
        settled_gc_rx: crossbeam_channel::Receiver<()>,
    ) -> (
        std::thread::JoinHandle<()>,
        std::thread::JoinHandle<()>,
        crossbeam_channel::Receiver<()>,
    ) {
        let cold_weak = Arc::downgrade(shared);
        let lifecycle_weak = Arc::downgrade(shared);
        let account_id = shared.account_state.account_id().to_string();
        let cold_shutdown = shared.shutdown_token.clone();
        let cold_shutdown_rx = cold_shutdown.subscribe();
        let lifecycle_shutdown = shared.shutdown_token.clone();
        let lifecycle_shutdown_rx = lifecycle_shutdown.subscribe();
        let (ready_tx, ready_rx) = crossbeam_channel::bounded(2);
        let cold_ready_tx = ready_tx.clone();
        let lifecycle_account_id = account_id.clone();
        let lifecycle_account_state = Arc::clone(&shared.account_state);
        let cold_handle = std::thread::Builder::new()
            .name(format!("poly-account-owner-{}", shared.instance_id))
            .spawn(move || {
                crate::os_tune::pin_private_account_cold("polymarket-account-owner", &account_id);
                if let Err(error) = account_owner.mark_current_thread() {
                    log::error!("[PolymarketTrade] account owner binding failed: {error}");
                    return;
                }
                let _ = cold_ready_tx.send(());
                loop {
                    let Some(_shared) = cold_weak.upgrade() else {
                        break;
                    };
                    if cold_shutdown.is_finished() {
                        // Cold control remains single-writer and drains its
                        // explicit commands before the coalesced wallet wake.
                        loop {
                            if let Ok(command) = account_owner.receiver().try_recv() {
                                account_owner.execute(command);
                                continue;
                            }
                            if account_owner.wallet_receiver().try_recv().is_ok() {
                                account_owner.execute_wallet_calibration();
                                continue;
                            }
                            break;
                        }
                        break;
                    }
                    crossbeam_channel::select_biased! {
                        recv(account_owner.receiver()) -> command => match command {
                            Ok(command) => account_owner.execute(command),
                            Err(_) => break,
                        },
                        // Absolute wallet snapshots are replaceable,
                        // condition-scoped and coalesced. They deliberately
                        // never share a consumer thread with lossless private
                        // trade/order lifecycle work.
                        recv(account_owner.wallet_receiver()) -> wake => match wake {
                            Ok(()) => account_owner.execute_wallet_calibration(),
                            Err(_) => break,
                        },
                        recv(cold_shutdown_rx) -> phase => {
                            if matches!(phase, Ok(ShutdownPhase::Finished) | Err(_)) {
                                continue;
                            }
                        },
                    }
                }
            })
            .expect("spawn Polymarket cold account owner");

        let lifecycle_handle = std::thread::Builder::new()
            .name(format!("poly-lifecycle-owner-{}", shared.instance_id))
            .spawn(move || {
                let mut execution = ExecutionStateOwner::new(initial_execution_state);
                let mut live_position = initial_live_position;
                let mut private_replay = super::user_feed::PrivateReplayOwner::new();
                crate::os_tune::pin_private_account_apply(
                    "polymarket-lifecycle-owner",
                    &lifecycle_account_id,
                );
                if let Err(error) = lifecycle_account_state.mark_account_lifecycle_thread() {
                    log::error!("[PolymarketTrade] lifecycle owner binding failed: {error}");
                    return;
                }
                // `quanta` calibrates its fast clock on the first recorder
                // call (about 200 ms on macOS). Pay that one-time cost before
                // the startup barrier, never on the first private lifecycle.
                let latency_clock_ready = crate::latency::Instant::now();
                crate::latency::record("polymarket.account.owner_started", latency_clock_ready);
                let _ = ready_tx.send(());
                loop {
                    let Some(shared) = lifecycle_weak.upgrade() else {
                        break;
                    };
                    if lifecycle_shutdown.is_finished() {
                        // Producers are joined. Drain high-priority lifecycle
                        // first, then maintenance. GC is coalesced and runs
                        // last; cold account commands have their own worker.
                        loop {
                            if let Ok(job) = lifecycle_rx.try_recv() {
                                shared.apply_account_lifecycle_job(
                                    &mut execution,
                                    &mut live_position,
                                    &mut private_replay,
                                    job,
                                );
                                continue;
                            }
                            if let Ok(job) = maintenance_rx.try_recv() {
                                shared.apply_account_maintenance_job(job);
                                continue;
                            }
                            if settled_gc_rx.try_recv().is_ok() {
                                shared.run_settled_gc_pass(&mut execution, &mut live_position);
                                continue;
                            }
                            break;
                        }
                        break;
                    }
                    crossbeam_channel::select_biased! {
                        recv(lifecycle_rx) -> job => match job {
                            Ok(job) => shared.apply_account_lifecycle_job(
                                &mut execution,
                                &mut live_position,
                                &mut private_replay,
                                job,
                            ),
                            Err(_) => break,
                        },
                        recv(maintenance_rx) -> job => match job {
                            Ok(job) => shared.apply_account_maintenance_job(job),
                            Err(_) => break,
                        },
                        recv(settled_gc_rx) -> wake => match wake {
                            Ok(()) => shared.run_settled_gc_pass(
                                &mut execution,
                                &mut live_position,
                            ),
                            Err(_) => break,
                        },
                        recv(lifecycle_shutdown_rx) -> phase => {
                            if matches!(phase, Ok(ShutdownPhase::Finished) | Err(_)) {
                                continue;
                            }
                        },
                        default(std::time::Duration::from_millis(100)) => {
                            // A certificate may become ready without another
                            // producer wake. This poll is account-owner local
                            // and lower priority than both bounded work lanes.
                            shared.run_settled_gc_pass(&mut execution, &mut live_position);
                        }
                    }
                }
            })
            .expect("spawn Polymarket lifecycle owner");
        (cold_handle, lifecycle_handle, ready_rx)
    }

    fn spawn_attempt_audit_worker(
        shared: &Arc<Self>,
        rx: crossbeam_channel::Receiver<AuditJob>,
    ) -> std::thread::JoinHandle<()> {
        let weak = Arc::downgrade(shared);
        let shutdown = shared.shutdown_token.clone();
        let shutdown_rx = shutdown.subscribe();
        std::thread::Builder::new()
            .name(format!("poly-execution-audit-{}", shared.instance_id))
            .spawn(move || {
                let mut lifecycle_traces = HashMap::new();
                let mut structured_recorder = OrderAttemptRecorder::new();
                // Audit formatting/export is explicitly lower priority than
                // lifecycle/account mutation and has its own bounded lane.
                crate::os_tune::pin_background("polymarket-execution-audit");
                loop {
                    let Some(shared) = weak.upgrade() else {
                        break;
                    };
                    if shutdown.is_finished() {
                        while let Ok(job) = rx.try_recv() {
                            shared.write_audit_job(
                                &mut lifecycle_traces,
                                &mut structured_recorder,
                                job,
                            );
                        }
                        break;
                    }
                    crossbeam_channel::select_biased! {
                        recv(rx) -> job => match job {
                            Ok(job) => shared.write_audit_job(
                                &mut lifecycle_traces,
                                &mut structured_recorder,
                                job,
                            ),
                            Err(_) => break,
                        },
                        recv(shutdown_rx) -> _ => continue,
                    }
                }
            })
            .expect("spawn Polymarket attempt audit worker")
    }

    fn apply_deferred_lifecycle(&self, job: DeferredLifecycleJob) {
        crate::latency::record_ns(
            "polymarket.account.lifecycle_queue",
            now_ns().saturating_sub(job.enqueued_ns),
        );
        let apply_started = crate::latency::Instant::now();
        let branch_stage = match job.status {
            OrderStatus::Filled => "polymarket.account.lifecycle_apply.filled",
            OrderStatus::Cancelled => "polymarket.account.lifecycle_apply.cancelled",
            OrderStatus::Rejected => "polymarket.account.lifecycle_apply.rejected",
            _ => "polymarket.account.lifecycle_apply.status_update",
        };
        let ((), timing) = measure_deferred_lifecycle_stages(|| match job.status {
            OrderStatus::Filled => {
                self.account_state
                    .mark_filled_pending_audit(&job.client_order_id);
            }
            OrderStatus::Cancelled => {
                self.account_state
                    .mark_cancelled_pending_audit(&job.client_order_id);
            }
            OrderStatus::Rejected => {
                // StrategyAccount receives the authoritative rejection first.
                // Shared monitoring cleanup may persist and take lifecycle
                // locks, so it belongs exclusively on this cold writer.
                self.remove_order_resolved_as(&job.client_order_id, OrderStatus::Rejected);
            }
            status => {
                self.account_state
                    .mark_order_status_effective(&job.client_order_id, status);
            }
        });
        let total_ns = apply_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        let attributed_ns = timing
            .lock_wait_ns
            .saturating_add(timing.mutation_ns)
            .saturating_add(timing.persist_enqueue_ns);
        crate::latency::record_ns(
            "polymarket.account.lifecycle_apply.lock_wait",
            timing.lock_wait_ns.max(1),
        );
        let branch_lock_wait_stage = match job.status {
            OrderStatus::Filled => "polymarket.account.lifecycle_apply.lock_wait.filled",
            OrderStatus::Cancelled => "polymarket.account.lifecycle_apply.lock_wait.cancelled",
            OrderStatus::Rejected => "polymarket.account.lifecycle_apply.lock_wait.rejected",
            _ => "polymarket.account.lifecycle_apply.lock_wait.status_update",
        };
        crate::latency::record_ns(branch_lock_wait_stage, timing.lock_wait_ns.max(1));
        crate::latency::record_ns(
            "polymarket.account.lifecycle_apply.mutation",
            timing.mutation_ns.max(1),
        );
        crate::latency::record_ns(
            "polymarket.account.lifecycle_apply.persist_enqueue",
            timing.persist_enqueue_ns.max(1),
        );
        // Route lookup, audit wakeup, anomaly cleanup and Rejected's runtime
        // mutex cleanup intentionally remain outside the three account stages.
        // Preserve their remainder so an apparently fast account mutation
        // cannot hide a slow non-account tail.
        crate::latency::record_ns(
            "polymarket.account.lifecycle_apply.unattributed",
            total_ns.saturating_sub(attributed_ns).max(1),
        );
        crate::latency::record_ns(branch_stage, total_ns.max(1));
        crate::latency::record_ns("polymarket.account.lifecycle_apply", total_ns.max(1));
        #[cfg(test)]
        if let Some(completion) = job.completion {
            let _ = completion.send(now_ns().saturating_sub(job.enqueued_ns));
        }
    }

    fn defer_lifecycle_account_apply(&self, client_order_id: &str, status: OrderStatus) {
        let job = DeferredLifecycleJob {
            client_order_id: client_order_id.to_string(),
            status,
            enqueued_ns: now_ns(),
            #[cfg(test)]
            completion: None,
        };
        if let Err(error) = self
            .account_lifecycle_tx
            .try_send(AccountLifecycleJob::DeferredLifecycle(job))
        {
            // StrategyAccount still receives the authoritative lifecycle
            // update. Fail the shared monitoring account closed and require
            // reconnect/reconcile rather than blocking the completion lane.
            self.user_feed_health.set_inventory_uncertain(true);
            log::error!(
                "[PolymarketTrade] deferred lifecycle queue unavailable coid={} status={:?}: {}; shared account forced uncertain",
                client_order_id,
                status,
                error,
            );
        }
    }

    pub(crate) fn enqueue_private_cold(
        &self,
        command: super::user_feed::PrivateColdCommand,
    ) -> std::result::Result<
        (),
        crossbeam_channel::TrySendError<super::user_feed::PrivateColdCommand>,
    > {
        match self
            .account_lifecycle_tx
            .try_send(AccountLifecycleJob::PrivateCold(command))
        {
            Ok(()) => Ok(()),
            Err(crossbeam_channel::TrySendError::Full(AccountLifecycleJob::PrivateCold(
                command,
            ))) => Err(crossbeam_channel::TrySendError::Full(command)),
            Err(crossbeam_channel::TrySendError::Disconnected(
                AccountLifecycleJob::PrivateCold(command),
            )) => Err(crossbeam_channel::TrySendError::Disconnected(command)),
            Err(_) => unreachable!("private cold command preserves its queue variant"),
        }
    }

    /// Join account/audit actors after the run token reaches `Finished`.
    /// Called once per deduplicated physical account by the live engine.
    pub fn join_background_workers(&self) -> usize {
        debug_assert!(
            self.shutdown_token.is_finished(),
            "background workers must only join after producer shutdown",
        );
        let handles = std::mem::take(
            &mut *self
                .background_worker_handles
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        let worker_count = handles.len();
        for handle in handles {
            if handle.join().is_err() {
                log::error!(
                    "[PolymarketTrade] background worker panicked account={}",
                    self.account_state.account_id(),
                );
            }
        }
        worker_count
    }

    pub(crate) fn request_filled_order_cleanup(&self, client_order_id: &str) {
        if client_order_id.is_empty() {
            return;
        }
        let job = FilledCleanupJob {
            client_order_id: client_order_id.to_string(),
            enqueued_ns: now_ns(),
        };
        if let Err(error) = self
            .account_maintenance_tx
            .try_send(AccountMaintenanceJob::FilledCleanup(job))
        {
            warn!(
                "[PolymarketTrade] filled cleanup queue unavailable; reconcile will retain coid safely: {}",
                error,
            );
        }
    }

    fn install_runtime_order_id(
        &self,
        client_order_id: &str,
        exchange_order_id: &str,
        token: &str,
        ownership: Option<&OrderOwnership>,
    ) -> Result<(), String> {
        if let Some(ownership) = ownership {
            self.runtime_order_ownership
                .insert(exchange_order_id, ownership.clone())?;
        }
        let command = ExecutionStateCommand::InstallIdentity {
            client_order_id: client_order_id.to_string(),
            exchange_order_id: exchange_order_id.to_string(),
            token: token.to_string(),
        };
        if let Err(error) = self
            .account_lifecycle_tx
            .try_send(AccountLifecycleJob::ExecutionState(command))
        {
            if ownership.is_some() {
                self.runtime_order_ownership.remove(exchange_order_id);
            }
            self.user_feed_health.set_inventory_uncertain(true);
            return Err(format!("execution owner queue unavailable: {error}"));
        }
        Ok(())
    }

    /// Install the deterministic local signing hash in runtime lookup maps.
    /// The ownership recorder has already persisted this exact OID, so
    /// re-entering account rebind here would repeat lifecycle locking.
    fn register_local_order_id(
        &self,
        client_order_id: &str,
        exchange_order_id: &str,
        token: &str,
        ownership: Option<OrderOwnership>,
    ) -> Result<(), String> {
        if let Some(ownership) = ownership.as_ref() {
            self.runtime_order_ownership
                .insert(exchange_order_id, ownership.clone())?;
        }
        let command = ExecutionStateCommand::InstallIdentity {
            client_order_id: client_order_id.to_string(),
            exchange_order_id: exchange_order_id.to_string(),
            token: token.to_string(),
        };
        if let Err(error) = self
            .account_lifecycle_tx
            .try_send(AccountLifecycleJob::RegisterLocalOrder { command, ownership })
        {
            // No HTTP request has been dispatched yet. Roll back the local
            // publication and fail the submit closed instead of blocking the
            // execution actor or allowing an unpersistable live order.
            self.runtime_order_ownership.remove(exchange_order_id);
            return Err(format!("ownership persistence queue unavailable: {error}"));
        }
        Ok(())
    }

    pub(crate) fn execution_snapshot(&self) -> Arc<ExecutionStateSnapshot> {
        self.execution_state.load_full()
    }

    pub(crate) fn track_open_order(
        &self,
        client_order_id: &str,
        tracked: TrackedOrder,
    ) -> Result<(), String> {
        let command = ExecutionStateCommand::TrackOpen {
            client_order_id: client_order_id.to_string(),
            tracked,
        };
        self.account_lifecycle_tx
            .try_send(AccountLifecycleJob::ExecutionState(command))
            .map_err(|error| {
                self.user_feed_health.set_inventory_uncertain(true);
                format!("execution owner queue unavailable: {error}")
            })
    }

    fn untrack_open_order(&self, client_order_id: &str) {
        let command = ExecutionStateCommand::RemoveOpen {
            client_order_id: client_order_id.to_string(),
        };
        if let Err(error) = self
            .account_lifecycle_tx
            .try_send(AccountLifecycleJob::ExecutionState(command))
        {
            self.user_feed_health.set_inventory_uncertain(true);
            log::error!(
                "[PolymarketTrade] execution owner queue unavailable while untracking coid={}: {}",
                client_order_id,
                error,
            );
        }
    }

    #[cfg(test)]
    pub(crate) fn flush_execution_state_for_test(&self) {
        let (done_tx, done_rx) = crossbeam_channel::bounded(1);
        self.account_lifecycle_tx
            .send(AccountLifecycleJob::StartupBarrier(done_tx))
            .expect("account owner test barrier enqueue");
        done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("account owner test barrier completion");
    }

    #[cfg(test)]
    pub(crate) fn clear_execution_state_for_test(&self) {
        self.account_lifecycle_tx
            .send(AccountLifecycleJob::ExecutionState(
                ExecutionStateCommand::Clear,
            ))
            .expect("account owner test clear enqueue");
        self.flush_execution_state_for_test();
    }

    pub(crate) fn register_order_lifecycle(&self, order: &OrderRequest) {
        self.enqueue_lifecycle_trace(LifecycleTraceJob::Register {
            client_order_id: order.client_order_id.clone(),
            trace: OrderLifecycleTrace {
                instance_id: order.instance_id.clone(),
                event_id: order.quote_event_id.clone(),
                symbol: order.symbol.clone(),
                side: order.side,
                trigger_source: order.quote_trigger_source,
                trigger_exchange_ns: order.quote_trigger_exchange_timestamp_ns,
                trigger_local_ns: order.quote_trigger_local_timestamp_ns,
                quote_emit_ns: order.timestamp_ns,
                submit_prep_ns: 0,
                signed_ns: 0,
                account_reserved_ns: 0,
                http_dispatched_ns: 0,
                cancel_signal_ns: 0,
                cancel_prep_ns: 0,
                cancel_dispatched_ns: 0,
            },
        });
    }

    pub(crate) fn forget_order_lifecycle(&self, client_order_id: &str) {
        self.enqueue_lifecycle_trace(LifecycleTraceJob::Forget {
            client_order_id: client_order_id.to_string(),
        });
    }

    fn record_cancel_signal(&self, client_order_id: &str, signal_ns: u64) {
        if signal_ns == 0 {
            return;
        }
        self.enqueue_lifecycle_trace(LifecycleTraceJob::CancelSignal {
            client_order_id: client_order_id.to_string(),
            signal_ns,
        });
    }

    pub(crate) fn log_order_lifecycle(
        &self,
        client_order_id: &str,
        stage: &'static str,
        exchange_order_id: Option<&str>,
        status: Option<OrderStatus>,
        trade_id: Option<&str>,
    ) {
        self.log_order_lifecycle_detail(
            client_order_id,
            stage,
            exchange_order_id,
            status,
            trade_id,
            "",
            "",
        );
    }

    fn log_admission_attempt(
        &self,
        attempt: &crate::http1_pool::AttemptTraceHandle,
        kind: &'static str,
        order: Option<&OrderRequest>,
        route_instance_id: &str,
        client_order_id: &str,
        exchange_order_id: Option<&str>,
        status: OrderStatus,
        error: Option<&str>,
    ) {
        let Some(attempt) = attempt.snapshot() else {
            log::error!(
                "[order_attempt] admission trace slot reused before completion kind={} coid={}",
                kind,
                client_order_id,
            );
            return;
        };
        let completed_ns = now_ns();
        let elapsed_ns = completed_ns.saturating_sub(attempt.dispatched_ns);
        crate::latency::record_ns(
            if kind == "cancel" {
                "polymarket.cancel.attempt_dispatch_to_lifecycle_done"
            } else {
                "polymarket.order.attempt_dispatch_to_lifecycle_done"
            },
            elapsed_ns,
        );
        let job = AttemptAuditJob {
            attempt,
            kind,
            route_instance_id: route_instance_id.to_string(),
            order: order.map(|order| AttemptOrderReplica::from_order(order, route_instance_id)),
            client_order_id: client_order_id.to_string(),
            exchange_order_id: exchange_order_id.map(str::to_string),
            status,
            error: error.map(str::to_string),
            completed_ns,
            enqueued_ns: now_ns(),
        };
        if let Err(error) = self.attempt_audit_tx.try_send(AuditJob::Attempt(job)) {
            // This is an audit/measurement loss, not an account mutation.
            // Keep the order completion lane non-blocking and make the loss
            // operationally loud instead of delaying StrategyAccount apply.
            log::error!(
                "[order_attempt] audit queue unavailable kind={} coid={}: {}",
                kind,
                client_order_id,
                error,
            );
        }
    }

    fn enqueue_lifecycle_trace(&self, job: LifecycleTraceJob) {
        if let Err(error) = self.attempt_audit_tx.try_send(AuditJob::Lifecycle(job)) {
            // Trace state is observability-only. It is intentionally isolated
            // from the lossless account/execution lane and never backpressures
            // order completion or private trade application.
            log::error!("[order_lifecycle] audit queue unavailable: {error}");
        }
    }

    fn write_audit_job(
        &self,
        lifecycle_traces: &mut HashMap<String, OrderLifecycleTrace>,
        structured_recorder: &mut OrderAttemptRecorder,
        job: AuditJob,
    ) {
        match job {
            AuditJob::Attempt(job) => self.write_attempt_audit(structured_recorder, job),
            AuditJob::Lifecycle(job) => {
                self.write_lifecycle_trace(lifecycle_traces, structured_recorder, job)
            }
            AuditJob::PrivateDiagnostic(job) => {
                let enqueued_ns = match &job {
                    PrivateDiagnosticJob::FailedTradeWithoutReason { enqueued_ns, .. }
                    | PrivateDiagnosticJob::ColdApplyFailure { enqueued_ns, .. } => *enqueued_ns,
                };
                crate::latency::record_ns(
                    "polymarket.account.private_diagnostic_queue",
                    now_ns().saturating_sub(enqueued_ns),
                );
                match job {
                    PrivateDiagnosticJob::FailedTradeWithoutReason {
                        trade_id,
                        tx_hash,
                        maker_legs,
                        ..
                    } => log::warn!(
                        "[PolyUserFeed] FAILED venue trade {} carries no known reason field (tx_hash={}, maker_legs={}); account-scoped reversals remain enabled",
                        trade_id,
                        tx_hash,
                        maker_legs,
                    ),
                    PrivateDiagnosticJob::ColdApplyFailure { detail, .. } => log::warn!(
                        "[PolyUserFeed] cold private-account apply failed; forcing reconnect/replay: {}",
                        detail,
                    ),
                }
            }
        }
    }

    pub(crate) fn enqueue_private_diagnostic(
        &self,
        trade_id: &str,
        tx_hash: &str,
        maker_legs: usize,
    ) {
        let job = PrivateDiagnosticJob::FailedTradeWithoutReason {
            trade_id: trade_id.to_string(),
            tx_hash: tx_hash.to_string(),
            maker_legs,
            enqueued_ns: now_ns(),
        };
        if self
            .attempt_audit_tx
            .try_send(AuditJob::PrivateDiagnostic(job))
            .is_err()
        {
            crate::latency::record_ns("polymarket.private_diagnostic_overflow", 1);
        }
    }

    pub(crate) fn enqueue_private_apply_failure_diagnostic(&self, detail: &str) {
        let job = PrivateDiagnosticJob::ColdApplyFailure {
            detail: detail.to_string(),
            enqueued_ns: now_ns(),
        };
        if self
            .attempt_audit_tx
            .try_send(AuditJob::PrivateDiagnostic(job))
            .is_err()
        {
            crate::latency::record_ns("polymarket.private_diagnostic_overflow", 1);
        }
    }

    fn write_attempt_audit(
        &self,
        structured_recorder: &mut OrderAttemptRecorder,
        job: AttemptAuditJob,
    ) {
        crate::latency::record_ns(
            "polymarket.account.attempt_audit_queue",
            now_ns().saturating_sub(job.enqueued_ns),
        );
        if let Err(error) = structured_recorder.write_attempt(&job) {
            log::error!(
                "[order_attempt_recorder] write failed coid={}: {}",
                job.client_order_id,
                error,
            );
        }
        if job.status == OrderStatus::Rejected {
            warn!(
                "[PolymarketTrade] {} rejected off completion lane: {} coid={}",
                job.kind,
                job.error.as_deref().unwrap_or("unspecified rejection"),
                job.client_order_id,
            );
        }
    }

    pub(crate) fn log_preflight_rejected(
        &self,
        client_order_id: &str,
        exchange_order_id: Option<&str>,
        update: &OrderUpdate,
    ) {
        let reason = update.error.as_deref().unwrap_or("unknown");
        self.log_order_lifecycle_detail(
            client_order_id,
            "preflight_rejected",
            exchange_order_id,
            Some(update.status),
            None,
            preflight_rejection_reason_code(reason),
            reason,
        );
    }

    fn log_order_lifecycle_detail(
        &self,
        client_order_id: &str,
        stage: &'static str,
        exchange_order_id: Option<&str>,
        status: Option<OrderStatus>,
        trade_id: Option<&str>,
        reason_code: &'static str,
        reason: &str,
    ) {
        // A private fill has no quote/HTTP segment to update. Avoid the trace
        // map lock, trace clone and INFO formatting on every normal lifecycle
        // edge; failures remain WARN and DEBUG retains the full audit line.
        if stage == "private_trade"
            && status != Some(OrderStatus::Failed)
            && !log::log_enabled!(log::Level::Debug)
        {
            return;
        }
        self.enqueue_lifecycle_trace(LifecycleTraceJob::Stage {
            client_order_id: client_order_id.to_string(),
            stage,
            exchange_order_id: exchange_order_id.map(str::to_string),
            status,
            trade_id: trade_id.map(str::to_string),
            reason_code,
            reason: reason.to_string(),
            stage_ns: now_ns(),
        });
    }

    fn write_lifecycle_trace(
        &self,
        traces: &mut HashMap<String, OrderLifecycleTrace>,
        structured_recorder: &mut OrderAttemptRecorder,
        job: LifecycleTraceJob,
    ) {
        match job {
            LifecycleTraceJob::Register {
                client_order_id,
                trace,
            } => {
                traces.insert(client_order_id, trace);
            }
            LifecycleTraceJob::Forget { client_order_id } => {
                traces.remove(&client_order_id);
            }
            LifecycleTraceJob::ForgetMany { client_order_ids } => {
                traces.retain(|coid, _| !client_order_ids.contains(coid));
            }
            LifecycleTraceJob::CancelSignal {
                client_order_id,
                signal_ns,
            } => {
                if let Some(trace) = traces.get_mut(&client_order_id) {
                    trace.cancel_signal_ns = signal_ns;
                }
            }
            LifecycleTraceJob::Stage {
                client_order_id,
                stage,
                exchange_order_id,
                status,
                trade_id,
                reason_code,
                reason,
                stage_ns,
            } => self.write_order_lifecycle_detail(
                traces,
                structured_recorder,
                &client_order_id,
                stage,
                exchange_order_id.as_deref(),
                status,
                trade_id.as_deref(),
                reason_code,
                &reason,
                stage_ns,
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn write_order_lifecycle_detail(
        &self,
        traces: &mut HashMap<String, OrderLifecycleTrace>,
        structured_recorder: &mut OrderAttemptRecorder,
        client_order_id: &str,
        stage: &str,
        exchange_order_id: Option<&str>,
        status: Option<OrderStatus>,
        trade_id: Option<&str>,
        reason_code: &str,
        reason: &str,
        stage_ns: u64,
    ) {
        let mut segment_ns = 0u64;
        let mut segment_name = "";
        if let Some(trace) = traces.get_mut(client_order_id) {
            let origin = match stage {
                "submit_prep" => {
                    segment_name = "quote_to_submit_prep";
                    trace.quote_emit_ns
                }
                "signed" => {
                    segment_name = "submit_prep_to_signed";
                    trace.submit_prep_ns
                }
                "account_recorded" => {
                    segment_name = "signed_to_account_recorded";
                    trace.signed_ns
                }
                "http_dispatched" => {
                    segment_name = "account_recorded_to_http_dispatch";
                    trace.account_reserved_ns
                }
                "http_response" => {
                    segment_name = "http_dispatch_to_response";
                    trace.http_dispatched_ns
                }
                "cancel_prep" => {
                    segment_name = "cancel_signal_to_prep";
                    trace.cancel_signal_ns
                }
                "cancel_dispatched" => {
                    segment_name = "cancel_prep_to_http_dispatch";
                    trace.cancel_prep_ns
                }
                "cancel_response" => {
                    segment_name = "cancel_http_dispatch_to_response";
                    trace.cancel_dispatched_ns
                }
                _ => 0,
            };
            if origin > 0 {
                segment_ns = stage_ns.saturating_sub(origin);
            }
            match stage {
                "submit_prep" => trace.submit_prep_ns = stage_ns,
                "signed" => trace.signed_ns = stage_ns,
                "account_recorded" => trace.account_reserved_ns = stage_ns,
                "http_dispatched" => trace.http_dispatched_ns = stage_ns,
                "cancel_prep" => trace.cancel_prep_ns = stage_ns,
                "cancel_dispatched" => trace.cancel_dispatched_ns = stage_ns,
                _ => {}
            }
        }
        match segment_name {
            "quote_to_submit_prep" => {
                crate::latency::record_ns("polymarket.order.quote_to_prep", segment_ns)
            }
            "submit_prep_to_signed" => {
                crate::latency::record_ns("polymarket.order.prep_to_signed", segment_ns)
            }
            "signed_to_account_recorded" => {
                crate::latency::record_ns("polymarket.order.signed_to_reserve", segment_ns)
            }
            "account_recorded_to_http_dispatch" => {
                crate::latency::record_ns("polymarket.order.reserve_to_http_dispatch", segment_ns)
            }
            "http_dispatch_to_response" => {
                // This ends after response classification/account mutation;
                // it is not a network RTT. The true wire/body segment is
                // `polymarket.http.place.network`.
                crate::latency::record_ns("polymarket.order.dispatch_to_lifecycle_done", segment_ns)
            }
            "cancel_prep_to_http_dispatch" => {
                crate::latency::record_ns("polymarket.cancel.prep_to_http_dispatch", segment_ns)
            }
            "cancel_http_dispatch_to_response" => {
                // See `polymarket.http.cancel.network` for the actual HTTP
                // send-through-body/JSON interval.
                crate::latency::record_ns(
                    "polymarket.cancel.dispatch_to_lifecycle_done",
                    segment_ns,
                )
            }
            _ => {}
        }
        let slow_threshold_ns = if matches!(stage, "http_response" | "cancel_response") {
            250_000_000
        } else {
            5_000_000
        };
        // The cluster edge itself emits one operational WARN. Every quote
        // rejected during its short safety window remains in the structured
        // lifecycle recorder, but must not flood the console one order at a
        // time while the gate is doing exactly what it was designed to do.
        let aggregated_connection_failure_cluster_reject = stage == "preflight_rejected"
            && reason == "connection failure cluster safety gate";
        let warning = (!reason_code.is_empty() && !aggregated_connection_failure_cluster_reject)
            || status == Some(OrderStatus::Failed);
        let slow = !warning && segment_ns >= slow_threshold_ns && segment_ns > 0;
        let trace = traces.get(client_order_id).cloned();
        let (
            iid,
            event,
            symbol,
            side,
            source,
            trigger_exchange_ns,
            trigger_local_ns,
            quote_emit_ns,
        ) = match trace {
            Some(trace) => (
                trace.instance_id,
                trace.event_id,
                trace.symbol,
                trace.side.to_string(),
                trace.trigger_source,
                trace.trigger_exchange_ns,
                trace.trigger_local_ns,
                trace.quote_emit_ns,
            ),
            None => (
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                QuoteTriggerSource::Unknown,
                0,
                0,
                0,
            ),
        };
        let status_name = status.map(|value| format!("{value:?}")).unwrap_or_default();
        let trigger_exchange_to_stage_ms = lifecycle_delta_ms(stage_ns, trigger_exchange_ns);
        let trigger_local_to_stage_ms = lifecycle_delta_ms(stage_ns, trigger_local_ns);
        let quote_to_stage_ms = lifecycle_delta_ms(stage_ns, quote_emit_ns);
        let record = serde_json::json!({
            "kind": "order_lifecycle",
            "stage": stage,
            "coid": client_order_id,
            "oid": exchange_order_id,
            "trade_id": trade_id,
            "status": status_name,
            "iid": iid,
            "event": event,
            "symbol": symbol,
            "side": side,
            "trigger_source": source.to_string(),
            "trigger_exchange_ns": trigger_exchange_ns,
            "trigger_local_ns": trigger_local_ns,
            "quote_emit_ns": quote_emit_ns,
            "stage_ns": stage_ns,
            "segment": segment_name,
            "segment_ns": segment_ns,
            "trigger_exchange_to_stage_ms": trigger_exchange_to_stage_ms,
            "trigger_local_to_stage_ms": trigger_local_to_stage_ms,
            "quote_to_stage_ms": quote_to_stage_ms,
            "reason_code": reason_code,
            "reason": reason,
        });
        if let Err(error) = structured_recorder.write_record(&record) {
            log::error!(
                "[order_lifecycle_recorder] write failed coid={}: {}",
                client_order_id,
                error,
            );
        }
        if !warning && !slow && !log::log_enabled!(log::Level::Debug) {
            return;
        }
        let message = format!(
            "[order_lifecycle] stage={} coid={} oid={} trade_id={} status={} iid={} event={} symbol={} side={} trigger_source={} trigger_exchange_ns={} trigger_local_ns={} quote_emit_ns={} stage_ns={} segment={} segment_ms={:.3} trigger_exchange_to_stage_ms={:.3} trigger_local_to_stage_ms={:.3} quote_to_stage_ms={:.3} reason_code={} reason={}",
            stage,
            client_order_id,
            exchange_order_id.unwrap_or(""),
            trade_id.unwrap_or(""),
            status_name,
            iid,
            event,
            symbol,
            side,
            source,
            trigger_exchange_ns,
            trigger_local_ns,
            quote_emit_ns,
            stage_ns,
            segment_name,
            segment_ns as f64 / 1_000_000.0,
            trigger_exchange_to_stage_ms,
            trigger_local_to_stage_ms,
            quote_to_stage_ms,
            reason_code,
            serde_json::to_string(reason).unwrap_or_else(|_| "\"unserializable\"".to_string()),
        );
        if warning {
            warn!("{}", message);
        } else if slow {
            warn!(
                "[order_lifecycle_slow] threshold_ms={:.3} {}",
                slow_threshold_ns as f64 / 1_000_000.0,
                message
            );
        } else {
            debug!("{}", message);
        }
    }

    /// Register a bidirectional order ID mapping plus the order's outcome
    /// token. The token lets settled-FIFO eviction retire the mapping after
    /// the full late-revision window (the only place coid↔oid mappings are
    /// reclaimed now that lifecycle rejects/cancels keep them).
    pub fn register_order_id(&self, client_order_id: &str, exchange_order_id: &str, token: &str) {
        // New-order signing uses `register_local_order_id`; this authoritative
        // path is reached only for a server mismatch or recovered mapping.
        // Avoid re-entering the account lifecycle when reconciliation merely
        // repeats an already-installed binding.
        let already_bound = self
            .execution_snapshot()
            .coid_to_oid
            .get(client_order_id)
            .is_some_and(|current| {
                normalize_order_id(current) == normalize_order_id(exchange_order_id)
            });
        if already_bound {
            return;
        }
        // Server identity repair is ordered with every other lifecycle
        // mutation on the account owner. Completion/private workers never
        // synchronously enter SharedAccount route or persistence locks.
        let job = AccountLifecycleJob::RebindServerIdentity {
            client_order_id: client_order_id.to_string(),
            exchange_order_id: exchange_order_id.to_string(),
            token: token.to_string(),
        };
        if let Err(error) = self.account_lifecycle_tx.try_send(job) {
            self.user_feed_health.set_inventory_uncertain(true);
            log::error!(
                "[PolymarketTrade] server identity owner queue unavailable coid={} oid={}: {}",
                client_order_id,
                exchange_order_id,
                error,
            );
        }
    }

    /// Look up client_order_id from Polymarket orderID.
    pub fn lookup_coid(&self, exchange_order_id: &str) -> Option<String> {
        if let Some(client_order_id) = self
            .runtime_order_ownership
            .client_order_id(exchange_order_id)
        {
            return Some(client_order_id);
        }
        self.execution_snapshot()
            .oid_to_coid
            .get(&normalize_order_id(exchange_order_id))
            .cloned()
    }

    pub(crate) fn lookup_order_ownership(&self, exchange_order_id: &str) -> Option<OrderOwnership> {
        // Runtime routing is published before asynchronous durable ownership.
        // Private messages therefore never wait for the shared ledger writer.
        if let Some(mirrored) = self.runtime_order_ownership.get(exchange_order_id) {
            return Some(mirrored);
        }
        // Restart/cold-recovery fallback only. Settled-event GC may have
        // retired the compact runtime route before a delayed private replay;
        // recover the retained durable terminal ownership by OID in that rare
        // case and republish the immutable owner index.
        let fallback_started = crate::latency::Instant::now();
        let ownership = if let Some(coid) = self.lookup_coid(exchange_order_id) {
            self.account_state
                .reconcile_order_route(&coid, exchange_order_id)
        } else {
            self.account_state.order_by_oid(exchange_order_id)
        };
        crate::latency::record(
            "polymarket.user.order_ownership_durable_fallback",
            fallback_started,
        );
        let ownership = ownership?;
        if let Err(error) = self.install_runtime_order_id(
            &ownership.client_order_id,
            exchange_order_id,
            &ownership.token_id,
            Some(&ownership),
        ) {
            log::error!(
                "[PolymarketTrade] durable ownership republish failed oid={}: {}",
                exchange_order_id,
                error,
            );
        }
        Some(ownership)
    }

    /// Lock-free ownership probe for ambiguity checks on the private owner
    /// lane. A maker trade's top-level taker order normally belongs to another
    /// account; consulting the durable ledger for that negative lookup would
    /// scan every StrategyAccount lifecycle and dominate P99.
    pub(crate) fn has_runtime_order_ownership(&self, exchange_order_id: &str) -> bool {
        self.runtime_order_ownership.contains(exchange_order_id)
    }

    /// Complete account-global settled-audit cleanup only after the durable
    /// shared reference ledger proves every instance has evicted the event and
    /// every associated order/trade is terminal.
    fn finalize_ready_settled_audit_retirements(
        &self,
        execution: &mut ExecutionStateOwner,
        live_position: &mut LivePositionManager,
    ) -> (usize, usize) {
        let ready = self
            .account_state
            .finalize_ready_settled_audit_retirements();
        if ready.is_empty() {
            return (0, 0);
        }
        let retired_events = ready.len();
        let mut retired_runtime_mappings = 0usize;
        for tokens in &ready {
            let owned_coids: HashSet<String> = execution
                .coid_to_token
                .iter()
                .filter(|(_, token)| tokens.contains(*token))
                .map(|(coid, _)| coid.clone())
                .collect();
            if owned_coids.is_empty() {
                continue;
            }
            let token_list: Vec<String> = tokens.iter().cloned().collect();
            retired_runtime_mappings =
                retired_runtime_mappings.saturating_add(reclaim_token_mappings(
                    &mut execution.coid_to_oid,
                    &mut execution.oid_to_coid,
                    &mut execution.coid_to_token,
                    &token_list,
                    Some(&owned_coids),
                ));
            self.enqueue_lifecycle_trace(LifecycleTraceJob::ForgetMany {
                client_order_ids: owned_coids.clone(),
            });
            self.runtime_order_ownership
                .remove_client_orders(&owned_coids);
        }
        execution.publish(self, false, true);
        let retired_live_trades = ready
            .iter()
            .map(|tokens| live_position.prune_terminal_history(tokens))
            .sum();
        debug!(
            "[PolySettledGc] account={} owner certificates complete runtime_mappings={}",
            self.account_state.account_id(),
            retired_runtime_mappings,
        );
        (retired_events, retired_live_trades)
    }

    /// Apply a live order status and restore every runtime structure that a
    /// preceding terminal update may have torn down. Polymarket lifecycle
    /// messages are not ordered, so Cancelled → Accepted is a valid
    /// resurrection and must re-lock collateral and re-enter `open_orders`
    /// as one cross-layer operation.
    pub(crate) fn mark_order_live(
        &self,
        client_order_id: &str,
        order_slot: OrderSlot,
        symbol: &str,
        side: Side,
        instance_id: &str,
        status: OrderStatus,
    ) -> Option<OrderStatus> {
        debug_assert!(matches!(
            status,
            OrderStatus::Accepted | OrderStatus::PartiallyFilled
        ));
        let job = AccountLifecycleJob::MarkLive {
            client_order_id: client_order_id.to_string(),
            tracked: TrackedOrder {
                order_slot,
                symbol: symbol.to_string(),
                side,
                instance_id: instance_id.to_string(),
            },
            status,
            enqueued_ns: now_ns(),
        };
        if let Err(error) = self.account_lifecycle_tx.try_send(job) {
            self.user_feed_health.set_inventory_uncertain(true);
            log::error!(
                "[PolymarketTrade] account owner queue unavailable while restoring live coid={}: {}",
                client_order_id,
                error,
            );
        }
        // StrategyAccount is the immediate lifecycle authority. The shared
        // monitoring ledger applies the same edge asynchronously on its sole
        // owner and may retain a stronger terminal state there.
        Some(status)
    }

    /// Apply the weak outcome of one cancel attempt without allowing a late
    /// HTTP/reconcile reply to overwrite terminal private-feed evidence. An
    /// explicit LIVE lookup is handled by `mark_order_live`; `Accepted` here
    /// only means this cancel attempt failed and therefore must not resurrect
    /// an order that was concurrently finalized.
    fn effective_cancel_attempt_status(
        &self,
        client_order_id: &str,
        candidate: OrderStatus,
    ) -> OrderStatus {
        if matches!(
            candidate,
            OrderStatus::CancelOrderTimeout | OrderStatus::CancelUncertain
        ) {
            self.defer_lifecycle_account_apply(client_order_id, candidate);
        }
        candidate
    }

    /// Drop the order from the **active-order** tracker.
    ///
    /// Deliberately KEEPS the `coid_to_oid` / `oid_to_coid` / `coid_to_token`
    /// maps intact — a delayed fill push for a just-cancelled OR just-rejected
    /// order (cancel-then-fill / racy "post-only crosses book" reject-then-fill)
    /// can still resolve its coid via `oid_to_coid`, so the fill is attributed
    /// to the placing instance instead of arriving `<unmapped>` (empty coid)
    /// and broadcasting to every instance's PositionManager. The maps are
    /// reclaimed per-event by `retire_event_audit` after settled-FIFO eviction
    /// (keyed by `coid_to_token`), and fully wiped by `cancel_all_orders` at
    /// shutdown / account-wide cancel.
    ///
    /// This is now the SINGLE local-tracking teardown used by both cancels and
    /// rejects (the old `unregister_order_id`, which eagerly dropped the maps
    /// on reject, is gone — its "an explicit reject means the order never
    /// existed" assumption is false for crosses-book rejects, which can still
    /// match). Removing the `open_orders` entry already keeps
    /// `handle_balance_error`'s (open_orders-based) snapshot from
    /// double-cancelling a just-rejected coid.
    pub fn remove_order(&self, client_order_id: &str) {
        self.remove_order_as(client_order_id, OrderStatus::Cancelled);
    }

    fn remove_cancelled_order_runtime(&self, client_order_id: &str) {
        self.untrack_open_order(client_order_id);
        self.pending_delayed_orphans.remove(client_order_id);
        self.reconcile_cancel_not_found_counts
            .remove(client_order_id);
        self.cancel_reconcile_next_retry_ns.remove(client_order_id);
    }

    pub fn remove_order_as(&self, client_order_id: &str, status: OrderStatus) {
        // `MATCHED`/`FILLED` from an order lookup or DELETE response proves the
        // order is terminal, but it does not prove that every associated trade
        // has reached the account ledger. Preserve the reservation and keep the
        // order reconcilable until private-feed/gap-replay trade audit consumes
        // it. Cancelled/rejected outcomes can release immediately.
        if !self.account_state.is_account_lifecycle_thread()
            && !self.account_state.is_account_owner_thread()
        {
            if status == OrderStatus::Cancelled {
                self.remove_cancelled_order_runtime(client_order_id);
            }
            self.defer_lifecycle_account_apply(client_order_id, status);
            return;
        }
        if status == OrderStatus::Filled {
            let transition = self
                .account_state
                .mark_filled_pending_audit(client_order_id);
            if transition.pending() {
                if transition.newly_pending() {
                    warn!(
                "[PolymarketTrade] Filled coid={} awaits complete trade audit; preserving reservation",
                client_order_id,
            );
                } else {
                    debug!(
                        "[PolymarketTrade] duplicate Filled coid={} remains pending trade audit",
                        client_order_id,
                    );
                }
                return;
            }
        }
        if status == OrderStatus::Cancelled {
            self.remove_cancelled_order_runtime(client_order_id);
            if self
                .account_state
                .mark_cancelled_pending_audit(client_order_id)
            {
                debug!(
                    "[PolymarketTrade] Cancelled coid={} queued routine size_matched audit; preserving reservation without blocking account",
                    client_order_id,
                );
            }
            return;
        }
        self.remove_order_resolved_as(client_order_id, status);
    }

    /// Final teardown after the exchange audit has proved that every
    /// associated trade was processed. Unlike the first Filled edge, this may
    /// release a residual reservation: FAK/partially matched orders can be
    /// terminal with `size_matched < original quantity`.
    fn remove_order_resolved_as(&self, client_order_id: &str, status: OrderStatus) {
        self.remove_order_resolved_as_inner(client_order_id, status, false);
    }

    /// Runtime teardown after `apply_authoritative_order_audit` has already
    /// committed status, exact trade IDs, reservation release and recovery-set
    /// cleanup in one account transaction. Re-entering `release_order` and
    /// `finish_order_recovery` here used to take the account control/lifecycle
    /// locks twice more for every terminal private order push.
    fn remove_order_after_authoritative_commit(&self, client_order_id: &str, status: OrderStatus) {
        self.remove_order_resolved_as_inner(client_order_id, status, true);
    }

    fn remove_order_resolved_as_inner(
        &self,
        client_order_id: &str,
        status: OrderStatus,
        account_already_committed: bool,
    ) {
        let terminal_token = if account_already_committed {
            self.execution_snapshot()
                .coid_to_token
                .get(client_order_id)
                .cloned()
        } else {
            self.account_state
                .order(client_order_id)
                .map(|order| order.token_id)
        };
        self.untrack_open_order(client_order_id);
        if !account_already_committed {
            self.account_state.release_order(client_order_id, status);
            self.account_state.finish_order_recovery(client_order_id);
        }
        // Conclusive resolution — drop any pending/delayed orphan flag so the
        // set never leaks and a future coid reuse starts fresh.
        self.pending_delayed_orphans.remove(client_order_id);
        self.reconcile_cancel_not_found_counts
            .remove(client_order_id);
        self.cancel_reconcile_next_retry_ns.remove(client_order_id);
        let now = crate::types::now_ns();
        if self
            .http_425_reconcile_backoff_until_ns
            .remove(client_order_id)
            .is_some()
        {
            let backoffs = self.http_425_reconcile_backoff_until_ns.snapshot();
            let active_count = backoffs
                .values()
                .filter(|deadline| **deadline > now)
                .count();
            info!(
                "[orphan_metric] http_425_circuit_active={} http_425_active_coids={} coid_425_active=0 coid={} reason=terminal",
                u8::from(active_count > 0), active_count, client_order_id,
            );
        }
        // Never scan account-wide history on this HTTP/reconcile completion,
        // and do not wake an old event's blocked GC for an unrelated active
        // market cancel.
        if let Some(token_id) = terminal_token {
            self.request_settled_gc_for_token(&token_id);
        }
    }

    pub(crate) fn complete_filled_order_audit(&self, client_order_id: &str) {
        self.remove_order_resolved_as(client_order_id, OrderStatus::Filled);
    }

    /// Atomically transfer an already-fetched complete order audit into the
    /// shared ledger. Pending recovery retains the exact trade-ID set and can
    /// fetch those rows directly; a second order GET is unnecessary.
    pub(crate) fn commit_authoritative_terminal_audit(
        &self,
        client_order_id: &str,
        status: OrderStatus,
        audit: &AuthoritativeOrderAudit,
    ) -> bool {
        let audit_started = crate::latency::Instant::now();
        let transition_result =
            self.account_state
                .apply_authoritative_order_audit(client_order_id, status, audit);
        crate::latency::record("polymarket.account.terminal_audit_commit", audit_started);
        let transition = match transition_result {
            Ok(transition) => transition,
            Err(error) => {
                warn!(
                    "[PolymarketTrade] authoritative order audit rejected coid={} status={:?}: {}",
                    client_order_id, status, error,
                );
                self.account_state
                    .mark_order_status(client_order_id, status);
                self.account_state.begin_order_recovery([client_order_id]);
                return true;
            }
        };
        if transition.pending() {
            self.untrack_open_order(client_order_id);
            self.pending_delayed_orphans.remove(client_order_id);
            self.reconcile_cancel_not_found_counts
                .remove(client_order_id);
            self.cancel_reconcile_next_retry_ns.remove(client_order_id);
            debug!(
                "[PolymarketTrade] authoritative terminal audit committed coid={} status={:?} trade_ids={} pending=true",
                client_order_id,
                status,
                audit.associate_trades.len(),
            );
            true
        } else {
            let teardown_started = crate::latency::Instant::now();
            self.remove_order_after_authoritative_commit(client_order_id, status);
            crate::latency::record(
                "polymarket.account.terminal_runtime_teardown",
                teardown_started,
            );
            false
        }
    }

    /// Remove executor-side tracking after the account ledger has consumed all
    /// reserved quantity/cash for a terminal fill.
    pub(crate) fn finish_filled_order_if_audited(&self, client_order_id: &str) {
        // MATCHED -> MINED -> CONFIRMED can enqueue the same coid three times.
        // Once the first job has retired executor tracking, later lifecycle
        // edges are strict background no-ops instead of repeated WAL writes.
        if !self
            .execution_snapshot()
            .open_orders
            .contains_key(client_order_id)
        {
            return;
        }
        let audited = self
            .account_state
            .filled_order_ready_for_retirement(client_order_id);
        if audited {
            self.complete_filled_order_audit(client_order_id);
        }
    }

    fn check_rate_limit(&self) -> bool {
        self.rate_limiter.try_acquire()
    }

    /// Duration of the suppression window triggered by a `not enough
    /// balance / allowance` rejection.
    ///
    /// Previously 200 ms. The 2026-05-06 21:32–21:34 burst (455 balance
    /// errors in 3 min during a Polymarket allowance-sync stall)
    /// demonstrated that 200 ms is too short: orders re-emit at
    /// 250–380 ms intervals (just past the backoff), each hits the
    /// still-depleted server-side per-token allowance, and the loop
    /// repeats. Raised to 1 s — covers a few `quote_interval_ms`=100 ms
    /// ticks plus typical server-side allowance-refresh latency, while
    /// still being short enough that the strategy resumes quickly once
    /// the actual balance issue clears.
    ///
    /// (A future refinement would be a per-token map so unrelated
    /// markets keep quoting; tracked separately.)
    pub(crate) const BALANCE_BACKOFF_NS: u64 = 1_000_000_000;

    /// True if `instance_id` is still within its last balance-error backoff
    /// window. Per-instance: a sibling's backoff never gates this caller.
    #[inline]
    pub(crate) fn in_balance_backoff(&self, instance_id: &str) -> bool {
        let map = self.balance_backoff_until_ns.load();
        match map.get(instance_id) {
            Some(&until) => crate::types::now_ns() < until,
            None => false,
        }
    }

    /// Record a balance-error rejection for `instance_id` and enter (or
    /// extend) its backoff window. Returns `true` iff this transitions that
    /// instance **into** backoff (i.e. it was not already in it). Callers
    /// use that signal to fire exactly one targeted-cancel batch on the
    /// edge, not on every subsequent reject during the same window.
    pub(crate) fn record_balance_error(&self, instance_id: &str) -> bool {
        let now = crate::types::now_ns();
        let transitioned = std::sync::atomic::AtomicBool::new(false);
        self.balance_backoff_until_ns.rcu(|current| {
            let mut next = (**current).clone();
            let previous = next.insert(
                instance_id.to_string(),
                now.saturating_add(Self::BALANCE_BACKOFF_NS),
            );
            transitioned.store(
                previous.is_none_or(|deadline| deadline < now),
                std::sync::atomic::Ordering::Relaxed,
            );
            next
        });
        transitioned.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Policy rejections normally persist for more than a quote tick. Keep
    /// the SDK quiet long enough for strategies to observe the rejection and
    /// arm their own market-level gate, without turning this into a permanent
    /// latch if trading is re-enabled.
    pub(crate) const TRADING_DISABLED_BACKOFF_NS: u64 = 30_000_000_000;

    pub(crate) fn is_trading_disabled_error(text: &str) -> bool {
        text.to_ascii_lowercase().contains("trading is disabled")
    }

    #[inline]
    pub(crate) fn in_trading_disabled_backoff(&self) -> bool {
        crate::types::now_ns()
            < self
                .trading_disabled_backoff_until_ns
                .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns true only on the edge into a new backoff window so callers can
    /// keep the operator warning to one line per outage window.
    pub(crate) fn record_trading_disabled(&self) -> bool {
        let now = crate::types::now_ns();
        let until = now.saturating_add(Self::TRADING_DISABLED_BACKOFF_NS);
        let previous = self
            .trading_disabled_backoff_until_ns
            .swap(until, std::sync::atomic::Ordering::Relaxed);
        previous <= now
    }

    /// Detect a `not enough balance / allowance` error in either HTTP 400
    /// body text (`HttpErr::Status`) or the per-order `errorMsg` field of
    /// a 200 response. Case-insensitive substring match — keeps working
    /// if the server tweaks wording.
    pub(crate) fn is_balance_error(text: &str) -> bool {
        let l = text.to_ascii_lowercase();
        l.contains("not enough balance") || l.contains("allowance")
    }

    /// Consecutive `invalid token id` rejects for a token before its submits
    /// are blocked, and the per-token block window (re-probed afterwards).
    pub(crate) const INVALID_TOKEN_STRIKES: u32 = 3;
    pub(crate) const INVALID_TOKEN_BACKOFF_NS: u64 = 30_000_000_000; // 30 s

    /// Detect the CLOB `invalid token id` rejection (token not registered on
    /// the orderbook). Case-insensitive substring — robust to wording tweaks.
    pub(crate) fn is_invalid_token_error(text: &str) -> bool {
        text.to_ascii_lowercase().contains("invalid token id")
    }

    /// True iff `token` is in an active invalid-token backoff window; submits
    /// for it should be pre-rejected. Hot path — single map lookup.
    pub(crate) fn in_invalid_token_backoff(&self, token: &str) -> bool {
        let now = crate::types::now_ns();
        self.invalid_token_backoff
            .load()
            .get(token)
            .is_some_and(|(_, until)| *until > now)
    }

    /// Record an `invalid token id` reject for `token`: bump its strike count
    /// and, once it reaches `INVALID_TOKEN_STRIKES`, (re)arm the block window.
    /// Returns `true` on the edge (window armed from a non-active state) so the
    /// caller logs exactly once per window.
    pub(crate) fn record_invalid_token(&self, token: &str) -> bool {
        let now = crate::types::now_ns();
        let armed = std::sync::atomic::AtomicBool::new(false);
        self.invalid_token_backoff.rcu(|current| {
            let mut next = (**current).clone();
            let entry = next.entry(token.to_string()).or_insert((0, 0));
            entry.0 = entry.0.saturating_add(1);
            if entry.0 >= Self::INVALID_TOKEN_STRIKES {
                let was_active = entry.1 > now;
                entry.1 = now.saturating_add(Self::INVALID_TOKEN_BACKOFF_NS);
                armed.store(!was_active, std::sync::atomic::Ordering::Relaxed);
            } else {
                armed.store(false, std::sync::atomic::Ordering::Relaxed);
            }
            next
        });
        armed.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Clear a token's invalid-token state — an order for it was accepted, so
    /// it's registered and tradeable again.
    pub(crate) fn clear_invalid_token(&self, token: &str) {
        let now = crate::types::now_ns();
        self.invalid_token_backoff.rcu(|current| {
            if !current.contains_key(token) {
                return Arc::clone(current);
            }
            let mut next = (**current).clone();
            next.remove(token);
            if next.len() > 256 {
                // Opportunistic prune: drop long-expired entries so a
                // long-lived process stays bounded by active failures.
                next.retain(|_, (_, until)| *until > now);
            }
            Arc::new(next)
        });
    }

    /// Should the calling site emit its WARN for an `unknown_state` HTTP
    /// error, or suppress it under the 5-min 425-storm dedup window?
    ///
    /// * Non-425 unknown_state (timeouts, 5xx) → always WARN. These are
    ///   genuine per-request anomalies the operator wants to see.
    /// * 425 (transient backpressure) → WARN at most once per 5 min.
    ///   Polymarket emits 425 in storms (15,045× in 30 min observed
    ///   2026-05-06 21:00–21:35) when the service is overloaded; the
    ///   storm hits cancel and place endpoints together, so one shared
    ///   silent window covers both.
    ///
    /// Returns `true` if the caller should emit its WARN; `false` if the
    /// 425 was suppressed under the dedup window.
    pub(crate) fn should_warn_unknown_state(&self, e: &HttpErr) -> bool {
        if !matches!(e, HttpErr::Status(425, _)) {
            return true;
        }
        let now = crate::types::now_ns();
        let until = self
            .http_425_warn_silent_until_ns
            .load(std::sync::atomic::Ordering::Relaxed);
        if now >= until {
            self.http_425_warn_silent_until_ns.store(
                now.saturating_add(300_000_000_000), // 5 min
                std::sync::atomic::Ordering::Relaxed,
            );
            true
        } else {
            false
        }
    }

    /// Record an HTTP 425 for one orphan. The deadline is scoped to `coid`,
    /// preventing a single throttled order from stopping every orphan audit
    /// in the shared account. Idempotent — only advances that coid's deadline.
    pub(crate) fn note_http_425_backoff(&self, coid: &str) {
        let now = crate::types::now_ns();
        let was_active = self
            .http_425_reconcile_backoff_until_ns
            .get(coid)
            .is_some_and(|deadline| deadline > now);
        if !coid.is_empty() {
            self.http_425_reconcile_backoff_until_ns
                .update(coid, |deadline| {
                    Some(
                        deadline
                            .copied()
                            .unwrap_or(0)
                            .max(now.saturating_add(HTTP_425_BACKOFF_NS)),
                    )
                });
        }
        let backoffs = self.http_425_reconcile_backoff_until_ns.snapshot();
        let active_count = backoffs
            .values()
            .filter(|deadline| **deadline > now)
            .count();
        let total = if was_active {
            self.http_425_circuit_entries_total
                .load(std::sync::atomic::Ordering::Relaxed)
        } else {
            self.http_425_circuit_entries_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                .saturating_add(1)
        };
        if !was_active {
            warn!(
                "[orphan_metric] http_425_circuit_active=1 http_425_active_coids={} http_425_entries_total={} coid={}",
                active_count, total, coid,
            );
        }
    }

    /// True iff this coid's 425 backoff is active. Expired entries are
    /// removed lazily; other coids never affect this decision.
    pub(crate) fn in_http_425_backoff(&self, coid: &str) -> bool {
        let now = crate::types::now_ns();
        let deadline = self.http_425_reconcile_backoff_until_ns.get(coid);
        let existed = deadline.is_some();
        let active = deadline.is_some_and(|until| now < until);
        if existed && !active {
            self.http_425_reconcile_backoff_until_ns.remove(coid);
            let backoffs = self.http_425_reconcile_backoff_until_ns.snapshot();
            let active_count = backoffs
                .values()
                .filter(|deadline| **deadline > now)
                .count();
            info!(
                "[orphan_metric] http_425_circuit_active={} http_425_active_coids={} coid_425_active=0 coid={}",
                u8::from(active_count > 0), active_count, coid,
            );
        }
        active
    }

    fn note_get_live_delete_uncertain(&self, coid: &str, order_id: &str) {
        let total = self
            .get_live_delete_uncertain_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .saturating_add(1);
        let now = crate::types::now_ns();
        if claim_rate_limited_warning(&self.get_live_delete_uncertain_warn_silent_until_ns, now) {
            let suppressed = self
                .get_live_delete_uncertain_suppressed
                .swap(0, std::sync::atomic::Ordering::Relaxed);
            warn!(
                "[orphan_metric] GET_LIVE_DELETE_UNCERTAIN=1 GET_LIVE_DELETE_UNCERTAIN_total={} occurrences_since_last={} coid={} orderID={} lock_release=forbidden warn_interval_s={}",
                total, suppressed.saturating_add(1), coid, order_id,
                EXPECTED_ORPHAN_WARN_INTERVAL_NS / 1_000_000_000,
            );
        } else {
            self.get_live_delete_uncertain_suppressed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn note_terminal_trade_backfill_noop(
        &self,
        trade_id: &str,
        records: usize,
        validated_no_update: usize,
    ) {
        let total = self
            .terminal_trade_backfill_noop_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .saturating_add(1);
        let now = crate::types::now_ns();
        if claim_rate_limited_warning(&self.terminal_trade_backfill_noop_log_silent_until_ns, now) {
            let suppressed = self
                .terminal_trade_backfill_noop_suppressed
                .swap(0, std::sync::atomic::Ordering::Relaxed);
            info!(
                "[orphan_metric] terminal_trade_backfill_validated_noop_total={} occurrences_since_last={} last_trade_id={} last_records={} last_validated_no_update={} reason=already_applied_or_nonadvancing log_interval_s={}",
                total,
                suppressed.saturating_add(1),
                trade_id,
                records,
                validated_no_update,
                EXPECTED_ORPHAN_WARN_INTERVAL_NS / 1_000_000_000,
            );
        } else {
            self.terminal_trade_backfill_noop_suppressed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Apply the bounded terminal policy for Polymarket's exact ambiguous
    /// DELETE not-found reply from the cancel-orphan reconciler. This method
    /// must not be called by initial, ordinary single, or batch cancel paths:
    /// only three DELETEs issued after reconcile GET returned LIVE may resolve
    /// the orphan to Cancelled.
    fn apply_reconcile_cancel_not_found_terminal(
        &self,
        coid: &str,
        reason: Option<&str>,
        outcome: CancelReasonOutcome,
    ) -> CancelReasonOutcome {
        let attempt = if outcome == CancelReasonOutcome::Uncertain
            && !coid.is_empty()
            && reason.is_some_and(is_cancel_not_found_already_canceled_or_matched)
        {
            self.reconcile_cancel_not_found_counts
                .update(coid, |count| {
                    Some(count.copied().unwrap_or(0).saturating_add(1))
                });
            self.reconcile_cancel_not_found_counts.get(coid)
        } else {
            None
        };
        let Some(attempt) = attempt else {
            return outcome;
        };
        let bounded_outcome = cancel_not_found_outcome_after_observation(outcome, Some(attempt));
        if bounded_outcome == CancelReasonOutcome::Cancelled {
            warn!(
                "[PolymarketTrade] Cancel orphan terminal coid={} reason=\"order can't be found - already canceled or matched\" reconcile_delete_observations={} → Cancelled",
                coid, attempt,
            );
        } else {
            info!(
                "[PolymarketTrade] Cancel orphan ambiguous not-found coid={} reconcile_delete_observation={}/{} → keeping orphan",
                coid, attempt, CANCEL_NOT_FOUND_TERMINAL_LIMIT,
            );
        }
        bounded_outcome
    }

    /// Dispatch an HTTP request onto the shared async runtime. Returns a
    /// crossbeam Receiver so the caller can poll for the response from a
    /// sync context. Kicks off the reqwest call immediately (non-blocking
    /// from the caller's perspective). Each call best-effort borrows a warm
    /// slot from the account pool; a busy/missing account pool spills to the
    /// fixed global fallback-order pool.
    ///
    /// Place sends exactly one HTTP request. An idempotent DELETE may use one
    /// alternate Cancel connection only after the primary connection timed
    /// out; all other ambiguity remains owned by explicit reconciliation.
    pub(crate) fn http_call_async(
        &self,
        method: &str,
        path: &str,
        body: &str,
    ) -> crossbeam_channel::Receiver<HttpReply> {
        self.http_call_async_rec(method, path, body, None)
    }

    /// [`http_call_async`] with an override for the latency-CSV `kind`
    /// column. `None` = classify by (method, path) as usual; `Some` is
    /// for callers whose traffic must stay distinguishable from real
    /// order flow on the same endpoints (the RTT probe's `probe_place` /
    /// `probe_cancel`). The override affects ONLY the CSV kind — stage
    /// histogram and routing are untouched.
    pub(crate) fn http_call_async_rec(
        &self,
        method: &str,
        path: &str,
        body: &str,
        rec_kind_override: Option<crate::latency_record::RequestKind>,
    ) -> crossbeam_channel::Receiver<HttpReply> {
        let method = match method {
            "POST" => reqwest::Method::POST,
            "DELETE" => reqwest::Method::DELETE,
            "GET" => reqwest::Method::GET,
            other => {
                let (tx, rx) = crossbeam_channel::bounded(1);
                let _ = tx.send(Err(HttpErr::Other(format!(
                    "unsupported method: {}",
                    other
                ))));
                return rx;
            }
        };
        // Pick a stable stage name by (method, path prefix) for the
        // latency histogram. Falls back to a generic bucket for paths
        // we haven't categorised.
        let stage = http_stage(method.as_str(), path);
        // Per-request latency CSV: classify place / cancel once up-front
        // (None ⇒ not recorded).
        let rec_kind = rec_kind_override.or_else(|| latency_record_kind(method.as_str(), path));
        let t_start = crate::latency::Instant::now();
        let url: Arc<str> = format!("{}{}", self.clob_base_url, path).into();
        // L2 HMAC covers the endpoint path, not the query string. This also
        // matches the existing `/data/trades?after=...` gap-replay request.
        let auth_path = canonical_l2_auth_path(path);
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);

        // Every authenticated account-owned order/reconcile request uses that
        // account's physical pool, including batch and cancel-all paths that
        // bypass the engine's single-order fire-and-track permit. Query stays
        // process-global. A missing/busy account pool (CLI, embedding, or a
        // rare edge-path burst) spills into the four-slot fallback-order pool
        // without blocking a completion callback that may hold another slot.
        let role = request_role(&method, path);
        let runtime_queue_stage = http_runtime_queue_stage(role);
        let network_stage = http_network_stage(role);
        let account_permit = (role != crate::http1_pool::Role::Query)
            .then(|| crate::http1_pool::try_borrow_account(self.account_state.account_id(), role))
            .flatten();
        let request_client = account_permit
            .as_ref()
            .map(crate::http1_pool::Permit::pooled_client)
            .unwrap_or_else(|| crate::http1_pool::pooled_client(role));
        let attempt_id = request_client.allocate_attempt_id();

        // Sign once and dispatch exactly one request.
        {
            let headers = self.auth.sign_request(method.as_str(), auth_path, body);
            let path_owned = path.to_string();
            let body_owned = Bytes::copy_from_slice(body.as_bytes());
            let url_owned = url.clone();
            let method_owned = method.clone();
            let tx_a = reply_tx;
            let iid_a = self.instance_id.clone();
            let account_id = self.account_state.account_id().to_string();
            let enqueued_at = crate::latency::Instant::now();
            async_rt::order_handle().spawn(async move {
                crate::latency::record(runtime_queue_stage, enqueued_at);
                let network_started = crate::latency::Instant::now();
                let reply = execute_http_with_cancel_connection_failure_hedge(
                    request_client,
                    attempt_id,
                    &account_id,
                    &method_owned,
                    &url_owned,
                    &path_owned,
                    &headers,
                    body_owned,
                )
                .await;
                crate::latency::record(network_stage, network_started);
                drop(account_permit);
                // Capture the CSV status before `reply` is moved into the
                // channel (only when we'll actually record).
                let rec = rec_kind
                    .filter(|_| crate::latency_record::is_active())
                    .map(|k| (k, latency_record_status(&reply)));
                if tx_a.try_send(reply).is_ok() {
                    crate::latency::record(stage, t_start);
                    if let Some((k, status)) = rec {
                        crate::latency_record::record(
                            &iid_a,
                            k,
                            t_start.elapsed().as_secs_f64() * 1000.0,
                            status,
                        );
                    }
                }
            });
        }

        reply_rx
    }

    /// Admission-bound variant of [`Self::http_call_async`]:
    /// dispatches the request on the exact `client` reserved by an
    /// [`crate::http1_pool::Permit`] (via [`execute_http_on`]) instead of a
    /// round-robin pick. Returns immediately with a receiver; the caller
    /// completes off-thread (fire-and-track).
    pub(crate) fn http_call_async_on(
        &self,
        client: crate::http1_pool::PooledClient,
        method: &str,
        path: &str,
        body: &str,
    ) -> crossbeam_channel::Receiver<HttpReply> {
        self.http_call_async_on_timed(client, method, path, body).0
    }

    fn http_call_async_on_timed(
        &self,
        client: crate::http1_pool::PooledClient,
        method: &str,
        path: &str,
        body: &str,
    ) -> (
        crossbeam_channel::Receiver<HttpReply>,
        Arc<HttpCompletionTiming>,
    ) {
        let attempt_id = client.allocate_attempt_id();
        self.http_call_async_on_timed_bytes(
            client,
            attempt_id,
            method,
            path,
            Bytes::copy_from_slice(body.as_bytes()),
        )
    }

    fn http_call_async_on_timed_bytes(
        &self,
        client: crate::http1_pool::PooledClient,
        attempt_id: u64,
        method: &str,
        path: &str,
        body: Bytes,
    ) -> (
        crossbeam_channel::Receiver<HttpReply>,
        Arc<HttpCompletionTiming>,
    ) {
        let timing = Arc::new(HttpCompletionTiming::default());
        let method = match method {
            "POST" => reqwest::Method::POST,
            "DELETE" => reqwest::Method::DELETE,
            "GET" => reqwest::Method::GET,
            other => {
                let (tx, rx) = crossbeam_channel::bounded(1);
                let _ = tx.send(Err(HttpErr::Other(format!(
                    "unsupported method: {}",
                    other
                ))));
                return (rx, timing);
            }
        };
        let stage = http_stage(method.as_str(), path);
        let role = request_role(&method, path);
        let runtime_queue_stage = http_runtime_queue_stage(role);
        let network_stage = http_network_stage(role);
        let reply_enqueue_stage = http_reply_enqueue_stage(role);
        let rec_kind = latency_record_kind(method.as_str(), path);
        let t_start = crate::latency::Instant::now();
        let url = if path == "/order" {
            Arc::clone(&self.order_url)
        } else {
            Arc::<str>::from(format!("{}{}", self.clob_base_url, path))
        };
        let auth_path = canonical_l2_auth_path(path);
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);

        // Single request on the reserved connection.
        {
            let body_text = std::str::from_utf8(body.as_ref())
                .expect("Polymarket request serialization must produce UTF-8 JSON");
            let headers = self
                .auth
                .sign_request(method.as_str(), auth_path, body_text);
            let method_a = method.clone();
            let path_a = path.to_string();
            let body_a = body;
            let url_a = url.clone();
            let tx_a = reply_tx;
            let iid_a = self.instance_id.clone();
            let account_id = self.account_state.account_id().to_string();
            let request_buffers = Arc::clone(&self.request_buffers);
            let enqueued_at = crate::latency::Instant::now();
            let timing_a = Arc::clone(&timing);
            async_rt::order_handle().spawn(async move {
                crate::latency::record(runtime_queue_stage, enqueued_at);
                let network_started = crate::latency::Instant::now();
                // Keep one cheap Bytes handle so the unique allocation can be
                // recovered and returned to the startup-filled pool after
                // reqwest drops its request body.
                let recyclable = body_a.clone();
                let reply = execute_http_with_cancel_connection_failure_hedge(
                    client,
                    attempt_id,
                    &account_id,
                    &method_a,
                    &url_a,
                    &path_a,
                    &headers,
                    body_a,
                )
                .await;
                if let Ok(mut buffer) = recyclable.try_into_mut() {
                    buffer.clear();
                    let _ = request_buffers.push(buffer);
                }
                crate::latency::record(network_stage, network_started);
                timing_a
                    .response_ready_ns
                    .store(now_ns(), Ordering::Release);
                let rec = rec_kind
                    .filter(|_| crate::latency_record::is_active())
                    .map(|k| (k, latency_record_status(&reply)));
                let reply_enqueue_started = crate::latency::Instant::now();
                if tx_a.try_send(reply).is_ok() {
                    timing_a
                        .reply_enqueued_ns
                        .store(now_ns(), Ordering::Release);
                    crate::latency::record(reply_enqueue_stage, reply_enqueue_started);
                    crate::latency::record(stage, t_start);
                    if let Some((k, status)) = rec {
                        crate::latency_record::record(
                            &iid_a,
                            k,
                            t_start.elapsed().as_secs_f64() * 1000.0,
                            status,
                        );
                    }
                }
            });
        }

        (reply_rx, timing)
    }

    /// Synchronous variant — dispatches and blocks on the reply. Used by
    /// single-op paths (POST /order, DELETE /order). Blocks the calling
    /// thread on a crossbeam recv; the actual I/O work happens on the
    /// tokio runtime thread.
    pub(crate) fn http_call_sync(&self, method: &str, path: &str, body: &str) -> HttpReply {
        self.http_call_async(method, path, body)
            .recv()
            .unwrap_or_else(|_| Err(HttpErr::Transport("async reply dropped".to_string())))
    }

    /// Permit-bound synchronous request. Unlike [`Self::http_call_sync`], this
    /// never round-robins away from the exact admission-pool connection the
    /// engine reserved.
    pub(crate) fn http_call_sync_on(
        &self,
        client: crate::http1_pool::PooledClient,
        method: &str,
        path: &str,
        body: &str,
    ) -> HttpReply {
        self.http_call_async_on(client, method, path, body)
            .recv()
            .unwrap_or_else(|_| Err(HttpErr::Transport("async reply dropped".to_string())))
    }

    /// [`http_call_sync`] with a latency-CSV kind override — see
    /// [`http_call_async_rec`](Self::http_call_async_rec).
    pub(crate) fn http_call_sync_rec(
        &self,
        method: &str,
        path: &str,
        body: &str,
        rec_kind_override: Option<crate::latency_record::RequestKind>,
    ) -> HttpReply {
        self.http_call_async_rec(method, path, body, rec_kind_override)
            .recv()
            .unwrap_or_else(|_| Err(HttpErr::Transport("async reply dropped".to_string())))
    }
}

// ════════════════════════════════════════════════════════════════
// PolymarketTrade
// ════════════════════════════════════════════════════════════════

/// Opaque in-flight place: the reply receiver + the pre-computed local
/// orderID. Produced by [`PolymarketTrade::submit_fire`], consumed by
/// [`PolymarketTrade::complete_submit`] off-thread. Fields are private so
/// the engine holds it opaquely (never touching `HttpReply`).
pub struct PendingSubmit {
    local_oid: String,
    rx: crossbeam_channel::Receiver<HttpReply>,
    timing: Arc<HttpCompletionTiming>,
    attempt: crate::http1_pool::AttemptTraceHandle,
}

/// Opaque in-flight cancel: the reply-handling context + the reply
/// receiver (`None` = nothing was sent, complete emits Cancelled directly).
/// Produced by [`PolymarketTrade::cancel_fire`], consumed by
/// [`PolymarketTrade::complete_cancel`].
pub struct PendingCancel {
    ctx: CancelCtx,
    rx: Option<crossbeam_channel::Receiver<HttpReply>>,
    timing: Option<Arc<HttpCompletionTiming>>,
    attempt: Option<crate::http1_pool::AttemptTraceHandle>,
}

struct PreparedSubmit {
    local_oid: String,
    body: Bytes,
    prep_ns: u64,
    signed_ns: u64,
    account_recorded_ns: u64,
}

pub(crate) enum PreparedCancelBody {
    Ready(Bytes),
    NoOrderId,
    BufferUnavailable,
}

/// Polymarket CLOB live order executor.
pub struct PolymarketTrade {
    shared: Arc<SharedState>,
    /// Owner UUID for the Polymarket CLOB (same as api_key for user accounts).
    owner: String,
    /// The strategy instance this route serves. The `SharedState` is shared
    /// per-ACCOUNT across instances; this per-route id is stamped onto every
    /// order placed through it (`TrackedOrder.instance_id`) so bulk cancels
    /// scope to this instance only. Empty for heartbeat/CLI/default routes.
    instance_id: String,
    /// Correlation hint for the `gen_ns=` field on the cancel log line: the
    /// strategy-side emission time (ns) of the signal currently being
    /// dispatched, set by the engine right before it calls a cancel/replace
    /// method on this route (same `&mut self` borrow ⇒ no interleaving). Lets
    /// offline log analysis compute the on_quote→dispatch latency for cancels
    /// (place lines carry `order.timestamp_ns` directly). 0 = unknown
    /// (heartbeat/CLI/reconcile paths that don't originate from a quote).
    gen_ns_hint: u64,
}

impl PolymarketTrade {
    /// Create a new PolymarketTrade with real API credentials.
    ///
    /// Create a new PolymarketTrade with real API credentials.
    /// For GnosisSafe, the maker/funder address is derived from private_key via CREATE2.
    pub fn new(
        api_key: &str,
        api_secret: &str,
        passphrase: &str,
        private_key: &str,
        neg_risk: bool,
        rate_limit_per_second: u32,
        sig_type: SignatureType,
    ) -> Result<Self> {
        Self::new_with_pool(
            api_key,
            api_secret,
            passphrase,
            private_key,
            neg_risk,
            rate_limit_per_second,
            sig_type,
            ClobVersion::V2,
            "",
            "",
            true,
            "cli",
            "",
            GapReplayConfig::default(),
            None,
        )
    }

    /// Live-engine entry point. Same as `new` but exposes the v1/v2
    /// dispatch knob, the api_url_prefix override, the builder
    /// attribution code, and the use_batch_orders flag.
    pub fn new_with_pool(
        api_key: &str,
        api_secret: &str,
        passphrase: &str,
        private_key: &str,
        neg_risk: bool,
        rate_limit_per_second: u32,
        sig_type: SignatureType,
        clob_version: ClobVersion,
        builder_code: &str,
        api_url_prefix: &str,
        use_batch_orders: bool,
        instance_id: &str,
        funder: &str,
        gap_replay: GapReplayConfig,
        account_ledger_path: Option<&std::path::Path>,
    ) -> Result<Self> {
        Self::new_with_pool_inner(
            api_key,
            api_secret,
            passphrase,
            private_key,
            neg_risk,
            rate_limit_per_second,
            sig_type,
            clob_version,
            builder_code,
            api_url_prefix,
            use_batch_orders,
            instance_id,
            funder,
            gap_replay,
            account_ledger_path,
            ShutdownToken::new(),
            false,
        )
    }

    /// Live-engine constructor that may conservatively open a narrowly
    /// repairable FAILED-trade under-reservation. The engine must run and
    /// complete authoritative startup order queries before strategy admission.
    pub fn new_with_pool_for_startup_query_repair(
        api_key: &str,
        api_secret: &str,
        passphrase: &str,
        private_key: &str,
        neg_risk: bool,
        rate_limit_per_second: u32,
        sig_type: SignatureType,
        clob_version: ClobVersion,
        builder_code: &str,
        api_url_prefix: &str,
        use_batch_orders: bool,
        instance_id: &str,
        funder: &str,
        gap_replay: GapReplayConfig,
        account_ledger_path: Option<&std::path::Path>,
    ) -> Result<Self> {
        Self::new_with_pool_inner(
            api_key,
            api_secret,
            passphrase,
            private_key,
            neg_risk,
            rate_limit_per_second,
            sig_type,
            clob_version,
            builder_code,
            api_url_prefix,
            use_batch_orders,
            instance_id,
            funder,
            gap_replay,
            account_ledger_path,
            ShutdownToken::new(),
            true,
        )
    }

    /// Live-engine constructor whose account actors share the run-level
    /// shutdown token with strategy, execution, admission and latency workers.
    pub fn new_with_pool_for_startup_query_repair_and_shutdown(
        api_key: &str,
        api_secret: &str,
        passphrase: &str,
        private_key: &str,
        neg_risk: bool,
        rate_limit_per_second: u32,
        sig_type: SignatureType,
        clob_version: ClobVersion,
        builder_code: &str,
        api_url_prefix: &str,
        use_batch_orders: bool,
        instance_id: &str,
        funder: &str,
        gap_replay: GapReplayConfig,
        account_ledger_path: Option<&std::path::Path>,
        shutdown_token: ShutdownToken,
    ) -> Result<Self> {
        Self::new_with_pool_inner(
            api_key,
            api_secret,
            passphrase,
            private_key,
            neg_risk,
            rate_limit_per_second,
            sig_type,
            clob_version,
            builder_code,
            api_url_prefix,
            use_batch_orders,
            instance_id,
            funder,
            gap_replay,
            account_ledger_path,
            shutdown_token,
            true,
        )
    }

    fn new_with_pool_inner(
        api_key: &str,
        api_secret: &str,
        passphrase: &str,
        private_key: &str,
        neg_risk: bool,
        rate_limit_per_second: u32,
        sig_type: SignatureType,
        clob_version: ClobVersion,
        builder_code: &str,
        api_url_prefix: &str,
        use_batch_orders: bool,
        instance_id: &str,
        funder: &str,
        gap_replay: GapReplayConfig,
        account_ledger_path: Option<&std::path::Path>,
        shutdown_token: ShutdownToken,
        startup_query_repair: bool,
    ) -> Result<Self> {
        let salt_sequence = Arc::new(super::signer::AccountSaltSequence::new());
        let signer = OrderSigner::new_with_salt_sequence(
            private_key,
            neg_risk,
            sig_type,
            Arc::clone(&salt_sequence),
        )?;
        // Build v2 signer eagerly iff v2 mode — it's tiny (a few keys +
        // strings) and keeps the sign-hot-path branch a simple Option
        // check rather than constructing per-call. For POLY_1271 the
        // deposit-wallet `funder` is the order maker/signer.
        let signer_v2 = Some(
            super::signer_v2::OrderSignerV2::new_with_salt_sequence(
                private_key,
                neg_risk,
                sig_type,
                builder_code,
                salt_sequence,
            )?
            .with_funder(funder),
        );

        // POLY_ADDRESS must be the signer (EOA) address, matching the API key
        let auth = PolyAuth::new(api_key, api_secret, passphrase, &signer.signer_address)?;

        let clob_base_url = if api_url_prefix.trim().is_empty() {
            DEFAULT_CLOB_BASE_URL.to_string()
        } else {
            api_url_prefix.trim_end_matches('/').to_string()
        };
        let order_url: Arc<str> = format!("{clob_base_url}/order").into();

        info!("[PolymarketTrade] Initialized: maker={} signer={} sig_type={:?} exchange={} clob={} host={} builder={} batch={}",
            signer.maker_address, signer.signer_address, signer.signature_type,
            if neg_risk { "NegRiskCTFExchange" } else { "CTFExchange" },
            clob_version.as_str(),
            clob_base_url,
            if builder_code.is_empty() { "<zero>" } else { builder_code },
            use_batch_orders,
        );

        // POL balance preflight (only when on-chain gas is enabled).
        // Catches "wallet truly empty" at startup so the operator can
        // top up before the first Maintenance redeem/split fires —
        // distinct from the false "balance 0" reported by an unhealthy
        // RPC node, which `fetch_pol_balance` surfaces as Err and we
        // log separately.
        if super::wallet::read_gas_via_signer_wallet_flag() {
            match super::wallet::fetch_pol_balance(&signer.signer_address) {
                Ok(pol) if pol < 0.5 => {
                    log::error!(
                        "[PolymarketTrade] Signer POL balance LOW: {:.6} POL on {} (< 0.5 threshold). \
                         On-chain redeem/split will fail with 'insufficient funds for gas' once balance \
                         drops below ~0.25 POL (max_fee 500 gwei × gas_limit 500k). Top up the signer EOA.",
                        pol, signer.signer_address);
                }
                Ok(pol) => {
                    info!(
                        "[PolymarketTrade] Signer POL balance OK: {:.6} POL on {}",
                        pol, signer.signer_address
                    );
                }
                Err(e) => {
                    warn!("[PolymarketTrade] Could not fetch signer POL balance (RPC issue, not enforcing minimum): {}", e);
                }
            }
        }

        // Post-cutover: `clob.polymarket.com` now serves the v2 schema
        // directly (the legacy `clob-v2.polymarket.com` test host was
        // folded into the canonical hostname). The v2-vs-host mismatch
        // warning that used to live here is no longer informative —
        // either host accepts v2-signed orders, and v1 was retired.

        // All HTTP now goes through the shared tokio runtime + reqwest
        // HTTP/2 client (`async_rt::http_client()`). No dedicated worker
        // threads are required.

        // Authoritative on-book maker address for fill matching. For
        // POLY_1271 the maker is the deposit wallet (funder), which
        // `with_funder` wrote into `signer_v2.maker_address`; v1 / non-v2
        // fall back to the signer's own maker (EOA or Safe proxy).
        let order_maker_address = signer_v2
            .as_ref()
            .map(|s| s.maker_address.clone())
            .unwrap_or_else(|| signer.maker_address.clone());

        let account_state = if let Some(path) = account_ledger_path {
            let account = if startup_query_repair {
                hexagent_account::account::shared_account::SharedAccount::new_persistent_for_query_repair(
                    instance_id,
                    path,
                )
            } else {
                hexagent_account::account::shared_account::SharedAccount::new_persistent(
                    instance_id,
                    path,
                )
            };
            Arc::new(account.map_err(anyhow::Error::msg)?)
        } else {
            Arc::new(hexagent_account::account::shared_account::SharedAccount::new(instance_id))
        };
        let recovered_orders = account_state.orders();
        let recovered_trades = account_state.restored_trades();
        let mut recovered_open = HashMap::new();
        let mut recovered_coid_to_oid = HashMap::new();
        let mut recovered_oid_to_coid = HashMap::new();
        let mut recovered_coid_to_token = HashMap::new();
        let recovered_runtime_ownership = RuntimeOwnershipIndex::new();
        for order in &recovered_orders {
            // Restore terminal mappings too: a late private trade lifecycle
            // must still resolve to the placing instance after restart.
            if !order.client_order_id.is_empty() && !order.order_id.is_empty() {
                recovered_coid_to_oid.insert(order.client_order_id.clone(), order.order_id.clone());
                recovered_oid_to_coid.insert(
                    normalize_order_id(&order.order_id),
                    order.client_order_id.clone(),
                );
            }
            if !order.client_order_id.is_empty() && !order.token_id.is_empty() {
                recovered_coid_to_token
                    .insert(order.client_order_id.clone(), order.token_id.clone());
            }
            if !order.client_order_id.is_empty() && !order.order_id.is_empty() {
                recovered_runtime_ownership
                    .insert(&order.order_id, order.clone())
                    .map_err(|error| anyhow!(error))?;
            }
            if matches!(
                order.status,
                OrderStatus::Pending
                    | OrderStatus::Accepted
                    | OrderStatus::PartiallyFilled
                    | OrderStatus::NewOrderTimeout
                    | OrderStatus::CancelOrderTimeout
                    | OrderStatus::CancelUncertain
                    | OrderStatus::Failed
            ) || order.reserved_cash > 1e-9
                || order.reserved_quantity > 1e-9
            {
                recovered_open.insert(
                    order.client_order_id.clone(),
                    TrackedOrder {
                        order_slot: Default::default(),
                        symbol: order.token_id.clone(),
                        side: order.side,
                        instance_id: order.instance_id.clone(),
                    },
                );
            }
        }
        if !recovered_open.is_empty() {
            account_state.begin_order_recovery(recovered_open.keys().map(String::as_str));
            warn!(
                "[PolymarketTrade] account={} restored {} potentially-live order(s) and reservations from {}",
                instance_id,
                recovered_open.len(),
                account_ledger_path.map(|path| path.display().to_string()).unwrap_or_default(),
            );
        }

        let (settled_gc_tx, settled_gc_rx) = crossbeam_channel::bounded(1);
        let (account_lifecycle_tx, account_lifecycle_rx) = crossbeam_channel::bounded(16_384);
        let (account_owner_handle, account_owner_state) = account_state
            .bind_account_owner()
            .map_err(|error| anyhow!("Polymarket account owner bind failed: {error}"))?;
        let (account_maintenance_tx, account_maintenance_rx) = crossbeam_channel::bounded(16_384);
        let (attempt_audit_tx, attempt_audit_rx) =
            crossbeam_channel::bounded(EXECUTION_AUDIT_QUEUE_CAPACITY);
        let initial_execution_state = ExecutionStateSnapshot {
            open_orders: ExecutionReadMap::from_hash_map(recovered_open),
            coid_to_oid: ExecutionReadMap::from_hash_map(recovered_coid_to_oid),
            oid_to_coid: ExecutionReadMap::from_hash_map(recovered_oid_to_coid),
            coid_to_token: ExecutionReadMap::from_hash_map(recovered_coid_to_token),
        };
        let initial_live_position = LivePositionManager::from_restored(recovered_trades);
        let restored_live_position_watermark = initial_live_position.last_match_time_secs();
        let shared = Arc::new(SharedState {
            instance_id: instance_id.to_string(),
            account_state,
            account_owner_handle,
            execution_state: ArcSwap::from_pointee(initial_execution_state.clone()),
            runtime_order_ownership: recovered_runtime_ownership,
            strategy_owner_by_instance: ArcSwap::from_pointee(HashMap::new()),
            probe_order_ids: ProbeOrderIdRing::default(),
            auth,
            signer,
            signer_v2,
            order_maker_address,
            clob_version,
            use_batch_orders,
            clob_base_url,
            order_url,
            request_buffers: new_request_buffer_pool(),
            live_position_last_match_secs: AtomicU64::new(restored_live_position_watermark),
            #[cfg(test)]
            test_live_position: Mutex::new(LivePositionManager::new()),
            user_feed_health: std::sync::Arc::new(super::live_position::UserFeedHealth::new()),
            gap_replay,
            rate_limiter: crate::exchange::AtomicRateLimiter::new(rate_limit_per_second.max(1)),
            balance_backoff_until_ns: ArcSwap::from_pointee(HashMap::new()),
            trading_disabled_backoff_until_ns: std::sync::atomic::AtomicU64::new(0),
            invalid_token_backoff: ArcSwap::from_pointee(HashMap::new()),
            http_425_warn_silent_until_ns: std::sync::atomic::AtomicU64::new(0),
            http_425_reconcile_backoff_until_ns: PublishedStringMap::default(),
            http_425_circuit_entries_total: std::sync::atomic::AtomicU64::new(0),
            startup_recovery_lookup_warn_silent_until_ns: std::sync::atomic::AtomicU64::new(0),
            get_live_delete_uncertain_total: std::sync::atomic::AtomicU64::new(0),
            get_live_delete_uncertain_warn_silent_until_ns: std::sync::atomic::AtomicU64::new(0),
            get_live_delete_uncertain_suppressed: std::sync::atomic::AtomicU64::new(0),
            terminal_trade_backfill_missing_total: std::sync::atomic::AtomicU64::new(0),
            terminal_trade_backfill_missing_warn_silent_until_ns: std::sync::atomic::AtomicU64::new(
                0,
            ),
            terminal_trade_backfill_missing_suppressed: std::sync::atomic::AtomicU64::new(0),
            terminal_trade_backfill_noop_total: std::sync::atomic::AtomicU64::new(0),
            terminal_trade_backfill_noop_log_silent_until_ns: std::sync::atomic::AtomicU64::new(0),
            terminal_trade_backfill_noop_suppressed: std::sync::atomic::AtomicU64::new(0),
            reconcile_cancel_not_found_counts: PublishedStringMap::default(),
            reconcile_attempts: ReconcileAttemptCounters::default(),
            placement_reconcile_next_retry_ns: PublishedStringMap::default(),
            cancel_reconcile_next_retry_ns: PublishedStringMap::default(),
            pending_delayed_orphans: PublishedStringSet::default(),
            settled_gc_tx,
            account_maintenance_tx,
            account_lifecycle_tx,
            attempt_audit_tx,
            shutdown_token,
            background_worker_handles: Mutex::new(Vec::with_capacity(3)),
        });
        let (account_owner, lifecycle_owner, account_owner_ready) =
            SharedState::spawn_account_owner_workers(
            &shared,
            initial_execution_state,
            initial_live_position,
            account_lifecycle_rx,
            account_owner_state,
            account_maintenance_rx,
            settled_gc_rx,
        );
        for worker in ["cold account owner", "private lifecycle owner"] {
            account_owner_ready
                .recv_timeout(std::time::Duration::from_secs(2))
                .map_err(|error| anyhow!("Polymarket {worker} failed to start: {error}"))?;
        }
        let (account_owner_barrier_tx, account_owner_barrier_rx) = crossbeam_channel::bounded(1);
        shared
            .account_lifecycle_tx
            .send(AccountLifecycleJob::StartupBarrier(
                account_owner_barrier_tx,
            ))
            .map_err(|error| anyhow!("Polymarket account owner startup lane closed: {error}"))?;
        account_owner_barrier_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .map_err(|error| anyhow!("Polymarket account owner startup barrier failed: {error}"))?;
        // Resume a durable zero-reference cleanup request without waiting for
        // an unrelated private trade to provide the first wake after restart.
        shared.request_settled_gc();
        let attempt_audit = SharedState::spawn_attempt_audit_worker(&shared, attempt_audit_rx);
        shared
            .background_worker_handles
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend([account_owner, lifecycle_owner, attempt_audit]);
        Ok(Self {
            shared,
            owner: api_key.to_string(),
            // CLI / first-build route: per-instance trading routes are
            // rebuilt via `from_shared(.., instance_id)`.
            instance_id: String::new(),
            gen_ns_hint: 0,
        })
    }

    /// Spawn an account heartbeat thread that sends one signed
    /// `POST /heartbeats` every `HEARTBEAT_INTERVAL`. Connection pools are
    /// warmed once by [`Self::prewarm_connections`]; heartbeats no longer fan
    /// out across every client.
    /// Returns a join handle; the thread stops when `shutdown` is set.
    pub fn spawn_heartbeat(
        &self,
        shutdown: Arc<std::sync::atomic::AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        let auth = self.shared.auth.clone();
        let base_url = self.shared.clob_base_url.clone();
        // Tag `[PolyHeartbeat]` lines with the ACCOUNT (`heartbeat{acct=
        // <account_id>}:`); the heartbeat is per-account. Async task →
        // `.instrument()`. `SharedState.instance_id` holds the account_id.
        use tracing::Instrument as _;
        let hb_span = tracing::info_span!("heartbeat", acct = %self.shared.instance_id);
        // Order runtime: any connection the heartbeat (re)establishes on
        // its Query slot must be owned by the order reactor, matching
        // keep-warm/prewarm ownership. Load is one request per 10 s.
        let task_handle = async_rt::order_handle()
            .spawn(heartbeat_loop(auth, shutdown, base_url).instrument(hb_span));
        // Return a std JoinHandle so existing engine shutdown code can
        // .join() it. The handle's thread awaits the tokio task to
        // finish via block_on_runtime — no polling loop.
        std::thread::Builder::new()
            .name("poly-heartbeat-join".into())
            .spawn(move || {
                crate::os_tune::pin_background("poly-heartbeat-join");
                async_rt::block_on_runtime(async move {
                    let _ = task_handle.await;
                });
            })
            .expect("Failed to spawn heartbeat thread")
    }

    /// Get a clone of the shared state (for user_feed thread).
    pub fn shared_state(&self) -> Arc<SharedState> {
        self.shared.clone()
    }

    /// Canonical physical maker/funder identity used for account de-duplication
    /// at engine startup.
    pub fn order_maker_address(&self) -> &str {
        &self.shared.order_maker_address
    }

    /// Pre-warm transport pools once per process, then send one signed
    /// heartbeat for this account.
    ///
    /// CLOB owns the hot place/cancel pools, so every CLOB client is warmed.
    /// data-api is query-only and therefore warms only the global Query role.
    /// Gamma is deliberately demand-driven: its ordinary client may reuse a
    /// recent connection, but receives no prewarm or keep-warm traffic.
    pub fn prewarm_connections(&self) {
        let start = std::time::Instant::now();
        let clob_base_url = self.shared.clob_base_url.clone();
        TRANSPORT_PREWARMED.get_or_init(|| {
            let clob_clients = crate::async_rt::http_clients_all();
            let query_clients =
                crate::http1_pool::clients_for_role(crate::http1_pool::Role::Query);
            info!(
                "[PolymarketTrade] Pre-warming transport once: clob={} data-api={} concurrency={} stagger={}ms",
                clob_clients.len(),
                query_clients.len(),
                PREWARM_CONCURRENCY,
                PREWARM_STAGGER_MS,
            );
            async_rt::block_on_order_runtime(async move {
                prewarm_clients_staggered(
                    "clob",
                    clob_clients,
                    format!("{}/time", clob_base_url.trim_end_matches('/')),
                )
                .await;
                prewarm_clients_staggered(
                    "data-api",
                    query_clients,
                    "https://data-api.polymarket.com/".into(),
                )
                .await;
            });
        });

        // Heartbeat is account-scoped, but connection warming is not. Send
        // exactly one signed request per account instead of one per client.
        let auth = self.shared.auth.clone();
        let headers = auth.sign_request("POST", "/heartbeats", "");
        let heartbeat_url = format!(
            "{}/heartbeats",
            self.shared.clob_base_url.trim_end_matches('/')
        );
        let client = crate::async_rt::http_client_query();
        let heartbeat_status = async_rt::block_on_order_runtime(async move {
            let mut request = client
                .post(&heartbeat_url)
                .header("Content-Type", "application/json")
                .body(String::new());
            for (name, value) in headers.as_pairs() {
                request = request.header(name, value);
            }
            send_and_drain(request).await
        });
        match heartbeat_status {
            Ok(status) if (200..400).contains(&status) => {
                info!("[PolymarketTrade] Account heartbeat pre-warm ok")
            }
            Ok(status) => warn!(
                "[PolymarketTrade] Account heartbeat pre-warm HTTP {}",
                status,
            ),
            Err(error) => warn!(
                "[PolymarketTrade] Account heartbeat pre-warm failed: {}",
                error,
            ),
        }
        info!(
            "[PolymarketTrade] Pre-warm complete ({:.0}ms total)",
            start.elapsed().as_secs_f64() * 1000.0,
        );
    }

    /// Create from existing SharedState (for LiveRouter inside execution thread).
    /// Build a per-instance route over a shared (per-account) `SharedState`.
    /// `instance_id` tags orders placed through this route so bulk cancels
    /// stay scoped to this instance (siblings on the same wallet untouched).
    /// Pass `""` for non-trading routes (heartbeat / CLI).
    /// Set the `gen_ns` correlation hint used by the next cancel log line.
    /// The engine calls this on the route immediately before a cancel/replace
    /// dispatch, in the same `&mut self` borrow, passing the signal's
    /// strategy-side emission time. See [`PolymarketTrade::gen_ns_hint`].
    #[inline]
    pub fn set_gen_ns_hint(&mut self, gen_ns: u64) {
        self.gen_ns_hint = gen_ns;
    }

    pub fn from_shared(shared: Arc<SharedState>, owner: &str, instance_id: &str) -> Self {
        Self {
            shared,
            owner: owner.to_string(),
            instance_id: instance_id.to_string(),
            gen_ns_hint: 0,
        }
    }

    /// Clone for callers that need a fresh value (e.g. thread-scope
    /// parallel dispatch). Shares the SharedState via Arc, and the
    /// reqwest client is a process-wide singleton accessed via
    /// `async_rt::http_client()` — no per-clone state.
    pub fn clone_worker(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            owner: self.owner.clone(),
            instance_id: self.instance_id.clone(),
            gen_ns_hint: self.gen_ns_hint,
        }
    }

    /// Resolve potentially-live orders restored from the durable account
    /// ledger before strategy workers are allowed to quote. A recovered order
    /// keeps its owner instance's balance-changing maintenance blocked until
    /// an order-specific lookup supplies authoritative terminal metadata,
    /// proves it cancelled/rejected, its event is durably known to have ended,
    /// or its recovery-only lookup returns literal JSON `null`. Quote admission
    /// continues under the original reservation. Once complete matched
    /// quantity and trade IDs are durable, exact private-trade replay continues
    /// concurrently under the resized reservation and strategy inventory bridge.
    ///
    /// The bounded retry window covers the common pending/delayed write-index
    /// race without turning startup into an unbounded wait. Anything still
    /// ambiguous is deliberately left reserved and operator-visible.
    fn audit_historical_order_trades(
        &self,
        order_id: &str,
        submitted_at_ms: u64,
    ) -> HistoricalOrderTradeAudit {
        let after_secs = (submitted_at_ms / 1_000).saturating_sub(ORPHAN_TRADE_AUDIT_REWIND_SECS);
        let mut cursor = String::new();
        let mut seen_cursors = HashSet::new();
        let mut matching_records = 0usize;
        for page in 1..=ORPHAN_TRADE_AUDIT_MAX_PAGES {
            let path = if cursor.is_empty() {
                format!("{AUTHENTICATED_TRADES_PATH}?after={after_secs}")
            } else {
                format!("{AUTHENTICATED_TRADES_PATH}?after={after_secs}&next_cursor={cursor}")
            };
            let json = match self.shared.http_call_sync("GET", &path, "") {
                Ok(json) => json,
                Err(error) => {
                    return HistoricalOrderTradeAudit::Unavailable(format!(
                        "page={page} error={error}"
                    ));
                }
            };
            let (records, next) = match authenticated_trade_page(json) {
                Ok(page) => page,
                Err(error) => {
                    return HistoricalOrderTradeAudit::Unavailable(format!(
                        "page={page} schema={error}"
                    ));
                }
            };
            matching_records = matching_records.saturating_add(
                records
                    .iter()
                    .filter(|record| trade_record_references_order(record, order_id))
                    .count(),
            );
            if next.is_empty() || next == "LTE=" {
                return if matching_records == 0 {
                    HistoricalOrderTradeAudit::CompleteNoFill {
                        pages: page,
                        after_secs,
                    }
                } else {
                    HistoricalOrderTradeAudit::FoundFill {
                        records: matching_records,
                    }
                };
            }
            if !seen_cursors.insert(next.clone()) {
                return HistoricalOrderTradeAudit::Incomplete { pages: page };
            }
            cursor = next;
        }
        HistoricalOrderTradeAudit::Incomplete {
            pages: ORPHAN_TRADE_AUDIT_MAX_PAGES,
        }
    }

    pub fn reconcile_persisted_orphan_order_anomalies(&self) -> OrphanOrderStartupRepair {
        const REBUILD_FEE_RESERVE_BPS: u32 = 10_000;
        let anomalies = self.shared.account_state.persisted_orphan_order_anomalies();
        let mut summary = OrphanOrderStartupRepair {
            examined: anomalies.len(),
            ..OrphanOrderStartupRepair::default()
        };
        for anomaly in anomalies {
            // A late lifecycle replay can race settled-event GC: the compact
            // runtime OID map is gone while the durable terminal ownership
            // row is still intentionally retained. Prefer that exact local
            // authority before asking the CLOB, whose historical response no
            // longer carries our client id. Token mismatch remains fail-closed.
            if let Some(ownership) = self.shared.lookup_order_ownership(&anomaly.order_id) {
                let token_matches = anomaly
                    .token_id
                    .as_deref()
                    .is_none_or(|token| token == ownership.token_id);
                if token_matches
                    && self
                        .shared
                        .account_state
                        .reconcile_order_route(&ownership.client_order_id, &anomaly.order_id)
                        .is_some()
                {
                    if let Err(error) = self.shared.install_runtime_order_id(
                        &ownership.client_order_id,
                        &anomaly.order_id,
                        &ownership.token_id,
                        Some(&ownership),
                    ) {
                        warn!(
                            "[PolymarketTrade] startup ownership publish failed oid={}: {}",
                            anomaly.order_id, error,
                        );
                        continue;
                    }
                    summary.rebuilt = summary.rebuilt.saturating_add(1);
                    info!(
                        "[PolymarketTrade] startup orphan ownership restored from durable row oid={} coid={}",
                        anomaly.order_id, ownership.client_order_id,
                    );
                    continue;
                }
            }
            let coid = anomaly.client_order_id.as_deref().unwrap_or("");
            let fetched = self.fetch_order_by_id(coid, &anomaly.order_id, None, false);
            let (fetched, absent_evidence) = match fetched {
                FetchOrderResult::Found(fetched) => (Some(fetched), None),
                FetchOrderResult::NotFound(evidence) => (None, Some(evidence)),
                FetchOrderResult::Unavailable(kind) if kind.is_json_null() => (
                    None,
                    Some("authenticated /data/order returned literal null".to_string()),
                ),
                FetchOrderResult::Unavailable(kind) => {
                    let failure = match kind {
                        FetchUnavailable::Timeout => OrphanOrderRepairFailure::LookupTimeout,
                        FetchUnavailable::Transport => OrphanOrderRepairFailure::LookupTransport,
                        FetchUnavailable::Http(_) => OrphanOrderRepairFailure::LookupHttp,
                        FetchUnavailable::InvalidResponse(_) => {
                            OrphanOrderRepairFailure::LookupInvalidResponse
                        }
                    };
                    summary.unresolved(failure);
                    continue;
                }
            };
            if let Some(absence) = absent_evidence {
                if coid.is_empty() {
                    summary.unresolved(OrphanOrderRepairFailure::MissingClientOrderId);
                    continue;
                }
                let Some(submitted_at_ms) = coid_wall_clock_ms(coid) else {
                    summary.unresolved(OrphanOrderRepairFailure::MissingHistoricalTimestamp);
                    continue;
                };
                // Do not redownload historical trade pages every 30 seconds
                // while an event is still live. Settlement/legacy-finality is
                // monotonic; wait for that cheap durable gate first, then run
                // one complete authenticated absence sweep.
                let now_ms = now_ns() / 1_000_000;
                let settlement = anomaly.token_id.as_deref().map_or_else(
                    || {
                        now_ms.saturating_sub(submitted_at_ms)
                            >= LEGACY_ORPHAN_COMPACTION_FINALITY_MS
                    },
                    |token| self.shared.account_state.token_event_has_ended(token),
                );
                if !settlement {
                    summary.unresolved(OrphanOrderRepairFailure::EventNotSettled);
                    continue;
                }
                let settlement_evidence = anomaly.token_id.as_deref().map_or_else(
                    || {
                        format!(
                            "legacy_coid_age_ms={} >= compaction_finality_ms={}",
                            now_ms.saturating_sub(submitted_at_ms),
                            LEGACY_ORPHAN_COMPACTION_FINALITY_MS,
                        )
                    },
                    |token| format!("durable event settlement token={token}"),
                );
                match self.audit_historical_order_trades(&anomaly.order_id, submitted_at_ms) {
                    HistoricalOrderTradeAudit::CompleteNoFill { pages, after_secs } => {
                        let evidence = format!(
                            "{absence}; authenticated /data/trades complete pages={pages} after={after_secs} oid_matches=0; {settlement_evidence}"
                        );
                        if self
                            .shared
                            .account_state
                            .record_authoritative_absent_orphan_order_audit(
                                &anomaly.order_id,
                                anomaly.client_order_id.as_deref(),
                                &evidence,
                            )
                        {
                            summary.tombstoned = summary.tombstoned.saturating_add(1);
                            info!(
                                "[PolymarketTrade] startup orphan ownership retired by authoritative absence oid={} coid={} evidence={}",
                                anomaly.order_id, coid, evidence,
                            );
                        } else {
                            summary.unresolved(OrphanOrderRepairFailure::TombstoneRejected);
                        }
                    }
                    HistoricalOrderTradeAudit::FoundFill { records } => {
                        debug!(
                            "[PolymarketTrade] startup orphan oid={} coid={} trade fallback found {} matching record(s); ownership rebuild remains required",
                            anomaly.order_id, coid, records,
                        );
                        summary.unresolved(OrphanOrderRepairFailure::TradeAuditFoundFill);
                    }
                    HistoricalOrderTradeAudit::Incomplete { pages } => {
                        debug!(
                            "[PolymarketTrade] startup orphan oid={} coid={} trade fallback incomplete pages={}",
                            anomaly.order_id, coid, pages,
                        );
                        summary.unresolved(OrphanOrderRepairFailure::TradeAuditIncomplete);
                    }
                    HistoricalOrderTradeAudit::Unavailable(error) => {
                        debug!(
                            "[PolymarketTrade] startup orphan oid={} coid={} trade fallback unavailable={}",
                            anomaly.order_id, coid, error,
                        );
                        summary.unresolved(OrphanOrderRepairFailure::TradeAuditUnavailable);
                    }
                }
                continue;
            }
            let fetched = fetched.expect("found lookup must retain its parsed order");
            let original_size = fetched
                .audit
                .original_size
                .as_deref()
                .and_then(|value| value.parse::<f64>().ok());
            let size_matched = fetched
                .audit
                .size_matched
                .as_deref()
                .and_then(|value| value.parse::<f64>().ok());
            let terminal_status = match fetched.status.as_str() {
                "MATCHED" | "FILLED" => Some(OrderStatus::Filled),
                "INVALID" => Some(OrderStatus::Rejected),
                status if status.starts_with("CANCELED") || status.starts_with("CANCELLED") => {
                    Some(OrderStatus::Cancelled)
                }
                _ => None,
            };

            let rebuilt = anomaly
                .client_order_id
                .as_deref()
                .zip(fetched.rebuild_identity.as_ref())
                .and_then(|(client_order_id, identity)| {
                    if !identity
                        .maker_address
                        .eq_ignore_ascii_case(&self.shared.order_maker_address)
                    {
                        return None;
                    }
                    let instance_id = self
                        .shared
                        .account_state
                        .instance_id_for_historical_coid(client_order_id)?;
                    let quantity = original_size?;
                    let fee_rate_bps = identity.fee_rate_bps.unwrap_or(REBUILD_FEE_RESERVE_BPS);
                    let reserve_cash = if identity.side == Side::Buy {
                        quantity * identity.price * (1.0 + f64::from(fee_rate_bps) / 10_000.0)
                    } else {
                        0.0
                    };
                    let reserve_quantity = if identity.side == Side::Sell {
                        quantity
                    } else {
                        0.0
                    };
                    let ownership = OrderOwnership {
                        order_slot: Default::default(),
                        account_id: self.shared.account_state.account_id().to_string(),
                        instance_id: instance_id.clone(),
                        client_order_id: client_order_id.to_string(),
                        order_id: anomaly.order_id.clone(),
                        token_id: identity.asset_id.clone(),
                        side: identity.side,
                        quantity,
                        filled_quantity: 0.0,
                        terminal_matched_quantity: None,
                        terminal_trade_ids: Vec::new(),
                        terminal_trade_ids_authoritative: false,
                        price: identity.price,
                        fee_rate_bps,
                        reserved_cash: reserve_cash,
                        reserved_quantity: reserve_quantity,
                        status: OrderStatus::Pending,
                    };
                    let ownership = self
                        .shared
                        .account_state
                        .backfill_order_ownership(&ownership)?;
                    self.shared
                        .install_runtime_order_id(
                            client_order_id,
                            &anomaly.order_id,
                            &identity.asset_id,
                            Some(&ownership),
                        )
                        .ok()?;
                    Some((client_order_id.to_string(), instance_id, identity.clone()))
                });

            if let Some((client_order_id, instance_id, identity)) = rebuilt {
                if let Some(status) = terminal_status {
                    self.shared.commit_authoritative_terminal_audit(
                        &client_order_id,
                        status,
                        &fetched.audit,
                    );
                    if !fetched.audit.associate_trades.is_empty() {
                        let _ = self.reconcile_orphans(&[], &[], &fetched.audit.associate_trades);
                    }
                    if self
                        .shared
                        .account_state
                        .terminal_order_audit_complete(&client_order_id)
                    {
                        self.shared
                            .remove_order_resolved_as(&client_order_id, status);
                    }
                } else {
                    if let Err(error) = self.shared.track_open_order(
                        &client_order_id,
                        TrackedOrder {
                            order_slot: Default::default(),
                            symbol: identity.asset_id,
                            side: identity.side,
                            instance_id,
                        },
                    ) {
                        warn!("[PolymarketTrade] startup open-order publish failed: {error}");
                        continue;
                    }
                    self.shared
                        .account_state
                        .begin_order_recovery([client_order_id.as_str()]);
                }
                summary.rebuilt = summary.rebuilt.saturating_add(1);
                info!(
                    "[PolymarketTrade] startup orphan ownership repaired oid={} coid={} status={}",
                    anomaly.order_id, client_order_id, fetched.status,
                );
                continue;
            }

            let zero_fill_terminal = terminal_status.zip(original_size).zip(size_matched).filter(
                |((status, _), matched)| {
                    matches!(status, OrderStatus::Cancelled | OrderStatus::Rejected)
                        && matched.abs() <= 1e-9
                        && fetched.audit.associate_trades.is_empty()
                },
            );
            if let Some(((status, quantity), matched)) = zero_fill_terminal {
                let evidence = format!(
                    "authenticated /data/order terminal status={} size_matched=0 associate_trades=[]",
                    fetched.status,
                );
                if self
                    .shared
                    .account_state
                    .record_terminal_orphan_order_audit(
                        &anomaly.order_id,
                        anomaly.client_order_id.as_deref(),
                        status,
                        quantity,
                        matched,
                        &fetched.audit.associate_trades,
                        &evidence,
                    )
                {
                    summary.tombstoned = summary.tombstoned.saturating_add(1);
                    info!(
                        "[PolymarketTrade] startup orphan ownership retired by zero-fill audit oid={} coid={}",
                        anomaly.order_id,
                        anomaly.client_order_id.as_deref().unwrap_or("<unknown>"),
                    );
                    continue;
                }
            }
            let failure = if terminal_status.is_some()
                && (size_matched.is_some_and(|matched| matched.abs() > 1e-9)
                    || !fetched.audit.associate_trades.is_empty())
            {
                OrphanOrderRepairFailure::TerminalOrderHasFills
            } else if anomaly.client_order_id.is_none() {
                OrphanOrderRepairFailure::MissingClientOrderId
            } else if original_size.is_none() {
                OrphanOrderRepairFailure::InvalidOriginalSize
            } else if fetched.rebuild_identity.is_none() {
                OrphanOrderRepairFailure::MissingRebuildIdentity
            } else if fetched.rebuild_identity.as_ref().is_some_and(|identity| {
                !identity
                    .maker_address
                    .eq_ignore_ascii_case(&self.shared.order_maker_address)
            }) {
                OrphanOrderRepairFailure::MakerMismatch
            } else if anomaly
                .client_order_id
                .as_deref()
                .is_some_and(|client_order_id| {
                    self.shared
                        .account_state
                        .instance_id_for_historical_coid(client_order_id)
                        .is_none()
                })
            {
                OrphanOrderRepairFailure::UnknownInstance
            } else if terminal_status.is_none() {
                OrphanOrderRepairFailure::NonTerminalOrder
            } else if zero_fill_terminal.is_some() {
                OrphanOrderRepairFailure::TombstoneRejected
            } else {
                OrphanOrderRepairFailure::OwnershipBackfillRejected
            };
            summary.unresolved(failure);
        }
        summary
    }

    pub fn reconcile_recovered_orders(&self) -> usize {
        self.reconcile_recovered_orders_with_updates().0
    }

    /// Runtime variant that also returns the replayed private-trade updates so
    /// active strategy workers receive the same inventory edges as the account
    /// ledger. Startup callers may use `reconcile_recovered_orders()` and seed
    /// strategy state from the recovered virtual snapshot instead.
    pub fn reconcile_recovered_orders_with_updates(&self) -> (usize, Vec<OrderUpdate>) {
        let _reconcile_stage =
            crate::latency::TimedStage::new("polymarket.recovery.reconcile_recovered_orders");
        // Keep a single pass sub-second. The account worker is now woken on
        // pending-audit edges and retries every 500 ms, so multi-second sleeps
        // here would only serialize unrelated orders behind one slow replica.
        const RETRY_DELAYS_MS: &[u64] = &[0, 100, 250, 500];
        let mut replayed_updates = Vec::new();

        for (attempt, delay_ms) in RETRY_DELAYS_MS.iter().copied().enumerate() {
            if delay_ms > 0 {
                std::thread::sleep(Duration::from_millis(delay_ms));
            }
            let mut unavailable_count = 0usize;
            let mut unavailable_samples = Vec::new();
            let mut json_null_count = 0usize;
            let pending: Vec<(String, String, OrderOwnership, bool, bool)> = {
                let coids = self.shared.account_state.pending_order_audit_ids();
                let recovered: HashSet<String> = self
                    .shared
                    .account_state
                    .recovery_pending_order_ids()
                    .into_iter()
                    .collect();
                let query_repairs: HashSet<String> = self
                    .shared
                    .account_state
                    .startup_query_repair_pending_order_ids()
                    .into_iter()
                    .collect();
                let execution = self.shared.execution_snapshot();
                let ids = &execution.coid_to_oid;
                coids
                    .into_iter()
                    .filter_map(|coid| {
                        let ownership = self.shared.account_state.order(&coid)?;
                        let is_recovered = recovered.contains(&coid);
                        let requires_query_repair = query_repairs.contains(&coid);
                        ids.get(&coid).map(|oid| {
                            (
                                coid,
                                oid.clone(),
                                ownership,
                                is_recovered,
                                requires_query_repair,
                            )
                        })
                    })
                    .collect()
            };
            if pending.is_empty() {
                // A durable pending row may temporarily lack an executor OID
                // binding, so preserve the old unresolved result without
                // materializing the aggregate account under control_gate.
                let unresolved = self.shared.account_state.pending_order_audit_counts().0;
                return (unresolved, replayed_updates);
            }

            for (coid, order_id, ownership, is_recovered, requires_query_repair) in pending {
                let event_has_ended =
                    if should_lookup_recovered_event_end(is_recovered, requires_query_repair) {
                        let event_end_started = crate::latency::Instant::now();
                        let ended = self
                            .shared
                            .account_state
                            .token_event_has_ended(&ownership.token_id);
                        crate::latency::record(
                            "polymarket.recovery.event_end_lookup",
                            event_end_started,
                        );
                        ended
                    } else {
                        false
                    };
                if let Some(reason) = prequery_recovered_order_close_reason(
                    is_recovered,
                    event_has_ended,
                    requires_query_repair,
                ) {
                    replayed_updates.push(self.close_recovered_order(
                        &ownership,
                        &order_id,
                        reason.as_str(),
                    ));
                    continue;
                }
                if can_reuse_persisted_terminal_audit(
                    ownership.terminal_trade_ids_authoritative,
                    requires_query_repair,
                ) {
                    let terminal_status = if ownership.status == OrderStatus::Cancelled {
                        OrderStatus::Cancelled
                    } else {
                        OrderStatus::Filled
                    };
                    let audit = AuthoritativeOrderAudit {
                        original_size: Some(ownership.quantity.to_string()),
                        size_matched: ownership
                            .terminal_matched_quantity
                            .map(|quantity| quantity.to_string()),
                        associate_trades: ownership.terminal_trade_ids.clone(),
                    };
                    replayed_updates.push(Self::authoritative_recovery_update(
                        &ownership,
                        &order_id,
                        terminal_status,
                        audit,
                    ));
                    if !ownership.terminal_trade_ids.is_empty() {
                        replayed_updates.extend(self.reconcile_orphans(
                            &[],
                            &[],
                            &ownership.terminal_trade_ids,
                        ));
                    }
                    if self
                        .shared
                        .account_state
                        .terminal_order_audit_complete(&coid)
                    {
                        self.shared.remove_order_resolved_as(&coid, terminal_status);
                    }
                    // The complete order audit is already durable. Whether
                    // trades resolved or remain temporarily unavailable, do
                    // not issue another order GET for identical metadata.
                    continue;
                }
                match self.fetch_order_by_id(&coid, &order_id, None, false) {
                    FetchOrderResult::Found(order) => {
                        match order.status.as_str() {
                            "LIVE" => {
                                // A runtime Filled edge can race a stale order
                                // lookup. Once the durable ledger has observed
                                // Filled, never regress it to LIVE or release
                                // its reservation through a later cancel ACK;
                                // wait for the associated trade audit instead.
                                if ownership.status == OrderStatus::Filled {
                                    warn!(
                                        "[PolymarketTrade] trade-audit coid={} orderID={} returned stale LIVE; retaining Filled reservation",
                                        coid, order_id,
                                    );
                                    continue;
                                }
                                if !order.audit.associate_trades.is_empty() {
                                    replayed_updates.extend(self.reconcile_orphans(
                                        &[], &[], &order.audit.associate_trades,
                                    ));
                                }
                                let body = serde_json::json!({ "orderID": order_id });
                                match self.delete_detailed("/order", &body) {
                                    Ok(response) => match cancel_delete_response_outcome(
                                        &response,
                                        &order_id,
                                    ) {
                                        CancelReasonOutcome::Cancelled => {
                                            self.shared.remove_order_as(&coid, OrderStatus::Cancelled);
                                        }
                                        // A matched response is not enough to
                                        // release ownership; the next lookup
                                        // must expose its complete trade audit.
                                        CancelReasonOutcome::Filled
                                        | CancelReasonOutcome::Uncertain => {}
                                    },
                                    Err(error) => warn!(
                                        "[PolymarketTrade] startup recovery DELETE coid={} orderID={} failed: {}",
                                        coid, order_id, error,
                                    ),
                                }
                            }
                            "MATCHED" | "MATCHED_NOT_BROADCASTED" | "FILLED" => {
                                let trade_ids = order.audit.associate_trades.clone();
                                self.shared.commit_authoritative_terminal_audit(
                                    &coid,
                                    OrderStatus::Filled,
                                    &order.audit,
                                );
                                replayed_updates.push(Self::authoritative_recovery_update(
                                    &ownership,
                                    &order_id,
                                    OrderStatus::Filled,
                                    order.audit.clone(),
                                ));
                                if !trade_ids.is_empty() {
                                    replayed_updates.extend(
                                        self.reconcile_orphans(&[], &[], &trade_ids),
                                    );
                                }
                                if self.shared.account_state.terminal_order_audit_complete(&coid) {
                                    self.shared.complete_filled_order_audit(&coid);
                                }
                            }
                            status if status.starts_with("CANCELED")
                                || status.starts_with("CANCELLED") => {
                                self.shared.commit_authoritative_terminal_audit(
                                    &coid,
                                    OrderStatus::Cancelled,
                                    &order.audit,
                                );
                                replayed_updates.push(Self::authoritative_recovery_update(
                                    &ownership,
                                    &order_id,
                                    OrderStatus::Cancelled,
                                    order.audit.clone(),
                                ));
                                if !order.audit.associate_trades.is_empty() {
                                    replayed_updates.extend(self.reconcile_orphans(
                                        &[], &[], &order.audit.associate_trades,
                                    ));
                                }
                                if self.shared.account_state.terminal_order_audit_complete(&coid) {
                                    self.shared.remove_order_resolved_as(
                                        &coid,
                                        OrderStatus::Cancelled,
                                    );
                                }
                            }
                            "INVALID" => {
                                self.shared.remove_order_as(&coid, OrderStatus::Rejected);
                            }
                            other => warn!(
                                "[PolymarketTrade] startup recovery coid={} orderID={} unexpected status={} attempt={}",
                                coid, order_id, other, attempt + 1,
                            ),
                        }
                    }
                    FetchOrderResult::NotFound(evidence) => {
                        if requires_query_repair
                            && event_has_ended
                            && attempt + 1 == RETRY_DELAYS_MS.len()
                        {
                            warn!(
                                "[PolymarketTrade] startup query repair coid={} orderID={} not found after {} attempts evidence={} and event ended — closing recovered order",
                                coid,
                                order_id,
                                RETRY_DELAYS_MS.len(),
                                evidence,
                            );
                            replayed_updates.push(self.close_recovered_order(
                                &ownership,
                                &order_id,
                                RecoveredOrderCloseReason::EventEnded.as_str(),
                            ));
                        } else {
                            warn!(
                                "[PolymarketTrade] startup recovery coid={} orderID={} not found attempt={} evidence={} — retaining reservation",
                                coid, order_id, attempt + 1, evidence,
                            );
                        }
                    }
                    FetchOrderResult::Unavailable(kind)
                        if recovered_order_close_reason(is_recovered, false, Some(&kind))
                            == Some(RecoveredOrderCloseReason::JsonNull) =>
                    {
                        replayed_updates.push(self.close_recovered_order(
                            &ownership,
                            &order_id,
                            RecoveredOrderCloseReason::JsonNull.as_str(),
                        ));
                    }
                    FetchOrderResult::Unavailable(kind) => {
                        if fetch_unavailable_should_warn(&kind) {
                            unavailable_count = unavailable_count.saturating_add(1);
                            if unavailable_samples.len() < 3 {
                                unavailable_samples.push(format!("{}:{:?}", coid, kind));
                            }
                        } else {
                            json_null_count = json_null_count.saturating_add(1);
                        }
                        debug!(
                            "[PolymarketTrade] startup recovery coid={} orderID={} unavailable={:?} attempt={}/{} — retaining reservation",
                            coid, order_id, kind, attempt + 1, RETRY_DELAYS_MS.len(),
                        );
                    }
                }
            }
            if json_null_count > 0 {
                debug!(
                    "[PolymarketTrade] startup recovery order lookup returned JSON null attempt={}/{} orders={} — eventual consistency; retaining reservations",
                    attempt + 1,
                    RETRY_DELAYS_MS.len(),
                    json_null_count,
                );
            }
            if unavailable_count > 0 {
                let should_warn = startup_recovery_warn_attempt(attempt, RETRY_DELAYS_MS.len())
                    && claim_rate_limited_warning(
                        &self.shared.startup_recovery_lookup_warn_silent_until_ns,
                        crate::types::now_ns(),
                    );
                if should_warn {
                    warn!(
                        "[PolymarketTrade] startup recovery lookup unavailable attempt={}/{} orders={} samples={:?} — retaining reservations warn_interval_s={}",
                        attempt + 1,
                        RETRY_DELAYS_MS.len(),
                        unavailable_count,
                        unavailable_samples,
                        EXPECTED_ORPHAN_WARN_INTERVAL_NS / 1_000_000_000,
                    );
                } else {
                    debug!(
                        "[PolymarketTrade] startup recovery lookup unavailable attempt={}/{} orders={} samples={:?}",
                        attempt + 1,
                        RETRY_DELAYS_MS.len(),
                        unavailable_count,
                        unavailable_samples,
                    );
                }
            }
            let (recovery_pending, routine_cancel_audits) =
                self.shared.account_state.pending_order_audit_counts();
            if startup_recovery_audits_complete(recovery_pending, routine_cancel_audits) {
                return (recovery_pending, replayed_updates);
            }
        }

        let unresolved = self.shared.account_state.pending_order_audit_counts().0;
        if unresolved > 0 {
            warn!(
                "[PolymarketTrade] startup recovery left {} order(s) awaiting exact private-trade replay",
                unresolved,
            );
        }
        (unresolved, replayed_updates)
    }

    /// Close only an order that is already in durable recovery. Event-end and
    /// literal-null evidence prove that it is no longer open, but do not
    /// rewrite already-observed fills or invent a terminal match quantity.
    fn close_recovered_order(
        &self,
        ownership: &OrderOwnership,
        order_id: &str,
        reason: &str,
    ) -> OrderUpdate {
        let terminal_status = match ownership.status {
            OrderStatus::Filled => OrderStatus::Filled,
            OrderStatus::Rejected => OrderStatus::Rejected,
            _ => OrderStatus::Cancelled,
        };
        self.shared
            .remove_order_resolved_as(&ownership.client_order_id, terminal_status);
        info!(
            "[PolymarketTrade] recovered order closed coid={} orderID={} token={} status={:?} reason={} — released residual reservation",
            ownership.client_order_id,
            order_id,
            ownership.token_id,
            terminal_status,
            reason,
        );
        OrderUpdate {
            order_slot: Default::default(),
            client_order_id: ownership.client_order_id.clone(),
            exchange: Exchange::Polymarket,
            symbol: ownership.token_id.clone(),
            side: ownership.side,
            exchange_order_id: Some(order_id.to_string()),
            status: terminal_status,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: if terminal_status == OrderStatus::Filled {
                0.0
            } else {
                (ownership.quantity - ownership.filled_quantity).max(0.0)
            },
            avg_fill_price: ownership.price,
            timestamp_ns: now_ns(),
            exchange_event_timestamp_ns: None,
            trade_id: None,
            order_audit: None,
            error: None,
        }
    }

    fn authoritative_recovery_update(
        ownership: &OrderOwnership,
        order_id: &str,
        status: OrderStatus,
        audit: AuthoritativeOrderAudit,
    ) -> OrderUpdate {
        let matched = audit
            .size_matched
            .as_deref()
            .and_then(|value| value.parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value >= 0.0)
            .unwrap_or(ownership.filled_quantity);
        OrderUpdate {
            order_slot: Default::default(),
            client_order_id: ownership.client_order_id.clone(),
            exchange: Exchange::Polymarket,
            symbol: ownership.token_id.clone(),
            side: ownership.side,
            exchange_order_id: Some(order_id.to_string()),
            status,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: (ownership.quantity - matched).max(0.0),
            avg_fill_price: ownership.price,
            timestamp_ns: now_ns(),
            exchange_event_timestamp_ns: None,
            trade_id: None,
            order_audit: Some(audit),
            error: Some(ORPHAN_RECONCILE_AUTHORITATIVE_TERMINAL.to_string()),
        }
    }

    /// Audit every locally live or cancellation-pending order after a user
    /// feed reconnect, including associated trades missed by the stream.
    pub(crate) fn reconcile_runtime_open_orders_with_updates(
        &self,
    ) -> std::result::Result<Vec<OrderUpdate>, String> {
        let pass = self.reconcile_runtime_open_orders_pass();
        if pass.errors.is_empty() {
            Ok(pass.updates)
        } else {
            Err(pass.errors.join("; "))
        }
    }

    /// Run a complete runtime order audit without discarding successful rows
    /// when one sibling order is temporarily unavailable. Shutdown uses the
    /// partial updates to converge accounting on every retry.
    fn reconcile_runtime_open_orders_pass(&self) -> RuntimeOrderAuditPass {
        self.reconcile_runtime_orders_pass(None)
    }

    /// Event-expiry counterpart of the account-wide audit. Only orders whose
    /// durable token belongs to `tokens` participate, so a next event trading
    /// concurrently on the same wallet cannot keep the previous event's
    /// settlement barrier open.
    fn reconcile_runtime_orders_for_tokens_pass(
        &self,
        tokens: &HashSet<String>,
    ) -> RuntimeOrderAuditPass {
        self.reconcile_runtime_orders_pass(Some(tokens))
    }

    /// The venue has retired the event, so the residual cannot still rest.
    /// Preserve coid/oid/token mappings for late trade attribution while
    /// releasing active reservation and strategy orphan state.
    fn close_retired_market_order(
        &self,
        missing: &RuntimeMissingOrder,
        market_condition_id: &str,
    ) -> Option<OrderUpdate> {
        let ownership = self.shared.account_state.order(&missing.client_order_id)?;
        let durable_status = if ownership.status == OrderStatus::Filled {
            OrderStatus::Filled
        } else {
            OrderStatus::Cancelled
        };
        self.shared
            .remove_order_resolved_as(&missing.client_order_id, durable_status);
        warn!(
            "[orphan_metric] retired_market_order_terminal=1 market={} coid={} orderID={} token={} prior_status={:?} durable_status={:?} evidence={} terminal=Cancelled lock_release=allowed late_trade_mapping=retained",
            market_condition_id,
            missing.client_order_id,
            missing.order_id,
            ownership.token_id,
            ownership.status,
            durable_status,
            missing.evidence,
        );
        Some(OrderUpdate {
            order_slot: Default::default(),
            client_order_id: missing.client_order_id.clone(),
            exchange: Exchange::Polymarket,
            symbol: missing.tracked.symbol.clone(),
            side: missing.tracked.side,
            exchange_order_id: Some(missing.order_id.clone()),
            status: OrderStatus::Cancelled,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: (ownership.quantity - ownership.filled_quantity).max(0.0),
            avg_fill_price: ownership.price,
            timestamp_ns: now_ns(),
            exchange_event_timestamp_ns: None,
            trade_id: None,
            order_audit: None,
            error: Some(ORPHAN_RECONCILE_AUTHORITATIVE_TERMINAL.to_string()),
        })
    }

    fn reconcile_runtime_orders_pass(
        &self,
        token_filter: Option<&HashSet<String>>,
    ) -> RuntimeOrderAuditPass {
        let tracked: Vec<(String, TrackedOrder, String)> = {
            let execution = self.shared.execution_snapshot();
            let open = &execution.open_orders;
            let ids = &execution.coid_to_oid;
            let mut rows: HashMap<String, (TrackedOrder, String)> = open
                .iter()
                .filter(|(_, tracked)| {
                    token_filter.is_none_or(|tokens| tokens.contains(&tracked.symbol))
                })
                .filter_map(|(coid, tracked)| {
                    ids.get(coid)
                        .map(|oid| (coid.clone(), (tracked.clone(), oid.clone())))
                })
                .collect();
            for coid in self.shared.account_state.pending_order_audit_ids() {
                if rows.contains_key(&coid) {
                    continue;
                }
                let Some(order) = self.shared.account_state.order(&coid) else {
                    continue;
                };
                if token_filter.is_some_and(|tokens| !tokens.contains(&order.token_id)) {
                    continue;
                }
                let Some(order_id) = ids.get(&coid) else {
                    continue;
                };
                rows.insert(
                    coid,
                    (
                        TrackedOrder {
                            order_slot: Default::default(),
                            symbol: order.token_id,
                            side: order.side,
                            instance_id: order.instance_id,
                        },
                        order_id.clone(),
                    ),
                );
            }
            rows.into_iter()
                .map(|(coid, (tracked, oid))| (coid, tracked, oid))
                .collect()
        };
        let mut updates = Vec::new();
        let mut errors = Vec::new();
        let mut not_found = Vec::new();
        let mut retired_market_absent = Vec::new();
        let recovered: HashSet<String> = self
            .shared
            .account_state
            .recovery_pending_order_ids()
            .into_iter()
            .collect();
        for (coid, tracked, order_id) in tracked {
            let ownership = match self.shared.account_state.order(&coid) {
                Some(ownership) => ownership,
                None => {
                    errors.push(format!(
                        "open order coid={coid} has no durable ownership row"
                    ));
                    continue;
                }
            };
            let is_recovered = recovered.contains(&coid);
            if let Some(reason) = recovered_order_close_reason(
                is_recovered,
                self.shared
                    .account_state
                    .token_event_has_ended(&ownership.token_id),
                None,
            ) {
                updates.push(self.close_recovered_order(&ownership, &order_id, reason.as_str()));
                continue;
            }
            let parallel_evidence = ownership.status == OrderStatus::NewOrderTimeout
                || self
                    .shared
                    .account_state
                    .token_event_has_ended(&ownership.token_id);
            let fetched = match self.fetch_order_by_id(
                &coid,
                &order_id,
                None,
                parallel_evidence,
            ) {
                FetchOrderResult::Found(order) => order,
                FetchOrderResult::NotFound(evidence) => {
                    errors.push(format!(
                        "open order coid={coid} orderID={order_id} was not found: {evidence}"
                    ));
                    let missing = RuntimeMissingOrder {
                        client_order_id: coid,
                        tracked,
                        order_id,
                        evidence,
                    };
                    not_found.push(missing.clone());
                    retired_market_absent.push(missing);
                    continue;
                }
                FetchOrderResult::Unavailable(kind)
                    if recovered_order_close_reason(is_recovered, false, Some(&kind))
                        == Some(RecoveredOrderCloseReason::JsonNull) =>
                {
                    updates.push(self.close_recovered_order(
                        &ownership,
                        &order_id,
                        RecoveredOrderCloseReason::JsonNull.as_str(),
                    ));
                    continue;
                }
                FetchOrderResult::Unavailable(kind) => {
                    errors.push(format!(
                        "open order coid={coid} orderID={order_id} audit unavailable: {kind:?}"
                    ));
                    if kind.is_json_null() {
                        retired_market_absent.push(RuntimeMissingOrder {
                            client_order_id: coid,
                            tracked,
                            order_id,
                            evidence: if kind.is_parallel_absence() {
                                "parallel_reconcile_absence_json_null".to_string()
                            } else {
                                "single_order_lookup_json_null".to_string()
                            },
                        });
                    }
                    continue;
                }
            };
            let (effective_size_matched, has_valid_size_matched) = effective_audited_match(
                fetched.audit.size_matched.as_deref(),
                ownership.quantity,
                ownership.filled_quantity,
            );
            let status_text = fetched.status.to_ascii_uppercase();
            let status = match status_text.as_str() {
                "LIVE" => {
                    if !fetched.audit.associate_trades.is_empty() {
                        updates.extend(self.reconcile_orphans(
                            &[],
                            &[],
                            &fetched.audit.associate_trades,
                        ));
                    }
                    if !has_valid_size_matched {
                        errors.push(format!(
                            "live order coid={coid} omitted or has invalid size_matched; preserving local filled_quantity={}",
                            ownership.filled_quantity,
                        ));
                        // Do not manufacture a lifecycle update from an
                        // incomplete LIVE response. The next strict GET pass
                        // remains the convergence path for the schema anomaly.
                        continue;
                    }
                    let candidate = if effective_size_matched > 1e-9 {
                        OrderStatus::PartiallyFilled
                    } else {
                        OrderStatus::Accepted
                    };
                    let live = self
                        .shared
                        .mark_order_live(
                            &coid,
                            tracked.order_slot,
                            &tracked.symbol,
                            tracked.side,
                            &tracked.instance_id,
                            candidate,
                        )
                        .unwrap_or(candidate);
                    live
                }
                "MATCHED" | "MATCHED_NOT_BROADCASTED" | "FILLED" => {
                    self.shared.commit_authoritative_terminal_audit(
                        &coid,
                        OrderStatus::Filled,
                        &fetched.audit,
                    );
                    if !fetched.audit.associate_trades.is_empty() {
                        updates.extend(self.reconcile_orphans(
                            &[],
                            &[],
                            &fetched.audit.associate_trades,
                        ));
                    }
                    if self
                        .shared
                        .account_state
                        .terminal_order_audit_complete(&coid)
                    {
                        self.shared.complete_filled_order_audit(&coid);
                    }
                    OrderStatus::Filled
                }
                value if value.starts_with("CANCELED") || value.starts_with("CANCELLED") => {
                    self.shared.commit_authoritative_terminal_audit(
                        &coid,
                        OrderStatus::Cancelled,
                        &fetched.audit,
                    );
                    if !fetched.audit.associate_trades.is_empty() {
                        updates.extend(self.reconcile_orphans(
                            &[],
                            &[],
                            &fetched.audit.associate_trades,
                        ));
                    }
                    let _matched = match fetched
                        .audit
                        .size_matched
                        .as_deref()
                        .and_then(|value| value.parse::<f64>().ok())
                        .filter(|value| value.is_finite() && *value >= 0.0)
                    {
                        Some(matched) => matched,
                        None => {
                            errors.push(format!(
                                "cancelled order coid={coid} omitted or has invalid size_matched"
                            ));
                            continue;
                        }
                    };
                    if self
                        .shared
                        .account_state
                        .terminal_order_audit_complete(&coid)
                    {
                        self.shared
                            .remove_order_resolved_as(&coid, OrderStatus::Cancelled);
                    }
                    OrderStatus::Cancelled
                }
                "INVALID" => {
                    self.shared.remove_order_as(&coid, OrderStatus::Rejected);
                    OrderStatus::Rejected
                }
                other => {
                    errors.push(format!(
                        "open order coid={coid} returned unsupported status={other}"
                    ));
                    continue;
                }
            };
            if matches!(status, OrderStatus::Accepted | OrderStatus::PartiallyFilled) {
                self.shared.account_state.finish_order_recovery(&coid);
            }
            self.shared
                .account_state
                .resolve_private_event_anomaly(&format!("order:{}", normalize_order_id(&order_id)));
            updates.push(OrderUpdate {
                order_slot: Default::default(),
                client_order_id: coid,
                exchange: Exchange::Polymarket,
                symbol: tracked.symbol,
                side: tracked.side,
                exchange_order_id: Some(order_id.clone()),
                status,
                liquidity: None,
                filled_quantity: 0.0,
                remaining_quantity: (ownership.quantity - effective_size_matched).max(0.0),
                avg_fill_price: ownership.price,
                timestamp_ns: now_ns(),
                exchange_event_timestamp_ns: None,
                trade_id: None,
                order_audit: Some(fetched.audit),
                error: None,
            });
        }
        RuntimeOrderAuditPass {
            updates,
            errors,
            not_found,
            retired_market_absent,
        }
    }

    /// POST variant that distinguishes timeout / status / other errors so
    /// callers can return `OrderStatus::NewOrderTimeout` vs `Rejected`
    /// appropriately. Dispatched through the shared HTTP worker pool.
    fn post_detailed(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> std::result::Result<serde_json::Value, HttpErr> {
        let body_str = body.to_string();
        self.shared.http_call_sync("POST", path, &body_str)
    }

    /// Cancel ALL open orders on the CLOB (DELETE /cancel-all, no body).
    pub fn cancel_all_orders(&self) {
        self.cancel_all_orders_inner(None);
    }

    /// Physical-owner variant used by the independent safety-cancel lane.
    /// Every request stays on the connection reserved by `permit`.
    pub fn cancel_all_orders_on(&self, permit: &crate::http1_pool::Permit) {
        self.cancel_all_orders_inner(Some(permit));
    }

    fn cancel_all_orders_inner(&self, permit: Option<&crate::http1_pool::Permit>) {
        let tracked: Vec<(String, String)> = {
            let execution = self.shared.execution_snapshot();
            let coids: Vec<String> = execution.open_orders.keys().cloned().collect();
            let ids = &execution.coid_to_oid;
            coids
                .into_iter()
                .filter_map(|coid| ids.get(&coid).map(|oid| (coid, oid.clone())))
                .collect()
        };
        let res = match permit {
            Some(permit) => self.shared.http_call_sync_on(
                permit.current_pooled_client(),
                "DELETE",
                "/cancel-all",
                "",
            ),
            None => self.shared.http_call_sync("DELETE", "/cancel-all", ""),
        };
        match res {
            Ok(json) => {
                let canceled = json
                    .get("canceled")
                    .and_then(|v| v.as_array())
                    .map(|a| a.len())
                    .unwrap_or(0);
                let not_canceled = json
                    .get("not_canceled")
                    .and_then(|v| v.as_object())
                    .map(|o| o.len())
                    .unwrap_or(0);
                info!(
                    "[PolymarketTrade] Cancel-all: {} canceled, {} failed",
                    canceled, not_canceled
                );
                for (coid, order_id) in &tracked {
                    match cancel_delete_response_outcome(&json, order_id) {
                        CancelReasonOutcome::Cancelled => {
                            self.shared.remove_order_as(coid, OrderStatus::Cancelled);
                        }
                        CancelReasonOutcome::Filled => {
                            self.shared.remove_order_as(coid, OrderStatus::Filled);
                        }
                        CancelReasonOutcome::Uncertain => {
                            self.shared
                                .defer_lifecycle_account_apply(coid, OrderStatus::CancelUncertain);
                            warn!(
                                "[PolymarketTrade] Cancel-all left coid={} orderID={} ambiguous — retaining reservation",
                                coid, order_id,
                            );
                        }
                    }
                }
            }
            Err(e) => {
                for (coid, _) in &tracked {
                    self.shared
                        .defer_lifecycle_account_apply(coid, OrderStatus::CancelOrderTimeout);
                }
                warn!(
                    "[PolymarketTrade] Cancel-all failed: {} — retaining {} order reservations",
                    e,
                    tracked.len(),
                );
            }
        }
    }

    /// Graceful-shutdown cancellation barrier. The caller must first stop all
    /// order-producing dispatchers. This deliberately has no fixed retry
    /// limit: exiting while the remote account may still have LIVE orders is
    /// less safe than remaining visibly in a risk-off shutdown state.
    ///
    /// A pass is final only when `/cancel-all` has the complete expected
    /// schema with no failures, all locally known orders were audited (with
    /// associated trades replayed, except the recovery-only ended-event/null
    /// closure rule), and both runtime and durable pending-order registries are
    /// empty.
    pub fn cancel_all_orders_until_final(&self, mut emit_update: impl FnMut(OrderUpdate)) -> usize {
        let retry_delays_ms = [0_u64, 100, 250, 500, 1_000, 2_000, 5_000];
        let mut attempt = 0usize;
        let mut remote_clean_not_found_streaks: HashMap<String, u32> = HashMap::new();
        loop {
            let delay_ms = retry_delays_ms[attempt.min(retry_delays_ms.len() - 1)];
            if delay_ms > 0 {
                std::thread::sleep(Duration::from_millis(delay_ms));
            }
            attempt = attempt.saturating_add(1);

            let remote_result = self.shared.http_call_sync("DELETE", "/cancel-all", "");
            let (remote_clean, remote_detail) = match remote_result {
                Ok(json) => match validated_cancel_all_counts(&json) {
                    Some((canceled, not_canceled)) => (
                        not_canceled == 0,
                        format!("canceled={canceled} not_canceled={not_canceled}"),
                    ),
                    None => (
                        false,
                        "invalid response schema (expected canceled[] and not_canceled{})"
                            .to_string(),
                    ),
                },
                Err(error) => (false, format!("request failed: {error}")),
            };

            let audit = self.reconcile_runtime_open_orders_pass();
            for update in audit.updates {
                emit_update(update);
            }

            // Historical deterministic rejects may have been persisted as
            // placement orphans by older binaries. During shutdown, a clean
            // account-wide cancel plus four consecutive authoritative GET
            // absences is sufficient to release only those phantom rows.
            // Unavailable/malformed lookups never enter this list and reset
            // the run, so a service outage cannot manufacture a rejection.
            let absent_coids: HashSet<&str> = audit
                .not_found
                .iter()
                .map(|missing| missing.client_order_id.as_str())
                .collect();
            if remote_clean {
                remote_clean_not_found_streaks
                    .retain(|coid, _| absent_coids.contains(coid.as_str()));
            } else {
                remote_clean_not_found_streaks.clear();
            }
            if remote_clean {
                for missing in &audit.not_found {
                    let streak = remote_clean_not_found_streaks
                        .entry(missing.client_order_id.clone())
                        .or_insert(0);
                    *streak = streak.saturating_add(1);
                    let Some(ownership) = self.shared.account_state.order(&missing.client_order_id)
                    else {
                        warn!(
                            "[PolymarketTrade] shutdown phantom coid={} orderID={} has no durable ownership row; retaining for diagnosis",
                            missing.client_order_id, missing.order_id,
                        );
                        continue;
                    };
                    if !shutdown_absent_placement_phantom_is_terminal(ownership.status, *streak) {
                        if *streak == RECONCILE_NOT_FOUND_RETRY_LIMIT {
                            warn!(
                                "[PolymarketTrade] shutdown absent order is not a placement phantom: coid={} orderID={} status={:?}; retaining for trade/cancel audit",
                                missing.client_order_id, missing.order_id, ownership.status,
                            );
                        }
                        continue;
                    }
                    self.shared
                        .remove_order_as(&missing.client_order_id, OrderStatus::Rejected);
                    warn!(
                        "[PolymarketTrade] shutdown resolved absent placement phantom as Rejected: coid={} orderID={} consecutive_remote_clean_not_found={} evidence={}",
                        missing.client_order_id,
                        missing.order_id,
                        streak,
                        missing.evidence,
                    );
                    emit_update(OrderUpdate {
                        order_slot: Default::default(),
                        client_order_id: missing.client_order_id.clone(),
                        exchange: Exchange::Polymarket,
                        symbol: missing.tracked.symbol.clone(),
                        side: missing.tracked.side,
                        exchange_order_id: Some(missing.order_id.clone()),
                        status: OrderStatus::Rejected,
                        liquidity: None,
                        filled_quantity: 0.0,
                        remaining_quantity: ownership.quantity,
                        avg_fill_price: ownership.price,
                        timestamp_ns: now_ns(),
                        exchange_event_timestamp_ns: None,
                        trade_id: None,
                        order_audit: None,
                        error: Some(format!(
                            "shutdown authoritative absence after {} clean cancel passes: {}",
                            streak, missing.evidence
                        )),
                    });
                }
            }
            let open_orders = self.shared.execution_snapshot().open_orders.len();
            let monitoring = self.shared.account_state.monitoring_snapshot();
            let recovery_pending = monitoring.recovery_pending_orders;
            let routine_cancel_audits = monitoring.routine_cancel_audits;
            let local_clean = audit.errors.is_empty()
                && open_orders == 0
                && recovery_pending == 0
                && routine_cancel_audits == 0;

            if remote_clean && local_clean {
                info!(
                    "[PolymarketTrade] shutdown cancel barrier final after {} attempt(s): {}",
                    attempt, remote_detail,
                );
                return attempt;
            }

            warn!(
                "[PolymarketTrade] shutdown cancel barrier retry attempt={} remote_clean={} remote={} open_orders={} recovery_pending={} routine_cancel_audits={} audit_errors={:?}",
                attempt,
                remote_clean,
                remote_detail,
                open_orders,
                recovery_pending,
                routine_cancel_audits,
                audit.errors,
            );
        }
    }

    pub fn cancel_instance_orders(&mut self) -> Result<Vec<OrderUpdate>> {
        if self.instance_id.is_empty() {
            return Err(anyhow!(
                "instance-scoped cancel requires a non-empty instance_id"
            ));
        }
        let execution = self.shared.execution_snapshot();
        let coids = instance_owned_open_coids(execution.open_orders.iter(), &self.instance_id);
        if coids.is_empty() {
            return Ok(Vec::new());
        }
        self.batch_cancel_orders(Exchange::Polymarket, "", &coids)
    }

    /// Physical-owner instance cancel. Requests are executed sequentially on
    /// the safety lane's reserved connection and completed on this same owner.
    pub fn cancel_instance_orders_on(
        &mut self,
        permit: &crate::http1_pool::Permit,
    ) -> Result<Vec<OrderUpdate>> {
        if self.instance_id.is_empty() {
            return Err(anyhow!(
                "instance-scoped cancel requires a non-empty instance_id"
            ));
        }
        let execution = self.shared.execution_snapshot();
        let coids = instance_owned_open_coids(execution.open_orders.iter(), &self.instance_id);
        drop(execution);
        let mut updates = Vec::with_capacity(coids.len());
        for client_order_id in coids {
            let pending = self.cancel_fire(&client_order_id, permit.current_pooled_client());
            updates.push(self.complete_cancel(Exchange::Polymarket, &client_order_id, pending));
        }
        Ok(updates)
    }

    /// Cancel every resting order for ONE market server-side via
    /// `DELETE /cancel-market-orders`. The endpoint requires BOTH `market`
    /// (condition_id) and `asset_id` (token_id) — they are both mandatory —
    /// so a binary market is **two calls**, one per outcome token; pass the
    /// market's `asset_ids` (e.g. `[up_token, down_token]`).
    ///
    /// Unlike `cancel_all(symbol)` — which only re-cancels orders still in
    /// our local `open_orders` map and therefore MISSES "forgotten" orders
    /// that were wrongly dropped from tracking — the server cancels by its
    /// own book, so this also kills orders we lost track of (e.g. a
    /// `pending/delayed` cancel race or a `matched`-then-FAILED trade) that
    /// would otherwise rest unmanaged to settlement. Scoped to a single
    /// `condition_id` so an account trading several markets concurrently
    /// keeps the others' orders intact — used as the event-expiry backstop.
    /// Routine expiry callers may treat repeated token-wide "cancels are
    /// disabled" responses as retired-market evidence, but finality still
    /// requires three complete passes and a clean local order/audit ledger.
    pub fn cancel_market_orders_until_final(
        &self,
        market_condition_id: &str,
        asset_ids: &[String],
        allow_expired_market_terminalization: bool,
    ) -> MarketCancelFinality {
        self.cancel_market_orders_until_final_inner(
            market_condition_id,
            asset_ids,
            allow_expired_market_terminalization,
            None,
        )
    }

    /// Physical-owner market cancel used by the safety lane.
    pub fn cancel_market_orders_until_final_on(
        &self,
        market_condition_id: &str,
        asset_ids: &[String],
        allow_expired_market_terminalization: bool,
        permit: &crate::http1_pool::Permit,
    ) -> MarketCancelFinality {
        self.cancel_market_orders_until_final_inner(
            market_condition_id,
            asset_ids,
            allow_expired_market_terminalization,
            Some(permit),
        )
    }

    fn cancel_market_orders_until_final_inner(
        &self,
        market_condition_id: &str,
        asset_ids: &[String],
        allow_expired_market_terminalization: bool,
        permit: Option<&crate::http1_pool::Permit>,
    ) -> MarketCancelFinality {
        let tokens: HashSet<String> = asset_ids
            .iter()
            .map(|token| token.trim())
            .filter(|token| !token.is_empty())
            .map(str::to_string)
            .collect();
        if market_condition_id.trim().is_empty() || tokens.len() != asset_ids.len() {
            return MarketCancelFinality {
                confirmed: false,
                updates: Vec::new(),
                detail:
                    "market cancel requires a non-empty condition id and distinct non-empty tokens"
                        .to_string(),
            };
        }

        // Keep one executor dispatch bounded; the strategy remains unsettled
        // and re-emits after a pending acknowledgement. This avoids monopolising
        // the shared execution worker during a venue outage while still making
        // settlement strictly dependent on an authoritative success.
        let retry_delays_ms = [0_u64, 100, 250];
        let mut all_updates = Vec::new();
        let mut last_detail = String::new();
        let mut retired_market_evidence_streak = 0usize;
        for (attempt_idx, delay_ms) in retry_delays_ms.into_iter().enumerate() {
            if delay_ms > 0 {
                std::thread::sleep(Duration::from_millis(delay_ms));
            }
            let mut remote_clean = true;
            let mut remote_details = Vec::new();
            let mut invalid_token_responses = 0usize;
            let mut cancels_disabled_responses = 0usize;
            for asset_id in &tokens {
                let body = serde_json::json!({
                    "market": market_condition_id,
                    "asset_id": asset_id,
                })
                .to_string();
                let reply = match permit {
                    Some(permit) => self.shared.http_call_sync_on(
                        permit.current_pooled_client(),
                        "DELETE",
                        "/cancel-market-orders",
                        &body,
                    ),
                    None => self
                        .shared
                        .http_call_sync("DELETE", "/cancel-market-orders", &body),
                };
                match reply {
                    Ok(json) => match validated_cancel_all_counts(&json) {
                        Some((canceled, not_canceled)) => {
                            remote_clean &= not_canceled == 0;
                            remote_details.push(format!(
                                "asset={asset_id} canceled={canceled} not_canceled={not_canceled}"
                            ));
                            if let Some(canceled_ids) =
                                json.get("canceled").and_then(|v| v.as_array())
                            {
                                let coids: Vec<String> = {
                                    let execution = self.shared.execution_snapshot();
                                    let oid_to_coid = &execution.oid_to_coid;
                                    canceled_ids
                                        .iter()
                                        .filter_map(|value| value.as_str())
                                        .filter_map(|oid| {
                                            oid_to_coid.get(&normalize_order_id(oid)).cloned()
                                        })
                                        .collect()
                                };
                                for coid in coids {
                                    self.shared.remove_order(&coid);
                                }
                            }
                        }
                        None => {
                            remote_clean = false;
                            remote_details
                                .push(format!("asset={asset_id} invalid response schema"));
                        }
                    },
                    Err(error) => {
                        remote_clean = false;
                        let error_text = error.to_string();
                        if SharedState::is_invalid_token_error(&error_text) {
                            invalid_token_responses = invalid_token_responses.saturating_add(1);
                        } else if is_retired_market_terminal_error(
                            &error_text,
                            allow_expired_market_terminalization,
                        ) {
                            cancels_disabled_responses =
                                cancels_disabled_responses.saturating_add(1);
                        }
                        remote_details.push(format!("asset={asset_id} request failed: {error}"));
                    }
                }
            }

            let audit = self.reconcile_runtime_orders_for_tokens_pass(&tokens);
            all_updates.extend(audit.updates);
            let terminal_token_responses =
                invalid_token_responses.saturating_add(cancels_disabled_responses);
            let complete_retired_evidence_pass = terminal_token_responses == tokens.len()
                && audit.errors.len() == audit.retired_market_absent.len();
            if complete_retired_evidence_pass {
                retired_market_evidence_streak = retired_market_evidence_streak.saturating_add(1);
            } else {
                retired_market_evidence_streak = 0;
            }
            let parallel_absence_fast_path =
                retired_market_parallel_absence_fast_path_allowed(
                    allow_expired_market_terminalization,
                    remote_clean,
                    audit.errors.len(),
                    &audit.retired_market_absent,
                );
            let retired_market_terminal = parallel_absence_fast_path
                || retired_market_terminalization_allowed(
                retired_market_evidence_streak,
                tokens.len(),
                terminal_token_responses,
                audit.errors.len(),
                audit.retired_market_absent.len(),
            );
            let retired_absent_count = audit.retired_market_absent.len();
            let mut retired_closed = 0usize;
            if retired_market_terminal {
                for missing in &audit.retired_market_absent {
                    if let Some(update) =
                        self.close_retired_market_order(missing, market_condition_id)
                    {
                        all_updates.push(update);
                        retired_closed = retired_closed.saturating_add(1);
                    }
                }
            }
            let open_orders = self
                .shared
                .execution_snapshot()
                .open_orders
                .values()
                .filter(|tracked| tokens.contains(&tracked.symbol))
                .count();
            let recovery_pending = self
                .shared
                .account_state
                .pending_order_audit_ids()
                .into_iter()
                .filter(|coid| {
                    self.shared
                        .account_state
                        .order(coid)
                        .is_some_and(|order| tokens.contains(&order.token_id))
                })
                .count();
            let remote_final = remote_clean || retired_market_terminal;
            let local_clean = if retired_market_terminal {
                retired_closed == retired_absent_count && open_orders == 0 && recovery_pending == 0
            } else {
                audit.errors.is_empty() && open_orders == 0 && recovery_pending == 0
            };
            last_detail = format!(
                "attempt={} remote=[{}] invalid_token_responses={} cancels_disabled_responses={} terminal_token_responses={}/{} retired_evidence_streak={} parallel_absence_fast_path={} retired_market_terminal={} retired_orders_closed={}/{} open_orders={} recovery_pending={} audit_errors={:?}",
                attempt_idx + 1,
                remote_details.join(", "),
                invalid_token_responses,
                cancels_disabled_responses,
                terminal_token_responses,
                tokens.len(),
                retired_market_evidence_streak,
                parallel_absence_fast_path,
                retired_market_terminal,
                retired_closed,
                retired_absent_count,
                open_orders,
                recovery_pending,
                audit.errors,
            );
            if remote_final && local_clean {
                info!(
                    "[PolymarketTrade] market cancel finality confirmed market={}: {}",
                    market_condition_id, last_detail,
                );
                return MarketCancelFinality {
                    confirmed: true,
                    updates: all_updates,
                    detail: last_detail,
                };
            }
        }

        warn!(
            "[PolymarketTrade] market cancel finality pending market={}: {}",
            market_condition_id, last_detail,
        );

        MarketCancelFinality {
            confirmed: false,
            updates: all_updates,
            detail: last_detail,
        }
    }

    /// Register the strategy's durable settled-FIFO promise in the account-wide
    /// reference ledger. Replays after restart are idempotent.
    pub fn retain_event_audit(&self, condition_id: &str, asset_ids: &[String]) -> Result<()> {
        self.shared.account_state.retain_settled_event_audit(
            &self.instance_id,
            condition_id,
            asset_ids,
        )?;
        Ok(())
    }

    /// Destroy event-scoped mappings and durable audit rows only when the
    /// strategy confirms its settled-event FIFO has evicted that event.
    pub fn retire_event_audit(&self, condition_id: &str, asset_ids: &[String]) {
        let retired_tokens: HashSet<String> = asset_ids
            .iter()
            .filter(|token| !token.is_empty())
            .cloned()
            .collect();
        if retired_tokens.is_empty() {
            return;
        }
        let shared = Arc::clone(&self.shared);
        let instance_id = self.instance_id.clone();
        let owned_condition_id = condition_id.to_string();
        let asset_ids = asset_ids.to_vec();
        if let Err(error) = hexagent_runtime::background_jobs::try_submit(move || {
            let owned_coids = shared
                .account_state
                .order_ids_for_instance_tokens(&instance_id, &retired_tokens);
            let reclaimed = owned_coids.len();
            let command = ExecutionStateCommand::RetireMappings {
                asset_ids: asset_ids.clone(),
                owned_coids: owned_coids.clone(),
            };
            if let Err(error) = shared
                .account_lifecycle_tx
                .try_send(AccountLifecycleJob::ExecutionState(command))
            {
                shared.user_feed_health.set_inventory_uncertain(true);
                warn!(
                    "[PolymarketTrade] settled runtime retirement queue unavailable instance={} condition={}: {}",
                    instance_id,
                    owned_condition_id,
                    error,
                );
                return;
            }
            shared.enqueue_lifecycle_trace(LifecycleTraceJob::ForgetMany {
                client_order_ids: owned_coids.clone(),
            });
            if let Err(error) = shared.account_state.release_settled_event_audit(
                &instance_id,
                &owned_condition_id,
                &asset_ids,
            ) {
                warn!(
                    "[PolymarketTrade] settled audit reference release failed instance={} condition={}: {}",
                    instance_id, owned_condition_id, error,
                );
            }
            shared.request_settled_gc();
            info!(
                "[PolymarketTrade] settled background cleanup retired {} runtime mapping(s); owner-thread ledger GC queued for {} token(s)",
                reclaimed,
                retired_tokens.len(),
            );
        }) {
            warn!(
                "[PolymarketTrade] cannot enqueue settled GC instance={} condition={}: {}",
                self.instance_id, condition_id, error,
            );
        }
    }

    fn handle_trading_disabled(&self, error: &str) {
        if self.shared.record_trading_disabled() {
            warn!(
                "[PolymarketTrade] CLOB trading disabled; rejecting placements definitively and pausing SDK submits for {}s: {}",
                SharedState::TRADING_DISABLED_BACKOFF_NS / 1_000_000_000,
                error,
            );
        }
    }

    /// React to a `not enough balance / allowance` rejection.
    ///
    /// Root cause observed in live.log: a cancel DELETE times out (p95
    /// ≥ 500 ms, ~70% of minute-windows), the order is parked as an
    /// orphan, but the server still reserves that order's collateral
    /// against our allowance. The next submit hits the same ceiling
    /// and comes back `balance: X, sum of active orders: X, order
    /// amount: X`.
    ///
    /// **Targeted cancel scope** (one knob, two cases):
    ///
    /// * BUY rejected → cancel ALL active **BUY** orders across all
    ///   tokens. BUY collateral is denominated in USDC (a single
    ///   per-wallet pool), so allowance pressure is global.
    /// * SELL rejected → cancel SELL orders **on the same `symbol`**
    ///   only. SELL collateral is per-token shares, so the pressure
    ///   is local to that outcome.
    ///
    /// Compared to the previous `cancel-all`: avoids wiping the other
    /// pool entirely (e.g. SELL Down rejected used to also kill all
    /// BUY orders — pure churn since BUYs don't share the SELL-Down
    /// share pool). Locally-tracked orders that have already been
    /// dropped from `open_orders` (rejected-by-server, terminal) are
    /// naturally skipped by the snapshot — no double-cancel.
    ///
    /// Mitigation flow:
    ///   1. Enter a 200 ms backoff — pre-reject new submits during
    ///      the window so we stop hammering doomed placements while
    ///      the racing cancel lands. Sized to cover cancel p95
    ///      (~320 ms in live but most often 60-80 ms p50).
    ///   2. On the entering edge, fire a single batch
    ///      `DELETE /orders` listing only the targeted orderIDs so
    ///      the server releases the relevant pool.
    ///
    /// Only the first balance reject in a window triggers the cancel
    /// — subsequent rejects extend the deadline but don't re-blast.
    fn handle_balance_error(&self, coid: &str, side: Side, symbol: &str) {
        if !self.shared.record_balance_error(&self.instance_id) {
            // Already in backoff — deadline extended, nothing more to do.
            return;
        }

        // Snapshot the targets while holding both locks briefly.
        // Already-cancelled / rejected orders aren't in `open_orders`
        // so they're skipped automatically — the user's "don't repeat
        // cancel" requirement is satisfied by the existing lifecycle.
        let (scope_label, targets): (&'static str, Vec<(String, String)>) = {
            let execution = self.shared.execution_snapshot();
            let open = &execution.open_orders;
            let coid_to_oid = &execution.coid_to_oid;
            let mut targets = Vec::with_capacity(open.len());
            for (c, t) in open.iter() {
                // Scope to THIS instance's own orders only — a shared-wallet
                // sibling's resting orders live in the same `open_orders` map
                // but must never be cancelled by our balance-error sweep.
                if t.instance_id != self.instance_id {
                    continue;
                }
                let in_scope = match side {
                    Side::Buy => t.side == Side::Buy,
                    Side::Sell => t.side == Side::Sell && t.symbol == symbol,
                };
                if !in_scope {
                    continue;
                }
                if let Some(oid) = coid_to_oid.get(c) {
                    targets.push((c.clone(), oid.clone()));
                }
            }
            let lbl = match side {
                Side::Buy => "all-BUYs (USDC pool)",
                Side::Sell => "same-symbol SELLs (token pool)",
            };
            (lbl, targets)
        };

        let backoff_ms = SharedState::BALANCE_BACKOFF_NS / 1_000_000;
        if targets.is_empty() {
            warn!(
                "[PolymarketTrade] Balance error coid={} side={:?} → {}ms backoff (no live orders in {} scope)",
                coid, side, backoff_ms, scope_label,
            );
            return;
        }

        let target_count = targets.len();
        warn!(
            "[PolymarketTrade] Balance error coid={} side={:?} → {}ms backoff + cancel {} {} (one-shot)",
            coid, side, backoff_ms, target_count, scope_label,
        );

        // Build batch DELETE /orders body — a JSON array of orderIDs.
        // The server responds with `canceled` / `not_canceled` maps
        // exactly like a regular batch cancel; the OrderUpdate flow
        // for each coid is driven separately by user_feed events
        // (server pushes a Cancelled trade message), so we don't need
        // to synthesise updates here.
        let body = serde_json::Value::Array(
            targets
                .iter()
                .map(|(_, oid)| serde_json::Value::String(oid.clone()))
                .collect(),
        )
        .to_string();
        let rx = self.shared.http_call_async("DELETE", "/orders", &body);

        async_rt::handle().spawn(async move {
            match tokio::task::spawn_blocking(move || rx.recv()).await {
                Ok(Ok(Ok(json))) => {
                    let canceled = json.get("canceled").and_then(|v| v.as_array())
                        .map(|a| a.len()).unwrap_or(0);
                    let not_canceled = json.get("not_canceled").and_then(|v| v.as_object())
                        .map(|o| o.len()).unwrap_or(0);
                    info!(
                        "[PolymarketTrade] Balance-backoff targeted cancel ({}): {}/{} canceled, {} not_canceled",
                        scope_label, canceled, target_count, not_canceled,
                    );
                }
                Ok(Ok(Err(e))) => warn!(
                    "[PolymarketTrade] Balance-backoff targeted cancel HTTP: {} ({})",
                    e, scope_label,
                ),
                _ => {}
            }
        });
    }

    /// React to an `invalid token id` rejection for `symbol` (token not
    /// registered on the CLOB). Bumps the token's strike count and, once it
    /// crosses the threshold, blocks further submits for that token (logged
    /// once per backoff window). Unlike a balance error there are no live
    /// orders to cancel — the rejected placements never reached the book.
    fn handle_invalid_token(&self, symbol: &str) {
        if self.shared.record_invalid_token(symbol) {
            let backoff_ms = SharedState::INVALID_TOKEN_BACKOFF_NS / 1_000_000;
            let sym = if symbol.len() > 16 {
                &symbol[..16]
            } else {
                symbol
            };
            warn!(
                "[PolymarketTrade] invalid token id {}... ×{} → {}ms submit backoff for this token (CLOB book not live for this event)",
                sym, SharedState::INVALID_TOKEN_STRIKES, backoff_ms,
            );
        }
    }

    /// DELETE variant, routed through the async reqwest client.
    fn delete_detailed(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> std::result::Result<serde_json::Value, HttpErr> {
        let body_str = body.to_string();
        self.shared.http_call_sync("DELETE", path, &body_str)
    }

    /// GET from a CLOB endpoint with authentication (used by reconcile path).
    #[allow(dead_code)]
    fn get(&self, path: &str) -> Result<serde_json::Value> {
        self.shared
            .http_call_sync("GET", path, "")
            .map_err(|e| anyhow!("GET {} failed: {}", path, e))
    }

    /// Sign an order and return both the signed form (incl. pre-computed
    /// `order_hash` aka Polymarket `orderID`) and the JSON body ready
    /// for POST /order. Keeping them together lets callers register the
    /// orderID in the coid↔orderID maps BEFORE issuing the HTTP call.
    ///
    /// `order.fee_rate_bps` is populated by the strategy from the event API's
    /// `takerBaseFee` via `BinaryOption.base_fee` → `OrderManager`. It is the
    /// single source of truth — no fallback `/fee-rate` fetch.
    /// Returns `(order_hash, POST body)`. Dispatches on `clob_version`:
    ///
    ///   * v1 (pre-cutover) — current CTFExchange schema. `feeRateBps`
    ///     in the signed order; `taker/expiration/nonce` fields present.
    ///   * v2 (post-cutover) — new CTFExchange. Signed order drops
    ///     `feeRateBps/taker/expiration/nonce`, adds `timestamp` (ms) /
    ///     `metadata` / `builder` (bytes32 each). The HTTP body follows
    ///     suit. Fee is computed protocol-side at match time, so
    ///     `order.fee_rate_bps` is informational only and not signed.
    fn sign_and_build_body(
        &self,
        order: &OrderRequest,
    ) -> Result<(String /* order_hash */, PolyOrderBody)> {
        let price = validate_order_for_signing(order)?;

        self.sign_and_build_body_v2(order, price)
    }

    /// Translate `OrderRequest::order_type` to Polymarket's wire string.
    /// (Wire-body structs for the typed one-pass serialization live just
    /// below `sign_and_build_body_v2`.)
    /// `Limit` (the default) maps to `"GTC"` (Good-Till-Cancel — resting
    /// limit). `Fak` / `Fok` pass through verbatim. `Market`,
    /// `LimitMaker` aren't valid for Polymarket and degrade to `"GTC"`
    /// for back-compat (pre-fak callers always passed `Limit`).
    fn poly_order_type_str(t: crate::types::OrderType) -> &'static str {
        match t {
            crate::types::OrderType::Fak => "FAK",
            crate::types::OrderType::Fok => "FOK",
            _ => "GTC",
        }
    }

    fn sign_and_build_body_v2(
        &self,
        order: &OrderRequest,
        price: f64,
    ) -> Result<(String, PolyOrderBody)> {
        let signer_v2 =
            self.shared.signer_v2.as_ref().ok_or_else(|| {
                anyhow!("clob_version=v2 but signer_v2 is None — constructor bug")
            })?;
        let signed = signer_v2.build_signed_order_dispatch(
            &order.symbol,
            price,
            order.quantity,
            order.side,
        )?;

        let salt_u64: u64 = signed
            .order
            .salt
            .parse::<u128>()
            .map(|v| v as u64)
            .unwrap_or(0);

        // v2 wire body — field set matches `orderToJsonV2` in
        // clob-client-v2/src/types/ordersV2.ts exactly. No `nonce`, no
        // `feeRateBps` (both removed in v2). `taker` and `expiration`
        // are wire-only (NOT in the signed struct). Typed struct →
        // one-pass serialization at dispatch; strings move, no clones.
        let o = signed.order;
        let body = PolyOrderBody::V2(WireBodyV2 {
            owner: self.owner.clone(),
            order_type: Self::poly_order_type_str(order.order_type),
            post_only: order.post_only,
            defer_exec: false,
            order: WireOrderV2 {
                salt: salt_u64,
                maker: o.maker,
                signer: o.signer,
                taker: o.taker,
                token_id: o.token_id,
                maker_amount: o.maker_amount,
                taker_amount: o.taker_amount,
                side: if order.side == Side::Buy {
                    "BUY"
                } else {
                    "SELL"
                },
                signature_type: o.signature_type,
                timestamp: o.timestamp,
                expiration: o.expiration,
                metadata: o.metadata,
                builder: o.builder,
                signature: signed.signature,
            },
        });
        Ok((signed.order_hash, body))
    }

    /// Normalise an `orderID` for comparison — Polymarket's API returns
    /// the hex in mixed case (no checksum); we lowercase both sides.
    fn oid_eq(a: &str, b: &str) -> bool {
        normalize_order_id(a) == normalize_order_id(b)
    }

    /// Make a rejected OrderUpdate for rate limit or other local errors.
    fn make_rejected(order: &OrderRequest, msg: &str) -> OrderUpdate {
        // `avg_fill_price` is repurposed here to carry the requested
        // order price for Rejected updates. Strategies use it to
        // back-infer market state from server rejection messages
        // (e.g. "post-only crosses book" implies the real best bid/ask
        // has moved past `order.price`, so the local OB cache should
        // be updated to reflect the inferred level). The convention
        // is safe: no fill happened on Rejected, so the field can
        // carry the requested price without breaking any consumer
        // that reads it on Filled / PartiallyFilled.
        let rejected_price = order.price.unwrap_or(0.0);
        OrderUpdate {
            order_slot: order.order_slot,
            client_order_id: order.client_order_id.clone(),
            exchange: Exchange::Polymarket,
            symbol: order.symbol.clone(),
            side: order.side,
            exchange_order_id: None,
            status: OrderStatus::Rejected,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: order.quantity,
            avg_fill_price: rejected_price,
            timestamp_ns: now_ns(),
            exchange_event_timestamp_ns: None,
            trade_id: None,
            order_audit: None,
            error: if msg.is_empty() {
                None
            } else {
                Some(msg.to_string())
            },
        }
    }

    fn make_logged_preflight_rejections(
        &self,
        orders: &[OrderRequest],
        reason: &str,
    ) -> Vec<OrderUpdate> {
        orders
            .iter()
            .map(|order| {
                self.shared.register_order_lifecycle(order);
                let rejected = Self::make_rejected(order, reason);
                self.shared.log_preflight_rejected(
                    &order.client_order_id,
                    rejected.exchange_order_id.as_deref(),
                    &rejected,
                );
                self.shared.forget_order_lifecycle(&order.client_order_id);
                rejected
            })
            .collect()
    }

    /// Make a timeout OrderUpdate for a placement whose HTTP call timed
    /// out. The server MAY have accepted the order; strategy should
    /// reconcile — but because we pre-compute and pass along `order_hash`
    /// (the Polymarket `orderID`), reconciliation can query / cancel by
    /// orderID directly via `GET /data/order/{orderID}` or
    /// `DELETE /order/{orderID}` without any salt/price matching.
    fn make_timeout_place(order: &OrderRequest, order_hash: Option<&str>) -> OrderUpdate {
        OrderUpdate {
            order_slot: order.order_slot,
            client_order_id: order.client_order_id.clone(),
            exchange: Exchange::Polymarket,
            symbol: order.symbol.clone(),
            side: order.side,
            exchange_order_id: order_hash.map(|h| h.to_string()),
            status: OrderStatus::NewOrderTimeout,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: order.quantity,
            avg_fill_price: order.price.unwrap_or(0.0),
            timestamp_ns: now_ns(),
            exchange_event_timestamp_ns: None,
            trade_id: None,
            order_audit: None,
            error: None,
        }
    }

    /// Make a timeout OrderUpdate for a cancel whose HTTP call timed out.
    fn make_timeout_cancel(
        coid: &str,
        symbol: &str,
        side: Side,
        order_id: Option<String>,
    ) -> OrderUpdate {
        Self::make_orphan_cancel(
            coid,
            symbol,
            side,
            order_id,
            OrderStatus::CancelOrderTimeout,
        )
    }

    /// Orphan-cancel update with an explicit status: `CancelOrderTimeout`
    /// for transport timeouts, `CancelUncertain` for healthy-but-ambiguous
    /// replies. Downstream handling is identical; the split is for
    /// observability (logs / metrics / sim calibration).
    fn make_orphan_cancel(
        coid: &str,
        symbol: &str,
        side: Side,
        order_id: Option<String>,
        status: OrderStatus,
    ) -> OrderUpdate {
        OrderUpdate {
            order_slot: Default::default(),
            client_order_id: coid.to_string(),
            exchange: Exchange::Polymarket,
            symbol: symbol.to_string(),
            side,
            exchange_order_id: order_id,
            status,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: 0.0,
            avg_fill_price: 0.0,
            timestamp_ns: now_ns(),
            exchange_event_timestamp_ns: None,
            trade_id: None,
            order_audit: None,
            error: None,
        }
    }

    /// Reconcile orphan orders whose HTTP call timed out:
    ///
    /// - `pending_places` — placements that may or may not have reached the
    ///   exchange. Each orphan carries its pre-computed EIP-712 `order_hash`
    ///   (== Polymarket server `orderID`), so we query `GET /data/order/{id}`
    ///   for a deterministic LIVE / MATCHED / CANCELED / 404 answer and
    ///   register the mapping if live. Fast, unambiguous, and unaffected
    ///   by snapshot pagination races.
    ///
    /// - `pending_cancels` — cancels whose response timed out. For each
    ///   (coid, order_id), query the specific order's status: if still Live,
    ///   emit a `CancelOrderTimeout` (retry next cycle); if Matched/Cancelled,
    ///   emit the corresponding terminal status.
    pub fn reconcile_orphans(
        &self,
        pending_places: &[(String, String, Side, f64, Option<String>)],
        pending_cancels: &[(String, String)],
        pending_trade_ids: &[String],
    ) -> Vec<OrderUpdate> {
        self.reconcile_orphans_with_permit(None, pending_places, pending_cancels, pending_trade_ids)
    }

    /// Admission-bound reconcile path used by the live engine. New-order
    /// timeout lookups start on the exact Reconcile slot held by `permit` and
    /// concurrently collect evidence from a second account slot. Cancel
    /// lookups remain single-slot because absence never releases their
    /// worst-case reservation; repeated transport failures still rebuild the
    /// affected pool slot.
    pub fn reconcile_orphans_on(
        &self,
        permit: &crate::http1_pool::Permit,
        pending_places: &[(String, String, Side, f64, Option<String>)],
        pending_cancels: &[(String, String)],
        pending_trade_ids: &[String],
    ) -> Vec<OrderUpdate> {
        self.reconcile_orphans_with_permit(
            Some(permit),
            pending_places,
            pending_cancels,
            pending_trade_ids,
        )
    }

    fn reconcile_orphans_with_permit(
        &self,
        permit: Option<&crate::http1_pool::Permit>,
        pending_places: &[(String, String, Side, f64, Option<String>)],
        pending_cancels: &[(String, String)],
        pending_trade_ids: &[String],
    ) -> Vec<OrderUpdate> {
        let mut updates: Vec<OrderUpdate> = Vec::new();

        // --- Placements: deterministic per-orderID lookup ---
        if !pending_places.is_empty() {
            debug!(
                "[PolymarketTrade] Reconcile: {} orphan placements",
                pending_places.len(),
            );
            for (coid, symbol, side, price, order_hash) in pending_places {
                let oid = match order_hash.as_deref() {
                    Some(s) => s,
                    None => {
                        // Caller didn't supply an order_hash. Given every
                        // current call site pre-computes it, this is a
                        // bug — keep as orphan and warn so the broken
                        // path surfaces.
                        warn!(
                            "[PolymarketTrade] Reconcile: placement coid={} has no order_hash — keeping as orphan",
                            coid,
                        );
                        continue;
                    }
                };
                // Per-coid exponential backoff: skip the GET until this coid's
                // next-retry deadline (set on the previous not-found). Keeps the
                // orphan parked without re-hammering a slow PM REST endpoint.
                if let Some(next_ns) = self.shared.placement_reconcile_next_retry_ns.get(coid) {
                    if now_ns() < next_ns {
                        continue;
                    }
                }
                // A prior 425 backs off only this orphan. Other coids in the
                // same reconcile batch must continue to authoritative audit.
                if self.shared.in_http_425_backoff(coid) {
                    continue;
                }
                let fetch_result = self.fetch_order_by_id(coid, oid, permit, true);
                // A 425 from this GET is not a not-found answer. Keep this
                // orphan parked without affecting the rest of the batch.
                if matches!(&fetch_result, FetchOrderResult::Unavailable(_))
                    && self.shared.in_http_425_backoff(coid)
                {
                    self.shared.reconcile_attempts.clear_placement(coid);
                    log::debug!(
                        "[PolymarketTrade] Reconcile placement coid={} orderID={}: fetch deferred (HTTP 425 backoff); keeping orphan",
                        coid, oid,
                    );
                    continue;
                }

                // Every placement orphan uses the same terminal rule,
                // regardless of whether the original POST failed with a
                // timeout, HTTP 5xx/DeadlineExceeded, HTTP 425, or a
                // status-less transport error. Only uninterrupted explicit
                // server not-found responses advance the counter.
                if fetch_result.is_explicit_not_found() {
                    let parallel_evidence = fetch_result.has_parallel_not_found_evidence();
                    let observations = if parallel_evidence {
                        2
                    } else {
                        1
                    };
                    let attempts = self
                        .shared
                        .reconcile_attempts
                        .next_placement_with_evidence(coid, observations);
                    if attempts >= RECONCILE_NOT_FOUND_RETRY_LIMIT {
                        self.shared.reconcile_attempts.clear_placement(coid);
                        self.shared.placement_reconcile_next_retry_ns.remove(coid);
                        warn!(
                            "[orphan_metric] placement_not_found_terminal=1 coid={} orderID={} consecutive_not_found_observations={} required_not_found_observations={} parallel_evidence={} terminal=Rejected lock_release=allowed",
                            coid,
                            oid,
                            attempts,
                            RECONCILE_NOT_FOUND_RETRY_LIMIT,
                            parallel_evidence,
                        );
                        self.shared.remove_order_as(coid, OrderStatus::Rejected);
                        updates.push(OrderUpdate {
                            order_slot: Default::default(),
                            client_order_id: coid.clone(),
                            exchange: Exchange::Polymarket,
                            symbol: symbol.clone(),
                            side: *side,
                            exchange_order_id: Some(oid.to_string()),
                            status: OrderStatus::Rejected,
                            liquidity: None,
                            filled_quantity: 0.0,
                            remaining_quantity: 0.0,
                            avg_fill_price: *price,
                            timestamp_ns: now_ns(),
                            exchange_event_timestamp_ns: None,
                            trade_id: None,
                            order_audit: None,
                            error: Some(format!(
                                "placement orphan followed by {} consecutive reconcile not-found observations",
                                attempts,
                            )),
                        });
                        continue;
                    }
                    warn!(
                        "[PolymarketTrade] Reconcile: placement coid={} orderID={} not found on server (attempt {}/{}) — keeping orphan, retrying",
                        coid, oid, attempts, RECONCILE_NOT_FOUND_RETRY_LIMIT,
                    );
                    let backoff_ms = placement_reconcile_backoff_ms(attempts);
                    self.shared.placement_reconcile_next_retry_ns.insert(
                        coid.clone(),
                        now_ns().saturating_add(backoff_ms.saturating_mul(1_000_000)),
                    );
                    continue;
                } else {
                    // Consecutiveness is strict: service/transport failures,
                    // found orders, and unexpected statuses all restart the
                    // four-result negative-proof window.
                    self.shared.reconcile_attempts.clear_placement(coid);
                }

                let fetched_order = fetch_result.order();
                let status_str = fetched_order
                    .map(|order| order.status.clone())
                    .unwrap_or_default();
                let order_audit = fetched_order.map(|order| order.audit.clone());
                match status_str.as_str() {
                    "LIVE" => {
                        // Conclusive answer — clear the not_found counter
                        // so a future unrelated 404 starts fresh.
                        self.shared.reconcile_attempts.clear_placement(coid);
                        self.shared.placement_reconcile_next_retry_ns.remove(coid);
                        let Some(ownership) = self.shared.account_state.order(coid) else {
                            warn!(
                                "[PolymarketTrade] Reconcile placement coid={} orderID={} LIVE without durable ownership — keeping orphan",
                                coid, oid,
                            );
                            continue;
                        };
                        let (effective_size_matched, has_valid_size_matched) =
                            effective_audited_match(
                                order_audit
                                    .as_ref()
                                    .and_then(|audit| audit.size_matched.as_deref()),
                                ownership.quantity,
                                ownership.filled_quantity,
                            );
                        if let Some(audit) = order_audit.as_ref() {
                            if !audit.associate_trades.is_empty() {
                                updates.extend(self.reconcile_orphans_with_permit(
                                    permit,
                                    &[],
                                    &[],
                                    &audit.associate_trades,
                                ));
                            }
                        }
                        if !has_valid_size_matched {
                            warn!(
                                "[PolymarketTrade] Reconcile placement coid={} LIVE omitted/invalid size_matched; preserving local filled_quantity={}",
                                coid, ownership.filled_quantity,
                            );
                        }
                        self.shared.register_order_id(coid, oid, symbol);
                        let candidate = if effective_size_matched > 1e-9 {
                            OrderStatus::PartiallyFilled
                        } else {
                            OrderStatus::Accepted
                        };
                        let status = self
                            .shared
                            .mark_order_live(
                                coid,
                                self.shared
                                    .execution_snapshot()
                                    .open_orders
                                    .get(coid)
                                    .map(|tracked| tracked.order_slot)
                                    .unwrap_or_default(),
                                symbol,
                                *side,
                                &ownership.instance_id,
                                candidate,
                            )
                            .unwrap_or(candidate);
                        info!(
                            "[PolymarketTrade] Reconciled placement coid={} orderID={} → LIVE status={:?} size_matched={}",
                            coid, oid, status, effective_size_matched,
                        );
                        updates.push(OrderUpdate {
                            order_slot: Default::default(),
                            client_order_id: coid.clone(),
                            exchange: Exchange::Polymarket,
                            symbol: symbol.clone(),
                            side: *side,
                            exchange_order_id: Some(oid.to_string()),
                            status,
                            liquidity: None,
                            filled_quantity: 0.0,
                            remaining_quantity: (ownership.quantity - effective_size_matched)
                                .max(0.0),
                            avg_fill_price: ownership.price,
                            timestamp_ns: now_ns(),
                            exchange_event_timestamp_ns: None,
                            trade_id: None,
                            order_audit: order_audit.clone(),
                            error: None,
                        });
                    }
                    "MATCHED" | "MATCHED_NOT_BROADCASTED" | "FILLED" => {
                        self.shared.reconcile_attempts.clear_placement(coid);
                        self.shared.placement_reconcile_next_retry_ns.remove(coid);
                        if let Some(audit) = order_audit.as_ref() {
                            self.shared.commit_authoritative_terminal_audit(
                                coid,
                                OrderStatus::Filled,
                                audit,
                            );
                            if !audit.associate_trades.is_empty() {
                                updates.extend(self.reconcile_orphans_with_permit(
                                    permit,
                                    &[],
                                    &[],
                                    &audit.associate_trades,
                                ));
                            }
                            if self
                                .shared
                                .account_state
                                .terminal_order_audit_complete(coid)
                            {
                                self.shared.complete_filled_order_audit(coid);
                            }
                        } else {
                            self.shared.remove_order_as(coid, OrderStatus::Filled);
                        }
                        info!(
                            "[PolymarketTrade] Reconciled placement coid={} orderID={} → Filled",
                            coid, oid,
                        );
                        updates.push(OrderUpdate {
                            order_slot: Default::default(),
                            client_order_id: coid.clone(),
                            exchange: Exchange::Polymarket,
                            symbol: symbol.clone(),
                            side: *side,
                            exchange_order_id: Some(oid.to_string()),
                            status: OrderStatus::Filled,
                            liquidity: None,
                            filled_quantity: 0.0,
                            remaining_quantity: 0.0,
                            avg_fill_price: *price,
                            timestamp_ns: now_ns(),
                            exchange_event_timestamp_ns: None,
                            trade_id: None,
                            order_audit: order_audit.clone(),
                            error: Some(ORPHAN_RECONCILE_AUTHORITATIVE_TERMINAL.to_string()),
                        });
                    }
                    value if value.starts_with("CANCELED") || value.starts_with("CANCELLED") => {
                        let Some(audit) = order_audit.as_ref() else {
                            warn!(
                                "[PolymarketTrade] Reconcile placement coid={} orderID={} status={} without order audit — preserving orphan reservation",
                                coid, oid, value,
                            );
                            continue;
                        };

                        // A cancellation only proves that the residual is off
                        // book. Replay its trades first so the durable account
                        // ledger has the latest local filled quantity before
                        // the authoritative cumulative match is applied.
                        if !audit.associate_trades.is_empty() {
                            updates.extend(self.reconcile_orphans_with_permit(
                                permit,
                                &[],
                                &[],
                                &audit.associate_trades,
                            ));
                        }
                        let Some(ownership) = self.shared.account_state.order(coid) else {
                            warn!(
                                "[PolymarketTrade] Reconcile placement coid={} orderID={} status={} without durable ownership — preserving orphan",
                                coid, oid, value,
                            );
                            continue;
                        };
                        let (matched, has_valid_size_matched) = effective_audited_match(
                            audit.size_matched.as_deref(),
                            ownership.quantity,
                            ownership.filled_quantity,
                        );
                        if !has_valid_size_matched {
                            warn!(
                                "[PolymarketTrade] Reconcile placement coid={} orderID={} status={} omitted/invalid size_matched — preserving reservation",
                                coid, oid, value,
                            );
                            continue;
                        }

                        self.shared.reconcile_attempts.clear_placement(coid);
                        self.shared.placement_reconcile_next_retry_ns.remove(coid);
                        self.shared.commit_authoritative_terminal_audit(
                            coid,
                            OrderStatus::Cancelled,
                            audit,
                        );
                        if self
                            .shared
                            .account_state
                            .terminal_order_audit_complete(coid)
                        {
                            self.shared
                                .remove_order_resolved_as(coid, OrderStatus::Cancelled);
                        }
                        info!(
                            "[PolymarketTrade] Reconciled placement coid={} orderID={} → Cancelled size_matched={}",
                            coid, oid, matched,
                        );
                        updates.push(OrderUpdate {
                            order_slot: Default::default(),
                            client_order_id: coid.clone(),
                            exchange: Exchange::Polymarket,
                            symbol: symbol.clone(),
                            side: *side,
                            exchange_order_id: Some(oid.to_string()),
                            status: OrderStatus::Cancelled,
                            liquidity: None,
                            filled_quantity: 0.0,
                            remaining_quantity: 0.0,
                            avg_fill_price: ownership.price,
                            timestamp_ns: now_ns(),
                            exchange_event_timestamp_ns: None,
                            trade_id: None,
                            order_audit: order_audit.clone(),
                            error: Some(ORPHAN_RECONCILE_AUTHORITATIVE_TERMINAL.to_string()),
                        });
                    }
                    "" => {
                        // Explicit not-found returned above. An empty status
                        // here therefore means the lookup was unavailable;
                        // keep the orphan and retry without advancing the
                        // consecutive-not-found counter.
                        log::debug!(
                            "[PolymarketTrade] Reconcile placement coid={} orderID={}: lookup unavailable; keeping orphan",
                            coid, oid,
                        );
                        continue;
                    }
                    "INVALID" => {
                        // Polymarket "INVALID" = order failed server-side
                        // validation (signature / expiration / nonce /
                        // already-spent collateral). Definitive terminal
                        // — never going to LIVE/MATCHED. Live evidence
                        // 2026-05-01 06:50: a single INVALID-status coid
                        // looped 2,088 reconcile attempts over 50 min,
                        // wedging the strategy's orphan-gate →
                        // on_quote early-returned every tick →
                        // poll_pending_snapshots never ran → 11 events
                        // ran with no quoting. Treat exactly like
                        // Rejected so the orphan clears immediately.
                        self.shared.reconcile_attempts.clear_placement(coid);
                        self.shared.placement_reconcile_next_retry_ns.remove(coid);
                        warn!(
                            "[PolymarketTrade] Reconcile: placement coid={} orderID={} status=INVALID → Rejected (server validation failed)",
                            coid, oid,
                        );
                        self.shared.remove_order_as(coid, OrderStatus::Rejected);
                        updates.push(OrderUpdate {
                            order_slot: Default::default(),
                            client_order_id: coid.clone(),
                            exchange: Exchange::Polymarket,
                            symbol: symbol.clone(),
                            side: *side,
                            exchange_order_id: None,
                            status: OrderStatus::Rejected,
                            liquidity: None,
                            filled_quantity: 0.0,
                            remaining_quantity: 0.0,
                            avg_fill_price: 0.0,
                            timestamp_ns: now_ns(),
                            exchange_event_timestamp_ns: None,
                            trade_id: None,
                            order_audit: None,
                            error: Some("server status=INVALID (validation failed)".to_string()),
                        });
                    }
                    other => {
                        // An unknown status is not authority to release risk
                        // and is not a not-found result. Keep the orphan,
                        // restart the consecutive-not-found window, and use a
                        // short retry delay to avoid quote-cadence polling.
                        warn!(
                            "[PolymarketTrade] Reconcile: placement coid={} orderID={} returned unexpected status '{}' — keeping as orphan",
                            coid, oid, other,
                        );
                        self.shared.placement_reconcile_next_retry_ns.insert(
                            coid.clone(),
                            now_ns().saturating_add(
                                RECONCILE_BACKOFF_BASE_MS.saturating_mul(1_000_000),
                            ),
                        );
                    }
                }
            }
        }

        // --- Cancels: query each order by id ---
        for (coid, order_id) in pending_cancels {
            {
                let now = now_ns();
                if self
                    .shared
                    .cancel_reconcile_next_retry_ns
                    .get(coid)
                    .is_some_and(|deadline| deadline > now)
                {
                    continue;
                }
                self.shared.cancel_reconcile_next_retry_ns.remove(coid);
            }
            if self.shared.in_http_425_backoff(coid) {
                continue;
            }
            let fetch_result = self.fetch_order_by_id(coid, order_id, permit, false);
            // A 425 mid-iteration parks only this cancel orphan; unrelated
            // orders continue through the loop and can release their locks.
            let http_425_backoff_active = matches!(&fetch_result, FetchOrderResult::Unavailable(_),)
                && self.shared.in_http_425_backoff(coid);
            if http_425_backoff_active {
                log::debug!(
                    "[PolymarketTrade] Reconcile cancel coid={} orderID={}: fetch deferred (HTTP 425 backoff); keeping orphan",
                    coid, order_id,
                );
            }
            let fetched_order = fetch_result.order();
            let status_str = fetched_order
                .map(|order| order.status.clone())
                .unwrap_or_default();
            let order_audit = fetched_order.map(|order| order.audit.clone());
            let mut retry_diagnostic: Option<String> = None;
            let status = match status_str.as_str() {
                "LIVE" => {
                    // The order is still active on the server — our
                    // earlier DELETE HTTP timed out but never landed.
                    // Re-issue DELETE now so the order doesn't linger
                    // on the book (where it would fill and show up as
                    // a "matched orders can't be canceled" race on the
                    // next tick — the most common cause of the 36
                    // cancel-race rejects observed in live.log).
                    //
                    // The DELETE response resolves the orphan only when it
                    // names an authoritative terminal state:
                    // `canceled=[orderID]` → Cancelled; an explicit matched
                    // reason → Filled. Ambiguous/missing outcomes stay parked.
                    let body = serde_json::json!({ "orderID": order_id });
                    match self.delete_detailed("/order", &body) {
                        Ok(resp) => {
                            let reason = resp
                                .get("not_canceled")
                                .and_then(|v| v.as_object())
                                .and_then(|nc| nc.get(order_id))
                                .and_then(|v| v.as_str());
                            let outcome = self.shared.apply_reconcile_cancel_not_found_terminal(
                                coid,
                                reason,
                                cancel_delete_response_outcome(&resp, order_id),
                            );
                            if outcome == CancelReasonOutcome::Uncertain {
                                self.shared.note_get_live_delete_uncertain(coid, order_id);
                            }
                            let status = match outcome {
                                CancelReasonOutcome::Cancelled => OrderStatus::Cancelled,
                                CancelReasonOutcome::Filled => OrderStatus::Filled,
                                CancelReasonOutcome::Uncertain => OrderStatus::CancelUncertain,
                            };
                            info!("[PolymarketTrade] Reconcile DELETE retry coid={} orderID={} → {:?} (reason={})",
                                coid, order_id, status, reason.unwrap_or("<omitted>"));
                            status
                        }
                        Err(e) => {
                            // HTTP 425 backs off this orphan only. It stays
                            // parked and gets re-checked after the deadline.
                            if matches!(e, HttpErr::Status(425, _)) {
                                self.shared.note_http_425_backoff(coid);
                            }
                            warn!("[PolymarketTrade] Reconcile DELETE retry coid={} orderID={} HTTP error: {} — keeping as orphan",
                                coid, order_id, e);
                            OrderStatus::CancelOrderTimeout
                        }
                    }
                }
                "MATCHED" | "MATCHED_NOT_BROADCASTED" | "FILLED" => OrderStatus::Filled,
                // Any `CANCELED*` variant is a terminal "no longer active" status.
                // Polymarket emits multiple suffixed forms — observed:
                //   * `CANCELED` / `CANCELLED` — plain user-cancel
                //   * `CANCELED_MARKET_RESOLVED` — market settled before our
                //     cancel landed (event ended; order auto-cancelled)
                //   * `CANCELED_UNFILLED` / `CANCELED_BY_USER` / `CANCELED_TOO_LATE`
                //     (defensive — future Polymarket additions)
                // Pre-fix this match arm only recognised the bare `CANCELED`
                // form and routed every suffixed variant through the wildcard
                // below → `CancelOrderTimeout` → re-orphan → reconciler loops
                // forever. Live evidence 2026-05-12 13:14–13:37: single coid
                // 1778515343156 hit this path 1,975× over 23 min after a
                // CANCELED_MARKET_RESOLVED response, wedging the orphan-gate
                // and silently killing 3 trading events (13:20/25/30, vol=0).
                s if s.starts_with("CANCELED") || s.starts_with("CANCELLED") => {
                    OrderStatus::Cancelled
                }
                "" => {
                    // A missing/read-error result is not a terminal state. The
                    // write may not be visible on this replica, or the order
                    // may have matched while its user-feed event is in flight.
                    // Count attempts for diagnostics only; never convert retry
                    // exhaustion into a fabricated Cancelled status.
                    let attempts = self.shared.reconcile_attempts.next_cancel(coid);
                    let pending_delayed = self.shared.pending_delayed_orphans.contains(coid);
                    let backoff_ms = cancel_reconcile_backoff_ms(coid, attempts).max(
                        if http_425_backoff_active {
                            HTTP_425_BACKOFF_NS / 1_000_000
                        } else {
                            0
                        },
                    );
                    self.shared.cancel_reconcile_next_retry_ns.insert(
                        coid.clone(),
                        now_ns().saturating_add(backoff_ms.saturating_mul(1_000_000)),
                    );
                    let evidence = match &fetch_result {
                        FetchOrderResult::NotFound(evidence) => {
                            format!("explicit_not_found:{evidence}")
                        }
                        FetchOrderResult::Unavailable(kind) => {
                            format!("unavailable:{kind:?}")
                        }
                        FetchOrderResult::Found(_) => "status_missing".to_string(),
                    };
                    retry_diagnostic = Some(format!(
                        "{}{};evidence={};attempt={}",
                        ORPHAN_RECONCILE_RETRY_AFTER_MS_PREFIX, backoff_ms, evidence, attempts,
                    ));
                    match &fetch_result {
                        FetchOrderResult::NotFound(evidence) => {
                            warn!(
                                "[PolymarketTrade] Reconcile cancel coid={} orderID={} evidence={} attempt={} pending_delayed={} retry_ms={} — keeping orphan and worst-case reservation",
                                coid, order_id, evidence, attempts, pending_delayed, backoff_ms,
                            );
                            // Replica answered (not-found): transport healthy,
                            // state ambiguous.
                            OrderStatus::CancelUncertain
                        }
                        FetchOrderResult::Unavailable(kind) => {
                            if fetch_unavailable_should_warn(kind) {
                                warn!(
                                "[PolymarketTrade] Reconcile cancel coid={} orderID={} evidence=unavailable kind={:?} attempt={} pending_delayed={} retry_ms={} — keeping orphan and worst-case reservation",
                                coid, order_id, kind, attempts, pending_delayed, backoff_ms,
                            );
                            } else {
                                debug!(
                                    "[PolymarketTrade] Reconcile cancel coid={} orderID={} evidence=json_null attempt={} pending_delayed={} retry_ms={} — eventual consistency; keeping orphan and worst-case reservation",
                                    coid, order_id, attempts, pending_delayed, backoff_ms,
                                );
                            }
                            OrderStatus::CancelOrderTimeout
                        }
                        FetchOrderResult::Found(_) => unreachable!(
                            "a fetched order with a status cannot reach the empty-status arm"
                        ),
                    }
                }
                other => {
                    // A future/unknown server status is operationally serious,
                    // but not evidence of cancellation. Keep the orphan and
                    // reservation indefinitely; alerting/risk-off may escalate
                    // without changing the semantic order state.
                    let attempts = self.shared.reconcile_attempts.next_cancel(coid);
                    let backoff_ms = cancel_reconcile_backoff_ms(coid, attempts);
                    self.shared.cancel_reconcile_next_retry_ns.insert(
                        coid.clone(),
                        now_ns().saturating_add(backoff_ms.saturating_mul(1_000_000)),
                    );
                    retry_diagnostic = Some(format!(
                        "{}{};evidence=unknown_status:{};attempt={}",
                        ORPHAN_RECONCILE_RETRY_AFTER_MS_PREFIX, backoff_ms, other, attempts,
                    ));
                    warn!(
                        "[PolymarketTrade] Reconcile cancel coid={} orderID={} unknown server status '{}' (attempt={}, retry_ms={}) — keeping orphan and worst-case reservation",
                        coid, order_id, other, attempts, backoff_ms,
                    );
                    OrderStatus::CancelUncertain
                }
            };
            if matches!(
                status,
                OrderStatus::CancelOrderTimeout | OrderStatus::CancelUncertain
            ) && retry_diagnostic.is_none()
            {
                let attempts = self.shared.reconcile_attempts.next_cancel(coid);
                let backoff_ms = cancel_reconcile_backoff_ms(coid, attempts).max(
                    if self.shared.in_http_425_backoff(coid) {
                        HTTP_425_BACKOFF_NS / 1_000_000
                    } else {
                        0
                    },
                );
                self.shared.cancel_reconcile_next_retry_ns.insert(
                    coid.clone(),
                    now_ns().saturating_add(backoff_ms.saturating_mul(1_000_000)),
                );
                retry_diagnostic = Some(format!(
                    "{}{};evidence=live_delete_ambiguous;attempt={}",
                    ORPHAN_RECONCILE_RETRY_AFTER_MS_PREFIX, backoff_ms, attempts,
                ));
            }
            let status = self.shared.effective_cancel_attempt_status(coid, status);
            let authoritative_terminal_audit = order_audit.as_ref().filter(|_| {
                matches!(
                    status_str.as_str(),
                    "MATCHED" | "MATCHED_NOT_BROADCASTED" | "FILLED"
                ) || status_str.starts_with("CANCELED")
                    || status_str.starts_with("CANCELLED")
            });
            if status == OrderStatus::Cancelled || status == OrderStatus::Filled {
                if let Some(audit) = authoritative_terminal_audit {
                    self.shared
                        .commit_authoritative_terminal_audit(coid, status, audit);
                    if !audit.associate_trades.is_empty() {
                        updates.extend(self.reconcile_orphans_with_permit(
                            permit,
                            &[],
                            &[],
                            &audit.associate_trades,
                        ));
                    }
                    if self
                        .shared
                        .account_state
                        .terminal_order_audit_complete(coid)
                    {
                        self.shared.remove_order_resolved_as(coid, status);
                    }
                } else {
                    self.shared.remove_order_as(coid, status);
                }
                // Clear the defensive-retry counter on conclusive resolution
                // so a later unrelated unknown-status arm for the same coid
                // starts fresh.
                self.shared.reconcile_attempts.clear_cancel(coid);
            }
            info!(
                "[PolymarketTrade] Reconcile cancel coid={} orderID={} → {:?} (server={})",
                coid, order_id, status, status_str
            );
            let tracked = self
                .shared
                .execution_snapshot()
                .open_orders
                .get(coid)
                .cloned();
            let (symbol, side) = tracked
                .map(|t| (t.symbol, t.side))
                .unwrap_or_else(|| (String::new(), Side::Buy));
            updates.push(OrderUpdate {
                order_slot: Default::default(),
                client_order_id: coid.clone(),
                exchange: Exchange::Polymarket,
                symbol,
                side,
                exchange_order_id: Some(order_id.clone()),
                status,
                liquidity: None,
                filled_quantity: 0.0,
                remaining_quantity: 0.0,
                avg_fill_price: 0.0,
                timestamp_ns: now_ns(),
                exchange_event_timestamp_ns: None,
                trade_id: None,
                // Metadata is valid only when that same GET returned an
                // authoritative terminal status. A LIVE snapshot followed by
                // an ambiguous retry DELETE must trigger another audit.
                order_audit: authoritative_terminal_audit.cloned(),
                error: if matches!(status, OrderStatus::Cancelled | OrderStatus::Filled) {
                    Some(ORPHAN_RECONCILE_AUTHORITATIVE_TERMINAL.to_string())
                } else {
                    retry_diagnostic
                },
            });
        }

        // The terminal order audit names the complete associated trade set.
        // Replay missing IDs through the same parser used by WS/gap recovery,
        // leaving PositionManager as the sole dedup/accounting authority.
        for trade_id in pending_trade_ids {
            let path = terminal_trade_lookup_path(&trade_id);
            let reply = permit.map_or_else(
                || self.shared.http_call_sync("GET", &path, ""),
                |permit| {
                    self.shared
                        .http_call_sync_on(permit.current_pooled_client(), "GET", &path, "")
                },
            );
            let json = match reply {
                Ok(json) => json,
                Err(error) => {
                    warn!(
                        "[orphan_metric] terminal_trade_backfill_failed=1 trade_id={} error={} lock_release=forbidden",
                        trade_id, error,
                    );
                    continue;
                }
            };
            let records = terminal_trade_records(json, &trade_id);
            if records.is_empty() {
                let total = self
                    .shared
                    .terminal_trade_backfill_missing_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    .saturating_add(1);
                if claim_rate_limited_warning(
                    &self
                        .shared
                        .terminal_trade_backfill_missing_warn_silent_until_ns,
                    crate::types::now_ns(),
                ) {
                    let suppressed = self
                        .shared
                        .terminal_trade_backfill_missing_suppressed
                        .swap(0, std::sync::atomic::Ordering::Relaxed);
                    warn!(
                        "[orphan_metric] terminal_trade_backfill_missing=1 terminal_trade_backfill_missing_total={} occurrences_since_last={} last_trade_id={} lock_release=forbidden warn_interval_s={}",
                        total,
                        suppressed.saturating_add(1),
                        trade_id,
                        EXPECTED_ORPHAN_WARN_INTERVAL_NS / 1_000_000_000,
                    );
                } else {
                    self.shared
                        .terminal_trade_backfill_missing_suppressed
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                continue;
            }
            let record_count = records.len();
            let mut matched = 0usize;
            let mut validated_no_update = 0usize;
            let mut rejection_reasons = Vec::new();
            for mut record in records {
                if let Some(object) = record.as_object_mut() {
                    object
                        .entry("event_type".to_string())
                        .or_insert(serde_json::Value::String("trade".to_string()));
                }
                let parsed = match self.shared.diagnose_private_record_on_owner(record) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        rejection_reasons.push(error);
                        continue;
                    }
                };
                if let Some(reason) = parsed.rejection_reason {
                    rejection_reasons.push(reason);
                } else if parsed.valid_business_event && parsed.updates.is_empty() {
                    validated_no_update += 1;
                }
                matched += parsed.updates.len();
                updates.extend(parsed.updates);
            }
            if !rejection_reasons.is_empty() {
                warn!(
                    "[orphan_metric] terminal_trade_backfill_parser_rejected={} trade_id={} records={} validated_no_update={} reasons={:?} ownership_anomalies={} lock_release=forbidden",
                    rejection_reasons.len(), trade_id, record_count, validated_no_update,
                    rejection_reasons, self.shared.account_state.ownership_anomalies().len(),
                );
            }
            if matched > 0 {
                debug!(
                    "[orphan_metric] terminal_trade_backfill_updates={} trade_id={}",
                    matched, trade_id,
                );
            } else if rejection_reasons.is_empty() {
                self.shared.note_terminal_trade_backfill_noop(
                    &trade_id,
                    record_count,
                    validated_no_update,
                );
            }
        }

        updates
    }

    /// Query a single order by orderID and retain exact reconciliation
    /// metadata. Status alone cannot prove that all private fills were booked.
    ///
    /// Endpoint: `GET /data/order/{orderID}`. Tried `GET /order/{id}`
    /// briefly (commit 8b4ce1b) on the guess that it was "more modern"
    /// — empirically returns `404 page not found` from clob.polymarket.com
    /// while `/data/order/{id}` returns proper status strings. The
    /// py-clob-client SDK also uses the /data path.
    fn fetch_order_by_id(
        &self,
        coid: &str,
        order_id: &str,
        permit: Option<&crate::http1_pool::Permit>,
        parallel_evidence: bool,
    ) -> FetchOrderResult {
        let path = format!("/data/order/{}", order_id);
        if parallel_evidence {
            let mut held_permits = Vec::with_capacity(2);
            let primary_client = if let Some(permit) = permit {
                permit.current_pooled_client()
            } else if let Some(owned) = crate::http1_pool::try_borrow_account(
                self.shared.account_state.account_id(),
                crate::http1_pool::Role::Reconcile,
            ) {
                let client = owned.current_pooled_client();
                held_permits.push(owned);
                client
            } else {
                crate::http1_pool::pooled_client(crate::http1_pool::Role::Reconcile)
            };
            let secondary_permit = crate::http1_pool::try_borrow_account(
                self.shared.account_state.account_id(),
                crate::http1_pool::Role::Reconcile,
            );
            if let Some(secondary_permit) = secondary_permit {
                let secondary_client = secondary_permit.current_pooled_client();
                if (primary_client.role(), primary_client.slot())
                    != (secondary_client.role(), secondary_client.slot())
                {
                    let primary_location = (primary_client.role(), primary_client.slot());
                    let secondary_location = (secondary_client.role(), secondary_client.slot());
                    let primary_rx =
                        self.shared
                            .http_call_async_on(primary_client, "GET", &path, "");
                    let secondary_rx =
                        self.shared
                            .http_call_async_on(secondary_client, "GET", &path, "");
                    held_permits.push(secondary_permit);
                    let primary = primary_rx.recv().unwrap_or_else(|_| {
                        Err(HttpErr::Transport("async reply dropped".to_string()))
                    });
                    let secondary = secondary_rx.recv().unwrap_or_else(|_| {
                        Err(HttpErr::Transport("async reply dropped".to_string()))
                    });
                    let primary = self.classify_order_lookup_reply(coid, order_id, primary);
                    let secondary = self.classify_order_lookup_reply(coid, order_id, secondary);
                    let combined = combine_parallel_order_lookups(
                        primary,
                        secondary,
                        primary_location,
                        secondary_location,
                    );
                    log::info!(
                        "[orphan_metric] parallel_reconcile_evidence=1 coid={} orderID={} primary_role={:?} primary_slot={} secondary_role={:?} secondary_slot={} outcome={}",
                        coid,
                        order_id,
                        primary_location.0,
                        primary_location.1,
                        secondary_location.0,
                        secondary_location.1,
                        fetch_order_result_name(&combined),
                    );
                    return combined;
                }
                held_permits.push(secondary_permit);
            }
            log::debug!(
                "[orphan_metric] parallel_reconcile_evidence=0 coid={} orderID={} reason=second_account_slot_unavailable primary_role={:?} primary_slot={}",
                coid,
                order_id,
                primary_client.role(),
                primary_client.slot(),
            );
            let reply = self
                .shared
                .http_call_sync_on(primary_client, "GET", &path, "");
            drop(held_permits);
            return self.classify_order_lookup_reply(coid, order_id, reply);
        }

        let reply = permit.map_or_else(
            || self.shared.http_call_sync("GET", &path, ""),
            |permit| {
                self.shared
                    .http_call_sync_on(permit.current_pooled_client(), "GET", &path, "")
            },
        );
        self.classify_order_lookup_reply(coid, order_id, reply)
    }

    fn classify_order_lookup_reply(
        &self,
        coid: &str,
        order_id: &str,
        reply: HttpReply,
    ) -> FetchOrderResult {
        let json = match reply {
            Ok(j) => j,
            Err(e) => {
                // HTTP 404 is the only transport result classified as an
                // explicit not-found observation. A 425 or any other failure
                // is unavailable evidence and must not advance the universal
                // consecutive-not-found terminalization rule.
                if e.is_explicit_not_found() {
                    warn!(
                        "[PolymarketTrade] Reconcile /data/order/{}: {}",
                        order_id, e
                    );
                    return FetchOrderResult::NotFound(format!("http_status=404 response={}", e));
                }
                if e.is_http_425() {
                    self.shared.note_http_425_backoff(coid);
                }
                let unavailable = e.fetch_unavailable();
                warn!(
                    "[PolymarketTrade] Reconcile /data/order/{} unavailable={:?}: {}",
                    order_id, unavailable, e,
                );
                return FetchOrderResult::Unavailable(unavailable);
            }
        };
        classify_successful_order_lookup(&json, order_id)
    }
}

/// Context captured at cancel-kickoff time, threaded into
/// `handle_cancel_reply` so it can build the OrderUpdate without
/// re-querying internal maps after the recv races.
pub(crate) struct CancelCtx {
    pub local_oid: Option<String>,
    pub order_slot: OrderSlot,
    pub symbol: String,
    pub side: Side,
}

impl PolymarketTrade {
    /// Sign + pre-register + dispatch a single `POST /order` onto the
    /// async runtime. Returns either:
    ///   * `Ok((local_oid, rx))` — order in flight; caller awaits
    ///     `rx.recv()` and feeds the reply to `handle_submit_reply`.
    ///   * `Err(OrderUpdate)` — pre-rejected (rate limit / balance
    ///     backoff / sign error). Return as-is to the caller.
    ///
    /// Used by both `submit_order` (recv inline) and the parallel
    /// fan-out path in `batch_submit_orders` when
    /// `use_batch_orders=false`.
    pub(crate) fn submit_kickoff(
        &mut self,
        order: &OrderRequest,
    ) -> std::result::Result<(String, crossbeam_channel::Receiver<HttpReply>), OrderUpdate> {
        let PreparedSubmit {
            local_oid,
            body: body_str,
            ..
        } = match self.submit_prep(order, true) {
            Ok(prepared) => prepared,
            Err(update) => {
                self.shared.log_preflight_rejected(
                    &order.client_order_id,
                    update.exchange_order_id.as_deref(),
                    &update,
                );
                self.shared.forget_order_lifecycle(&order.client_order_id);
                return Err(update);
            }
        };
        let body_text = std::str::from_utf8(body_str.as_ref())
            .expect("Polymarket request serialization must produce UTF-8 JSON");
        let rx = self.shared.http_call_async("POST", "/order", body_text);
        if let Ok(buffer) = body_str.try_into_mut() {
            self.shared.recycle_request_buffer(buffer);
        }
        self.shared.log_order_lifecycle(
            &order.client_order_id,
            "http_dispatched",
            Some(&local_oid),
            None,
            None,
        );
        Ok((local_oid, rx))
    }

    // ── Fire-and-track + admission-bound dispatch ──────────────────
    // These mirror `submit_kickoff` / `cancel_kickoff` but dispatch on the
    // exact connection reserved by an admission permit (no round-robin, no
    // cold connection) and return an opaque handle so the reply is awaited
    // OFF the dispatch thread (the engine's completion drainer), never
    // block_on-ing a worker for the RTT.

    /// Fire-and-track place: sync prep + dispatch on the permit-bound
    /// `client`, WITHOUT blocking on the reply. `Ok(pending)` → complete
    /// off-thread via [`Self::complete_submit`]; `Err(update)` → a pre-flight
    /// reject (nothing was sent — the caller should release its permit).
    pub fn submit_fire(
        &mut self,
        order: &OrderRequest,
        client: crate::http1_pool::PooledClient,
    ) -> std::result::Result<PendingSubmit, OrderUpdate> {
        let prepared = match self.submit_prep(order, false) {
            Ok(prepared) => prepared,
            Err(update) => {
                self.shared.log_preflight_rejected(
                    &order.client_order_id,
                    update.exchange_order_id.as_deref(),
                    &update,
                );
                return Err(update);
            }
        };
        let attempt = client.begin_attempt(
            order.timestamp_ns,
            prepared.prep_ns,
            prepared.signed_ns,
            prepared.account_recorded_ns,
        );
        let (rx, timing) = self.shared.http_call_async_on_timed_bytes(
            client,
            attempt.attempt_id(),
            "POST",
            "/order",
            prepared.body,
        );
        let dispatched_ns = now_ns();
        attempt.mark_dispatched(dispatched_ns);
        let trace = attempt
            .snapshot()
            .expect("admission permit must retain its trace slot");
        crate::latency::record_ns(
            "polymarket.order.attempt_quote_to_prep",
            trace.prep_ns.saturating_sub(trace.signal_ns),
        );
        crate::latency::record_ns(
            "polymarket.order.attempt_prep_to_signed",
            trace.signed_ns.saturating_sub(trace.prep_ns),
        );
        crate::latency::record_ns(
            "polymarket.order.attempt_signed_to_reserve",
            trace.account_recorded_ns.saturating_sub(trace.signed_ns),
        );
        crate::latency::record_ns(
            "polymarket.order.attempt_reserve_to_http_dispatch",
            trace
                .dispatched_ns
                .saturating_sub(trace.account_recorded_ns),
        );
        Ok(PendingSubmit {
            local_oid: prepared.local_oid,
            rx,
            timing,
            attempt,
        })
    }

    /// Complete a fired place: block on its reply, then run the normal reply
    /// handler (open_orders / coid bookkeeping, balance-error trigger).
    pub fn complete_submit(&mut self, order: &OrderRequest, pending: PendingSubmit) -> OrderUpdate {
        let PendingSubmit {
            local_oid,
            rx,
            timing,
            attempt,
        } = pending;
        let completion_started_ns = now_ns();
        let reply = rx
            .recv()
            .unwrap_or_else(|_| Err(HttpErr::Transport("async reply dropped".to_string())));
        let response_received_ns = now_ns();
        crate::latency::record_ns(
            "polymarket.order.completion_wait_response",
            response_received_ns.saturating_sub(completion_started_ns),
        );
        let response_ready_ns = timing.response_ready_ns.load(Ordering::Acquire);
        if response_ready_ns > 0 {
            crate::latency::record_ns(
                "polymarket.order.response_ready_to_handler",
                response_received_ns.saturating_sub(response_ready_ns),
            );
        }
        let reply_enqueued_ns = timing.reply_enqueued_ns.load(Ordering::Acquire);
        if reply_enqueued_ns > 0 {
            crate::latency::record_ns(
                "polymarket.order.reply_enqueued_to_handler",
                response_received_ns.saturating_sub(reply_enqueued_ns),
            );
        }
        let handler_started = crate::latency::Instant::now();
        let update = self.handle_submit_reply_inner(order, &local_oid, reply, false);
        crate::latency::record("polymarket.order.response_handler", handler_started);
        self.shared.log_admission_attempt(
            &attempt,
            "place",
            Some(order),
            &self.instance_id,
            &order.client_order_id,
            Some(&local_oid),
            update.status,
            update.error.as_deref(),
        );
        update
    }

    /// Fire-and-track cancel: sync prep + exactly one dispatch on the
    /// permit-bound `client`. `PendingCancel.rx == None` when the coid had
    /// no local orderID (nothing to send).
    pub fn cancel_fire(
        &mut self,
        client_order_id: &str,
        client: crate::http1_pool::PooledClient,
    ) -> PendingCancel {
        let signal_ns = self.gen_ns_hint;
        let prep_ns = now_ns();
        let (ctx, body) = self.cancel_prep(client_order_id, false);
        let (rx, timing, attempt) = match body {
            PreparedCancelBody::Ready(body_bytes) => {
                let attempt = client.begin_attempt(signal_ns, prep_ns, 0, 0);
                let (rx, timing) = self.shared.http_call_async_on_timed_bytes(
                    client,
                    attempt.attempt_id(),
                    "DELETE",
                    "/order",
                    body_bytes,
                );
                (Some(rx), Some(timing), Some(attempt))
            }
            PreparedCancelBody::NoOrderId => (None, None, None),
            PreparedCancelBody::BufferUnavailable => {
                let (tx, rx) = crossbeam_channel::bounded(1);
                let _ = tx.send(Err(HttpErr::Transport(
                    "cancel request buffer pool exhausted before dispatch".to_string(),
                )));
                (Some(rx), None, None)
            }
        };
        let dispatched_ns = rx.as_ref().map(|_| now_ns()).unwrap_or(0);
        if let Some(attempt) = &attempt {
            attempt.mark_dispatched(dispatched_ns);
        }
        if dispatched_ns > 0 {
            crate::latency::record_ns(
                "polymarket.cancel.attempt_signal_to_prep",
                prep_ns.saturating_sub(signal_ns),
            );
            crate::latency::record_ns(
                "polymarket.cancel.attempt_prep_to_http_dispatch",
                dispatched_ns.saturating_sub(prep_ns),
            );
        }
        PendingCancel {
            ctx,
            rx,
            timing,
            attempt,
        }
    }

    /// Complete a fired cancel: block on its reply (if any), then run the
    /// normal cancel reply handler (drops local tracking on terminal states).
    pub fn complete_cancel(
        &mut self,
        exchange: Exchange,
        client_order_id: &str,
        pending: PendingCancel,
    ) -> OrderUpdate {
        let PendingCancel {
            ctx,
            rx,
            timing,
            attempt,
        } = pending;
        let completion_started_ns = now_ns();
        let reply = rx.map(|rx| {
            rx.recv()
                .unwrap_or_else(|_| Err(HttpErr::Transport("async reply dropped".to_string())))
        });
        let response_received_ns = now_ns();
        if reply.is_some() {
            crate::latency::record_ns(
                "polymarket.cancel.completion_wait_response",
                response_received_ns.saturating_sub(completion_started_ns),
            );
        }
        if let Some(timing) = timing {
            let response_ready_ns = timing.response_ready_ns.load(Ordering::Acquire);
            if response_ready_ns > 0 {
                crate::latency::record_ns(
                    "polymarket.cancel.response_ready_to_handler",
                    response_received_ns.saturating_sub(response_ready_ns),
                );
            }
            let reply_enqueued_ns = timing.reply_enqueued_ns.load(Ordering::Acquire);
            if reply_enqueued_ns > 0 {
                crate::latency::record_ns(
                    "polymarket.cancel.reply_enqueued_to_handler",
                    response_received_ns.saturating_sub(reply_enqueued_ns),
                );
            }
        }
        let handler_started = crate::latency::Instant::now();
        let update = self.handle_cancel_reply_inner(exchange, client_order_id, ctx, reply, false);
        crate::latency::record("polymarket.cancel.response_handler", handler_started);
        if let Some(attempt) = &attempt {
            self.shared.log_admission_attempt(
                attempt,
                "cancel",
                None,
                &self.instance_id,
                client_order_id,
                update.exchange_order_id.as_deref(),
                update.status,
                update.error.as_deref(),
            );
        }
        update
    }

    /// Synchronous prep for a place: rate/backoff gate, sign, register the
    /// orderID + `open_orders` entry, and return `(local_oid, body_json)`.
    /// Split out of `submit_kickoff` so the kickoff path can prep then
    /// dispatch separately. Returns the synthetic `OrderUpdate` on a
    /// pre-flight reject (rate-limit / backoff / sign).
    fn record_account_order(
        &self,
        order: &OrderRequest,
        local_oid: &str,
    ) -> std::result::Result<Option<OrderOwnership>, OrderUpdate> {
        // CLI/legacy one-off routes have no strategy instance. Engine-built
        // routes record durable ownership for private-feed routing/recovery,
        // but balance and inventory admission belong to StrategyAccount.
        if self.instance_id.is_empty() {
            return Ok(None);
        }
        self.shared
            .account_state
            .prepare_order_ownership_in_slot(
                &self.instance_id,
                &order.client_order_id,
                local_oid,
                &order.symbol,
                order.side,
                order.quantity,
                order.price.unwrap_or(0.0),
                order.fee_rate_bps,
                order.order_slot,
            )
            .map(Some)
            .map_err(|error| {
                Self::make_rejected(order, &format!("order ownership registration: {error}"))
            })
    }

    fn submit_prep(
        &mut self,
        order: &OrderRequest,
        legacy_trace: bool,
    ) -> std::result::Result<PreparedSubmit, OrderUpdate> {
        let prep_ns = now_ns();
        if legacy_trace {
            self.shared.register_order_lifecycle(order);
            self.shared.log_order_lifecycle(
                &order.client_order_id,
                "submit_prep",
                None,
                Some(OrderStatus::Pending),
                None,
            );
        }
        if !self.shared.check_rate_limit() {
            return Err(Self::make_rejected(order, "rate limited"));
        }
        if self.shared.in_trading_disabled_backoff() {
            return Err(Self::make_rejected(order, "trading disabled backoff"));
        }
        if super::network_incident::place_blocked_by_connection_failure_cluster() {
            return Err(Self::make_rejected(
                order,
                "connection failure cluster safety gate",
            ));
        }
        if self.shared.in_balance_backoff(&self.instance_id) {
            return Err(Self::make_rejected(order, "balance backoff"));
        }
        if self.shared.in_invalid_token_backoff(&order.symbol) {
            return Err(Self::make_rejected(order, "invalid token backoff"));
        }
        let (order_hash, body) = match self.sign_and_build_body(order) {
            Ok(v) => v,
            Err(e) => return Err(Self::make_rejected(order, &e.to_string())),
        };
        let local_oid = order_hash;
        let signed_ns = now_ns();
        if legacy_trace {
            self.shared.log_order_lifecycle(
                &order.client_order_id,
                "signed",
                Some(&local_oid),
                None,
                None,
            );
        }
        let Some(mut body_json) = self.shared.take_request_buffer() else {
            return Err(Self::make_rejected(
                order,
                "request buffer pool exhausted before dispatch",
            ));
        };
        body_json.clear();
        if let Err(e) = serde_json::to_writer((&mut body_json).writer(), &body) {
            self.shared.recycle_request_buffer(body_json);
            return Err(Self::make_rejected(
                order,
                &format!("body serialize: {}", e),
            ));
        }
        let ownership = match self.record_account_order(order, &local_oid) {
            Ok(ownership) => ownership,
            Err(update) => {
                self.shared.recycle_request_buffer(body_json);
                return Err(update);
            }
        };
        let account_recorded_ns = now_ns();
        if legacy_trace {
            self.shared.log_order_lifecycle(
                &order.client_order_id,
                "account_recorded",
                Some(&local_oid),
                Some(OrderStatus::Pending),
                None,
            );
        }
        if let Err(error) = self.shared.register_local_order_id(
            &order.client_order_id,
            &local_oid,
            &order.symbol,
            ownership,
        ) {
            self.shared.recycle_request_buffer(body_json);
            return Err(Self::make_rejected(order, &error));
        }
        // Track in `open_orders` BEFORE the HTTP call resolves: from this
        // point on the order may already be live on the server (a
        // POST landing but its reply timing out leaves an orphan-place
        // whose collateral the server holds against our allowance).
        // Inserting here makes `open_orders` the single source of truth
        // for "may be on the server" — `handle_balance_error` snapshots
        // it to issue targeted DELETEs, and `remove_order` is the
        // symmetric removal on Rejected (keeps the coid↔oid map for
        // a possible late fill). Order survives
        // here through Submit success / NewOrderTimeout / orphan
        // reconciliation; only definitive `Rejected` (server explicitly
        // refused, e.g. balance / fee / post-only) removes it.
        if let Err(error) = self.shared.track_open_order(
            &order.client_order_id,
            TrackedOrder {
                order_slot: order.order_slot,
                symbol: order.symbol.clone(),
                side: order.side,
                instance_id: self.instance_id.clone(),
            },
        ) {
            self.shared.recycle_request_buffer(body_json);
            return Err(Self::make_rejected(order, &error));
        }

        Ok(PreparedSubmit {
            local_oid,
            body: body_json.freeze(),
            prep_ns,
            signed_ns,
            account_recorded_ns,
        })
    }

    /// Parse the `POST /order` reply and produce an `OrderUpdate`.
    /// Side effects: open_orders insert on success, balance-backoff
    /// trigger on a balance reject, orderID re-register on mismatch.
    pub(crate) fn handle_submit_reply(
        &mut self,
        order: &OrderRequest,
        local_oid: &str,
        reply: HttpReply,
    ) -> OrderUpdate {
        self.handle_submit_reply_inner(order, local_oid, reply, true)
    }

    fn handle_submit_reply_inner(
        &mut self,
        order: &OrderRequest,
        local_oid: &str,
        reply: HttpReply,
        legacy_trace: bool,
    ) -> OrderUpdate {
        let update = (|| {
            let resp = match reply {
                Ok(r) => r,
                Err(e) if e.is_submit_unknown_state() => {
                    if matches!(e, HttpErr::Timeout) {
                        let detail = format!(
                            "operation=place target={} coid={}",
                            self.shared.clob_base_url, order.client_order_id,
                        );
                        super::network_incident::record(
                            super::network_incident::NetworkSignal::HttpPlaceTimeout,
                            &detail,
                        );
                    }
                    if e.is_http_425() {
                        self.shared.note_http_425_backoff(&order.client_order_id);
                    }
                    if self.shared.should_warn_unknown_state(&e) {
                        warn!("[PolymarketTrade] Order unknown state ({}) coid={} oid={} → NewOrderTimeout",
                        e, order.client_order_id, &local_oid[..18.min(local_oid.len())]);
                    }
                    if legacy_trace {
                        self.shared.defer_lifecycle_account_apply(
                            &order.client_order_id,
                            OrderStatus::NewOrderTimeout,
                        );
                    } else {
                        self.shared.defer_lifecycle_account_apply(
                            &order.client_order_id,
                            OrderStatus::NewOrderTimeout,
                        );
                    }
                    return Self::make_timeout_place(order, Some(local_oid));
                }
                Err(e) => {
                    let err_s = e.to_string();
                    if SharedState::is_trading_disabled_error(&err_s) {
                        self.handle_trading_disabled(&err_s);
                    } else if SharedState::is_balance_error(&err_s) {
                        self.handle_balance_error(
                            &order.client_order_id,
                            order.side,
                            &order.symbol,
                        );
                    } else if SharedState::is_invalid_token_error(&err_s) {
                        self.handle_invalid_token(&order.symbol);
                    }
                    if legacy_trace {
                        self.shared
                            .remove_order_as(&order.client_order_id, OrderStatus::Rejected);
                    } else {
                        self.shared.defer_lifecycle_account_apply(
                            &order.client_order_id,
                            OrderStatus::Rejected,
                        );
                    }
                    if legacy_trace && e.is_definitive_submit_rejection() {
                        warn!(
                            "[PolymarketTrade] Order server-rejected: {} coid={} → Rejected",
                            e, order.client_order_id
                        );
                    } else if legacy_trace {
                        warn!(
                            "[PolymarketTrade] Local order failure: {} coid={}",
                            e, order.client_order_id
                        );
                    }
                    return Self::make_rejected(order, &err_s);
                }
            };

            let response_parse_started = crate::latency::Instant::now();
            let parsed_result = parse_placement_response(&resp);
            crate::latency::record("polymarket.order.response_parse", response_parse_started);
            let parsed = match parsed_result {
                Ok(parsed) => parsed,
                Err(reason) => {
                    if legacy_trace {
                        self.shared.defer_lifecycle_account_apply(
                            &order.client_order_id,
                            OrderStatus::NewOrderTimeout,
                        );
                        warn!(
                        "[PolymarketTrade] ambiguous HTTP 2xx placement response coid={} local={} reason={} body={} → NewOrderTimeout",
                        order.client_order_id,
                        local_oid,
                        reason,
                        resp,
                    );
                    } else {
                        self.shared.defer_lifecycle_account_apply(
                            &order.client_order_id,
                            OrderStatus::NewOrderTimeout,
                        );
                    }
                    return Self::make_timeout_place(order, Some(local_oid));
                }
            };
            let success = parsed.success;
            let order_id = parsed.order_id;
            let status_str = parsed.status;
            let error_msg = parsed.error_msg;

            if !success {
                if legacy_trace {
                    self.shared
                        .remove_order_as(&order.client_order_id, OrderStatus::Rejected);
                } else {
                    self.shared.defer_lifecycle_account_apply(
                        &order.client_order_id,
                        OrderStatus::Rejected,
                    );
                }
                if SharedState::is_trading_disabled_error(&error_msg) {
                    self.handle_trading_disabled(&error_msg);
                } else if SharedState::is_balance_error(&error_msg) {
                    self.handle_balance_error(&order.client_order_id, order.side, &order.symbol);
                } else if SharedState::is_invalid_token_error(&error_msg) {
                    self.handle_invalid_token(&order.symbol);
                }
                if legacy_trace {
                    warn!(
                        "[PolymarketTrade] Order rejected by server: {} coid={} → Rejected",
                        error_msg, order.client_order_id
                    );
                }
                return Self::make_rejected(order, &error_msg);
            }
            // Accepted by the server → token is registered/tradeable; clear any
            // invalid-token strikes/backoff for it.
            self.shared.clear_invalid_token(&order.symbol);
            let account_apply_started = crate::latency::Instant::now();
            let effective_ack_status = if legacy_trace {
                self.shared.mark_order_live(
                    &order.client_order_id,
                    order.order_slot,
                    &order.symbol,
                    order.side,
                    &self.instance_id,
                    OrderStatus::Accepted,
                )
            } else {
                // Runtime ownership/open-order publication happened before HTTP
                // dispatch. StrategyAccount is the lifecycle authority; durable
                // shared monitoring follows on its cold single-writer lane.
                self.shared
                    .defer_lifecycle_account_apply(&order.client_order_id, OrderStatus::Accepted);
                Some(OrderStatus::Accepted)
            };
            crate::latency::record(
                "polymarket.order.response_account_apply",
                account_apply_started,
            );

            if !order_id.is_empty() && !Self::oid_eq(&order_id, local_oid) {
                warn!(
                "[PolymarketTrade] orderID MISMATCH coid={} local={} server={} — local hash is wrong!",
                order.client_order_id, local_oid, order_id,
            );
                self.shared
                    .register_order_id(&order.client_order_id, &order_id, &order.symbol);
            }

            // `mark_order_live` idempotently restores `open_orders` and account
            // collateral if an out-of-order cancellation arrived first.

            // Map HTTP `status` → local OrderStatus and book-keeping fields.
            //
            // `matched`: the server reports the order matched fully on submit
            // (no resting). The WS user_feed will deliver the authoritative
            // fill ~300 ms later carrying the real `trade_id` and price; that
            // push books the ledger entry. We emit a placeholder `Filled`
            // *now* with `filled_quantity = 0.0` so:
            //   - OrderManager removes the order from `self.orders` immediately
            //     (its Filled branch ignores filled_quantity), eliminating the
            //     "matched orders can't be canceled" race where the strategy
            //     emits a Cancel signal off stale OM state in the 300 ms gap.
            //   - PositionManager's ledger ingestion is gated by
            //     `filled_quantity > 0.0` (see strategy/polymaker/strategy.rs
            //     ~ line 4781) so the placeholder does NOT double-count
            //     volume / cashflow / fees. The real WS push (trade_id present,
            //     filled_quantity = qty) lands the trade exactly once.
            // Polymaker treats this zero-quantity terminal as a placement orphan
            // and immediately performs an order-specific REST lookup. A complete
            // MATCHED/FILLED audit then moves the residual into its
            // `UnauditedMatchedOrders` bridge; the POST response itself does not
            // create a parallel inventory cache.
            match status_str.as_str() {
                "matched" => {
                    let trade_ids = resp
                        .get("tradeIDs")
                        .and_then(|v| v.as_array())
                        .map(|ids| ids.len())
                        .unwrap_or(0);
                    debug!(
                        "[PolymarketTrade] Matched immediately: orderID={} trades={} \
                       (emitting placeholder Filled for immediate order REST audit; \
                       ledger updated via authoritative trade updates)",
                        order_id, trade_ids
                    );
                }
                "delayed" => {
                    debug!("[PolymarketTrade] Deferred execution: orderID={}", order_id);
                }
                _ => {}
            }
            let status = placement_response_status(true, &status_str, effective_ack_status);
            // The admission-bound path returns this update to the sole-writer
            // StrategyAccount, which already owns the current lifecycle state.
            // Reading the cold shared account here can block behind private-trade
            // persistence for tens of milliseconds. Legacy synchronous callers
            // still need their effective shared-state quantity.
            let effective_remaining = if legacy_trace {
                self.shared
                    .account_state
                    .order(&order.client_order_id)
                    .map(|owned| (owned.quantity - owned.filled_quantity).max(0.0))
                    .unwrap_or(order.quantity)
            } else {
                order.quantity
            };
            let (filled_quantity, remaining_quantity) = if status == OrderStatus::Filled {
                (0.0, 0.0)
            } else {
                (0.0, effective_remaining)
            };

            debug!(
                "[PolymarketTrade] Order accepted: orderID={} status={} coid={}",
                order_id, status_str, order.client_order_id
            );

            OrderUpdate {
                order_slot: order.order_slot,
                client_order_id: order.client_order_id.clone(),
                exchange: Exchange::Polymarket,
                symbol: order.symbol.clone(),
                side: order.side,
                exchange_order_id: Some(if order_id.is_empty() {
                    local_oid.to_string()
                } else {
                    order_id
                }),
                status,
                liquidity: None,
                filled_quantity,
                remaining_quantity,
                // Carry the resting price on an `Accepted` reply so a resurrection
                // (PositionManager::sync_pending_from_update + OrderManager) can
                // re-lock / re-track at the right price if a pending/delayed cancel
                // race already dropped this order. Mirrors the placement-reconcile
                // "LIVE" arm (which already sets avg_fill_price = price). Harmless
                // for the normal path: an Accepted has filled_quantity = 0, so the
                // PM ledger's `filled_quantity > 0` gate ignores it; 0.0 for the
                // `matched`→Filled placeholder (the WS push books the real price).
                avg_fill_price: if status == OrderStatus::Accepted {
                    order.price.unwrap_or(0.0)
                } else {
                    0.0
                },
                timestamp_ns: now_ns(),
                exchange_event_timestamp_ns: None,
                trade_id: None,
                order_audit: None,
                error: None,
            }
        })();
        if legacy_trace {
            self.shared.log_order_lifecycle(
                &order.client_order_id,
                "http_response",
                update.exchange_order_id.as_deref().or(Some(local_oid)),
                Some(update.status),
                None,
            );
        }
        update
    }

    /// Look up local state for `coid`, dispatch a `DELETE /order` (or
    /// none if no orderID is mapped), and return:
    ///   * `(ctx, Some(rx))` — request in flight
    ///   * `(ctx, None)`     — nothing to send; emit Cancelled directly.
    pub(crate) fn cancel_kickoff(
        &mut self,
        client_order_id: &str,
    ) -> (CancelCtx, Option<crossbeam_channel::Receiver<HttpReply>>) {
        let (ctx, body) = self.cancel_prep(client_order_id, true);
        match body {
            PreparedCancelBody::Ready(body_bytes) => {
                let body_text = std::str::from_utf8(body_bytes.as_ref())
                    .expect("Polymarket cancel serialization must produce UTF-8 JSON");
                let rx = self.shared.http_call_async("DELETE", "/order", body_text);
                if let Ok(buffer) = body_bytes.try_into_mut() {
                    self.shared.recycle_request_buffer(buffer);
                }
                self.shared.log_order_lifecycle(
                    client_order_id,
                    "cancel_dispatched",
                    ctx.local_oid.as_deref(),
                    None,
                    None,
                );
                (ctx, Some(rx))
            }
            PreparedCancelBody::NoOrderId => {
                self.shared.log_order_lifecycle(
                    client_order_id,
                    "cancel_not_dispatched",
                    None,
                    None,
                    None,
                );
                (ctx, None)
            }
            PreparedCancelBody::BufferUnavailable => {
                let (tx, rx) = crossbeam_channel::bounded(1);
                let _ = tx.send(Err(HttpErr::Transport(
                    "cancel request buffer pool exhausted before dispatch".to_string(),
                )));
                self.shared.log_order_lifecycle(
                    client_order_id,
                    "cancel_not_dispatched_buffer_unavailable",
                    ctx.local_oid.as_deref(),
                    None,
                    None,
                );
                (ctx, Some(rx))
            }
        }
    }

    /// Synchronous prep for a cancel: resolve the server orderID, build the
    /// `CancelCtx` (for reply handling), and return the DELETE body string
    /// (or `None` when the coid has no local orderID → nothing to send).
    /// Prep half of `cancel_kickoff`: resolve the server orderID + tracked
    /// symbol/side and build the DELETE body (None = nothing to send).
    pub(crate) fn cancel_prep(
        &mut self,
        client_order_id: &str,
        legacy_trace: bool,
    ) -> (CancelCtx, PreparedCancelBody) {
        let prep_ns = now_ns();
        if self.gen_ns_hint > 0 {
            crate::latency::record_ns(
                "polymarket.cancel.signal_to_prep",
                prep_ns.saturating_sub(self.gen_ns_hint),
            );
        }
        if legacy_trace {
            self.shared
                .record_cancel_signal(client_order_id, self.gen_ns_hint);
            self.shared
                .log_order_lifecycle(client_order_id, "cancel_prep", None, None, None);
        }
        let execution = self.shared.execution_snapshot();
        let order_id = execution.coid_to_oid.get(client_order_id).cloned();
        let tracked = execution.open_orders.get(client_order_id).cloned();
        let (order_slot, symbol, side) = tracked
            .map(|t| (t.order_slot, t.symbol, t.side))
            .unwrap_or_else(|| (OrderSlot::UNASSIGNED, String::new(), Side::Buy));
        let ctx = CancelCtx {
            local_oid: order_id.clone(),
            order_slot,
            symbol,
            side,
        };
        match order_id {
            Some(ref oid) => {
                let oid_short = &oid[..16.min(oid.len())];
                // `gen_ns` = strategy on_quote emission time (ns) of the
                // cancel/replace signal being dispatched (set by the engine on
                // this route just before the call). Pairs this cancel with its
                // quote for offline on_quote→dispatch latency analysis
                // (dispatch wall-clock − gen_ns). 0 = non-quote origin.
                debug!(
                    "[PolymarketTrade] Cancel request orderID={}... coid={} gen_ns={}",
                    oid_short, client_order_id, self.gen_ns_hint
                );
                let Some(mut body) = self.shared.take_request_buffer() else {
                    return (ctx, PreparedCancelBody::BufferUnavailable);
                };
                body.clear();
                if let Err(error) =
                    serde_json::to_writer((&mut body).writer(), &CancelBody { order_id: oid })
                {
                    self.shared.recycle_request_buffer(body);
                    warn!(
                        "[PolymarketTrade] pooled cancel serialization failed coid={}; cancel not dispatched: {}",
                        client_order_id,
                        error,
                    );
                    return (ctx, PreparedCancelBody::BufferUnavailable);
                }
                (ctx, PreparedCancelBody::Ready(body.freeze()))
            }
            None => {
                info!(
                    "[PolymarketTrade] Cancel coid={} — no orderID locally, nothing to send",
                    client_order_id
                );
                (ctx, PreparedCancelBody::NoOrderId)
            }
        }
    }

    /// Parse the `DELETE /order` reply (or absence thereof) and build
    /// an OrderUpdate. Drops local tracking on terminal outcomes.
    pub(crate) fn handle_cancel_reply(
        &mut self,
        exchange: Exchange,
        client_order_id: &str,
        ctx: CancelCtx,
        reply: Option<HttpReply>,
    ) -> OrderUpdate {
        self.handle_cancel_reply_inner(exchange, client_order_id, ctx, reply, true)
    }

    fn handle_cancel_reply_inner(
        &mut self,
        exchange: Exchange,
        client_order_id: &str,
        ctx: CancelCtx,
        reply: Option<HttpReply>,
        legacy_trace: bool,
    ) -> OrderUpdate {
        let update = (|| {
            let CancelCtx {
                local_oid,
                order_slot,
                symbol,
                side,
            } = ctx;
            let classify_started = crate::latency::Instant::now();
            // Worst-case default: the order is still live. Only an explicit
            // terminal response below is allowed to remove tracking.
            let mut should_remove = false;
            let mut orphan_status: Option<OrderStatus> = None;
            let mut ok_status = OrderStatus::Accepted;

            if let Some(reply) = reply {
                let oid_ref = local_oid.as_deref().unwrap_or("");
                let oid_short = &oid_ref[..16.min(oid_ref.len())];
                match reply {
                    Ok(resp) => {
                        let canceled = resp.get("canceled").and_then(|v| v.as_array());
                        let canceled_n = canceled.map(|a| a.len()).unwrap_or(0);
                        let not_canceled = resp.get("not_canceled").and_then(|v| v.as_object());
                        let nc_n = not_canceled.map(|o| o.len()).unwrap_or(0);
                        debug!(
                        "[PolymarketTrade] Cancel result orderID={}... coid={} canceled={} not_canceled={}",
                        oid_short, client_order_id, canceled_n, nc_n,
                    );
                        let explicitly_canceled = canceled
                            .map(|a| a.iter().filter_map(|v| v.as_str()).any(|id| id == oid_ref))
                            .unwrap_or(false);
                        let matching_reason = not_canceled
                            .and_then(|nc| nc.get(oid_ref))
                            .and_then(|reason| reason.as_str());
                        if let Some(nc) = not_canceled {
                            for (id, reason) in nc {
                                let reason_str = reason.as_str().unwrap_or("");
                                debug!(
                                    "[PolymarketTrade] Cancel rejected: {} reason={} coid={}",
                                    id, reason_str, client_order_id
                                );
                            }
                        }
                        if !oid_ref.is_empty() {
                            // The initial/ordinary cancel establishes the orphan
                            // but does not consume one of its three reconcile
                            // DELETE observations.
                            let outcome = cancel_delete_response_outcome(&resp, oid_ref);
                            match outcome {
                                CancelReasonOutcome::Cancelled => {
                                    should_remove = true;
                                    ok_status = OrderStatus::Cancelled;
                                }
                                CancelReasonOutcome::Filled => {
                                    should_remove = true;
                                    ok_status = OrderStatus::Filled;
                                }
                                CancelReasonOutcome::Uncertain => {
                                    if matching_reason.is_some_and(is_pending_delayed_reason) {
                                        self.shared
                                            .pending_delayed_orphans
                                            .insert(client_order_id.to_string());
                                    }
                                    if explicitly_canceled {
                                        warn!("[PolymarketTrade] Cancel response contradictory for coid={} orderID={}... (canceled + not_canceled reason={}) → orphan",
                                        client_order_id, oid_short, matching_reason.unwrap_or(""));
                                    } else if let Some(reason) = matching_reason {
                                        debug!("[PolymarketTrade] Cancel reply uncertain (reason={}) coid={} → orphan",
                                        reason, client_order_id);
                                    } else {
                                        warn!("[PolymarketTrade] Cancel response omitted coid={} orderID={}... → orphan",
                                        client_order_id, oid_short);
                                    }
                                    // Reply received but ambiguous — orphan as
                                    // CancelUncertain, NOT a transport timeout.
                                    orphan_status = Some(OrderStatus::CancelUncertain);
                                }
                            }
                        } else {
                            // No exchange OID means we cannot reconcile by ID.
                            // Preserve the local Active state so the normal refresh
                            // path can retry once the mapping appears.
                        }
                    }
                    Err(e) if e.is_unknown_state() => {
                        if matches!(e, HttpErr::Timeout) {
                            let detail = format!(
                                "operation=cancel target={} coid={}",
                                self.shared.clob_base_url, client_order_id,
                            );
                            super::network_incident::record(
                                super::network_incident::NetworkSignal::HttpCancelTimeout,
                                &detail,
                            );
                        }
                        // 425 falls through here too (per `is_unknown_state`); the
                        // dedup helper suppresses repeats of 425 storms within 5
                        // min. Timeouts / 5xx always WARN.
                        if self.shared.should_warn_unknown_state(&e) {
                            warn!("[PolymarketTrade] Cancel unknown state ({}) coid={} orderID={}... → CancelOrderTimeout",
                            e, client_order_id, oid_short);
                        }
                        // HTTP 425 backs off reconciliation for this orphan only.
                        // The order still becomes an orphan (we cannot know if the
                        // cancel landed), while unrelated audits keep running.
                        if matches!(e, HttpErr::Status(425, _)) {
                            self.shared.note_http_425_backoff(client_order_id);
                        }
                        should_remove = false;
                        orphan_status = Some(OrderStatus::CancelOrderTimeout);
                    }
                    Err(e) => {
                        // Genuine 4xx rejection (post-425-routing): no dedup —
                        // these are per-request anomalies the operator should see.
                        warn!("[PolymarketTrade] Cancel HTTP error, will retry: {} coid={} orderID={}...",
                        e, client_order_id, oid_short);
                        should_remove = false;
                    }
                }
            }
            crate::latency::record("polymarket.cancel.response_classify", classify_started);

            if let Some(status) = orphan_status {
                let account_apply_started = crate::latency::Instant::now();
                let effective = if legacy_trace {
                    self.shared
                        .effective_cancel_attempt_status(client_order_id, status)
                } else {
                    self.shared
                        .defer_lifecycle_account_apply(client_order_id, status);
                    status
                };
                let mut update =
                    Self::make_orphan_cancel(client_order_id, &symbol, side, local_oid, effective);
                update.order_slot = order_slot;
                crate::latency::record(
                    "polymarket.cancel.response_account_apply",
                    account_apply_started,
                );
                return update;
            }
            let account_apply_started = crate::latency::Instant::now();
            if should_remove {
                if !legacy_trace && ok_status == OrderStatus::Cancelled {
                    self.shared.remove_cancelled_order_runtime(client_order_id);
                    self.shared
                        .defer_lifecycle_account_apply(client_order_id, ok_status);
                } else {
                    self.shared.remove_order_as(client_order_id, ok_status);
                }
            }
            let status = if should_remove {
                ok_status
            } else {
                OrderStatus::Accepted
            };
            let status = if legacy_trace || (should_remove && ok_status != OrderStatus::Cancelled) {
                self.shared
                    .effective_cancel_attempt_status(client_order_id, status)
            } else {
                status
            };

            let update = OrderUpdate {
                order_slot,
                client_order_id: client_order_id.to_string(),
                exchange,
                symbol,
                side,
                exchange_order_id: local_oid,
                status,
                liquidity: None,
                filled_quantity: 0.0,
                remaining_quantity: 0.0,
                avg_fill_price: 0.0,
                timestamp_ns: now_ns(),
                exchange_event_timestamp_ns: None,
                trade_id: None,
                order_audit: None,
                error: None,
            };
            crate::latency::record(
                "polymarket.cancel.response_account_apply",
                account_apply_started,
            );
            update
        })();
        if legacy_trace {
            self.shared.log_order_lifecycle(
                client_order_id,
                "cancel_response",
                update.exchange_order_id.as_deref(),
                Some(update.status),
                None,
            );
        }
        update
    }
}

impl ExchangeTrade for PolymarketTrade {
    fn submit_order(&mut self, order: &OrderRequest) -> Result<OrderUpdate> {
        let (local_oid, rx) = match self.submit_kickoff(order) {
            Ok(v) => v,
            Err(update) => return Ok(update),
        };
        let reply = rx
            .recv()
            .unwrap_or_else(|_| Err(HttpErr::Transport("async reply dropped".to_string())));
        Ok(self.handle_submit_reply(order, &local_oid, reply))
    }

    fn cancel_order(&mut self, exchange: Exchange, client_order_id: &str) -> Result<OrderUpdate> {
        let (ctx, rx_opt) = self.cancel_kickoff(client_order_id);
        let reply = rx_opt.map(|rx| {
            rx.recv()
                .unwrap_or_else(|_| Err(HttpErr::Transport("async reply dropped".to_string())))
        });
        Ok(self.handle_cancel_reply(exchange, client_order_id, ctx, reply))
    }

    fn cancel_all(&mut self, exchange: Exchange, symbol: &str) -> Result<Vec<OrderUpdate>> {
        // Collect all open order IDs for this symbol
        let mut order_ids: Vec<String> = Vec::new();
        let mut coids: Vec<String> = Vec::new();
        {
            let execution = self.shared.execution_snapshot();
            let open = &execution.open_orders;
            let coid_to_oid = &execution.coid_to_oid;
            for (coid, tracked) in open.iter() {
                if tracked.symbol == symbol
                    && (self.instance_id.is_empty() || tracked.instance_id == self.instance_id)
                {
                    if let Some(oid) = coid_to_oid.get(coid) {
                        order_ids.push(oid.clone());
                        coids.push(coid.clone());
                    }
                }
            }
        }

        if order_ids.is_empty() {
            return Ok(vec![]);
        }

        info!(
            "[PolymarketTrade] Cancel all request: {} orders for {}",
            order_ids.len(),
            symbol
        );

        // Batch cancel (up to 3000)
        let body = serde_json::Value::Array(
            order_ids
                .iter()
                .map(|id| serde_json::Value::String(id.clone()))
                .collect(),
        );
        let (response, fallback_status) = match self.delete_detailed("/orders", &body) {
            Ok(resp) => {
                let canceled_n = resp
                    .get("canceled")
                    .and_then(|v| v.as_array())
                    .map(|a| a.len())
                    .unwrap_or(0);
                let nc_n = resp
                    .get("not_canceled")
                    .and_then(|v| v.as_object())
                    .map(|o| o.len())
                    .unwrap_or(0);
                info!(
                    "[PolymarketTrade] Cancel all result for {}: canceled={} not_canceled={}",
                    symbol, canceled_n, nc_n
                );
                // Reply received; orders the response doesn't explicitly
                // resolve are ambiguous, not timed out.
                (Some(resp), OrderStatus::CancelUncertain)
            }
            Err(e) if e.is_unknown_state() => {
                if matches!(e, HttpErr::Timeout) {
                    let detail = format!(
                        "operation=cancel_all target={} orders={}",
                        self.shared.clob_base_url,
                        coids.len(),
                    );
                    super::network_incident::record(
                        super::network_incident::NetworkSignal::HttpCancelTimeout,
                        &detail,
                    );
                }
                if matches!(e, HttpErr::Status(425, _)) {
                    for coid in &coids {
                        self.shared.note_http_425_backoff(coid);
                    }
                }
                warn!(
                    "[PolymarketTrade] Cancel all unknown state: {} — keeping orders as orphans",
                    e
                );
                (None, OrderStatus::CancelOrderTimeout)
            }
            Err(e) => {
                warn!(
                    "[PolymarketTrade] Cancel all rejected: {} — keeping orders live for retry",
                    e
                );
                (None, OrderStatus::Accepted)
            }
        };

        // Resolve each OID independently. A successful batch response may
        // omit or contradict one order; only explicit per-order terminal
        // evidence may release tracking and collateral.
        let mut updates = Vec::new();
        for (coid, order_id) in coids.iter().zip(order_ids.iter()) {
            let tracked = self
                .shared
                .execution_snapshot()
                .open_orders
                .get(coid)
                .cloned();
            let status = response
                .as_ref()
                .map(|resp| {
                    // Ordinary cancel-all responses do not advance the bounded
                    // reconcile DELETE counter.
                    match cancel_delete_response_outcome(resp, order_id) {
                        CancelReasonOutcome::Cancelled => OrderStatus::Cancelled,
                        CancelReasonOutcome::Filled => OrderStatus::Filled,
                        CancelReasonOutcome::Uncertain => OrderStatus::CancelUncertain,
                    }
                })
                .unwrap_or(fallback_status);
            let status = self.shared.effective_cancel_attempt_status(coid, status);
            if matches!(
                status,
                OrderStatus::CancelUncertain | OrderStatus::CancelOrderTimeout
            ) {
                if let Some(reason) = response
                    .as_ref()
                    .and_then(|resp| resp.get("not_canceled"))
                    .and_then(|v| v.as_object())
                    .and_then(|nc| nc.get(order_id))
                    .and_then(|v| v.as_str())
                {
                    if is_pending_delayed_reason(reason) {
                        self.shared.pending_delayed_orphans.insert(coid.clone());
                    }
                }
            }
            if matches!(status, OrderStatus::Cancelled | OrderStatus::Filled) {
                self.shared.remove_order_as(coid, status);
            }
            updates.push(OrderUpdate {
                order_slot: Default::default(),
                client_order_id: coid.clone(),
                exchange,
                symbol: symbol.to_string(),
                side: tracked.map(|t| t.side).unwrap_or(Side::Buy),
                exchange_order_id: Some(order_id.clone()),
                status,
                liquidity: None,
                filled_quantity: 0.0,
                remaining_quantity: 0.0,
                avg_fill_price: 0.0,
                timestamp_ns: now_ns(),
                exchange_event_timestamp_ns: None,
                trade_id: None,
                order_audit: None,
                error: None,
            });
        }

        Ok(updates)
    }

    fn batch_submit_orders(
        &mut self,
        _market_id: &str,
        orders: &[OrderRequest],
    ) -> Result<Vec<OrderUpdate>> {
        if self.shared.in_trading_disabled_backoff() {
            return Ok(self.make_logged_preflight_rejections(orders, "trading disabled backoff"));
        }
        if super::network_incident::place_blocked_by_connection_failure_cluster() {
            return Ok(self.make_logged_preflight_rejections(
                orders,
                "connection failure cluster safety gate",
            ));
        }
        // Balance-backoff short-circuit (see `submit_order` for rationale).
        if self.shared.in_balance_backoff(&self.instance_id) {
            return Ok(self.make_logged_preflight_rejections(orders, "balance backoff"));
        }
        // Single-endpoint mode: per `use_batch_orders=false`, dispatch
        // each order through `POST /order` concurrently — kickoff all
        // requests first (each call returns immediately, the HTTP work
        // runs on the shared async runtime; reqwest h2 multiplexes them
        // onto a single TCP connection), then drain the receivers in
        // order. Critical path = max single-RTT, not sum of singles.
        if !self.shared.use_batch_orders {
            let mut updates: Vec<OrderUpdate> = Vec::with_capacity(orders.len());
            // (idx, local_oid, rx) for each in-flight request; pre-rejected
            // orders go straight into `updates` and are merged at the end.
            let mut waiters: Vec<(usize, String, crossbeam_channel::Receiver<HttpReply>)> =
                Vec::with_capacity(orders.len());
            // Indexed slot per input order so we can preserve caller order
            // when stitching pre-rejected updates with awaited ones.
            let mut slots: Vec<Option<OrderUpdate>> = (0..orders.len()).map(|_| None).collect();
            for (idx, o) in orders.iter().enumerate() {
                match self.submit_kickoff(o) {
                    Ok((local_oid, rx)) => waiters.push((idx, local_oid, rx)),
                    Err(rejected) => slots[idx] = Some(rejected),
                }
            }
            for (idx, local_oid, rx) in waiters {
                let reply = rx
                    .recv()
                    .unwrap_or_else(|_| Err(HttpErr::Transport("async reply dropped".to_string())));
                slots[idx] = Some(self.handle_submit_reply(&orders[idx], &local_oid, reply));
            }
            for slot in slots {
                if let Some(u) = slot {
                    updates.push(u);
                }
            }
            return Ok(updates);
        }
        // Polymarket batch limit: 15 orders
        let mut all_updates = Vec::new();
        for chunk in orders.chunks(15) {
            // Sign each chunk member, keeping the SignedOrder around so
            // we can pre-register coid↔orderID before POST and pass the
            // pre-computed orderID into any timeout path. An order that
            // fails local validation (e.g. invalid price) is dropped
            // here — it gets a Rejected update at the end of the chunk
            // and never enters the HTTP request.
            let mut signed_hashes: Vec<String> = Vec::with_capacity(chunk.len());
            let mut bodies: Vec<serde_json::Value> = Vec::with_capacity(chunk.len());
            // `body_to_chunk[i]` is the index within `chunk` of the i-th
            // successfully-signed order. Response index i maps back via
            // this to `chunk[body_to_chunk[i]]`.
            let mut body_to_chunk: Vec<usize> = Vec::with_capacity(chunk.len());
            for (idx, o) in chunk.iter().enumerate() {
                self.shared.register_order_lifecycle(o);
                self.shared.log_order_lifecycle(
                    &o.client_order_id,
                    "submit_prep",
                    None,
                    Some(OrderStatus::Pending),
                    None,
                );
                match self.sign_and_build_body(o) {
                    Ok((order_hash, b)) => {
                        self.shared.log_order_lifecycle(
                            &o.client_order_id,
                            "signed",
                            Some(&order_hash),
                            None,
                            None,
                        );
                        let b = serde_json::to_value(&b).unwrap_or_default();
                        let ownership = match self.record_account_order(o, &order_hash) {
                            Ok(ownership) => ownership,
                            Err(rejected) => {
                                self.shared.log_preflight_rejected(
                                    &o.client_order_id,
                                    Some(&order_hash),
                                    &rejected,
                                );
                                self.shared.forget_order_lifecycle(&o.client_order_id);
                                all_updates.push(rejected);
                                continue;
                            }
                        };
                        self.shared.log_order_lifecycle(
                            &o.client_order_id,
                            "account_recorded",
                            Some(&order_hash),
                            Some(OrderStatus::Pending),
                            None,
                        );
                        // Pre-register BEFORE the HTTP call so the map
                        // survives a timeout / dropped ack. Same
                        // open_orders insert as `submit_kickoff` —
                        // makes the map the single source of truth
                        // for "may be live on the server".
                        if let Err(error) = self.shared.register_local_order_id(
                            &o.client_order_id,
                            &order_hash,
                            &o.symbol,
                            ownership,
                        ) {
                            let rejected = Self::make_rejected(o, &error);
                            self.shared.log_preflight_rejected(
                                &o.client_order_id,
                                Some(&order_hash),
                                &rejected,
                            );
                            self.shared.forget_order_lifecycle(&o.client_order_id);
                            all_updates.push(rejected);
                            continue;
                        }
                        if let Err(error) = self.shared.track_open_order(
                            &o.client_order_id,
                            TrackedOrder {
                                order_slot: o.order_slot,
                                symbol: o.symbol.clone(),
                                side: o.side,
                                instance_id: self.instance_id.clone(),
                            },
                        ) {
                            let rejected = Self::make_rejected(o, &error);
                            self.shared.remove_order_resolved_as(
                                &o.client_order_id,
                                OrderStatus::Rejected,
                            );
                            self.shared.log_preflight_rejected(
                                &o.client_order_id,
                                Some(&order_hash),
                                &rejected,
                            );
                            self.shared.forget_order_lifecycle(&o.client_order_id);
                            all_updates.push(rejected);
                            continue;
                        }
                        signed_hashes.push(order_hash);
                        bodies.push(b);
                        body_to_chunk.push(idx);
                    }
                    Err(e) => {
                        warn!(
                            "[PolymarketTrade] sign failed coid={}: {} — skipping",
                            o.client_order_id, e,
                        );
                        let rejected = Self::make_rejected(o, &e.to_string());
                        self.shared
                            .log_preflight_rejected(&o.client_order_id, None, &rejected);
                        self.shared.forget_order_lifecycle(&o.client_order_id);
                        all_updates.push(rejected);
                    }
                }
            }

            if bodies.is_empty() {
                continue;
            }

            // Single order → POST /order with the single object; multiple →
            // POST /orders with an array. POST /order returns the order
            // object directly; POST /orders returns an array of per-order
            // results. Normalize both into `responses: Vec<Value>` below.
            let (path, body) = if bodies.len() == 1 {
                ("/order", bodies[0].clone())
            } else {
                ("/orders", serde_json::Value::Array(bodies.clone()))
            };
            let chunk_coids: Vec<String> =
                chunk.iter().map(|o| o.client_order_id.clone()).collect();
            let details: Vec<String> = chunk.iter().map(|o| format_order_brief(o)).collect();
            debug!(
                "[PolymarketTrade] Submit request: {} orders [{}]",
                bodies.len(),
                details.join(", "),
            );
            for (body_index, order_hash) in signed_hashes.iter().enumerate() {
                let order = &chunk[body_to_chunk[body_index]];
                self.shared.log_order_lifecycle(
                    &order.client_order_id,
                    "http_dispatched",
                    Some(order_hash),
                    None,
                    None,
                );
            }
            match self.post_detailed(path, &body) {
                Ok(resp) => {
                    let responses: Vec<serde_json::Value> = if resp.is_array() {
                        resp.as_array().cloned().unwrap_or_default()
                    } else {
                        vec![resp]
                    };
                    let mut accepted_coids: Vec<String> = Vec::new();
                    let mut rejected_coids: Vec<String> = Vec::new();
                    for i in 0..bodies.len() {
                        // Response[i] pairs with bodies[i] / signed_orders[i];
                        // the chunk entry is chunk[body_to_chunk[i]].
                        let order = &chunk[body_to_chunk[i]];
                        let local_oid = &signed_hashes[i];
                        let Some(r) = responses.get(i) else {
                            self.shared.defer_lifecycle_account_apply(
                                &order.client_order_id,
                                OrderStatus::NewOrderTimeout,
                            );
                            all_updates.push(Self::make_timeout_place(order, Some(local_oid)));
                            warn!(
                                "[PolymarketTrade] batch submit omitted response index={} coid={} → NewOrderTimeout",
                                i, order.client_order_id,
                            );
                            continue;
                        };
                        let parsed = match parse_placement_response(r) {
                            Ok(parsed) => parsed,
                            Err(reason) => {
                                self.shared.defer_lifecycle_account_apply(
                                    &order.client_order_id,
                                    OrderStatus::NewOrderTimeout,
                                );
                                all_updates.push(Self::make_timeout_place(order, Some(local_oid)));
                                warn!(
                                    "[PolymarketTrade] ambiguous batch HTTP 2xx placement response index={} coid={} reason={} body={} → NewOrderTimeout",
                                    i, order.client_order_id, reason, r,
                                );
                                continue;
                            }
                        };
                        let success = parsed.success;
                        let order_id = parsed.order_id;
                        let status_str = parsed.status;
                        let error_msg = parsed.error_msg;

                        if success && order_id.is_empty() {
                            self.shared.defer_lifecycle_account_apply(
                                &order.client_order_id,
                                OrderStatus::NewOrderTimeout,
                            );
                            all_updates.push(Self::make_timeout_place(order, Some(local_oid)));
                            warn!(
                                "[PolymarketTrade] batch success missing orderID coid={} → NewOrderTimeout",
                                order.client_order_id,
                            );
                            continue;
                        }

                        let mut effective_ack_status = None;
                        if success {
                            accepted_coids.push(order.client_order_id.clone());
                            effective_ack_status = self.shared.mark_order_live(
                                &order.client_order_id,
                                order.order_slot,
                                &order.symbol,
                                order.side,
                                &self.instance_id,
                                OrderStatus::Accepted,
                            );
                            // Cross-check vs our pre-computed hash — if the
                            // server's orderID disagrees, our local hash
                            // algorithm has drifted; re-register under the
                            // server's value so cancel-by-id still works.
                            if !Self::oid_eq(&order_id, local_oid) {
                                warn!(
                                    "[PolymarketTrade] orderID MISMATCH coid={} local={} server={}",
                                    order.client_order_id, local_oid, order_id,
                                );
                                self.shared.register_order_id(
                                    &order.client_order_id,
                                    &order_id,
                                    &order.symbol,
                                );
                            }
                            // open_orders already populated at sign time.
                        } else {
                            rejected_coids.push(order.client_order_id.clone());
                            self.shared
                                .remove_order_as(&order.client_order_id, OrderStatus::Rejected);
                            if SharedState::is_trading_disabled_error(&error_msg) {
                                self.handle_trading_disabled(&error_msg);
                            } else if SharedState::is_balance_error(&error_msg) {
                                self.handle_balance_error(
                                    &order.client_order_id,
                                    order.side,
                                    &order.symbol,
                                );
                            }
                            warn!(
                                "[PolymarketTrade] Submit rejected: coid={} err=\"{}\" status={}",
                                order.client_order_id, error_msg, status_str,
                            );
                        }

                        let response_status =
                            placement_response_status(success, &status_str, effective_ack_status);
                        // Private fills are delivered on the lossless owner
                        // lane and remain authoritative over this weaker HTTP
                        // acknowledgement. Do not enter SharedAccount's live
                        // lifecycle shard merely to decorate the ACK.
                        let effective_remaining = order.quantity;
                        all_updates.push(OrderUpdate {
                            order_slot: order.order_slot,
                            client_order_id: order.client_order_id.clone(),
                            exchange: Exchange::Polymarket,
                            symbol: order.symbol.clone(),
                            side: order.side,
                            // On success, prefer server orderID; on reject,
                            // still expose our local hash — callers may
                            // want to query by-id as a sanity check.
                            exchange_order_id: Some(if order_id.is_empty() {
                                local_oid.clone()
                            } else {
                                order_id
                            }),
                            status: response_status,
                            liquidity: None,
                            filled_quantity: 0.0,
                            remaining_quantity: if response_status == OrderStatus::Filled {
                                0.0
                            } else {
                                effective_remaining
                            },
                            avg_fill_price: if !success || response_status == OrderStatus::Accepted
                            {
                                order.price.unwrap_or(0.0)
                            } else {
                                0.0
                            },
                            timestamp_ns: now_ns(),
                            exchange_event_timestamp_ns: None,
                            trade_id: None,
                            order_audit: None,
                            error: (!success && !error_msg.is_empty()).then_some(error_msg),
                        });
                    }
                    debug!(
                        "[PolymarketTrade] Submit result: accepted={:?} rejected={:?}",
                        accepted_coids, rejected_coids,
                    );
                }
                Err(e) if e.is_submit_unknown_state() => {
                    if matches!(e, HttpErr::Timeout) {
                        let detail = format!(
                            "operation=batch_place target={} orders={}",
                            self.shared.clob_base_url,
                            chunk_coids.len(),
                        );
                        super::network_incident::record(
                            super::network_incident::NetworkSignal::HttpPlaceTimeout,
                            &detail,
                        );
                    }
                    let is_http_425 = e.is_http_425();
                    if self.shared.should_warn_unknown_state(&e) {
                        warn!(
                            "[PolymarketTrade] Submit unknown state ({}) coids={:?} → NewOrderTimeout",
                            e, chunk_coids,
                        );
                    }
                    // Emit NewOrderTimeout for every successfully-signed
                    // order in this chunk, carrying the pre-computed
                    // orderID so the strategy can cancel / query by id.
                    // Orders that failed to sign were already Rejected above.
                    for (i, oh) in signed_hashes.iter().enumerate() {
                        let order = &chunk[body_to_chunk[i]];
                        if is_http_425 {
                            self.shared.note_http_425_backoff(&order.client_order_id);
                        }
                        self.shared.defer_lifecycle_account_apply(
                            &order.client_order_id,
                            OrderStatus::NewOrderTimeout,
                        );
                        all_updates.push(Self::make_timeout_place(order, Some(oh)));
                    }
                }
                Err(e) => {
                    let err_s = e.to_string();
                    if SharedState::is_trading_disabled_error(&err_s) {
                        self.handle_trading_disabled(&err_s);
                    } else if SharedState::is_balance_error(&err_s) {
                        // Use the first chunk order's side+symbol as the
                        // representative for the targeted-cancel scope.
                        // Polymarket batches sent by the strategy are
                        // typically uniform side/symbol (one outcome's
                        // BIDs or one outcome's ASKs), so first-order is
                        // a faithful sample. record_balance_error()
                        // de-dupes if multiple orders trigger.
                        if let Some(first) = chunk.first() {
                            self.handle_balance_error(
                                &first.client_order_id,
                                first.side,
                                &first.symbol,
                            );
                        }
                    } else if SharedState::is_invalid_token_error(&err_s) {
                        if let Some(first) = chunk.first() {
                            self.handle_invalid_token(&first.symbol);
                        }
                    }
                    warn!(
                        "[PolymarketTrade] Submit failed: {} coids={:?}",
                        e, chunk_coids
                    );
                    for (i, _) in signed_hashes.iter().enumerate() {
                        let order = &chunk[body_to_chunk[i]];
                        self.shared
                            .remove_order_as(&order.client_order_id, OrderStatus::Rejected);
                        all_updates.push(Self::make_rejected(order, &err_s));
                    }
                }
            }
        }
        for update in all_updates
            .iter()
            .filter(|update| update.exchange_order_id.is_some())
        {
            self.shared.log_order_lifecycle(
                &update.client_order_id,
                "http_response",
                update.exchange_order_id.as_deref(),
                Some(update.status),
                None,
            );
        }
        Ok(all_updates)
    }

    fn batch_cancel_orders(
        &mut self,
        exchange: Exchange,
        _market_id: &str,
        client_order_ids: &[String],
    ) -> Result<Vec<OrderUpdate>> {
        // Single-endpoint mode: kickoff every `DELETE /order` first so
        // they fly concurrently over the h2 connection, then drain the
        // receivers. Same pattern as `batch_submit_orders`.
        if !self.shared.use_batch_orders {
            let mut waiters: Vec<(
                usize,
                CancelCtx,
                Option<crossbeam_channel::Receiver<HttpReply>>,
            )> = Vec::with_capacity(client_order_ids.len());
            for (idx, coid) in client_order_ids.iter().enumerate() {
                let (ctx, rx_opt) = self.cancel_kickoff(coid);
                waiters.push((idx, ctx, rx_opt));
            }
            let mut updates: Vec<OrderUpdate> = Vec::with_capacity(client_order_ids.len());
            for (idx, ctx, rx_opt) in waiters {
                let reply = rx_opt.map(|rx| {
                    rx.recv().unwrap_or_else(|_| {
                        Err(HttpErr::Transport("async reply dropped".to_string()))
                    })
                });
                updates.push(self.handle_cancel_reply(
                    exchange,
                    &client_order_ids[idx],
                    ctx,
                    reply,
                ));
            }
            return Ok(updates);
        }
        let mut order_ids: Vec<String> = Vec::new();
        let mut sent_coids: Vec<String> = Vec::new();
        let mut unmapped_coids: Vec<String> = Vec::new();
        {
            let execution = self.shared.execution_snapshot();
            let map = &execution.coid_to_oid;
            for coid in client_order_ids {
                if let Some(oid) = map.get(coid) {
                    order_ids.push(oid.clone());
                    sent_coids.push(coid.clone());
                } else {
                    unmapped_coids.push(coid.clone());
                }
            }
        }

        if !order_ids.is_empty() {
            // Single order → DELETE /order; multiple → DELETE /orders.
            let (path, body) = if order_ids.len() == 1 {
                ("/order", serde_json::json!({ "orderID": order_ids[0] }))
            } else {
                (
                    "/orders",
                    serde_json::Value::Array(
                        order_ids
                            .iter()
                            .map(|id| serde_json::Value::String(id.clone()))
                            .collect(),
                    ),
                )
            };
            if unmapped_coids.is_empty() {
                debug!(
                    "[PolymarketTrade] Cancel request: {} orders coids={:?}",
                    sent_coids.len(),
                    sent_coids,
                );
            } else {
                debug!(
                    "[PolymarketTrade] Cancel request: {} orders coids={:?} (+ {} unmapped coids={:?})",
                    sent_coids.len(), sent_coids,
                    unmapped_coids.len(), unmapped_coids,
                );
            }
            // Per-coid outcome map. On Ok: fill from `canceled` +
            // `not_canceled`; omitted or ambiguous orders stay orphaned.
            // On unknown_state: all coids → CancelOrderTimeout.
            // On a definite cancel failure: all coids remain Accepted/live so
            // the normal refresh path retries without releasing collateral.
            let mut per_coid_outcome: std::collections::HashMap<String, OrderStatus> =
                std::collections::HashMap::new();
            let fallback_outcome: OrderStatus = match self.delete_detailed(path, &body) {
                Ok(resp) => {
                    let oid_to_coid = self.shared.execution_snapshot().oid_to_coid.clone();
                    let canceled_oids: Vec<String> = resp
                        .get("canceled")
                        .and_then(|v| v.as_array())
                        .map(|a| {
                            a.iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default();
                    let not_canceled = resp.get("not_canceled").and_then(|v| v.as_object());
                    for oid in &canceled_oids {
                        if let Some(coid) = oid_to_coid.get(&normalize_order_id(oid)) {
                            per_coid_outcome.insert(coid.clone(), OrderStatus::Cancelled);
                        }
                    }
                    let canceled_coids: Vec<String> = canceled_oids
                        .iter()
                        .map(|oid| {
                            oid_to_coid
                                .get(&normalize_order_id(oid))
                                .cloned()
                                .unwrap_or_default()
                        })
                        .collect();
                    let not_canceled_coids: Vec<String> = not_canceled
                        .map(|m| {
                            m.keys()
                                .map(|oid| {
                                    oid_to_coid
                                        .get(&normalize_order_id(oid))
                                        .cloned()
                                        .unwrap_or_default()
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    debug!(
                        "[PolymarketTrade] Cancel result: canceled={:?} not_canceled={:?}",
                        canceled_coids, not_canceled_coids,
                    );
                    if let Some(nc) = not_canceled {
                        for (id, reason) in nc {
                            let coid = oid_to_coid
                                .get(&normalize_order_id(id))
                                .cloned()
                                .unwrap_or_default();
                            let reason_str = reason.as_str().unwrap_or("");
                            debug!(
                                "[PolymarketTrade] Cancel rejected: orderID={} reason={} coid={}",
                                id, reason_str, coid,
                            );
                            if !coid.is_empty() {
                                // This is the initial/ordinary cancel path;
                                // only reconciler-issued DELETEs are counted.
                                let outcome = cancel_not_canceled_outcome(reason_str);
                                let s = match outcome {
                                    CancelReasonOutcome::Cancelled => OrderStatus::Cancelled,
                                    CancelReasonOutcome::Filled => OrderStatus::Filled,
                                    CancelReasonOutcome::Uncertain => OrderStatus::CancelUncertain,
                                };
                                if s == OrderStatus::CancelUncertain
                                    && is_pending_delayed_reason(reason_str)
                                {
                                    self.shared.pending_delayed_orphans.insert(coid.clone());
                                }
                                per_coid_outcome.insert(coid, s);
                            }
                        }
                    }
                    // A successful batch response that omits an orderID says
                    // nothing authoritative about that order's state — but
                    // the reply itself was healthy: ambiguous, not timed out.
                    OrderStatus::CancelUncertain
                }
                Err(e) if e.is_unknown_state() => {
                    if matches!(e, HttpErr::Timeout) {
                        let detail = format!(
                            "operation=batch_cancel target={} orders={}",
                            self.shared.clob_base_url,
                            client_order_ids.len(),
                        );
                        super::network_incident::record(
                            super::network_incident::NetworkSignal::HttpCancelTimeout,
                            &detail,
                        );
                    }
                    if self.shared.should_warn_unknown_state(&e) {
                        warn!(
                            "[PolymarketTrade] Cancel unknown state ({}) coids={:?} → CancelOrderTimeout",
                            e, client_order_ids,
                        );
                    }
                    OrderStatus::CancelOrderTimeout
                }
                Err(e) => {
                    warn!(
                        "[PolymarketTrade] Cancel HTTP error: {} coids={:?}",
                        e, client_order_ids
                    );
                    OrderStatus::Accepted
                }
            };
            let mut updates = Vec::new();
            for coid in client_order_ids {
                let execution = self.shared.execution_snapshot();
                let tracked = execution.open_orders.get(coid).cloned();
                let order_id = execution.coid_to_oid.get(coid).cloned();
                let mut outcome = per_coid_outcome
                    .get(coid)
                    .copied()
                    .unwrap_or(fallback_outcome);
                if matches!(
                    outcome,
                    OrderStatus::CancelOrderTimeout | OrderStatus::CancelUncertain
                ) && order_id.is_none()
                {
                    // Cannot reconcile an orphan without an exchange OID.
                    // Revert to live/Accepted so OrderManager retries instead
                    // of wedging forever in Cancelling or releasing the lock.
                    outcome = OrderStatus::Accepted;
                }
                outcome = self.shared.effective_cancel_attempt_status(coid, outcome);
                // Drop local tracking for terminal outcomes; keep for
                // CancelOrderTimeout so the orphan reconciler can re-query.
                if matches!(outcome, OrderStatus::Cancelled | OrderStatus::Filled) {
                    self.shared.remove_order_as(coid, outcome);
                }
                updates.push(OrderUpdate {
                    order_slot: Default::default(),
                    client_order_id: coid.clone(),
                    exchange,
                    symbol: tracked
                        .as_ref()
                        .map(|t| t.symbol.clone())
                        .unwrap_or_default(),
                    side: tracked.map(|t| t.side).unwrap_or(Side::Buy),
                    exchange_order_id: order_id,
                    status: outcome,
                    liquidity: None,
                    filled_quantity: 0.0,
                    remaining_quantity: 0.0,
                    avg_fill_price: 0.0,
                    timestamp_ns: now_ns(),
                    exchange_event_timestamp_ns: None,
                    trade_id: None,
                    order_audit: None,
                    error: None,
                });
            }
            return Ok(updates);
        }

        // No orderIDs to cancel. Absence of a local mapping is not an
        // authoritative exchange terminal state: preserve each order as live
        // so OrderManager can retry after the mapping catches up.
        let mut updates = Vec::new();
        for coid in client_order_ids {
            let tracked = self
                .shared
                .execution_snapshot()
                .open_orders
                .get(coid)
                .cloned();
            updates.push(OrderUpdate {
                order_slot: Default::default(),
                client_order_id: coid.clone(),
                exchange,
                symbol: tracked
                    .as_ref()
                    .map(|t| t.symbol.clone())
                    .unwrap_or_default(),
                side: tracked.map(|t| t.side).unwrap_or(Side::Buy),
                exchange_order_id: None,
                status: OrderStatus::Accepted,
                liquidity: None,
                filled_quantity: 0.0,
                remaining_quantity: 0.0,
                avg_fill_price: 0.0,
                timestamp_ns: now_ns(),
                exchange_event_timestamp_ns: None,
                trade_id: None,
                order_audit: None,
                error: None,
            });
        }
        Ok(updates)
    }

    fn batch_update_orders(
        &mut self,
        exchange: Exchange,
        _market_id: &str,
        cancel_client_order_ids: &[String],
        place_orders: &[OrderRequest],
    ) -> Result<Vec<OrderUpdate>> {
        // Parallel cancel + place via the persistent HTTP worker pool.
        // Each side chooses the single-order endpoint (`POST /order` /
        // `DELETE /order`) when there's exactly one op, and the batch
        // endpoint (`POST /orders` / `DELETE /orders`) when there are two
        // or more. Critical path ≈ max(cancel_rtt, place_rtt).

        // Trading-disabled and balance backoffs short-circuit the PLACE side
        // only — cancels must still dispatch so local/server state converges.
        if self.shared.in_trading_disabled_backoff() && !place_orders.is_empty() {
            let mut pre =
                self.make_logged_preflight_rejections(place_orders, "trading disabled backoff");
            let rest =
                self.batch_update_orders(exchange, _market_id, cancel_client_order_ids, &[])?;
            pre.extend(rest);
            return Ok(pre);
        }

        if super::network_incident::place_blocked_by_connection_failure_cluster()
            && !place_orders.is_empty()
        {
            let mut pre = self.make_logged_preflight_rejections(
                place_orders,
                "connection failure cluster safety gate",
            );
            let rest =
                self.batch_update_orders(exchange, _market_id, cancel_client_order_ids, &[])?;
            pre.extend(rest);
            return Ok(pre);
        }

        // Balance-backoff short-circuit on the PLACE side only — let
        // the cancels still dispatch. The strategy's coid-specific
        // cancels (issued by its own quote-tick decision) are
        // independent of the targeted batch DELETE we fired in
        // `handle_balance_error`; both need to land for local state
        // and the server's allowance pool to converge. Pre-reject
        // every place during the 200 ms window so doomed submits
        // don't get hammered while the cancels race to land.
        if self.shared.in_balance_backoff(&self.instance_id) && !place_orders.is_empty() {
            let mut pre = self.make_logged_preflight_rejections(place_orders, "balance backoff");
            // Still process cancels — recurse into the cancel-only path.
            let rest =
                self.batch_update_orders(exchange, _market_id, cancel_client_order_ids, &[])?;
            pre.extend(rest);
            return Ok(pre);
        }

        // Per-token invalid-token backoff: pre-reject only the places whose
        // token is gated (CLOB book not live for that event), keep the rest +
        // cancels. Per-token, so concurrent events with valid tokens proceed.
        // (Single-endpoint mode also re-checks per order in `submit_prep`;
        // this entry filter additionally covers true-batch `POST /orders`.)
        if place_orders
            .iter()
            .any(|o| self.shared.in_invalid_token_backoff(&o.symbol))
        {
            let (blocked, allowed): (Vec<OrderRequest>, Vec<OrderRequest>) = place_orders
                .iter()
                .cloned()
                .partition(|o| self.shared.in_invalid_token_backoff(&o.symbol));
            let mut pre = self.make_logged_preflight_rejections(&blocked, "invalid token backoff");
            let rest =
                self.batch_update_orders(exchange, _market_id, cancel_client_order_ids, &allowed)?;
            pre.extend(rest);
            return Ok(pre);
        }

        // Single-endpoint mode (`use_batch_orders=false`).
        //
        // FULLY CONCURRENT dispatch — cancels AND places kicked off together,
        // no ordering between them. Both roles borrow distinct slots from the
        // account's merged order pool, so every request of a two-leg replace
        // is on the wire immediately after signing. Critical path ≈ max(single
        // RTT).
        //
        // History: until 2026-07 a replace (both cancels and places in one
        // batch) took a SERIAL path (all cancels written before all places
        // on one connection) to make cancel→place arrival order
        // deterministic and close the place-before-cancel double-commit
        // window (SELL `balance:0` rejects). That ordering was dropped by
        // operator decision for latency — on h1.1 it cost a full extra RTT
        // per replace. The double-commit window (~one RTT) is back and is
        // accepted: balance backoff + the reconciler absorb the fallout.
        if !self.shared.use_batch_orders {
            let mut updates: Vec<OrderUpdate> =
                Vec::with_capacity(cancel_client_order_ids.len() + place_orders.len());

            // ── Cancel side: kickoff all, drain after places kicked off ─
            let mut cancel_waiters: Vec<(
                usize,
                CancelCtx,
                Option<crossbeam_channel::Receiver<HttpReply>>,
            )> = Vec::with_capacity(cancel_client_order_ids.len());
            for (idx, coid) in cancel_client_order_ids.iter().enumerate() {
                let (ctx, rx_opt) = self.cancel_kickoff(coid);
                cancel_waiters.push((idx, ctx, rx_opt));
            }

            // ── Place side: kickoff all (interleaved on the wire with
            //    the cancels above) ────────────────────────────────────
            let mut place_waiters: Vec<(usize, String, crossbeam_channel::Receiver<HttpReply>)> =
                Vec::with_capacity(place_orders.len());
            let mut place_slots: Vec<Option<OrderUpdate>> =
                (0..place_orders.len()).map(|_| None).collect();
            for (idx, o) in place_orders.iter().enumerate() {
                match self.submit_kickoff(o) {
                    Ok((local_oid, rx)) => place_waiters.push((idx, local_oid, rx)),
                    Err(rejected) => place_slots[idx] = Some(rejected),
                }
            }

            // ── Drain: cancel replies first (typically fastest), then
            //    place replies. Within each set, recv blocks in the
            //    order issued; total wall-clock = max RTT across all
            //    in-flight requests. ─────────────────────────────────
            for (idx, ctx, rx_opt) in cancel_waiters {
                let reply = rx_opt.map(|rx| {
                    rx.recv().unwrap_or_else(|_| {
                        Err(HttpErr::Transport("async reply dropped".to_string()))
                    })
                });
                updates.push(self.handle_cancel_reply(
                    exchange,
                    &cancel_client_order_ids[idx],
                    ctx,
                    reply,
                ));
            }
            for (idx, local_oid, rx) in place_waiters {
                let reply = rx
                    .recv()
                    .unwrap_or_else(|_| Err(HttpErr::Transport("async reply dropped".to_string())));
                place_slots[idx] =
                    Some(self.handle_submit_reply(&place_orders[idx], &local_oid, reply));
            }
            for slot in place_slots {
                if let Some(u) = slot {
                    updates.push(u);
                }
            }

            return Ok(updates);
        }

        // ─── Prepare cancel request ─────────────────────────────────────
        // Partition the caller's coids into `sent_coids` (have an orderID
        // mapping → go into the HTTP request) and `unmapped_coids` (no
        // orderID → nothing to send to the server; handled as Cancelled
        // locally below). This keeps the request log honest: the count
        // matches what was actually dispatched.
        let mut cancel_order_ids: Vec<String> = Vec::new();
        let mut sent_coids: Vec<String> = Vec::new();
        let mut unmapped_coids: Vec<String> = Vec::new();
        let cancel_prep_ns = now_ns();
        if self.gen_ns_hint > 0 {
            crate::latency::record_ns(
                "polymarket.cancel.signal_to_prep",
                cancel_prep_ns.saturating_sub(self.gen_ns_hint),
            );
        }
        {
            let execution = self.shared.execution_snapshot();
            let map = &execution.coid_to_oid;
            for coid in cancel_client_order_ids {
                self.shared.record_cancel_signal(coid, self.gen_ns_hint);
                self.shared.log_order_lifecycle(
                    coid,
                    "cancel_prep",
                    map.get(coid).map(String::as_str),
                    None,
                    None,
                );
                if let Some(oid) = map.get(coid) {
                    cancel_order_ids.push(oid.clone());
                    sent_coids.push(coid.clone());
                } else {
                    unmapped_coids.push(coid.clone());
                }
            }
        }
        // Decide cancel endpoint: /order for 1 id, /orders for >1.
        let cancel_req: Option<(&'static str, String)> = match cancel_order_ids.len() {
            0 => None,
            1 => {
                let body = serde_json::json!({ "orderID": cancel_order_ids[0] }).to_string();
                Some(("/order", body))
            }
            _ => {
                let body = serde_json::Value::Array(
                    cancel_order_ids
                        .iter()
                        .map(|id| serde_json::Value::String(id.clone()))
                        .collect(),
                )
                .to_string();
                Some(("/orders", body))
            }
        };

        // ─── Prepare place request ──────────────────────────────────────
        // Polymarket POST /orders takes up to 15. >15 falls back to the
        // serial chunked path below.
        let place_chunk: &[OrderRequest] = if place_orders.len() > 15 {
            warn!("[PolymarketTrade] batch_update_orders: >15 places, splitting");
            &place_orders[..0]
        } else {
            place_orders
        };
        // Sign each member and pre-register coid↔orderID. Keep the Vec
        // of SignedOrders in scope so the response matching / timeout
        // path can pass each pre-computed hash into its OrderUpdate.
        let mut place_signed: Vec<String> = Vec::with_capacity(place_chunk.len());
        let mut place_bodies: Vec<serde_json::Value> = Vec::with_capacity(place_chunk.len());
        // `place_body_to_chunk[i]` is the index within `place_chunk` of
        // the i-th successfully-signed order.
        let mut place_body_to_chunk: Vec<usize> = Vec::with_capacity(place_chunk.len());
        // Track signing failures so we can emit Rejected for them below.
        let mut place_sign_failures: Vec<OrderUpdate> = Vec::new();
        for (idx, o) in place_chunk.iter().enumerate() {
            self.shared.register_order_lifecycle(o);
            self.shared.log_order_lifecycle(
                &o.client_order_id,
                "submit_prep",
                None,
                Some(OrderStatus::Pending),
                None,
            );
            match self.sign_and_build_body(o) {
                Ok((order_hash, b)) => {
                    self.shared.log_order_lifecycle(
                        &o.client_order_id,
                        "signed",
                        Some(&order_hash),
                        None,
                        None,
                    );
                    let b = serde_json::to_value(&b).unwrap_or_default();
                    let ownership = match self.record_account_order(o, &order_hash) {
                        Ok(ownership) => ownership,
                        Err(rejected) => {
                            self.shared.log_preflight_rejected(
                                &o.client_order_id,
                                Some(&order_hash),
                                &rejected,
                            );
                            self.shared.forget_order_lifecycle(&o.client_order_id);
                            place_sign_failures.push(rejected);
                            continue;
                        }
                    };
                    self.shared.log_order_lifecycle(
                        &o.client_order_id,
                        "account_recorded",
                        Some(&order_hash),
                        Some(OrderStatus::Pending),
                        None,
                    );
                    if let Err(error) = self.shared.register_local_order_id(
                        &o.client_order_id,
                        &order_hash,
                        &o.symbol,
                        ownership,
                    ) {
                        let rejected = Self::make_rejected(o, &error);
                        self.shared.log_preflight_rejected(
                            &o.client_order_id,
                            Some(&order_hash),
                            &rejected,
                        );
                        self.shared.forget_order_lifecycle(&o.client_order_id);
                        place_sign_failures.push(rejected);
                        continue;
                    }
                    // Same sign-time open_orders insert as `submit_kickoff`
                    // and `batch_submit_orders` so all submit paths share
                    // the "open_orders = may be on server" invariant.
                    if let Err(error) = self.shared.track_open_order(
                        &o.client_order_id,
                        TrackedOrder {
                            order_slot: o.order_slot,
                            symbol: o.symbol.clone(),
                            side: o.side,
                            instance_id: self.instance_id.clone(),
                        },
                    ) {
                        let rejected = Self::make_rejected(o, &error);
                        self.shared
                            .remove_order_resolved_as(&o.client_order_id, OrderStatus::Rejected);
                        self.shared.log_preflight_rejected(
                            &o.client_order_id,
                            Some(&order_hash),
                            &rejected,
                        );
                        self.shared.forget_order_lifecycle(&o.client_order_id);
                        place_sign_failures.push(rejected);
                        continue;
                    }
                    place_signed.push(order_hash);
                    place_bodies.push(b);
                    place_body_to_chunk.push(idx);
                }
                Err(e) => {
                    warn!(
                        "[PolymarketTrade] sign failed coid={}: {} — skipping",
                        o.client_order_id, e,
                    );
                    let rejected = Self::make_rejected(o, &e.to_string());
                    self.shared
                        .log_preflight_rejected(&o.client_order_id, None, &rejected);
                    self.shared.forget_order_lifecycle(&o.client_order_id);
                    place_sign_failures.push(rejected);
                }
            }
        }
        // Decide place endpoint: /order for 1 order body, /orders for >1.
        let place_req: Option<(&'static str, String)> = match place_bodies.len() {
            0 => None,
            1 => Some(("/order", place_bodies[0].to_string())),
            _ => Some((
                "/orders",
                serde_json::Value::Array(place_bodies.clone()).to_string(),
            )),
        };

        // ─── Dispatch both async ────────────────────────────────────────
        let place_coids: Vec<String> = place_chunk
            .iter()
            .map(|o| o.client_order_id.clone())
            .collect();
        // Captured at cancel-dispatch time so a later "Submit rejected"
        // log line can report `cancel_dispatched_ms_ago` — distinguishes
        // a balance-reject caused by genuine phantom server state from
        // one caused by a cancel/submit race within this batch call
        // (cancel not yet landed when submit hit the server).
        let batch_start_ns = now_ns();
        let batch_cancel_coids: Vec<String> = cancel_client_order_ids.to_vec();
        let cancel_rx = cancel_req.as_ref().map(|(path, body)| {
            if unmapped_coids.is_empty() {
                debug!(
                    "[PolymarketTrade] Cancel request: {} orders coids={:?}",
                    sent_coids.len(), sent_coids,
                );
            } else {
                debug!(
                    "[PolymarketTrade] Cancel request: {} orders coids={:?} (+ {} unmapped coids={:?})",
                    sent_coids.len(), sent_coids,
                    unmapped_coids.len(), unmapped_coids,
                );
            }
            let rx = self.shared.http_call_async("DELETE", path, body);
            for (coid, oid) in sent_coids.iter().zip(cancel_order_ids.iter()) {
                self.shared.log_order_lifecycle(
                    coid,
                    "cancel_dispatched",
                    Some(oid),
                    None,
                    None,
                );
            }
            rx
        });
        for coid in &unmapped_coids {
            self.shared
                .log_order_lifecycle(coid, "cancel_not_dispatched", None, None, None);
        }
        let place_rx = place_req.as_ref().map(|(path, body)| {
            let details: Vec<String> = place_chunk.iter().map(|o| format_order_brief(o)).collect();
            debug!(
                "[PolymarketTrade] Submit request: {} orders [{}]",
                place_chunk.len(),
                details.join(", "),
            );
            let rx = self.shared.http_call_async("POST", path, body);
            for (body_index, order_hash) in place_signed.iter().enumerate() {
                let order = &place_chunk[place_body_to_chunk[body_index]];
                self.shared.log_order_lifecycle(
                    &order.client_order_id,
                    "http_dispatched",
                    Some(order_hash),
                    None,
                    None,
                );
            }
            rx
        });

        let mut updates: Vec<OrderUpdate> = Vec::new();

        // ─── Await + parse cancel ───────────────────────────────────────
        // Resolve the local control state even when we skipped the HTTP request
        // because no requested coid had an orderID mapping. Missing local
        // metadata is not exchange-terminal evidence: emit Accepted/live so
        // OrderManager retries rather than releasing collateral.
        // `Some((fallback_outcome, per_coid_overrides))` — the fallback
        // applies to coids the server didn't mention; per-coid overrides
        // come from `canceled` / `not_canceled` (the latter with a
        // "matched" reason is mapped to Filled, see `cancel_not_canceled_outcome`).
        let cancel_outcome: Option<(OrderStatus, std::collections::HashMap<String, OrderStatus>)> =
            match cancel_rx {
                None if !cancel_client_order_ids.is_empty() => {
                    debug!(
                    "[PolymarketTrade] Cancel request: 0 orders ({} unmapped coids={:?}) → keep live for retry",
                    unmapped_coids.len(), unmapped_coids,
                );
                    Some((OrderStatus::Accepted, std::collections::HashMap::new()))
                }
                None => None,
                Some(rx) => {
                    // Classify the response so we can emit the right OrderStatus for
                    // each coid:
                    //   - Ok                     → per-order authoritative result;
                    //                              omitted/ambiguous → orphan
                    //   - Err::is_unknown_state  → CancelOrderTimeout (timeout or
                    //                              HTTP 5xx — server state unknown,
                    //                              orphan reconciler will verify)
                    //   - Err (other / 4xx)      → Accepted/live (the cancel was
                    //                              rejected; keep collateral and
                    //                              retry through normal refresh)
                    // Build a per-coid outcome map; on Ok responses use
                    // `canceled`/`not_canceled` to distinguish plain cancels
                    // from fills that raced ahead of our cancel. On errors
                    // every coid gets the fallback outcome.
                    let mut per_coid_outcome: std::collections::HashMap<String, OrderStatus> =
                        std::collections::HashMap::new();
                    let fallback = match rx
                        .recv()
                        .unwrap_or_else(|_| Err(HttpErr::Transport("reply dropped".into())))
                    {
                        Ok(resp) => {
                            // Both /order and /orders return { canceled: [...], not_canceled: {...} }.
                            let oid_to_coid = self.shared.execution_snapshot().oid_to_coid.clone();
                            let canceled_oids: Vec<String> = resp
                                .get("canceled")
                                .and_then(|v| v.as_array())
                                .map(|a| {
                                    a.iter()
                                        .filter_map(|v| v.as_str().map(String::from))
                                        .collect()
                                })
                                .unwrap_or_default();
                            let not_canceled = resp.get("not_canceled").and_then(|v| v.as_object());
                            for oid in &canceled_oids {
                                if let Some(coid) = oid_to_coid.get(&normalize_order_id(oid)) {
                                    per_coid_outcome.insert(coid.clone(), OrderStatus::Cancelled);
                                }
                            }
                            let canceled_coids: Vec<String> = canceled_oids
                                .iter()
                                .map(|oid| {
                                    oid_to_coid
                                        .get(&normalize_order_id(oid))
                                        .cloned()
                                        .unwrap_or_default()
                                })
                                .collect();
                            let not_canceled_coids: Vec<String> = not_canceled
                                .map(|m| {
                                    m.keys()
                                        .map(|oid| {
                                            oid_to_coid
                                                .get(&normalize_order_id(oid))
                                                .cloned()
                                                .unwrap_or_default()
                                        })
                                        .collect()
                                })
                                .unwrap_or_default();
                            debug!(
                                "[PolymarketTrade] Cancel result: canceled={:?} not_canceled={:?}",
                                canceled_coids, not_canceled_coids,
                            );
                            if let Some(nc) = not_canceled {
                                for (id, reason) in nc {
                                    let coid = oid_to_coid
                                        .get(&normalize_order_id(id))
                                        .cloned()
                                        .unwrap_or_default();
                                    let reason_str = reason.as_str().unwrap_or("");
                                    debug!(
                                    "[PolymarketTrade] Cancel rejected: orderID={} reason={} coid={}",
                                    id, reason_str, coid,
                                );
                                    if !coid.is_empty() {
                                        // This is the initial/ordinary cancel
                                        // path; it must not advance reconcile
                                        // DELETE observations.
                                        let outcome = cancel_not_canceled_outcome(reason_str);
                                        let s = match outcome {
                                            CancelReasonOutcome::Cancelled => {
                                                OrderStatus::Cancelled
                                            }
                                            CancelReasonOutcome::Filled => OrderStatus::Filled,
                                            CancelReasonOutcome::Uncertain => {
                                                OrderStatus::CancelUncertain
                                            }
                                        };
                                        if s == OrderStatus::CancelUncertain
                                            && is_pending_delayed_reason(reason_str)
                                        {
                                            self.shared
                                                .pending_delayed_orphans
                                                .insert(coid.clone());
                                        }
                                        per_coid_outcome.insert(coid, s);
                                    }
                                }
                            }
                            // Healthy reply; unresolved orders are ambiguous.
                            OrderStatus::CancelUncertain
                        }
                        Err(e) if e.is_unknown_state() => {
                            if matches!(e, HttpErr::Timeout) {
                                let detail = format!(
                                    "operation=cancel_replace_cancel target={} orders={}",
                                    self.shared.clob_base_url,
                                    cancel_client_order_ids.len(),
                                );
                                super::network_incident::record(
                                    super::network_incident::NetworkSignal::HttpCancelTimeout,
                                    &detail,
                                );
                            }
                            if self.shared.should_warn_unknown_state(&e) {
                                warn!(
                                "[PolymarketTrade] Cancel unknown state ({}) coids={:?} → CancelOrderTimeout",
                                e, cancel_client_order_ids,
                            );
                            }
                            OrderStatus::CancelOrderTimeout
                        }
                        Err(e) => {
                            warn!(
                                "[PolymarketTrade] Cancel HTTP error: {} coids={:?}",
                                e, cancel_client_order_ids
                            );
                            OrderStatus::Accepted
                        }
                    };
                    Some((fallback, per_coid_outcome))
                }
            };
        if let Some((fallback_outcome, per_coid_outcome)) = cancel_outcome {
            for coid in cancel_client_order_ids {
                let execution = self.shared.execution_snapshot();
                let tracked = execution.open_orders.get(coid).cloned();
                let order_id = execution.coid_to_oid.get(coid).cloned();
                let mut outcome = per_coid_outcome
                    .get(coid)
                    .copied()
                    .unwrap_or(fallback_outcome);
                if matches!(
                    outcome,
                    OrderStatus::CancelOrderTimeout | OrderStatus::CancelUncertain
                ) && order_id.is_none()
                {
                    outcome = OrderStatus::Accepted;
                }
                outcome = self.shared.effective_cancel_attempt_status(coid, outcome);
                self.shared.log_order_lifecycle(
                    coid,
                    "cancel_response",
                    order_id.as_deref(),
                    Some(outcome),
                    None,
                );
                // Drop local tracking for terminal (Cancelled / Filled)
                // outcomes — keep for CancelOrderTimeout so the orphan
                // reconciler can re-query by orderID.
                if matches!(outcome, OrderStatus::Cancelled | OrderStatus::Filled) {
                    self.shared.remove_order_as(coid, outcome);
                }
                updates.push(OrderUpdate {
                    order_slot: Default::default(),
                    client_order_id: coid.clone(),
                    exchange,
                    symbol: tracked
                        .as_ref()
                        .map(|t| t.symbol.clone())
                        .unwrap_or_default(),
                    side: tracked.map(|t| t.side).unwrap_or(Side::Buy),
                    exchange_order_id: order_id,
                    status: outcome,
                    liquidity: None,
                    filled_quantity: 0.0,
                    remaining_quantity: 0.0,
                    avg_fill_price: 0.0,
                    timestamp_ns: now_ns(),
                    exchange_event_timestamp_ns: None,
                    trade_id: None,
                    order_audit: None,
                    error: None,
                });
            }
        }

        // ─── Await + parse place ────────────────────────────────────────
        if let Some(rx) = place_rx {
            match rx
                .recv()
                .unwrap_or_else(|_| Err(HttpErr::Transport("reply dropped".into())))
            {
                Ok(resp) => {
                    // POST /order returns a single object; POST /orders
                    // returns an array. Normalize to Vec<&Value>.
                    let single = !resp.is_array();
                    let array_buf;
                    let responses: &[serde_json::Value] = if single {
                        array_buf = [resp];
                        &array_buf
                    } else {
                        resp.as_array().map(|a| a.as_slice()).unwrap_or(&[])
                    };
                    let mut accepted_coids: Vec<String> = Vec::new();
                    let mut rejected_coids: Vec<String> = Vec::new();
                    for i in 0..place_bodies.len() {
                        let order = &place_chunk[place_body_to_chunk[i]];
                        let local_oid = &place_signed[i];
                        let Some(r) = responses.get(i) else {
                            self.shared.defer_lifecycle_account_apply(
                                &order.client_order_id,
                                OrderStatus::NewOrderTimeout,
                            );
                            updates.push(Self::make_timeout_place(order, Some(local_oid)));
                            warn!(
                                "[PolymarketTrade] batch update submit omitted response index={} coid={} → NewOrderTimeout",
                                i, order.client_order_id,
                            );
                            continue;
                        };
                        let parsed = match parse_placement_response(r) {
                            Ok(parsed) => parsed,
                            Err(reason) => {
                                self.shared.defer_lifecycle_account_apply(
                                    &order.client_order_id,
                                    OrderStatus::NewOrderTimeout,
                                );
                                updates.push(Self::make_timeout_place(order, Some(local_oid)));
                                warn!(
                                    "[PolymarketTrade] ambiguous batch-update HTTP 2xx placement response index={} coid={} reason={} body={} → NewOrderTimeout",
                                    i, order.client_order_id, reason, r,
                                );
                                continue;
                            }
                        };
                        let success = parsed.success;
                        let order_id = parsed.order_id;
                        let status_str = parsed.status;
                        let error_msg = parsed.error_msg;
                        if success && order_id.is_empty() {
                            self.shared.defer_lifecycle_account_apply(
                                &order.client_order_id,
                                OrderStatus::NewOrderTimeout,
                            );
                            updates.push(Self::make_timeout_place(order, Some(local_oid)));
                            warn!(
                                "[PolymarketTrade] batch update success missing orderID coid={} → NewOrderTimeout",
                                order.client_order_id,
                            );
                            continue;
                        }
                        let mut effective_ack_status = None;
                        if success {
                            accepted_coids.push(order.client_order_id.clone());
                            effective_ack_status = self.shared.mark_order_live(
                                &order.client_order_id,
                                order.order_slot,
                                &order.symbol,
                                order.side,
                                &self.instance_id,
                                OrderStatus::Accepted,
                            );
                            if !Self::oid_eq(&order_id, local_oid) {
                                warn!(
                                    "[PolymarketTrade] orderID MISMATCH coid={} local={} server={}",
                                    order.client_order_id, local_oid, order_id,
                                );
                                self.shared.register_order_id(
                                    &order.client_order_id,
                                    &order_id,
                                    &order.symbol,
                                );
                            }
                            // open_orders already populated at sign time.
                        } else {
                            rejected_coids.push(order.client_order_id.clone());
                            self.shared
                                .remove_order_as(&order.client_order_id, OrderStatus::Rejected);
                            let is_trading_disabled =
                                SharedState::is_trading_disabled_error(&error_msg);
                            let is_balance_err = SharedState::is_balance_error(&error_msg);
                            if is_trading_disabled {
                                self.handle_trading_disabled(&error_msg);
                                warn!(
                                    "[PolymarketTrade] Submit rejected: coid={} err=\"{}\" status={}",
                                    order.client_order_id, error_msg, status_str,
                                );
                            } else if is_balance_err {
                                // Balance rejects in batch_update_orders
                                // are usually a cancel/submit race: the
                                // server evaluated our new submit's
                                // allowance BEFORE the concurrent cancel
                                // of the old order landed, so the old
                                // order's reservation was still counted.
                                // Log the batch's cancel coids + time
                                // elapsed since cancel-dispatch so
                                // post-mortem can separate race (small
                                // elapsed) from true phantom (larger).
                                let elapsed_ms =
                                    (now_ns().saturating_sub(batch_start_ns)) / 1_000_000;
                                // Enter the 200 ms balance backoff + fire
                                // a targeted batch DELETE for the
                                // affected pool (BUY → all BUYs / SELL
                                // → same-symbol SELLs) so the server
                                // releases any allowance still tied up
                                // in orphaned orders whose cancel DELETE
                                // timed out in flight. Subsequent
                                // submits in this window are
                                // short-circuited at the top of
                                // submit_order / batch_submit_orders /
                                // batch_update_orders.
                                self.handle_balance_error(
                                    &order.client_order_id,
                                    order.side,
                                    &order.symbol,
                                );
                                warn!(
                                    "[PolymarketTrade] Submit rejected: coid={} err=\"{}\" status={} \
                                     (batch_concurrent_cancels={:?} elapsed_since_dispatch={}ms)",
                                    order.client_order_id, error_msg, status_str,
                                    batch_cancel_coids, elapsed_ms,
                                );
                            } else {
                                warn!(
                                    "[PolymarketTrade] Submit rejected: coid={} err=\"{}\" status={}",
                                    order.client_order_id, error_msg, status_str,
                                );
                            }
                        }
                        // For Rejected, repurpose `avg_fill_price` to
                        // carry the requested order price so strategies
                        // can back-infer market state from the error
                        // (e.g. post-only-crosses-book → real bid/ask
                        // moved past `order.price`). Same convention as
                        // `make_rejected`. For Accepted, the field stays
                        // 0.0 (no fill yet).
                        let response_status =
                            placement_response_status(success, &status_str, effective_ack_status);
                        let response_price = if !success || response_status == OrderStatus::Accepted
                        {
                            order.price.unwrap_or(0.0)
                        } else {
                            0.0
                        };
                        let err_field = if !success && !error_msg.is_empty() {
                            Some(error_msg)
                        } else {
                            None
                        };
                        let effective_remaining = order.quantity;
                        updates.push(OrderUpdate {
                            order_slot: order.order_slot,
                            client_order_id: order.client_order_id.clone(),
                            exchange: Exchange::Polymarket,
                            symbol: order.symbol.clone(),
                            side: order.side,
                            exchange_order_id: Some(if order_id.is_empty() {
                                local_oid.clone()
                            } else {
                                order_id
                            }),
                            status: response_status,
                            liquidity: None,
                            filled_quantity: 0.0,
                            remaining_quantity: if response_status == OrderStatus::Filled {
                                0.0
                            } else {
                                effective_remaining
                            },
                            avg_fill_price: response_price,
                            timestamp_ns: now_ns(),
                            exchange_event_timestamp_ns: None,
                            trade_id: None,
                            order_audit: None,
                            error: err_field,
                        });
                    }
                    debug!(
                        "[PolymarketTrade] Submit result: accepted={:?} rejected={:?}",
                        accepted_coids, rejected_coids,
                    );
                }
                Err(e) if e.is_submit_unknown_state() => {
                    if matches!(e, HttpErr::Timeout) {
                        let detail = format!(
                            "operation=cancel_replace_place target={} orders={}",
                            self.shared.clob_base_url,
                            place_coids.len(),
                        );
                        super::network_incident::record(
                            super::network_incident::NetworkSignal::HttpPlaceTimeout,
                            &detail,
                        );
                    }
                    // Timeout, status-less transport failure, HTTP 5xx, or
                    // 425 — server state is unknown.
                    // Emit NewOrderTimeout with the pre-computed orderID so
                    // the strategy can cancel / status-query by orderID
                    // directly, no open-order scan needed.
                    let is_http_425 = e.is_http_425();
                    if self.shared.should_warn_unknown_state(&e) {
                        warn!(
                            "[PolymarketTrade] Submit unknown state ({}) coids={:?} → NewOrderTimeout",
                            e, place_coids,
                        );
                    }
                    for (i, oh) in place_signed.iter().enumerate() {
                        let order = &place_chunk[place_body_to_chunk[i]];
                        if is_http_425 {
                            self.shared.note_http_425_backoff(&order.client_order_id);
                        }
                        self.shared.defer_lifecycle_account_apply(
                            &order.client_order_id,
                            OrderStatus::NewOrderTimeout,
                        );
                        updates.push(Self::make_timeout_place(order, Some(oh)));
                    }
                }
                Err(e) => {
                    let err_s = e.to_string();
                    if SharedState::is_trading_disabled_error(&err_s) {
                        self.handle_trading_disabled(&err_s);
                    } else if SharedState::is_balance_error(&err_s) {
                        // Pick the first place_chunk order (mapped via
                        // place_body_to_chunk) as the targeted-cancel
                        // scope representative. See `batch_submit_orders`
                        // comment for the uniformity rationale.
                        if let Some(&first_idx) = place_body_to_chunk.first() {
                            let first = &place_chunk[first_idx];
                            self.handle_balance_error(
                                &first.client_order_id,
                                first.side,
                                &first.symbol,
                            );
                        }
                    } else if SharedState::is_invalid_token_error(&err_s) {
                        if let Some(&first_idx) = place_body_to_chunk.first() {
                            self.handle_invalid_token(&place_chunk[first_idx].symbol);
                        }
                    }
                    warn!(
                        "[PolymarketTrade] Submit failed: {} coids={:?}",
                        e, place_coids
                    );
                    for (i, _) in place_signed.iter().enumerate() {
                        let order = &place_chunk[place_body_to_chunk[i]];
                        self.shared
                            .remove_order_as(&order.client_order_id, OrderStatus::Rejected);
                        updates.push(Self::make_rejected(order, &err_s));
                    }
                }
            }
        }
        // Local-signing failures collected at the top of the fn — emit
        // their Rejected updates now (they never reached the server).
        updates.extend(place_sign_failures);

        // If the caller handed us >15 places, finish the remainder via the
        // existing serial batch_submit_orders path (it already chunks 15s).
        if place_orders.len() > 15 {
            updates.extend(self.batch_submit_orders(_market_id, place_orders)?);
        }

        for update in &updates {
            if update.exchange_order_id.is_some()
                && place_orders
                    .iter()
                    .any(|order| order.client_order_id == update.client_order_id)
            {
                self.shared.log_order_lifecycle(
                    &update.client_order_id,
                    "http_response",
                    update.exchange_order_id.as_deref(),
                    Some(update.status),
                    None,
                );
            }
        }
        Ok(updates)
    }

    fn name(&self) -> &str {
        "polymarket-live"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shutdown_test_trade(shutdown: ShutdownToken) -> PolymarketTrade {
        PolymarketTrade::new_with_pool_for_startup_query_repair_and_shutdown(
            "api-key",
            "c2VjcmV0",
            "passphrase",
            "0x0000000000000000000000000000000000000000000000000000000000000001",
            false,
            10,
            SignatureType::Eoa,
            ClobVersion::V2,
            "",
            "",
            true,
            "shutdown-test",
            "",
            GapReplayConfig::default(),
            None,
            shutdown,
        )
        .unwrap()
    }

    #[test]
    fn unified_shutdown_drains_and_joins_account_owner_and_audit_workers() {
        let shutdown = ShutdownToken::new();
        let trade = shutdown_test_trade(shutdown.clone());
        let shared = trade.shared_state();

        shutdown.request();
        assert_eq!(
            shared.background_worker_handles.lock().unwrap().len(),
            3,
            "cold account, private lifecycle, and audit each have one worker",
        );
        shutdown.finish();
        assert_eq!(shared.join_background_workers(), 3);
        assert!(shared.background_worker_handles.lock().unwrap().is_empty());
    }

    #[test]
    fn single_writer_lifecycle_queue_has_bounded_tail_and_no_overflow() {
        // One full private-owner scheduling burst. Production ingress applies
        // backpressure/replay long before an account accumulates thousands of
        // lifecycle mutations at once.
        const EVENTS: usize = 128;
        let shutdown = ShutdownToken::new();
        let trade = shutdown_test_trade(shutdown.clone());
        let shared = trade.shared_state();
        shared.account_state.register_instance("owner", 1.0);
        shared
            .account_state
            .apply_physical_snapshot(100.0, HashMap::new())
            .unwrap();
        shared
            .account_state
            .register_token_fee_config(&["UP".to_string()], 0.0, 1.0)
            .unwrap();
        for index in 0..EVENTS {
            shared
                .account_state
                .reserve_order(
                    "owner",
                    &format!("owner-{index}"),
                    &format!("oid-{index}"),
                    "UP",
                    Side::Buy,
                    0.01,
                    0.01,
                    0,
                )
                .unwrap();
        }

        // Test setup mutates the account directly and can overlap a pending
        // background snapshot. Establish the same quiescent edge that live
        // startup provides before measuring the steady-state owner lane.
        let drain_started = std::time::Instant::now();
        let (drain_tx, drain_rx) = crossbeam_channel::bounded(1);
        shared
            .account_lifecycle_tx
            .send(AccountLifecycleJob::StartupBarrier(drain_tx))
            .unwrap();
        drain_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("account owner pre-measurement barrier stalled");
        let pre_measurement_drain_ns =
            drain_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;

        let (completion_tx, completion_rx) = crossbeam_channel::bounded(EVENTS);
        let mut queue_peak = 0usize;
        let mut overflow = 0usize;
        for index in 0..EVENTS {
            let job = DeferredLifecycleJob {
                client_order_id: format!("owner-{index}"),
                status: OrderStatus::Accepted,
                enqueued_ns: now_ns(),
                completion: Some(completion_tx.clone()),
            };
            if shared
                .account_lifecycle_tx
                .try_send(AccountLifecycleJob::DeferredLifecycle(job))
                .is_err()
            {
                overflow += 1;
            }
            queue_peak = queue_peak.max(shared.account_lifecycle_tx.len());
        }
        drop(completion_tx);
        let mut samples = Vec::with_capacity(EVENTS);
        for _ in 0..EVENTS - overflow {
            samples.push(
                completion_rx
                    .recv_timeout(std::time::Duration::from_secs(2))
                    .expect("single account writer stalled"),
            );
        }
        samples.sort_unstable();
        let percentile = |numerator: usize, denominator: usize| {
            samples[(samples.len() - 1) * numerator / denominator]
        };
        let p50 = percentile(1, 2);
        let p99 = percentile(99, 100);
        let p999 = percentile(999, 1_000);
        let max = *samples.last().unwrap();
        eprintln!(
            "single-writer lifecycle: n={} pre_measurement_drain_ns={} min_ns={} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_peak={} overflow={}",
            samples.len(), pre_measurement_drain_ns, samples[0], p50, p99, p999, max, queue_peak, overflow,
        );
        assert_eq!(overflow, 0);
        assert!(
            max < 50_000_000,
            "steady-state single-writer lifecycle maximum exceeded 50 ms: {max} ns",
        );

        shutdown.request();
        shutdown.finish();
        assert_eq!(shared.join_background_workers(), 3);
    }

    #[test]
    fn execution_owner_publication_has_bounded_tail_ordering_and_no_overflow() {
        const EVENTS: usize = 128;
        let shutdown = ShutdownToken::new();
        let trade = shutdown_test_trade(shutdown.clone());
        let shared = trade.shared_state();
        let (completion_tx, completion_rx) = crossbeam_channel::bounded(EVENTS);
        let mut queue_peak = 0usize;
        let mut overflow = 0usize;

        for index in 0..EVENTS {
            let command = ExecutionStateCommand::BenchmarkPublishOpen {
                client_order_id: format!("benchmark-{index}"),
                tracked: TrackedOrder {
                    order_slot: Default::default(),
                    symbol: format!("token-{index}"),
                    side: Side::Buy,
                    instance_id: "benchmark-owner".to_string(),
                },
                enqueued_ns: now_ns(),
                completion: completion_tx.clone(),
            };
            if shared
                .account_lifecycle_tx
                .try_send(AccountLifecycleJob::ExecutionState(command))
                .is_err()
            {
                overflow += 1;
            }
            queue_peak = queue_peak.max(shared.account_lifecycle_tx.len());
        }
        drop(completion_tx);

        let mut samples = Vec::with_capacity(EVENTS);
        for _ in 0..EVENTS - overflow {
            samples.push(
                completion_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("execution owner publication stalled"),
            );
        }
        samples.sort_unstable();
        let percentile = |numerator: usize, denominator: usize| {
            samples[(samples.len() - 1) * numerator / denominator]
        };
        let snapshot = shared.execution_snapshot();
        assert_eq!(snapshot.open_orders.len(), EVENTS);
        assert_eq!(
            snapshot.open_orders["benchmark-127"].symbol, "token-127",
            "FIFO application must publish the final command",
        );
        let p50 = percentile(1, 2);
        let p99 = percentile(99, 100);
        let p999 = percentile(999, 1_000);
        let max = *samples.last().unwrap();
        eprintln!(
            "execution-owner publication: boundary=try_send_to_arc_swap_store n={} min_ns={} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_peak={} overflow={}",
            samples.len(), samples[0], p50, p99, p999, max, queue_peak, overflow,
        );
        assert_eq!(overflow, 0);
        assert!(
            max < 50_000_000,
            "execution owner publication maximum exceeded 50 ms: {max} ns",
        );

        shutdown.request();
        shutdown.finish();
        assert_eq!(shared.join_background_workers(), 3);
    }

    #[test]
    fn execution_owner_lane_overflow_is_bounded_and_preserves_the_command() {
        let (tx, rx) = crossbeam_channel::bounded(1);
        let command = || {
            AccountLifecycleJob::ExecutionState(ExecutionStateCommand::TrackOpen {
                client_order_id: "bounded-coid".to_string(),
                tracked: TrackedOrder {
                    order_slot: Default::default(),
                    symbol: "TOKEN".to_string(),
                    side: Side::Buy,
                    instance_id: "owner".to_string(),
                },
            })
        };
        tx.try_send(command()).unwrap();
        assert!(matches!(
            tx.try_send(command()),
            Err(crossbeam_channel::TrySendError::Full(
                AccountLifecycleJob::ExecutionState(ExecutionStateCommand::TrackOpen { .. })
            )),
        ));
        assert!(matches!(
            rx.try_recv(),
            Ok(AccountLifecycleJob::ExecutionState(
                ExecutionStateCommand::TrackOpen { .. }
            )),
        ));
    }

    fn runtime_ownership(order_id: &str, client_order_id: &str) -> OrderOwnership {
        OrderOwnership {
            order_slot: Default::default(),
            account_id: "acct".into(),
            instance_id: "maker".into(),
            client_order_id: client_order_id.into(),
            order_id: order_id.into(),
            token_id: "UP".into(),
            side: Side::Buy,
            quantity: 10.0,
            filled_quantity: 0.0,
            terminal_matched_quantity: None,
            terminal_trade_ids: Vec::new(),
            terminal_trade_ids_authoritative: false,
            price: 0.5,
            fee_rate_bps: 0,
            reserved_cash: 5.0,
            reserved_quantity: 0.0,
            status: OrderStatus::Pending,
        }
    }

    #[test]
    fn runtime_ownership_reader_never_waits_for_writer_serialization() {
        let index = Arc::new(RuntimeOwnershipIndex::new());
        let ownership = runtime_ownership("0xABC", "maker-1");
        index
            .insert(&ownership.order_id, ownership.clone())
            .unwrap();

        let writer_index = index.clone();
        let writer = std::thread::spawn(move || {
            for sequence in 0..1_000 {
                let oid = format!("0x{sequence:064x}");
                writer_index
                    .insert(&oid, runtime_ownership(&oid, "writer"))
                    .unwrap();
            }
        });
        assert_eq!(
            index
                .get(&ownership.order_id)
                .map(|value| value.client_order_id),
            Some("maker-1".to_string()),
            "a fixed-table reader must remain available while another writer publishes",
        );
        writer.join().unwrap();
    }

    #[test]
    fn runtime_ownership_snapshot_publishes_insert_and_remove() {
        let index = RuntimeOwnershipIndex::new();
        let ownership = runtime_ownership("0xDEF", "maker-2");
        index
            .insert(&ownership.order_id, ownership.clone())
            .unwrap();
        assert!(index.get("def").is_some());
        assert!(index.contains("  0XDeF "));
        index.remove("  0XdEf  ");
        assert!(index.get("DEF").is_none());
    }

    #[test]
    fn runtime_ownership_numeric_lookup_latency_profile() {
        const EVENTS: usize = 20_000;
        let index = RuntimeOwnershipIndex::new();
        let ownership = runtime_ownership("0x0123456789abcdef", "maker-latency");
        index
            .insert("0x0123456789abcdef", ownership)
            .expect("benchmark ownership insert");

        let mut samples = Vec::with_capacity(EVENTS);
        for sequence in 0..EVENTS {
            let oid = if sequence & 1 == 0 {
                "0123456789ABCDEF"
            } else {
                "  0x0123456789abcdef  "
            };
            let started = std::time::Instant::now();
            assert!(index.contains(oid));
            samples.push(started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        }
        samples.sort_unstable();
        let percentile = |numerator: usize, denominator: usize| {
            samples[(samples.len() - 1) * numerator / denominator]
        };
        eprintln!(
            "runtime-ownership lookup: boundary=normalized_oid_view_to_fixed_table_match n={} p50_ns={} p99_ns={} p999_ns={} max_ns={} table_capacity={} probe_budget={} queue_depth=0 overflow=0",
            samples.len(),
            percentile(1, 2),
            percentile(99, 100),
            percentile(999, 1_000),
            samples.last().copied().unwrap_or_default(),
            RUNTIME_OWNERSHIP_CAPACITY,
            RUNTIME_OWNERSHIP_MAX_PROBES,
        );
    }

    #[test]
    fn request_body_pool_reuses_the_same_backing_allocation() {
        let pool = new_request_buffer_pool();
        assert_eq!(pool.len(), REQUEST_BUFFER_SLOTS);
        let mut buffer = pool.pop().unwrap();
        let original_ptr = buffer.as_ptr();
        buffer.extend_from_slice(br#"{"orderID":"oid"}"#);
        let body = buffer.freeze();
        let retained = body.clone();
        drop(body);
        let mut recovered = retained
            .try_into_mut()
            .expect("unique Bytes owner recovers BytesMut without copying");
        recovered.clear();
        assert_eq!(recovered.as_ptr(), original_ptr);
        pool.push(recovered).unwrap();
        assert_eq!(pool.len(), REQUEST_BUFFER_SLOTS);
    }

    #[test]
    fn request_body_pool_exhaustion_is_explicit_and_bounded() {
        let pool = new_request_buffer_pool();
        let mut leased = Vec::with_capacity(REQUEST_BUFFER_SLOTS);
        while let Some(buffer) = pool.pop() {
            leased.push(buffer);
        }
        assert_eq!(leased.len(), REQUEST_BUFFER_SLOTS);
        assert!(pool.pop().is_none(), "pool must not allocate a fallback buffer");
        for buffer in leased {
            pool.push(buffer).unwrap();
        }
        assert_eq!(pool.len(), REQUEST_BUFFER_SLOTS);
    }

    #[test]
    fn routine_cancel_audits_keep_the_bounded_recovery_pass_running() {
        assert!(startup_recovery_audits_complete(0, 0));
        assert!(!startup_recovery_audits_complete(1, 0));
        assert!(!startup_recovery_audits_complete(0, 1));
        assert!(!startup_recovery_audits_complete(1, 1));
    }

    #[test]
    fn startup_recovery_warns_only_after_the_final_retry() {
        let attempts = 6;
        for attempt in 0..attempts - 1 {
            assert!(!startup_recovery_warn_attempt(attempt, attempts));
        }
        assert!(startup_recovery_warn_attempt(attempts - 1, attempts));
    }

    #[test]
    fn query_repair_never_uses_event_end_to_skip_the_order_lookup() {
        assert!(!should_lookup_recovered_event_end(false, false));
        assert!(should_lookup_recovered_event_end(true, false));
        assert!(!should_lookup_recovered_event_end(true, true));
        assert_eq!(
            prequery_recovered_order_close_reason(true, true, false),
            Some(RecoveredOrderCloseReason::EventEnded),
        );
        assert_eq!(
            prequery_recovered_order_close_reason(true, true, true),
            None,
        );
        assert!(can_reuse_persisted_terminal_audit(true, false));
        assert!(!can_reuse_persisted_terminal_audit(true, true));
    }

    #[test]
    fn expected_json_null_remains_fail_closed_without_operator_warning() {
        let json_null = FetchUnavailable::InvalidResponse("null".to_string());
        let malformed = FetchUnavailable::InvalidResponse("{}".to_string());
        assert!(!fetch_unavailable_should_warn(&json_null));
        assert!(fetch_unavailable_should_warn(&malformed));
        assert!(fetch_unavailable_should_warn(&FetchUnavailable::Timeout));
    }

    #[test]
    fn repetitive_orphan_warning_window_is_atomic_and_reopens() {
        let silent_until = std::sync::atomic::AtomicU64::new(0);
        assert!(claim_rate_limited_warning(&silent_until, 100));
        assert!(!claim_rate_limited_warning(&silent_until, 101));
        assert!(claim_rate_limited_warning(
            &silent_until,
            100 + EXPECTED_ORPHAN_WARN_INTERVAL_NS,
        ));
    }

    #[test]
    fn recovery_terminal_update_hands_authoritative_audit_to_strategy_first() {
        let ownership = OrderOwnership {
            order_slot: Default::default(),
            account_id: "acct".into(),
            instance_id: "maker".into(),
            client_order_id: "maker-1".into(),
            order_id: "oid-1".into(),
            token_id: "UP".into(),
            side: Side::Buy,
            quantity: 10.0,
            filled_quantity: 1.0,
            terminal_matched_quantity: Some(4.0),
            terminal_trade_ids: vec!["trade-1".into()],
            terminal_trade_ids_authoritative: true,
            price: 0.5,
            fee_rate_bps: 0,
            reserved_cash: 1.5,
            reserved_quantity: 0.0,
            status: OrderStatus::Filled,
        };
        let audit = AuthoritativeOrderAudit {
            original_size: Some("10".into()),
            size_matched: Some("4".into()),
            associate_trades: vec!["trade-1".into()],
        };
        let update = PolymarketTrade::authoritative_recovery_update(
            &ownership,
            "oid-1",
            OrderStatus::Filled,
            audit.clone(),
        );
        assert_eq!(update.order_audit, Some(audit));
        assert_eq!(update.remaining_quantity, 6.0);
        assert_eq!(
            update.error.as_deref(),
            Some(ORPHAN_RECONCILE_AUTHORITATIVE_TERMINAL),
        );
    }

    fn valid_signing_order() -> OrderRequest {
        OrderRequest::new_limit(
            Exchange::Polymarket,
            "123456789".to_string(),
            Side::Buy,
            0.5,
            1.0,
        )
    }

    #[test]
    fn attempt_order_replica_preserves_replay_identity_and_inputs() {
        let mut order = valid_signing_order();
        order.client_order_id = "btc01-42".into();
        order.instance_id = "untrusted-request-iid".into();
        order.quote_event_id = "btc-updown-5m-42".into();
        order.quote_trigger_source = QuoteTriggerSource::OrderBook(Exchange::Polymarket);
        order.quote_trigger_exchange_timestamp_ns = 101;
        order.quote_trigger_local_timestamp_ns = 202;
        order.order_type = crate::types::OrderType::Fak;
        order.post_only = false;
        order.reduce_only = true;
        order.fee_rate_bps = 125;

        let replica = AttemptOrderReplica::from_order(&order, "btc01");

        assert_eq!(replica.instance_id, "btc01");
        assert_eq!(replica.event_id, "btc-updown-5m-42");
        assert_eq!(replica.token, "123456789");
        assert_eq!(replica.side, Side::Buy);
        assert_eq!(replica.order_type, crate::types::OrderType::Fak);
        assert_eq!(replica.price, Some(0.5));
        assert_eq!(replica.quantity, 1.0);
        assert!(!replica.post_only);
        assert!(replica.reduce_only);
        assert_eq!(replica.fee_rate_bps, 125);
        assert_eq!(
            replica.trigger_source,
            QuoteTriggerSource::OrderBook(Exchange::Polymarket)
        );
        assert_eq!(replica.trigger_exchange_ns, 101);
        assert_eq!(replica.trigger_local_ns, 202);
    }

    #[test]
    fn terminal_trade_lookup_distinguishes_absent_from_present_record() {
        let trade_id = "43535f84-454f-4302-b4cd-23b4510d9723";
        assert!(terminal_trade_records(serde_json::json!([]), trade_id).is_empty());
        assert!(terminal_trade_records(
            serde_json::json!({"data": [{"id": "different"}]}),
            trade_id,
        )
        .is_empty());

        let present = terminal_trade_records(
            serde_json::json!({
                "data": [{"id": trade_id, "status": "MATCHED", "malformed": true}]
            }),
            trade_id,
        );
        assert_eq!(present.len(), 1);
        assert_eq!(present[0]["id"], trade_id);
    }

    #[test]
    fn terminal_trade_lookup_uses_official_path_and_canonical_hmac_endpoint() {
        let path = terminal_trade_lookup_path("trade-123");
        assert_eq!(path, "/data/trades?id=trade-123");
        assert_eq!(canonical_l2_auth_path(&path), "/data/trades");
        assert_eq!(
            canonical_l2_auth_path("/data/order/oid-123"),
            "/data/order/oid-123"
        );
    }

    #[test]
    fn historical_trade_audit_matches_taker_and_maker_order_ids() {
        let taker = serde_json::json!({"taker_order_id": "0xABC", "maker_orders": []});
        let maker = serde_json::json!({
            "taker_order_id": "other",
            "maker_orders": [{"order_id": "abc"}],
        });
        let unrelated = serde_json::json!({
            "taker_order_id": "other",
            "maker_orders": [{"order_id": "different"}],
        });
        assert!(trade_record_references_order(&taker, "abc"));
        assert!(trade_record_references_order(&maker, "0xABC"));
        assert!(!trade_record_references_order(&unrelated, "abc"));
    }

    #[test]
    fn historical_trade_page_requires_complete_pagination_schema() {
        let (records, next) = authenticated_trade_page(serde_json::json!({
            "data": [{"id": "trade-1"}],
            "next_cursor": "cursor-2",
        }))
        .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(next, "cursor-2");
        assert!(authenticated_trade_page(serde_json::json!({"data": {}})).is_err());
        assert!(authenticated_trade_page(serde_json::json!(null)).is_err());
    }

    #[test]
    fn orphan_failure_summary_counts_each_class() {
        let mut summary = OrphanOrderStartupRepair::default();
        summary.unresolved(OrphanOrderRepairFailure::TradeAuditUnavailable);
        summary.unresolved(OrphanOrderRepairFailure::TradeAuditUnavailable);
        summary.unresolved(OrphanOrderRepairFailure::EventNotSettled);
        assert_eq!(summary.unresolved, 3);
        assert_eq!(
            summary.failures[&OrphanOrderRepairFailure::TradeAuditUnavailable],
            2,
        );
        assert_eq!(
            summary.failures[&OrphanOrderRepairFailure::EventNotSettled],
            1
        );
    }

    #[test]
    fn signing_validation_rejects_non_finite_and_unrepresentable_numbers() {
        for price in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 0.0, 1.0] {
            let mut order = valid_signing_order();
            order.price = Some(price);
            assert!(validate_order_for_signing(&order).is_err(), "price={price}");
        }

        for quantity in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 0.0, -1.0] {
            let mut order = valid_signing_order();
            order.quantity = quantity;
            assert!(
                validate_order_for_signing(&order).is_err(),
                "quantity={quantity}"
            );
        }

        let mut sub_precision = valid_signing_order();
        sub_precision.quantity = 0.000_000_1;
        assert!(validate_order_for_signing(&sub_precision).is_err());

        let mut overflow = valid_signing_order();
        overflow.quantity = f64::MAX;
        assert!(validate_order_for_signing(&overflow).is_err());

        let mut invalid_fee = valid_signing_order();
        invalid_fee.fee_rate_bps = 10_001;
        assert!(validate_order_for_signing(&invalid_fee).is_err());

        let mut invalid_token = valid_signing_order();
        invalid_token.symbol = "not-a-token-id".to_string();
        assert!(validate_order_for_signing(&invalid_token).is_err());

        assert_eq!(
            validate_order_for_signing(&valid_signing_order()).unwrap(),
            0.5
        );
    }

    #[test]
    fn late_http_ack_response_cannot_regress_matched_status() {
        assert_eq!(
            placement_response_status(true, "live", Some(OrderStatus::Filled)),
            OrderStatus::Filled,
        );
        assert_eq!(
            placement_response_status(true, "delayed", Some(OrderStatus::Filled)),
            OrderStatus::Filled,
        );
        assert_eq!(
            placement_response_status(true, "matched", Some(OrderStatus::Accepted)),
            OrderStatus::Filled,
        );
        assert_eq!(
            placement_response_status(true, "live", Some(OrderStatus::PartiallyFilled)),
            OrderStatus::PartiallyFilled,
        );
        assert_eq!(
            placement_response_status(true, "matched", Some(OrderStatus::PartiallyFilled)),
            OrderStatus::Filled,
        );
        assert_eq!(
            placement_response_status(false, "", None),
            OrderStatus::Rejected,
            "a complete exchange-side rejection must release active reservation",
        );
    }

    #[test]
    fn placement_response_requires_explicit_complete_decision_fields() {
        for malformed in [
            serde_json::json!(null),
            serde_json::json!({}),
            serde_json::json!({"success": "false", "errorMsg": "rejected"}),
            serde_json::json!({"success": false}),
            serde_json::json!({"success": true}),
            serde_json::json!({"success": true, "orderID": 123}),
        ] {
            assert!(
                parse_placement_response(&malformed).is_err(),
                "malformed response accepted: {malformed}",
            );
        }
        assert_eq!(
            parse_placement_response(&serde_json::json!({
                "success": false,
                "errorMsg": "post-only order crosses book",
            }))
            .unwrap()
            .success,
            false,
        );
        assert_eq!(
            parse_placement_response(&serde_json::json!({
                "success": true,
                "orderID": "0xabc",
                "status": "live",
            }))
            .unwrap()
            .order_id,
            "0xabc",
        );
    }

    #[test]
    fn cancel_all_finality_requires_complete_response_schema() {
        assert_eq!(
            validated_cancel_all_counts(&serde_json::json!({
                "canceled": ["oid-1"],
                "not_canceled": {},
            })),
            Some((1, 0)),
        );
        for json in [
            serde_json::json!({}),
            serde_json::json!({ "canceled": [] }),
            serde_json::json!({ "canceled": null, "not_canceled": {} }),
            serde_json::json!({ "canceled": [], "not_canceled": [] }),
            serde_json::json!({ "canceled": [null], "not_canceled": {} }),
            serde_json::json!({ "canceled": [""], "not_canceled": {} }),
            serde_json::json!({ "canceled": [], "not_canceled": { "oid": null } }),
            serde_json::json!({ "success": false, "canceled": [], "not_canceled": {} }),
            serde_json::json!({ "error": "unavailable", "canceled": [], "not_canceled": {} }),
        ] {
            assert_eq!(validated_cancel_all_counts(&json), None);
        }
    }

    #[test]
    fn clob_request_roles_separate_account_orders_reconcile_and_global_query() {
        assert_eq!(
            request_role(&reqwest::Method::POST, "/order"),
            crate::http1_pool::Role::Fast,
        );
        assert_eq!(
            request_role(&reqwest::Method::DELETE, "/orders"),
            crate::http1_pool::Role::Cancel,
        );
        assert_eq!(
            request_role(&reqwest::Method::GET, "/data/order/0xabc"),
            crate::http1_pool::Role::Reconcile,
        );
        assert_eq!(
            request_role(&reqwest::Method::GET, "/balance-allowance"),
            crate::http1_pool::Role::Query,
        );
    }

    #[test]
    fn authoritative_order_parser_retains_exact_sizes_and_trade_ids() {
        let json = serde_json::json!({
            "id": "0xabc",
            "status": "MATCHED",
            "original_size": "40",
            "size_matched": "39.993332",
            "associate_trades": ["trade-1", "trade-2", "trade-3", "trade-4"],
            "maker_address": "0x1234",
            "asset_id": "up-token",
            "side": "BUY",
            "price": "0.42",
            "fee_rate_bps": "200"
        });
        let fetched = parse_fetched_order(&json, "0xabc").expect("matched order");
        assert_eq!(fetched.status, "MATCHED");
        assert_eq!(fetched.audit.original_size.as_deref(), Some("40"));
        assert_eq!(fetched.audit.size_matched.as_deref(), Some("39.993332"));
        assert_eq!(
            fetched.audit.associate_trades,
            vec!["trade-1", "trade-2", "trade-3", "trade-4"]
        );
        let identity = fetched.rebuild_identity.expect("rebuild identity");
        assert_eq!(identity.maker_address, "0x1234");
        assert_eq!(identity.asset_id, "up-token");
        assert_eq!(identity.side, Side::Buy);
        assert_eq!(identity.price, 0.42);
        assert_eq!(identity.fee_rate_bps, Some(200));
    }

    #[test]
    fn authoritative_order_parser_canonicalizes_matched_not_broadcasted() {
        let json = serde_json::json!({
            "orderID": "0xabc",
            "status": "MATCHED_NOT_BROADCASTED",
            "original_size": "5",
            "size_matched": "5",
            "associate_trades": ["trade-1"]
        });
        let fetched = parse_fetched_order(&json, "0xabc").expect("matched order");
        assert_eq!(fetched.status, "MATCHED");
    }

    #[test]
    fn authoritative_order_parser_preserves_numeric_fixed_point_values() {
        let json = serde_json::json!({
            "order_id": "0xabc",
            "status": "MATCHED",
            "original_size": 20,
            "size_matched": 19.990489,
            "associate_trades": ["trade-1"]
        });
        let fetched = parse_fetched_order(&json, "0xabc").expect("matched order");
        assert_eq!(fetched.audit.original_size.as_deref(), Some("20"));
        assert_eq!(fetched.audit.size_matched.as_deref(), Some("19.990489"));
    }

    #[test]
    fn reconnect_audit_never_reduces_durable_matched_quantity() {
        assert_eq!(effective_audited_match(Some("2"), 10.0, 4.0), (4.0, true));
        assert_eq!(effective_audited_match(Some("6"), 10.0, 4.0), (6.0, true));
        assert_eq!(effective_audited_match(None, 10.0, 4.0), (4.0, false));
        assert_eq!(
            effective_audited_match(Some("NaN"), 10.0, 4.0),
            (4.0, false)
        );
        assert_eq!(effective_audited_match(Some("11"), 10.0, 4.0), (4.0, false));
    }

    // Event-expiry reclaim purges only the settling event's tokens, leaving
    // other concurrent events' coid↔oid mappings intact — and a coid kept
    // alive past a reject (so a racy late fill still resolves) is reclaimed
    // exactly when its token settles.
    #[test]
    fn reclaim_token_mappings_purges_only_settling_tokens() {
        let mut coid_to_oid: HashMap<String, String> = HashMap::new();
        let mut oid_to_coid: HashMap<String, String> = HashMap::new();
        let mut coid_to_token: HashMap<String, String> = HashMap::new();
        // Two events: event A tokens {AUP, ADN}, event B token {BUP}.
        for (coid, oid, tok) in [
            ("c1", "0xa1", "AUP"), // a rejected-but-kept order on event A
            ("c2", "0xa2", "ADN"),
            ("c3", "0xb1", "BUP"), // event B — must survive
        ] {
            coid_to_oid.insert(coid.into(), oid.into());
            oid_to_coid.insert(normalize_order_id(oid), coid.into());
            coid_to_token.insert(coid.into(), tok.into());
        }

        let n = reclaim_token_mappings(
            &mut coid_to_oid,
            &mut oid_to_coid,
            &mut coid_to_token,
            &["AUP".to_string(), "ADN".to_string()],
            None,
        );
        assert_eq!(n, 2, "both event-A coids reclaimed");
        // Event A fully purged from all three maps.
        assert!(!coid_to_oid.contains_key("c1") && !coid_to_oid.contains_key("c2"));
        assert!(!oid_to_coid.contains_key("a1") && !oid_to_coid.contains_key("a2"));
        assert!(!coid_to_token.contains_key("c1"));
        // Event B untouched — its late fill can still map.
        assert_eq!(coid_to_oid.get("c3").map(String::as_str), Some("0xb1"));
        assert_eq!(oid_to_coid.get("b1").map(String::as_str), Some("c3"));
        assert_eq!(coid_to_token.get("c3").map(String::as_str), Some("BUP"));
    }

    /// Locks the 3-way reason → outcome mapping against the live-observed
    /// strings. If the server changes wording we want this test to fail.
    #[test]
    fn cancel_not_canceled_outcome_recognises_live_reasons() {
        // Definite Filled — order matched before cancel landed.
        assert_eq!(
            cancel_not_canceled_outcome("matched orders can't be canceled"),
            CancelReasonOutcome::Filled,
        );
        // Definite Cancelled — server confirms already gone, no fill.
        assert_eq!(
            cancel_not_canceled_outcome("the order is already canceled"),
            CancelReasonOutcome::Cancelled,
        );
        // Ambiguous — server admits both possibilities. Defer to reconcile.
        assert_eq!(
            cancel_not_canceled_outcome("order can't be found - already canceled or matched"),
            CancelReasonOutcome::Uncertain,
        );
    }

    #[test]
    fn initial_cancel_is_excluded_and_third_reconcile_delete_is_terminal() {
        let reason = "order can't be found - already canceled or matched";
        let initial_outcome = cancel_not_canceled_outcome(reason);
        let mut counts = HashMap::new();

        assert_eq!(initial_outcome, CancelReasonOutcome::Uncertain);
        assert!(
            counts.is_empty(),
            "the initial cancel response must not enter the reconcile counter",
        );

        assert_eq!(
            record_cancel_not_found_observation(
                &mut counts,
                "coid-a",
                Some(reason),
                initial_outcome,
            ),
            Some(1),
        );
        assert_eq!(
            cancel_not_found_outcome_after_observation(
                initial_outcome,
                counts.get("coid-a").copied(),
            ),
            CancelReasonOutcome::Uncertain,
            "first reconcile DELETE keeps the orphan",
        );
        assert_eq!(
            record_cancel_not_found_observation(
                &mut counts,
                "coid-a",
                Some(reason),
                initial_outcome,
            ),
            Some(2),
        );
        assert_eq!(
            cancel_not_found_outcome_after_observation(
                initial_outcome,
                counts.get("coid-a").copied(),
            ),
            CancelReasonOutcome::Uncertain,
            "second reconcile DELETE keeps the orphan",
        );
        let third = record_cancel_not_found_observation(
            &mut counts,
            "coid-a",
            Some(reason),
            initial_outcome,
        );
        assert_eq!(third, Some(CANCEL_NOT_FOUND_TERMINAL_LIMIT));
        assert_eq!(
            cancel_not_found_outcome_after_observation(initial_outcome, third),
            CancelReasonOutcome::Cancelled,
            "third reconcile DELETE observation must be a Cancelled terminal",
        );
    }

    #[test]
    fn retired_market_requires_three_complete_invalid_token_and_absence_passes() {
        assert!(!retired_market_terminalization_allowed(2, 2, 2, 1, 1));
        assert!(!retired_market_terminalization_allowed(3, 2, 1, 1, 1));
        assert!(!retired_market_terminalization_allowed(3, 2, 2, 2, 1));
        assert!(retired_market_terminalization_allowed(3, 2, 2, 1, 1));
        assert!(retired_market_terminalization_allowed(3, 2, 2, 0, 0));
    }

    #[test]
    fn parallel_reconcile_absence_requires_both_slots() {
        let combined = combine_parallel_order_lookups(
            FetchOrderResult::Unavailable(FetchUnavailable::InvalidResponse("null".into())),
            FetchOrderResult::Unavailable(FetchUnavailable::InvalidResponse("null".into())),
            (crate::http1_pool::Role::Reconcile, 0),
            (crate::http1_pool::Role::Reconcile, 1),
        );
        assert!(matches!(
            combined,
            FetchOrderResult::Unavailable(ref kind) if kind.is_parallel_absence()
        ));

        let incomplete = combine_parallel_order_lookups(
            FetchOrderResult::NotFound("404".into()),
            FetchOrderResult::Unavailable(FetchUnavailable::Transport),
            (crate::http1_pool::Role::Reconcile, 0),
            (crate::http1_pool::Role::Reconcile, 1),
        );
        assert!(matches!(
            incomplete,
            FetchOrderResult::Unavailable(FetchUnavailable::Transport)
        ));

        let dual_404 = combine_parallel_order_lookups(
            FetchOrderResult::NotFound("primary 404".into()),
            FetchOrderResult::NotFound("secondary 404".into()),
            (crate::http1_pool::Role::Reconcile, 0),
            (crate::http1_pool::Role::Reconcile, 1),
        );
        assert!(dual_404.has_parallel_not_found_evidence());
        let attempts = ReconcileAttemptCounters::default();
        assert_eq!(attempts.next_placement_with_evidence("coid", 2), 2);
        assert_eq!(attempts.next_placement_with_evidence("coid", 2), 4);
    }

    #[test]
    fn expired_market_fast_path_requires_remote_clean_and_parallel_absence() {
        let missing = RuntimeMissingOrder {
            client_order_id: "coid".into(),
            tracked: TrackedOrder {
                order_slot: Default::default(),
                symbol: "token".into(),
                side: Side::Buy,
                instance_id: "instance".into(),
            },
            order_id: "order".into(),
            evidence: "parallel_reconcile_absence_json_null".into(),
        };
        assert!(retired_market_parallel_absence_fast_path_allowed(
            true,
            true,
            1,
            std::slice::from_ref(&missing),
        ));
        assert!(!retired_market_parallel_absence_fast_path_allowed(
            true,
            false,
            1,
            std::slice::from_ref(&missing),
        ));
        let mut single = missing;
        single.evidence = "single_order_lookup_json_null".into();
        assert!(!retired_market_parallel_absence_fast_path_allowed(
            true,
            true,
            1,
            std::slice::from_ref(&single),
        ));
    }

    #[test]
    fn cancels_disabled_is_terminal_evidence_only_for_expired_market_sweeps() {
        let error = "503 Service Unavailable: {\"error\":\"cancels are disabled\"}";
        assert!(is_cancels_disabled_error(error));
        assert!(is_retired_market_terminal_error(error, true));
        assert!(!is_retired_market_terminal_error(error, false));
        assert!(is_retired_market_terminal_error("invalid token id", false));
    }

    #[test]
    fn preflight_rejection_reason_codes_are_stable_and_structured() {
        assert_eq!(
            preflight_rejection_reason_code("rate limited"),
            "rate_limit"
        );
        assert_eq!(
            preflight_rejection_reason_code("invalid token backoff"),
            "invalid_token_backoff"
        );
        assert_eq!(preflight_rejection_reason_code("bad price"), "validation");
    }

    #[test]
    fn cancel_not_found_counter_is_per_order_and_exact_reason_only() {
        let reason = "order can't be found - already canceled or matched";
        let outcome = cancel_not_canceled_outcome(reason);
        let mut counts = HashMap::new();

        assert_eq!(
            record_cancel_not_found_observation(&mut counts, "coid-a", Some(reason), outcome,),
            Some(1),
        );
        assert_eq!(
            record_cancel_not_found_observation(&mut counts, "coid-b", Some(reason), outcome,),
            Some(1),
        );
        assert_eq!(
            record_cancel_not_found_observation(
                &mut counts,
                "coid-a",
                Some("order not found"),
                CancelReasonOutcome::Uncertain,
            ),
            None,
        );
        assert_eq!(counts.get("coid-a"), Some(&1));
        assert_eq!(counts.get("coid-b"), Some(&1));
    }

    #[test]
    fn cancel_not_canceled_outcome_handles_case_and_variants() {
        // Case-insensitive.
        assert_eq!(
            cancel_not_canceled_outcome("MATCHED ORDERS CAN'T BE CANCELED"),
            CancelReasonOutcome::Filled,
        );
        // "cant" (no apostrophe) variant — defensive against server typo.
        assert_eq!(
            cancel_not_canceled_outcome("order cant be found"),
            CancelReasonOutcome::Uncertain,
        );
        // A plain not-found can still be read-replica lag.
        assert_eq!(
            cancel_not_canceled_outcome("order not found"),
            CancelReasonOutcome::Uncertain,
        );
    }

    #[test]
    fn cancel_not_canceled_outcome_unrecognised_stays_uncertain() {
        // Retry exhaustion must never manufacture a terminal state. Unknown
        // wording and an empty reason preserve the orphan's worst-case lock.
        assert_eq!(
            cancel_not_canceled_outcome("server explosion - try again later"),
            CancelReasonOutcome::Uncertain,
        );
        assert_eq!(
            cancel_not_canceled_outcome(""),
            CancelReasonOutcome::Uncertain,
        );
    }

    /// Regression for the live race: GET said LIVE, but the following DELETE
    /// returned "already canceled or matched". The reconcile retry must emit
    /// another timeout/orphan update, never a synthetic Cancelled that releases
    /// collateral before the delayed fill push arrives.
    #[test]
    fn reconcile_delete_uncertain_remains_cancel_orphan() {
        let oid = "0xlive";
        let outcome = cancel_delete_response_outcome(
            &serde_json::json!({
                "canceled": [],
                "not_canceled": {(oid): "order can't be found - already canceled or matched"}
            }),
            oid,
        );
        assert_eq!(outcome, CancelReasonOutcome::Uncertain);
        let emitted = match outcome {
            CancelReasonOutcome::Cancelled => OrderStatus::Cancelled,
            CancelReasonOutcome::Filled => OrderStatus::Filled,
            CancelReasonOutcome::Uncertain => OrderStatus::CancelUncertain,
        };
        assert_eq!(emitted, OrderStatus::CancelUncertain);
        assert_eq!(
            cancel_reason_order_status("order can't be found - already canceled or matched"),
            OrderStatus::CancelUncertain,
        );
        assert_eq!(
            cancel_reason_order_status("can't be canceled because it is pending/delayed"),
            OrderStatus::CancelUncertain,
        );
        assert_eq!(
            cancel_reason_order_status("the order is already canceled"),
            OrderStatus::Cancelled,
        );
        assert_eq!(
            cancel_reason_order_status("matched orders can't be canceled"),
            OrderStatus::Filled,
        );
    }

    #[test]
    fn delete_response_requires_authoritative_per_order_terminal() {
        let oid = "0xabc";
        assert_eq!(
            cancel_delete_response_outcome(
                &serde_json::json!({"canceled": [oid], "not_canceled": {}}),
                oid,
            ),
            CancelReasonOutcome::Cancelled,
        );
        assert_eq!(
            cancel_delete_response_outcome(
                &serde_json::json!({
                    "canceled": [],
                    "not_canceled": {"0xabc": "order can't be found - already canceled or matched"}
                }),
                oid,
            ),
            CancelReasonOutcome::Uncertain,
        );
        assert_eq!(
            cancel_delete_response_outcome(
                &serde_json::json!({"canceled": [], "not_canceled": {}}),
                oid,
            ),
            CancelReasonOutcome::Uncertain,
            "HTTP success that omits the OID is not terminal",
        );
        assert_eq!(
            cancel_delete_response_outcome(
                &serde_json::json!({
                    "canceled": [oid],
                    "not_canceled": {"0xabc": "pending/delayed"}
                }),
                oid,
            ),
            CancelReasonOutcome::Uncertain,
            "contradictory per-order outcomes stay orphaned",
        );
    }

    /// The cancel-raced-ahead-of-placement reason must defer to reconcile,
    /// NOT drop the order. Before this branch existed the reason fell into
    /// the Cancelled fallback and abandoned a still-live order on the book
    /// (live.log 2026-06-24: 9 forgotten orders riding to settlement).
    #[test]
    fn cancel_not_canceled_outcome_pending_delayed_defers_to_reconcile() {
        assert_eq!(
            cancel_not_canceled_outcome("can't be canceled because it is pending/delayed"),
            CancelReasonOutcome::Uncertain,
        );
        // Case-insensitive + wording variants.
        assert_eq!(
            cancel_not_canceled_outcome("order is DELAYED, cannot cancel"),
            CancelReasonOutcome::Uncertain,
        );
        assert_eq!(
            cancel_not_canceled_outcome("order still processing"),
            CancelReasonOutcome::Uncertain,
        );
        // Must not shadow the definite paths: a "matched" reason that also
        // happens to mention pending stays Filled (matched wins).
        assert_eq!(
            cancel_not_canceled_outcome("matched orders can't be canceled"),
            CancelReasonOutcome::Filled,
        );
    }

    /// `is_pending_delayed_reason` flags the cancel/placement race so the
    /// reconcile not-found arm treats the orphan as Uncertain (keeps retrying)
    /// rather than committing Cancelled — never for genuinely-gone / matched.
    #[test]
    fn is_pending_delayed_reason_flags_race_only() {
        assert!(is_pending_delayed_reason(
            "can't be canceled because it is pending/delayed"
        ));
        assert!(is_pending_delayed_reason("order is DELAYED, cannot cancel")); // case-insensitive
        assert!(is_pending_delayed_reason("order still processing"));
        assert!(!is_pending_delayed_reason(
            "order can't be found - already canceled or matched"
        ));
        assert!(!is_pending_delayed_reason(
            "matched orders can't be canceled"
        ));
        assert!(!is_pending_delayed_reason("the order is already canceled"));
        assert!(!is_pending_delayed_reason(""));
        // Consistent with the classifier: pending/delayed → Uncertain.
        for r in ["pending/delayed", "DELAYED", "processing"] {
            assert_eq!(
                cancel_not_canceled_outcome(r),
                CancelReasonOutcome::Uncertain
            );
        }
    }

    /// `is_unknown_state` must classify HTTP 425 as unknown_state so the
    /// cancel-reply path routes through CancelOrderTimeout + sets the
    /// reconcile backoff, instead of falling through to "definite reject".
    /// Regression guard for Bug #3.
    #[test]
    fn http_425_classified_as_unknown_state() {
        assert!(
            HttpErr::Status(500, "DeadlineExceeded".to_string()).is_submit_unknown_state(),
            "500 DeadlineExceeded must create a placement orphan"
        );
        assert!(
            HttpErr::Status(425, "Too Early".to_string()).is_unknown_state(),
            "425 must route through unknown_state so the cancel path treats it as transient"
        );
        assert!(
            HttpErr::Status(425, "service not ready".to_string()).is_submit_unknown_state(),
            "service-not-ready must create a placement orphan"
        );
        assert!(
            HttpErr::Status(503, "Service Unavailable".to_string()).is_unknown_state(),
            "5xx must route through unknown_state (server-side failure, state unknown)"
        );
        assert!(
            HttpErr::Timeout.is_unknown_state(),
            "timeout must route through unknown_state (server never responded)"
        );
        assert!(
            !HttpErr::Status(400, "bad request".to_string()).is_unknown_state(),
            "4xx (non-425) remains a known HTTP result for cancel/GET classification"
        );
        assert!(
            !HttpErr::Status(404, "not found".to_string()).is_unknown_state(),
            "404 is a definitive answer — must NOT be unknown_state"
        );
        assert!(
            HttpErr::Status(404, "not found".to_string()).is_explicit_not_found(),
            "among HTTP errors, only an explicit 404 is a not-found result",
        );
        assert!(
            !HttpErr::Status(425, "Too Early".to_string()).is_explicit_not_found(),
            "a reconcile 425 is unavailable evidence, not not-found",
        );
        assert!(
            !HttpErr::Status(503, "Service Unavailable".to_string()).is_explicit_not_found(),
            "a reconcile 5xx is unavailable evidence, not not-found",
        );
    }

    #[test]
    fn successful_order_lookup_requires_documented_status_envelope() {
        for json in [
            serde_json::Value::Null,
            serde_json::json!({}),
            serde_json::json!([]),
            serde_json::json!({"unexpected": "non-empty malformed response"}),
            serde_json::json!({"error": "upstream unavailable"}),
            serde_json::json!({"error": "market not found"}),
        ] {
            assert!(
                matches!(
                    classify_successful_order_lookup(&json, "0xabc"),
                    FetchOrderResult::Unavailable(FetchUnavailable::InvalidResponse(_))
                ),
                "json={json}",
            );
        }

        for json in [
            serde_json::json!({"error": "Order not found"}),
            serde_json::json!({"errorMsg": "Order not found: 0xabc"}),
            serde_json::json!({"detail": "Could not find order"}),
        ] {
            match classify_successful_order_lookup(&json, "0xabc") {
                FetchOrderResult::NotFound(evidence) => {
                    assert!(evidence.starts_with("http_2xx_error_envelope="));
                    assert!(evidence.contains("not found") || evidence.contains("find order"));
                }
                other => panic!("expected authoritative not-found for {json}, got {other:?}"),
            }
        }

        assert!(matches!(
            classify_successful_order_lookup(
                &serde_json::json!({
                    "id": "0xabc",
                    "status": "FUTURE_STATUS",
                    "original_size": "10",
                    "size_matched": "0",
                    "associate_trades": [],
                }),
                "0xabc",
            ),
            FetchOrderResult::Unavailable(FetchUnavailable::InvalidResponse(_))
        ));

        let null = classify_successful_order_lookup(&serde_json::Value::Null, "0xabc");
        assert!(matches!(
            null,
            FetchOrderResult::Unavailable(ref kind) if kind.is_json_null()
        ));
        assert!(matches!(
            classify_successful_order_lookup(&serde_json::json!({}), "0xabc"),
            FetchOrderResult::Unavailable(ref kind) if !kind.is_json_null()
        ));
        let null = FetchUnavailable::InvalidResponse("null".to_string());
        assert_eq!(
            recovered_order_close_reason(true, false, Some(&null)),
            Some(RecoveredOrderCloseReason::JsonNull),
        );
        assert_eq!(
            recovered_order_close_reason(true, true, None),
            Some(RecoveredOrderCloseReason::EventEnded),
        );
        assert_eq!(recovered_order_close_reason(false, true, Some(&null)), None);
        assert_eq!(
            recovered_order_close_reason(
                true,
                false,
                Some(&FetchUnavailable::InvalidResponse("{}".to_string())),
            ),
            None,
        );

        assert!(matches!(
            classify_successful_order_lookup(&serde_json::json!({"status": "LIVE"}), "0xabc"),
            FetchOrderResult::Unavailable(FetchUnavailable::InvalidResponse(_))
        ));
        assert!(matches!(
            classify_successful_order_lookup(
                &serde_json::json!({
                    "id": "0xabc",
                    "status": "LIVE",
                    "original_size": "10",
                    "size_matched": "0",
                    "associate_trades": [],
                }),
                "0xabc",
            ),
            FetchOrderResult::Found(FetchedOrder { status, .. }) if status == "LIVE"
        ));
        assert!(matches!(
            classify_successful_order_lookup(
                &serde_json::json!({
                    "orderID": "0xabc",
                    "status": "ORDER_STATUS_MATCHED",
                    "original_size": "10",
                    "size_matched": "10",
                    "associate_trades": ["trade-1"],
                }),
                "0xabc",
            ),
            FetchOrderResult::Found(FetchedOrder { status, .. }) if status == "MATCHED"
        ));
        for json in [
            serde_json::json!({
                "id": "different",
                "status": "LIVE",
                "original_size": "10",
                "size_matched": "0",
                "associate_trades": [],
            }),
            serde_json::json!({
                "id": "0xabc",
                "status": "LIVE",
                "original_size": "10",
                "size_matched": "11",
                "associate_trades": [],
            }),
            serde_json::json!({
                "id": "0xabc",
                "status": "LIVE",
                "original_size": "10",
                "size_matched": "0",
                "associate_trades": "trade-1",
            }),
            serde_json::json!({
                "id": "0xabc",
                "status": "LIVE",
                "original_size": "10",
                "size_matched": "0",
                "associate_trades": [],
                "error": "upstream envelope",
            }),
        ] {
            assert!(matches!(
                classify_successful_order_lookup(&json, "0xabc"),
                FetchOrderResult::Unavailable(FetchUnavailable::InvalidResponse(_))
            ));
        }
    }

    /// Every placement-orphan provenance uses the same four-result terminal
    /// rule. Clearing the counter models any intervening non-not-found lookup
    /// and must restart the consecutive run.
    #[test]
    fn all_placement_orphans_require_four_consecutive_not_found_results() {
        assert_eq!(RECONCILE_NOT_FOUND_RETRY_LIMIT, 4);

        for coid in [
            "timeout",
            "deadline-exceeded",
            "service-not-ready",
            "transport",
        ] {
            let attempts = ReconcileAttemptCounters::default();
            assert_eq!(attempts.next_placement(coid), 1);
            assert_eq!(attempts.next_placement(coid), 2);
            attempts.clear_placement(coid);
            assert_eq!(attempts.next_placement(coid), 1);
            assert_eq!(attempts.next_placement(coid), 2);
            assert_eq!(attempts.next_placement(coid), 3);
            assert_eq!(attempts.next_placement(coid), 4);
        }
    }

    #[test]
    fn shutdown_absence_only_terminalizes_placement_phantoms() {
        assert!(!shutdown_absent_placement_phantom_is_terminal(
            OrderStatus::NewOrderTimeout,
            RECONCILE_NOT_FOUND_RETRY_LIMIT - 1,
        ));
        assert!(shutdown_absent_placement_phantom_is_terminal(
            OrderStatus::NewOrderTimeout,
            RECONCILE_NOT_FOUND_RETRY_LIMIT,
        ));
        for status in [
            OrderStatus::Accepted,
            OrderStatus::PartiallyFilled,
            OrderStatus::CancelOrderTimeout,
            OrderStatus::CancelUncertain,
        ] {
            assert!(!shutdown_absent_placement_phantom_is_terminal(
                status,
                RECONCILE_NOT_FOUND_RETRY_LIMIT,
            ));
        }
    }

    /// A status-less reqwest send failure is ambiguous only for placement
    /// handlers. This guards against both the original false rejection and
    /// accidentally broadening cancel/GET behavior by changing the shared
    /// classifier.
    #[test]
    fn transport_error_is_unknown_for_submit_only() {
        let transport = HttpErr::Transport(
            "error sending request for url (https://clob.polymarket.com/order)".to_string(),
        );
        assert!(
            transport.is_submit_unknown_state(),
            "a placement POST may have landed before its transport failed"
        );
        assert!(
            !transport.is_unknown_state(),
            "the shared cancel/GET classifier must retain its existing behavior"
        );
        let invalid_response = HttpErr::InvalidResponse("json parse: invalid response".to_string());
        assert!(
            invalid_response.is_submit_unknown_state(),
            "a malformed HTTP-success body cannot prove placement rejection"
        );
        assert!(
            !invalid_response.is_unknown_state(),
            "invalid response remains placement-specific so cancel/GET semantics are unchanged"
        );
        assert!(
            !HttpErr::Other("local request construction failed".to_string())
                .is_submit_unknown_state(),
            "pre-dispatch local failures remain definitive"
        );
        assert!(
            !HttpErr::Status(400, "bad request".to_string()).is_submit_unknown_state(),
            "HTTP 400 has a definitive response"
        );
        assert!(HttpErr::Status(400, "bad request".to_string()).is_definitive_submit_rejection());
        assert!(!HttpErr::Status(425, "too early".to_string()).is_definitive_submit_rejection());
        assert!(!HttpErr::Status(503, "unavailable".to_string()).is_definitive_submit_rejection());
        let trading_disabled =
            HttpErr::Status(503, r#"{"error":"trading is disabled"}"#.to_string());
        assert!(
            !trading_disabled.is_submit_unknown_state(),
            "an explicit policy rejection must not create an orphan",
        );
        assert!(trading_disabled.is_definitive_submit_rejection());
        assert!(!HttpErr::Other("local validation".to_string()).is_definitive_submit_rejection());
    }

    /// Four not-found observations use 0.5/1/2 second gaps and reach the
    /// terminal Rejected decision after about 3.5 seconds.
    #[test]
    fn reconcile_not_found_backoff_schedule_spans_three_and_a_half_seconds() {
        let gaps_ms: Vec<u64> = (1..RECONCILE_NOT_FOUND_RETRY_LIMIT)
            .map(placement_reconcile_backoff_ms)
            .collect();

        assert_eq!(gaps_ms, vec![500, 1_000, 2_000]);
        assert_eq!(gaps_ms.iter().sum::<u64>(), 3_500);
    }

    #[test]
    fn cancel_reconcile_backoff_is_capped_and_deterministically_jittered() {
        let gaps: Vec<u64> = (1..=6)
            .map(|attempt| cancel_reconcile_backoff_ms("btc03-order", attempt))
            .collect();
        assert!((500..=750).contains(&gaps[0]));
        assert!((1_000..=1_250).contains(&gaps[1]));
        assert!((2_000..=2_250).contains(&gaps[2]));
        assert!(gaps[3..].iter().all(|gap| (4_000..=4_250).contains(gap)));
        assert_eq!(
            gaps,
            (1..=6)
                .map(|attempt| cancel_reconcile_backoff_ms("btc03-order", attempt))
                .collect::<Vec<_>>(),
        );
    }

    #[test]
    fn reconcile_unavailable_classification_separates_transport_from_http() {
        assert_eq!(
            HttpErr::Timeout.fetch_unavailable(),
            FetchUnavailable::Timeout,
        );
        assert_eq!(
            HttpErr::Transport("reset".to_string()).fetch_unavailable(),
            FetchUnavailable::Transport,
        );
        assert_eq!(
            HttpErr::Status(503, "unavailable".to_string()).fetch_unavailable(),
            FetchUnavailable::Http(503),
        );
        assert_eq!(
            HttpErr::Other("json parse".to_string()).fetch_unavailable(),
            FetchUnavailable::InvalidResponse("json parse".to_string()),
        );
    }

    /// A cancel-before-ack timeout can put one coid in both reconcile lists.
    /// Unbounded cancel polls must not consume placement's four observations.
    #[test]
    fn placement_and_cancel_reconcile_attempts_are_isolated() {
        let attempts = ReconcileAttemptCounters::default();
        let coid = "mixed-place-cancel";

        assert_eq!(attempts.next_placement(coid), 1);
        for expected in 1..=8 {
            assert_eq!(attempts.next_cancel(coid), expected);
        }
        assert_eq!(attempts.next_placement(coid), 2);

        attempts.clear_cancel(coid);
        assert_eq!(attempts.next_cancel(coid), 1);
        assert_eq!(attempts.next_placement(coid), 3);

        attempts.clear_placement(coid);
        assert_eq!(attempts.next_placement(coid), 1);
        assert_eq!(attempts.next_cancel(coid), 2);
    }

    /// A 425 must back off only the affected orphan. A healthy sibling must
    /// remain eligible for reconcile, otherwise one throttled order becomes
    /// an account-wide circuit breaker. Deadlines also remain monotonic per
    /// coid and expired entries are removed lazily.
    #[test]
    fn http_425_backoff_is_per_coid_and_monotonic() {
        let mut backoffs = HashMap::new();
        let now: u64 = 1_000_000_000_000; // arbitrary wall-clock proxy

        record_http_425_backoff(&mut backoffs, "throttled", now);
        let d1 = now.saturating_add(HTTP_425_BACKOFF_NS);
        assert_eq!(backoffs.get("throttled"), Some(&d1));
        assert!(is_http_425_backoff_active(&mut backoffs, "throttled", now));
        assert!(
            !is_http_425_backoff_active(&mut backoffs, "healthy-sibling", now),
            "a 425 on one coid must not block unrelated orphan audits",
        );

        // Operator-style bump: extend to +60 s for a sustained storm.
        let bumped = now.saturating_add(60_000_000_000);
        backoffs.insert("throttled".to_string(), bumped);
        assert!(bumped > d1);

        // Another 425 cannot pull that coid's deadline backwards.
        record_http_425_backoff(&mut backoffs, "throttled", now);
        assert_eq!(backoffs.get("throttled"), Some(&bumped));

        // Expiry is local and removes only the expired coid's entry.
        assert!(!is_http_425_backoff_active(
            &mut backoffs,
            "throttled",
            bumped,
        ));
        assert!(!backoffs.contains_key("throttled"));
    }

    /// `HTTP_425_BACKOFF_NS` breaks the immediate reconcile loop but remains
    /// short enough to enter the normal four-not-found proof promptly.
    #[test]
    fn http_425_backoff_constant_is_in_sane_range() {
        assert_eq!(
            HTTP_425_BACKOFF_NS, 1_000_000_000,
            "425/service-not-ready must use the selected one-second per-coid backoff",
        );
    }

    #[test]
    fn emergency_cancel_selector_excludes_healthy_sibling_orders() {
        let open = HashMap::from([
            (
                "a-2".to_string(),
                TrackedOrder {
                    order_slot: Default::default(),
                    symbol: "TOK".into(),
                    side: Side::Buy,
                    instance_id: "a".into(),
                },
            ),
            (
                "b-1".to_string(),
                TrackedOrder {
                    order_slot: Default::default(),
                    symbol: "TOK".into(),
                    side: Side::Sell,
                    instance_id: "b".into(),
                },
            ),
            (
                "a-1".to_string(),
                TrackedOrder {
                    order_slot: Default::default(),
                    symbol: "OTHER".into(),
                    side: Side::Sell,
                    instance_id: "a".into(),
                },
            ),
        ]);
        assert_eq!(
            instance_owned_open_coids(open.iter(), "a"),
            vec!["a-1".to_string(), "a-2".to_string()],
        );
        assert_eq!(
            instance_owned_open_coids(open.iter(), "b"),
            vec!["b-1".to_string()],
        );
    }
}
