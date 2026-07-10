use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

/// Interval between keepalive pings. Polymarket's CLOB market/user
/// channel requires the client to send a text `"PING"` every 10 s
/// (server replies `"PONG"`); it drops connections that go ~10 s
/// without one. See clob_ws_task for the send site.
const PING_INTERVAL: Duration = Duration::from_secs(10);

use crate::exchange::ExchangeMarket;
use crate::types::*;

const POLYMARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const POLYMARKET_RTDS_URL: &str = "wss://ws-live-data.polymarket.com";

const RTDS_PING_INTERVAL: Duration = Duration::from_secs(4);
const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";

/// Per-task read-side stall watchdogs. CLOB book diffs + trade ticks
/// arrive frequently during active markets but go quiet when there's
/// no currently-trading event — `has_active_subscription()` already
/// suppresses the engine-side data-timeout in that case, but the
/// in-task watchdog has no visibility into that. Use a generous 90 s
/// to cover quiet periods between events without false-tripping.
/// RTDS streams (spot prices) push ~10 Hz when subscribed; 30 s of
/// silence is plenty anomalous.
const CLOB_STALE_THRESHOLD: Duration = Duration::from_secs(90);
const RTDS_STALE_THRESHOLD: Duration = Duration::from_secs(30);

// ── Polymarket Event Types ─────────────────────────────────────────

/// Deserialize a field that is either a JSON array or a stringified JSON array.
/// Handles: `["a","b"]`, `"[\"a\",\"b\"]"`, and `null` / missing.
fn deserialize_json_string_array<'de, D>(deserializer: D) -> std::result::Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(s) => {
            serde_json::from_str(&s).map_err(de::Error::custom)
        }
        serde_json::Value::Array(arr) => arr
            .into_iter()
            .map(|v| match v {
                serde_json::Value::String(s) => Ok(s),
                other => Ok(other.to_string()),
            })
            .collect(),
        serde_json::Value::Null => Ok(Vec::new()),
        _ => Err(de::Error::custom("expected string or array")),
    }
}

/// Deserialize a field that may be a string or number into f64.
/// Handles: `0.01`, `"0.01"`, and `null` / missing → 0.0.
// fn deserialize_string_f64<'de, D>(deserializer: D) -> std::result::Result<f64, D::Error>
// where
//     D: serde::Deserializer<'de>,
// {
//     let value = serde_json::Value::deserialize(deserializer)?;
//     match value {
//         serde_json::Value::Number(n) => Ok(n.as_f64().unwrap_or(0.0)),
//         serde_json::Value::String(s) => s.parse::<f64>().map_err(serde::de::Error::custom),
//         serde_json::Value::Null => Ok(0.0),
//         _ => Err(serde::de::Error::custom("expected number or string")),
//     }
// }

/// A single market within a Polymarket event.
/// Each market has a question (e.g. "Will BTC go up?") and 2+ outcomes (e.g. Yes/No, Up/Down).
/// Each outcome has a CLOB token ID for trading on the orderbook.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolyMarketInfo {
    pub id: String,
    pub question: String,
    #[serde(default, rename = "conditionId")]
    pub condition_id: String,
    #[serde(default)]
    pub slug: String,
    /// CLOB token IDs for each outcome (e.g. [YES_TOKEN_ID, NO_TOKEN_ID])
    #[serde(default, deserialize_with = "deserialize_json_string_array", rename = "clobTokenIds")]
    pub clob_token_ids: Vec<String>,
    /// Outcome labels — stringified JSON array in the API: `"[\"Yes\",\"No\"]"`
    #[serde(default, deserialize_with = "deserialize_json_string_array")]
    pub outcomes: Vec<String>,
    /// Outcome prices — stringified JSON array in the API: `"[\"0.65\",\"0.35\"]"`
    #[serde(default, deserialize_with = "deserialize_json_string_array", rename = "outcomePrices")]
    pub outcome_prices: Vec<String>,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub closed: bool,
    #[serde(default, rename = "volumeNum")]
    pub volume: f64,
    #[serde(default, rename = "liquidityNum")]
    pub liquidity: f64,
    #[serde(default, rename = "orderPriceMinTickSize")]
    pub tick_size: f64,
    #[serde(default, rename = "orderMinSize")]
    pub order_min_size: f64,
    /// Group item title (e.g. "Anthropic", "OpenAI") for categorical markets
    #[serde(default, rename = "groupItemTitle")]
    pub group_item_title: String,
    /// Event start time (ISO 8601 string from API, e.g. "2026-03-29T06:10:00Z").
    #[serde(default, rename = "eventStartTime")]
    pub event_start_time: String,
    /// Taker base fee in basis points, from the event API.
    #[serde(default, rename = "takerBaseFee")]
    pub base_fee: u32,
    /// Fee schedule from the event API's `feeSchedule` object.
    /// Provides `exponent` and `rate`.
    #[serde(default, rename = "feeSchedule")]
    pub fee_schedule: FeeSchedule,
}

/// Polymarket fee curve config, nested under each market as `feeSchedule`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FeeSchedule {
    #[serde(default)]
    pub exponent: f64,
    #[serde(default)]
    pub rate: f64,
    #[serde(default, rename = "takerOnly")]
    pub taker_only: bool,
    #[serde(default, rename = "rebateRate")]
    pub rebate_rate: f64,
}

impl From<PolyMarketInfo> for crate::types::BinaryOption {
    fn from(m: PolyMarketInfo) -> Self {
        Self {
            exchange: crate::types::Exchange::Polymarket,
            id: m.id,
            question: m.question,
            condition_id: m.condition_id,
            slug: m.slug,
            clob_token_ids: m.clob_token_ids,
            outcomes: m.outcomes,
            outcome_prices: m.outcome_prices,
            active: m.active,
            closed: m.closed,
            volume: m.volume,
            liquidity: m.liquidity,
            tick_size: m.tick_size,
            order_min_size: m.order_min_size,
            group_item_title: m.group_item_title,
            event_start_time: m.event_start_time,
            base_fee: m.base_fee,
            fee_exponent: m.fee_schedule.exponent,
            fee_rate: m.fee_schedule.rate,
        }
    }
}

/// A Polymarket event containing one or more markets.
/// Structure: Event → Market(s) → Outcome(s)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymarketEvent {
    pub id: String,
    pub slug: String,
    pub title: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub closed: bool,
    #[serde(default, rename = "endDate")]
    pub end_date: String,
    /// Markets within this event, each with its own outcomes
    #[serde(default)]
    pub markets: Vec<PolyMarketInfo>,
}

impl PolymarketEvent {
    /// Collect all CLOB token IDs from all markets' outcomes
    pub fn all_token_ids(&self) -> Vec<String> {
        self.markets
            .iter()
            .flat_map(|m| m.clob_token_ids.clone())
            .collect()
    }

    /// Collect CLOB token IDs only from active (non-closed) markets
    pub fn active_token_ids(&self) -> Vec<String> {
        self.markets
            .iter()
            .filter(|m| m.active && !m.closed)
            .flat_map(|m| m.clob_token_ids.clone())
            .collect()
    }
}

/// Parse an ISO 8601 date string to nanoseconds since epoch.
fn parse_date_ns(date_str: &str) -> Result<u64> {
    let dt = chrono::DateTime::parse_from_rfc3339(date_str)
        .or_else(|_| {
            // Try without fractional seconds: "2026-02-13T12:15:00Z"
            chrono::NaiveDateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%SZ")
                .map(|ndt| ndt.and_utc().fixed_offset())
        })
        .map_err(|e| anyhow!("Failed to parse date '{}': {}", date_str, e))?;
    Ok(dt.timestamp_nanos_opt().unwrap_or(0) as u64)
}

/// Fetch a Polymarket event by its slug (e.g. "which-company-has-the-best-ai-model-end-of-march-751").
pub fn fetch_event_by_slug(slug: &str) -> Result<PolymarketEvent> {
    fetch_event_by_slug_with_log(slug, true)
}

/// Same as `fetch_event_by_slug` but with optional logging. CLI tools that
/// don't want noisy URL/response dumps can pass `log=false`.
pub fn fetch_event_by_slug_with_log(slug: &str, log: bool) -> Result<PolymarketEvent> {
    let url = format!("{}/events?slug={}", GAMMA_API_BASE, slug);
    if log { info!("[Polymarket] Fetching event by slug: {}", url); }

    // 5 attempts × exponential backoff (200 ms base) ≈ 6 s ceiling —
    // covers brief gamma-api 5xx blips during event rotation without
    // permanently stalling the subscribe / maintenance path.
    let resp_text = crate::async_rt::blocking_get_text_retry(&url, 5, 200)?;
    if log {
        info!("[Polymarket] Gamma API response (first 500 chars): {}", &resp_text[..resp_text.len().min(500)]);
    }

    let events: Vec<PolymarketEvent> = serde_json::from_str(&resp_text)
        .map_err(|e| anyhow!("Failed to parse Gamma API response: {}", e))?;

    let event = events.into_iter()
        .next()
        .ok_or_else(|| anyhow!("No Polymarket event found for slug: {}", slug))?;

    if log {
        info!(
            "[Polymarket] Found event: '{}' (id={}, {} markets)",
            event.title, event.id, event.markets.len()
        );
    }
    Ok(event)
}

/// Check if a symbol is an event-series slug (prefix "series:").
fn is_event_series(symbol: &str) -> bool {
    symbol.starts_with("series:")
}

/// A Polymarket series (group of related events).
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct PolymarketSeries {
    id: String,
    slug: String,
    #[serde(default)]
    title: String,
}

/// Step 1: Fetch the series ID by slug.
/// Uses GET /series?slug=xxx&exclude_events=true.
///
/// `closed` is intentionally NOT in the query — gamma-api can briefly
/// mark a series `closed` between event-rotation cycles even though the
/// series itself stays alive. Adding the filter would cause a false
/// "series not found" failure during those transitions; matching by
/// slug alone is enough.
fn fetch_series_id(series_slug: &str) -> Result<String> {
    let url = format!(
        "{}/series?slug={}&exclude_events=true",
        GAMMA_API_BASE, series_slug
    );
    info!("[Polymarket] Fetching series by slug: {}", url);

    // 5 attempts × exponential backoff (200 ms base) ≈ 6 s ceiling —
    // covers brief gamma-api 5xx blips during event rotation without
    // permanently stalling the subscribe / maintenance path.
    let resp_text = crate::async_rt::blocking_get_text_retry(&url, 5, 200)?;
    let series_list: Vec<PolymarketSeries> = serde_json::from_str(&resp_text)
        .map_err(|e| anyhow!("Failed to parse series response: {}", e))?;

    let series = series_list.into_iter()
        .next()
        .ok_or_else(|| anyhow!("No series found for slug: {}", series_slug))?;

    info!("[Polymarket] Series '{}' (id={})", series.title, series.id);
    Ok(series.id)
}

/// Step 2: Fetch the soonest-to-end event in a series whose `end_date`
/// is ≥ now+1s. This is the "currently trading" event for cycle-based
/// series like `btc-up-or-down-5m`.
///
/// Uses `GET /events?series_id=...&end_date_min=<now+1s>&ascending=true&limit=1`.
/// The +1s skew makes sure the just-expired event isn't picked up during
/// the rotation second (event end_date == now → boundary case).
///
/// `closed` is intentionally NOT in the query — gamma-api occasionally
/// flips a freshly-rotated event's `closed` flag to `true` for a few
/// seconds before the next event is published, which would otherwise
/// surface as a spurious "no live event" warning. The
/// `end_date_min ≥ now+1s` filter alone is enough to exclude expired
/// events; the strategy's own settle-and-detach machinery handles
/// closed-flag transitions.
fn fetch_active_events_by_series_id(series_id: &str) -> Result<Vec<PolymarketEvent>> {
    let now_secs = chrono::Utc::now().timestamp() as u64;
    let end_min_iso = chrono::DateTime::<chrono::Utc>::from_timestamp((now_secs + 1) as i64, 0)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_default();
    let url = format!(
        "{}/events?series_id={}&end_date_min={}&ascending=true&limit=1",
        GAMMA_API_BASE, series_id, end_min_iso,
    );
    info!("[Polymarket] Fetching active events for series_id={}: {}", series_id, url);

    // 5 attempts × exponential backoff (200 ms base) ≈ 6 s ceiling —
    // covers brief gamma-api 5xx blips during event rotation without
    // permanently stalling the subscribe / maintenance path.
    let resp_text = crate::async_rt::blocking_get_text_retry(&url, 5, 200)?;
    let events: Vec<PolymarketEvent> = serde_json::from_str(&resp_text)
        .map_err(|e| anyhow!("Failed to parse events response: {}", e))?;

    info!("[Polymarket] Found {} active events in series", events.len());
    Ok(events)
}

/// Resolve an event's opening time (ns since epoch). Prefers the API's
/// per-market `eventStartTime`; falls back to the trailing unix-second
/// timestamp embedded in the slug (e.g. `btc-updown-5m-1781728200`).
/// Returns `None` when neither is present/parseable — callers treat an
/// unknown open time as "already open" so series whose events carry no
/// start timestamp (e.g. categorical markets) keep the old behaviour.
fn event_open_ns(event: &PolymarketEvent) -> Option<u64> {
    if let Some(m) = event.markets.first() {
        if !m.event_start_time.is_empty() {
            if let Ok(ns) = parse_date_ns(&m.event_start_time) {
                if ns > 0 { return Some(ns); }
            }
        }
    }
    // Fallback: trailing unix-second timestamp in the slug.
    if let Some(last_dash) = event.slug.rfind('-') {
        if let Ok(secs) = event.slug[last_dash + 1..].parse::<u64>() {
            if secs > 1_700_000_000 {
                return Some(secs.saturating_mul(1_000_000_000));
            }
        }
    }
    None
}

/// Pick the currently trading event from a list of events.
///
/// "Currently trading" = the event is **already open** (`start ≤ now`) and
/// not yet expired (`end > now`), choosing the soonest-to-expire among
/// those. An event whose open time is unknown is treated as open, so
/// series without a start timestamp keep the previous end-only behaviour.
///
/// When no event is open yet but one is scheduled to open soon (a series
/// "gap" — the next cycle's market hasn't started), we log a WARN and
/// return that upcoming event so the subscribe/rotation path still has a
/// handle; the strike-fetch layer defers its Chainlink read until the
/// event actually opens (`event_start_ns ≤ now`). This avoids hammering
/// the Data Streams REST for a not-yet-existent opening-price report
/// (the "No 'report' in response" spin).
fn pick_current_event(events: Vec<PolymarketEvent>, series_slug: &str) -> Result<PolymarketEvent> {
    let now = chrono::Utc::now();
    let now_ns = now.timestamp_nanos_opt().unwrap_or(0) as u64;

    let parse_end = |s: &str| -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::parse_from_rfc3339(s)
            .or_else(|_| {
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%SZ")
                    .map(|ndt| ndt.and_utc().fixed_offset())
            })
            .ok()
            .map(|dt| dt.with_timezone(&chrono::Utc))
    };

    // Among un-expired events, prefer the soonest-to-expire that is
    // already open; separately track the soonest-to-open upcoming event
    // as a fallback for the series-gap case.
    let mut open: Option<PolymarketEvent> = None;
    let mut open_end = chrono::DateTime::<chrono::Utc>::MAX_UTC;
    let mut upcoming: Option<PolymarketEvent> = None;
    let mut upcoming_start_ns = u64::MAX;

    for event in events {
        if event.end_date.is_empty() { continue; }
        let Some(end_dt) = parse_end(&event.end_date) else { continue };
        if end_dt <= now { continue; } // already expired

        let start_ns = event_open_ns(&event).unwrap_or(0);
        let is_open = start_ns == 0 || start_ns <= now_ns;
        if is_open {
            if end_dt < open_end {
                open_end = end_dt;
                open = Some(event);
            }
        } else if start_ns < upcoming_start_ns {
            upcoming_start_ns = start_ns;
            upcoming = Some(event);
        }
    }

    if let Some(event) = open {
        info!("[Polymarket] Current event: '{}' (ends {})", event.title, event.end_date);
        return Ok(event);
    }
    if let Some(event) = upcoming {
        let wait_s = upcoming_start_ns.saturating_sub(now_ns) / 1_000_000_000;
        warn!(
            "[Polymarket] No event currently open in series '{}'; nearest upcoming '{}' (ends {}) opens in {}s — \
             treating as pending, strike fetch deferred until it opens",
            series_slug, event.title, event.end_date, wait_s,
        );
        return Ok(event);
    }
    Err(anyhow!("No currently trading event in series '{}'", series_slug))
}

/// Fetch the currently trading event for a series slug (first call — resolves series_id).
/// Returns (series_id, event).
fn fetch_active_event_with_series_id(series_slug: &str) -> Result<(String, PolymarketEvent)> {
    fetch_active_event_with_series_id_pub(series_slug)
}

/// Public entry point for CLI tools / external callers that need the
/// currently trading event of a series slug. Same logic as the
/// private helper above; kept under a distinct name so we don't have
/// to touch existing private callers.
pub fn fetch_active_event(series_slug: &str) -> Result<(String, PolymarketEvent)> {
    fetch_active_event_with_series_id_pub(series_slug)
}

fn fetch_active_event_with_series_id_pub(series_slug: &str) -> Result<(String, PolymarketEvent)> {
    let series_id = fetch_series_id(series_slug)?;
    let events = fetch_active_events_by_series_id(&series_id)?;
    let event = pick_current_event(events, series_slug)?;
    Ok((series_id, event))
}

/// Resolve a series slug to its numeric series_id. Public wrapper so other
/// modules (e.g. strategy-side maintenance) can cache the id and avoid
/// re-resolving on every call.
pub fn resolve_series_id(series_slug: &str) -> Result<String> {
    fetch_series_id(series_slug)
}

/// Parse the event-cycle duration (seconds) embedded in a Polymarket
/// series / event slug. Scans `-`-separated parts and returns the first
/// one matching `<N>m` (minutes) or `<N>h` (hours).
///
/// Examples:
///   - "btc-up-or-down-5m"          → Some(300)
///   - "btc-updown-5m-1776521700"   → Some(300)
///   - "eth-updown-1h"              → Some(3600)
///   - "xyz-daily-forecast"         → None
pub fn parse_slug_duration_secs(slug: &str) -> Option<u64> {
    for part in slug.split('-') {
        if let Some(mins) = part.strip_suffix('m') {
            if let Ok(n) = mins.parse::<u64>() {
                return Some(n * 60);
            }
        }
        if let Some(hours) = part.strip_suffix('h') {
            if let Ok(n) = hours.parse::<u64>() {
                return Some(n * 3600);
            }
        }
    }
    None
}

/// Fetch the earliest event in `series_id` whose `end_date` is strictly
/// greater than `end_date_min_secs` (unix seconds). Uses
/// `GET /events?series_id=...&end_date_min=...&ascending=true&limit=1`.
/// Returns the full `PolymarketEvent` so callers can log / inspect details
/// (title, id, slug, start/end times); the maintenance pipeline pulls the
/// first market's `condition_id` off it for `splitPosition`.
///
/// `closed` is intentionally NOT in the query — gamma-api occasionally
/// flips a freshly-rotated event's `closed` flag for a few seconds
/// before the next event is published, which would otherwise look like
/// "no next event" and stall the maintenance pipeline.
pub fn fetch_next_event(
    series_id: &str,
    end_date_min_secs: u64,
) -> Result<Option<PolymarketEvent>> {
    // Polymarket gamma API accepts RFC3339 / ISO8601 for `end_date_min`.
    let end_min_iso = chrono::DateTime::<chrono::Utc>::from_timestamp(end_date_min_secs as i64, 0)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_default();
    let url = format!(
        "{}/events?series_id={}&end_date_min={}&ascending=true&limit=1",
        GAMMA_API_BASE, series_id, end_min_iso,
    );
    info!("[Polymarket] Fetching next event: {}", url);

    // 5 attempts × exponential backoff (200 ms base) ≈ 6 s ceiling —
    // covers brief gamma-api 5xx blips during event rotation without
    // permanently stalling the subscribe / maintenance path.
    let resp_text = crate::async_rt::blocking_get_text_retry(&url, 5, 200)?;
    let events: Vec<PolymarketEvent> = serde_json::from_str(&resp_text)
        .map_err(|e| anyhow!("Failed to parse next-event response: {}", e))?;

    let Some(event) = events.into_iter().next() else {
        info!("[Polymarket] Next event: <none> (no event with end_date ≥ {})", end_min_iso);
        return Ok(None);
    };
    let start_time = event.markets.first()
        .map(|m| m.event_start_time.as_str())
        .unwrap_or("?");
    let cid = event.markets.first()
        .map(|m| m.condition_id.as_str())
        .unwrap_or("?");
    info!(
        "[Polymarket] Next event: title='{}' id={} slug={} start={} end={} cid={}",
        event.title, event.id, event.slug, start_time, event.end_date, cid,
    );
    Ok(Some(event))
}

/// Convenience wrapper: return just the first market's condition_id of the
/// next event (or None). Backwards-compatible with earlier callers that
/// only need the cid.
pub fn fetch_next_event_condition_id(
    series_id: &str,
    end_date_min_secs: u64,
) -> Result<Option<String>> {
    Ok(fetch_next_event(series_id, end_date_min_secs)?
        .and_then(|e| e.markets.first().map(|m| m.condition_id.clone()))
        .filter(|s| !s.is_empty()))
}

/// Fetch the currently trading event using a cached series_id (rotation calls).
fn fetch_active_event_by_series_id(series_id: &str, series_slug: &str) -> Result<PolymarketEvent> {
    let events = fetch_active_events_by_series_id(series_id)?;
    pick_current_event(events, series_slug)
}

/// Control signal sent from the sync engine thread to the async WS task.
enum WsCtrl {
    /// Subscribe (or resubscribe) with this exact set of CLOB token IDs.
    /// The async task should close its current connection and reconnect so
    /// the server's subscription matches. We do it this way (reconnect
    /// rather than incremental add/remove) because the CLOB WS's
    /// `{type: market, assets_ids: [...]}` message is a full-state
    /// subscription — the server treats a second subscribe as additive and
    /// there's no unsubscribe verb, so a fresh connection is the only
    /// portable way to drop stale tokens across a rotation.
    Resubscribe(Vec<String>),
    /// Shutdown the WS task cleanly.
    Shutdown,
}

/// A single symbol (CLOB token) within a Polymarket event/market.
struct SymbolState {
    token_id: String,
    // Outcome label, e.g. "Yes", "No", "Up", "Down"
    _outcome: String,
    // Which condition (market) within the event this token belongs to
    _condition_id: String,
}

/// A single event (market) within a series — rotates every interval.
#[allow(dead_code)]
struct MarketState {
    event_id: String,
    start_ns: u64,
    end_ns: u64,
    symbols: Vec<SymbolState>,
}

impl MarketState {
    fn token_ids(&self) -> Vec<String> {
        self.symbols.iter().map(|s| s.token_id.clone()).collect()
    }
}

/// A subscription entry. interval_minutes: 0 = static slug, -1 = event series (auto-refresh).
struct SeriesState {
    name: String,
    interval_minutes: i64,
    market: MarketState,
    /// Cached series ID from API (avoids re-fetching on every rotation).
    series_id: Option<String>,
    /// Retry timer to avoid spamming the API when next event isn't available yet.
    next_retry_ns: u64,
    /// Consecutive failed refresh attempts since the last successful rotation.
    /// Used to throttle warn-spam and enter an "idling" backoff when the
    /// upstream gamma-api keeps returning no currently-trading event.
    refresh_fail_count: u32,
    /// Wall-clock ns of the first failure in the current failure streak.
    /// Used to print the duration of the outage when we enter idling.
    refresh_fail_first_ns: u64,
    /// Whether we've already logged the idling banner for this streak.
    refresh_idling_logged: bool,
}

/// RTDS (Real-Time Data Source) subscription config.
#[derive(Debug, Clone)]
struct RtdsSubscription {
    /// "binance" or "chainlink"
    source: String,
    /// Filter symbols: e.g. ["btcusdt", "solusdt"] for binance, ["btc/usd"] for chainlink.
    filters: Vec<String>,
}

impl RtdsSubscription {
    /// Convert to Polymarket RTDS subscription message topic and type.
    fn topic_and_type(&self) -> (&str, &str) {
        match self.source.as_str() {
            "binance" => ("crypto_prices", "update"),
            "chainlink" => ("crypto_prices_chainlink", "*"),
            "pyth" | "equity" => ("equity_prices", "update"),
            _ => ("crypto_prices", "update"),
        }
    }
}

pub struct PolymarketMarket {
    series: Vec<SeriesState>,
    /// Maps CLOB token_id → index into `series`, so we can tag events with the series symbol.
    token_to_series: HashMap<String, usize>,
    pending_events: VecDeque<MarketEvent>,
    /// Events parsed by the async WS task land here; `next_event()` drains.
    event_rx: Option<crossbeam_channel::Receiver<MarketEvent>>,
    /// Control channel to the async WS task (Resubscribe / Shutdown).
    ws_ctrl_tx: Option<tokio::sync::mpsc::UnboundedSender<WsCtrl>>,
    /// Shared shutdown flag — shared between the main CLOB task and RTDS task.
    ws_shutdown: Arc<AtomicBool>,
    /// RTDS subscriptions (parsed during subscribe, spawned as task in connect).
    rtds_subscriptions: Vec<RtdsSubscription>,
    /// Sender for RTDS task to push SpotPrice events directly to engine.
    rtds_tx: Option<crossbeam_channel::Sender<MarketEvent>>,
    /// Shared shutdown flag for RTDS task.
    rtds_shutdown: Arc<AtomicBool>,
}

impl PolymarketMarket {
    pub fn new() -> Self {
        Self {
            series: Vec::new(),
            token_to_series: HashMap::new(),
            pending_events: VecDeque::new(),
            event_rx: None,
            ws_ctrl_tx: None,
            ws_shutdown: Arc::new(AtomicBool::new(false)),
            rtds_subscriptions: Vec::new(),
            rtds_tx: None,
            rtds_shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Set the engine's market_tx and shutdown flag so RTDS task can send events directly.
    pub fn set_market_tx(&mut self, tx: crossbeam_channel::Sender<MarketEvent>, shutdown: Arc<AtomicBool>) {
        self.rtds_tx = Some(tx);
        self.rtds_shutdown = shutdown;
    }

    /// Map event symbols if needed.
    /// OrderBook, Trade, and TickSizeChange all keep the clob_token_id as their symbol.
    fn map_event_symbol(&self, _event: &mut MarketEvent) {
        // All Polymarket events now use clob_token_id as symbol — no remapping needed.
    }

    /// Collect all currently-subscribed CLOB token ids across every series.
    fn current_tokens(&self) -> Vec<String> {
        self.series.iter().flat_map(|s| s.market.token_ids()).collect()
    }

    /// Send a Resubscribe message to the async WS task. No-op if the task
    /// hasn't been started yet (e.g. rotation fires before connect()).
    fn resubscribe_ws(&self) {
        if let Some(tx) = &self.ws_ctrl_tx {
            let tokens = self.current_tokens();
            let _ = tx.send(WsCtrl::Resubscribe(tokens));
        }
    }

    /// Check whether any series has reached its event end time and rotate to the next event.
    /// If any series rotated, disconnect and reconnect the WebSocket with all current tokens.
    fn check_rotation(&mut self) -> Result<()> {
        let now = now_ns();
        let mut rotated = false;

        for i in 0..self.series.len() {
            if now < self.series[i].market.end_ns {
                continue;
            }
            if self.series[i].next_retry_ns > 0 && now < self.series[i].next_retry_ns {
                continue;
            }

            // Event series mode: re-fetch active markets
            if self.series[i].interval_minutes == -1 {
                let series_slug = self.series[i].name["series:".len()..].to_string();
                let cached_id = self.series[i].series_id.clone();
                let result = match &cached_id {
                    Some(id) => fetch_active_event_by_series_id(id, &series_slug),
                    None => {
                        match fetch_active_event_with_series_id(&series_slug) {
                            Ok((id, ev)) => {
                                self.series[i].series_id = Some(id);
                                Ok(ev)
                            }
                            Err(e) => Err(e),
                        }
                    }
                };
                match result {
                    Ok(event) => {
                        info!("[Polymarket] Event series '{}' refresh: '{}'", series_slug, event.title);

                        // Remove old token mappings
                        for sym in &self.series[i].market.symbols {
                            self.token_to_series.remove(&sym.token_id);
                        }

                        // Build new token list from active markets only
                        let mut symbols_state = Vec::new();
                        for condition in &event.markets {
                            if !condition.active || condition.closed { continue; }
                            for (j, token_id) in condition.clob_token_ids.iter().enumerate() {
                                self.token_to_series.insert(token_id.clone(), i);
                                let outcome = condition.outcomes.get(j).cloned().unwrap_or_default();
                                symbols_state.push(SymbolState {
                                    token_id: token_id.clone(),
                                    _outcome: outcome,
                                    _condition_id: condition.condition_id.clone(),
                                });
                            }
                        }

                        // Queue EventStart so recorder updates file context
                        self.pending_events.push_back(MarketEvent::EventStart {
                            exchange: Exchange::Polymarket,
                            symbol: self.series[i].name.clone(),
                            event_id: event.id.clone(),
                            event_start_ns: now,
                        });

                        // Queue Instrument events for any newly active markets
                        for condition in &event.markets {
                            if !condition.active || condition.closed { continue; }
                            let mut bo: crate::types::BinaryOption = condition.clone().into();
                            bo.slug = event.slug.clone();
                            self.pending_events.push_back(MarketEvent::Instrument(
                                crate::types::Instrument::BinaryOption(bo),
                            ));
                        }

                        let end_ns = parse_date_ns(&event.end_date)
                            .unwrap_or(now + 300_000_000_000);

                        self.series[i].market = MarketState {
                            event_id: event.id,
                            start_ns: now,
                            end_ns,
                            symbols: symbols_state,
                        };
                        self.series[i].next_retry_ns = 0;
                        // Reset failure-streak counters on success.
                        if self.series[i].refresh_idling_logged {
                            let dur_s = (now.saturating_sub(self.series[i].refresh_fail_first_ns)) as f64 / 1e9;
                            info!(
                                "[Polymarket] Event series '{}' recovered after {:.0}s of idling",
                                series_slug, dur_s,
                            );
                        }
                        self.series[i].refresh_fail_count = 0;
                        self.series[i].refresh_fail_first_ns = 0;
                        self.series[i].refresh_idling_logged = false;
                        rotated = true;
                    }
                    Err(e) => {
                        // Track failure streak so we can throttle warn-spam
                        // and surface a single, clear "idling" notice when
                        // the upstream gamma-api keeps returning no event.
                        let s = &mut self.series[i];
                        if s.refresh_fail_count == 0 {
                            s.refresh_fail_first_ns = now;
                        }
                        s.refresh_fail_count = s.refresh_fail_count.saturating_add(1);

                        // For the first 4 failures keep the original 5s WARN
                        // cadence so transient blips remain visible. From the
                        // 5th failure onward, log once at WARN level
                        // ("idling") and back the retry cadence off to 30s
                        // to reduce log noise during extended outages.
                        if s.refresh_fail_count < 5 {
                            warn!(
                                "[Polymarket] Event series '{}' refresh failed: {}",
                                series_slug, e,
                            );
                            s.next_retry_ns = now + 5_000_000_000; // retry in 5s
                        } else {
                            if !s.refresh_idling_logged {
                                let dur_s = (now.saturating_sub(s.refresh_fail_first_ns)) as f64 / 1e9;
                                warn!(
                                    "[Polymarket] Series '{}' has no live event for {:.0}s, idling (last error: {})",
                                    series_slug, dur_s, e,
                                );
                                s.refresh_idling_logged = true;
                            }
                            s.next_retry_ns = now + 30_000_000_000; // retry in 30s
                        }
                    }
                }
                continue;
            }

            // Slug-based subscriptions (interval_minutes == 0) never rotate
        }

        // Resubscribe the async WS task with the updated token list if any
        // series rotated. The task will close + reconnect so the server's
        // subscription reflects the new set.
        if rotated {
            self.resubscribe_ws();
        }

        Ok(())
    }
}

// ────────────────────────────────────────────────────────────────
// Async WS tasks
// ────────────────────────────────────────────────────────────────

/// Main CLOB orderbook WS task. Runs on the shared tokio runtime.
///
/// Protocol:
///   - On start (and on each Resubscribe): (re)connect, send full
///     `{type: market, assets_ids: [...]}` subscription, then read messages
///     until a Resubscribe/Shutdown arrives or the socket fails.
///   - Parses each message into `MarketEvent`s and forwards them through
///     the sync crossbeam `event_tx` for `next_event()` to drain.
///   - Exponential backoff on connect failures; ping every PING_INTERVAL.
async fn clob_ws_task(
    initial_tokens: Vec<String>,
    event_tx: crossbeam_channel::Sender<MarketEvent>,
    mut ctrl_rx: tokio::sync::mpsc::UnboundedReceiver<WsCtrl>,
    shutdown: Arc<AtomicBool>,
) {
    let mut tokens = initial_tokens;
    let mut backoff = crate::exchange::ReconnectBackoff::new(200, 30_000);
    // Guard: if we enter with shutdown already latched true we'll exit
    // immediately below — surface it so the silent-reconnect-loop failure
    // mode that shipped on 2026-04-20 is detectable from day-1 logs.
    if shutdown.load(Ordering::Relaxed) {
        warn!("[Polymarket] CLOB task started with shutdown=true — will exit immediately (connect() forgot to reset the flag?)");
    }

    'outer: loop {
        if shutdown.load(Ordering::Relaxed) { break; }

        // Drain any buffered ctrl messages — take the latest Resubscribe so
        // we don't churn through stale token lists if rotations piled up.
        loop {
            match ctrl_rx.try_recv() {
                Ok(WsCtrl::Resubscribe(new_tokens)) => { tokens = new_tokens; }
                Ok(WsCtrl::Shutdown) => break 'outer,
                Err(_) => break,
            }
        }

        info!("[Polymarket] Connecting to {} ({} tokens)", POLYMARKET_WS_URL, tokens.len());
        let stream = match tokio_tungstenite::connect_async(POLYMARKET_WS_URL).await {
            Ok((s, _)) => s,
            Err(e) => {
                let delay = backoff.next_delay();
                warn!("[Polymarket] WS connect failed: {}, retry in {:.1}s", e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        backoff.reset();
        let (mut write, mut read) = stream.split();

        // Align with the official CLOB SDK: lowercase channel `"market"`
        // (the user channel already uses lowercase `"user"`; the server is
        // case-tolerant) plus `custom_feature_enabled`. Our frame parser
        // drops unknown messages/fields silently, so this can only add data.
        let sub_msg = serde_json::json!({
            "type": "market",
            "assets_ids": tokens,
            "custom_feature_enabled": true,
        });
        if let Err(e) = write.send(Message::Text(sub_msg.to_string())).await {
            warn!("[Polymarket] WS subscribe send failed: {}", e);
            continue;
        }
        info!("[Polymarket] Subscribed to {} tokens", tokens.len());

        let mut ping_interval = tokio::time::interval(PING_INTERVAL);
        ping_interval.tick().await; // consume immediate tick

        loop {
            tokio::select! {
                biased;

                ctrl = ctrl_rx.recv() => {
                    match ctrl {
                        Some(WsCtrl::Resubscribe(new_tokens)) => {
                            tokens = new_tokens;
                            info!("[Polymarket] Resubscribe requested ({} tokens) — reconnecting", tokens.len());
                            let _ = write.send(Message::Close(None)).await;
                            continue 'outer;
                        }
                        Some(WsCtrl::Shutdown) | None => break 'outer,
                    }
                }

                _ = ping_interval.tick() => {
                    // Polymarket CLOB expects an application-level text
                    // "PING" heartbeat (NOT a WebSocket protocol ping
                    // frame) every 10 s, else it resets the connection
                    // without a close handshake. Server replies "PONG".
                    if let Err(e) = write.send(Message::Text("PING".to_string())).await {
                        warn!("[Polymarket] Ping send failed: {}", e);
                        break;
                    }
                }

                read_result = tokio::time::timeout(CLOB_STALE_THRESHOLD, read.next()) => {
                    let msg = match read_result {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => {
                            warn!("[Polymarket] WS read error: {} — reconnecting", e);
                            break;
                        }
                        Ok(None) => {
                            warn!("[Polymarket] WS closed — reconnecting");
                            break;
                        }
                        Err(_elapsed) => {
                            warn!("[Polymarket] CLOB no message for {:.0}s (stall watchdog) — reconnecting",
                                CLOB_STALE_THRESHOLD.as_secs_f64());
                            break;
                        }
                    };
                    match msg {
                        Message::Text(text) => {
                            // Server answers our text "PING" heartbeat with
                            // "PONG" (and may echo "PING"). These aren't JSON
                            // frames — skip them so parse_clob_frame doesn't
                            // warn on every heartbeat.
                            let body = text.trim();
                            if body.eq_ignore_ascii_case("PONG")
                                || body.eq_ignore_ascii_case("PING")
                            {
                                continue;
                            }
                            let t_parse = crate::latency::Instant::now();
                            for event in parse_clob_frame(&text) {
                                if event_tx.send(event).is_err() {
                                    break 'outer; // engine gone
                                }
                            }
                            // Parse + dispatch latency for the whole
                            // CLOB WS frame (simd-json + typed deser +
                            // all contained items + crossbeam sends).
                            crate::latency::record("polymarket.ws.clob_parse", t_parse);
                        }
                        Message::Ping(payload) => {
                            let _ = write.send(Message::Pong(payload)).await;
                        }
                        Message::Close(_) => {
                            warn!("[Polymarket] Server closed WS — reconnecting");
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        // Inner loop broke → reconnect after backoff.
        if shutdown.load(Ordering::Relaxed) { break; }
        let delay = backoff.next_delay();
        tokio::time::sleep(delay).await;
    }

    info!("[Polymarket] CLOB WS task exiting");
}

/// RTDS async task: connects to wss://ws-live-data.polymarket.com, subscribes,
/// reads messages, and sends SpotPrice events to the engine channel.
/// Auto-reconnects with backoff.
async fn rtds_task(
    subscriptions: Vec<RtdsSubscription>,
    tx: crossbeam_channel::Sender<MarketEvent>,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = crate::exchange::ReconnectBackoff::new(100, 30_000);
    loop {
        if shutdown.load(Ordering::Relaxed) {
            info!("[RTDS] Shutdown, exiting");
            return;
        }
        let start = std::time::Instant::now();
        match rtds_connect_and_run(&subscriptions, &tx, &shutdown).await {
            Ok(()) => {
                info!("[RTDS] Task exiting");
                return;
            }
            Err(e) => {
                if shutdown.load(Ordering::Relaxed) { return; }
                if start.elapsed().as_secs() > 30 { backoff.reset(); }
                let delay = backoff.next_delay();
                warn!("[RTDS] Error: {}, reconnecting in {:.1}s...", e, delay.as_secs_f64());
                tokio::time::sleep(delay).await;
            }
        }
    }
}

async fn rtds_connect_and_run(
    subscriptions: &[RtdsSubscription],
    tx: &crossbeam_channel::Sender<MarketEvent>,
    shutdown: &AtomicBool,
) -> Result<()> {
    info!("[RTDS] Connecting to {}", POLYMARKET_RTDS_URL);
    let (stream, _) = tokio_tungstenite::connect_async(POLYMARKET_RTDS_URL).await?;
    let (mut write, mut read) = stream.split();

    // Build and send subscriptions. RTDS honors only ONE subscription per
    // topic per connection — several per-symbol filtered entries on the
    // same topic silently keep the FIRST and drop the rest, and a filters
    // ARRAY is rejected (observed 2026-07-11 on crypto_prices_chainlink).
    // Single filter → keep the server-side filter; multiple → subscribe
    // the whole topic unfiltered and rely on the client-side `pass`
    // symbol filter in the read loop.
    let mut subs = Vec::new();
    for rtds in subscriptions {
        let (topic, typ) = rtds.topic_and_type();
        if rtds.filters.len() == 1 {
            let filters_json = serde_json::json!({"symbol": rtds.filters[0]}).to_string();
            subs.push(serde_json::json!({
                "topic": topic,
                "type": typ,
                "filters": filters_json,
            }));
        } else {
            subs.push(serde_json::json!({"topic": topic, "type": typ}));
        }
    }

    let msg = serde_json::json!({
        "action": "subscribe",
        "subscriptions": subs,
    });
    info!("[RTDS] Subscribe: {}", msg);
    write.send(Message::Text(msg.to_string())).await?;

    info!("[RTDS] Connected, {} subscriptions", subscriptions.len());

    let mut ping_interval = tokio::time::interval(RTDS_PING_INTERVAL);
    ping_interval.tick().await;

    loop {
        if shutdown.load(Ordering::Relaxed) { return Ok(()); }

        tokio::select! {
            biased;
            _ = ping_interval.tick() => {
                write.send(Message::Text(r#"{"action":"ping"}"#.to_string())).await?;
            }
            read_result = tokio::time::timeout(RTDS_STALE_THRESHOLD, read.next()) => {
                let msg = match read_result {
                    Ok(Some(Ok(m))) => m,
                    Ok(Some(Err(e))) => return Err(e.into()),
                    Ok(None) => return Err(anyhow!("RTDS stream ended")),
                    Err(_elapsed) => {
                        return Err(anyhow!(
                            "RTDS no message for {:.0}s (stall watchdog) — forcing reconnect",
                            RTDS_STALE_THRESHOLD.as_secs_f64(),
                        ));
                    }
                };
                match msg {
                    Message::Ping(payload) => {
                        let _ = write.send(Message::Pong(payload)).await;
                    }
                    Message::Close(reason) => {
                        warn!("[RTDS] Server closed: {:?}", reason);
                        return Err(anyhow!("RTDS closed"));
                    }
                    Message::Text(text) => {
                        if text.is_empty() { continue; }
                        // simd-json drop-in — same Value output, SIMD parse.
                        let mut buf = text.as_bytes().to_vec();
                        let data: serde_json::Value = match simd_json::serde::from_slice(&mut buf) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        let topic = match data.get("topic").and_then(|v| v.as_str()) {
                            Some(t) if !t.is_empty() => t,
                            _ => continue,
                        };
                        let payload = match data.get("payload") { Some(p) => p, None => continue };
                        let symbol = match payload.get("symbol").and_then(|v| v.as_str()) {
                            Some(s) => s, None => continue,
                        };
                        let price = match payload.get("value").and_then(|v| v.as_f64()) {
                            Some(p) => p, None => continue,
                        };
                        let server_ts_ms = data.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0);
                        let source = match topic {
                            "crypto_prices" => "rtds_binance",
                            "crypto_prices_chainlink" => "rtds_chainlink",
                            "equity_prices" => "rtds_pyth",
                            _ => continue,
                        };
                        let pass = subscriptions.iter().any(|r| {
                            let (t, _) = r.topic_and_type();
                            t == topic && (r.filters.is_empty() || r.filters.iter().any(|f| f.eq_ignore_ascii_case(symbol)))
                        });
                        if !pass {
                            log::trace!("[RTDS] Filtered out: topic={} symbol={} price={}", topic, symbol, price);
                            continue;
                        }
                        let event = MarketEvent::SpotPrice(SpotPrice {
                            source: source.to_string(),
                            symbol: symbol.to_string(),
                            price,
                            timestamp_ns: server_ts_ms * 1_000_000,
                            local_timestamp_ns: now_ns(),
                        });
                        if tx.send(event).is_err() {
                            return Ok(());
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

// ────────────────────────────────────────────────────────────────
// Typed CLOB WS message schemas (simd-json fast path)
// ────────────────────────────────────────────────────────────────
//
// Each incoming frame is either a single JSON object or a JSON array
// of objects. Each object is either a tagged CLOB event (carrying
// `event_type`: book / trade / last_trade_price / tick_size_change /
// price_change) OR an RTDS spot-price record (has `source` + `pair`,
// no event_type) — the server multiplexes both streams on the same
// socket. We model this with a `#[serde(untagged)]` outer enum that
// picks tagged-vs-RTDS per message, and a `#[serde(tag = "event_type")]`
// inner enum for the tagged flavour.
//
// Why typed + simd-json? Replacing `serde_json::from_str::<Value>` +
// `.get("field")` tree walks with a single-pass typed deserialize
// avoids per-frame HashMap construction for every object / nested
// level — hot path wins ~3-5x in practice.

#[derive(serde::Deserialize)]
struct BookLevel {
    price: String,
    size: String,
}

#[derive(serde::Deserialize)]
struct BookFields {
    asset_id: String,
    #[serde(default)]
    bids: Vec<BookLevel>,
    #[serde(default)]
    asks: Vec<BookLevel>,
    /// Polymarket emits timestamps as stringified ms (e.g. "1730000000123").
    #[serde(default)]
    timestamp: Option<String>,
}

#[derive(serde::Deserialize)]
struct TradeFields {
    asset_id: String,
    price: String,
    size: String,
    side: String, // "BUY" | "SELL"
}

#[derive(serde::Deserialize)]
struct TickSizeFields {
    asset_id: String,
    #[serde(default, deserialize_with = "de_str_or_num_f64")]
    old_tick_size: f64,
    #[serde(default, deserialize_with = "de_str_or_num_f64")]
    new_tick_size: f64,
}

/// Inline RTDS spot-price record seen on the CLOB socket (distinct from
/// the dedicated RTDS WS schema, which wraps in `topic`/`payload`).
#[derive(serde::Deserialize)]
struct InlineRtdsFields {
    source: String,
    #[serde(default)]
    pair: Option<String>,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    filter: Option<String>,
    #[serde(default, deserialize_with = "de_opt_str_or_num_f64")]
    value: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_str_or_num_f64")]
    price: Option<f64>,
    #[serde(default)]
    server_timestamp: Option<serde_json::Value>,
    #[serde(default)]
    timestamp: Option<serde_json::Value>,
}

#[derive(serde::Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
enum TaggedMessage {
    Book(BookFields),
    Trade(TradeFields),
    LastTradePrice(TradeFields),
    TickSizeChange(TickSizeFields),
    PriceChange {},
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum ClobFrame {
    /// Matches anything with `event_type` set to a known variant.
    Tagged(TaggedMessage),
    /// Matches RTDS records inlined on the CLOB socket (no event_type).
    Rtds(InlineRtdsFields),
    /// Anything else — silently dropped.
    #[allow(dead_code)]
    Unknown(serde::de::IgnoredAny),
}

/// Deserialize a field that may arrive as a number or a string-encoded
/// number. Defaults to 0.0 on any other shape.
fn de_str_or_num_f64<'de, D>(d: D) -> Result<f64, D::Error>
where D: serde::Deserializer<'de>
{
    let v = serde_json::Value::deserialize(d)?;
    Ok(match v {
        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
        serde_json::Value::String(s) => s.parse().unwrap_or(0.0),
        _ => 0.0,
    })
}

fn de_opt_str_or_num_f64<'de, D>(d: D) -> Result<Option<f64>, D::Error>
where D: serde::Deserializer<'de>
{
    let v = Option::<serde_json::Value>::deserialize(d)?;
    Ok(match v {
        Some(serde_json::Value::Number(n)) => n.as_f64(),
        Some(serde_json::Value::String(s)) => s.parse().ok(),
        _ => None,
    })
}

/// Parse a whole CLOB WS text frame (single object OR array of objects)
/// using simd-json. Returns an owned Vec of MarketEvents to emit.
fn parse_clob_frame(text: &str) -> Vec<MarketEvent> {
    if text.is_empty() { return Vec::new(); }
    // simd-json mutates the input buffer in place, so we need an owned
    // Vec<u8>. This is one allocation per frame — cheaper than the
    // multiple HashMap allocations serde_json::Value does internally.
    let mut buf = text.as_bytes().to_vec();
    // Peek the first non-whitespace byte to decide array vs single.
    let first = buf.iter().copied().find(|b| !b.is_ascii_whitespace());
    let is_array = first == Some(b'[');

    let frames: Vec<ClobFrame> = if is_array {
        match simd_json::serde::from_slice::<Vec<ClobFrame>>(&mut buf) {
            Ok(v) => v,
            Err(e) => {
                warn!("[Polymarket] simd-json parse (array) failed: {} (raw: {})",
                    e, &text[..text.len().min(200)]);
                return Vec::new();
            }
        }
    } else {
        match simd_json::serde::from_slice::<ClobFrame>(&mut buf) {
            Ok(v) => vec![v],
            Err(e) => {
                warn!("[Polymarket] simd-json parse (single) failed: {} (raw: {})",
                    e, &text[..text.len().min(200)]);
                return Vec::new();
            }
        }
    };

    let now = now_ns();
    let mut out = Vec::with_capacity(frames.len());
    for f in frames {
        match f {
            ClobFrame::Tagged(TaggedMessage::Book(b)) => {
                out.push(make_book_event(b, now));
            }
            ClobFrame::Tagged(TaggedMessage::Trade(t))
            | ClobFrame::Tagged(TaggedMessage::LastTradePrice(t)) => {
                if let Some(e) = make_trade_event(t, now) { out.push(e); }
            }
            ClobFrame::Tagged(TaggedMessage::TickSizeChange(t)) => {
                out.push(make_tick_size_event(t, now));
            }
            ClobFrame::Tagged(TaggedMessage::PriceChange {}) => { /* ignored */ }
            ClobFrame::Rtds(r) => {
                if let Some(e) = make_inline_rtds_event(r, now) { out.push(e); }
            }
            ClobFrame::Unknown(_) => { /* ignored */ }
        }
    }
    out
}

fn make_book_event(b: BookFields, now: u64) -> MarketEvent {
    let parse_side = |levels: Vec<BookLevel>| -> Vec<PriceLevel> {
        levels.into_iter()
            .filter_map(|l| {
                let price: f64 = l.price.parse().ok()?;
                let quantity: f64 = l.size.parse().ok()?;
                Some(PriceLevel { price, quantity })
            })
            .collect()
    };
    let exchange_ts_ns = b.timestamp
        .as_deref()
        .and_then(|s| s.parse::<u64>().ok())
        .map(|ms| ms * 1_000_000)
        .unwrap_or(now);
    MarketEvent::OrderBook(OrderBookSnapshot {
        exchange: Exchange::Polymarket,
        symbol: b.asset_id,
        bids: parse_side(b.bids),
        asks: parse_side(b.asks),
        exchange_timestamp_ns: exchange_ts_ns,
        local_timestamp_ns: now,
    })
}

fn make_trade_event(t: TradeFields, now: u64) -> Option<MarketEvent> {
    let price: f64 = t.price.parse().ok()?;
    let quantity: f64 = t.size.parse().ok()?;
    Some(MarketEvent::Trade(TradeTick {
        exchange: Exchange::Polymarket,
        symbol: t.asset_id,
        price,
        quantity,
        side: if t.side == "BUY" { Side::Buy } else { Side::Sell },
        exchange_timestamp_ns: now,
        local_timestamp_ns: now,
    }))
}

fn make_tick_size_event(t: TickSizeFields, now: u64) -> MarketEvent {
    MarketEvent::TickSizeChange(TickSizeChange {
        exchange: Exchange::Polymarket,
        symbol: t.asset_id,
        old_tick_size: t.old_tick_size,
        new_tick_size: t.new_tick_size,
        local_timestamp_ns: now,
    })
}

fn make_inline_rtds_event(r: InlineRtdsFields, local_now: u64) -> Option<MarketEvent> {
    let symbol = r.pair.or(r.symbol).or(r.filter)?;
    let price = r.value.or(r.price)?;
    // Normalize timestamp (sec / ms / ns) to ns.
    let ts_raw = r.server_timestamp.or(r.timestamp).and_then(|v| {
        v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
    });
    let ts_ns = match ts_raw {
        Some(ts) if ts < 1_000_000_000_000 => ts * 1_000_000_000,
        Some(ts) if ts < 1_000_000_000_000_000 => ts * 1_000_000,
        Some(ts) => ts,
        None => local_now,
    };
    Some(MarketEvent::SpotPrice(SpotPrice {
        source: format!("rtds_{}", r.source),
        symbol,
        price,
        timestamp_ns: ts_ns,
        local_timestamp_ns: local_now,
    }))
}

impl ExchangeMarket for PolymarketMarket {
    fn connect(&mut self) -> Result<()> {
        // Per-task shutdown Arc: each connect() creates a FRESH Arc
        // rather than reusing the struct field. Old tasks (still
        // draining a previous connection — possibly hung in
        // `read.next()` on a TCP zombie) keep their own Arc which
        // stays `false`; they never learn shutdown=true and would
        // otherwise race the new task here when the next disconnect/
        // connect cycle resets the shared atomic. Replaces the
        // pre-2026-05-10 single-Arc scheme that historically dropped
        // shutdown=true within a few microseconds of the next connect.
        // The previous "infinite dead-reconnect loop" guard (2026-04-20)
        // is now naturally satisfied because the FRESH Arc starts at
        // `false` at construction time.
        let shutdown = Arc::new(AtomicBool::new(false));
        self.ws_shutdown = shutdown.clone();

        // Spawn the main CLOB async task on the shared runtime. Bridge into
        // the sync engine via a crossbeam event channel; take control input
        // (resubscribe / shutdown) via a tokio mpsc.
        let all_tokens = self.current_tokens();
        let (event_tx, event_rx) = crossbeam_channel::unbounded::<MarketEvent>();
        let (ctrl_tx, ctrl_rx) = tokio::sync::mpsc::unbounded_channel::<WsCtrl>();
        self.event_rx = Some(event_rx);
        self.ws_ctrl_tx = Some(ctrl_tx);

        crate::async_rt::handle().spawn(clob_ws_task(
            all_tokens.clone(),
            event_tx,
            ctrl_rx,
            shutdown,
        ));

        // Spawn RTDS task if subscriptions exist (only once)
        if !self.rtds_subscriptions.is_empty() && self.rtds_tx.is_some() {
            let subs = self.rtds_subscriptions.clone();
            let tx = self.rtds_tx.clone().unwrap();
            let sd = self.rtds_shutdown.clone();
            crate::async_rt::handle().spawn(rtds_task(subs, tx, sd));
            self.rtds_tx = None; // don't respawn
        }

        info!(
            "[Polymarket] WS tasks launched — {} CLOB tokens, {} rtds sources",
            all_tokens.len(), self.rtds_subscriptions.len(),
        );
        Ok(())
    }

    fn subscribe(&mut self, symbols: &[String]) -> Result<()> {
        for symbol_str in symbols {
            // RTDS format: "rtds:binance:btcusdt,solusdt" or "rtds:chainlink:btc/usd,eth/usd"
            if let Some(rtds_rest) = symbol_str.strip_prefix("rtds:") {
                let parts: Vec<&str> = rtds_rest.splitn(2, ':').collect();
                if parts.len() == 2 {
                    let source = parts[0].to_string();
                    let filters: Vec<String> = parts[1].split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect();
                    info!(
                        "[Polymarket] RTDS subscription: source={}, filters={:?}",
                        source, filters,
                    );
                    self.rtds_subscriptions.push(RtdsSubscription { source, filters });
                } else {
                    warn!("[Polymarket] Invalid rtds format: {}", symbol_str);
                }
                continue;
            }

            if is_event_series(symbol_str) {
                // Event series format: "series:slug-name"
                // Subscribe to the current active event in the series, with automatic re-fetch
                let series_slug = &symbol_str["series:".len()..];
                let (series_id, event) = fetch_active_event_with_series_id(series_slug)?;
                info!(
                    "[Polymarket] Event series '{}': found '{}' (id={}, {} markets)",
                    series_slug, event.title, event.id, event.markets.len()
                );

                let series_idx = self.series.len();

                // Register all active market tokens
                let mut symbols_state = Vec::new();
                for condition in &event.markets {
                    if !condition.active || condition.closed {
                        continue; // skip resolved markets
                    }
                    for (i, token_id) in condition.clob_token_ids.iter().enumerate() {
                        self.token_to_series.insert(token_id.clone(), series_idx);
                        let outcome = condition.outcomes.get(i).cloned().unwrap_or_default();
                        symbols_state.push(SymbolState {
                            token_id: token_id.clone(),
                            _outcome: outcome,
                            _condition_id: condition.condition_id.clone(),
                        });
                    }
                }

                // Queue EventStart so recorder knows the event context
                self.pending_events.push_back(MarketEvent::EventStart {
                    exchange: Exchange::Polymarket,
                    symbol: symbol_str.clone(),
                    event_id: event.id.clone(),
                    event_start_ns: now_ns(),
                });

                // Queue Instrument events for active markets
                for condition in &event.markets {
                    if !condition.active || condition.closed {
                        continue;
                    }
                    let mut binary_option: crate::types::BinaryOption = condition.clone().into();
                    binary_option.slug = event.slug.clone();
                    self.pending_events.push_back(MarketEvent::Instrument(
                        crate::types::Instrument::BinaryOption(binary_option),
                    ));
                }

                // Parse end_date for rotation check — use event level end_date,
                // or if not available, set to check every 5 minutes
                let end_ns = parse_date_ns(&event.end_date)
                    .unwrap_or(now_ns() + 300_000_000_000); // 5 min default

                let market = MarketState {
                    event_id: event.id.clone(),
                    start_ns: now_ns(),
                    end_ns,
                    symbols: symbols_state,
                };

                let active_count = event.markets.iter().filter(|m| m.active && !m.closed).count();
                info!(
                    "[Polymarket] Event series '{}': subscribed to {}/{} active markets, {} tokens",
                    series_slug, active_count, event.markets.len(), market.symbols.len()
                );

                self.series.push(SeriesState {
                    name: symbol_str.clone(),
                    interval_minutes: -1, // Special: event series mode (re-fetch on expiry)
                    market,
                    series_id: Some(series_id),
                    next_retry_ns: 0,
                    refresh_fail_count: 0,
                    refresh_fail_first_ns: 0,
                    refresh_idling_logged: false,
                });
            } else {
                // Event slug format: subscribe by slug for price reference (no rotation)
                let event = fetch_event_by_slug(symbol_str)?;
                info!(
                    "[Polymarket] Found event by slug '{}': {} ({} markets)",
                    symbol_str, event.title, event.markets.len()
                );

                let series_idx = self.series.len();

                // Register all token IDs for WS subscription
                let mut symbols_state = Vec::new();
                for condition in &event.markets {
                    for (i, token_id) in condition.clob_token_ids.iter().enumerate() {
                        self.token_to_series.insert(token_id.clone(), series_idx);
                        let outcome = condition.outcomes.get(i).cloned().unwrap_or_default();
                        symbols_state.push(SymbolState {
                            token_id: token_id.clone(),
                            _outcome: outcome,
                            _condition_id: condition.condition_id.clone(),
                        });
                    }
                }

                // Queue Instrument events (override slug to event slug for cross-exchange matching)
                for condition in &event.markets {
                    let mut binary_option: crate::types::BinaryOption = condition.clone().into();
                    binary_option.slug = event.slug.clone();
                    self.pending_events.push_back(MarketEvent::Instrument(
                        crate::types::Instrument::BinaryOption(binary_option),
                    ));
                }

                let market = MarketState {
                    event_id: event.id.clone(),
                    start_ns: now_ns(),
                    end_ns: u64::MAX, // No expiry for slug-based subscriptions
                    symbols: symbols_state,
                };

                self.series.push(SeriesState {
                    name: symbol_str.clone(),
                    interval_minutes: 0, // No rotation
                    market,
                    series_id: None,
                    next_retry_ns: 0,
                    refresh_fail_count: 0,
                    refresh_fail_first_ns: 0,
                    refresh_idling_logged: false,
                });
            }
        }

        let total_markets: usize = self.series.iter()
            .map(|s| s.market.symbols.len())
            .sum();
        info!(
            "[Polymarket] {} events, {} markets, {} pending instrument events",
            self.series.len(),
            total_markets,
            self.pending_events.len(),
        );
        Ok(())
    }

    fn next_event(&mut self) -> Result<Option<MarketEvent>> {
        // Drain pending synthetic events first (EventStart, Instrument, ...)
        if let Some(event) = self.pending_events.pop_front() {
            return Ok(Some(event));
        }

        // Check for event rotation — may push more synthetic events.
        self.check_rotation()?;
        if let Some(event) = self.pending_events.pop_front() {
            return Ok(Some(event));
        }

        // Drain one event from the async WS task if available.
        if let Some(rx) = &self.event_rx {
            match rx.try_recv() {
                Ok(mut event) => {
                    self.map_event_symbol(&mut event);
                    return Ok(Some(event));
                }
                Err(crossbeam_channel::TryRecvError::Empty) => return Ok(None),
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    return Err(anyhow!("Polymarket WS task ended unexpectedly"));
                }
            }
        }
        Ok(None)
    }

    fn disconnect(&mut self) {
        self.ws_shutdown.store(true, Ordering::Relaxed);
        if let Some(tx) = self.ws_ctrl_tx.take() {
            let _ = tx.send(WsCtrl::Shutdown);
        }
        self.event_rx = None;
        info!("[Polymarket] Disconnected");
    }

    fn name(&self) -> &str {
        "polymarket"
    }

    /// A Polymarket feed only produces market data while at least one
    /// CLOB token is subscribed. Between events (no currently-trading
    /// event in any series) the WS will legitimately go silent — the
    /// engine's data-timeout watchdog should not flap-reconnect during
    /// those windows.
    fn has_active_subscription(&self) -> bool {
        self.series.iter().any(|s| !s.market.symbols.is_empty())
    }
}

#[cfg(test)]
mod pick_current_event_tests {
    use super::*;

    /// Build a minimal event whose open time comes from the slug's trailing
    /// timestamp (`btc-updown-5m-<start_secs>`) and whose `end_date` is the
    /// ISO form of `end_secs`. `markets` is left empty so `event_open_ns`
    /// resolves via the slug-fallback path.
    fn mk_event(start_secs: u64, end_secs: u64) -> PolymarketEvent {
        let end_iso = chrono::DateTime::<chrono::Utc>::from_timestamp(end_secs as i64, 0)
            .unwrap()
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();
        PolymarketEvent {
            id: format!("id-{}", start_secs),
            slug: format!("btc-updown-5m-{}", start_secs),
            title: format!("evt-{}", start_secs),
            description: String::new(),
            active: true,
            closed: false,
            end_date: end_iso,
            markets: vec![],
        }
    }

    fn now() -> u64 { chrono::Utc::now().timestamp() as u64 }

    #[test]
    fn picks_open_event_not_future_one() {
        let n = now();
        let open = mk_event(n - 60, n + 240);        // started 60s ago, ends in 4m
        let upcoming = mk_event(n + 1200, n + 1500);  // opens in 20m (the live.log case)
        let picked = pick_current_event(vec![upcoming, open.clone()], "btc-updown-5m").unwrap();
        assert_eq!(picked.slug, open.slug, "must pick the already-open event over a future one");
    }

    #[test]
    fn series_gap_returns_upcoming_not_err() {
        // Reproduces live.log 2026-06-17T20:10: the only event in the series
        // opens ~20m later (20:30). Must return it as pending, not error.
        let n = now();
        let upcoming = mk_event(n + 1186, n + 1186 + 300);
        let picked = pick_current_event(vec![upcoming.clone()], "btc-updown-5m").unwrap();
        assert_eq!(picked.slug, upcoming.slug, "series gap -> return upcoming event as pending");
    }

    #[test]
    fn picks_soonest_to_expire_among_open() {
        let n = now();
        let ends_sooner = mk_event(n - 120, n + 60);
        let ends_later = mk_event(n - 60, n + 240);
        let picked = pick_current_event(vec![ends_later, ends_sooner.clone()], "s").unwrap();
        assert_eq!(picked.slug, ends_sooner.slug);
    }

    #[test]
    fn all_expired_is_err() {
        let n = now();
        let expired = mk_event(n - 600, n - 300);
        assert!(pick_current_event(vec![expired], "s").is_err());
    }

    #[test]
    fn unknown_start_treated_as_open() {
        // No parseable start (no slug timestamp, no markets) -> legacy
        // end-only behaviour: treated as open as long as end > now.
        let n = now();
        let mut e = mk_event(n + 1200, n + 1500); // end in the future
        e.slug = "categorical-market-no-timestamp".into();
        let picked = pick_current_event(vec![e.clone()], "s").unwrap();
        assert_eq!(picked.slug, e.slug, "unknown start -> open (legacy end-only)");
    }
}
