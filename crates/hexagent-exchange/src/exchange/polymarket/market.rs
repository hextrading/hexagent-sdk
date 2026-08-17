use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

use crate::exchange::{
    ws_send, ExchangeMarket, WsHealth, POLYMARKET_RTDS_PING_INTERVAL, POLYMARKET_RTDS_PING_PAYLOAD,
    POLYMARKET_WS_HEALTH_LOG_INTERVAL, POLYMARKET_WS_HEARTBEAT_INTERVAL, WS_CONNECT_TIMEOUT,
};
use crate::types::*;

const POLYMARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const POLYMARKET_RTDS_URL: &str = "wss://ws-live-data.polymarket.com";
const MAX_PUBLIC_EVENT_FUTURE_SKEW_NS: u64 = 2_000_000_000;

const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";
const GAMMA_EVENT_CACHE_TTL: Duration = Duration::from_secs(120);
const GAMMA_HTTP_IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const ROTATION_GAMMA_ATTEMPTS: u32 = 2;
const ROTATION_REFRESH_TIMEOUT_NS: u64 = 15_000_000_000;

/// Gamma is control-plane traffic (startup metadata and event rotation), not
/// an order-path service. Keep one ordinary client for opportunistic HTTP/1.1
/// reuse, but do not pre-warm it or enable TCP keepalive. An idle connection
/// naturally leaves the pool after 90 seconds.
pub(crate) fn gamma_http_client() -> reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .http1_only()
                .pool_idle_timeout(GAMMA_HTTP_IDLE_TIMEOUT)
                .pool_max_idle_per_host(2)
                .tcp_nodelay(true)
                .timeout(Duration::from_secs(5))
                .connect_timeout(Duration::from_secs(2))
                .build()
                .expect("build Gamma HTTP client")
        })
        .clone()
}

/// Gamma GET with the same transient-error policy used previously:
/// retry network/body errors plus 408/425/429/5xx with exponential backoff.
pub(crate) fn gamma_get_text_retry(
    url: &str,
    attempts: u32,
    base_backoff_ms: u64,
) -> Result<String> {
    let url = url.to_string();
    let attempts = attempts.max(1);
    let client = gamma_http_client();
    crate::async_rt::block_on_runtime(async move {
        let mut last_err: Option<anyhow::Error> = None;
        for i in 0..attempts {
            match client.get(&url).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        match resp.text().await {
                            Ok(body) => return Ok(body),
                            Err(e) => {
                                last_err = Some(anyhow!("GET {} read body: {}", url, e));
                            }
                        }
                    } else {
                        let retriable =
                            matches!(status.as_u16(), 408 | 425 | 429) || status.is_server_error();
                        if !retriable || i + 1 == attempts {
                            return Err(anyhow!("GET {} returned {}", url, status));
                        }
                        last_err = Some(anyhow!("GET {} returned {}", url, status));
                    }
                }
                Err(e) => {
                    last_err = Some(anyhow!("GET {} failed: {}", url, e));
                }
            }

            if i + 1 < attempts {
                let exp = i.min(7);
                let delay_ms = base_backoff_ms.saturating_mul(1u64 << exp).min(30_000);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("GET {} failed after {} attempts", url, attempts)))
    })
}

/// Per-task read-side stall watchdogs. CLOB book diffs + trade ticks
/// arrive frequently during active markets but go quiet when there's
/// no currently-trading event — `has_active_subscription()` already
/// suppresses the engine-side data-timeout in that case, but the
/// in-task watchdog has no visibility into that. Use a generous 90 s
/// to cover quiet periods between events without false-tripping.
/// RTDS streams (spot prices) push ~10 Hz when subscribed; 30 s of
/// silence is plenty anomalous.
///
/// `CLOB_STALE_THRESHOLD` bounds the RAW-frame read: it only fires when
/// the socket delivers nothing at all. That is necessary but not
/// sufficient — the server answers our 5 s `PING` with `PONG`, and a
/// `PONG` is a raw frame, so this timer is reset every 5 s for as long
/// as the server's heartbeat responder is alive. A CLOB feed whose
/// market data has frozen while its heartbeat keeps answering is
/// therefore INVISIBLE to it. `CLOB_TOPIC_STALL_THRESHOLD` closes that
/// hole by bounding the age of the last TOPIC frame (book / trade /
/// price change) instead. Same 90 s for the same reason — it must clear
/// the quiet stretch between events — and it stays above the engine's
/// own 45 s Polymarket data-timeout, so the engine remains the first
/// responder and this is the backstop it was always documented to be.
const CLOB_STALE_THRESHOLD: Duration = Duration::from_secs(90);
/// Cooperative-scheduler probe for the dedicated CLOB runtime.  Any long
/// synchronous task accidentally moved onto that runtime appears as timer
/// drift in `polymarket.ws.clob_scheduler_lag`.
const CLOB_SCHEDULER_PROBE_INTERVAL: Duration = Duration::from_millis(10);
const CLOB_TOPIC_STALL_THRESHOLD: Duration = Duration::from_secs(90);
/// Dirty non-BBO L2 changes are collapsed into at most four full snapshots
/// per second per token. BBO changes still publish immediately.
const CLOB_BOOK_COALESCE_INTERVAL: Duration = Duration::from_millis(250);
const CLOB_BURST_METRIC_INTERVAL: Duration = Duration::from_secs(1);
const CLOB_DIAGNOSTIC_SAMPLE_INTERVAL: Duration = Duration::from_secs(30);
const RTDS_STALE_THRESHOLD: Duration = Duration::from_secs(30);
const TOPIC_STALE_WARNING_THRESHOLD: Duration = Duration::from_secs(30);

// ── Polymarket Event Types ─────────────────────────────────────────

/// Deserialize a field that is either a JSON array or a stringified JSON array.
/// Handles: `["a","b"]`, `"[\"a\",\"b\"]"`, and `null` / missing.
fn deserialize_json_string_array<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(s) => serde_json::from_str(&s).map_err(de::Error::custom),
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
    #[serde(
        default,
        deserialize_with = "deserialize_json_string_array",
        rename = "clobTokenIds"
    )]
    pub clob_token_ids: Vec<String>,
    /// Outcome labels — stringified JSON array in the API: `"[\"Yes\",\"No\"]"`
    #[serde(default, deserialize_with = "deserialize_json_string_array")]
    pub outcomes: Vec<String>,
    /// Outcome prices — stringified JSON array in the API: `"[\"0.65\",\"0.35\"]"`
    #[serde(
        default,
        deserialize_with = "deserialize_json_string_array",
        rename = "outcomePrices"
    )]
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

impl PolyMarketInfo {
    /// Validate the binary-market structure before token IDs can reach a
    /// strategy. Gamma returns parallel arrays, so accepting an empty,
    /// duplicate, or length-mismatched response would make outcome identity
    /// depend on array position alone.
    pub fn validate_binary_structure(&self) -> Result<()> {
        if self.condition_id.trim().is_empty() {
            return Err(anyhow!("missing condition id"));
        }
        if self.clob_token_ids.len() != 2 || self.outcomes.len() != 2 {
            return Err(anyhow!(
                "expected exactly two tokens and outcomes, got tokens={} outcomes={}",
                self.clob_token_ids.len(),
                self.outcomes.len(),
            ));
        }
        let first_token = self.clob_token_ids[0].trim();
        let second_token = self.clob_token_ids[1].trim();
        if first_token.is_empty() || second_token.is_empty() || first_token == second_token {
            return Err(anyhow!("token ids must be non-empty and distinct"));
        }
        let first_outcome = self.outcomes[0].trim().to_ascii_lowercase();
        let second_outcome = self.outcomes[1].trim().to_ascii_lowercase();
        if first_outcome.is_empty() || second_outcome.is_empty() || first_outcome == second_outcome
        {
            return Err(anyhow!("outcome labels must be non-empty and distinct"));
        }
        if !self.tick_size.is_finite() || self.tick_size <= 0.0 || self.tick_size >= 1.0 {
            return Err(anyhow!("invalid tick size {}", self.tick_size));
        }
        if !self.order_min_size.is_finite() || self.order_min_size <= 0.0 {
            return Err(anyhow!(
                "invalid minimum order size {}",
                self.order_min_size
            ));
        }
        Ok(())
    }
}

fn accepted_binary_markets<'a>(
    markets: &'a [PolyMarketInfo],
    active_only: bool,
) -> Vec<&'a PolyMarketInfo> {
    markets
        .iter()
        .filter(|market| !active_only || (market.active && !market.closed))
        .filter(|market| match market.validate_binary_structure() {
            Ok(()) => true,
            Err(error) => {
                warn!(
                    "[Polymarket] Rejecting malformed binary market id={} condition={}: {}",
                    market.id, market.condition_id, error,
                );
                false
            }
        })
        .collect()
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
            series_slug: String::new(),
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

#[derive(Clone)]
struct CachedGammaEvent {
    series_id: String,
    event: PolymarketEvent,
    cached_at: Instant,
}

/// Process-wide cache keyed by Gamma event id. `fetch_next_event` callers do
/// not know that id in advance, so lookups scan the small live set by series
/// and end-date threshold. There is intentionally no in-flight/singleflight
/// state: simultaneous first misses may each call Gamma, while every completed
/// result is immediately reusable by the other accounts and by rotation.
static GAMMA_EVENT_CACHE: OnceLock<Mutex<HashMap<String, CachedGammaEvent>>> = OnceLock::new();

fn gamma_event_cache() -> &'static Mutex<HashMap<String, CachedGammaEvent>> {
    GAMMA_EVENT_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn cache_entry_is_fresh(entry: &CachedGammaEvent, now: Instant) -> bool {
    now.checked_duration_since(entry.cached_at)
        .map(|age| age <= GAMMA_EVENT_CACHE_TTL)
        .unwrap_or(false)
}

fn cache_gamma_events_at(
    cache: &Mutex<HashMap<String, CachedGammaEvent>>,
    series_id: &str,
    events: &[PolymarketEvent],
    now: Instant,
) {
    let mut cache = cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache.retain(|_, entry| cache_entry_is_fresh(entry, now));
    for event in events {
        if event.id.is_empty() {
            continue;
        }
        cache.insert(
            event.id.clone(),
            CachedGammaEvent {
                series_id: series_id.to_string(),
                event: event.clone(),
                cached_at: now,
            },
        );
    }
}

fn cache_gamma_events(series_id: &str, events: &[PolymarketEvent]) {
    cache_gamma_events_at(gamma_event_cache(), series_id, events, Instant::now());
}

fn cached_gamma_event_after_at(
    cache: &Mutex<HashMap<String, CachedGammaEvent>>,
    series_id: &str,
    end_date_min_secs: u64,
    now: Instant,
) -> Option<PolymarketEvent> {
    let min_end_ns = end_date_min_secs.saturating_mul(1_000_000_000);
    let mut cache = cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache.retain(|_, entry| cache_entry_is_fresh(entry, now));
    cache
        .values()
        .filter(|entry| entry.series_id == series_id)
        .filter_map(|entry| {
            let end_ns = parse_date_ns(&entry.event.end_date).ok()?;
            (end_ns > min_end_ns).then_some((end_ns, entry.event.clone()))
        })
        .min_by_key(|(end_ns, _)| *end_ns)
        .map(|(_, event)| event)
}

fn cached_gamma_event_after(series_id: &str, end_date_min_secs: u64) -> Option<PolymarketEvent> {
    cached_gamma_event_after_at(
        gamma_event_cache(),
        series_id,
        end_date_min_secs,
        Instant::now(),
    )
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
    if log {
        info!("[Polymarket] Fetching event by slug: {}", url);
    }

    // 5 attempts × exponential backoff (200 ms base) ≈ 6 s ceiling —
    // covers brief gamma-api 5xx blips during event rotation without
    // permanently stalling the subscribe / maintenance path.
    let resp_text = gamma_get_text_retry(&url, 5, 200)?;
    if log {
        info!(
            "[Polymarket] Gamma API response (first 500 chars): {}",
            &resp_text[..resp_text.len().min(500)]
        );
    }

    let events: Vec<PolymarketEvent> = serde_json::from_str(&resp_text)
        .map_err(|e| anyhow!("Failed to parse Gamma API response: {}", e))?;

    let event = events
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("No Polymarket event found for slug: {}", slug))?;

    if log {
        info!(
            "[Polymarket] Found event: '{}' (id={}, {} markets)",
            event.title,
            event.id,
            event.markets.len()
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
    fetch_series_id_with_attempts(series_slug, 5)
}

fn fetch_series_id_with_attempts(series_slug: &str, attempts: u32) -> Result<String> {
    let url = format!(
        "{}/series?slug={}&exclude_events=true",
        GAMMA_API_BASE, series_slug
    );
    info!("[Polymarket] Fetching series by slug: {}", url);

    // Startup uses five attempts; the detached rotation worker uses two.
    let resp_text = gamma_get_text_retry(&url, attempts, 200)?;
    let series_list: Vec<PolymarketSeries> = serde_json::from_str(&resp_text)
        .map_err(|e| anyhow!("Failed to parse series response: {}", e))?;

    let series = series_list
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("No series found for slug: {}", series_slug))?;

    info!("[Polymarket] Series '{}' (id={})", series.title, series.id);
    Ok(series.id)
}

/// Step 2: Fetch the soonest-to-end event in a series whose `end_date`
/// leaves enough time to initialize safely. This is the "currently trading"
/// event for cycle-based series like `btc-up-or-down-5m`, or the next event
/// when the current cycle is already too close to expiry.
///
/// Uses `GET /events?series_id=...&end_date_min=<now+guard>&ascending=true&limit=1`.
/// The guard is 20% of the parsed event duration, clamped to 5–60 seconds
/// (60 seconds for a 5-minute event). Unknown-duration series keep the
/// legacy 1-second boundary guard.
///
/// `closed` is intentionally NOT in the query — gamma-api occasionally
/// flips a freshly-rotated event's `closed` flag to `true` for a few
/// seconds before the next event is published, which would otherwise
/// surface as a spurious "no live event" warning. The
/// The guarded `end_date_min` filter excludes expired/nearly-expired events;
/// the strategy's own settle-and-detach machinery handles closed-flag
/// transitions.
fn fetch_active_events_by_series_id(
    series_id: &str,
    series_slug: &str,
) -> Result<Vec<PolymarketEvent>> {
    fetch_active_events_by_series_id_with_attempts(series_id, series_slug, 5)
}

fn fetch_active_events_by_series_id_with_attempts(
    series_id: &str,
    series_slug: &str,
    attempts: u32,
) -> Result<Vec<PolymarketEvent>> {
    let now_secs = chrono::Utc::now().timestamp() as u64;
    let guard_secs = min_event_remaining_secs(series_slug);
    let end_min_iso =
        chrono::DateTime::<chrono::Utc>::from_timestamp((now_secs + guard_secs) as i64, 0)
            .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
            .unwrap_or_default();
    let url = format!(
        "{}/events?series_id={}&end_date_min={}&ascending=true&limit=1",
        GAMMA_API_BASE, series_id, end_min_iso,
    );
    info!(
        "[Polymarket] Fetching active events for series_id={}: {}",
        series_id, url
    );

    // Startup uses five attempts; the detached rotation worker uses two and
    // is additionally bounded by its poll-side deadline.
    let resp_text = gamma_get_text_retry(&url, attempts, 200)?;
    let events: Vec<PolymarketEvent> = serde_json::from_str(&resp_text)
        .map_err(|e| anyhow!("Failed to parse events response: {}", e))?;

    cache_gamma_events(series_id, &events);
    info!(
        "[Polymarket] Found {} active events in series",
        events.len()
    );
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
                if ns > 0 {
                    return Some(ns);
                }
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

/// Minimum useful lifetime for a newly-discovered event. Joining a nearly
/// finished cycle causes avoidable CLOB churn and often leaves too little time
/// to resolve its opening strike. Use a duration-relative guard for short
/// series while capping longer series at the strategy's 60-second strike
/// recovery window.
fn min_event_remaining_secs(series_slug: &str) -> u64 {
    parse_slug_duration_secs(series_slug)
        .map(|duration| (duration / 5).clamp(5, 60))
        .unwrap_or(1)
}

/// Pick the currently trading event from a list of events.
///
/// "Currently trading" = the event is **already open** (`start ≤ now`) and
/// has enough useful lifetime left (`end > now + guard`), choosing the
/// soonest-to-expire among those. An event whose open time is unknown is
/// treated as open, so series without a start timestamp keep the previous
/// end-only behaviour.
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
    let min_remaining_secs = min_event_remaining_secs(series_slug);
    let min_end = now + chrono::Duration::seconds(min_remaining_secs as i64);

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
        if event.end_date.is_empty() {
            continue;
        }
        let Some(end_dt) = parse_end(&event.end_date) else {
            continue;
        };
        if end_dt <= min_end {
            continue;
        } // expired or too close to expiry

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
        info!(
            "[Polymarket] Current event: '{}' (ends {})",
            event.title, event.end_date
        );
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
    Err(anyhow!(
        "No event with at least {}s remaining in series '{}'",
        min_remaining_secs,
        series_slug,
    ))
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
    let events = fetch_active_events_by_series_id(&series_id, series_slug)?;
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
    if let Some(event) = cached_gamma_event_after(series_id, end_date_min_secs) {
        info!(
            "[Polymarket] Next event cache hit: series_id={} id={} slug={}",
            series_id, event.id, event.slug,
        );
        return Ok(Some(event));
    }

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
    let resp_text = gamma_get_text_retry(&url, 5, 200)?;
    let events: Vec<PolymarketEvent> = serde_json::from_str(&resp_text)
        .map_err(|e| anyhow!("Failed to parse next-event response: {}", e))?;

    cache_gamma_events(series_id, &events);
    let Some(event) = events.into_iter().next() else {
        info!(
            "[Polymarket] Next event: <none> (no event with end_date ≥ {})",
            end_min_iso
        );
        return Ok(None);
    };
    let start_time = event
        .markets
        .first()
        .map(|m| m.event_start_time.as_str())
        .unwrap_or("?");
    let cid = event
        .markets
        .first()
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

fn fetch_active_event_by_series_id_with_attempts(
    series_id: &str,
    series_slug: &str,
    attempts: u32,
) -> Result<PolymarketEvent> {
    let now_secs = chrono::Utc::now().timestamp().max(0) as u64;
    let end_date_min_secs = now_secs.saturating_add(min_event_remaining_secs(series_slug));
    if let Some(event) = cached_gamma_event_after(series_id, end_date_min_secs) {
        info!(
            "[Polymarket] Rotation cache hit: series_id={} event_id={} slug={}",
            series_id, event.id, event.slug,
        );
        return pick_current_event(vec![event], series_slug);
    }

    let events = fetch_active_events_by_series_id_with_attempts(series_id, series_slug, attempts)?;
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
    Resubscribe(ClobSubscription),
    /// Shutdown the WS task cleanly.
    Shutdown,
}

#[derive(Debug, Clone)]
struct CanonicalEventSpec {
    condition_id: String,
    up_token: String,
    down_token: String,
}

#[derive(Debug, Clone, Default)]
struct ClobSubscription {
    tokens: Vec<String>,
    canonical_events: Vec<CanonicalEventSpec>,
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

type RotationFetchResult = std::result::Result<(String, PolymarketEvent), String>;

struct RotationRefresh {
    started_ns: u64,
    rx: crossbeam_channel::Receiver<RotationFetchResult>,
}

fn spawn_rotation_refresh(
    series_slug: String,
    cached_series_id: Option<String>,
    started_ns: u64,
) -> Result<RotationRefresh> {
    let (tx, rx) = crossbeam_channel::bounded(1);
    std::thread::Builder::new()
        .name("polymarket-rotation".to_string())
        .spawn(move || {
            let result = (|| -> Result<(String, PolymarketEvent)> {
                let series_id = match cached_series_id {
                    Some(id) => id,
                    None => fetch_series_id_with_attempts(&series_slug, ROTATION_GAMMA_ATTEMPTS)?,
                };
                let event = fetch_active_event_by_series_id_with_attempts(
                    &series_id,
                    &series_slug,
                    ROTATION_GAMMA_ATTEMPTS,
                )?;
                Ok((series_id, event))
            })()
            .map_err(|error| error.to_string());
            let _ = tx.send(result);
        })
        .map_err(|error| anyhow!("spawn rotation refresh worker: {error}"))?;
    Ok(RotationRefresh { started_ns, rx })
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
    /// In-flight Gamma lookup. Rotation is control-plane work and must never
    /// block the synchronous market-feed loop or its liveness watchdogs.
    rotation_refresh: Option<RotationRefresh>,
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
    /// Persists across engine-level disconnect/connect cycles. Once any CLOB
    /// subscription has been advertised READY, every later task must wait for
    /// a valid book before advertising recovery.
    clob_subscribed_once: Arc<AtomicBool>,
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
            clob_subscribed_once: Arc::new(AtomicBool::new(false)),
            rtds_subscriptions: Vec::new(),
            rtds_tx: None,
            rtds_shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Set the engine's market_tx and shutdown flag so RTDS task can send events directly.
    pub fn set_market_tx(
        &mut self,
        tx: crossbeam_channel::Sender<MarketEvent>,
        shutdown: Arc<AtomicBool>,
    ) {
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
        self.series
            .iter()
            .flat_map(|s| s.market.token_ids())
            .collect()
    }

    fn current_clob_subscription(&self) -> ClobSubscription {
        let tokens = self.current_tokens();
        let mut by_condition: HashMap<String, (Option<String>, Option<String>)> = HashMap::new();
        for symbol in self.series.iter().flat_map(|series| &series.market.symbols) {
            let entry = by_condition
                .entry(symbol._condition_id.clone())
                .or_insert((None, None));
            match symbol._outcome.trim().to_ascii_lowercase().as_str() {
                "up" | "yes" => entry.0 = Some(symbol.token_id.clone()),
                "down" | "no" => entry.1 = Some(symbol.token_id.clone()),
                _ => {}
            }
        }
        let mut canonical_events: Vec<_> = by_condition
            .into_iter()
            .filter_map(|(condition_id, (up_token, down_token))| {
                Some(CanonicalEventSpec {
                    condition_id,
                    up_token: up_token?,
                    down_token: down_token?,
                })
            })
            .collect();
        canonical_events.sort_by(|left, right| left.condition_id.cmp(&right.condition_id));
        ClobSubscription {
            tokens,
            canonical_events,
        }
    }

    /// Send a Resubscribe message to the async WS task. No-op if the task
    /// hasn't been started yet (e.g. rotation fires before connect()).
    fn resubscribe_ws(&self) {
        if let Some(tx) = &self.ws_ctrl_tx {
            let subscription = self.current_clob_subscription();
            let _ = tx.send(WsCtrl::Resubscribe(subscription));
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
                let refresh_result = match self.series[i]
                    .rotation_refresh
                    .as_ref()
                    .map(|refresh| (refresh.started_ns, refresh.rx.try_recv()))
                {
                    Some((_, Ok(result))) => {
                        self.series[i].rotation_refresh = None;
                        result
                    }
                    Some((started_ns, Err(crossbeam_channel::TryRecvError::Empty)))
                        if now.saturating_sub(started_ns) < ROTATION_REFRESH_TIMEOUT_NS =>
                    {
                        continue;
                    }
                    Some((started_ns, Err(crossbeam_channel::TryRecvError::Empty))) => {
                        self.series[i].rotation_refresh = None;
                        Err(format!(
                            "Gamma rotation lookup timed out after {:.1}s",
                            now.saturating_sub(started_ns) as f64 / 1e9,
                        ))
                    }
                    Some((_, Err(crossbeam_channel::TryRecvError::Disconnected))) => {
                        self.series[i].rotation_refresh = None;
                        Err("Gamma rotation lookup worker disconnected".to_string())
                    }
                    None => {
                        let refresh = spawn_rotation_refresh(
                            series_slug.clone(),
                            self.series[i].series_id.clone(),
                            now,
                        );
                        match refresh {
                            Ok(refresh) => {
                                info!(
                                    "[Polymarket] Event series '{}' rotation refresh started",
                                    series_slug,
                                );
                                self.series[i].rotation_refresh = Some(refresh);
                                continue;
                            }
                            Err(error) => Err(error.to_string()),
                        }
                    }
                };
                match refresh_result {
                    Ok((series_id, event)) => {
                        info!(
                            "[Polymarket] Event series '{}' refresh: '{}'",
                            series_slug, event.title
                        );
                        self.series[i].series_id = Some(series_id);

                        // Remove old token mappings
                        for sym in &self.series[i].market.symbols {
                            self.token_to_series.remove(&sym.token_id);
                        }

                        // Build new token list from structurally valid active
                        // markets only. Reuse the same accepted set for the
                        // token map and Instrument events.
                        let active_markets = accepted_binary_markets(&event.markets, true);
                        let mut symbols_state = Vec::new();
                        for condition in &active_markets {
                            for (j, token_id) in condition.clob_token_ids.iter().enumerate() {
                                self.token_to_series.insert(token_id.clone(), i);
                                let outcome =
                                    condition.outcomes.get(j).cloned().unwrap_or_default();
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
                        for condition in &active_markets {
                            let mut bo: crate::types::BinaryOption = (*condition).clone().into();
                            bo.slug = event.slug.clone();
                            bo.series_slug = series_slug
                                .strip_prefix("series:")
                                .unwrap_or(&series_slug)
                                .to_ascii_lowercase();
                            self.pending_events.push_back(MarketEvent::Instrument(
                                crate::types::Instrument::BinaryOption(bo),
                            ));
                        }

                        let end_ns =
                            parse_date_ns(&event.end_date).unwrap_or(now + 300_000_000_000);

                        self.series[i].market = MarketState {
                            event_id: event.id,
                            start_ns: now,
                            end_ns,
                            symbols: symbols_state,
                        };
                        self.series[i].next_retry_ns = 0;
                        // Reset failure-streak counters on success.
                        if self.series[i].refresh_idling_logged {
                            let dur_s = (now.saturating_sub(self.series[i].refresh_fail_first_ns))
                                as f64
                                / 1e9;
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
                                let dur_s =
                                    (now.saturating_sub(s.refresh_fail_first_ns)) as f64 / 1e9;
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

/// Main CLOB orderbook WS task. Runs on the dedicated CLOB tokio runtime.
///
/// Protocol:
///   - On start (and on each Resubscribe): (re)connect, send full
///     `{type: market, assets_ids: [...]}` subscription, then read messages
///     until a Resubscribe/Shutdown arrives or the socket fails.
///   - Parses each message into `MarketEvent`s and forwards them through
///     the sync crossbeam `event_tx` for `next_event()` to drain.
///   - Exponential backoff on connect failures; shared 5-second heartbeat.
#[derive(Debug, Default)]
struct ClobLifecycle {
    subscribed_once: bool,
    ready: bool,
    not_ready_announced: bool,
    not_ready_since: Option<Instant>,
    not_ready_reason: Option<String>,
}

#[derive(Debug)]
struct ClobReadyTransition {
    recovery: Option<Duration>,
    reason: Option<String>,
}

#[derive(Debug, Default, Clone, Copy)]
struct ClobWireCounters {
    books: u64,
    price_changes: u64,
    best_bid_asks: u64,
    trades: u64,
    last_trade_prices: u64,
    tick_size_changes: u64,
    inline_rtds: u64,
    price_change_entries: u64,
    level_upserts: u64,
    level_deletes: u64,
    bbo_mismatches: u64,
    unseeded_deltas: u64,
    ignored: u64,
    unknown: u64,
    parse_errors: u64,
}

impl ClobWireCounters {
    fn add(&mut self, rhs: Self) {
        self.books = self.books.saturating_add(rhs.books);
        self.price_changes = self.price_changes.saturating_add(rhs.price_changes);
        self.best_bid_asks = self.best_bid_asks.saturating_add(rhs.best_bid_asks);
        self.trades = self.trades.saturating_add(rhs.trades);
        self.last_trade_prices = self.last_trade_prices.saturating_add(rhs.last_trade_prices);
        self.tick_size_changes = self.tick_size_changes.saturating_add(rhs.tick_size_changes);
        self.inline_rtds = self.inline_rtds.saturating_add(rhs.inline_rtds);
        self.price_change_entries = self
            .price_change_entries
            .saturating_add(rhs.price_change_entries);
        self.level_upserts = self.level_upserts.saturating_add(rhs.level_upserts);
        self.level_deletes = self.level_deletes.saturating_add(rhs.level_deletes);
        self.bbo_mismatches = self.bbo_mismatches.saturating_add(rhs.bbo_mismatches);
        self.unseeded_deltas = self.unseeded_deltas.saturating_add(rhs.unseeded_deltas);
        self.ignored = self.ignored.saturating_add(rhs.ignored);
        self.unknown = self.unknown.saturating_add(rhs.unknown);
        self.parse_errors = self.parse_errors.saturating_add(rhs.parse_errors);
    }
}

struct ClobWindowMetrics {
    window_started_at: Instant,
    last_data_frame_at: Option<Instant>,
    data_frames: u64,
    frame_bytes: u64,
    events: u64,
    books: u64,
    quotes: u64,
    trades: u64,
    tick_size_changes: u64,
    other_events: u64,
    max_frame_bytes: usize,
    max_events_per_frame: usize,
    max_event_queue_depth: usize,
    bbo_change_snapshots: u64,
    coalesced_snapshots: u64,
    wire: ClobWireCounters,
    ws_sends: u64,
    ws_send_errors: u64,
    ws_send_max_us: u64,
}

impl ClobWindowMetrics {
    fn new(now: Instant) -> Self {
        Self {
            window_started_at: now,
            last_data_frame_at: None,
            data_frames: 0,
            frame_bytes: 0,
            events: 0,
            books: 0,
            quotes: 0,
            trades: 0,
            tick_size_changes: 0,
            other_events: 0,
            max_frame_bytes: 0,
            max_events_per_frame: 0,
            max_event_queue_depth: 0,
            bbo_change_snapshots: 0,
            coalesced_snapshots: 0,
            wire: ClobWireCounters::default(),
            ws_sends: 0,
            ws_send_errors: 0,
            ws_send_max_us: 0,
        }
    }

    fn record_frame(&mut self, now: Instant, frame_bytes: usize, batch: &ClobParsedBatch) {
        if let Some(previous) = self.last_data_frame_at {
            crate::latency::record_ns(
                "polymarket.ws.clob_data_frame_gap",
                now.saturating_duration_since(previous).as_nanos() as u64,
            );
        }
        self.last_data_frame_at = Some(now);
        self.data_frames = self.data_frames.saturating_add(1);
        self.frame_bytes = self.frame_bytes.saturating_add(frame_bytes as u64);
        self.max_frame_bytes = self.max_frame_bytes.max(frame_bytes);
        self.max_events_per_frame = self.max_events_per_frame.max(batch.events.len());
        self.bbo_change_snapshots = self
            .bbo_change_snapshots
            .saturating_add(batch.bbo_change_snapshots as u64);
        self.wire.add(batch.wire);
        self.record_events(&batch.events);
    }

    fn record_events(&mut self, events: &[MarketEvent]) {
        self.events = self.events.saturating_add(events.len() as u64);
        for event in events {
            match event {
                MarketEvent::OrderBook(_) => self.books += 1,
                MarketEvent::Quote(_) => self.quotes += 1,
                MarketEvent::Trade(_) => self.trades += 1,
                MarketEvent::TickSizeChange(_) => self.tick_size_changes += 1,
                _ => self.other_events += 1,
            }
        }
    }

    fn record_coalesced(&mut self, events: &[MarketEvent]) {
        self.coalesced_snapshots = self.coalesced_snapshots.saturating_add(events.len() as u64);
        self.record_events(events);
    }

    fn record_ws_send(&mut self, elapsed: Duration, failed: bool) {
        self.ws_sends = self.ws_sends.saturating_add(1);
        self.ws_send_errors = self.ws_send_errors.saturating_add(u64::from(failed));
        self.ws_send_max_us = self
            .ws_send_max_us
            .max(elapsed.as_micros().min(u64::MAX as u128) as u64);
    }

    fn record_queue_depth(&mut self, depth: usize) {
        self.max_event_queue_depth = self.max_event_queue_depth.max(depth);
    }

    fn log_and_reset(&mut self, now: Instant, queue_depth_now: usize) {
        let window_secs = now
            .saturating_duration_since(self.window_started_at)
            .as_secs_f64();
        info!(
            "[clob_metric] window_secs={:.1} data_frames={} frame_bytes={} events={} books={} quotes={} trades={} tick_size_changes={} other_events={} max_frame_bytes={} max_events_per_frame={} event_queue_depth={} event_queue_high_water={} bbo_change_snapshots={} coalesced_snapshots={} wire_book={} wire_price_change={} wire_best_bid_ask={} wire_trade={} wire_last_trade_price={} wire_tick_size_change={} wire_inline_rtds={} price_change_entries={} level_upserts={} level_deletes={} bbo_mismatches={} unseeded_deltas={} ignored={} unknown={} parse_errors={} ws_sends={} ws_send_errors={} ws_send_max_us={}",
            window_secs,
            self.data_frames,
            self.frame_bytes,
            self.events,
            self.books,
            self.quotes,
            self.trades,
            self.tick_size_changes,
            self.other_events,
            self.max_frame_bytes,
            self.max_events_per_frame,
            queue_depth_now,
            self.max_event_queue_depth,
            self.bbo_change_snapshots,
            self.coalesced_snapshots,
            self.wire.books,
            self.wire.price_changes,
            self.wire.best_bid_asks,
            self.wire.trades,
            self.wire.last_trade_prices,
            self.wire.tick_size_changes,
            self.wire.inline_rtds,
            self.wire.price_change_entries,
            self.wire.level_upserts,
            self.wire.level_deletes,
            self.wire.bbo_mismatches,
            self.wire.unseeded_deltas,
            self.wire.ignored,
            self.wire.unknown,
            self.wire.parse_errors,
            self.ws_sends,
            self.ws_send_errors,
            self.ws_send_max_us,
        );
        let last_data_frame_at = self.last_data_frame_at;
        *self = Self::new(now);
        self.last_data_frame_at = last_data_frame_at;
    }
}

struct ClobBurstMetrics {
    window_started_at: Instant,
    frames: u64,
    bytes: u64,
    max_frame_bytes: usize,
}

impl ClobBurstMetrics {
    fn new(now: Instant) -> Self {
        Self {
            window_started_at: now,
            frames: 0,
            bytes: 0,
            max_frame_bytes: 0,
        }
    }

    fn record_frame(&mut self, bytes: usize) {
        self.frames = self.frames.saturating_add(1);
        self.bytes = self.bytes.saturating_add(bytes as u64);
        self.max_frame_bytes = self.max_frame_bytes.max(bytes);
    }

    fn log_and_reset(&mut self, now: Instant, tcp: TcpSocketMetrics) {
        info!(
            "[clob_1s_metric] window_ms={} frames={} frame_bytes={} max_frame_bytes={} tcp_rcv_space={} tcp_rcv_wnd={} tcp_rcv_ssthresh={} tcp_rcv_wscale={} so_rcvbuf={}",
            now.saturating_duration_since(self.window_started_at).as_millis(),
            self.frames,
            self.bytes,
            self.max_frame_bytes,
            tcp.rcv_space.map(i64::from).unwrap_or(-1),
            tcp.rcv_wnd.map(i64::from).unwrap_or(-1),
            tcp.rcv_ssthresh.map(i64::from).unwrap_or(-1),
            tcp.rcv_wscale.map(i64::from).unwrap_or(-1),
            tcp.so_rcvbuf.map(i64::from).unwrap_or(-1),
        );
        *self = Self::new(now);
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct TcpSocketMetrics {
    rcv_space: Option<u32>,
    rcv_wnd: Option<u32>,
    rcv_ssthresh: Option<u32>,
    rcv_wscale: Option<u8>,
    so_rcvbuf: Option<i32>,
}

// Linux keeps `struct tcp_info` ABI-compatible by appending fields, but the
// Rust `libc::tcp_info` definition can lag behind the running kernel. In
// particular, libc 0.2.186's glibc definition ends at `tcpi_total_retrans`,
// while newer kernels append `tcpi_rcv_wnd` at byte 232. Request the stable
// UAPI prefix as bytes so compiling this crate does not depend on which fields
// the consumer's Cargo.lock happens to expose.
#[cfg(any(target_os = "linux", test))]
const LINUX_TCP_INFO_PREFIX_LEN: usize = 236;
#[cfg(any(target_os = "linux", test))]
const LINUX_TCP_INFO_RCV_WSCALE_OFFSET: usize = 6;
#[cfg(any(target_os = "linux", test))]
const LINUX_TCP_INFO_RCV_SSTHRESH_OFFSET: usize = 64;
#[cfg(any(target_os = "linux", test))]
const LINUX_TCP_INFO_RCV_SPACE_OFFSET: usize = 96;
#[cfg(any(target_os = "linux", test))]
const LINUX_TCP_INFO_RCV_WND_OFFSET: usize = 232;

#[cfg(any(target_os = "linux", test))]
fn linux_tcp_info_u32(info: &[u8], returned_len: usize, offset: usize) -> Option<u32> {
    let end = offset.checked_add(std::mem::size_of::<u32>())?;
    if end > returned_len {
        return None;
    }
    let bytes: [u8; 4] = info.get(offset..end)?.try_into().ok()?;
    Some(u32::from_ne_bytes(bytes))
}

#[cfg(unix)]
fn clob_socket_fd(
    stream: &tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Option<i32> {
    match stream.get_ref() {
        tokio_tungstenite::MaybeTlsStream::Plain(tcp) => Some(tcp.as_raw_fd()),
        tokio_tungstenite::MaybeTlsStream::Rustls(tls) => Some(tls.get_ref().0.as_raw_fd()),
        _ => None,
    }
}

#[cfg(not(unix))]
fn clob_socket_fd(
    _stream: &tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Option<i32> {
    None
}

fn sample_tcp_socket(fd: Option<i32>) -> TcpSocketMetrics {
    let Some(fd) = fd else {
        return TcpSocketMetrics::default();
    };
    let mut metrics = TcpSocketMetrics::default();
    #[cfg(unix)]
    unsafe {
        let mut value: libc::c_int = 0;
        let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        if libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            (&mut value as *mut libc::c_int).cast(),
            &mut len,
        ) == 0
        {
            metrics.so_rcvbuf = Some(value);
        }
    }
    #[cfg(target_os = "linux")]
    unsafe {
        let mut info = [0_u8; LINUX_TCP_INFO_PREFIX_LEN];
        let mut len = info.len() as libc::socklen_t;
        if libc::getsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_INFO,
            info.as_mut_ptr().cast(),
            &mut len,
        ) == 0
        {
            let returned_len = len as usize;
            metrics.rcv_space =
                linux_tcp_info_u32(&info, returned_len, LINUX_TCP_INFO_RCV_SPACE_OFFSET);
            metrics.rcv_wnd =
                linux_tcp_info_u32(&info, returned_len, LINUX_TCP_INFO_RCV_WND_OFFSET);
            metrics.rcv_ssthresh =
                linux_tcp_info_u32(&info, returned_len, LINUX_TCP_INFO_RCV_SSTHRESH_OFFSET);
            metrics.rcv_wscale = info
                .get(LINUX_TCP_INFO_RCV_WSCALE_OFFSET)
                .filter(|_| returned_len > LINUX_TCP_INFO_RCV_WSCALE_OFFSET)
                .map(|scales| (scales >> 4) & 0x0f);
        }
    }
    metrics
}

#[derive(Debug)]
struct ClobDiagnostic {
    key: String,
    detail: String,
}

#[derive(Default)]
struct ClobDiagnosticSampler {
    last_logged: HashMap<String, Instant>,
    suppressed: HashMap<String, u64>,
}

impl ClobDiagnosticSampler {
    fn observe(&mut self, now: Instant, diagnostic: ClobDiagnostic) {
        let should_log = self.last_logged.get(&diagnostic.key).map_or(true, |last| {
            now.saturating_duration_since(*last) >= CLOB_DIAGNOSTIC_SAMPLE_INTERVAL
        });
        if should_log {
            let suppressed = self.suppressed.remove(&diagnostic.key).unwrap_or(0);
            warn!(
                "[clob_event_sample] kind={} suppressed={} detail={}",
                diagnostic.key, suppressed, diagnostic.detail,
            );
            self.last_logged.insert(diagnostic.key, now);
        } else {
            *self.suppressed.entry(diagnostic.key).or_default() += 1;
        }
    }
}

async fn timed_clob_ws_send<S>(
    sink: &mut S,
    msg: Message,
    stage: &'static str,
    metrics: &mut ClobWindowMetrics,
) -> std::result::Result<(), String>
where
    S: futures_util::SinkExt<Message> + Unpin,
    <S as futures_util::Sink<Message>>::Error: std::fmt::Display,
{
    let started_at = Instant::now();
    let result = ws_send(sink, msg).await;
    let elapsed = started_at.elapsed();
    crate::latency::record_ns(stage, elapsed.as_nanos().min(u64::MAX as u128) as u64);
    metrics.record_ws_send(elapsed, result.is_err());
    if elapsed >= Duration::from_millis(10) {
        warn!(
            "[clob_send_metric] stage={} elapsed_us={} failed={}",
            stage,
            elapsed.as_micros(),
            result.is_err(),
        );
    }
    result
}

impl ClobLifecycle {
    /// A successful subscription proves only transport setup, not usable
    /// market state.  Initial startup and every reconnect remain NOT_READY
    /// until a subscribed token produces a valid full book snapshot or a
    /// valid two-sided L1 best-bid/ask update.
    fn subscribed(&mut self) {
        self.subscribed_once = true;
        self.ready = false;
    }

    fn disconnected(&mut self, now: Instant, reason: &str) -> bool {
        if self.not_ready_since.is_none() {
            self.not_ready_since = Some(now);
            self.not_ready_reason = Some(reason.to_string());
        }
        if self.not_ready_announced {
            false
        } else {
            self.ready = false;
            self.not_ready_announced = true;
            true
        }
    }

    fn valid_market_data(&mut self, now: Instant) -> Option<ClobReadyTransition> {
        if self.subscribed_once && !self.ready {
            self.ready = true;
            self.not_ready_announced = false;
            Some(ClobReadyTransition {
                recovery: self
                    .not_ready_since
                    .take()
                    .map(|started| now.saturating_duration_since(started)),
                reason: self.not_ready_reason.take(),
            })
        } else {
            None
        }
    }
}

fn is_usable_subscribed_book_event(event: &MarketEvent, tokens: &[String]) -> bool {
    let symbol = match event {
        MarketEvent::OrderBook(book) => &book.symbol,
        MarketEvent::Quote(quote) => &quote.symbol,
        _ => return false,
    };
    tokens.iter().any(|token| token == symbol)
}

fn has_complete_clob_subscription(tokens: &[String]) -> bool {
    // The subscription frame contains the full token set for every current
    // event. Sending that frame is atomic: there is no per-token ACK in the
    // CLOB protocol. A non-empty set therefore means every token selected for
    // the current event(s) was included, regardless of market cardinality.
    !tokens.is_empty()
}

fn clob_subscription_message(tokens: &[String]) -> serde_json::Value {
    // Keep custom L1 best-bid/ask updates permanently enabled. They are both
    // useful quote inputs and an allowed readiness source after reconnect.
    serde_json::json!({
        "type": "market",
        "assets_ids": tokens,
        "custom_feature_enabled": true,
    })
}

fn announce_clob_not_ready(
    event_tx: &crossbeam_channel::Sender<MarketEvent>,
    lifecycle: &mut ClobLifecycle,
    reason: impl Into<String>,
) {
    let reason = reason.into();
    if lifecycle.disconnected(Instant::now(), &reason) {
        let _ = event_tx.send(MarketEvent::Disconnected {
            exchange: Exchange::Polymarket,
            reason,
        });
    }
}

fn forward_clob_events(
    events: Vec<MarketEvent>,
    event_tx: &crossbeam_channel::Sender<MarketEvent>,
    lifecycle: &mut ClobLifecycle,
    health: &mut WsHealth,
    diagnostics: &mut ClobWindowMetrics,
    books: &ClobLocalBooks,
    tokens: &[String],
    now: Instant,
) -> bool {
    let has_usable_book = events
        .iter()
        .any(|event| is_usable_subscribed_book_event(event, tokens));
    if has_usable_book {
        health.record_usable_book(now);
    }
    for event in events {
        if event_tx.send(event).is_err() {
            return false;
        }
    }
    diagnostics.record_queue_depth(event_tx.len());

    // The local L2 must be seeded for every subscribed token before the
    // strategy is allowed back into READY. A best_bid_ask push alone cannot
    // establish the quantities needed to apply subsequent price deltas.
    if has_usable_book && books.has_all_seeded(tokens) {
        if let Some(transition) = lifecycle.valid_market_data(now) {
            let recovery_ms = transition
                .recovery
                .map(|duration| duration.as_secs_f64() * 1_000.0);
            info!(
                "[Polymarket] CLOB READY after seeded local L2 recovery_ms={} reason={}",
                recovery_ms
                    .map(|value| format!("{value:.3}"))
                    .unwrap_or_else(|| "initial".to_string()),
                transition.reason.as_deref().unwrap_or("initial_startup"),
            );
            if event_tx
                .send(MarketEvent::Connected {
                    exchange: Exchange::Polymarket,
                })
                .is_err()
            {
                return false;
            }
        }
    }
    true
}

async fn clob_ws_task(
    initial_subscription: ClobSubscription,
    event_tx: crossbeam_channel::Sender<MarketEvent>,
    mut ctrl_rx: tokio::sync::mpsc::UnboundedReceiver<WsCtrl>,
    shutdown: Arc<AtomicBool>,
    subscribed_once: Arc<AtomicBool>,
) {
    let mut subscription = initial_subscription;
    let mut backoff = crate::exchange::ReconnectBackoff::new(200, 30_000);
    let mut diagnostic_sampler = ClobDiagnosticSampler::default();
    let was_previously_subscribed = subscribed_once.load(Ordering::Relaxed);
    let mut lifecycle = ClobLifecycle {
        subscribed_once: was_previously_subscribed,
        ready: false,
        // Engine-level reconnects create a fresh task after already placing
        // the feed in NOT_READY. Preserve that recovery state so this task
        // still waits for a valid book.
        not_ready_announced: was_previously_subscribed,
        not_ready_since: was_previously_subscribed.then(Instant::now),
        not_ready_reason: was_previously_subscribed.then(|| "engine reconnect".to_string()),
    };
    // Guard: if we enter with shutdown already latched true we'll exit
    // immediately below — surface it so the silent-reconnect-loop failure
    // mode that shipped on 2026-04-20 is detectable from day-1 logs.
    if shutdown.load(Ordering::Relaxed) {
        warn!("[Polymarket] CLOB task started with shutdown=true — will exit immediately (connect() forgot to reset the flag?)");
    }

    'outer: loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // Drain any buffered ctrl messages — take the latest Resubscribe so
        // we don't churn through stale token lists if rotations piled up.
        loop {
            match ctrl_rx.try_recv() {
                Ok(WsCtrl::Resubscribe(new_subscription)) => {
                    subscription = new_subscription;
                }
                Ok(WsCtrl::Shutdown) => break 'outer,
                Err(_) => break,
            }
        }

        info!(
            "[Polymarket] Connecting to {} ({} tokens)",
            POLYMARKET_WS_URL,
            subscription.tokens.len()
        );
        let stream = match tokio::time::timeout(
            WS_CONNECT_TIMEOUT,
            tokio_tungstenite::connect_async(POLYMARKET_WS_URL),
        )
        .await
        {
            Ok(Ok((s, _))) => s,
            Ok(Err(e)) => {
                announce_clob_not_ready(
                    &event_tx,
                    &mut lifecycle,
                    format!("WS connect failed: {}", e),
                );
                let delay = backoff.next_delay();
                warn!(
                    "[Polymarket] WS connect failed: {}, retry in {:.1}s",
                    e,
                    delay.as_secs_f64()
                );
                tokio::time::sleep(delay).await;
                continue;
            }
            Err(_) => {
                announce_clob_not_ready(
                    &event_tx,
                    &mut lifecycle,
                    format!(
                        "WS connect stalled >{:.0}s",
                        WS_CONNECT_TIMEOUT.as_secs_f64(),
                    ),
                );
                let delay = backoff.next_delay();
                warn!(
                    "[Polymarket] WS connect stalled >{:.0}s (TLS handshake never \
                     completed), retry in {:.1}s",
                    WS_CONNECT_TIMEOUT.as_secs_f64(),
                    delay.as_secs_f64(),
                );
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        backoff.reset();
        let tcp_fd = clob_socket_fd(&stream);
        let (mut write, mut read) = stream.split();
        let connected_at = Instant::now();
        let mut health = WsHealth::new(connected_at);
        let mut diagnostics = ClobWindowMetrics::new(connected_at);
        let mut burst_metrics = ClobBurstMetrics::new(connected_at);
        let mut books = ClobLocalBooks::new(&subscription.canonical_events);

        // Align with the official CLOB SDK: lowercase channel `"market"`
        // (the user channel already uses lowercase `"user"`; the server is
        // case-tolerant) plus `custom_feature_enabled`. Our frame parser
        // drops unknown messages/fields silently, so this can only add data.
        let sub_msg = clob_subscription_message(&subscription.tokens);
        if let Err(e) = timed_clob_ws_send(
            &mut write,
            Message::Text(sub_msg.to_string()),
            "polymarket.ws.clob_send.subscribe",
            &mut diagnostics,
        )
        .await
        {
            announce_clob_not_ready(
                &event_tx,
                &mut lifecycle,
                format!("WS subscribe send failed: {}", e),
            );
            warn!("[Polymarket] WS subscribe send failed: {}", e);
            continue;
        }
        info!(
            "[Polymarket] Subscribed to {} tokens across {} canonical events",
            subscription.tokens.len(),
            subscription.canonical_events.len(),
        );
        if !has_complete_clob_subscription(&subscription.tokens) {
            announce_clob_not_ready(
                &event_tx,
                &mut lifecycle,
                "CLOB subscription has no event tokens",
            );
            warn!(
                "[Polymarket] CLOB subscription has no event tokens; readiness remains NOT_READY",
            );
        } else {
            lifecycle.subscribed();
            subscribed_once.store(true, Ordering::Relaxed);
        }

        let mut ping_interval = tokio::time::interval(POLYMARKET_WS_HEARTBEAT_INTERVAL);
        ping_interval.tick().await; // consume immediate tick
        let mut health_interval = tokio::time::interval(POLYMARKET_WS_HEALTH_LOG_INTERVAL);
        health_interval.tick().await;
        let mut burst_interval = tokio::time::interval(CLOB_BURST_METRIC_INTERVAL);
        burst_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        burst_interval.tick().await;
        let mut coalesce_interval = tokio::time::interval(CLOB_BOOK_COALESCE_INTERVAL);
        coalesce_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        coalesce_interval.tick().await;
        let mut scheduler_probe = tokio::time::interval(CLOB_SCHEDULER_PROBE_INTERVAL);
        scheduler_probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        scheduler_probe.tick().await;

        loop {
            tokio::select! {
                biased;

                ctrl = ctrl_rx.recv() => {
                    match ctrl {
                        Some(WsCtrl::Resubscribe(new_subscription)) => {
                            subscription = new_subscription;
                            announce_clob_not_ready(
                                &event_tx,
                                &mut lifecycle,
                                "CLOB resubscribe requested",
                            );
                            info!("[Polymarket] Resubscribe requested ({} tokens) — reconnecting", subscription.tokens.len());
                            let _ = timed_clob_ws_send(
                                &mut write,
                                Message::Close(None),
                                "polymarket.ws.clob_send.close",
                                &mut diagnostics,
                            ).await;
                            continue 'outer;
                        }
                        Some(WsCtrl::Shutdown) | None => break 'outer,
                    }
                }

                scheduled_at = scheduler_probe.tick() => {
                    crate::latency::record_ns(
                        "polymarket.ws.clob_scheduler_lag",
                        scheduled_at.elapsed().as_nanos() as u64,
                    );
                }

                _ = ping_interval.tick() => {
                    let now = Instant::now();
                    // Send both the CLOB application-level text heartbeat and
                    // a WebSocket protocol Ping frame every 5s.
                    if let Err(e) = timed_clob_ws_send(
                        &mut write,
                        Message::Text("PING".to_string()),
                        "polymarket.ws.clob_send.heartbeat_text",
                        &mut diagnostics,
                    ).await {
                        announce_clob_not_ready(
                            &event_tx,
                            &mut lifecycle,
                            format!("Ping send failed: {}", e),
                        );
                        warn!(
                            "[Polymarket] PING send failed: {}; {}",
                            e,
                            health.clob_summary(now),
                        );
                        break;
                    }
                    if let Err(e) = timed_clob_ws_send(
                        &mut write,
                        Message::Ping(Vec::new()),
                        "polymarket.ws.clob_send.heartbeat_frame",
                        &mut diagnostics,
                    ).await {
                        announce_clob_not_ready(
                            &event_tx,
                            &mut lifecycle,
                            format!("Frame Ping send failed: {}", e),
                        );
                        warn!(
                            "[Polymarket] Frame Ping send failed: {}; {}",
                            e,
                            health.clob_summary(now),
                        );
                        break;
                    }
                }

                _ = coalesce_interval.tick() => {
                    let now = Instant::now();
                    let events = books.flush_due(now, now_ns());
                    if !events.is_empty() {
                        diagnostics.record_coalesced(&events);
                        if !forward_clob_events(
                            events,
                            &event_tx,
                            &mut lifecycle,
                            &mut health,
                            &mut diagnostics,
                            &books,
                            &subscription.tokens,
                            now,
                        ) {
                            break 'outer;
                        }
                    }
                }

                _ = burst_interval.tick() => {
                    let now = Instant::now();
                    burst_metrics.log_and_reset(now, sample_tcp_socket(tcp_fd));
                }

                _ = health_interval.tick() => {
                    let now = Instant::now();
                    diagnostics.log_and_reset(now, event_tx.len());
                    if health.topic_is_stale(now, TOPIC_STALE_WARNING_THRESHOLD) {
                        warn!(
                            "[Polymarket] CLOB topic silent; {}",
                            health.clob_summary(now),
                        );
                    } else if health.usable_book_is_stale(now, TOPIC_STALE_WARNING_THRESHOLD) {
                        warn!(
                            "[Polymarket] CLOB topic active but usable book stale; {}",
                            health.clob_summary(now),
                        );
                    }
                    // Topic-level stall watchdog. Only meaningful while we
                    // actually hold event tokens: between events the CLOB is
                    // legitimately silent, which is exactly why the engine
                    // gates its own data-timeout on `has_active_subscription()`.
                    // `has_complete_clob_subscription` is the in-task equivalent
                    // of that check, so reuse it rather than churning the socket
                    // every 90 s while nothing is trading.
                    if has_complete_clob_subscription(&subscription.tokens)
                        && health.topic_is_stale(now, CLOB_TOPIC_STALL_THRESHOLD)
                    {
                        announce_clob_not_ready(
                            &event_tx,
                            &mut lifecycle,
                            format!(
                                "CLOB no topic frame for {:.0}s",
                                CLOB_TOPIC_STALL_THRESHOLD.as_secs_f64(),
                            ),
                        );
                        warn!(
                            "[Polymarket] CLOB no topic frame for {:.0}s (topic stall watchdog) \
                             — reconnecting; {}",
                            CLOB_TOPIC_STALL_THRESHOLD.as_secs_f64(),
                            health.clob_summary(now),
                        );
                        break;
                    }
                }

                read_result = tokio::time::timeout(CLOB_STALE_THRESHOLD, read.next()) => {
                    let msg = match read_result {
                        Ok(Some(Ok(m))) => m,
                        Ok(Some(Err(e))) => {
                            announce_clob_not_ready(
                                &event_tx,
                                &mut lifecycle,
                                format!("WS read error: {}", e),
                            );
                            let now = Instant::now();
                            warn!(
                                "[Polymarket] WS read error: {} — reconnecting; {}",
                                e,
                                health.clob_summary(now),
                            );
                            break;
                        }
                        Ok(None) => {
                            announce_clob_not_ready(
                                &event_tx,
                                &mut lifecycle,
                                "WS closed",
                            );
                            let now = Instant::now();
                            warn!(
                                "[Polymarket] WS closed — reconnecting; {}",
                                health.clob_summary(now),
                            );
                            break;
                        }
                        Err(_elapsed) => {
                            announce_clob_not_ready(
                                &event_tx,
                                &mut lifecycle,
                                format!(
                                    "CLOB no message for {:.0}s",
                                    CLOB_STALE_THRESHOLD.as_secs_f64(),
                                ),
                            );
                            let now = Instant::now();
                            warn!(
                                "[Polymarket] CLOB no raw frame for {:.0}s (stall watchdog) — reconnecting; {}",
                                CLOB_STALE_THRESHOLD.as_secs_f64(),
                                health.clob_summary(now),
                            );
                            break;
                        }
                    };
                    let received_at = Instant::now();
                    match msg {
                        Message::Text(text) => {
                            // Record only non-Close transport traffic here.
                            // Previously the unconditional call above the
                            // match made every close diagnostic report
                            // `last_raw_frame=0.0s_ago`, hiding the actual
                            // pre-close receive gap.
                            health.record_raw_frame(received_at);
                            // Server answers our text "PING" heartbeat with
                            // "PONG" (and may echo "PING"). These aren't JSON
                            // frames — skip them so parse_clob_frame doesn't
                            // warn on every heartbeat.
                            let body = text.trim();
                            if body.eq_ignore_ascii_case("PONG") {
                                health.record_pong(received_at);
                                continue;
                            }
                            if body.eq_ignore_ascii_case("PING") {
                                let _ = timed_clob_ws_send(
                                    &mut write,
                                    Message::Text("PONG".to_string()),
                                    "polymarket.ws.clob_send.text_pong",
                                    &mut diagnostics,
                                ).await;
                                continue;
                            }
                            let t_parse = crate::latency::Instant::now();
                            let batch = process_clob_frame(
                                &text,
                                &mut books,
                                &subscription.tokens,
                                received_at,
                                now_ns(),
                            );
                            burst_metrics.record_frame(text.len());
                            diagnostics.record_frame(received_at, text.len(), &batch);
                            if batch.recognized_topic {
                                health.record_topic_frame(received_at);
                            }
                            for diagnostic in batch.diagnostics {
                                diagnostic_sampler.observe(received_at, diagnostic);
                            }
                            if !forward_clob_events(
                                batch.events,
                                &event_tx,
                                &mut lifecycle,
                                &mut health,
                                &mut diagnostics,
                                &books,
                                &subscription.tokens,
                                received_at,
                            ) {
                                break 'outer;
                            }
                            // Parse + dispatch latency for the whole
                            // CLOB WS frame (simd-json + typed deser +
                            // all contained items + crossbeam sends).
                            crate::latency::record("polymarket.ws.clob_parse", t_parse);
                        }
                        Message::Ping(payload) => {
                            health.record_raw_frame(received_at);
                            let _ = timed_clob_ws_send(
                                &mut write,
                                Message::Pong(payload),
                                "polymarket.ws.clob_send.frame_pong",
                                &mut diagnostics,
                            ).await;
                        }
                        Message::Pong(_) => {
                            health.record_raw_frame(received_at);
                            health.record_pong(received_at);
                        }
                        Message::Close(reason) => {
                            announce_clob_not_ready(
                                &event_tx,
                                &mut lifecycle,
                                "Server closed WS",
                            );
                            match reason.as_ref() {
                                Some(frame) => warn!(
                                    "[clob_close_metric] code={:?} reason={:?} {}",
                                    frame.code,
                                    frame.reason,
                                    health.clob_summary(received_at),
                                ),
                                None => warn!(
                                    "[clob_close_metric] code=none reason=none {}",
                                    health.clob_summary(received_at),
                                ),
                            }
                            warn!(
                                "[Polymarket] Server closed WS {:?} — reconnecting; {}",
                                reason,
                                health.clob_summary(received_at),
                            );
                            break;
                        }
                        _ => health.record_raw_frame(received_at),
                    }
                }
            }
        }

        // Inner loop broke → reconnect after backoff.
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
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
                if shutdown.load(Ordering::Relaxed) {
                    return;
                }
                if start.elapsed().as_secs() > 30 {
                    backoff.reset();
                }
                let delay = backoff.next_delay();
                warn!(
                    "[RTDS] Error: {}, reconnecting in {:.1}s...",
                    e,
                    delay.as_secs_f64()
                );
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
    let (stream, _) = tokio::time::timeout(
        WS_CONNECT_TIMEOUT,
        tokio_tungstenite::connect_async(POLYMARKET_RTDS_URL),
    )
    .await
    .map_err(|_| {
        anyhow!(
            "RTDS connect stalled >{:.0}s",
            WS_CONNECT_TIMEOUT.as_secs_f64()
        )
    })??;
    let (mut write, mut read) = stream.split();
    let mut health = WsHealth::new(Instant::now());
    let monitors_btc = subscriptions.iter().any(|rtds| {
        let (topic, _) = rtds.topic_and_type();
        matches!(topic, "crypto_prices" | "crypto_prices_chainlink")
            && (rtds.filters.is_empty() || rtds.filters.iter().any(|symbol| is_btc_symbol(symbol)))
    });

    // Build and send subscriptions — ALWAYS one unfiltered subscription
    // per topic, symbols filtered client-side by the `pass` check in the
    // read loop. Server-side `filters` is a trap (observed 2026-07-11 on
    // crypto_prices_chainlink): only ONE subscription per topic is
    // honored (per-symbol entries silently keep the first, drop the
    // rest), and the filtered path itself intermittently goes silently
    // dead — healthy connection, zero pushes — while unfiltered delivers
    // normally in the same window. Topic volume is tiny (crypto_prices ~6
    // symbols, crypto_prices_chainlink ~7, each ~1 msg/s), so client-side
    // filtering costs nothing.
    let mut subs = Vec::new();
    let mut seen_topics = std::collections::HashSet::new();
    for rtds in subscriptions {
        let (topic, typ) = rtds.topic_and_type();
        if seen_topics.insert(topic.to_string()) {
            subs.push(serde_json::json!({"topic": topic, "type": typ}));
        }
    }

    let msg = serde_json::json!({
        "action": "subscribe",
        "subscriptions": subs,
    });
    info!("[RTDS] Subscribe: {}", msg);
    ws_send(&mut write, Message::Text(msg.to_string()))
        .await
        .map_err(|e| anyhow!("RTDS subscribe failed: {}", e))?;

    info!("[RTDS] Connected, {} subscriptions", subscriptions.len());

    let mut ping_interval = tokio::time::interval(POLYMARKET_RTDS_PING_INTERVAL);
    ping_interval.tick().await;
    let mut health_interval = tokio::time::interval(POLYMARKET_WS_HEALTH_LOG_INTERVAL);
    health_interval.tick().await;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }

        tokio::select! {
            biased;
            _ = ping_interval.tick() => {
                let now = Instant::now();
                if let Err(e) = write
                    .send(Message::Text(POLYMARKET_RTDS_PING_PAYLOAD.to_string()))
                    .await
                {
                    return Err(anyhow!(
                        "RTDS ping send failed: {}; {}",
                        e,
                        health.rtds_summary(now),
                    ));
                }
                if let Err(e) = ws_send(&mut write, Message::Ping(Vec::new())).await {
                    return Err(anyhow!(
                        "RTDS frame Ping send failed: {}; {}",
                        e,
                        health.rtds_summary(now),
                    ));
                }
            }
            _ = health_interval.tick() => {
                let now = Instant::now();
                if health.topic_is_stale(now, TOPIC_STALE_WARNING_THRESHOLD) {
                    warn!(
                        "[RTDS] Subscription silent; {}",
                        health.rtds_summary(now),
                    );
                } else if monitors_btc
                    && health.btc_price_is_stale(
                        now,
                        TOPIC_STALE_WARNING_THRESHOLD,
                    )
                {
                    warn!(
                        "[RTDS] BTC price gap; {}",
                        health.rtds_summary(now),
                    );
                }
            }
            read_result = tokio::time::timeout(RTDS_STALE_THRESHOLD, read.next()) => {
                let msg = match read_result {
                    Ok(Some(Ok(m))) => m,
                    Ok(Some(Err(e))) => {
                        let now = Instant::now();
                        return Err(anyhow!(
                            "RTDS read error: {}; {}",
                            e,
                            health.rtds_summary(now),
                        ));
                    }
                    Ok(None) => {
                        let now = Instant::now();
                        return Err(anyhow!(
                            "RTDS stream ended; {}",
                            health.rtds_summary(now),
                        ));
                    }
                    Err(_elapsed) => {
                        let now = Instant::now();
                        return Err(anyhow!(
                            "RTDS no raw frame for {:.0}s (stall watchdog) — forcing reconnect; {}",
                            RTDS_STALE_THRESHOLD.as_secs_f64(),
                            health.rtds_summary(now),
                        ));
                    }
                };
                let received_at = Instant::now();
                health.record_raw_frame(received_at);
                match msg {
                    Message::Ping(payload) => {
                        let _ = ws_send(&mut write, Message::Pong(payload)).await;
                    }
                    Message::Pong(_) => {
                        health.record_pong(received_at);
                    }
                    Message::Close(reason) => {
                        warn!(
                            "[RTDS] Server closed: {:?}; {}",
                            reason,
                            health.rtds_summary(received_at),
                        );
                        return Err(anyhow!("RTDS closed"));
                    }
                    Message::Text(text) => {
                        if text.is_empty() { continue; }
                        let body = text.trim();
                        if body.eq_ignore_ascii_case("PONG") {
                            health.record_pong(received_at);
                            continue;
                        }
                        if body.eq_ignore_ascii_case("PING") {
                            let _ = ws_send(&mut write, Message::Text("PONG".to_string())).await;
                            continue;
                        }
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
                        let source = match topic {
                            "crypto_prices" => "rtds_binance",
                            "crypto_prices_chainlink" => "rtds_chainlink",
                            "equity_prices" => "rtds_pyth",
                            _ => continue,
                        };
                        health.record_topic_frame(received_at);
                        let payload = match data.get("payload") { Some(p) => p, None => continue };
                        let symbol = match payload.get("symbol").and_then(|v| v.as_str()) {
                            Some(s) => s, None => continue,
                        };
                        let price = match payload.get("value").and_then(json_f64) {
                            Some(p) => p, None => continue,
                        };
                        let server_ts_ms = data.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0);
                        if is_btc_symbol(symbol) {
                            health.record_btc_price(received_at);
                        }
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

fn json_f64(value: &serde_json::Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn is_btc_symbol(symbol: &str) -> bool {
    matches!(
        symbol.to_ascii_lowercase().as_str(),
        "btc/usd" | "btcusd" | "btcusdt"
    )
}

// ────────────────────────────────────────────────────────────────
// Typed CLOB WS message schemas (simd-json fast path)
// ────────────────────────────────────────────────────────────────
//
// Each incoming frame is either a single JSON object or a JSON array
// of objects. Each object is either a tagged CLOB event (carrying
// `event_type`: book / trade / last_trade_price / tick_size_change /
// price_change / best_bid_ask) OR an RTDS spot-price record (has `source` + `pair`,
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
    /// Polymarket normally emits stringified milliseconds; accept JSON
    /// numbers too so server timestamps always drive event-level ordering.
    #[serde(default)]
    timestamp: Option<serde_json::Value>,
}

#[derive(serde::Deserialize)]
struct TradeFields {
    asset_id: String,
    price: String,
    size: String,
    side: String, // "BUY" | "SELL"
    #[serde(default)]
    timestamp: Option<serde_json::Value>,
    /// Venue execution identity. A transaction can contain multiple fills, so
    /// its hash alone is deliberately not accepted as a trade id.
    #[serde(default, alias = "trade_id", alias = "tradeId", alias = "executionId")]
    execution_id: Option<String>,
    #[serde(default, alias = "transactionHash")]
    transaction_hash: Option<String>,
    #[serde(default, alias = "logIndex")]
    log_index: Option<serde_json::Value>,
    /// Only this explicit field authorizes consumers to fold complementary
    /// Up/Down public prints into one economic execution.
    #[serde(
        default,
        alias = "mirrorId",
        alias = "mirror_trade_id",
        alias = "mirrorTradeId"
    )]
    mirror_id: Option<String>,
}

#[derive(serde::Deserialize)]
struct TickSizeFields {
    asset_id: String,
    #[serde(default, deserialize_with = "de_str_or_num_f64")]
    old_tick_size: f64,
    #[serde(default, deserialize_with = "de_str_or_num_f64")]
    new_tick_size: f64,
    #[serde(default)]
    timestamp: Option<serde_json::Value>,
}

#[derive(serde::Deserialize)]
struct BestBidAskFields {
    asset_id: String,
    #[serde(default, deserialize_with = "de_opt_str_or_num_f64")]
    best_bid: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_str_or_num_f64")]
    best_ask: Option<f64>,
    /// Polymarket emits timestamps as stringified milliseconds, but accept
    /// JSON numbers as well for wire compatibility across server versions.
    #[serde(default)]
    timestamp: Option<serde_json::Value>,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum WireDecimal {
    String(String),
    Number(serde_json::Number),
}

impl WireDecimal {
    fn decimal(&self) -> Option<Decimal> {
        match self {
            Self::String(value) => Decimal::from_str(value.trim()).ok(),
            Self::Number(value) => Decimal::from_str(&value.to_string()).ok(),
        }
    }
}

#[derive(serde::Deserialize)]
struct PriceChangeEntry {
    asset_id: String,
    price: WireDecimal,
    size: WireDecimal,
    side: String,
    #[serde(default)]
    hash: Option<String>,
    #[serde(default)]
    best_bid: Option<WireDecimal>,
    #[serde(default)]
    best_ask: Option<WireDecimal>,
}

#[derive(serde::Deserialize)]
struct PriceChangeFields {
    #[serde(default)]
    market: Option<String>,
    #[serde(default)]
    price_changes: Vec<PriceChangeEntry>,
    #[serde(default)]
    timestamp: Option<serde_json::Value>,
}

#[derive(Default)]
struct ReportedBbo {
    /// Outer Option means the field was present; inner Option is the
    /// tradeable price after mapping terminal 0/1 sentinels to no level.
    bid: Option<Option<Decimal>>,
    ask: Option<Option<Decimal>>,
}

fn normalize_reported_bbo(price: Decimal) -> Option<Decimal> {
    if price == Decimal::ZERO || price == Decimal::ONE {
        None
    } else {
        Some(price)
    }
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
    PriceChange(PriceChangeFields),
    BestBidAsk(BestBidAskFields),
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum ClobFrame {
    /// Matches anything with `event_type` set to a known variant.
    Tagged(TaggedMessage),
    /// Matches RTDS records inlined on the CLOB socket (no event_type).
    Rtds(InlineRtdsFields),
    /// Preserve unexpected values for rate-limited event-type sampling.
    Unknown(serde_json::Value),
}

/// Deserialize a field that may arrive as a number or a string-encoded
/// number. Defaults to 0.0 on any other shape.
fn de_str_or_num_f64<'de, D>(d: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v = serde_json::Value::deserialize(d)?;
    Ok(match v {
        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
        serde_json::Value::String(s) => s.parse().unwrap_or(0.0),
        _ => 0.0,
    })
}

fn de_opt_str_or_num_f64<'de, D>(d: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v = Option::<serde_json::Value>::deserialize(d)?;
    Ok(match v {
        Some(serde_json::Value::Number(n)) => n.as_f64(),
        Some(serde_json::Value::String(s)) => s.parse().ok(),
        _ => None,
    })
}

#[derive(Debug, Default)]
struct ClobParsedBatch {
    events: Vec<MarketEvent>,
    wire: ClobWireCounters,
    recognized_topic: bool,
    bbo_change_snapshots: usize,
    diagnostics: Vec<ClobDiagnostic>,
}

#[derive(Debug, Clone)]
struct ClobCanonicalRole {
    condition_id: String,
    up_token: String,
    is_down: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ClobBookVersion {
    exchange_timestamp_ns: u64,
    wire_sequence: u64,
}

#[derive(Debug, Default)]
struct ClobLocalBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    exchange_timestamp_ns: u64,
    wire_sequence: u64,
    dirty_since: Option<Instant>,
}

impl ClobLocalBook {
    fn top(&self) -> (Option<Decimal>, Option<Decimal>) {
        (
            self.bids.keys().next_back().copied(),
            self.asks.keys().next().copied(),
        )
    }

    fn is_semantically_valid(&self) -> bool {
        !matches!((self.top().0, self.top().1), (Some(bid), Some(ask)) if bid >= ask)
    }

    fn snapshot(
        &self,
        symbol: String,
        mirror_down: bool,
        local_now: u64,
    ) -> Option<OrderBookSnapshot> {
        let level = |price: Decimal, quantity: Decimal| {
            Some(PriceLevel {
                price: price.to_f64()?,
                quantity: quantity.to_f64()?,
            })
        };
        let (bids, asks): (Vec<_>, Vec<_>) = if mirror_down {
            // A bid to buy Down is an ask to sell Up at 1-p; a Down ask
            // maps to an Up bid. Iteration order remains best-to-worst after
            // the complement transformation.
            let bids = self
                .asks
                .iter()
                .map(|(price, quantity)| level(Decimal::ONE - *price, *quantity))
                .collect::<Option<Vec<_>>>()?;
            let asks = self
                .bids
                .iter()
                .rev()
                .map(|(price, quantity)| level(Decimal::ONE - *price, *quantity))
                .collect::<Option<Vec<_>>>()?;
            (bids, asks)
        } else {
            let bids = self
                .bids
                .iter()
                .rev()
                .map(|(price, quantity)| level(*price, *quantity))
                .collect::<Option<Vec<_>>>()?;
            let asks = self
                .asks
                .iter()
                .map(|(price, quantity)| level(*price, *quantity))
                .collect::<Option<Vec<_>>>()?;
            (bids, asks)
        };
        Some(OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol,
            bids,
            asks,
            exchange_timestamp_ns: self.exchange_timestamp_ns,
            local_timestamp_ns: local_now,
        })
    }
}

#[derive(Debug, Default)]
struct ClobLocalBooks {
    token_books: HashMap<String, ClobLocalBook>,
    roles: HashMap<String, ClobCanonicalRole>,
    canonical_versions: HashMap<String, ClobBookVersion>,
    canonical_books: HashMap<String, OrderBookSnapshot>,
    quote_versions: HashMap<String, ClobBookVersion>,
    wire_sequence: u64,
}

impl ClobLocalBooks {
    fn new(specs: &[CanonicalEventSpec]) -> Self {
        let mut state = Self::default();
        for spec in specs {
            state.roles.insert(
                spec.up_token.clone(),
                ClobCanonicalRole {
                    condition_id: spec.condition_id.clone(),
                    up_token: spec.up_token.clone(),
                    is_down: false,
                },
            );
            state.roles.insert(
                spec.down_token.clone(),
                ClobCanonicalRole {
                    condition_id: spec.condition_id.clone(),
                    up_token: spec.up_token.clone(),
                    is_down: true,
                },
            );
        }
        state
    }

    fn next_sequence(&mut self) -> u64 {
        self.wire_sequence = self.wire_sequence.saturating_add(1);
        self.wire_sequence
    }

    fn has_all_seeded(&self, tokens: &[String]) -> bool {
        !tokens.is_empty()
            && tokens
                .iter()
                .all(|token| self.token_books.contains_key(token))
    }

    fn canonicalize_token(&mut self, token: &str, local_now: u64) -> Option<MarketEvent> {
        let book = self.token_books.get(token)?;
        if !book.is_semantically_valid() {
            return None;
        }
        let version = ClobBookVersion {
            exchange_timestamp_ns: book.exchange_timestamp_ns,
            wire_sequence: book.wire_sequence,
        };
        let role = self.roles.get(token).cloned();
        let (condition_id, symbol, mirror_down) = match role {
            Some(role) => (Some(role.condition_id), role.up_token, role.is_down),
            None => (None, token.to_string(), false),
        };
        if let Some(condition_id) = condition_id.as_ref() {
            if self
                .canonical_versions
                .get(condition_id)
                .is_some_and(|current| version < *current)
            {
                return None;
            }
        }
        let snapshot = book.snapshot(symbol, mirror_down, local_now)?;
        if let Some(condition_id) = condition_id {
            self.canonical_versions
                .insert(condition_id.clone(), version);
            self.canonical_books.insert(condition_id, snapshot.clone());
        }
        Some(MarketEvent::OrderBook(snapshot))
    }

    fn canonical_snapshot_for_token(&self, token: &str) -> Option<MarketEvent> {
        let role = self.roles.get(token)?;
        self.canonical_books
            .get(&role.condition_id)
            .cloned()
            .map(MarketEvent::OrderBook)
    }

    fn canonicalize_quote(&mut self, mut quote: QuoteTick) -> Option<MarketEvent> {
        let sequence = self.next_sequence();
        let Some(role) = self.roles.get(&quote.symbol).cloned() else {
            return Some(MarketEvent::Quote(quote));
        };
        let version = ClobBookVersion {
            exchange_timestamp_ns: quote.exchange_timestamp_ns,
            wire_sequence: sequence,
        };
        if self
            .quote_versions
            .get(&role.condition_id)
            .is_some_and(|current| version < *current)
        {
            return None;
        }
        if role.is_down {
            let down_bid = quote.bid_price;
            let down_ask = quote.ask_price;
            quote.bid_price = 1.0 - down_ask;
            quote.ask_price = 1.0 - down_bid;
        }
        quote.symbol = role.up_token;
        self.quote_versions.insert(role.condition_id, version);
        Some(MarketEvent::Quote(quote))
    }

    fn apply_book(
        &mut self,
        fields: BookFields,
        received_at: Instant,
        local_now: u64,
    ) -> std::result::Result<Option<MarketEvent>, String> {
        let symbol = fields.asset_id;
        if symbol.trim().is_empty() {
            return Err("book has empty asset_id".to_string());
        }
        let exchange_timestamp_ns = timestamp_value_to_ns(fields.timestamp.as_ref(), local_now);
        if self
            .token_books
            .get(&symbol)
            .is_some_and(|current| exchange_timestamp_ns < current.exchange_timestamp_ns)
        {
            return Err(format!(
                "stale book token={} incoming_ts={} current_ts={}",
                symbol, exchange_timestamp_ns, self.token_books[&symbol].exchange_timestamp_ns,
            ));
        }
        let parse_levels = |levels: Vec<BookLevel>| {
            let mut parsed = BTreeMap::new();
            for level in levels {
                let price = Decimal::from_str(level.price.trim())
                    .map_err(|_| format!("invalid price {}", level.price))?;
                let size = Decimal::from_str(level.size.trim())
                    .map_err(|_| format!("invalid size {}", level.size))?;
                if price <= Decimal::ZERO || price >= Decimal::ONE || size <= Decimal::ZERO {
                    return Err(format!("invalid level price={} size={}", price, size));
                }
                parsed.insert(price, size);
            }
            Ok(parsed)
        };
        let bids = parse_levels(fields.bids)?;
        let asks = parse_levels(fields.asks)?;
        let sequence = self.next_sequence();
        let book = ClobLocalBook {
            bids,
            asks,
            exchange_timestamp_ns,
            wire_sequence: sequence,
            dirty_since: None,
        };
        if !book.is_semantically_valid() {
            return Err(format!("crossed book token={symbol}"));
        }
        self.token_books.insert(symbol.clone(), book);
        let _ = received_at;
        // An initial snapshot for the complementary token can be older than
        // the event-level Up snapshot already accepted. Keep the newer event
        // book, but re-emit it once so completion of initial L2 seeding can
        // transition the feed to READY without letting the old Down book win.
        Ok(self
            .canonicalize_token(&symbol, local_now)
            .or_else(|| self.canonical_snapshot_for_token(&symbol)))
    }

    fn apply_price_change(
        &mut self,
        fields: PriceChangeFields,
        received_at: Instant,
        local_now: u64,
        counters: &mut ClobWireCounters,
        diagnostics: &mut Vec<ClobDiagnostic>,
    ) -> (Vec<MarketEvent>, usize) {
        let exchange_timestamp_ns = timestamp_value_to_ns(fields.timestamp.as_ref(), local_now);
        let mut before: HashMap<String, (Option<Decimal>, Option<Decimal>)> = HashMap::new();
        let mut reported_bbo: HashMap<String, ReportedBbo> = HashMap::new();

        for change in fields.price_changes {
            counters.price_change_entries = counters.price_change_entries.saturating_add(1);
            let token = change.asset_id;
            let Some(price) = change.price.decimal() else {
                counters.ignored = counters.ignored.saturating_add(1);
                diagnostics.push(ClobDiagnostic {
                    key: "invalid_price_change".to_string(),
                    detail: format!("token={token} reason=invalid_price"),
                });
                continue;
            };
            let Some(size) = change.size.decimal() else {
                counters.ignored = counters.ignored.saturating_add(1);
                diagnostics.push(ClobDiagnostic {
                    key: "invalid_price_change".to_string(),
                    detail: format!("token={token} reason=invalid_size"),
                });
                continue;
            };
            if price <= Decimal::ZERO || price >= Decimal::ONE || size < Decimal::ZERO {
                counters.ignored = counters.ignored.saturating_add(1);
                diagnostics.push(ClobDiagnostic {
                    key: "invalid_price_change".to_string(),
                    detail: format!("token={token} price={price} size={size}"),
                });
                continue;
            }
            let Some(current_book) = self.token_books.get(&token) else {
                counters.unseeded_deltas = counters.unseeded_deltas.saturating_add(1);
                counters.ignored = counters.ignored.saturating_add(1);
                diagnostics.push(ClobDiagnostic {
                    key: "unseeded_price_change".to_string(),
                    detail: format!("token={token} ts={exchange_timestamp_ns}"),
                });
                continue;
            };
            if exchange_timestamp_ns < current_book.exchange_timestamp_ns {
                counters.ignored = counters.ignored.saturating_add(1);
                diagnostics.push(ClobDiagnostic {
                    key: "stale_price_change".to_string(),
                    detail: format!(
                        "token={token} incoming_ts={} current_ts={}",
                        exchange_timestamp_ns, current_book.exchange_timestamp_ns,
                    ),
                });
                continue;
            }
            let sequence = self.next_sequence();
            let book = self
                .token_books
                .get_mut(&token)
                .expect("book existence checked above");
            if !before.contains_key(&token) {
                before.insert(token.clone(), book.top());
            }
            let side = change.side.trim().to_ascii_uppercase();
            let levels = match side.as_str() {
                "BUY" => &mut book.bids,
                "SELL" => &mut book.asks,
                _ => {
                    counters.ignored = counters.ignored.saturating_add(1);
                    diagnostics.push(ClobDiagnostic {
                        key: "invalid_price_change".to_string(),
                        detail: format!("token={token} reason=unknown_side side={side}"),
                    });
                    continue;
                }
            };
            if size == Decimal::ZERO {
                levels.remove(&price);
                counters.level_deletes = counters.level_deletes.saturating_add(1);
            } else {
                levels.insert(price, size);
                counters.level_upserts = counters.level_upserts.saturating_add(1);
            }
            book.exchange_timestamp_ns = exchange_timestamp_ns;
            // Assign sequence per entry, not per token after the frame. This
            // preserves the server's original price_changes[] order even when
            // Up and Down entries for one event are interleaved.
            book.wire_sequence = sequence;
            book.dirty_since.get_or_insert(received_at);
            let _ = change.hash;

            let reported = reported_bbo.entry(token.clone()).or_default();
            if let Some(value) = change.best_bid.as_ref() {
                match value.decimal() {
                    Some(price) => reported.bid = Some(normalize_reported_bbo(price)),
                    None => diagnostics.push(ClobDiagnostic {
                        key: "invalid_price_change_bbo".to_string(),
                        detail: format!("token={token} side=bid"),
                    }),
                }
            }
            if let Some(value) = change.best_ask.as_ref() {
                match value.decimal() {
                    Some(price) => reported.ask = Some(normalize_reported_bbo(price)),
                    None => diagnostics.push(ClobDiagnostic {
                        key: "invalid_price_change_bbo".to_string(),
                        detail: format!("token={token} side=ask"),
                    }),
                }
            }
        }

        // A price_change message is one atomic wire frame. Its reported BBO
        // describes the completed frame, not every intermediate entry. Apply
        // all entries first, then compare once per token. Boundary prices 0/1
        // are venue sentinels for a missing tradeable side and intentionally
        // match an empty side in the local (0,1)-only L2 book.
        let mut reported_tokens: Vec<_> = reported_bbo.keys().cloned().collect();
        reported_tokens.sort();
        for token in reported_tokens {
            let Some(book) = self.token_books.get(&token) else {
                continue;
            };
            let actual = book.top();
            let expected = &reported_bbo[&token];
            let bid_mismatch = expected.bid.is_some_and(|bid| bid != actual.0);
            let ask_mismatch = expected.ask.is_some_and(|ask| ask != actual.1);
            if bid_mismatch || ask_mismatch {
                counters.bbo_mismatches = counters.bbo_mismatches.saturating_add(1);
                diagnostics.push(ClobDiagnostic {
                    key: "price_change_bbo_mismatch".to_string(),
                    detail: format!(
                        "token={token} expected_bid={:?} expected_ask={:?} actual_bid={:?} actual_ask={:?}",
                        expected.bid, expected.ask, actual.0, actual.1,
                    ),
                });
            }
        }

        let mut immediate = Vec::new();
        let mut touched_order: Vec<_> = before
            .keys()
            .filter_map(|token| {
                self.token_books
                    .get(token)
                    .map(|book| (book.wire_sequence, token.clone()))
            })
            .collect();
        touched_order.sort_by_key(|(sequence, _)| *sequence);
        for (_, token) in touched_order {
            let Some(book) = self.token_books.get_mut(&token) else {
                continue;
            };
            let top_changed = before.get(&token).copied() != Some(book.top());
            if top_changed && book.is_semantically_valid() {
                book.dirty_since = None;
                if let Some(event) = self.canonicalize_token(&token, local_now) {
                    push_latest_order_book(&mut immediate, event);
                }
            }
        }
        let bbo_change_snapshots = immediate.len();
        let _ = fields.market;
        (immediate, bbo_change_snapshots)
    }

    fn flush_due(&mut self, now: Instant, local_now: u64) -> Vec<MarketEvent> {
        let mut due: Vec<_> = self
            .token_books
            .iter()
            .filter_map(|(token, book)| {
                let dirty_since = book.dirty_since?;
                (now.saturating_duration_since(dirty_since) >= CLOB_BOOK_COALESCE_INTERVAL)
                    .then_some((book.wire_sequence, token.clone()))
            })
            .collect();
        due.sort_by_key(|(sequence, _)| *sequence);
        let mut events = Vec::new();
        for (_, token) in due {
            if let Some(book) = self.token_books.get_mut(&token) {
                book.dirty_since = None;
            }
            if let Some(event) = self.canonicalize_token(&token, local_now) {
                push_latest_order_book(&mut events, event);
            }
        }
        events
    }
}

fn push_latest_order_book(events: &mut Vec<MarketEvent>, event: MarketEvent) {
    let MarketEvent::OrderBook(incoming) = &event else {
        events.push(event);
        return;
    };
    if let Some(index) = events.iter().position(|existing| {
        matches!(existing, MarketEvent::OrderBook(book) if book.symbol == incoming.symbol)
    }) {
        // Remove the prior event before appending so the final output order
        // still follows the wire position of the newest event-level book.
        events.remove(index);
    }
    events.push(event);
}

fn subscribed_token(tokens: &[String], token: &str) -> bool {
    tokens.iter().any(|subscribed| subscribed == token)
}

fn diagnostic_preview(value: &serde_json::Value) -> String {
    value.to_string().chars().take(300).collect()
}

fn process_clob_frame(
    text: &str,
    books: &mut ClobLocalBooks,
    tokens: &[String],
    received_at: Instant,
    local_now: u64,
) -> ClobParsedBatch {
    let mut batch = ClobParsedBatch::default();
    if text.is_empty() {
        return batch;
    }
    let mut buf = text.as_bytes().to_vec();
    let is_array = buf.iter().copied().find(|byte| !byte.is_ascii_whitespace()) == Some(b'[');
    let frames = if is_array {
        simd_json::serde::from_slice::<Vec<ClobFrame>>(&mut buf)
    } else {
        simd_json::serde::from_slice::<ClobFrame>(&mut buf).map(|frame| vec![frame])
    };
    let frames = match frames {
        Ok(frames) => frames,
        Err(error) => {
            batch.wire.parse_errors = 1;
            batch.diagnostics.push(ClobDiagnostic {
                key: "parse_error".to_string(),
                detail: format!(
                    "error={} raw={}",
                    error,
                    text.chars().take(300).collect::<String>(),
                ),
            });
            return batch;
        }
    };

    for frame in frames {
        match frame {
            ClobFrame::Tagged(TaggedMessage::Book(fields)) => {
                batch.wire.books = batch.wire.books.saturating_add(1);
                batch.recognized_topic |= subscribed_token(tokens, &fields.asset_id);
                match books.apply_book(fields, received_at, local_now) {
                    Ok(Some(event)) => push_latest_order_book(&mut batch.events, event),
                    Ok(None) => {}
                    Err(detail) => {
                        batch.wire.ignored = batch.wire.ignored.saturating_add(1);
                        batch.diagnostics.push(ClobDiagnostic {
                            key: "invalid_book".to_string(),
                            detail,
                        });
                    }
                }
            }
            ClobFrame::Tagged(TaggedMessage::PriceChange(fields)) => {
                batch.wire.price_changes = batch.wire.price_changes.saturating_add(1);
                batch.recognized_topic |= fields
                    .price_changes
                    .iter()
                    .any(|change| subscribed_token(tokens, &change.asset_id));
                let (events, bbo_snapshots) = books.apply_price_change(
                    fields,
                    received_at,
                    local_now,
                    &mut batch.wire,
                    &mut batch.diagnostics,
                );
                batch.bbo_change_snapshots =
                    batch.bbo_change_snapshots.saturating_add(bbo_snapshots);
                for event in events {
                    push_latest_order_book(&mut batch.events, event);
                }
            }
            ClobFrame::Tagged(TaggedMessage::BestBidAsk(fields)) => {
                batch.wire.best_bid_asks = batch.wire.best_bid_asks.saturating_add(1);
                batch.recognized_topic |= subscribed_token(tokens, &fields.asset_id);
                let exchange_timestamp_ns =
                    timestamp_value_to_ns(fields.timestamp.as_ref(), local_now);
                if is_non_tradeable_bbo(fields.best_bid, fields.best_ask) {
                    // A missing side or an exact 0/1 boundary is the normal
                    // terminal representation of an empty tradeable side.
                    // QuoteTick requires two prices strictly inside (0,1), so
                    // consume this frame without emitting a false warning.
                    batch.wire.ignored = batch.wire.ignored.saturating_add(1);
                    continue;
                }
                match make_quote_event(
                    fields.asset_id,
                    fields.best_bid,
                    fields.best_ask,
                    exchange_timestamp_ns,
                    local_now,
                ) {
                    Some(MarketEvent::Quote(quote)) => {
                        if let Some(event) = books.canonicalize_quote(quote) {
                            batch.events.push(event);
                        }
                    }
                    _ => {
                        batch.wire.ignored = batch.wire.ignored.saturating_add(1);
                        batch.diagnostics.push(ClobDiagnostic {
                            key: "invalid_best_bid_ask".to_string(),
                            detail: format!("ts={exchange_timestamp_ns}"),
                        });
                    }
                }
            }
            ClobFrame::Tagged(TaggedMessage::Trade(fields)) => {
                batch.wire.trades = batch.wire.trades.saturating_add(1);
                batch.recognized_topic |= subscribed_token(tokens, &fields.asset_id);
                match make_trade_event(fields, local_now) {
                    Some(event) => batch.events.push(event),
                    None => batch.wire.ignored = batch.wire.ignored.saturating_add(1),
                }
            }
            ClobFrame::Tagged(TaggedMessage::LastTradePrice(fields)) => {
                batch.wire.last_trade_prices = batch.wire.last_trade_prices.saturating_add(1);
                batch.recognized_topic |= subscribed_token(tokens, &fields.asset_id);
                match make_trade_event(fields, local_now) {
                    Some(event) => batch.events.push(event),
                    None => batch.wire.ignored = batch.wire.ignored.saturating_add(1),
                }
            }
            ClobFrame::Tagged(TaggedMessage::TickSizeChange(fields)) => {
                batch.wire.tick_size_changes = batch.wire.tick_size_changes.saturating_add(1);
                batch.recognized_topic |= subscribed_token(tokens, &fields.asset_id);
                match make_tick_size_event(fields, local_now) {
                    Some(event) => batch.events.push(event),
                    None => batch.wire.ignored = batch.wire.ignored.saturating_add(1),
                }
            }
            ClobFrame::Rtds(fields) => {
                batch.wire.inline_rtds = batch.wire.inline_rtds.saturating_add(1);
                match make_inline_rtds_event(fields, local_now) {
                    Some(event) => batch.events.push(event),
                    None => batch.wire.ignored = batch.wire.ignored.saturating_add(1),
                }
            }
            ClobFrame::Unknown(value) => {
                let event_type = value
                    .get("event_type")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("missing");
                let known_ignored = matches!(event_type, "new_market" | "market_resolved");
                if known_ignored {
                    batch.wire.ignored = batch.wire.ignored.saturating_add(1);
                } else {
                    batch.wire.unknown = batch.wire.unknown.saturating_add(1);
                }
                batch.diagnostics.push(ClobDiagnostic {
                    key: if known_ignored {
                        format!("ignored_{event_type}")
                    } else {
                        format!("unknown_{event_type}")
                    },
                    detail: diagnostic_preview(&value),
                });
            }
        }
    }
    batch
}

/// Stateless compatibility helper used by focused parser tests. Stateful
/// PriceChange behavior is tested through `process_clob_frame` with one
/// persistent `ClobLocalBooks` instance.
#[cfg(test)]
fn parse_clob_frame(text: &str) -> Vec<MarketEvent> {
    let now = now_ns();
    process_clob_frame(
        text,
        &mut ClobLocalBooks::default(),
        &[],
        Instant::now(),
        now,
    )
    .events
}

fn timestamp_value_to_ns(timestamp: Option<&serde_json::Value>, fallback_ns: u64) -> u64 {
    normalized_timestamp_ns(timestamp).unwrap_or(fallback_ns)
}

fn normalized_timestamp_ns(timestamp: Option<&serde_json::Value>) -> Option<u64> {
    let raw = timestamp.and_then(|v| {
        v.as_u64()
            .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
    });
    match raw {
        // The CLOB protocol defines timestamps as Unix milliseconds. Accept
        // already-normalized nanoseconds too, which is useful for internal
        // replay fixtures without losing the exchange/local distinction.
        Some(ts) if ts < 1_000_000_000_000_000 => Some(ts.saturating_mul(1_000_000)),
        Some(ts) => Some(ts),
        None => None,
    }
}

fn is_non_tradeable_bbo(best_bid: Option<f64>, best_ask: Option<f64>) -> bool {
    let in_venue_range = |price: f64| price.is_finite() && (0.0..=1.0).contains(&price);
    if best_bid.is_some_and(|price| !in_venue_range(price))
        || best_ask.is_some_and(|price| !in_venue_range(price))
    {
        return false;
    }

    let bid = best_bid.unwrap_or(0.0);
    let ask = best_ask.unwrap_or(1.0);
    let missing_or_boundary = best_bid.is_none()
        || best_ask.is_none()
        || bid == 0.0
        || bid == 1.0
        || ask == 0.0
        || ask == 1.0;
    missing_or_boundary && bid <= ask
}

fn make_quote_event(
    asset_id: String,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    exchange_ts_ns: u64,
    local_now: u64,
) -> Option<MarketEvent> {
    let bid_price = best_bid?;
    let ask_price = best_ask?;
    if !bid_price.is_finite()
        || !ask_price.is_finite()
        || bid_price <= 0.0
        || bid_price >= 1.0
        || ask_price <= 0.0
        || ask_price >= 1.0
        || bid_price >= ask_price
    {
        return None;
    }
    Some(MarketEvent::Quote(QuoteTick {
        exchange: Exchange::Polymarket,
        symbol: asset_id,
        bid_price,
        // The best_bid_ask and price_change messages do not carry top-level
        // quantities. Zero means unavailable; consumers use only prices.
        bid_qty: 0.0,
        ask_price,
        ask_qty: 0.0,
        exchange_timestamp_ns: exchange_ts_ns,
        local_timestamp_ns: local_now,
    }))
}

fn make_trade_event(t: TradeFields, now: u64) -> Option<MarketEvent> {
    let price: f64 = t.price.parse().ok()?;
    let quantity: f64 = t.size.parse().ok()?;
    if t.asset_id.trim().is_empty()
        || !price.is_finite()
        || price <= 0.0
        || price >= 1.0
        || !quantity.is_finite()
        || quantity <= 0.0
    {
        return None;
    }
    let side = match t.side.trim().to_ascii_uppercase().as_str() {
        "BUY" => Side::Buy,
        "SELL" => Side::Sell,
        _ => return None,
    };
    let exchange_timestamp_ns = timestamp_value_to_ns(t.timestamp.as_ref(), now);
    if exchange_timestamp_ns > now.saturating_add(MAX_PUBLIC_EVENT_FUTURE_SKEW_NS) {
        return None;
    }
    let clean_id = |value: Option<String>| {
        value
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    };
    let log_index = t.log_index.as_ref().and_then(|value| match value {
        serde_json::Value::Number(value) => value.as_u64().map(|value| value.to_string()),
        serde_json::Value::String(value) => {
            let value = value.trim();
            if let Some(hex) = value
                .strip_prefix("0x")
                .or_else(|| value.strip_prefix("0X"))
            {
                u64::from_str_radix(hex, 16)
                    .ok()
                    .map(|value| value.to_string())
            } else {
                value.parse::<u64>().ok().map(|value| value.to_string())
            }
        }
        _ => None,
    });
    let mirror_id = clean_id(t.mirror_id);
    let execution_id = clean_id(t.execution_id).or_else(|| {
        clean_id(t.transaction_hash)
            .zip(log_index)
            .map(|(hash, index)| format!("{hash}:log:{index}"))
    });
    let exchange_trade_id = mirror_id
        .map(|value| format!("mirror:{value}"))
        .or_else(|| execution_id.map(|value| format!("execution:{value}")));
    Some(MarketEvent::Trade(TradeTick {
        exchange: Exchange::Polymarket,
        symbol: t.asset_id,
        exchange_trade_id,
        price,
        quantity,
        side,
        exchange_timestamp_ns,
        local_timestamp_ns: now,
    }))
}

fn make_tick_size_event(t: TickSizeFields, now: u64) -> Option<MarketEvent> {
    if t.asset_id.trim().is_empty()
        || !t.old_tick_size.is_finite()
        || t.old_tick_size <= 0.0
        || t.old_tick_size >= 1.0
        || !t.new_tick_size.is_finite()
        || t.new_tick_size <= 0.0
        || t.new_tick_size >= 1.0
        || t.new_tick_size > t.old_tick_size
    {
        return None;
    }
    // A missing timestamp cannot participate in the model's source high-water
    // mark, so fail closed instead of silently assigning receipt time.
    let exchange_timestamp_ns = normalized_timestamp_ns(t.timestamp.as_ref())?;
    if exchange_timestamp_ns > now.saturating_add(MAX_PUBLIC_EVENT_FUTURE_SKEW_NS) {
        return None;
    }
    Some(MarketEvent::TickSizeChange(TickSizeChange {
        exchange: Exchange::Polymarket,
        symbol: t.asset_id,
        old_tick_size: t.old_tick_size,
        new_tick_size: t.new_tick_size,
        exchange_timestamp_ns,
        local_timestamp_ns: now,
    }))
}

fn make_inline_rtds_event(r: InlineRtdsFields, local_now: u64) -> Option<MarketEvent> {
    let symbol = r.pair.or(r.symbol).or(r.filter)?;
    let price = r.value.or(r.price)?;
    // Normalize timestamp (sec / ms / ns) to ns.
    let ts_raw = r.server_timestamp.or(r.timestamp).and_then(|v| {
        v.as_u64()
            .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
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

        // Spawn the main CLOB async task on its dedicated runtime. Bridge into
        // the sync engine via a crossbeam event channel; take control input
        // (resubscribe / shutdown) via a tokio mpsc.
        let clob_subscription = self.current_clob_subscription();
        let clob_token_count = clob_subscription.tokens.len();
        let (event_tx, event_rx) = crossbeam_channel::unbounded::<MarketEvent>();
        let (ctrl_tx, ctrl_rx) = tokio::sync::mpsc::unbounded_channel::<WsCtrl>();
        self.event_rx = Some(event_rx);
        self.ws_ctrl_tx = Some(ctrl_tx);

        crate::async_rt::clob_handle().spawn(clob_ws_task(
            clob_subscription,
            event_tx,
            ctrl_rx,
            shutdown,
            self.clob_subscribed_once.clone(),
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
            clob_token_count,
            self.rtds_subscriptions.len(),
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
                    let filters: Vec<String> = parts[1]
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect();
                    info!(
                        "[Polymarket] RTDS subscription: source={}, filters={:?}",
                        source, filters,
                    );
                    self.rtds_subscriptions
                        .push(RtdsSubscription { source, filters });
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
                    series_slug,
                    event.title,
                    event.id,
                    event.markets.len()
                );

                let series_idx = self.series.len();

                let active_markets = accepted_binary_markets(&event.markets, true);

                // Register all active market tokens
                let mut symbols_state = Vec::new();
                for condition in &active_markets {
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
                for condition in &active_markets {
                    let mut binary_option: crate::types::BinaryOption = (*condition).clone().into();
                    binary_option.slug = event.slug.clone();
                    binary_option.series_slug = symbol_str
                        .strip_prefix("series:")
                        .unwrap_or(symbol_str)
                        .to_ascii_lowercase();
                    self.pending_events.push_back(MarketEvent::Instrument(
                        crate::types::Instrument::BinaryOption(binary_option),
                    ));
                }

                // Parse end_date for rotation check — use event level end_date,
                // or if not available, set to check every 5 minutes
                let end_ns = parse_date_ns(&event.end_date).unwrap_or(now_ns() + 300_000_000_000); // 5 min default

                let market = MarketState {
                    event_id: event.id.clone(),
                    start_ns: now_ns(),
                    end_ns,
                    symbols: symbols_state,
                };

                let active_count = active_markets.len();
                info!(
                    "[Polymarket] Event series '{}': subscribed to {}/{} active markets, {} tokens",
                    series_slug,
                    active_count,
                    event.markets.len(),
                    market.symbols.len()
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
                    rotation_refresh: None,
                });
            } else {
                // Event slug format: subscribe by slug for price reference (no rotation)
                let event = fetch_event_by_slug(symbol_str)?;
                info!(
                    "[Polymarket] Found event by slug '{}': {} ({} markets)",
                    symbol_str,
                    event.title,
                    event.markets.len()
                );

                let series_idx = self.series.len();

                let accepted_markets = accepted_binary_markets(&event.markets, false);

                // Register all token IDs for WS subscription
                let mut symbols_state = Vec::new();
                for condition in &accepted_markets {
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
                for condition in &accepted_markets {
                    let mut binary_option: crate::types::BinaryOption = (*condition).clone().into();
                    binary_option.slug = event.slug.clone();
                    binary_option.series_slug = symbol_str
                        .strip_prefix("series:")
                        .unwrap_or(symbol_str)
                        .to_ascii_lowercase();
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
                    rotation_refresh: None,
                });
            }
        }

        let total_markets: usize = self.series.iter().map(|s| s.market.symbols.len()).sum();
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

        // Wait for one event from the async WS task. A short blocking receive
        // is wake-driven (the sender unparks us immediately) and avoids the
        // old 100 µs SCHED_FIFO polling loop burning this shared CLOB core.
        // The timeout keeps rotation/readiness watchdogs responsive.
        if let Some(rx) = &self.event_rx {
            match rx.recv_timeout(Duration::from_millis(1)) {
                Ok(mut event) => {
                    self.map_event_symbol(&mut event);
                    return Ok(Some(event));
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => return Ok(None),
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
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

    #[test]
    fn linux_tcp_info_tail_is_parsed_without_libc_field_support() {
        let mut info = [0_u8; LINUX_TCP_INFO_PREFIX_LEN];
        let rcv_space = 256_000_u32;
        let rcv_wnd = 128_000_u32;
        let rcv_ssthresh = 512_000_u32;
        info[LINUX_TCP_INFO_RCV_WSCALE_OFFSET] = 7 << 4;
        info[LINUX_TCP_INFO_RCV_SPACE_OFFSET..LINUX_TCP_INFO_RCV_SPACE_OFFSET + 4]
            .copy_from_slice(&rcv_space.to_ne_bytes());
        info[LINUX_TCP_INFO_RCV_WND_OFFSET..LINUX_TCP_INFO_RCV_WND_OFFSET + 4]
            .copy_from_slice(&rcv_wnd.to_ne_bytes());
        info[LINUX_TCP_INFO_RCV_SSTHRESH_OFFSET..LINUX_TCP_INFO_RCV_SSTHRESH_OFFSET + 4]
            .copy_from_slice(&rcv_ssthresh.to_ne_bytes());

        assert_eq!((info[LINUX_TCP_INFO_RCV_WSCALE_OFFSET] >> 4) & 0x0f, 7);
        assert_eq!(
            linux_tcp_info_u32(
                &info,
                LINUX_TCP_INFO_PREFIX_LEN,
                LINUX_TCP_INFO_RCV_SSTHRESH_OFFSET,
            ),
            Some(rcv_ssthresh)
        );

        assert_eq!(
            linux_tcp_info_u32(
                &info,
                LINUX_TCP_INFO_PREFIX_LEN,
                LINUX_TCP_INFO_RCV_SPACE_OFFSET,
            ),
            Some(rcv_space)
        );
        assert_eq!(
            linux_tcp_info_u32(
                &info,
                LINUX_TCP_INFO_PREFIX_LEN,
                LINUX_TCP_INFO_RCV_WND_OFFSET,
            ),
            Some(rcv_wnd)
        );
        assert_eq!(
            linux_tcp_info_u32(
                &info,
                LINUX_TCP_INFO_RCV_WND_OFFSET,
                LINUX_TCP_INFO_RCV_WND_OFFSET,
            ),
            None,
            "an older kernel's shorter TCP_INFO must leave rcv_wnd unavailable"
        );
    }

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

    fn now() -> u64 {
        chrono::Utc::now().timestamp() as u64
    }

    #[test]
    fn picks_open_event_not_future_one() {
        let n = now();
        let open = mk_event(n - 60, n + 240); // started 60s ago, ends in 4m
        let upcoming = mk_event(n + 1200, n + 1500); // opens in 20m (the live.log case)
        let picked = pick_current_event(vec![upcoming, open.clone()], "btc-updown-5m").unwrap();
        assert_eq!(
            picked.slug, open.slug,
            "must pick the already-open event over a future one"
        );
    }

    #[test]
    fn series_gap_returns_upcoming_not_err() {
        // Reproduces live.log 2026-06-17T20:10: the only event in the series
        // opens ~20m later (20:30). Must return it as pending, not error.
        let n = now();
        let upcoming = mk_event(n + 1186, n + 1186 + 300);
        let picked = pick_current_event(vec![upcoming.clone()], "btc-updown-5m").unwrap();
        assert_eq!(
            picked.slug, upcoming.slug,
            "series gap -> return upcoming event as pending"
        );
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
    fn skips_nearly_expired_cycle_for_next_full_event() {
        let n = now();
        let near_end = mk_event(n - 270, n + 30);
        let next = mk_event(n + 30, n + 330);
        let picked = pick_current_event(vec![near_end, next.clone()], "btc-updown-5m").unwrap();
        assert_eq!(
            picked.slug, next.slug,
            "5m series requires 60s useful lifetime"
        );
    }

    #[test]
    fn remaining_time_guard_scales_and_caps() {
        assert_eq!(min_event_remaining_secs("btc-updown-1m"), 12);
        assert_eq!(min_event_remaining_secs("btc-updown-5m"), 60);
        assert_eq!(min_event_remaining_secs("eth-updown-1h"), 60);
        assert_eq!(min_event_remaining_secs("categorical-market"), 1);
    }

    #[test]
    fn gamma_event_cache_is_keyed_by_event_id_and_selects_earliest_match() {
        let cache = Mutex::new(HashMap::new());
        let inserted_at = Instant::now();
        let base = now();
        let first = mk_event(base, base + 300);
        let second = mk_event(base + 300, base + 600);

        cache_gamma_events_at(
            &cache,
            "series-1",
            &[second.clone(), first.clone(), first.clone()],
            inserted_at,
        );

        assert_eq!(
            cache.lock().unwrap().len(),
            2,
            "duplicate event ids must replace rather than grow the cache",
        );
        let cached = cached_gamma_event_after_at(&cache, "series-1", base + 299, inserted_at)
            .expect("first event should satisfy the strict end-date threshold");
        assert_eq!(cached.id, first.id);

        let cached = cached_gamma_event_after_at(&cache, "series-1", base + 300, inserted_at)
            .expect("second event should be selected once the first no longer qualifies");
        assert_eq!(cached.id, second.id);
        assert!(
            cached_gamma_event_after_at(&cache, "other-series", base, inserted_at).is_none(),
            "cache entries must not leak between series",
        );
    }

    #[test]
    fn gamma_event_cache_expires_without_blocking_on_a_refresh() {
        let cache = Mutex::new(HashMap::new());
        let now_instant = Instant::now();
        let inserted_at = now_instant
            .checked_sub(GAMMA_EVENT_CACHE_TTL + Duration::from_secs(1))
            .unwrap();
        let base = now();
        let event = mk_event(base, base + 300);
        cache_gamma_events_at(&cache, "series-1", &[event], inserted_at);

        assert!(
            cached_gamma_event_after_at(&cache, "series-1", base, now_instant).is_none(),
            "expired entries must fall through to a normal Gamma request",
        );
        assert!(cache.lock().unwrap().is_empty());
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
        assert_eq!(
            picked.slug, e.slug,
            "unknown start -> open (legacy end-only)"
        );
    }

    #[test]
    fn in_flight_rotation_refresh_does_not_block_market_loop() {
        let started_ns = now_ns();
        let (_rotation_tx, rotation_rx) = crossbeam_channel::bounded(1);
        let mut market = PolymarketMarket::new();
        market.series.push(SeriesState {
            name: "series:btc-up-or-down-5m".to_string(),
            interval_minutes: -1,
            market: MarketState {
                event_id: "expired".to_string(),
                start_ns: started_ns.saturating_sub(300_000_000_000),
                end_ns: started_ns.saturating_sub(1),
                symbols: Vec::new(),
            },
            series_id: Some("10684".to_string()),
            next_retry_ns: 0,
            refresh_fail_count: 0,
            refresh_fail_first_ns: 0,
            refresh_idling_logged: false,
            rotation_refresh: Some(RotationRefresh {
                started_ns,
                rx: rotation_rx,
            }),
        });

        let call_started = Instant::now();
        market.check_rotation().unwrap();
        assert!(
            call_started.elapsed() < Duration::from_millis(100),
            "an in-flight Gamma lookup must not stall next_event"
        );
        assert!(market.series[0].rotation_refresh.is_some());
    }

    #[test]
    fn completed_rotation_refresh_replaces_tokens_and_queues_metadata() {
        let started_ns = now_ns();
        let (rotation_tx, rotation_rx) = crossbeam_channel::bounded(1);
        let mut next_event = mk_event(now() - 1, now() + 299);
        next_event.markets.push(PolyMarketInfo {
            id: "new-market".to_string(),
            question: "BTC up or down?".to_string(),
            condition_id: "new-condition".to_string(),
            slug: "new-market".to_string(),
            clob_token_ids: vec!["new-up".to_string(), "new-down".to_string()],
            outcomes: vec!["Up".to_string(), "Down".to_string()],
            outcome_prices: vec!["0.5".to_string(), "0.5".to_string()],
            active: true,
            closed: false,
            volume: 0.0,
            liquidity: 0.0,
            tick_size: 0.01,
            order_min_size: 5.0,
            group_item_title: String::new(),
            event_start_time: String::new(),
            base_fee: 0,
            fee_schedule: FeeSchedule::default(),
        });
        let next_event_id = next_event.id.clone();
        rotation_tx
            .send(Ok(("10684".to_string(), next_event)))
            .unwrap();

        let mut market = PolymarketMarket::new();
        market.token_to_series.insert("old-up".to_string(), 0);
        market.series.push(SeriesState {
            name: "series:btc-up-or-down-5m".to_string(),
            interval_minutes: -1,
            market: MarketState {
                event_id: "expired".to_string(),
                start_ns: started_ns.saturating_sub(300_000_000_000),
                end_ns: started_ns.saturating_sub(1),
                symbols: vec![SymbolState {
                    token_id: "old-up".to_string(),
                    _outcome: "Up".to_string(),
                    _condition_id: "old-condition".to_string(),
                }],
            },
            series_id: Some("10684".to_string()),
            next_retry_ns: 0,
            refresh_fail_count: 0,
            refresh_fail_first_ns: 0,
            refresh_idling_logged: false,
            rotation_refresh: Some(RotationRefresh {
                started_ns,
                rx: rotation_rx,
            }),
        });

        market.check_rotation().unwrap();
        assert_eq!(market.series[0].market.event_id, next_event_id);
        assert!(market.series[0].rotation_refresh.is_none());
        assert!(!market.token_to_series.contains_key("old-up"));
        assert_eq!(market.token_to_series.get("new-up"), Some(&0));
        assert_eq!(market.token_to_series.get("new-down"), Some(&0));
        assert!(matches!(
            market.pending_events.front(),
            Some(MarketEvent::EventStart { .. })
        ));
        assert!(market
            .pending_events
            .iter()
            .any(|event| matches!(event, MarketEvent::Instrument(_))));
    }

    #[test]
    fn parses_best_bid_ask_as_quote_with_server_timestamp() {
        let events = parse_clob_frame(
            r#"{
                "event_type":"best_bid_ask",
                "market":"0xcondition",
                "asset_id":"up-token",
                "best_bid":"0.48",
                "best_ask":"0.52",
                "spread":"0.04",
                "timestamp":"1757908892351"
            }"#,
        );
        assert_eq!(events.len(), 1);
        let MarketEvent::Quote(q) = &events[0] else {
            panic!("best_bid_ask must produce QuoteTick");
        };
        assert_eq!(q.exchange, Exchange::Polymarket);
        assert_eq!(q.symbol, "up-token");
        assert_eq!(q.bid_price, 0.48);
        assert_eq!(q.ask_price, 0.52);
        assert_eq!(q.bid_qty, 0.0);
        assert_eq!(q.ask_qty, 0.0);
        assert_eq!(q.exchange_timestamp_ns, 1_757_908_892_351_000_000);
        assert!(q.local_timestamp_ns > 0);
    }

    #[test]
    fn incomplete_or_invalid_top_is_ignored() {
        let missing_ask = parse_clob_frame(
            r#"{
                "event_type":"best_bid_ask",
                "asset_id":"token",
                "best_bid":"0.48",
                "timestamp":"1757908892351"
            }"#,
        );
        assert!(missing_ask.is_empty());

        let invalid_price = parse_clob_frame(
            r#"{
                "event_type":"best_bid_ask",
                "asset_id":"token",
                "best_bid":"1.1",
                "best_ask":"0.52",
                "timestamp":"1757908892351"
            }"#,
        );
        assert!(invalid_price.is_empty());

        let crossed = parse_clob_frame(
            r#"{
                "event_type":"best_bid_ask",
                "asset_id":"token",
                "best_bid":"0.60",
                "best_ask":"0.50",
                "timestamp":"1757908892351"
            }"#,
        );
        assert!(crossed.is_empty());
    }

    #[test]
    fn terminal_boundary_bbo_is_silent_and_not_a_quote() {
        let mut books = ClobLocalBooks::default();
        let batch = process_clob_frame(
            r#"{
                "event_type":"best_bid_ask",
                "asset_id":"token",
                "best_bid":"0.99",
                "best_ask":"1",
                "timestamp":"1757908892351"
            }"#,
            &mut books,
            &["token".to_string()],
            Instant::now(),
            1_757_908_892_351_000_000,
        );
        assert!(batch.recognized_topic);
        assert!(batch.events.is_empty());
        assert_eq!(batch.wire.ignored, 1);
        assert!(batch.diagnostics.is_empty());
    }

    #[test]
    fn invalid_polymarket_book_is_rejected_as_a_whole() {
        let valid = parse_clob_frame(
            r#"{
                "event_type":"book",
                "asset_id":"token",
                "bids":[{"price":"0.48","size":"10"}],
                "asks":[{"price":"0.52","size":"12"}],
                "timestamp":"1757908892351"
            }"#,
        );
        assert_eq!(valid.len(), 1);

        for invalid in [
            r#"{"event_type":"book","asset_id":"token","bids":[{"price":"0.48","size":"0"}],"asks":[{"price":"0.52","size":"12"}],"timestamp":"1757908892352"}"#,
            r#"{"event_type":"book","asset_id":"token","bids":[{"price":"1","size":"10"}],"asks":[{"price":"0.52","size":"12"}],"timestamp":"1757908892352"}"#,
            r#"{"event_type":"book","asset_id":"token","bids":[{"price":"0.60","size":"10"}],"asks":[{"price":"0.50","size":"12"}],"timestamp":"1757908892352"}"#,
        ] {
            assert!(parse_clob_frame(invalid).is_empty());
        }
    }

    #[test]
    fn public_trade_preserves_source_time_and_rejects_invalid_semantics() {
        let events = parse_clob_frame(
            r#"{"event_type":"last_trade_price","asset_id":"up","price":"0.51","size":"2","side":"BUY","timestamp":"1757908892351","transaction_hash":"0xabc"}"#,
        );
        let MarketEvent::Trade(trade) = &events[0] else {
            panic!("expected trade");
        };
        assert_eq!(trade.exchange_timestamp_ns, 1_757_908_892_351_000_000);
        assert_eq!(
            trade.exchange_trade_id, None,
            "a transaction hash is not a fill id"
        );

        let events = parse_clob_frame(
            r#"{"event_type":"trade","asset_id":"up","price":"0.51","size":"2","side":"BUY","timestamp":"1757908892351","trade_id":"fill-7","transaction_hash":"0xabc"}"#,
        );
        let MarketEvent::Trade(trade) = &events[0] else {
            panic!("expected trade");
        };
        assert_eq!(trade.exchange_trade_id.as_deref(), Some("execution:fill-7"));

        let corrected = parse_clob_frame(
            r#"{"event_type":"trade","asset_id":"up","price":"0.52","size":"2.5","side":"BUY","timestamp":"1757908892352","trade_id":"fill-7","transaction_hash":"0xabc"}"#,
        );
        let MarketEvent::Trade(corrected) = &corrected[0] else {
            panic!("expected trade");
        };
        assert_eq!(
            corrected.exchange_trade_id.as_deref(),
            Some("execution:fill-7"),
            "economic corrections must retain the execution identity",
        );
        assert_eq!(corrected.price, 0.52);
        assert_eq!(corrected.quantity, 2.5);

        let events = parse_clob_frame(
            r#"{"event_type":"trade","asset_id":"up","price":"0.51","size":"2","side":"BUY","timestamp":"1757908892351","transactionHash":"0xabc","logIndex":"0x2"}"#,
        );
        let MarketEvent::Trade(trade) = &events[0] else {
            panic!("expected trade");
        };
        assert_eq!(
            trade.exchange_trade_id.as_deref(),
            Some("execution:0xabc:log:2"),
        );

        let events = parse_clob_frame(
            r#"{"event_type":"trade","asset_id":"down","price":"0.49","size":"2","side":"SELL","timestamp":"1757908892351","trade_id":"down-fill","mirrorTradeId":"pair-9"}"#,
        );
        let MarketEvent::Trade(trade) = &events[0] else {
            panic!("expected trade");
        };
        assert_eq!(trade.exchange_trade_id.as_deref(), Some("mirror:pair-9"));

        for invalid in [
            r#"{"event_type":"trade","asset_id":"up","price":"NaN","size":"2","side":"BUY","timestamp":"1757908892351"}"#,
            r#"{"event_type":"trade","asset_id":"up","price":"0.51","size":"0","side":"BUY","timestamp":"1757908892351"}"#,
            r#"{"event_type":"trade","asset_id":"up","price":"0.51","size":"2","side":"UNKNOWN","timestamp":"1757908892351"}"#,
        ] {
            assert!(parse_clob_frame(invalid).is_empty());
        }
    }

    #[test]
    fn tick_size_change_requires_valid_narrowing_and_source_time() {
        let events = parse_clob_frame(
            r#"{"event_type":"tick_size_change","asset_id":"up","old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"1757908892351"}"#,
        );
        let MarketEvent::TickSizeChange(change) = &events[0] else {
            panic!("expected tick size change");
        };
        assert_eq!(change.exchange_timestamp_ns, 1_757_908_892_351_000_000);
        for invalid in [
            r#"{"event_type":"tick_size_change","asset_id":"up","old_tick_size":"0.01","new_tick_size":"0.001"}"#,
            r#"{"event_type":"tick_size_change","asset_id":"up","old_tick_size":"0.001","new_tick_size":"0.01","timestamp":"1757908892351"}"#,
            r#"{"event_type":"tick_size_change","asset_id":"up","old_tick_size":"0.01","new_tick_size":"NaN","timestamp":"1757908892351"}"#,
        ] {
            assert!(parse_clob_frame(invalid).is_empty());
        }
    }

    fn canonical_event_spec() -> CanonicalEventSpec {
        CanonicalEventSpec {
            condition_id: "condition".to_string(),
            up_token: "up".to_string(),
            down_token: "down".to_string(),
        }
    }

    fn order_book(event: &MarketEvent) -> &OrderBookSnapshot {
        let MarketEvent::OrderBook(book) = event else {
            panic!("expected order book, got {event:?}");
        };
        book
    }

    #[test]
    fn event_book_maps_down_to_up_and_newer_server_timestamp_wins() {
        let tokens = vec!["up".to_string(), "down".to_string()];
        let mut books = ClobLocalBooks::new(&[canonical_event_spec()]);
        let received_at = Instant::now();
        let batch = process_clob_frame(
            r#"[
                {"event_type":"book","asset_id":"up","bids":[{"price":"0.40","size":"10"}],"asks":[{"price":"0.60","size":"11"}],"timestamp":"2000"},
                {"event_type":"book","asset_id":"down","bids":[{"price":"0.30","size":"20"}],"asks":[{"price":"0.70","size":"21"}],"timestamp":"1999"}
            ]"#,
            &mut books,
            &tokens,
            received_at,
            9_000_000_000,
        );

        assert!(batch.recognized_topic);
        assert!(books.has_all_seeded(&tokens));
        assert_eq!(batch.events.len(), 1, "one latest book per event");
        let current = order_book(&batch.events[0]);
        assert_eq!(current.symbol, "up");
        assert_eq!(current.exchange_timestamp_ns, 2_000_000_000);
        assert_eq!(current.bids[0].price, 0.40);
        assert_eq!(current.asks[0].price, 0.60);

        let newer_down = process_clob_frame(
            r#"{
                "event_type":"price_change",
                "market":"condition",
                "price_changes":[
                    {"asset_id":"down","price":"0.65","size":"7","side":"SELL","best_bid":"0.30","best_ask":"0.65"}
                ],
                "timestamp":"2001"
            }"#,
            &mut books,
            &tokens,
            received_at + Duration::from_millis(1),
            9_001_000_000,
        );
        assert_eq!(newer_down.events.len(), 1, "BBO changes emit immediately");
        let mapped = order_book(&newer_down.events[0]);
        assert_eq!(mapped.symbol, "up");
        assert_eq!(mapped.exchange_timestamp_ns, 2_001_000_000);
        assert_eq!(mapped.bids[0].price, 0.35);
        assert_eq!(mapped.bids[0].quantity, 7.0);
        assert_eq!(mapped.asks[0].price, 0.70);
        assert_eq!(mapped.asks[0].quantity, 20.0);

        let stale_up = process_clob_frame(
            r#"{"event_type":"book","asset_id":"up","bids":[{"price":"0.45","size":"99"}],"asks":[{"price":"0.55","size":"99"}],"timestamp":"2000"}"#,
            &mut books,
            &tokens,
            received_at + Duration::from_millis(2),
            9_002_000_000,
        );
        assert_eq!(stale_up.events.len(), 1);
        let still_newer = order_book(&stale_up.events[0]);
        assert_eq!(still_newer.exchange_timestamp_ns, 2_001_000_000);
        assert_eq!(still_newer.bids[0].price, 0.35);
    }

    #[test]
    fn price_change_applies_all_entries_and_coalesces_depth_only_updates() {
        let tokens = vec!["up".to_string()];
        let mut books = ClobLocalBooks::default();
        let received_at = Instant::now();
        let seed = process_clob_frame(
            r#"{"event_type":"book","asset_id":"up","bids":[{"price":"0.40","size":"10"}],"asks":[{"price":"0.60","size":"11"}],"timestamp":"3000"}"#,
            &mut books,
            &tokens,
            received_at,
            10_000_000_000,
        );
        assert_eq!(seed.events.len(), 1);

        let delta = process_clob_frame(
            r#"{
                "event_type":"price_change",
                "market":"condition",
                "price_changes":[
                    {"asset_id":"up","price":"0.20","size":"5","side":"BUY","best_bid":"0.40","best_ask":"0.60"},
                    {"asset_id":"up","price":"0.80","size":"6","side":"SELL","best_bid":"0.40","best_ask":"0.60"}
                ],
                "timestamp":"3001"
            }"#,
            &mut books,
            &tokens,
            received_at + Duration::from_millis(1),
            10_001_000_000,
        );
        assert_eq!(delta.wire.price_change_entries, 2);
        assert_eq!(delta.wire.level_upserts, 2);
        assert!(
            delta.events.is_empty(),
            "unchanged BBO waits for coalescing"
        );
        assert!(books
            .flush_due(received_at + CLOB_BOOK_COALESCE_INTERVAL, 10_250_000_000,)
            .is_empty());
        let flushed = books.flush_due(
            received_at + CLOB_BOOK_COALESCE_INTERVAL + Duration::from_millis(1),
            10_251_000_000,
        );
        assert_eq!(flushed.len(), 1);
        let book = order_book(&flushed[0]);
        assert!(book
            .bids
            .iter()
            .any(|level| level.price == 0.20 && level.quantity == 5.0));
        assert!(book
            .asks
            .iter()
            .any(|level| level.price == 0.80 && level.quantity == 6.0));

        let deletion = process_clob_frame(
            r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.20","size":"0","side":"BUY","best_bid":"0.40","best_ask":"0.60"}],"timestamp":"3002"}"#,
            &mut books,
            &tokens,
            received_at + Duration::from_millis(300),
            10_300_000_000,
        );
        assert_eq!(deletion.wire.level_deletes, 1);
        assert!(deletion.events.is_empty());
        let flushed = books.flush_due(received_at + Duration::from_millis(551), 10_551_000_000);
        assert_eq!(flushed.len(), 1);
        assert!(!order_book(&flushed[0])
            .bids
            .iter()
            .any(|level| level.price == 0.20));
    }

    #[test]
    fn price_change_bbo_is_checked_after_the_complete_frame() {
        let tokens = vec!["up".to_string()];
        let mut books = ClobLocalBooks::default();
        let received_at = Instant::now();
        let seed = process_clob_frame(
            r#"{"event_type":"book","asset_id":"up","bids":[{"price":"0.40","size":"10"}],"asks":[{"price":"0.60","size":"11"}],"timestamp":"6000"}"#,
            &mut books,
            &tokens,
            received_at,
            14_000_000_000,
        );
        assert_eq!(seed.events.len(), 1);

        let delta = process_clob_frame(
            r#"{
                "event_type":"price_change",
                "price_changes":[
                    {"asset_id":"up","price":"0.60","size":"0","side":"SELL","best_bid":"0.40","best_ask":"0.70"},
                    {"asset_id":"up","price":"0.70","size":"9","side":"SELL","best_bid":"0.40","best_ask":"0.70"}
                ],
                "timestamp":"6001"
            }"#,
            &mut books,
            &tokens,
            received_at + Duration::from_millis(1),
            14_001_000_000,
        );
        assert_eq!(delta.wire.bbo_mismatches, 0);
        assert!(delta
            .diagnostics
            .iter()
            .all(|diagnostic| diagnostic.key != "price_change_bbo_mismatch"));
        assert_eq!(order_book(&delta.events[0]).asks[0].price, 0.70);
    }

    #[test]
    fn terminal_price_change_boundary_matches_empty_book_side() {
        let tokens = vec!["up".to_string()];
        let mut books = ClobLocalBooks::default();
        let received_at = Instant::now();
        process_clob_frame(
            r#"{"event_type":"book","asset_id":"up","bids":[{"price":"0.99","size":"10"}],"asks":[{"price":"0.999","size":"11"}],"timestamp":"7000"}"#,
            &mut books,
            &tokens,
            received_at,
            15_000_000_000,
        );

        let terminal = process_clob_frame(
            r#"{
                "event_type":"price_change",
                "price_changes":[
                    {"asset_id":"up","price":"0.999","size":"0","side":"SELL","best_bid":"0.99","best_ask":"1"}
                ],
                "timestamp":"7001"
            }"#,
            &mut books,
            &tokens,
            received_at + Duration::from_millis(1),
            15_001_000_000,
        );
        assert_eq!(terminal.wire.bbo_mismatches, 0);
        assert!(terminal
            .diagnostics
            .iter()
            .all(|diagnostic| diagnostic.key != "price_change_bbo_mismatch"));
        let book = order_book(&terminal.events[0]);
        assert_eq!(book.bids[0].price, 0.99);
        assert!(book.asks.is_empty());
    }

    #[test]
    fn equal_timestamp_up_down_deltas_follow_wire_order() {
        let tokens = vec!["up".to_string(), "down".to_string()];
        let mut books = ClobLocalBooks::new(&[canonical_event_spec()]);
        let received_at = Instant::now();
        let seed = process_clob_frame(
            r#"[
                {"event_type":"book","asset_id":"up","bids":[{"price":"0.40","size":"10"}],"asks":[{"price":"0.60","size":"11"}],"timestamp":"5000"},
                {"event_type":"book","asset_id":"down","bids":[{"price":"0.40","size":"12"}],"asks":[{"price":"0.60","size":"13"}],"timestamp":"5000"}
            ]"#,
            &mut books,
            &tokens,
            received_at,
            12_000_000_000,
        );
        assert_eq!(seed.events.len(), 1);

        let delta = process_clob_frame(
            r#"{
                "event_type":"price_change",
                "price_changes":[
                    {"asset_id":"down","price":"0.55","size":"7","side":"SELL","best_bid":"0.40","best_ask":"0.55"},
                    {"asset_id":"up","price":"0.46","size":"8","side":"BUY","best_bid":"0.46","best_ask":"0.60"}
                ],
                "timestamp":"5001"
            }"#,
            &mut books,
            &tokens,
            received_at + Duration::from_millis(1),
            12_001_000_000,
        );
        assert_eq!(delta.events.len(), 1, "one canonical event book per frame");
        let latest = order_book(&delta.events[0]);
        assert_eq!(latest.symbol, "up");
        assert_eq!(latest.exchange_timestamp_ns, 5_001_000_000);
        assert_eq!(latest.bids[0].price, 0.46, "later Up delta wins tie");
        assert_eq!(latest.bids[0].quantity, 8.0);
    }

    #[test]
    fn unseeded_price_change_is_recognized_but_not_usable() {
        let tokens = vec!["up".to_string()];
        let mut books = ClobLocalBooks::default();
        let batch = process_clob_frame(
            r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.40","size":"2","side":"BUY"}],"timestamp":"4000"}"#,
            &mut books,
            &tokens,
            Instant::now(),
            11_000_000_000,
        );
        assert!(batch.recognized_topic);
        assert!(batch.events.is_empty());
        assert_eq!(batch.wire.price_changes, 1);
        assert_eq!(batch.wire.price_change_entries, 1);
        assert_eq!(batch.wire.unseeded_deltas, 1);
        assert_eq!(batch.wire.ignored, 1);
    }

    #[test]
    fn ignored_and_unknown_event_types_are_counted_separately() {
        let mut books = ClobLocalBooks::default();
        let batch = process_clob_frame(
            r#"[
                {"event_type":"new_market","market":"condition"},
                {"event_type":"future_wire_type","asset_id":"up"}
            ]"#,
            &mut books,
            &["up".to_string()],
            Instant::now(),
            13_000_000_000,
        );
        assert_eq!(batch.wire.ignored, 1);
        assert_eq!(batch.wire.unknown, 1);
        assert_eq!(batch.diagnostics.len(), 2);
    }

    #[test]
    fn binary_market_structure_rejects_ambiguous_token_arrays() {
        let market = |tokens: Vec<&str>, outcomes: Vec<&str>| PolyMarketInfo {
            id: "market".into(),
            question: "question".into(),
            condition_id: "condition".into(),
            slug: "slug".into(),
            clob_token_ids: tokens.into_iter().map(str::to_string).collect(),
            outcomes: outcomes.into_iter().map(str::to_string).collect(),
            outcome_prices: vec!["0.5".into(), "0.5".into()],
            active: true,
            closed: false,
            volume: 0.0,
            liquidity: 0.0,
            tick_size: 0.01,
            order_min_size: 5.0,
            group_item_title: String::new(),
            event_start_time: String::new(),
            base_fee: 0,
            fee_schedule: FeeSchedule::default(),
        };
        assert!(market(vec!["up", "down"], vec!["Up", "Down"])
            .validate_binary_structure()
            .is_ok());
        assert!(market(vec!["up", "up"], vec!["Up", "Down"])
            .validate_binary_structure()
            .is_err());
        assert!(market(vec!["up", "down"], vec!["Up", " up "])
            .validate_binary_structure()
            .is_err());
        assert!(market(vec!["up"], vec!["Up"])
            .validate_binary_structure()
            .is_err());
    }

    #[test]
    fn startup_and_reconnect_wait_for_seeded_local_l2() {
        assert!(!has_complete_clob_subscription(&[]));
        assert!(has_complete_clob_subscription(
            &["only-outcome".to_string()]
        ));
        assert!(has_complete_clob_subscription(&[
            "up".to_string(),
            "down".to_string(),
            "third-outcome".to_string(),
        ]));
        let mut lifecycle = ClobLifecycle::default();
        lifecycle.subscribed();
        assert!(!lifecycle.ready, "subscription alone must remain NOT_READY");

        let disconnected_at = Instant::now();
        assert!(
            lifecycle.disconnected(disconnected_at, "read error"),
            "read error emits NOT_READY once"
        );
        assert!(
            !lifecycle.disconnected(disconnected_at, "second error"),
            "retry failures do not flap NOT_READY"
        );
        lifecycle.subscribed();
        assert!(
            !lifecycle.ready,
            "re-subscribe alone must not restore READY"
        );

        let ready_at = disconnected_at + Duration::from_secs(2);
        let transition = lifecycle
            .valid_market_data(ready_at)
            .expect("seeded local L2 restores READY");
        assert_eq!(transition.recovery, Some(Duration::from_secs(2)));
        assert_eq!(transition.reason.as_deref(), Some("read error"));
        assert!(
            lifecycle.valid_market_data(ready_at).is_none(),
            "subsequent books do not duplicate READY"
        );
    }

    #[test]
    fn clob_subscription_always_enables_custom_l1_updates() {
        let message = clob_subscription_message(&["up".to_string(), "down".to_string()]);
        assert_eq!(message["type"], "market");
        assert_eq!(message["custom_feature_enabled"], true);
    }

    #[test]
    fn engine_level_reconnect_also_waits_for_valid_book() {
        let mut lifecycle = ClobLifecycle {
            subscribed_once: true,
            ready: false,
            not_ready_announced: true,
            not_ready_since: Some(Instant::now()),
            not_ready_reason: Some("engine reconnect".to_string()),
        };
        lifecycle.subscribed();
        assert!(!lifecycle.ready);
        assert!(lifecycle.valid_market_data(Instant::now()).is_some());
    }
}
