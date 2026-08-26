use anyhow::{anyhow, Result};
use arc_swap::{ArcSwap, ArcSwapOption};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, Stream, StreamExt};
use log::{debug, info, warn};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
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
/// Absorb short CLOB frame bursts without closing the TCP receive window while
/// the single-threaded parser processes the preceding frame. Linux doubles
/// the requested value for bookkeeping, so metrics normally report 16 MiB.
const CLOB_SOCKET_RCVBUF_BYTES: libc::c_int = 8 * 1024 * 1024;
/// A redundant CLOB lane is considered safe to promote only when it has been
/// actively drained recently. This rejects a half-open standby while still
/// covering the venue's observed microburst slow-consumer closes.
const CLOB_STANDBY_MAX_RAW_AGE: Duration = Duration::from_secs(15);
const CLOB_STANDBY_RECONNECT_DELAY: Duration = Duration::from_millis(500);
const CLOB_STANDBY_SLOW_CONSUMER_MAX_BACKOFF: Duration = Duration::from_secs(30);
const CLOB_STANDBY_HEALTHY_RESET: Duration = Duration::from_secs(60);
const CLOB_DUAL_SILENCE_WINDOWS: u8 = 2;
const CLOB_RARE_HANDLER_TAIL: Duration = Duration::from_millis(10);
const CLOB_RESOURCE_BASELINE_FRAMES: u64 = 256;

fn clob_standby_is_hot(observed_data: bool, last_raw_at: Instant, now: Instant) -> bool {
    observed_data && now.saturating_duration_since(last_raw_at) <= CLOB_STANDBY_MAX_RAW_AGE
}

fn clob_peers_are_anti_affine(
    active_peer: Option<SocketAddr>,
    standby_peer: Option<SocketAddr>,
) -> bool {
    matches!((active_peer, standby_peer), (Some(active), Some(standby)) if active.ip() != standby.ip())
}

fn clob_standby_slow_consumer_delay(streak: u32, lane_id: u64) -> Duration {
    let exponent = streak.saturating_sub(1).min(6);
    let base_ms = CLOB_STANDBY_RECONNECT_DELAY
        .as_millis()
        .saturating_mul(1_u128 << exponent);
    // Deterministic per-lane jitter avoids a new allocator/RNG dependency and
    // prevents several account feeds from reconnecting on the same boundary.
    let jitter_ms = lane_id.wrapping_mul(1_103_515_245).wrapping_add(12_345) % 251;
    Duration::from_millis(
        base_ms
            .saturating_add(u128::from(jitter_ms))
            .min(CLOB_STANDBY_SLOW_CONSUMER_MAX_BACKOFF.as_millis()) as u64,
    )
}

/// The synchronous Polymarket feed's externally visible phase.  This lives in
/// atomics shared with the engine supervisor, so it remains observable even if
/// the feed thread is blocked inside one phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PolymarketFeedPhase {
    Starting = 0,
    Subscribe = 1,
    Connect = 2,
    Poll = 3,
    Rotation = 4,
    Dispatch = 5,
    Backoff = 6,
    Stopped = 7,
}

impl PolymarketFeedPhase {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Subscribe,
            2 => Self::Connect,
            3 => Self::Poll,
            4 => Self::Rotation,
            5 => Self::Dispatch,
            6 => Self::Backoff,
            7 => Self::Stopped,
            _ => Self::Starting,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Subscribe => "subscribe",
            Self::Connect => "connect",
            Self::Poll => "poll",
            Self::Rotation => "rotation",
            Self::Dispatch => "dispatch",
            Self::Backoff => "backoff",
            Self::Stopped => "stopped",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PolymarketLivenessSnapshot {
    pub active: bool,
    pub phase: PolymarketFeedPhase,
    pub phase_age_ns: u64,
    /// Sticky recovery milestones for the lifetime of one engine worker
    /// generation. They are intentionally not reset by an in-worker reconnect.
    pub connecting_seen: bool,
    pub subscribed_seen: bool,
    pub first_raw_frame_seen: bool,
    pub ready_seen: bool,
    /// Time since this generation first became responsible for an active
    /// subscription. None while legitimately idle between events.
    pub recovery_age_ns: Option<u64>,
    pub raw_frame_age_ns: Option<u64>,
    pub market_data_age_ns: Option<u64>,
    pub feed_loop_age_ns: Option<u64>,
    /// Wall-clock event deadline. Heartbeat ages above are monotonic.
    pub current_event_end_ns: u64,
}

/// Three independent heartbeats plus reconnect control shared between the
/// async CLOB reader, synchronous feed loop, and independent supervisor.
///
/// * raw frame: transport is still delivering frames (including PONG)
/// * market data: a recognized CLOB topic was received
/// * feed loop: the synchronous `next_event`/dispatch loop made progress
pub struct PolymarketLiveness {
    connection_started_ns: AtomicU64,
    last_raw_frame_ns: AtomicU64,
    last_market_data_ns: AtomicU64,
    last_feed_loop_ns: AtomicU64,
    current_event_end_ns: AtomicU64,
    phase: AtomicU8,
    phase_started_ns: AtomicU64,
    connected: AtomicBool,
    active: AtomicBool,
    connecting_seen: AtomicBool,
    subscribed_seen: AtomicBool,
    first_raw_frame_seen: AtomicBool,
    ready_seen: AtomicBool,
    recovery_started_ns: AtomicU64,
    reconnect: ArcSwap<ReconnectControl>,
    clob_abort: ArcSwapOption<tokio::task::AbortHandle>,
}

#[derive(Clone, Default)]
struct ReconnectControl {
    requested: bool,
    reason: String,
}

impl Default for PolymarketLiveness {
    fn default() -> Self {
        let now = clob_monotonic_now_ns().max(1);
        Self {
            connection_started_ns: AtomicU64::new(0),
            last_raw_frame_ns: AtomicU64::new(0),
            last_market_data_ns: AtomicU64::new(0),
            last_feed_loop_ns: AtomicU64::new(now),
            current_event_end_ns: AtomicU64::new(0),
            phase: AtomicU8::new(PolymarketFeedPhase::Starting as u8),
            phase_started_ns: AtomicU64::new(now),
            connected: AtomicBool::new(false),
            active: AtomicBool::new(false),
            connecting_seen: AtomicBool::new(false),
            subscribed_seen: AtomicBool::new(false),
            first_raw_frame_seen: AtomicBool::new(false),
            ready_seen: AtomicBool::new(false),
            recovery_started_ns: AtomicU64::new(0),
            reconnect: ArcSwap::from_pointee(ReconnectControl::default()),
            clob_abort: ArcSwapOption::empty(),
        }
    }
}

impl PolymarketLiveness {
    pub fn set_phase(&self, phase: PolymarketFeedPhase) {
        let previous = self.phase.swap(phase as u8, Ordering::AcqRel);
        if previous != phase as u8 {
            self.phase_started_ns
                .store(clob_monotonic_now_ns().max(1), Ordering::Release);
        }
    }

    pub fn heartbeat_feed_loop(&self) {
        self.last_feed_loop_ns
            .store(clob_monotonic_now_ns().max(1), Ordering::Release);
    }

    pub fn snapshot(&self) -> PolymarketLivenessSnapshot {
        self.snapshot_at(clob_monotonic_now_ns())
    }

    fn snapshot_at(&self, now_ns: u64) -> PolymarketLivenessSnapshot {
        let connection_started = self.connection_started_ns.load(Ordering::Acquire);
        let age = |last: u64, fallback: u64| {
            let basis = if last == 0 { fallback } else { last };
            (basis > 0).then_some(now_ns.saturating_sub(basis))
        };
        PolymarketLivenessSnapshot {
            active: self.active.load(Ordering::Acquire),
            phase: PolymarketFeedPhase::from_u8(self.phase.load(Ordering::Acquire)),
            phase_age_ns: now_ns.saturating_sub(self.phase_started_ns.load(Ordering::Acquire)),
            connecting_seen: self.connecting_seen.load(Ordering::Acquire),
            subscribed_seen: self.subscribed_seen.load(Ordering::Acquire),
            first_raw_frame_seen: self.first_raw_frame_seen.load(Ordering::Acquire),
            ready_seen: self.ready_seen.load(Ordering::Acquire),
            recovery_age_ns: age(self.recovery_started_ns.load(Ordering::Acquire), 0),
            raw_frame_age_ns: age(
                self.last_raw_frame_ns.load(Ordering::Acquire),
                connection_started,
            ),
            market_data_age_ns: age(
                self.last_market_data_ns.load(Ordering::Acquire),
                connection_started,
            ),
            feed_loop_age_ns: age(self.last_feed_loop_ns.load(Ordering::Acquire), 0),
            current_event_end_ns: self.current_event_end_ns.load(Ordering::Acquire),
        }
    }

    /// Request exactly one reconnect for the current connection and abort its
    /// async reader immediately. The synchronous feed observes the request and
    /// rebuilds the connection on its next iteration.
    pub fn request_reconnect(&self, reason: impl Into<String>) -> bool {
        let reason = reason.into();
        loop {
            let current = self.reconnect.load_full();
            if current.requested {
                return false;
            }
            let next = Arc::new(ReconnectControl {
                requested: true,
                reason: reason.clone(),
            });
            let observed = self.reconnect.compare_and_swap(&current, next);
            if Arc::ptr_eq(&observed, &current) {
                break;
            }
        }
        if let Some(abort) = self.clob_abort.load_full() {
            abort.abort();
        }
        true
    }

    fn reconnect_reason(&self) -> Option<String> {
        let reconnect = self.reconnect.load();
        reconnect.requested.then(|| reconnect.reason.clone())
    }

    fn begin_connection(&self, active: bool) {
        let now = clob_monotonic_now_ns().max(1);
        self.connection_started_ns.store(now, Ordering::Release);
        self.last_raw_frame_ns.store(0, Ordering::Release);
        self.last_market_data_ns.store(0, Ordering::Release);
        self.reconnect.store(Arc::new(ReconnectControl::default()));
        self.connected.store(true, Ordering::Release);
        self.active.store(active, Ordering::Release);
        if active {
            let _ = self.recovery_started_ns.compare_exchange(
                0,
                now,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }

    fn mark_connecting(&self) {
        self.connecting_seen.store(true, Ordering::Release);
    }

    fn mark_subscribed(&self) {
        self.subscribed_seen.store(true, Ordering::Release);
    }

    pub fn mark_ready(&self) {
        self.ready_seen.store(true, Ordering::Release);
    }

    fn install_abort(&self, abort: tokio::task::AbortHandle) {
        self.clob_abort.store(Some(Arc::new(abort)));
    }

    fn end_connection(&self) {
        self.connected.store(false, Ordering::Release);
        self.active.store(false, Ordering::Release);
        self.connection_started_ns.store(0, Ordering::Release);
        self.clob_abort.store(None);
    }

    fn record_raw_frame(&self, now_ns: u64) {
        self.first_raw_frame_seen.store(true, Ordering::Release);
        self.last_raw_frame_ns
            .store(now_ns.max(1), Ordering::Release);
    }

    fn record_market_data(&self, now_ns: u64) {
        self.last_market_data_ns
            .store(now_ns.max(1), Ordering::Release);
    }

    fn update_subscription(&self, active: bool, current_event_end_ns: u64) {
        let connected = self.connected.load(Ordering::Acquire);
        let active = active && connected;
        let was_active = self.active.swap(active, Ordering::AcqRel);
        if active && !was_active && !self.ready_seen.load(Ordering::Acquire) {
            self.recovery_started_ns
                .store(clob_monotonic_now_ns().max(1), Ordering::Release);
        } else if !active && connected {
            // A connected worker with no event tokens is legitimately idle.
            // Its next event gets a fresh bounded recovery window.
            self.recovery_started_ns.store(0, Ordering::Release);
        }
        self.current_event_end_ns
            .store(current_event_end_ns, Ordering::Release);
    }
}

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
/// Polymarket may split one logical price update across several WebSocket
/// frames carrying the same exchange timestamp.  Give those sibling frames a
/// small quiet window before declaring their advertised BBO irreconcilable.
// Polymarket can split one logical BBO update across adjacent websocket
// frames (including a delayed deletion carrying a newer millisecond
// timestamp).  Three milliseconds was below observed scheduler/network
// jitter and promoted harmless frame reordering into REST repairs.
const CLOB_BBO_SETTLE_INTERVAL: Duration = Duration::from_millis(50);
/// A recovered checkpoint must remain continuously Healthy before strategy
/// callbacks resume taker/requote activity. This is deliberately longer than
/// the 50ms wire-level BBO settle window: repeated one-frame mismatches remain
/// fail-closed immediately, but no longer produce a recovery order burst on
/// every micro-flap.
const CLOB_HEALTH_RECOVERY_STABLE_INTERVAL: Duration = Duration::from_millis(500);
const CLOB_BBO_DIAGNOSTIC_FRAMES: usize = 4;
const CLOB_BBO_MAX_SUPERSEDED_REPAIRS: u8 = 2;
const CLOB_BURST_METRIC_INTERVAL: Duration = Duration::from_secs(1);
/// Sample the kernel receive queue often enough to expose sub-second CLOB
/// bursts without turning socket introspection into a per-frame syscall.
const CLOB_SOCKET_UNREAD_PROBE_INTERVAL: Duration = Duration::from_millis(100);
/// Peak frame/byte rates are retained in fixed 100 ms buckets inside each
/// one-second diagnostic window. This is deliberately identical to the
/// unread probe interval so the two series have the same time boundary.
const CLOB_MICROBURST_BUCKET_INTERVAL: Duration = Duration::from_millis(100);
/// `poll_next` is normally re-armed by the 10 ms scheduler probe even when the
/// venue is quiet. Twice that interval is therefore an actionable poll gap.
const CLOB_SOCKET_POLL_STALL_THRESHOLD: Duration = Duration::from_millis(20);
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
const GAMMA_EVENT_CACHE_CAPACITY: usize = 1_024;
static GAMMA_EVENT_CACHE: OnceLock<ArcSwap<HashMap<String, CachedGammaEvent>>> = OnceLock::new();

#[derive(Clone)]
struct RestFutureEventCandidate {
    series_id: String,
    event: PolymarketEvent,
}

/// Process-wide fan-out from maintenance REST discovery to every live
/// Polymarket market adapter. Dead subscribers are pruned on publish. This is
/// deliberately a channel rather than another cache poll: discovery wakes the
/// feed on its next 1 ms loop and registers the future instrument before the
/// current event expires.
#[derive(Clone)]
struct RestFutureEventSubscriber {
    tx: crossbeam_channel::Sender<RestFutureEventCandidate>,
    replace_rx: crossbeam_channel::Receiver<RestFutureEventCandidate>,
}

static REST_FUTURE_EVENT_SUBSCRIBERS: OnceLock<ArcSwap<Vec<RestFutureEventSubscriber>>> =
    OnceLock::new();

fn subscribe_rest_future_events() -> crossbeam_channel::Receiver<RestFutureEventCandidate> {
    // Discovery is replaceable state. A slow adapter needs only the newest
    // candidate; one-slot mailboxes bound memory and publisher latency.
    let (tx, rx) = crossbeam_channel::bounded(1);
    let subscribers =
        REST_FUTURE_EVENT_SUBSCRIBERS.get_or_init(|| ArcSwap::from_pointee(Vec::new()));
    let subscriber = RestFutureEventSubscriber {
        tx,
        replace_rx: rx.clone(),
    };
    subscribers.rcu(|current| {
        let mut next = (**current).clone();
        next.push(subscriber.clone());
        Arc::new(next)
    });
    rx
}

fn publish_rest_future_event(series_id: &str, event: &PolymarketEvent) {
    let Some(subscribers) = REST_FUTURE_EVENT_SUBSCRIBERS.get() else {
        return;
    };
    let candidate = RestFutureEventCandidate {
        series_id: series_id.to_string(),
        event: event.clone(),
    };
    let mut disconnected = Vec::new();
    for subscriber in subscribers.load().iter() {
        match subscriber.tx.try_send(candidate.clone()) {
            Ok(()) => true,
            Err(crossbeam_channel::TrySendError::Full(candidate)) => {
                let _ = subscriber.replace_rx.try_recv();
                subscriber.tx.try_send(candidate).is_ok()
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                disconnected.push(subscriber.tx.clone());
                false
            }
        };
    }
    if !disconnected.is_empty() {
        subscribers.rcu(|current| {
            Arc::new(
                current
                    .iter()
                    .filter(|subscriber| {
                        !disconnected
                            .iter()
                            .any(|dead| dead.same_channel(&subscriber.tx))
                    })
                    .cloned()
                    .collect(),
            )
        });
    }
}

fn gamma_event_cache() -> &'static ArcSwap<HashMap<String, CachedGammaEvent>> {
    GAMMA_EVENT_CACHE.get_or_init(|| ArcSwap::from_pointee(HashMap::new()))
}

fn cache_entry_is_fresh(entry: &CachedGammaEvent, now: Instant) -> bool {
    now.checked_duration_since(entry.cached_at)
        .map(|age| age <= GAMMA_EVENT_CACHE_TTL)
        .unwrap_or(false)
}

fn cache_gamma_events_at(
    cache: &ArcSwap<HashMap<String, CachedGammaEvent>>,
    series_id: &str,
    events: &[PolymarketEvent],
    now: Instant,
) {
    cache.rcu(|current| {
        let mut next = (**current).clone();
        next.retain(|_, entry| cache_entry_is_fresh(entry, now));
        for event in events {
            if !event.id.is_empty() {
                next.insert(
                    event.id.clone(),
                    CachedGammaEvent {
                        series_id: series_id.to_string(),
                        event: event.clone(),
                        cached_at: now,
                    },
                );
            }
        }
        while next.len() > GAMMA_EVENT_CACHE_CAPACITY {
            let Some(oldest) = next
                .iter()
                .min_by_key(|(_, entry)| entry.cached_at)
                .map(|(id, _)| id.clone())
            else {
                break;
            };
            next.remove(&oldest);
        }
        Arc::new(next)
    });
}

fn cache_gamma_events(series_id: &str, events: &[PolymarketEvent]) {
    cache_gamma_events_at(gamma_event_cache(), series_id, events, Instant::now());
}

fn cached_gamma_event_after_at(
    cache: &ArcSwap<HashMap<String, CachedGammaEvent>>,
    series_id: &str,
    end_date_min_secs: u64,
    now: Instant,
) -> Option<PolymarketEvent> {
    let min_end_ns = end_date_min_secs.saturating_mul(1_000_000_000);
    cache
        .load()
        .values()
        .filter(|entry| cache_entry_is_fresh(entry, now))
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
    fetch_next_event_inner(series_id, end_date_min_secs, true, true)
}

fn fetch_next_event_inner(
    series_id: &str,
    end_date_min_secs: u64,
    allow_cache: bool,
    publish: bool,
) -> Result<Option<PolymarketEvent>> {
    if allow_cache {
        if let Some(event) = cached_gamma_event_after(series_id, end_date_min_secs) {
            info!(
                "[Polymarket] Next event cache hit: series_id={} id={} slug={}",
                series_id, event.id, event.slug,
            );
            if publish {
                publish_rest_future_event(series_id, &event);
            }
            return Ok(Some(event));
        }
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
    if publish {
        publish_rest_future_event(series_id, &event);
    }
    Ok(Some(event))
}

fn event_bounds_secs(event: &PolymarketEvent) -> Option<(u64, u64)> {
    let start = event
        .markets
        .first()
        .and_then(|market| chrono::DateTime::parse_from_rfc3339(&market.event_start_time).ok())
        .and_then(|time| u64::try_from(time.timestamp()).ok())?;
    let end = chrono::DateTime::parse_from_rfc3339(&event.end_date)
        .ok()
        .and_then(|time| u64::try_from(time.timestamp()).ok())?;
    Some((start, end))
}

fn contiguous_event_error(
    event: &PolymarketEvent,
    expected_start_secs: u64,
    expected_duration_secs: u64,
) -> Option<String> {
    let Some((actual_start, actual_end)) = event_bounds_secs(event) else {
        return Some(format!(
            "future event has invalid start/end timestamps: id={} slug={}",
            event.id, event.slug,
        ));
    };
    let expected_end = expected_start_secs.saturating_add(expected_duration_secs);
    if actual_start == expected_start_secs && actual_end == expected_end {
        None
    } else {
        Some(format!(
            "future-event continuity gap: expected_start={} expected_end={} actual_start={} actual_end={} id={} slug={}",
            expected_start_secs,
            expected_end,
            actual_start,
            actual_end,
            event.id,
            event.slug,
        ))
    }
}

/// Resolve only the immediately-following event. A later Gamma candidate is
/// never published to the strategy registration channel as if it were the
/// next rotation; brief publication gaps are retried with uncached reads.
pub fn fetch_contiguous_next_event(
    series_id: &str,
    end_date_min_secs: u64,
    expected_start_secs: u64,
    expected_duration_secs: u64,
) -> Result<Option<PolymarketEvent>> {
    const ATTEMPTS: usize = 5;
    for attempt in 1..=ATTEMPTS {
        let event = fetch_next_event_inner(series_id, end_date_min_secs, attempt == 1, false)?;
        match event {
            Some(event) => {
                if let Some(reason) = contiguous_event_error(
                    &event,
                    expected_start_secs,
                    expected_duration_secs,
                ) {
                    warn!(
                        "[Polymarket] {} attempt={}/{}; retrying exact next event",
                        reason,
                        attempt,
                        ATTEMPTS,
                    );
                } else {
                    publish_rest_future_event(series_id, &event);
                    return Ok(Some(event));
                }
            }
            None => info!(
                "[Polymarket] contiguous future event not published yet expected_start={} attempt={}/{}",
                expected_start_secs,
                attempt,
                ATTEMPTS,
            ),
        }
        if attempt < ATTEMPTS {
            std::thread::sleep(Duration::from_millis(250 * attempt as u64));
        }
    }
    Err(anyhow!(
        "future-event continuity unresolved after {} attempts: series_id={} expected_start={} expected_end={}",
        ATTEMPTS,
        series_id,
        expected_start_secs,
        expected_start_secs.saturating_add(expected_duration_secs),
    ))
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
    /// Establish and L2-seed this current+next union without changing the
    /// logical token set forwarded to strategies.
    Prepare(ClobSubscription),
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
    tick_size: f64,
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
    // Current tick learned from Gamma. Mid-event narrowing is applied from
    // the public tick_size_change stream inside ClobLocalBooks.
    _tick_size: f64,
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

fn clob_monotonic_now_ns() -> u64 {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    ORIGIN
        .get_or_init(Instant::now)
        .elapsed()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

// Public trades, lifecycle and health transitions are ordered/lossless. Book
// and quote snapshots are replaceable: when their lane is saturated the CLOB
// reader evicts the oldest replaceable item and publishes the newest state.
// This keeps a stalled engine from turning a transient burst into an
// unbounded resident-memory queue while preserving the events that cannot be
// reconstructed from a later snapshot.
const CLOB_CRITICAL_EVENT_CAPACITY: usize = 2_048;
const CLOB_REPLACEABLE_EVENT_CAPACITY: usize = 8_192;

#[derive(Clone)]
struct ClobEventSender {
    critical_tx: crossbeam_channel::Sender<ClobEventEnvelope>,
    replaceable_tx: crossbeam_channel::Sender<ClobEventEnvelope>,
    replaceable_evict_rx: crossbeam_channel::Receiver<ClobEventEnvelope>,
    next_sequence: Arc<AtomicU64>,
    critical_overflows: Arc<AtomicU64>,
    replaceable_evictions: Arc<AtomicU64>,
}

struct ClobEventEnvelope {
    sequence: u64,
    event: MarketEvent,
}

struct ClobEventReceiver {
    critical_rx: crossbeam_channel::Receiver<ClobEventEnvelope>,
    replaceable_rx: crossbeam_channel::Receiver<ClobEventEnvelope>,
    pending_critical: Option<ClobEventEnvelope>,
    pending_replaceable: Option<ClobEventEnvelope>,
}

fn clob_event_lanes() -> (ClobEventSender, ClobEventReceiver) {
    let (critical_tx, critical_rx) = crossbeam_channel::bounded(CLOB_CRITICAL_EVENT_CAPACITY);
    let (replaceable_tx, replaceable_rx) =
        crossbeam_channel::bounded(CLOB_REPLACEABLE_EVENT_CAPACITY);
    (
        ClobEventSender {
            critical_tx,
            replaceable_tx,
            replaceable_evict_rx: replaceable_rx.clone(),
            next_sequence: Arc::new(AtomicU64::new(0)),
            critical_overflows: Arc::new(AtomicU64::new(0)),
            replaceable_evictions: Arc::new(AtomicU64::new(0)),
        },
        ClobEventReceiver {
            critical_rx,
            replaceable_rx,
            pending_critical: None,
            pending_replaceable: None,
        },
    )
}

impl ClobEventSender {
    #[inline]
    fn is_replaceable(event: &MarketEvent) -> bool {
        matches!(event, MarketEvent::OrderBook(_) | MarketEvent::Quote(_))
    }

    /// Returns false only when the consumer is gone or the lossless lane
    /// is saturated. The CLOB task then reconnects and re-seeds books instead
    /// of blocking the socket reader or silently losing an ordered event.
    fn send(&self, event: MarketEvent) -> bool {
        let replaceable = Self::is_replaceable(&event);
        let event = ClobEventEnvelope {
            sequence: self.next_sequence.fetch_add(1, Ordering::Relaxed),
            event,
        };
        if !replaceable {
            return match self.critical_tx.try_send(event) {
                Ok(()) => true,
                Err(_) => {
                    self.critical_overflows.fetch_add(1, Ordering::Relaxed);
                    false
                }
            };
        }

        match self.replaceable_tx.try_send(event) {
            Ok(()) => true,
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => false,
            Err(crossbeam_channel::TrySendError::Full(event)) => {
                // There is one CLOB producer. A cloned receiver is retained
                // solely to implement bounded latest-value overflow.
                if self.replaceable_evict_rx.try_recv().is_ok() {
                    self.replaceable_evictions.fetch_add(1, Ordering::Relaxed);
                }
                self.replaceable_tx.try_send(event).is_ok()
            }
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.critical_tx.len() + self.replaceable_tx.len()
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.critical_tx.is_full() || self.replaceable_tx.is_full()
    }

    #[inline]
    fn overflow_totals(&self) -> (u64, u64) {
        (
            self.critical_overflows.load(Ordering::Relaxed),
            self.replaceable_evictions.load(Ordering::Relaxed),
        )
    }
}

impl ClobEventReceiver {
    fn fill_pending(&mut self) {
        if self.pending_critical.is_none() {
            self.pending_critical = self.critical_rx.try_recv().ok();
        }
        if self.pending_replaceable.is_none() {
            self.pending_replaceable = self.replaceable_rx.try_recv().ok();
        }
    }

    fn pop_next(&mut self) -> Option<MarketEvent> {
        match (&self.pending_critical, &self.pending_replaceable) {
            (Some(critical), Some(replaceable)) if critical.sequence <= replaceable.sequence => {
                self.pending_critical.take().map(|envelope| envelope.event)
            }
            (Some(_), Some(_)) => self
                .pending_replaceable
                .take()
                .map(|envelope| envelope.event),
            (Some(_), None) => self.pending_critical.take().map(|envelope| envelope.event),
            (None, Some(_)) => self
                .pending_replaceable
                .take()
                .map(|envelope| envelope.event),
            (None, None) => None,
        }
    }

    fn recv_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<MarketEvent, crossbeam_channel::RecvTimeoutError> {
        self.fill_pending();
        if let Some(event) = self.pop_next() {
            return Ok(event);
        }
        crossbeam_channel::select_biased! {
            recv(self.critical_rx) -> event => {
                self.pending_critical = Some(event.map_err(|_| crossbeam_channel::RecvTimeoutError::Disconnected)?);
            },
            recv(self.replaceable_rx) -> event => {
                self.pending_replaceable = Some(event.map_err(|_| crossbeam_channel::RecvTimeoutError::Disconnected)?);
            },
            default(timeout) => return Err(crossbeam_channel::RecvTimeoutError::Timeout),
        }
        self.fill_pending();
        self.pop_next()
            .ok_or(crossbeam_channel::RecvTimeoutError::Timeout)
    }
}

pub struct PolymarketMarket {
    series: Vec<SeriesState>,
    /// Maps CLOB token_id → index into `series`, so we can tag events with the series symbol.
    token_to_series: HashMap<String, usize>,
    pending_events: VecDeque<MarketEvent>,
    /// Events parsed by the async WS task land here; `next_event()` drains.
    event_rx: Option<ClobEventReceiver>,
    /// Control channel to the async WS task (Resubscribe / Shutdown).
    ws_ctrl_tx: Option<tokio::sync::mpsc::Sender<WsCtrl>>,
    /// Shared shutdown flag — shared between the main CLOB task and RTDS task.
    ws_shutdown: Arc<AtomicBool>,
    /// Persists across engine-level disconnect/connect cycles. Once any CLOB
    /// subscription has been advertised READY, every later task must wait for
    /// a valid book before advertising recovery.
    clob_subscribed_once: Arc<AtomicBool>,
    /// RTDS subscriptions (parsed during subscribe, spawned as task in connect).
    rtds_subscriptions: Vec<RtdsSubscription>,
    /// Sender for RTDS task to push SpotPrice events directly to engine.
    rtds_tx: Option<crate::exchange::PublicMarketPublisher>,
    /// Shared shutdown flag for RTDS task.
    rtds_shutdown: Arc<AtomicBool>,
    /// A rotation wave may complete one series a few milliseconds before its
    /// siblings. Keep the subscription refresh pending until every currently
    /// expired series has finished its in-flight lookup, then send one exact
    /// full-state subscription instead of reconnecting once per series.
    clob_resubscribe_pending: bool,
    /// REST-discovered future events are delivered here immediately, instead
    /// of relying on the maintenance cache still being fresh at rotation.
    rest_future_event_rx: crossbeam_channel::Receiver<RestFutureEventCandidate>,
    future_events: HashMap<String, PolymarketEvent>,
    future_registered_event_ids: HashSet<String>,
    /// Three-heartbeat state shared with the independent engine supervisor.
    liveness: Arc<PolymarketLiveness>,
    /// Abort handle for the current reader; used after an external stall.
    clob_task_abort: Option<tokio::task::AbortHandle>,
    /// A stalled dedicated runtime is bypassed for later reconnects.
    clob_runtime_fallback: bool,
}

impl PolymarketMarket {
    pub fn new() -> Self {
        Self::with_liveness(Arc::new(PolymarketLiveness::default()))
    }

    pub fn with_liveness(liveness: Arc<PolymarketLiveness>) -> Self {
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
            clob_resubscribe_pending: false,
            rest_future_event_rx: subscribe_rest_future_events(),
            future_events: HashMap::new(),
            future_registered_event_ids: HashSet::new(),
            liveness,
            clob_task_abort: None,
            clob_runtime_fallback: false,
        }
    }

    /// Force the CLOB reader onto the general runtime. Engine-level worker
    /// replacements use this because the dedicated reader runtime may be the
    /// component that stopped scheduling; the replacement must not inherit
    /// the same execution path that just stalled.
    pub fn force_clob_runtime_fallback(&mut self) {
        self.clob_runtime_fallback = true;
    }

    /// Set the engine's market_tx and shutdown flag so RTDS task can send events directly.
    pub fn set_market_tx(
        &mut self,
        tx: crate::exchange::PublicMarketPublisher,
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
        let mut by_condition: HashMap<String, (Option<String>, Option<String>, f64)> =
            HashMap::new();
        for symbol in self.series.iter().flat_map(|series| &series.market.symbols) {
            let entry = by_condition.entry(symbol._condition_id.clone()).or_insert((
                None,
                None,
                symbol._tick_size,
            ));
            match symbol._outcome.trim().to_ascii_lowercase().as_str() {
                "up" | "yes" => entry.0 = Some(symbol.token_id.clone()),
                "down" | "no" => entry.1 = Some(symbol.token_id.clone()),
                _ => {}
            }
        }
        let mut canonical_events: Vec<_> = by_condition
            .into_iter()
            .filter_map(|(condition_id, (up_token, down_token, tick_size))| {
                Some(CanonicalEventSpec {
                    condition_id,
                    up_token: up_token?,
                    down_token: down_token?,
                    tick_size,
                })
            })
            .collect();
        canonical_events.sort_by(|left, right| left.condition_id.cmp(&right.condition_id));
        ClobSubscription {
            tokens,
            canonical_events,
        }
    }

    fn current_and_future_clob_subscription(&self) -> ClobSubscription {
        let mut subscription = self.current_clob_subscription();
        for event in self.future_events.values() {
            for market in accepted_binary_markets(&event.markets, true) {
                subscription
                    .tokens
                    .extend(market.clob_token_ids.iter().cloned());
                let mut up_token = None;
                let mut down_token = None;
                for (token, outcome) in market.clob_token_ids.iter().zip(&market.outcomes) {
                    match outcome.trim().to_ascii_lowercase().as_str() {
                        "up" | "yes" => up_token = Some(token.clone()),
                        "down" | "no" => down_token = Some(token.clone()),
                        _ => {}
                    }
                }
                if let (Some(up_token), Some(down_token)) = (up_token, down_token) {
                    subscription.canonical_events.push(CanonicalEventSpec {
                        condition_id: market.condition_id.clone(),
                        up_token,
                        down_token,
                        tick_size: market.tick_size,
                    });
                }
            }
        }
        subscription.tokens.sort();
        subscription.tokens.dedup();
        subscription
            .canonical_events
            .sort_by(|left, right| left.condition_id.cmp(&right.condition_id));
        subscription
            .canonical_events
            .dedup_by(|left, right| left.condition_id == right.condition_id);
        subscription
    }

    fn update_liveness_subscription(&self) {
        let active = !self.current_tokens().is_empty();
        let current_event_end_ns = self
            .series
            .iter()
            .filter(|series| series.interval_minutes == -1 && !series.market.symbols.is_empty())
            .map(|series| series.market.end_ns)
            .min()
            .unwrap_or(0);
        self.liveness
            .update_subscription(active, current_event_end_ns);
    }

    fn fail_supervised_clob_task(&mut self, reason: &str) -> anyhow::Error {
        self.clob_runtime_fallback = true;
        self.ws_shutdown.store(true, Ordering::Relaxed);
        if let Some(tx) = self.ws_ctrl_tx.take() {
            let _ = tx.try_send(WsCtrl::Shutdown);
        }
        if let Some(abort) = self.clob_task_abort.take() {
            abort.abort();
        }
        self.event_rx = None;
        anyhow!("CLOB supervisor forced reconnect on fallback runtime: {reason}")
    }

    fn drain_rest_future_events(&mut self) {
        let mut prewarm_changed = false;
        while let Ok(candidate) = self.rest_future_event_rx.try_recv() {
            let Some(series_idx) = self.series.iter().position(|series| {
                series.series_id.as_deref() == Some(candidate.series_id.as_str())
            }) else {
                continue;
            };
            let current = &self.series[series_idx];
            if candidate.event.id.is_empty() || candidate.event.id == current.market.event_id {
                continue;
            }
            let Ok(candidate_end_ns) = parse_date_ns(&candidate.event.end_date) else {
                continue;
            };
            if candidate_end_ns <= current.market.end_ns {
                continue;
            }
            let expected_start_secs = current.market.end_ns / 1_000_000_000;
            let Some((candidate_start_secs, _)) = event_bounds_secs(&candidate.event) else {
                warn!(
                    "[Polymarket] REST future event rejected: invalid bounds series='{}' event_id={} slug={}",
                    current.name,
                    candidate.event.id,
                    candidate.event.slug,
                );
                continue;
            };
            if candidate_start_secs != expected_start_secs {
                warn!(
                    "[Polymarket] REST future-event continuity gap: series='{}' current_event_id={} expected_start={} actual_start={} candidate_id={} slug={} — registration deferred",
                    current.name,
                    current.market.event_id,
                    expected_start_secs,
                    candidate_start_secs,
                    candidate.event.id,
                    candidate.event.slug,
                );
                continue;
            }

            let should_replace = self
                .future_events
                .get(&candidate.series_id)
                .and_then(|event| parse_date_ns(&event.end_date).ok())
                .map_or(true, |stored_end_ns| candidate_end_ns < stored_end_ns);
            if should_replace {
                self.future_events
                    .insert(candidate.series_id.clone(), candidate.event.clone());
            }

            // Before expiry, register the next event's token ids with the
            // strategy router immediately. Do not emit EventStart and do not
            // change the live CLOB subscription yet; both remain rotation
            // boundary operations.
            if now_ns() < current.market.end_ns
                && self
                    .future_registered_event_ids
                    .insert(candidate.event.id.clone())
            {
                let series_slug = current
                    .name
                    .strip_prefix("series:")
                    .unwrap_or(&current.name)
                    .to_ascii_lowercase();
                let active_markets = accepted_binary_markets(&candidate.event.markets, true);
                for condition in active_markets {
                    let mut instrument: crate::types::BinaryOption = condition.clone().into();
                    instrument.slug = candidate.event.slug.clone();
                    instrument.series_slug = series_slug.clone();
                    self.pending_events.push_back(MarketEvent::Instrument(
                        crate::types::Instrument::BinaryOption(instrument),
                    ));
                }
                info!(
                    "[Polymarket] REST future event registered before rotation: series='{}' event_id={} slug={} end={}",
                    current.name,
                    candidate.event.id,
                    candidate.event.slug,
                    candidate.event.end_date,
                );
                prewarm_changed = true;
            }
        }
        if prewarm_changed {
            self.prepare_ws_with(self.current_and_future_clob_subscription());
        }
    }

    /// Send a Resubscribe message to the async WS task. No-op if the task
    /// hasn't been started yet (e.g. rotation fires before connect()).
    fn resubscribe_ws(&self) {
        self.resubscribe_ws_with(self.current_clob_subscription());
    }

    fn resubscribe_ws_with(&self, subscription: ClobSubscription) {
        self.send_ws_subscription(WsCtrl::Resubscribe(subscription));
    }

    fn prepare_ws_with(&self, subscription: ClobSubscription) {
        self.send_ws_subscription(WsCtrl::Prepare(subscription));
    }

    fn send_ws_subscription(&self, command: WsCtrl) {
        if let Some(tx) = &self.ws_ctrl_tx {
            if tx.try_send(command).is_err() {
                // A stale resubscription must never win merely because the
                // bounded control lane is full. Fail this generation closed;
                // the supervisor will rebuild it from the current state.
                self.ws_shutdown.store(true, Ordering::Release);
                if let Some(abort) = &self.clob_task_abort {
                    abort.abort();
                }
            }
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
                self.liveness.set_phase(PolymarketFeedPhase::Rotation);
                let series_slug = self.series[i].name["series:".len()..].to_string();
                // Maintenance resolves the next split target through REST up
                // to a minute before rotation and deposits the full Gamma
                // event in the process cache. Promote that discovery directly
                // into the normal Instrument/EventStart path instead of using
                // it only for splitPosition and starting a second lookup here.
                let cached_rest_event = self.series[i].series_id.as_deref().and_then(|series_id| {
                    self.future_events
                        .get(series_id)
                        .filter(|event| {
                            parse_date_ns(&event.end_date)
                                .is_ok_and(|end_ns| end_ns > self.series[i].market.end_ns)
                        })
                        .cloned()
                        .or_else(|| {
                            cached_gamma_event_after(
                                series_id,
                                self.series[i].market.end_ns / 1_000_000_000,
                            )
                        })
                        .map(|event| (series_id.to_string(), event))
                });
                let refresh_result = if let Some((series_id, event)) = cached_rest_event {
                    self.series[i].rotation_refresh = None;
                    info!(
                        "[Polymarket] REST-discovered event promoted to strategy registration: series='{}' event_id={} slug={}",
                        series_slug, event.id, event.slug,
                    );
                    Ok((series_id, event))
                } else {
                    match self.series[i]
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
                    }
                };
                match refresh_result {
                    Ok((series_id, event)) => {
                        info!(
                            "[Polymarket] Event series '{}' refresh: '{}'",
                            series_slug, event.title
                        );
                        self.series[i].series_id = Some(series_id);
                        self.future_events
                            .remove(self.series[i].series_id.as_deref().unwrap_or_default());
                        self.future_registered_event_ids.remove(&event.id);

                        // Publish retirement before the next EventStart and
                        // before any new-token book can reach the router. The
                        // router uses this ordered lifecycle boundary to drop
                        // old token routes and recycle its fixed latest-value
                        // key slots without confusing queued old markers with
                        // the new event.
                        let retired_symbols: Vec<String> = self.series[i]
                            .market
                            .symbols
                            .iter()
                            .map(|symbol| symbol.token_id.clone())
                            .collect();
                        if !retired_symbols.is_empty() {
                            self.pending_events.push_back(MarketEvent::EventEnd {
                                exchange: Exchange::Polymarket,
                                symbol: self.series[i].name.clone(),
                                event_id: self.series[i].market.event_id.clone(),
                                retired_symbols,
                                event_end_ns: self.series[i].market.end_ns,
                            });
                        }

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
                                    _tick_size: condition.tick_size,
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
        self.clob_resubscribe_pending |= rotated;
        let rotation_wave_in_flight = self.series.iter().any(|series| {
            series.interval_minutes == -1
                && now >= series.market.end_ns
                && series.rotation_refresh.is_some()
        });
        if self.clob_resubscribe_pending && !rotation_wave_in_flight {
            self.resubscribe_ws();
            self.clob_resubscribe_pending = false;
        }

        self.update_liveness_subscription();

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
    bbo_transient_recoveries: u64,
    bbo_mismatches: u64,
    bbo_repair_requests: u64,
    bbo_recovery_same_timestamp: u64,
    bbo_recovery_newer_timestamp: u64,
    bbo_recovery_book: u64,
    bbo_recovery_rest: u64,
    bbo_repair_superseded_by_ws: u64,
    bbo_settle_samples: u64,
    bbo_settle_total_us: u64,
    bbo_settle_max_us: u64,
    bbo_recovery_samples: u64,
    bbo_recovery_total_us: u64,
    bbo_recovery_max_us: u64,
    bbo_tick_distance_samples: u64,
    bbo_tick_distance_total: u64,
    bbo_tick_distance_max: u64,
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
        self.bbo_transient_recoveries = self
            .bbo_transient_recoveries
            .saturating_add(rhs.bbo_transient_recoveries);
        self.bbo_mismatches = self.bbo_mismatches.saturating_add(rhs.bbo_mismatches);
        self.bbo_repair_requests = self
            .bbo_repair_requests
            .saturating_add(rhs.bbo_repair_requests);
        self.bbo_recovery_same_timestamp = self
            .bbo_recovery_same_timestamp
            .saturating_add(rhs.bbo_recovery_same_timestamp);
        self.bbo_recovery_newer_timestamp = self
            .bbo_recovery_newer_timestamp
            .saturating_add(rhs.bbo_recovery_newer_timestamp);
        self.bbo_recovery_book = self.bbo_recovery_book.saturating_add(rhs.bbo_recovery_book);
        self.bbo_recovery_rest = self.bbo_recovery_rest.saturating_add(rhs.bbo_recovery_rest);
        self.bbo_repair_superseded_by_ws = self
            .bbo_repair_superseded_by_ws
            .saturating_add(rhs.bbo_repair_superseded_by_ws);
        self.bbo_settle_samples = self
            .bbo_settle_samples
            .saturating_add(rhs.bbo_settle_samples);
        self.bbo_settle_total_us = self
            .bbo_settle_total_us
            .saturating_add(rhs.bbo_settle_total_us);
        self.bbo_settle_max_us = self.bbo_settle_max_us.max(rhs.bbo_settle_max_us);
        self.bbo_recovery_samples = self
            .bbo_recovery_samples
            .saturating_add(rhs.bbo_recovery_samples);
        self.bbo_recovery_total_us = self
            .bbo_recovery_total_us
            .saturating_add(rhs.bbo_recovery_total_us);
        self.bbo_recovery_max_us = self.bbo_recovery_max_us.max(rhs.bbo_recovery_max_us);
        self.bbo_tick_distance_samples = self
            .bbo_tick_distance_samples
            .saturating_add(rhs.bbo_tick_distance_samples);
        self.bbo_tick_distance_total = self
            .bbo_tick_distance_total
            .saturating_add(rhs.bbo_tick_distance_total);
        self.bbo_tick_distance_max = self.bbo_tick_distance_max.max(rhs.bbo_tick_distance_max);
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
    health_healthy: u64,
    health_settling: u64,
    health_repairing: u64,
    health_degraded: u64,
    max_frame_bytes: usize,
    max_events_per_frame: usize,
    max_event_queue_depth: usize,
    bbo_change_snapshots: u64,
    coalesced_snapshots: u64,
    wire: ClobWireCounters,
    ws_sends: u64,
    ws_send_errors: u64,
    ws_send_max_us: u64,
    forward_calls: u64,
    forward_events: u64,
    forward_total_us: u64,
    forward_max_us: u64,
    event_send_max_us: u64,
    event_send_over_1ms: u64,
    event_send_full: u64,
    loop_scheduler_max_us: u64,
    runtime_scheduler_max_us: u64,
    read_handler_max_us: u64,
    parse_apply_max_us: u64,
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
            health_healthy: 0,
            health_settling: 0,
            health_repairing: 0,
            health_degraded: 0,
            max_frame_bytes: 0,
            max_events_per_frame: 0,
            max_event_queue_depth: 0,
            bbo_change_snapshots: 0,
            coalesced_snapshots: 0,
            wire: ClobWireCounters::default(),
            ws_sends: 0,
            ws_send_errors: 0,
            ws_send_max_us: 0,
            forward_calls: 0,
            forward_events: 0,
            forward_total_us: 0,
            forward_max_us: 0,
            event_send_max_us: 0,
            event_send_over_1ms: 0,
            event_send_full: 0,
            loop_scheduler_max_us: 0,
            runtime_scheduler_max_us: 0,
            read_handler_max_us: 0,
            parse_apply_max_us: 0,
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
                MarketEvent::MarketDataHealth(health) => match health.state {
                    MarketDataHealthState::Healthy => self.health_healthy += 1,
                    MarketDataHealthState::Settling => self.health_settling += 1,
                    MarketDataHealthState::Repairing => self.health_repairing += 1,
                    MarketDataHealthState::Degraded => self.health_degraded += 1,
                },
                _ => self.other_events += 1,
            }
        }
    }

    fn record_coalesced(&mut self, events: &[MarketEvent]) {
        self.coalesced_snapshots = self.coalesced_snapshots.saturating_add(events.len() as u64);
        self.record_events(events);
    }

    fn record_deferred(&mut self, batch: &ClobDeferredBatch) {
        self.wire.add(batch.wire);
        self.bbo_change_snapshots = self.bbo_change_snapshots.saturating_add(
            batch
                .events
                .iter()
                .filter(|event| matches!(event, MarketEvent::OrderBook(_)))
                .count() as u64,
        );
        self.record_events(&batch.events);
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

    fn record_event_send(&mut self, elapsed: Duration, was_full: bool) {
        let elapsed_us = elapsed.as_micros().min(u64::MAX as u128) as u64;
        self.event_send_max_us = self.event_send_max_us.max(elapsed_us);
        self.event_send_over_1ms = self
            .event_send_over_1ms
            .saturating_add(u64::from(elapsed >= Duration::from_millis(1)));
        self.event_send_full = self.event_send_full.saturating_add(u64::from(was_full));
    }

    fn record_forward(&mut self, elapsed: Duration, event_count: usize) {
        let elapsed_us = elapsed.as_micros().min(u64::MAX as u128) as u64;
        self.forward_calls = self.forward_calls.saturating_add(1);
        self.forward_events = self.forward_events.saturating_add(event_count as u64);
        self.forward_total_us = self.forward_total_us.saturating_add(elapsed_us);
        self.forward_max_us = self.forward_max_us.max(elapsed_us);
    }

    fn record_loop_scheduler(&mut self, elapsed: Duration) {
        self.loop_scheduler_max_us = self
            .loop_scheduler_max_us
            .max(elapsed.as_micros().min(u64::MAX as u128) as u64);
    }

    fn record_read_handler(&mut self, elapsed: Duration) {
        self.read_handler_max_us = self
            .read_handler_max_us
            .max(elapsed.as_micros().min(u64::MAX as u128) as u64);
    }

    fn record_parse_apply(&mut self, elapsed: Duration) {
        self.parse_apply_max_us = self
            .parse_apply_max_us
            .max(elapsed.as_micros().min(u64::MAX as u128) as u64);
    }

    fn close_summary(&self, now: Instant, queue_depth_now: usize) -> String {
        format!(
            "clob_window_ms={} frames={} frame_bytes={} max_frame_bytes={} events={} forward_calls={} forward_events={} forward_avg_us={:.1} forward_max_us={} event_send_max_us={} event_send_over_1ms={} event_send_full={} event_queue_depth={} event_queue_high_water={} health_healthy={} health_settling={} health_repairing={} health_degraded={} bbo_settle_max_us={} parse_errors={} ws_send_max_us={} loop_scheduler_max_us={} runtime_scheduler_max_us={} read_handler_max_us={} parse_apply_max_us={}",
            now.saturating_duration_since(self.window_started_at).as_millis(),
            self.data_frames,
            self.frame_bytes,
            self.max_frame_bytes,
            self.events,
            self.forward_calls,
            self.forward_events,
            if self.forward_calls == 0 {
                0.0
            } else {
                self.forward_total_us as f64 / self.forward_calls as f64
            },
            self.forward_max_us,
            self.event_send_max_us,
            self.event_send_over_1ms,
            self.event_send_full,
            queue_depth_now,
            self.max_event_queue_depth,
            self.health_healthy,
            self.health_settling,
            self.health_repairing,
            self.health_degraded,
            self.wire.bbo_settle_max_us,
            self.wire.parse_errors,
            self.ws_send_max_us,
            self.loop_scheduler_max_us,
            self.runtime_scheduler_max_us,
            self.read_handler_max_us,
            self.parse_apply_max_us,
        )
    }

    fn log_and_reset(&mut self, now: Instant, queue_depth_now: usize) {
        let window_secs = now
            .saturating_duration_since(self.window_started_at)
            .as_secs_f64();
        info!(
            "[clob_metric] window_secs={:.1} data_frames={} frame_bytes={} events={} books={} quotes={} trades={} tick_size_changes={} other_events={} health_healthy={} health_settling={} health_repairing={} health_degraded={} max_frame_bytes={} max_events_per_frame={} event_queue_depth={} event_queue_high_water={} forward_calls={} forward_events={} forward_total_us={} forward_max_us={} event_send_max_us={} event_send_over_1ms={} event_send_full={} loop_scheduler_max_us={} runtime_scheduler_max_us={} read_handler_max_us={} parse_apply_max_us={} bbo_change_snapshots={} coalesced_snapshots={} wire_book={} wire_price_change={} wire_best_bid_ask={} wire_trade={} wire_last_trade_price={} wire_tick_size_change={} wire_inline_rtds={} price_change_entries={} level_upserts={} level_deletes={} bbo_transient_recoveries={} bbo_mismatches={} bbo_repair_requests={} bbo_recovery_same_ts={} bbo_recovery_newer_ts={} bbo_recovery_book={} bbo_recovery_rest={} bbo_repair_superseded_by_ws={} bbo_settle_samples={} bbo_settle_total_us={} bbo_settle_max_us={} bbo_recovery_samples={} bbo_recovery_total_us={} bbo_recovery_max_us={} bbo_tick_distance_samples={} bbo_tick_distance_total={} bbo_tick_distance_max={} unseeded_deltas={} ignored={} unknown={} parse_errors={} ws_sends={} ws_send_errors={} ws_send_max_us={}",
            window_secs,
            self.data_frames,
            self.frame_bytes,
            self.events,
            self.books,
            self.quotes,
            self.trades,
            self.tick_size_changes,
            self.other_events,
            self.health_healthy,
            self.health_settling,
            self.health_repairing,
            self.health_degraded,
            self.max_frame_bytes,
            self.max_events_per_frame,
            queue_depth_now,
            self.max_event_queue_depth,
            self.forward_calls,
            self.forward_events,
            self.forward_total_us,
            self.forward_max_us,
            self.event_send_max_us,
            self.event_send_over_1ms,
            self.event_send_full,
            self.loop_scheduler_max_us,
            self.runtime_scheduler_max_us,
            self.read_handler_max_us,
            self.parse_apply_max_us,
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
            self.wire.bbo_transient_recoveries,
            self.wire.bbo_mismatches,
            self.wire.bbo_repair_requests,
            self.wire.bbo_recovery_same_timestamp,
            self.wire.bbo_recovery_newer_timestamp,
            self.wire.bbo_recovery_book,
            self.wire.bbo_recovery_rest,
            self.wire.bbo_repair_superseded_by_ws,
            self.wire.bbo_settle_samples,
            self.wire.bbo_settle_total_us,
            self.wire.bbo_settle_max_us,
            self.wire.bbo_recovery_samples,
            self.wire.bbo_recovery_total_us,
            self.wire.bbo_recovery_max_us,
            self.wire.bbo_tick_distance_samples,
            self.wire.bbo_tick_distance_total,
            self.wire.bbo_tick_distance_max,
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

#[derive(Debug, Default, Clone, Copy)]
struct ClobSocketPollWindow {
    poll_calls: u64,
    poll_gap_samples: u64,
    poll_gap_max_us: u64,
    poll_gap_over_20ms: u64,
}

impl ClobSocketPollWindow {
    fn record_poll(&mut self, last_poll_at: &mut Option<Instant>, now: Instant) {
        self.poll_calls = self.poll_calls.saturating_add(1);
        if let Some(previous) = last_poll_at.replace(now) {
            let gap = now.saturating_duration_since(previous);
            let gap_us = gap.as_micros().min(u64::MAX as u128) as u64;
            self.poll_gap_samples = self.poll_gap_samples.saturating_add(1);
            self.poll_gap_max_us = self.poll_gap_max_us.max(gap_us);
            self.poll_gap_over_20ms = self
                .poll_gap_over_20ms
                .saturating_add(u64::from(gap >= CLOB_SOCKET_POLL_STALL_THRESHOLD));
        }
    }
}

/// Instruments the exact `WebSocketStream::poll_next` boundary. The wrapper is
/// owned and mutated only by the dedicated CLOB runtime thread; it adds no
/// locks, allocation, queueing, or cross-thread state to socket processing.
struct ClobPollInstrumentedStream<S> {
    inner: S,
    last_poll_at: Option<Instant>,
    pending: ClobSocketPollWindow,
}

impl<S> ClobPollInstrumentedStream<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            last_poll_at: None,
            pending: ClobSocketPollWindow::default(),
        }
    }

    fn take_poll_window(&mut self) -> ClobSocketPollWindow {
        std::mem::take(&mut self.pending)
    }
}

impl<S> Stream for ClobPollInstrumentedStream<S>
where
    S: Stream + Unpin,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        this.pending
            .record_poll(&mut this.last_poll_at, Instant::now());
        Pin::new(&mut this.inner).poll_next(cx)
    }
}

type ClobWebSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
type ClobSocketWriter = SplitSink<ClobWebSocket, Message>;
type ClobSocketReader = ClobPollInstrumentedStream<SplitStream<ClobWebSocket>>;
type ClobSocketReadResult =
    Option<std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>;

struct ClobConnection {
    lane_id: u64,
    write: ClobSocketWriter,
    read: ClobSocketReader,
    tcp_fd: Option<i32>,
    peer_addr: Option<SocketAddr>,
    last_raw_at: Instant,
    observed_data: bool,
    diagnostics: ClobWindowMetrics,
    burst: ClobBurstMetrics,
}

impl ClobConnection {
    fn record_raw(&mut self, now: Instant) {
        self.last_raw_at = now;
    }

    fn record_data_frame(&mut self, now: Instant) {
        self.last_raw_at = now;
        self.observed_data = true;
    }

    fn is_hot_standby(&self, now: Instant) -> bool {
        clob_standby_is_hot(self.observed_data, self.last_raw_at, now)
    }
}

enum ClobLaneRead {
    Active(ClobSocketReadResult),
    Standby(ClobSocketReadResult),
}

async fn next_clob_lane(
    active: &mut ClobConnection,
    standby: Option<&mut ClobConnection>,
) -> ClobLaneRead {
    if let Some(standby) = standby {
        tokio::select! {
            message = active.read.next() => ClobLaneRead::Active(message),
            message = standby.read.next() => ClobLaneRead::Standby(message),
        }
    } else {
        ClobLaneRead::Active(active.read.next().await)
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct ClobMicroburstBucket {
    frames: u64,
    bytes: u64,
    max_frame_bytes: usize,
}

struct ClobBurstMetrics {
    window_started_at: Instant,
    frames: u64,
    bytes: u64,
    max_frame_bytes: usize,
    micro_started_at: Instant,
    micro: ClobMicroburstBucket,
    peak_100ms_frames: u64,
    peak_100ms_bytes: u64,
    peak_100ms_max_frame_bytes: usize,
    kernel_unread_latest: Option<u32>,
    kernel_unread_max: u32,
    kernel_unread_samples: u64,
    kernel_unread_errors: u64,
    socket_probe_max_us: u64,
    socket_poll: ClobSocketPollWindow,
}

impl ClobBurstMetrics {
    fn new(now: Instant) -> Self {
        Self {
            window_started_at: now,
            frames: 0,
            bytes: 0,
            max_frame_bytes: 0,
            micro_started_at: now,
            micro: ClobMicroburstBucket::default(),
            peak_100ms_frames: 0,
            peak_100ms_bytes: 0,
            peak_100ms_max_frame_bytes: 0,
            kernel_unread_latest: None,
            kernel_unread_max: 0,
            kernel_unread_samples: 0,
            kernel_unread_errors: 0,
            socket_probe_max_us: 0,
            socket_poll: ClobSocketPollWindow::default(),
        }
    }

    fn finish_micro_bucket(&mut self) {
        self.peak_100ms_frames = self.peak_100ms_frames.max(self.micro.frames);
        self.peak_100ms_bytes = self.peak_100ms_bytes.max(self.micro.bytes);
        self.peak_100ms_max_frame_bytes = self
            .peak_100ms_max_frame_bytes
            .max(self.micro.max_frame_bytes);
        self.micro = ClobMicroburstBucket::default();
    }

    fn advance_micro_bucket(&mut self, now: Instant) {
        if now.saturating_duration_since(self.micro_started_at) >= CLOB_MICROBURST_BUCKET_INTERVAL {
            self.finish_micro_bucket();
            self.micro_started_at = now;
        }
    }

    fn record_frame(&mut self, now: Instant, bytes: usize) {
        self.advance_micro_bucket(now);
        self.frames = self.frames.saturating_add(1);
        self.bytes = self.bytes.saturating_add(bytes as u64);
        self.max_frame_bytes = self.max_frame_bytes.max(bytes);
        self.micro.frames = self.micro.frames.saturating_add(1);
        self.micro.bytes = self.micro.bytes.saturating_add(bytes as u64);
        self.micro.max_frame_bytes = self.micro.max_frame_bytes.max(bytes);
    }

    fn record_socket_probe(&mut self, now: Instant, unread_bytes: Option<u32>, elapsed: Duration) {
        self.advance_micro_bucket(now);
        self.socket_probe_max_us = self
            .socket_probe_max_us
            .max(elapsed.as_micros().min(u64::MAX as u128) as u64);
        match unread_bytes {
            Some(unread) => {
                self.kernel_unread_latest = Some(unread);
                self.kernel_unread_max = self.kernel_unread_max.max(unread);
                self.kernel_unread_samples = self.kernel_unread_samples.saturating_add(1);
            }
            None => {
                self.kernel_unread_errors = self.kernel_unread_errors.saturating_add(1);
            }
        }
    }

    fn record_socket_polls(&mut self, polls: ClobSocketPollWindow) {
        self.socket_poll.poll_calls = self.socket_poll.poll_calls.saturating_add(polls.poll_calls);
        self.socket_poll.poll_gap_samples = self
            .socket_poll
            .poll_gap_samples
            .saturating_add(polls.poll_gap_samples);
        self.socket_poll.poll_gap_max_us =
            self.socket_poll.poll_gap_max_us.max(polls.poll_gap_max_us);
        self.socket_poll.poll_gap_over_20ms = self
            .socket_poll
            .poll_gap_over_20ms
            .saturating_add(polls.poll_gap_over_20ms);
    }

    fn finish_window(&mut self, now: Instant) {
        self.advance_micro_bucket(now);
        self.finish_micro_bucket();
    }

    fn close_summary(&mut self, now: Instant) -> String {
        self.finish_window(now);
        format!(
            "clob_recent_window_ms={} recent_frames={} recent_frame_bytes={} recent_max_frame_bytes={} peak_100ms_frames={} peak_100ms_frame_bytes={} peak_100ms_max_frame_bytes={} kernel_unread_latest={} kernel_unread_max={} kernel_unread_samples={} kernel_unread_errors={} socket_probe_max_us={} socket_poll_calls={} socket_poll_gap_samples={} socket_poll_gap_max_us={} socket_poll_gap_over_20ms={} decoded_frame_queue_mode=inline decoded_frame_queue_depth=0 decoded_frame_queue_capacity=0",
            now.saturating_duration_since(self.window_started_at).as_millis(),
            self.frames,
            self.bytes,
            self.max_frame_bytes,
            self.peak_100ms_frames,
            self.peak_100ms_bytes,
            self.peak_100ms_max_frame_bytes,
            self.kernel_unread_latest.map(i64::from).unwrap_or(-1),
            if self.kernel_unread_samples == 0 {
                -1
            } else {
                i64::from(self.kernel_unread_max)
            },
            self.kernel_unread_samples,
            self.kernel_unread_errors,
            self.socket_probe_max_us,
            self.socket_poll.poll_calls,
            self.socket_poll.poll_gap_samples,
            self.socket_poll.poll_gap_max_us,
            self.socket_poll.poll_gap_over_20ms,
        )
    }

    fn log_and_reset(
        &mut self,
        now: Instant,
        tcp: TcpSocketMetrics,
        peer_addr: Option<SocketAddr>,
        lane_role: &'static str,
        lane_id: u64,
        subscription_tokens: usize,
        event_queue: &ClobEventSender,
        diagnostics: &ClobWindowMetrics,
    ) {
        self.finish_window(now);
        let event_queue_depth = event_queue.len();
        let (critical_overflows, replaceable_evictions) = event_queue.overflow_totals();
        debug!(
            "[clob_1s_metric] lane_role={} lane_id={} peer={:?} subscription_tokens={} window_ms={} frames={} frame_bytes={} max_frame_bytes={} peak_100ms_frames={} peak_100ms_frame_bytes={} peak_100ms_max_frame_bytes={} kernel_unread_latest={} kernel_unread_max={} kernel_unread_samples={} kernel_unread_errors={} socket_probe_max_us={} socket_poll_calls={} socket_poll_gap_samples={} socket_poll_gap_max_us={} socket_poll_gap_over_20ms={} decoded_frame_queue_mode=inline decoded_frame_queue_depth=0 decoded_frame_queue_capacity=0 event_queue_mode=bounded_tiered event_queue_critical_capacity={} event_queue_replaceable_capacity={} event_queue_depth={} event_queue_high_water_30s={} event_queue_critical_overflows_total={} event_queue_replaceable_evictions_total={} parse_apply_max_us_30s={} read_handler_max_us_30s={} forward_max_us_30s={} event_send_max_us_30s={} event_send_over_1ms_30s={} loop_scheduler_max_us_30s={} runtime_scheduler_max_us_30s={} tcp_unread_bytes={} tcp_rcv_space={} tcp_rcv_wnd={} tcp_rcv_ssthresh={} tcp_rcv_wscale={} tcp_total_retrans={} so_rcvbuf={}",
            lane_role,
            lane_id,
            peer_addr,
            subscription_tokens,
            now.saturating_duration_since(self.window_started_at).as_millis(),
            self.frames,
            self.bytes,
            self.max_frame_bytes,
            self.peak_100ms_frames,
            self.peak_100ms_bytes,
            self.peak_100ms_max_frame_bytes,
            self.kernel_unread_latest.map(i64::from).unwrap_or(-1),
            if self.kernel_unread_samples == 0 {
                -1
            } else {
                i64::from(self.kernel_unread_max)
            },
            self.kernel_unread_samples,
            self.kernel_unread_errors,
            self.socket_probe_max_us,
            self.socket_poll.poll_calls,
            self.socket_poll.poll_gap_samples,
            self.socket_poll.poll_gap_max_us,
            self.socket_poll.poll_gap_over_20ms,
            CLOB_CRITICAL_EVENT_CAPACITY,
            CLOB_REPLACEABLE_EVENT_CAPACITY,
            event_queue_depth,
            diagnostics.max_event_queue_depth,
            critical_overflows,
            replaceable_evictions,
            diagnostics.parse_apply_max_us,
            diagnostics.read_handler_max_us,
            diagnostics.forward_max_us,
            diagnostics.event_send_max_us,
            diagnostics.event_send_over_1ms,
            diagnostics.loop_scheduler_max_us,
            diagnostics.runtime_scheduler_max_us,
            tcp.unread_bytes.map(i64::from).unwrap_or(-1),
            tcp.rcv_space.map(i64::from).unwrap_or(-1),
            tcp.rcv_wnd.map(i64::from).unwrap_or(-1),
            tcp.rcv_ssthresh.map(i64::from).unwrap_or(-1),
            tcp.rcv_wscale.map(i64::from).unwrap_or(-1),
            tcp.total_retrans.map(i64::from).unwrap_or(-1),
            tcp.so_rcvbuf.map(i64::from).unwrap_or(-1),
        );
        *self = Self::new(now);
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct TcpSocketMetrics {
    /// Exact bytes currently pending in the kernel receive queue, sampled via
    /// `FIONREAD`. Unlike TCP_INFO receive-space fields, this is the Recv-Q.
    unread_bytes: Option<u32>,
    rcv_space: Option<u32>,
    rcv_wnd: Option<u32>,
    rcv_ssthresh: Option<u32>,
    rcv_wscale: Option<u8>,
    total_retrans: Option<u32>,
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
const LINUX_TCP_INFO_TOTAL_RETRANS_OFFSET: usize = 100;
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

fn clob_socket_peer_addr(
    stream: &tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Option<SocketAddr> {
    match stream.get_ref() {
        tokio_tungstenite::MaybeTlsStream::Plain(tcp) => tcp.peer_addr().ok(),
        tokio_tungstenite::MaybeTlsStream::Rustls(tls) => tls.get_ref().0.peer_addr().ok(),
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

/// Return the exact number of bytes currently pending in the kernel receive
/// queue. This deliberately uses portable `FIONREAD` rather than treating
/// `TCP_INFO.tcpi_rcv_space` as Recv-Q; the latter is receive-space tuning
/// state and does not report unread bytes.
#[cfg(unix)]
fn sample_socket_unread_bytes(fd: Option<i32>) -> Option<u32> {
    let fd = fd?;
    let mut unread: libc::c_int = 0;
    let result = unsafe { libc::ioctl(fd, libc::FIONREAD as _, &mut unread) };
    (result == 0 && unread >= 0).then_some(unread as u32)
}

#[cfg(not(unix))]
fn sample_socket_unread_bytes(_fd: Option<i32>) -> Option<u32> {
    None
}

#[cfg(unix)]
fn configure_clob_socket_receive_buffer(
    fd: Option<i32>,
    requested_bytes: libc::c_int,
) -> std::result::Result<libc::c_int, String> {
    let fd = fd.ok_or_else(|| "CLOB socket file descriptor unavailable".to_string())?;
    if requested_bytes <= 0 {
        return Err(format!(
            "invalid CLOB socket receive buffer request: {requested_bytes}"
        ));
    }
    unsafe {
        if libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            (&requested_bytes as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        ) != 0
        {
            return Err(std::io::Error::last_os_error().to_string());
        }
        let mut actual: libc::c_int = 0;
        let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        if libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            (&mut actual as *mut libc::c_int).cast(),
            &mut len,
        ) != 0
        {
            return Err(std::io::Error::last_os_error().to_string());
        }
        Ok(actual)
    }
}

#[cfg(not(unix))]
fn configure_clob_socket_receive_buffer(
    _fd: Option<i32>,
    _requested_bytes: libc::c_int,
) -> std::result::Result<libc::c_int, String> {
    Err("CLOB socket receive-buffer tuning is unsupported on this platform".to_string())
}

fn sample_tcp_socket(fd: Option<i32>) -> TcpSocketMetrics {
    let Some(fd) = fd else {
        return TcpSocketMetrics::default();
    };
    let mut metrics = TcpSocketMetrics::default();
    metrics.unread_bytes = sample_socket_unread_bytes(Some(fd));
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
            metrics.total_retrans =
                linux_tcp_info_u32(&info, returned_len, LINUX_TCP_INFO_TOTAL_RETRANS_OFFSET);
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
    key: &'static str,
    detail: String,
}

fn clob_diagnostic_token(detail: &str) -> Option<&str> {
    detail
        .strip_prefix("token=")
        .and_then(|tail| tail.split_ascii_whitespace().next())
}

/// The wire lane can temporarily carry the current and next event together.
/// Continue applying both so the next event stays pre-seeded, but discard
/// diagnostics and REST repair requests for tokens outside the logical active
/// generation before they reach formatting/logging or the network worker.
fn retain_active_clob_diagnostics(batch: &mut ClobParsedBatch, active_tokens: &[String]) {
    batch.diagnostics.retain(|diagnostic| {
        clob_diagnostic_token(&diagnostic.detail)
            .is_none_or(|token| subscribed_token(active_tokens, token))
    });
    batch
        .repair_tokens
        .retain(|token| subscribed_token(active_tokens, token));
}

fn retain_active_clob_deferred_diagnostics(
    batch: &mut ClobDeferredBatch,
    active_tokens: &[String],
) {
    batch.diagnostics.retain(|diagnostic| {
        clob_diagnostic_token(&diagnostic.detail)
            .is_none_or(|token| subscribed_token(active_tokens, token))
    });
    batch
        .repair_tokens
        .retain(|token| subscribed_token(active_tokens, token));
}

#[derive(Clone, Copy, Debug, Default)]
struct ClobThreadResourceSnapshot {
    cpu_ns: u64,
    voluntary_switches: u64,
    involuntary_switches: u64,
    minor_faults: u64,
    major_faults: u64,
}

#[cfg(target_os = "linux")]
fn clob_thread_cpu_ns() -> u64 {
    unsafe {
        let mut clock: libc::timespec = std::mem::zeroed();
        if libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut clock) == 0 {
            (clock.tv_sec as u64)
                .saturating_mul(1_000_000_000)
                .saturating_add(clock.tv_nsec as u64)
        } else {
            0
        }
    }
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
struct ClobPerfTrigger {
    elapsed: Duration,
    frame_len: usize,
    phases: ClobFramePhaseTimings,
}

/// A dedicated housekeeping owner continuously runs a low-frequency,
/// overwrite-mode perf ring for the CLOB TID. The hot CLOB owner only performs
/// a bounded try_send. On a tail the worker freezes the ring, so the artifact
/// contains the lead-up to the spike instead of 34-39 samples collected after
/// it was already over.
#[cfg(target_os = "linux")]
struct ClobPerfRing {
    tx: crossbeam_channel::Sender<ClobPerfTrigger>,
}

#[cfg(target_os = "linux")]
impl ClobPerfRing {
    fn start() -> Option<Self> {
        let enabled = std::env::var("HEXBOT_CLOB_PERF_TRIGGER")
            .map(|value| !matches!(value.trim(), "0" | "false" | "off"))
            .unwrap_or(true);
        if !enabled {
            return None;
        }
        let tid = unsafe { libc::syscall(libc::SYS_gettid) as i64 };
        let frequency = std::env::var("HEXBOT_CLOB_PERF_FREQUENCY")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|value| (9..=99).contains(value))
            .unwrap_or(19);
        let output_dir = std::env::var("HEXBOT_CLOB_PERF_DIR")
            .unwrap_or_else(|_| "/tmp/hexbot-clob-perf".to_string());
        let (tx, rx) = crossbeam_channel::bounded::<ClobPerfTrigger>(2);
        let spawn = std::thread::Builder::new()
            .name("clob-perf-ring".to_string())
            .spawn(move || {
                crate::os_tune::pin_background("clob-perf-ring");
                if let Err(error) = std::fs::create_dir_all(&output_dir) {
                    warn!(
                        "[clob_perf_ring] action=create_dir_failed dir={} error={}",
                        output_dir, error
                    );
                    return;
                }
                loop {
                    let epoch = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    let output = format!("{output_dir}/clob-prewindow-tid-{tid}-{epoch}.data");
                    let mut child = match std::process::Command::new("perf")
                        .arg("record")
                        .arg("--snapshot")
                        .arg("--overwrite")
                        .arg("-m")
                        .arg("32")
                        .arg("--freq")
                        .arg(frequency.to_string())
                        .arg("--call-graph")
                        .arg("fp")
                        .arg("--tid")
                        .arg(tid.to_string())
                        .arg("--output")
                        .arg(&output)
                        .spawn()
                    {
                        Ok(child) => child,
                        Err(error) => {
                            warn!(
                                "[clob_perf_ring] action=spawn_failed tid={} output={} error={}",
                                tid, output, error
                            );
                            return;
                        }
                    };
                    let trigger = match rx.recv() {
                        Ok(trigger) => trigger,
                        Err(_) => {
                            let _ = unsafe { libc::kill(child.id() as i32, libc::SIGINT) };
                            let _ = child.wait();
                            return;
                        }
                    };
                    // perf uses SIGUSR2 for snapshot/switch-output control.
                    let _ = unsafe { libc::kill(child.id() as i32, libc::SIGUSR2) };
                    std::thread::sleep(Duration::from_millis(100));
                    let _ = unsafe { libc::kill(child.id() as i32, libc::SIGINT) };
                    let status = child.wait();
                    warn!(
                        "[clob_perf_ring] action=frozen tid={} frequency_hz={} output={} status={:?} triggering_tail_us={} frame_bytes={} simd_json_us={} book_apply_us={} price_change_apply_us={} event_construction_us={}",
                        tid,
                        frequency,
                        output,
                        status,
                        trigger.elapsed.as_micros(),
                        trigger.frame_len,
                        trigger.phases.simd_json_ns / 1_000,
                        trigger.phases.book_apply_ns / 1_000,
                        trigger.phases.price_change_apply_ns / 1_000,
                        trigger.phases.event_construction_ns / 1_000,
                    );
                }
            });
        match spawn {
            Ok(_) => Some(Self { tx }),
            Err(error) => {
                warn!("[clob_perf_ring] action=thread_spawn_failed error={error}");
                None
            }
        }
    }

    fn trigger(&self, trigger: ClobPerfTrigger) {
        static HIGH_WATER: std::sync::atomic::AtomicUsize =
            std::sync::atomic::AtomicUsize::new(0);
        let depth = self.tx.len().saturating_add(1);
        HIGH_WATER.fetch_max(depth.min(2), Ordering::Relaxed);
        crate::latency::record_ns(
            "polymarket.ws.clob_perf_ring_queue_high_water",
            HIGH_WATER.load(Ordering::Relaxed) as u64,
        );
        if self.tx.try_send(trigger).is_err() {
            crate::latency::record_ns("polymarket.ws.clob_perf_ring_overflow", 1);
        }
    }
}

#[cfg(target_os = "linux")]
fn maybe_trigger_clob_perf(
    ring: Option<&ClobPerfRing>,
    elapsed: Duration,
    frame_len: usize,
    phases: ClobFramePhaseTimings,
) {
    static LAST_TRIGGER_NS: AtomicU64 = AtomicU64::new(0);
    const COOLDOWN_NS: u64 = 300_000_000_000;
    if elapsed < CLOB_RARE_HANDLER_TAIL {
        return;
    }
    let Some(ring) = ring else {
        return;
    };
    let now_ns = now_ns();
    loop {
        let previous = LAST_TRIGGER_NS.load(Ordering::Acquire);
        if previous != 0 && now_ns.saturating_sub(previous) < COOLDOWN_NS {
            return;
        }
        if LAST_TRIGGER_NS
            .compare_exchange(previous, now_ns, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            break;
        }
    }
    ring.trigger(ClobPerfTrigger {
        elapsed,
        frame_len,
        phases,
    });
}

#[cfg(not(target_os = "linux"))]
struct ClobPerfRing;

#[cfg(not(target_os = "linux"))]
impl ClobPerfRing {
    fn start() -> Option<Self> {
        None
    }
}

#[cfg(not(target_os = "linux"))]
fn maybe_trigger_clob_perf(
    _ring: Option<&ClobPerfRing>,
    _elapsed: Duration,
    _frame_len: usize,
    _phases: ClobFramePhaseTimings,
) {
}

#[cfg(not(target_os = "linux"))]
fn clob_thread_cpu_ns() -> u64 {
    0
}

#[cfg(target_os = "linux")]
fn clob_thread_resource_snapshot() -> ClobThreadResourceSnapshot {
    unsafe {
        let cpu_ns = clob_thread_cpu_ns();
        let mut usage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_THREAD, &mut usage) != 0 {
            return ClobThreadResourceSnapshot {
                cpu_ns,
                ..ClobThreadResourceSnapshot::default()
            };
        }
        ClobThreadResourceSnapshot {
            cpu_ns,
            voluntary_switches: usage.ru_nvcsw.max(0) as u64,
            involuntary_switches: usage.ru_nivcsw.max(0) as u64,
            minor_faults: usage.ru_minflt.max(0) as u64,
            major_faults: usage.ru_majflt.max(0) as u64,
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn clob_thread_resource_snapshot() -> ClobThreadResourceSnapshot {
    ClobThreadResourceSnapshot::default()
}

#[derive(Debug)]
struct ClobThreadResourceSampler {
    frames: u64,
    baseline_frame: u64,
    baseline: ClobThreadResourceSnapshot,
}

impl Default for ClobThreadResourceSampler {
    fn default() -> Self {
        Self {
            frames: 0,
            baseline_frame: 0,
            baseline: clob_thread_resource_snapshot(),
        }
    }
}

impl ClobThreadResourceSampler {
    fn begin_frame(&mut self) -> ClobThreadResourceSnapshot {
        self.frames = self.frames.saturating_add(1);
        if self.frames.saturating_sub(self.baseline_frame) >= CLOB_RESOURCE_BASELINE_FRAMES {
            self.baseline = clob_thread_resource_snapshot();
            self.baseline_frame = self.frames;
        }
        // CLOCK_THREAD_CPUTIME_ID is vDSO-backed on Linux and gives an exact
        // wall-vs-CPU split without adding two getrusage syscalls per frame.
        let mut snapshot = ClobThreadResourceSnapshot::default();
        snapshot.cpu_ns = clob_thread_cpu_ns();
        snapshot
    }

    fn tail_delta(
        &self,
        frame_start: ClobThreadResourceSnapshot,
        wall: Duration,
    ) -> (u64, u64, ClobThreadResourceSnapshot, u64) {
        let now = clob_thread_resource_snapshot();
        let cpu_ns = now.cpu_ns.saturating_sub(frame_start.cpu_ns);
        let wall_ns = wall.as_nanos().min(u64::MAX as u128) as u64;
        let resource_delta = ClobThreadResourceSnapshot {
            cpu_ns,
            voluntary_switches: now
                .voluntary_switches
                .saturating_sub(self.baseline.voluntary_switches),
            involuntary_switches: now
                .involuntary_switches
                .saturating_sub(self.baseline.involuntary_switches),
            minor_faults: now.minor_faults.saturating_sub(self.baseline.minor_faults),
            major_faults: now.major_faults.saturating_sub(self.baseline.major_faults),
        };
        (
            cpu_ns,
            wall_ns.saturating_sub(cpu_ns),
            resource_delta,
            self.frames
                .saturating_sub(self.baseline_frame)
                .saturating_add(1),
        )
    }
}

#[derive(Default)]
struct ClobDiagnosticSampler {
    last_logged: HashMap<&'static str, Instant>,
    suppressed: HashMap<&'static str, u64>,
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

async fn connect_clob_lane(
    tokens: &[String],
    lane_id: u64,
) -> std::result::Result<ClobConnection, String> {
    let stream = match tokio::time::timeout(
        WS_CONNECT_TIMEOUT,
        tokio_tungstenite::connect_async(POLYMARKET_WS_URL),
    )
    .await
    {
        Ok(Ok((stream, _))) => stream,
        Ok(Err(error)) => return Err(format!("connect failed: {error}")),
        Err(_) => {
            return Err(format!(
                "connect stalled >{:.0}s",
                WS_CONNECT_TIMEOUT.as_secs_f64(),
            ));
        }
    };
    let tcp_fd = clob_socket_fd(&stream);
    let peer_addr = clob_socket_peer_addr(&stream);
    match configure_clob_socket_receive_buffer(tcp_fd, CLOB_SOCKET_RCVBUF_BYTES) {
        Ok(actual_bytes) => info!(
            "[clob_socket_config] lane_id={} peer={:?} requested_rcvbuf={} actual_rcvbuf={} unread_probe_ms={} microburst_bucket_ms={} decoded_frame_queue_mode=inline decoded_frame_queue_capacity=0",
            lane_id,
            peer_addr,
            CLOB_SOCKET_RCVBUF_BYTES,
            actual_bytes,
            CLOB_SOCKET_UNREAD_PROBE_INTERVAL.as_millis(),
            CLOB_MICROBURST_BUCKET_INTERVAL.as_millis(),
        ),
        Err(error) => warn!(
            "[clob_socket_config] lane_id={} peer={:?} failed to raise receive buffer requested_rcvbuf={}: {}",
            lane_id,
            peer_addr,
            CLOB_SOCKET_RCVBUF_BYTES,
            error,
        ),
    }

    let (write, read) = stream.split();
    let connected_at = Instant::now();
    let mut lane = ClobConnection {
        lane_id,
        write,
        read: ClobPollInstrumentedStream::new(read),
        tcp_fd,
        peer_addr,
        last_raw_at: connected_at,
        observed_data: false,
        diagnostics: ClobWindowMetrics::new(connected_at),
        burst: ClobBurstMetrics::new(connected_at),
    };
    let subscription = clob_subscription_message(tokens);
    timed_clob_ws_send(
        &mut lane.write,
        Message::Text(subscription.to_string()),
        "polymarket.ws.clob_send.subscribe",
        &mut lane.diagnostics,
    )
    .await
    .map_err(|error| format!("subscribe send failed: {error}"))?;
    info!(
        "[clob_lane_connected] lane_id={} peer={:?} subscription_tokens={}",
        lane_id,
        peer_addr,
        tokens.len(),
    );
    Ok(lane)
}

fn spawn_clob_standby_connect(
    tokens: Vec<String>,
    lane_id: u64,
    delay: Duration,
) -> tokio::task::JoinHandle<std::result::Result<ClobConnection, String>> {
    tokio::spawn(async move {
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        connect_clob_lane(&tokens, lane_id).await
    })
}

struct ClobSeededCandidate {
    lane: ClobConnection,
    books: ClobLocalBooks,
    seed_events: Vec<MarketEvent>,
    warmup: Duration,
}

fn spawn_clob_seeded_candidate(
    subscription: ClobSubscription,
    lane_id: u64,
    delay: Duration,
) -> tokio::task::JoinHandle<std::result::Result<ClobSeededCandidate, String>> {
    tokio::spawn(async move {
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        let started = Instant::now();
        let mut lane = connect_clob_lane(&subscription.tokens, lane_id).await?;
        let mut books = ClobLocalBooks::new(&subscription.canonical_events);
        let mut seed_events = Vec::with_capacity(subscription.tokens.len() * 2);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let message = tokio::time::timeout_at(deadline, lane.read.next())
                .await
                .map_err(|_| "candidate L2 seed timed out".to_string())?
                .ok_or_else(|| "candidate socket closed before L2 seed".to_string())?
                .map_err(|error| format!("candidate socket read failed: {error}"))?;
            let received_at = Instant::now();
            match message {
                Message::Text(text) => {
                    lane.record_raw(received_at);
                    let body = text.trim();
                    if body.eq_ignore_ascii_case("PONG") {
                        continue;
                    }
                    if body.eq_ignore_ascii_case("PING") {
                        let _ = timed_clob_ws_send(
                            &mut lane.write,
                            Message::Text("PONG".to_string()),
                            "polymarket.ws.clob_candidate_send.text_pong",
                            &mut lane.diagnostics,
                        )
                        .await;
                        continue;
                    }
                    let frame_len = text.len();
                    // Consuming String into Vec transfers its allocation;
                    // simd-json can parse it in place without a per-frame
                    // memcpy into a second scratch buffer.
                    let mut frame_bytes = text.into_bytes();
                    let mut frame_phases = ClobFramePhaseTimings::default();
                    let batch = process_clob_frame_in_place(
                        &mut frame_bytes,
                        &mut books,
                        &subscription.tokens,
                        &subscription.tokens,
                        received_at,
                        now_ns(),
                        &mut frame_phases,
                    );
                    frame_phases.record();
                    lane.burst.record_frame(received_at, frame_len);
                    lane.diagnostics
                        .record_frame(received_at, frame_len, &batch);
                    for event in batch.events {
                        match &event {
                            MarketEvent::OrderBook(_) => {
                                push_latest_order_book(&mut seed_events, event)
                            }
                            MarketEvent::Quote(incoming) => {
                                if let Some(index) = seed_events.iter().position(|existing| {
                                    matches!(existing, MarketEvent::Quote(quote) if quote.symbol == incoming.symbol)
                                }) {
                                    seed_events.remove(index);
                                }
                                seed_events.push(event);
                            }
                            MarketEvent::TickSizeChange(incoming) => {
                                if let Some(index) = seed_events.iter().position(|existing| {
                                    matches!(existing, MarketEvent::TickSizeChange(change) if change.symbol == incoming.symbol)
                                }) {
                                    seed_events.remove(index);
                                }
                                seed_events.push(event);
                            }
                            // Candidate trades and other transient diagnostics
                            // predate activation and must not leak into the
                            // logical stream. Keeping only one book/quote/tick
                            // record per token also makes the seed handoff
                            // strictly bounded by subscription cardinality.
                            _ => {}
                        }
                    }
                    if books.has_all_seeded(&subscription.tokens) {
                        return Ok(ClobSeededCandidate {
                            lane,
                            books,
                            seed_events,
                            warmup: started.elapsed(),
                        });
                    }
                }
                Message::Ping(payload) => {
                    lane.record_raw(received_at);
                    let _ = timed_clob_ws_send(
                        &mut lane.write,
                        Message::Pong(payload),
                        "polymarket.ws.clob_candidate_send.frame_pong",
                        &mut lane.diagnostics,
                    )
                    .await;
                }
                Message::Pong(_) => lane.record_raw(received_at),
                Message::Close(frame) => {
                    return Err(format!("candidate socket closed before L2 seed: {frame:?}"));
                }
                _ => {}
            }
        }
    })
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

fn should_forward_clob_event(event: &MarketEvent, tokens: &[String]) -> bool {
    let symbol = match event {
        MarketEvent::OrderBook(book) => Some(&book.symbol),
        MarketEvent::Quote(quote) => Some(&quote.symbol),
        MarketEvent::Trade(trade) => Some(&trade.symbol),
        MarketEvent::TickSizeChange(change) => Some(&change.symbol),
        _ => None,
    };
    symbol.is_none_or(|symbol| tokens.iter().any(|token| token == symbol))
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
    event_tx: &ClobEventSender,
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
    event_tx: &ClobEventSender,
    lifecycle: &mut ClobLifecycle,
    health: &mut WsHealth,
    diagnostics: &mut ClobWindowMetrics,
    books: &ClobLocalBooks,
    tokens: &[String],
    now: Instant,
) -> bool {
    let forward_started = Instant::now();
    let mut forwarded = 0usize;
    let has_usable_book = events
        .iter()
        .filter(|event| should_forward_clob_event(event, tokens))
        .any(|event| is_usable_subscribed_book_event(event, tokens));
    if has_usable_book {
        health.record_usable_book(now);
    }
    for event in events {
        if !should_forward_clob_event(&event, tokens) {
            continue;
        }
        let was_full = event_tx.is_full();
        let send_started = Instant::now();
        let send_result = event_tx.send(event);
        diagnostics.record_event_send(send_started.elapsed(), was_full);
        if !send_result {
            let elapsed = forward_started.elapsed();
            diagnostics.record_forward(elapsed, forwarded);
            crate::latency::record_ns(
                "polymarket.ws.clob_forward",
                elapsed.as_nanos().min(u64::MAX as u128) as u64,
            );
            return false;
        }
        forwarded += 1;
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
            let was_full = event_tx.is_full();
            let send_started = Instant::now();
            let send_result = event_tx.send(MarketEvent::Connected {
                exchange: Exchange::Polymarket,
            });
            diagnostics.record_event_send(send_started.elapsed(), was_full);
            if !send_result {
                let elapsed = forward_started.elapsed();
                diagnostics.record_forward(elapsed, forwarded);
                crate::latency::record_ns(
                    "polymarket.ws.clob_forward",
                    elapsed.as_nanos().min(u64::MAX as u128) as u64,
                );
                return false;
            }
            forwarded += 1;
        }
    }
    diagnostics.record_queue_depth(event_tx.len());
    let elapsed = forward_started.elapsed();
    diagnostics.record_forward(elapsed, forwarded);
    crate::latency::record_ns(
        "polymarket.ws.clob_forward",
        elapsed.as_nanos().min(u64::MAX as u128) as u64,
    );
    true
}

struct ClobBookRepairResult {
    token: String,
    generation: u64,
    result: std::result::Result<BookFields<'static>, String>,
}

fn advance_clob_repair_generation(epoch: &AtomicU64) -> u64 {
    epoch.fetch_add(1, Ordering::AcqRel).saturating_add(1)
}

fn clob_repair_generation_is_current(epoch: &AtomicU64, generation: u64) -> bool {
    epoch.load(Ordering::Acquire) == generation
}

async fn fetch_authoritative_clob_book(
    token: &str,
) -> std::result::Result<BookFields<'static>, String> {
    let base = std::env::var("POLYMARKET_V2_API_URL")
        .unwrap_or_else(|_| "https://clob.polymarket.com".to_string());
    let url = format!("{}/book", base.trim_end_matches('/'));
    let response = gamma_http_client()
        .get(url)
        .query(&[("token_id", token)])
        .send()
        .await
        .map_err(|error| format!("request failed: {error}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|error| format!("read response failed: {error}"))?;
    if !status.is_success() {
        return Err(format!(
            "HTTP {} body={}",
            status,
            text.chars().take(300).collect::<String>(),
        ));
    }
    let book: BookFields<'_> =
        serde_json::from_str(&text).map_err(|error| format!("invalid book response: {error}"))?;
    if book.asset_id != token {
        return Err(format!(
            "token mismatch requested={} returned={}",
            token, book.asset_id,
        ));
    }
    Ok(book.into_owned())
}

fn request_clob_book_repairs(
    tokens: Vec<String>,
    active_tokens: &[String],
    generation: u64,
    generation_epoch: &Arc<AtomicU64>,
    in_flight: &mut HashSet<String>,
    tx: &tokio::sync::mpsc::Sender<ClobBookRepairResult>,
) {
    for token in tokens {
        if !active_tokens.contains(&token) {
            log::debug!(
                "[clob_bbo_repair_skipped] token={} generation={} reason=retired_token",
                token,
                generation,
            );
            continue;
        }
        if !in_flight.insert(token.clone()) {
            continue;
        }
        let tx = tx.clone();
        let generation_epoch = Arc::clone(generation_epoch);
        // REST repair is control-plane I/O, not socket reading. Keep it off
        // the single-threaded CLOB runtime so a slow HTTP client/future cannot
        // stop frame reads and the runtime's own watchdog timers together.
        crate::async_rt::handle().spawn(async move {
            if !clob_repair_generation_is_current(&generation_epoch, generation) {
                return;
            }
            let result = fetch_authoritative_clob_book(&token).await;
            let _ = tx
                .send(ClobBookRepairResult {
                    token,
                    generation,
                    result,
                })
                .await;
        });
    }
}

fn request_clob_book_repair_after(
    token: String,
    delay: Duration,
    active_tokens: &[String],
    generation: u64,
    generation_epoch: &Arc<AtomicU64>,
    in_flight: &mut HashSet<String>,
    tx: &tokio::sync::mpsc::Sender<ClobBookRepairResult>,
) {
    if !active_tokens.contains(&token) {
        return;
    }
    if !in_flight.insert(token.clone()) {
        return;
    }
    let tx = tx.clone();
    let generation_epoch = Arc::clone(generation_epoch);
    crate::async_rt::handle().spawn(async move {
        tokio::time::sleep(delay).await;
        if !clob_repair_generation_is_current(&generation_epoch, generation) {
            return;
        }
        let result = fetch_authoritative_clob_book(&token).await;
        let _ = tx
            .send(ClobBookRepairResult {
                token,
                generation,
                result,
            })
            .await;
    });
}

fn reset_clob_books_for_failover(
    books: &mut ClobLocalBooks,
    subscription: &ClobSubscription,
    now: Instant,
) {
    *books = ClobLocalBooks::new(&subscription.canonical_events);
    for token in &subscription.tokens {
        books.quarantined_tokens.insert(token.clone());
        books.repair_started_at.insert(token.clone(), now);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClobStandbyReadDisposition {
    Keep,
    Reconnect,
    SlowConsumerReconnect,
}

async fn handle_clob_standby_read(
    lane: &mut ClobConnection,
    result: ClobSocketReadResult,
    subscription_tokens: usize,
    log_slow_consumer_incident: bool,
) -> ClobStandbyReadDisposition {
    let received_at = Instant::now();
    match result {
        Some(Ok(Message::Text(text))) => {
            let body = text.trim();
            if body.eq_ignore_ascii_case("PING") {
                lane.record_raw(received_at);
                if let Err(error) = timed_clob_ws_send(
                    &mut lane.write,
                    Message::Text("PONG".to_string()),
                    "polymarket.ws.clob_standby_send.text_pong",
                    &mut lane.diagnostics,
                )
                .await
                {
                    warn!(
                        "[clob_standby_send_failed] lane_id={} peer={:?} error={}",
                        lane.lane_id, lane.peer_addr, error,
                    );
                    return ClobStandbyReadDisposition::Reconnect;
                }
            } else if body.eq_ignore_ascii_case("PONG") {
                lane.record_raw(received_at);
            } else {
                lane.record_data_frame(received_at);
                lane.burst.record_frame(received_at, text.len());
            }
            ClobStandbyReadDisposition::Keep
        }
        Some(Ok(Message::Ping(payload))) => {
            lane.record_raw(received_at);
            if let Err(error) = timed_clob_ws_send(
                &mut lane.write,
                Message::Pong(payload),
                "polymarket.ws.clob_standby_send.frame_pong",
                &mut lane.diagnostics,
            )
            .await
            {
                warn!(
                    "[clob_standby_send_failed] lane_id={} peer={:?} error={}",
                    lane.lane_id, lane.peer_addr, error,
                );
                return ClobStandbyReadDisposition::Reconnect;
            }
            ClobStandbyReadDisposition::Keep
        }
        Some(Ok(Message::Close(reason))) => {
            let normalized_reason = reason
                .as_ref()
                .map(|frame| frame.reason.to_ascii_lowercase())
                .unwrap_or_default();
            let slow_consumer = ["slow consumer", "slow-consumer", "slow_consumer"]
                .iter()
                .any(|needle| normalized_reason.contains(needle));
            let tcp = sample_tcp_socket(lane.tcp_fd);
            let burst_summary = lane.burst.close_summary(received_at);
            if !slow_consumer || log_slow_consumer_incident {
                warn!(
                    "[clob_close_metric] lane_role=standby lane_id={} peer={:?} subscription_tokens={} reason={:?} server_slow_consumer={} tcp_unread_bytes={} tcp_rcv_wnd={} tcp_total_retrans={} so_rcvbuf={} {}",
                    lane.lane_id,
                    lane.peer_addr,
                    subscription_tokens,
                    reason,
                    slow_consumer,
                    tcp.unread_bytes.map(i64::from).unwrap_or(-1),
                    tcp.rcv_wnd.map(i64::from).unwrap_or(-1),
                    tcp.total_retrans.map(i64::from).unwrap_or(-1),
                    tcp.so_rcvbuf.map(i64::from).unwrap_or(-1),
                    burst_summary,
                );
            }
            if slow_consumer {
                ClobStandbyReadDisposition::SlowConsumerReconnect
            } else {
                ClobStandbyReadDisposition::Reconnect
            }
        }
        Some(Ok(_)) => {
            lane.record_raw(received_at);
            ClobStandbyReadDisposition::Keep
        }
        Some(Err(error)) => {
            warn!(
                "[clob_standby_read_failed] lane_id={} peer={:?} error={}",
                lane.lane_id, lane.peer_addr, error,
            );
            ClobStandbyReadDisposition::Reconnect
        }
        None => {
            warn!(
                "[clob_standby_closed] lane_id={} peer={:?}",
                lane.lane_id, lane.peer_addr,
            );
            ClobStandbyReadDisposition::Reconnect
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn promote_clob_standby(
    active: &mut ClobConnection,
    standby: &mut Option<ClobConnection>,
    standby_connect: &mut Option<
        tokio::task::JoinHandle<std::result::Result<ClobConnection, String>>,
    >,
    next_lane_id: &mut u64,
    subscription: &ClobSubscription,
    health: &mut WsHealth,
    books: &mut ClobLocalBooks,
    repair_tx: &mut tokio::sync::mpsc::Sender<ClobBookRepairResult>,
    repair_rx: &mut tokio::sync::mpsc::Receiver<ClobBookRepairResult>,
    repair_generation: &mut u64,
    repair_generation_epoch: &Arc<AtomicU64>,
    repairs_in_flight: &mut HashSet<String>,
    repair_superseded_attempts: &mut HashMap<String, u8>,
    liveness: &PolymarketLiveness,
    reason: &str,
) -> bool {
    let now = Instant::now();
    let Some(mut promoted) = standby.take() else {
        return false;
    };
    let standby_raw_age = now.saturating_duration_since(promoted.last_raw_at);
    if !promoted.is_hot_standby(now) {
        warn!(
            "[clob_failover_rejected] old_lane_id={} candidate_lane_id={} candidate_peer={:?} candidate_raw_age_ms={} max_raw_age_ms={} reason={}",
            active.lane_id,
            promoted.lane_id,
            promoted.peer_addr,
            standby_raw_age.as_millis(),
            CLOB_STANDBY_MAX_RAW_AGE.as_millis(),
            reason,
        );
        return false;
    }

    let old_lane_id = active.lane_id;
    let old_peer = active.peer_addr;
    std::mem::swap(active, &mut promoted);
    super::network_incident::update_ws_peers(active.peer_addr, None);
    // Delineate active processing from the preceding drain-only standby
    // window. The promoted socket and its kernel receive queue stay intact.
    active.diagnostics = ClobWindowMetrics::new(now);
    active.burst = ClobBurstMetrics::new(now);
    *health = WsHealth::new(now);

    // The standby was continuously drained but deliberately not parsed. It
    // can therefore contain one newer frame that the former active lane never
    // applied. Fail closed, discard the derived books, and reseed every token
    // from an authoritative full snapshot before READY is emitted again.
    reset_clob_books_for_failover(books, subscription, now);
    let (new_repair_tx, new_repair_rx) = tokio::sync::mpsc::channel::<ClobBookRepairResult>(256);
    *repair_tx = new_repair_tx;
    *repair_rx = new_repair_rx;
    *repair_generation = advance_clob_repair_generation(repair_generation_epoch);
    repairs_in_flight.clear();
    repair_superseded_attempts.clear();
    request_clob_book_repairs(
        subscription.tokens.clone(),
        &subscription.tokens,
        *repair_generation,
        repair_generation_epoch,
        repairs_in_flight,
        repair_tx,
    );
    liveness.mark_subscribed();

    if let Some(connect) = standby_connect.take() {
        connect.abort();
    }
    let replacement_lane_id = *next_lane_id;
    *next_lane_id = (*next_lane_id).saturating_add(1);
    *standby_connect = Some(spawn_clob_standby_connect(
        subscription.tokens.clone(),
        replacement_lane_id,
        Duration::ZERO,
    ));
    crate::latency::record_ns(
        "polymarket.ws.clob_hot_failover",
        now.elapsed().as_nanos().min(u64::MAX as u128) as u64,
    );
    warn!(
        "[clob_hot_failover] old_lane_id={} old_peer={:?} promoted_lane_id={} promoted_peer={:?} standby_raw_age_ms={} repair_tokens={} reason={}",
        old_lane_id,
        old_peer,
        active.lane_id,
        active.peer_addr,
        standby_raw_age.as_millis(),
        subscription.tokens.len(),
        reason,
    );
    true
}

async fn clob_ws_task(
    initial_subscription: ClobSubscription,
    event_tx: ClobEventSender,
    mut ctrl_rx: tokio::sync::mpsc::Receiver<WsCtrl>,
    shutdown: Arc<AtomicBool>,
    subscribed_once: Arc<AtomicBool>,
    liveness: Arc<PolymarketLiveness>,
) {
    // A sibling task on the same runtime distinguishes runtime-wide timer
    // starvation from delay inside this socket loop's biased select. If only
    // `clob_loop_scheduler_lag` rises, a ready higher-priority branch is
    // starving the inline timer; if both rise, the runtime/core itself was not
    // scheduled. Subtracting 1 ms exposes actionable lag above Tokio/Linux
    // timer-wheel granularity while retaining the raw series for comparison.
    let runtime_scheduler_max_us = Arc::new(AtomicU64::new(0));
    let runtime_probe_max = runtime_scheduler_max_us.clone();
    let runtime_probe_shutdown = shutdown.clone();
    let runtime_probe = tokio::spawn(async move {
        let mut probe = tokio::time::interval(CLOB_SCHEDULER_PROBE_INTERVAL);
        probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        probe.tick().await;
        while !runtime_probe_shutdown.load(Ordering::Relaxed) {
            let scheduled_at = probe.tick().await;
            let lag = scheduled_at.elapsed();
            let lag_ns = lag.as_nanos().min(u64::MAX as u128) as u64;
            crate::latency::record_ns("polymarket.ws.clob_runtime_scheduler_lag", lag_ns);
            crate::latency::record_ns(
                "polymarket.ws.clob_runtime_scheduler_over_1ms",
                lag_ns.saturating_sub(1_000_000),
            );
            runtime_probe_max.fetch_max(
                lag.as_micros().min(u64::MAX as u128) as u64,
                Ordering::Relaxed,
            );
        }
    });
    let mut subscription = initial_subscription;
    let mut wire_subscription = subscription.clone();
    let mut backoff = crate::exchange::ReconnectBackoff::new(200, 30_000);
    let mut next_lane_id = 1_u64;
    let mut diagnostic_sampler = ClobDiagnosticSampler::default();
    let mut thread_resource_sampler = ClobThreadResourceSampler::default();
    let clob_perf_ring = ClobPerfRing::start();
    let repair_generation_epoch = Arc::new(AtomicU64::new(0));
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
        // Fence every REST repair spawned by an older socket/subscription
        // generation before reconnect delay or control-message processing.
        let mut repair_generation =
            advance_clob_repair_generation(repair_generation_epoch.as_ref());

        // Drain any buffered ctrl messages — take the latest Resubscribe so
        // we don't churn through stale token lists if rotations piled up.
        loop {
            match ctrl_rx.try_recv() {
                Ok(WsCtrl::Resubscribe(new_subscription)) => {
                    subscription = new_subscription.clone();
                    wire_subscription = new_subscription;
                }
                Ok(WsCtrl::Prepare(new_subscription)) => {
                    wire_subscription = new_subscription;
                }
                Ok(WsCtrl::Shutdown) => break 'outer,
                Err(_) => break,
            }
        }

        liveness.mark_connecting();
        info!(
            "[Polymarket] Connecting to {} ({} tokens)",
            POLYMARKET_WS_URL,
            wire_subscription.tokens.len()
        );
        let active_lane_id = next_lane_id;
        next_lane_id = next_lane_id.saturating_add(1);
        let mut active = match connect_clob_lane(&wire_subscription.tokens, active_lane_id).await {
            Ok(lane) => lane,
            Err(error) => {
                announce_clob_not_ready(
                    &event_tx,
                    &mut lifecycle,
                    format!("WS connect failed: {error}"),
                );
                let delay = backoff.next_delay();
                warn!(
                    "[Polymarket] WS connect failed: {}, retry in {:.1}s",
                    error,
                    delay.as_secs_f64()
                );
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        super::network_incident::update_ws_peers(active.peer_addr, None);
        backoff.reset();
        let connected_at = Instant::now();
        let mut health = WsHealth::new(connected_at);
        let mut books = ClobLocalBooks::new(&wire_subscription.canonical_events);
        let (repair_tx, mut repair_rx) = tokio::sync::mpsc::channel::<ClobBookRepairResult>(256);
        let mut repair_tx = repair_tx;
        let mut repairs_in_flight = HashSet::new();
        let mut repair_superseded_attempts: HashMap<String, u8> = HashMap::new();
        let standby_lane_id = next_lane_id;
        next_lane_id = next_lane_id.saturating_add(1);
        let mut standby = None;
        let mut standby_ready_at: Option<Instant> = None;
        let mut standby_slow_consumer_streak = 0_u32;
        let mut standby_connect = Some(spawn_clob_standby_connect(
            wire_subscription.tokens.clone(),
            standby_lane_id,
            Duration::ZERO,
        ));
        let mut pending_cutover: Option<(ClobSubscription, bool)> = None;
        let mut candidate_connect: Option<
            tokio::task::JoinHandle<std::result::Result<ClobSeededCandidate, String>>,
        > = None;
        info!(
            "[Polymarket] Subscribed to {} tokens across {} canonical events",
            wire_subscription.tokens.len(),
            wire_subscription.canonical_events.len(),
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
            liveness.mark_subscribed();
        }

        let mut ping_interval = tokio::time::interval(POLYMARKET_WS_HEARTBEAT_INTERVAL);
        ping_interval.tick().await; // consume immediate tick
        let mut health_interval = tokio::time::interval(POLYMARKET_WS_HEALTH_LOG_INTERVAL);
        health_interval.tick().await;
        let mut burst_interval = tokio::time::interval(CLOB_BURST_METRIC_INTERVAL);
        burst_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        burst_interval.tick().await;
        let mut socket_unread_probe = tokio::time::interval(CLOB_SOCKET_UNREAD_PROBE_INTERVAL);
        socket_unread_probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        socket_unread_probe.tick().await;
        let mut coalesce_interval = tokio::time::interval(CLOB_BOOK_COALESCE_INTERVAL);
        coalesce_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        coalesce_interval.tick().await;
        let mut scheduler_probe = tokio::time::interval(CLOB_SCHEDULER_PROBE_INTERVAL);
        scheduler_probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        scheduler_probe.tick().await;
        let mut immediate_reconnect = false;
        let mut dual_silence_windows = 0_u8;

        loop {
            let deferred_deadline = books
                .next_deferred_deadline()
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(86_400));
            let deferred_sleep =
                tokio::time::sleep_until(tokio::time::Instant::from_std(deferred_deadline));
            tokio::pin!(deferred_sleep);
            tokio::select! {
                // Fair polling is intentional. With `biased`, bursts of due
                // repair/deferred/timer branches ahead of `read.next()` could
                // repeatedly postpone socket draining and trigger the venue's
                // slow-consumer close even though the engine queue was empty.
                // Control shutdown remains prompt because every branch is
                // bounded and the dedicated runtime has no unrelated work.

                ctrl = ctrl_rx.recv() => {
                    match ctrl {
                        Some(command) => {
                            let (new_subscription, activate) = match command {
                                WsCtrl::Resubscribe(subscription) => (subscription, true),
                                WsCtrl::Prepare(subscription) => (subscription, false),
                                WsCtrl::Shutdown => break 'outer,
                            };
                            let already_seeded = new_subscription.tokens.iter().all(|token| {
                                wire_subscription.tokens.iter().any(|active_token| active_token == token)
                            }) && books.has_all_seeded(&new_subscription.tokens);
                            if already_seeded {
                                // Boundary commit after an ahead-of-time union
                                // subscription: the target tokens already have
                                // L2 state, so only the logical routing set
                                // changes. The next candidate refresh drops the
                                // now-stale socket tokens.
                                wire_subscription = new_subscription.clone();
                                if activate {
                                    subscription = new_subscription;
                                }
                                repair_generation = advance_clob_repair_generation(
                                    repair_generation_epoch.as_ref(),
                                );
                                repairs_in_flight.clear();
                                repair_superseded_attempts.clear();
                                info!(
                                    "[clob_atomic_cutover] mode=preseeded_subset wire_tokens={} logical_tokens={} activate={} not_ready_ms=0",
                                    wire_subscription.tokens.len(),
                                    subscription.tokens.len(),
                                    activate,
                                );
                                continue;
                            }
                            if let Some(connect) = candidate_connect.take() {
                                connect.abort();
                            }
                            let lane_id = next_lane_id;
                            next_lane_id = next_lane_id.saturating_add(1);
                            pending_cutover = Some((new_subscription.clone(), activate));
                            candidate_connect = Some(spawn_clob_seeded_candidate(
                                new_subscription,
                                lane_id,
                                Duration::ZERO,
                            ));
                            info!(
                                "[clob_cutover_prepare] lane_id={} current_tokens={} target_tokens={} action=keep_active_until_l2_seed",
                                lane_id,
                                subscription.tokens.len(),
                                pending_cutover.as_ref().map_or(0, |(pending, _)| pending.tokens.len()),
                            );
                        }
                        None => break 'outer,
                    }
                }

                candidate_result = async {
                    candidate_connect
                        .as_mut()
                        .expect("candidate branch is guarded")
                        .await
                }, if candidate_connect.is_some() => {
                    candidate_connect = None;
                    match candidate_result {
                        Ok(Ok(mut candidate)) => {
                            let Some((target, activate)) = pending_cutover.take() else {
                                drop(candidate);
                                continue;
                            };
                            if let Some(connect) = standby_connect.take() {
                                connect.abort();
                            }
                            standby = None;
                            wire_subscription = target.clone();
                            if activate {
                                subscription = target;
                            }
                            let cutover_at = Instant::now();
                            active = candidate.lane;
                            books = candidate.books;
                            health = WsHealth::new(cutover_at);
                            repairs_in_flight.clear();
                            repair_superseded_attempts.clear();
                            let (new_repair_tx, new_repair_rx) =
                                tokio::sync::mpsc::channel::<ClobBookRepairResult>(256);
                            repair_tx = new_repair_tx;
                            repair_rx = new_repair_rx;
                            repair_generation = advance_clob_repair_generation(
                                repair_generation_epoch.as_ref(),
                            );
                            super::network_incident::update_ws_peers(active.peer_addr, None);
                            liveness.mark_subscribed();
                            liveness.record_market_data(clob_monotonic_now_ns());
                            if !forward_clob_events(
                                std::mem::take(&mut candidate.seed_events),
                                &event_tx,
                                &mut lifecycle,
                                &mut health,
                                &mut active.diagnostics,
                                &books,
                                &subscription.tokens,
                                cutover_at,
                            ) {
                                break 'outer;
                            }
                            info!(
                                "[clob_atomic_cutover] mode=seeded_candidate lane_id={} peer={:?} wire_tokens={} logical_tokens={} activate={} l2_seed_ms={:.3} not_ready_ms=0",
                                active.lane_id,
                                active.peer_addr,
                                wire_subscription.tokens.len(),
                                subscription.tokens.len(),
                                activate,
                                candidate.warmup.as_secs_f64() * 1_000.0,
                            );
                            let lane_id = next_lane_id;
                            next_lane_id = next_lane_id.saturating_add(1);
                            standby_connect = Some(spawn_clob_standby_connect(
                                wire_subscription.tokens.clone(),
                                lane_id,
                                Duration::ZERO,
                            ));
                        }
                        Ok(Err(error)) => {
                            warn!("[clob_cutover_prepare_failed] error={error}; active lane retained");
                            if let Some((target, _)) = pending_cutover.as_ref().cloned() {
                                let lane_id = next_lane_id;
                                next_lane_id = next_lane_id.saturating_add(1);
                                candidate_connect = Some(spawn_clob_seeded_candidate(
                                    target,
                                    lane_id,
                                    CLOB_STANDBY_RECONNECT_DELAY,
                                ));
                            }
                        }
                        Err(error) => {
                            warn!("[clob_cutover_prepare_failed] join_error={error}; active lane retained");
                            if let Some((target, _)) = pending_cutover.as_ref().cloned() {
                                let lane_id = next_lane_id;
                                next_lane_id = next_lane_id.saturating_add(1);
                                candidate_connect = Some(spawn_clob_seeded_candidate(
                                    target,
                                    lane_id,
                                    CLOB_STANDBY_RECONNECT_DELAY,
                                ));
                            }
                        }
                    }
                }

                standby_result = async {
                    standby_connect
                        .as_mut()
                        .expect("standby connect branch is guarded")
                        .await
                }, if standby_connect.is_some() => {
                    standby_connect = None;
                    match standby_result {
                        Ok(Ok(lane)) if clob_peers_are_anti_affine(active.peer_addr, lane.peer_addr) => {
                            info!(
                                "[clob_standby_ready] lane_id={} peer={:?} active_lane_id={} active_peer={:?}",
                                lane.lane_id,
                                lane.peer_addr,
                                active.lane_id,
                                active.peer_addr,
                            );
                            super::network_incident::update_ws_peers(active.peer_addr, lane.peer_addr);
                            standby_ready_at = Some(Instant::now());
                            standby = Some(lane);
                        }
                        Ok(Ok(lane)) => {
                            super::network_incident::update_ws_peers(active.peer_addr, lane.peer_addr);
                            super::network_incident::record(
                                super::network_incident::NetworkSignal::PeerCollision,
                                "standby candidate rejected; reconnecting for a distinct peer IP",
                            );
                            warn!(
                                "[clob_peer_collision] candidate_lane_id={} candidate_peer={:?} active_lane_id={} active_peer={:?}; retrying",
                                lane.lane_id,
                                lane.peer_addr,
                                active.lane_id,
                                active.peer_addr,
                            );
                            super::network_incident::update_ws_peers(active.peer_addr, None);
                            drop(lane);
                            let lane_id = next_lane_id;
                            next_lane_id = next_lane_id.saturating_add(1);
                            standby_connect = Some(spawn_clob_standby_connect(
                                wire_subscription.tokens.clone(),
                                lane_id,
                                CLOB_STANDBY_RECONNECT_DELAY,
                            ));
                        }
                        Ok(Err(error)) => {
                            warn!("[clob_standby_connect_failed] error={error}; retrying");
                            let lane_id = next_lane_id;
                            next_lane_id = next_lane_id.saturating_add(1);
                            standby_connect = Some(spawn_clob_standby_connect(
                                wire_subscription.tokens.clone(),
                                lane_id,
                                CLOB_STANDBY_RECONNECT_DELAY,
                            ));
                        }
                        Err(error) => {
                            warn!("[clob_standby_connect_failed] join_error={error}; retrying");
                            let lane_id = next_lane_id;
                            next_lane_id = next_lane_id.saturating_add(1);
                            standby_connect = Some(spawn_clob_standby_connect(
                                wire_subscription.tokens.clone(),
                                lane_id,
                                CLOB_STANDBY_RECONNECT_DELAY,
                            ));
                        }
                    }
                }

                _ = &mut deferred_sleep => {
                    let now = Instant::now();
                    let mut batch =
                        books.flush_deferred_due(now, now_ns(), &subscription.tokens);
                    retain_active_clob_deferred_diagnostics(&mut batch, &subscription.tokens);
                    active.diagnostics.record_deferred(&batch);
                    for diagnostic in batch.diagnostics {
                        diagnostic_sampler.observe(now, diagnostic);
                    }
                    for token in &batch.repair_tokens {
                        repair_superseded_attempts.entry(token.clone()).or_insert(0);
                    }
                    request_clob_book_repairs(
                        batch.repair_tokens,
                        &subscription.tokens,
                        repair_generation,
                        &repair_generation_epoch,
                        &mut repairs_in_flight,
                        &repair_tx,
                    );
                    if !forward_clob_events(
                        batch.events,
                        &event_tx,
                        &mut lifecycle,
                        &mut health,
                        &mut active.diagnostics,
                        &books,
                        &subscription.tokens,
                        now,
                    ) {
                        break 'outer;
                    }
                }

                repair = repair_rx.recv(), if !repairs_in_flight.is_empty() => {
                    let Some(repair) = repair else {
                        continue;
                    };
                    if repair.generation != repair_generation
                        || !subscription.tokens.contains(&repair.token)
                    {
                        log::debug!(
                            "[clob_bbo_repair_skipped] token={} result_generation={} active_generation={} reason=stale_or_retired_result",
                            repair.token,
                            repair.generation,
                            repair_generation,
                        );
                        continue;
                    }
                    repairs_in_flight.remove(&repair.token);
                    if !books.quarantined_tokens.contains(&repair.token) {
                        repair_superseded_attempts.remove(&repair.token);
                        continue;
                    }
                    let now = Instant::now();
                    let local_now = now_ns();
                    match repair.result {
                        Ok(fields) => match books.apply_book(
                            fields,
                            now,
                            local_now,
                            ClobBookSource::RestRepair,
                            &mut active.diagnostics.wire,
                        ) {
                            Ok(ClobBookApplyOutcome::Applied(events)) => {
                                repair_superseded_attempts.remove(&repair.token);
                                info!(
                                    "[Polymarket] authoritative BBO repair completed token={}",
                                    repair.token,
                                );
                                if !events.is_empty() {
                                    active.diagnostics.record_coalesced(&events);
                                    if !forward_clob_events(
                                        events,
                                        &event_tx,
                                        &mut lifecycle,
                                        &mut health,
                                        &mut active.diagnostics,
                                        &books,
                                        &subscription.tokens,
                                        now,
                                    ) {
                                        break 'outer;
                                    }
                                }
                            }
                            Ok(ClobBookApplyOutcome::Superseded {
                                incoming_timestamp_ns,
                                current_timestamp_ns,
                            }) => {
                                let attempts = repair_superseded_attempts
                                    .entry(repair.token.clone())
                                    .or_insert(0);
                                *attempts = attempts.saturating_add(1);
                                let retry = *attempts <= CLOB_BBO_MAX_SUPERSEDED_REPAIRS;
                                diagnostic_sampler.observe(now, ClobDiagnostic {
                                    key: "bbo_repair_superseded_by_ws",
                                    detail: format!(
                                        "token={} repair_ts={} ws_ts={} attempt={} action={}",
                                        repair.token,
                                        incoming_timestamp_ns,
                                        current_timestamp_ns,
                                        *attempts,
                                        if retry { "retry" } else { "degrade" },
                                    ),
                                });
                                if retry {
                                    let delay = Duration::from_millis(
                                        CLOB_BBO_SETTLE_INTERVAL.as_millis() as u64
                                            * (1_u64 << attempts.saturating_sub(1)),
                                    );
                                    request_clob_book_repair_after(
                                        repair.token,
                                        delay,
                                        &subscription.tokens,
                                        repair_generation,
                                        &repair_generation_epoch,
                                        &mut repairs_in_flight,
                                        &repair_tx,
                                    );
                                } else if let Some(event) = books.mark_repair_failed(
                                    &repair.token,
                                    "authoritative BBO repair remained older than websocket state",
                                    now,
                                    local_now,
                                ) {
                                    repair_superseded_attempts.remove(&repair.token);
                                    active.diagnostics.record_coalesced(std::slice::from_ref(&event));
                                    if !forward_clob_events(
                                        vec![event],
                                        &event_tx,
                                        &mut lifecycle,
                                        &mut health,
                                        &mut active.diagnostics,
                                        &books,
                                        &subscription.tokens,
                                        now,
                                    ) {
                                        break 'outer;
                                    }
                                }
                            }
                            Err(error) => {
                                repair_superseded_attempts.remove(&repair.token);
                                diagnostic_sampler.observe(now, ClobDiagnostic {
                                    key: "bbo_repair_failed",
                                    detail: format!("token={} reason={error}", repair.token),
                                });
                                if let Some(event) = books.mark_repair_failed(
                                    &repair.token,
                                    format!("authoritative BBO repair rejected: {error}"),
                                    now,
                                    local_now,
                                ) {
                                    active.diagnostics.record_coalesced(std::slice::from_ref(&event));
                                    if !forward_clob_events(
                                        vec![event],
                                        &event_tx,
                                        &mut lifecycle,
                                        &mut health,
                                        &mut active.diagnostics,
                                        &books,
                                        &subscription.tokens,
                                        now,
                                    ) {
                                        break 'outer;
                                    }
                                }
                            }
                        },
                        Err(error) => {
                            repair_superseded_attempts.remove(&repair.token);
                            diagnostic_sampler.observe(now, ClobDiagnostic {
                                key: "bbo_repair_failed",
                                detail: format!("token={} reason={error}", repair.token),
                            });
                            if let Some(event) = books.mark_repair_failed(
                                &repair.token,
                                format!("authoritative BBO repair failed: {error}"),
                                now,
                                local_now,
                            ) {
                                active.diagnostics.record_coalesced(std::slice::from_ref(&event));
                                if !forward_clob_events(
                                    vec![event],
                                    &event_tx,
                                    &mut lifecycle,
                                    &mut health,
                                    &mut active.diagnostics,
                                    &books,
                                    &subscription.tokens,
                                    now,
                                ) {
                                    break 'outer;
                                }
                            }
                        }
                    }
                }

                scheduled_at = scheduler_probe.tick() => {
                    let lag = scheduled_at.elapsed();
                    active.diagnostics.record_loop_scheduler(lag);
                    active.diagnostics.runtime_scheduler_max_us = active.diagnostics.runtime_scheduler_max_us.max(
                        runtime_scheduler_max_us.load(Ordering::Relaxed),
                    );
                    let lag_ns = lag.as_nanos().min(u64::MAX as u128) as u64;
                    crate::latency::record_ns(
                        "polymarket.ws.clob_scheduler_lag",
                        lag_ns,
                    );
                    crate::latency::record_ns(
                        "polymarket.ws.clob_loop_scheduler_lag",
                        lag_ns,
                    );
                    crate::latency::record_ns(
                        "polymarket.ws.clob_loop_scheduler_over_1ms",
                        lag_ns.saturating_sub(1_000_000),
                    );
                }

                _ = ping_interval.tick() => {
                    let now = Instant::now();
                    // Send both the CLOB application-level text heartbeat and
                    // a WebSocket protocol Ping frame every 5s.
                    if let Err(e) = timed_clob_ws_send(
                        &mut active.write,
                        Message::Text("PING".to_string()),
                        "polymarket.ws.clob_send.heartbeat_text",
                        &mut active.diagnostics,
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
                        &mut active.write,
                        Message::Ping(Vec::new()),
                        "polymarket.ws.clob_send.heartbeat_frame",
                        &mut active.diagnostics,
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
                    if let Some(lane) = standby.as_mut() {
                        let text_result = timed_clob_ws_send(
                            &mut lane.write,
                            Message::Text("PING".to_string()),
                            "polymarket.ws.clob_standby_send.heartbeat_text",
                            &mut lane.diagnostics,
                        ).await;
                        let frame_result = if text_result.is_ok() {
                            timed_clob_ws_send(
                                &mut lane.write,
                                Message::Ping(Vec::new()),
                                "polymarket.ws.clob_standby_send.heartbeat_frame",
                                &mut lane.diagnostics,
                            ).await
                        } else {
                            text_result
                        };
                        if let Err(error) = frame_result {
                            warn!(
                                "[clob_standby_send_failed] lane_id={} peer={:?} error={}",
                                lane.lane_id,
                                lane.peer_addr,
                                error,
                            );
                            standby = None;
                            super::network_incident::update_ws_peers(active.peer_addr, None);
                            let lane_id = next_lane_id;
                            next_lane_id = next_lane_id.saturating_add(1);
                            standby_connect = Some(spawn_clob_standby_connect(
                                wire_subscription.tokens.clone(),
                                lane_id,
                                CLOB_STANDBY_RECONNECT_DELAY,
                            ));
                        }
                    }
                }

                _ = coalesce_interval.tick() => {
                    let now = Instant::now();
                    let events = books.flush_due(now, now_ns());
                    if !events.is_empty() {
                        active.diagnostics.record_coalesced(&events);
                        if !forward_clob_events(
                            events,
                            &event_tx,
                            &mut lifecycle,
                            &mut health,
                            &mut active.diagnostics,
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
                    let both_silent = standby.as_ref().is_some_and(|lane| {
                        active.observed_data
                            && lane.observed_data
                            && active.burst.frames == 0
                            && lane.burst.frames == 0
                    });
                    if both_silent {
                        dual_silence_windows = dual_silence_windows.saturating_add(1);
                        if dual_silence_windows == CLOB_DUAL_SILENCE_WINDOWS {
                            let detail = format!(
                                "windows={} active_raw_age_ms={} standby_raw_age_ms={}",
                                dual_silence_windows,
                                now.saturating_duration_since(active.last_raw_at).as_millis(),
                                standby
                                    .as_ref()
                                    .map(|lane| now.saturating_duration_since(lane.last_raw_at).as_millis())
                                    .unwrap_or_default(),
                            );
                            super::network_incident::record(
                                super::network_incident::NetworkSignal::DualWsSilence,
                                &detail,
                            );
                        }
                    } else {
                        dual_silence_windows = 0;
                    }
                    active.burst.record_socket_polls(active.read.take_poll_window());
                    active.burst.log_and_reset(
                        now,
                        sample_tcp_socket(active.tcp_fd),
                        active.peer_addr,
                        "active",
                        active.lane_id,
                        wire_subscription.tokens.len(),
                        &event_tx,
                        &active.diagnostics,
                    );
                    if let Some(lane) = standby.as_mut() {
                        lane.burst.record_socket_polls(lane.read.take_poll_window());
                        lane.burst.log_and_reset(
                            now,
                            sample_tcp_socket(lane.tcp_fd),
                            lane.peer_addr,
                            "standby",
                            lane.lane_id,
                            wire_subscription.tokens.len(),
                            &event_tx,
                            &lane.diagnostics,
                        );
                    }
                }

                _ = socket_unread_probe.tick() => {
                    let now = Instant::now();
                    let probe_started = Instant::now();
                    let unread_bytes = sample_socket_unread_bytes(active.tcp_fd);
                    let probe_elapsed = probe_started.elapsed();
                    active.burst.record_socket_probe(now, unread_bytes, probe_elapsed);
                    crate::latency::record_ns(
                        "polymarket.ws.clob_socket_unread_probe",
                        probe_elapsed.as_nanos().min(u64::MAX as u128) as u64,
                    );
                    if let Some(lane) = standby.as_mut() {
                        let probe_started = Instant::now();
                        let unread_bytes = sample_socket_unread_bytes(lane.tcp_fd);
                        let probe_elapsed = probe_started.elapsed();
                        lane.burst.record_socket_probe(now, unread_bytes, probe_elapsed);
                        crate::latency::record_ns(
                            "polymarket.ws.clob_standby_socket_unread_probe",
                            probe_elapsed.as_nanos().min(u64::MAX as u128) as u64,
                        );
                    }
                }

                _ = health_interval.tick() => {
                    let now = Instant::now();
                    active.diagnostics.log_and_reset(now, event_tx.len());
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
                    if has_complete_clob_subscription(&wire_subscription.tokens)
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

                read_result = tokio::time::timeout(
                    CLOB_STALE_THRESHOLD,
                    next_clob_lane(&mut active, standby.as_mut()),
                ) => {
                    let lane_read = match read_result {
                        Ok(lane_read) => lane_read,
                        Err(_elapsed) => {
                            announce_clob_not_ready(
                                &event_tx,
                                &mut lifecycle,
                                format!(
                                    "CLOB no message on either lane for {:.0}s",
                                    CLOB_STALE_THRESHOLD.as_secs_f64(),
                                ),
                            );
                            let now = Instant::now();
                            warn!(
                                "[Polymarket] CLOB no raw frame on either lane for {:.0}s (stall watchdog) — reconnecting; {}",
                                CLOB_STALE_THRESHOLD.as_secs_f64(),
                                health.clob_summary(now),
                            );
                            break;
                        }
                    };

                    let result = match lane_read {
                        ClobLaneRead::Standby(result) => {
                            let lane = standby
                                .as_mut()
                                .expect("standby read result requires a standby lane");
                            lane.burst.record_socket_polls(lane.read.take_poll_window());
                            let disposition = handle_clob_standby_read(
                                lane,
                                result,
                                wire_subscription.tokens.len(),
                                standby_slow_consumer_streak == 0,
                            ).await;
                            if disposition != ClobStandbyReadDisposition::Keep {
                                if standby_ready_at.is_some_and(|ready_at| {
                                    ready_at.elapsed() >= CLOB_STANDBY_HEALTHY_RESET
                                }) {
                                    standby_slow_consumer_streak = 0;
                                }
                                let reconnect_delay = if disposition
                                    == ClobStandbyReadDisposition::SlowConsumerReconnect
                                {
                                    standby_slow_consumer_streak =
                                        standby_slow_consumer_streak.saturating_add(1);
                                    let delay = clob_standby_slow_consumer_delay(
                                        standby_slow_consumer_streak,
                                        lane.lane_id,
                                    );
                                    super::network_incident::suppress_peer_collision_for(
                                        delay + CLOB_STANDBY_RECONNECT_DELAY,
                                    );
                                    if standby_slow_consumer_streak == 1
                                        || standby_slow_consumer_streak.is_power_of_two()
                                    {
                                        warn!(
                                            "[clob_standby_backoff] reason=server_slow_consumer streak={} dedup_repeats={} delay_ms={} active_lane_retained=true",
                                            standby_slow_consumer_streak,
                                            standby_slow_consumer_streak.saturating_sub(1),
                                            delay.as_millis(),
                                        );
                                    }
                                    delay
                                } else {
                                    CLOB_STANDBY_RECONNECT_DELAY
                                };
                                standby = None;
                                standby_ready_at = None;
                                super::network_incident::update_ws_peers(active.peer_addr, None);
                                let lane_id = next_lane_id;
                                next_lane_id = next_lane_id.saturating_add(1);
                                standby_connect = Some(spawn_clob_standby_connect(
                                    wire_subscription.tokens.clone(),
                                    lane_id,
                                    reconnect_delay,
                                ));
                            }
                            continue;
                        }
                        ClobLaneRead::Active(result) => result,
                    };
                    active.burst.record_socket_polls(active.read.take_poll_window());
                    let msg = match result {
                        Some(Ok(message)) => message,
                        Some(Err(error)) => {
                            let failover_reason = format!("active WS read error: {error}");
                            announce_clob_not_ready(&event_tx, &mut lifecycle, &failover_reason);
                            let now = Instant::now();
                            warn!(
                                "[Polymarket] active WS read error: {} — failover/reconnect; {}",
                                error,
                                health.clob_summary(now),
                            );
                            if promote_clob_standby(
                                &mut active,
                                &mut standby,
                                &mut standby_connect,
                                &mut next_lane_id,
                                &wire_subscription,
                                &mut health,
                                &mut books,
                                &mut repair_tx,
                                &mut repair_rx,
                                &mut repair_generation,
                                &repair_generation_epoch,
                                &mut repairs_in_flight,
                                &mut repair_superseded_attempts,
                                &liveness,
                                &failover_reason,
                            ) {
                                continue;
                            }
                            break;
                        }
                        None => {
                            let failover_reason = "active WS closed";
                            announce_clob_not_ready(&event_tx, &mut lifecycle, failover_reason);
                            let now = Instant::now();
                            warn!(
                                "[Polymarket] active WS closed — failover/reconnect; {}",
                                health.clob_summary(now),
                            );
                            if promote_clob_standby(
                                &mut active,
                                &mut standby,
                                &mut standby_connect,
                                &mut next_lane_id,
                                &wire_subscription,
                                &mut health,
                                &mut books,
                                &mut repair_tx,
                                &mut repair_rx,
                                &mut repair_generation,
                                &repair_generation_epoch,
                                &mut repairs_in_flight,
                                &mut repair_superseded_attempts,
                                &liveness,
                                failover_reason,
                            ) {
                                continue;
                            }
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
                            active.record_raw(received_at);
                            health.record_raw_frame(received_at);
                            liveness.record_raw_frame(clob_monotonic_now_ns());
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
                                    &mut active.write,
                                    Message::Text("PONG".to_string()),
                                    "polymarket.ws.clob_send.text_pong",
                                    &mut active.diagnostics,
                                ).await;
                                continue;
                            }
                            let resource_start = thread_resource_sampler.begin_frame();
                            let t_parse = crate::latency::Instant::now();
                            let frame_len = text.len();
                            let mut frame_bytes = text.into_bytes();
                            let mut frame_phases = ClobFramePhaseTimings::default();
                            let mut batch = process_clob_frame_in_place(
                                &mut frame_bytes,
                                &mut books,
                                &wire_subscription.tokens,
                                &subscription.tokens,
                                received_at,
                                now_ns(),
                                &mut frame_phases,
                            );
                            frame_phases.record();
                            let parse_apply_elapsed = t_parse.elapsed();
                            let parse_cpu_ns = clob_thread_cpu_ns()
                                .saturating_sub(resource_start.cpu_ns);
                            let parse_wall_ns = parse_apply_elapsed
                                .as_nanos()
                                .min(u64::MAX as u128) as u64;
                            let parse_preempted_ns = parse_wall_ns.saturating_sub(parse_cpu_ns);
                            crate::latency::record_ns(
                                "polymarket.ws.clob_parse_apply_cpu",
                                parse_cpu_ns,
                            );
                            crate::latency::record_ns(
                                "polymarket.ws.clob_parse_apply_preempted",
                                parse_preempted_ns,
                            );
                            retain_active_clob_diagnostics(&mut batch, &subscription.tokens);
                            active.diagnostics.record_parse_apply(parse_apply_elapsed);
                            crate::latency::record_ns(
                                "polymarket.ws.clob_parse_apply",
                                parse_apply_elapsed.as_nanos().min(u64::MAX as u128) as u64,
                            );
                            active.burst.record_frame(received_at, frame_len);
                            active.diagnostics.record_frame(received_at, frame_len, &batch);
                            if batch.recognized_topic {
                                health.record_topic_frame(received_at);
                                liveness.record_market_data(clob_monotonic_now_ns());
                            }
                            for diagnostic in batch.diagnostics {
                                diagnostic_sampler.observe(received_at, diagnostic);
                            }
                            for token in &batch.repair_tokens {
                                repair_superseded_attempts.entry(token.clone()).or_insert(0);
                            }
                            request_clob_book_repairs(
                                batch.repair_tokens,
                                &subscription.tokens,
                                repair_generation,
                                &repair_generation_epoch,
                                &mut repairs_in_flight,
                                &repair_tx,
                            );
                            if !forward_clob_events(
                                batch.events,
                                &event_tx,
                                &mut lifecycle,
                                &mut health,
                                &mut active.diagnostics,
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
                            let read_handler_elapsed = received_at.elapsed();
                            if read_handler_elapsed >= CLOB_RARE_HANDLER_TAIL {
                                let (cpu_ns, preempted_ns, resources, resource_span_frames) =
                                    thread_resource_sampler.tail_delta(
                                        resource_start,
                                        parse_apply_elapsed,
                                    );
                                diagnostic_sampler.observe(received_at, ClobDiagnostic {
                                    key: "clob_read_handler_tail",
                                    detail: format!(
                                        "frame_bytes={} total_us={} parse_apply_us={} simd_json_us={} book_apply_us={} price_change_apply_us={} event_construction_us={} parse_cpu_us={} preempted_us={} non_parse_us={} voluntary_cs_delta={} involuntary_cs_delta={} minor_fault_delta={} major_fault_delta={} resource_span_frames={} runtime_scheduler_max_us={}",
                                        frame_len,
                                        read_handler_elapsed.as_micros(),
                                        parse_apply_elapsed.as_micros(),
                                        frame_phases.simd_json_ns / 1_000,
                                        frame_phases.book_apply_ns / 1_000,
                                        frame_phases.price_change_apply_ns / 1_000,
                                        frame_phases.event_construction_ns / 1_000,
                                        cpu_ns / 1_000,
                                        preempted_ns / 1_000,
                                        read_handler_elapsed
                                            .saturating_sub(parse_apply_elapsed)
                                            .as_micros(),
                                        resources.voluntary_switches,
                                        resources.involuntary_switches,
                                        resources.minor_faults,
                                        resources.major_faults,
                                        resource_span_frames,
                                        runtime_scheduler_max_us.load(Ordering::Relaxed),
                                    ),
                                });
                                maybe_trigger_clob_perf(
                                    clob_perf_ring.as_ref(),
                                    parse_apply_elapsed,
                                    frame_len,
                                    frame_phases,
                                );
                            }
                            active.diagnostics.record_read_handler(read_handler_elapsed);
                        }
                        Message::Ping(payload) => {
                            active.record_raw(received_at);
                            health.record_raw_frame(received_at);
                            liveness.record_raw_frame(clob_monotonic_now_ns());
                            let _ = timed_clob_ws_send(
                                &mut active.write,
                                Message::Pong(payload),
                                "polymarket.ws.clob_send.frame_pong",
                                &mut active.diagnostics,
                            ).await;
                            active.diagnostics.record_read_handler(received_at.elapsed());
                        }
                        Message::Pong(_) => {
                            active.record_raw(received_at);
                            health.record_raw_frame(received_at);
                            health.record_pong(received_at);
                            liveness.record_raw_frame(clob_monotonic_now_ns());
                            active.diagnostics.record_read_handler(received_at.elapsed());
                        }
                        Message::Close(reason) => {
                            active.diagnostics.runtime_scheduler_max_us = active.diagnostics
                                .runtime_scheduler_max_us
                                .max(runtime_scheduler_max_us.load(Ordering::Relaxed));
                            let mut server_slow_consumer = false;
                            match reason.as_ref() {
                                Some(frame) => {
                                    let normalized_reason = frame.reason.to_ascii_lowercase();
                                    server_slow_consumer = [
                                        "slow consumer",
                                        "slow-consumer",
                                        "slow_consumer",
                                    ]
                                    .iter()
                                    .any(|needle| normalized_reason.contains(needle));
                                    let tcp = sample_tcp_socket(active.tcp_fd);
                                    let burst_summary = active.burst.close_summary(received_at);
                                    warn!(
                                        "[clob_close_metric] lane_role=active lane_id={} peer={:?} subscription_tokens={} code={:?} reason={:?} server_slow_consumer={} tcp_unread_bytes={} tcp_rcv_space={} tcp_rcv_wnd={} tcp_total_retrans={} so_rcvbuf={} {} {} {}",
                                        active.lane_id,
                                        active.peer_addr,
                                        wire_subscription.tokens.len(),
                                        frame.code,
                                        frame.reason,
                                        server_slow_consumer,
                                        tcp.unread_bytes.map(i64::from).unwrap_or(-1),
                                        tcp.rcv_space.map(i64::from).unwrap_or(-1),
                                        tcp.rcv_wnd.map(i64::from).unwrap_or(-1),
                                        tcp.total_retrans.map(i64::from).unwrap_or(-1),
                                        tcp.so_rcvbuf.map(i64::from).unwrap_or(-1),
                                        health.clob_summary(received_at),
                                        active.diagnostics.close_summary(received_at, event_tx.len()),
                                        burst_summary,
                                    );
                                }
                                None => {
                                    let tcp = sample_tcp_socket(active.tcp_fd);
                                    let burst_summary = active.burst.close_summary(received_at);
                                    warn!(
                                        "[clob_close_metric] lane_role=active lane_id={} peer={:?} subscription_tokens={} code=none reason=none server_slow_consumer=false tcp_unread_bytes={} tcp_rcv_space={} tcp_rcv_wnd={} tcp_total_retrans={} so_rcvbuf={} {} {} {}",
                                        active.lane_id,
                                        active.peer_addr,
                                        wire_subscription.tokens.len(),
                                        tcp.unread_bytes.map(i64::from).unwrap_or(-1),
                                        tcp.rcv_space.map(i64::from).unwrap_or(-1),
                                        tcp.rcv_wnd.map(i64::from).unwrap_or(-1),
                                        tcp.total_retrans.map(i64::from).unwrap_or(-1),
                                        tcp.so_rcvbuf.map(i64::from).unwrap_or(-1),
                                        health.clob_summary(received_at),
                                        active.diagnostics.close_summary(received_at, event_tx.len()),
                                        burst_summary,
                                    );
                                }
                            }
                            warn!(
                                "[Polymarket] Server closed active WS {:?} — failover/reconnect; {}",
                                reason,
                                health.clob_summary(received_at),
                            );
                            let failover_reason = if server_slow_consumer {
                                "active server slow-consumer close"
                            } else {
                                "active server close"
                            };
                            announce_clob_not_ready(&event_tx, &mut lifecycle, failover_reason);
                            if promote_clob_standby(
                                &mut active,
                                &mut standby,
                                &mut standby_connect,
                                &mut next_lane_id,
                                &wire_subscription,
                                &mut health,
                                &mut books,
                                &mut repair_tx,
                                &mut repair_rx,
                                &mut repair_generation,
                                &repair_generation_epoch,
                                &mut repairs_in_flight,
                                &mut repair_superseded_attempts,
                                &liveness,
                                failover_reason,
                            ) {
                                continue;
                            }
                            immediate_reconnect = server_slow_consumer;
                            break;
                        }
                        _ => {
                            active.record_raw(received_at);
                            health.record_raw_frame(received_at);
                            liveness.record_raw_frame(clob_monotonic_now_ns());
                            active.diagnostics.record_read_handler(received_at.elapsed());
                        }
                    }
                }
            }
        }

        // Inner loop broke → retire any in-flight standby handshake before
        // reconnecting or shutting down this subscription generation.
        if let Some(connect) = standby_connect.take() {
            connect.abort();
        }
        if let Some(connect) = candidate_connect.take() {
            connect.abort();
        }
        if let Some((target, activate)) = pending_cutover.take() {
            wire_subscription = target.clone();
            if activate {
                subscription = target;
            }
        }
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        if immediate_reconnect {
            warn!("[clob_fast_reconnect] skipping backoff after server slow-consumer close");
            continue;
        }
        let delay = backoff.next_delay();
        tokio::time::sleep(delay).await;
    }

    runtime_probe.abort();
    info!("[Polymarket] CLOB WS task exiting");
}

/// RTDS async task: connects to wss://ws-live-data.polymarket.com, subscribes,
/// reads messages, and sends SpotPrice events to the engine channel.
/// Auto-reconnects with backoff.
async fn rtds_task(
    subscriptions: Vec<RtdsSubscription>,
    tx: crate::exchange::PublicMarketPublisher,
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
    tx: &crate::exchange::PublicMarketPublisher,
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
                        crate::exchange::publish_market_event(tx, event)
                            .map_err(|error| anyhow!(
                                "RTDS engine market mailbox disconnected: {}", error,
                            ))?;
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
struct BookLevel<'a> {
    #[serde(borrow)]
    price: std::borrow::Cow<'a, str>,
    #[serde(borrow)]
    size: std::borrow::Cow<'a, str>,
}

#[derive(serde::Deserialize)]
struct BookFields<'a> {
    #[serde(borrow)]
    asset_id: std::borrow::Cow<'a, str>,
    #[serde(default)]
    bids: Vec<BookLevel<'a>>,
    #[serde(default)]
    asks: Vec<BookLevel<'a>>,
    /// Polymarket normally emits stringified milliseconds; accept JSON
    /// numbers too so server timestamps always drive event-level ordering.
    #[serde(default)]
    timestamp: Option<serde_json::Value>,
}

impl BookFields<'_> {
    fn into_owned(self) -> BookFields<'static> {
        BookFields {
            asset_id: std::borrow::Cow::Owned(self.asset_id.into_owned()),
            bids: self
                .bids
                .into_iter()
                .map(|level| BookLevel {
                    price: std::borrow::Cow::Owned(level.price.into_owned()),
                    size: std::borrow::Cow::Owned(level.size.into_owned()),
                })
                .collect(),
            asks: self
                .asks
                .into_iter()
                .map(|level| BookLevel {
                    price: std::borrow::Cow::Owned(level.price.into_owned()),
                    size: std::borrow::Cow::Owned(level.size.into_owned()),
                })
                .collect(),
            timestamp: self.timestamp,
        }
    }
}

#[derive(serde::Deserialize)]
struct TradeFields<'a> {
    #[serde(borrow)]
    asset_id: std::borrow::Cow<'a, str>,
    #[serde(borrow)]
    price: std::borrow::Cow<'a, str>,
    #[serde(borrow)]
    size: std::borrow::Cow<'a, str>,
    #[serde(borrow)]
    side: std::borrow::Cow<'a, str>, // "BUY" | "SELL"
    #[serde(default)]
    timestamp: Option<serde_json::Value>,
    /// Venue execution identity. A transaction can contain multiple fills, so
    /// its hash alone is deliberately not accepted as a trade id.
    #[serde(
        default,
        borrow,
        alias = "trade_id",
        alias = "tradeId",
        alias = "executionId"
    )]
    execution_id: Option<std::borrow::Cow<'a, str>>,
    #[serde(default, borrow, alias = "transactionHash")]
    transaction_hash: Option<std::borrow::Cow<'a, str>>,
    #[serde(default, alias = "logIndex")]
    log_index: Option<serde_json::Value>,
    /// Only this explicit field authorizes consumers to fold complementary
    /// Up/Down public prints into one economic execution.
    #[serde(
        default,
        borrow,
        alias = "mirrorId",
        alias = "mirror_trade_id",
        alias = "mirrorTradeId"
    )]
    mirror_id: Option<std::borrow::Cow<'a, str>>,
}

#[derive(serde::Deserialize)]
struct TickSizeFields<'a> {
    #[serde(borrow)]
    asset_id: std::borrow::Cow<'a, str>,
    #[serde(default, deserialize_with = "de_str_or_num_f64")]
    old_tick_size: f64,
    #[serde(default, deserialize_with = "de_str_or_num_f64")]
    new_tick_size: f64,
    #[serde(default)]
    timestamp: Option<serde_json::Value>,
}

#[derive(serde::Deserialize)]
struct BestBidAskFields<'a> {
    #[serde(borrow)]
    asset_id: std::borrow::Cow<'a, str>,
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
enum WireDecimal<'a> {
    String(#[serde(borrow)] std::borrow::Cow<'a, str>),
    Number(serde_json::Number),
}

impl WireDecimal<'_> {
    fn decimal(&self) -> Option<Decimal> {
        match self {
            Self::String(value) => Decimal::from_str(value.trim()).ok(),
            Self::Number(value) => Decimal::from_str(&value.to_string()).ok(),
        }
    }
}

#[derive(serde::Deserialize)]
struct PriceChangeEntry<'a> {
    #[serde(borrow)]
    asset_id: std::borrow::Cow<'a, str>,
    #[serde(borrow)]
    price: WireDecimal<'a>,
    #[serde(borrow)]
    size: WireDecimal<'a>,
    #[serde(borrow)]
    side: std::borrow::Cow<'a, str>,
    #[serde(default)]
    #[serde(borrow)]
    hash: Option<std::borrow::Cow<'a, str>>,
    #[serde(default, borrow)]
    best_bid: Option<WireDecimal<'a>>,
    #[serde(default, borrow)]
    best_ask: Option<WireDecimal<'a>>,
}

#[derive(serde::Deserialize)]
struct PriceChangeFields<'a> {
    #[serde(default, borrow)]
    market: Option<std::borrow::Cow<'a, str>>,
    #[serde(default)]
    #[serde(borrow)]
    price_changes: Vec<PriceChangeEntry<'a>>,
    #[serde(default)]
    timestamp: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default)]
struct ReportedBbo {
    /// Outer Option means the field was present; inner Option is the
    /// tradeable price after mapping terminal 0/1 sentinels to no level.
    bid: Option<Option<Decimal>>,
    ask: Option<Option<Decimal>>,
}

impl ReportedBbo {
    fn merge(&mut self, newer: Self) {
        if newer.bid.is_some() {
            self.bid = newer.bid;
        }
        if newer.ask.is_some() {
            self.ask = newer.ask;
        }
    }

    fn matches(&self, actual: (Option<Decimal>, Option<Decimal>)) -> bool {
        self.bid.is_none_or(|expected| expected == actual.0)
            && self.ask.is_none_or(|expected| expected == actual.1)
    }
}

#[derive(Debug)]
struct PendingBboCheck {
    exchange_timestamp_ns: u64,
    expected: ReportedBbo,
    first_observed_at: Instant,
    last_update_at: Instant,
    saw_mismatch: bool,
    saw_newer_checkpoint: bool,
    /// Bounded, sanitized summaries only.  Raw public frames can be large and
    /// may contain fields unrelated to the failing condition.
    frame_summaries: VecDeque<String>,
    /// An off-grid price is evidence that a narrowing tick_size_change is in
    /// the same logical market batch but may be delivered in a sibling frame.
    /// Keep publication behind the tick event for the same quiet window.
    awaiting_tick_change: bool,
}

#[derive(Debug)]
struct PendingQuote {
    quote: QuoteTick,
    received_at: Instant,
}

#[derive(Debug, Default)]
struct ClobDeferredBatch {
    events: Vec<MarketEvent>,
    wire: ClobWireCounters,
    diagnostics: Vec<ClobDiagnostic>,
    repair_tokens: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClobBookSource {
    Websocket,
    RestRepair,
}

#[derive(Debug)]
enum ClobBookApplyOutcome {
    Applied(Vec<MarketEvent>),
    /// A REST snapshot older than already-applied websocket state is a benign
    /// race, not a transport failure.  The caller may request a fresh repair
    /// while the condition stays locally scoped.
    Superseded {
        incoming_timestamp_ns: u64,
        current_timestamp_ns: u64,
    },
}

fn normalize_reported_bbo(price: Decimal) -> Option<Decimal> {
    if price == Decimal::ZERO || price == Decimal::ONE {
        None
    } else {
        Some(price)
    }
}

fn record_bbo_settle_duration(
    counters: &mut ClobWireCounters,
    finished_at: Instant,
    started_at: Instant,
) {
    let elapsed_us = finished_at
        .saturating_duration_since(started_at)
        .as_micros()
        .min(u64::MAX as u128) as u64;
    counters.bbo_settle_samples = counters.bbo_settle_samples.saturating_add(1);
    counters.bbo_settle_total_us = counters.bbo_settle_total_us.saturating_add(elapsed_us);
    counters.bbo_settle_max_us = counters.bbo_settle_max_us.max(elapsed_us);
}

fn record_bbo_recovery_duration(
    counters: &mut ClobWireCounters,
    finished_at: Instant,
    started_at: Instant,
) {
    let elapsed_us = finished_at
        .saturating_duration_since(started_at)
        .as_micros()
        .min(u64::MAX as u128) as u64;
    counters.bbo_recovery_samples = counters.bbo_recovery_samples.saturating_add(1);
    counters.bbo_recovery_total_us = counters.bbo_recovery_total_us.saturating_add(elapsed_us);
    counters.bbo_recovery_max_us = counters.bbo_recovery_max_us.max(elapsed_us);
}

fn bbo_tick_distance(
    expected: &ReportedBbo,
    actual: (Option<Decimal>, Option<Decimal>),
    tick: Option<Decimal>,
) -> Option<u64> {
    let tick = tick.filter(|tick| *tick > Decimal::ZERO)?;
    let mut max_distance = None;
    for (expected, actual) in [(expected.bid, actual.0), (expected.ask, actual.1)] {
        let Some(expected) = expected else { continue };
        let distance = match (expected, actual) {
            (Some(expected), Some(actual)) => ((expected - actual).abs() / tick)
                .floor()
                .to_u64()
                .unwrap_or(u64::MAX),
            (None, None) => 0,
            // One side unexpectedly appeared/disappeared. Count it as at
            // least one tick while retaining exact prices in the diagnostic.
            _ => 1,
        };
        max_distance = Some(max_distance.unwrap_or(0).max(distance));
    }
    max_distance
}

/// Inline RTDS spot-price record seen on the CLOB socket (distinct from
/// the dedicated RTDS WS schema, which wraps in `topic`/`payload`).
#[derive(serde::Deserialize)]
struct InlineRtdsFields<'a> {
    #[serde(borrow)]
    source: std::borrow::Cow<'a, str>,
    #[serde(default, borrow)]
    pair: Option<std::borrow::Cow<'a, str>>,
    #[serde(default, borrow)]
    symbol: Option<std::borrow::Cow<'a, str>>,
    #[serde(default, borrow)]
    filter: Option<std::borrow::Cow<'a, str>>,
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
enum TaggedMessage<'a> {
    Book(#[serde(borrow)] BookFields<'a>),
    Trade(#[serde(borrow)] TradeFields<'a>),
    LastTradePrice(#[serde(borrow)] TradeFields<'a>),
    TickSizeChange(#[serde(borrow)] TickSizeFields<'a>),
    PriceChange(#[serde(borrow)] PriceChangeFields<'a>),
    BestBidAsk(#[serde(borrow)] BestBidAskFields<'a>),
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum ClobFrame<'a> {
    /// Matches anything with `event_type` set to a known variant.
    Tagged(#[serde(borrow)] TaggedMessage<'a>),
    /// Matches RTDS records inlined on the CLOB socket (no event_type).
    Rtds(#[serde(borrow)] InlineRtdsFields<'a>),
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
    repair_tokens: Vec<String>,
}

/// Exclusive top-level CLOB frame phases. Canonicalization and BBO-settle
/// histograms are additionally recorded at their exact inner boundaries.
#[derive(Clone, Copy, Debug, Default)]
struct ClobFramePhaseTimings {
    simd_json_ns: u64,
    book_apply_ns: u64,
    price_change_apply_ns: u64,
    event_construction_ns: u64,
}

struct ClobLatencyScope {
    stage: &'static str,
    started: crate::latency::Instant,
}

impl ClobLatencyScope {
    #[inline]
    fn new(stage: &'static str) -> Self {
        Self {
            stage,
            started: crate::latency::Instant::now(),
        }
    }
}

impl Drop for ClobLatencyScope {
    #[inline]
    fn drop(&mut self) {
        crate::latency::record(self.stage, self.started);
    }
}

impl ClobFramePhaseTimings {
    fn record(self) {
        crate::latency::record_ns("polymarket.ws.clob_simd_json", self.simd_json_ns);
        if self.book_apply_ns != 0 {
            crate::latency::record_ns("polymarket.ws.clob_book_apply", self.book_apply_ns);
        }
        if self.price_change_apply_ns != 0 {
            crate::latency::record_ns(
                "polymarket.ws.clob_price_change_apply",
                self.price_change_apply_ns,
            );
        }
        crate::latency::record_ns(
            "polymarket.ws.clob_event_construction",
            self.event_construction_ns,
        );
    }
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
    /// Startup-resident token identities. Wire strings are borrowed from the
    /// parse buffer, resolved once to a compact index, and never inserted into
    /// a hot-path growable routing map.
    token_indices: HashMap<Box<str>, u16>,
    resident_tokens: Vec<Box<str>>,
    token_books: HashMap<String, ClobLocalBook>,
    roles: HashMap<String, ClobCanonicalRole>,
    current_ticks: HashMap<String, Decimal>,
    tick_versions: HashMap<String, u64>,
    canonical_versions: HashMap<String, ClobBookVersion>,
    canonical_books: HashMap<String, OrderBookSnapshot>,
    quote_versions: HashMap<String, ClobBookVersion>,
    pending_bbo: HashMap<String, PendingBboCheck>,
    pending_quotes: HashMap<String, PendingQuote>,
    quarantined_tokens: HashSet<String>,
    repair_started_at: HashMap<String, Instant>,
    degraded_tokens: HashSet<String>,
    health_states: HashMap<String, MarketDataHealthState>,
    /// A transient BBO mismatch must stop taker use immediately, but a
    /// matching checkpoint in the very next frame should not produce a
    /// Settling→Healthy callback pair for every microbatch.  Keep the
    /// condition in Settling until Healthy has remained stable for one settle
    /// interval.  Any renewed non-Healthy state cancels the pending recovery.
    pending_health_recoveries: HashMap<String, PendingHealthRecovery>,
    wire_sequence: u64,
}

#[derive(Debug)]
struct PendingHealthRecovery {
    due_at: Instant,
    reason: String,
}

impl ClobLocalBooks {
    fn new(specs: &[CanonicalEventSpec]) -> Self {
        let mut state = Self::default();
        for spec in specs {
            if let Ok(tick) = Decimal::from_str(&spec.tick_size.to_string()) {
                state.current_ticks.insert(spec.condition_id.clone(), tick);
            }
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
            for token in [&spec.up_token, &spec.down_token] {
                if !state.token_indices.contains_key(token.as_str()) {
                    let index = u16::try_from(state.resident_tokens.len())
                        .expect("CLOB token table exceeds u16 startup capacity");
                    let resident = token.clone().into_boxed_str();
                    state.token_indices.insert(resident.clone(), index);
                    state.resident_tokens.push(resident);
                }
            }
        }
        state
    }

    #[inline]
    fn token_index(&self, token: &str) -> Option<u16> {
        let index = self.token_indices.get(token).copied()?;
        debug_assert_eq!(self.resident_tokens[index as usize].as_ref(), token);
        Some(index)
    }

    fn market_key(&self, token: &str) -> String {
        self.roles
            .get(token)
            .map(|role| role.condition_id.clone())
            .unwrap_or_else(|| token.to_string())
    }

    fn condition_is_seeded(&self, condition_id: &str) -> bool {
        let tokens: Vec<_> = self
            .roles
            .iter()
            .filter_map(|(token, role)| {
                (role.condition_id == condition_id).then_some(token.as_str())
            })
            .collect();
        !tokens.is_empty()
            && tokens
                .iter()
                .all(|token| self.token_books.contains_key(*token))
    }

    fn desired_health_state(&self, condition_id: &str) -> Option<MarketDataHealthState> {
        if !self.condition_is_seeded(condition_id) {
            return None;
        }
        if self
            .degraded_tokens
            .iter()
            .any(|token| self.market_key(token) == condition_id)
        {
            return Some(MarketDataHealthState::Degraded);
        }
        if self
            .quarantined_tokens
            .iter()
            .any(|token| self.market_key(token) == condition_id)
        {
            return Some(MarketDataHealthState::Repairing);
        }
        if self
            .pending_bbo
            .keys()
            .any(|token| self.market_key(token) == condition_id)
        {
            return Some(MarketDataHealthState::Settling);
        }
        Some(MarketDataHealthState::Healthy)
    }

    fn reconcile_health(
        &mut self,
        token: &str,
        reason: impl Into<String>,
        observed_at: Instant,
        local_now: u64,
    ) -> Option<MarketEvent> {
        let condition_id = self.market_key(token);
        let state = self.desired_health_state(&condition_id)?;
        let reason = reason.into();
        let previous = self.health_states.get(&condition_id).copied();

        // Entering a restrictive state is edge-triggered and immediate. Every
        // non-healthy→Healthy edge is delayed; a stable recovery is released
        // by `flush_deferred_due` below. A renewed restrictive observation
        // removes the pending edge and restarts the full stable window.
        if state == MarketDataHealthState::Healthy
            && previous.is_some_and(|previous| previous != MarketDataHealthState::Healthy)
        {
            self.pending_health_recoveries
                .entry(condition_id)
                .or_insert_with(|| PendingHealthRecovery {
                    due_at: observed_at + CLOB_HEALTH_RECOVERY_STABLE_INTERVAL,
                    reason,
                });
            return None;
        }
        self.pending_health_recoveries.remove(&condition_id);
        if self.health_states.get(&condition_id) == Some(&state) {
            return None;
        }
        self.health_states.insert(condition_id.clone(), state);
        Some(self.health_event(&condition_id, token, state, reason, local_now))
    }

    fn health_event(
        &self,
        condition_id: &str,
        fallback_token: &str,
        state: MarketDataHealthState,
        reason: String,
        local_now: u64,
    ) -> MarketEvent {
        let symbol = self
            .roles
            .iter()
            .find_map(|(candidate, role)| {
                (role.condition_id == condition_id && !role.is_down).then_some(candidate.clone())
            })
            .unwrap_or_else(|| fallback_token.to_string());
        let (passive_ready, taker_ready) = match state {
            MarketDataHealthState::Healthy => (true, true),
            // L1 advertised by the venue remains available for passive
            // pricing, but inconsistent L2 quantities must never drive a
            // taker decision.
            MarketDataHealthState::Settling | MarketDataHealthState::Repairing => (true, false),
            MarketDataHealthState::Degraded => (false, false),
        };
        MarketEvent::MarketDataHealth(MarketDataHealth {
            exchange: Exchange::Polymarket,
            market_id: condition_id.to_string(),
            symbol,
            state,
            passive_ready,
            taker_ready,
            reason,
            local_timestamp_ns: local_now,
        })
    }

    fn flush_health_recoveries_due(&mut self, now: Instant, local_now: u64) -> Vec<MarketEvent> {
        let mut due: Vec<_> = self
            .pending_health_recoveries
            .iter()
            .filter_map(|(condition_id, pending)| {
                (now >= pending.due_at).then_some(condition_id.clone())
            })
            .collect();
        due.sort();
        let mut events = Vec::with_capacity(due.len());
        for condition_id in due {
            let Some(pending) = self.pending_health_recoveries.remove(&condition_id) else {
                continue;
            };
            if self.desired_health_state(&condition_id) != Some(MarketDataHealthState::Healthy)
                || !self
                    .health_states
                    .get(&condition_id)
                    .is_some_and(|state| *state != MarketDataHealthState::Healthy)
            {
                continue;
            }
            self.health_states
                .insert(condition_id.clone(), MarketDataHealthState::Healthy);
            events.push(self.health_event(
                &condition_id,
                &condition_id,
                MarketDataHealthState::Healthy,
                pending.reason,
                local_now,
            ));
        }
        events
    }

    fn mark_repair_failed(
        &mut self,
        token: &str,
        reason: impl Into<String>,
        observed_at: Instant,
        local_now: u64,
    ) -> Option<MarketEvent> {
        self.degraded_tokens.insert(token.to_string());
        self.reconcile_health(token, reason, observed_at, local_now)
    }

    fn price_is_on_current_tick(&self, token: &str, price: Decimal) -> bool {
        let key = self.market_key(token);
        self.current_ticks
            .get(&key)
            .filter(|tick| **tick > Decimal::ZERO)
            .map_or(true, |tick| price % *tick == Decimal::ZERO)
    }

    fn market_is_quarantined(&self, token: &str) -> bool {
        let key = self.market_key(token);
        self.quarantined_tokens
            .iter()
            .any(|candidate| candidate == token || self.market_key(candidate) == key)
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
        let _timing = ClobLatencyScope::new("polymarket.ws.clob_book_canonicalization");
        if self.pending_bbo.contains_key(token) || self.market_is_quarantined(token) {
            return None;
        }
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

    fn canonicalize_quote_ready(&mut self, mut quote: QuoteTick) -> Option<MarketEvent> {
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

    fn canonicalize_quote(
        &mut self,
        quote: QuoteTick,
        received_at: Instant,
    ) -> Option<MarketEvent> {
        let _timing = ClobLatencyScope::new("polymarket.ws.clob_quote_canonicalization");
        let prices_on_tick = [quote.bid_price, quote.ask_price].into_iter().all(|price| {
            Decimal::from_str(&price.to_string())
                .ok()
                .is_some_and(|price| self.price_is_on_current_tick(&quote.symbol, price))
        });
        if !prices_on_tick {
            let key = self.market_key(&quote.symbol);
            let replace = self.pending_quotes.get(&key).map_or(true, |pending| {
                quote.exchange_timestamp_ns >= pending.quote.exchange_timestamp_ns
            });
            if replace {
                self.pending_quotes
                    .insert(key, PendingQuote { quote, received_at });
            }
            return None;
        }
        self.canonicalize_quote_ready(quote)
    }

    fn apply_book(
        &mut self,
        fields: BookFields<'_>,
        received_at: Instant,
        local_now: u64,
        source: ClobBookSource,
        counters: &mut ClobWireCounters,
    ) -> std::result::Result<ClobBookApplyOutcome, String> {
        let symbol = fields.asset_id.into_owned();
        if symbol.trim().is_empty() {
            return Err("book has empty asset_id".to_string());
        }
        let exchange_timestamp_ns = timestamp_value_to_ns(fields.timestamp.as_ref(), local_now);
        if let Some(current) = self.token_books.get(&symbol) {
            if exchange_timestamp_ns < current.exchange_timestamp_ns {
                if source == ClobBookSource::RestRepair {
                    counters.bbo_repair_superseded_by_ws =
                        counters.bbo_repair_superseded_by_ws.saturating_add(1);
                }
                return Ok(ClobBookApplyOutcome::Superseded {
                    incoming_timestamp_ns: exchange_timestamp_ns,
                    current_timestamp_ns: current.exchange_timestamp_ns,
                });
            }
        }
        let parse_levels = |levels: Vec<BookLevel<'_>>| {
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
        // A full book is authoritative for this token and ends any deferred
        // validation/quarantine created by an incomplete price-change batch.
        let pending = self.pending_bbo.remove(&symbol);
        let was_quarantined = self.quarantined_tokens.remove(&symbol);
        if let Some(pending) = pending.as_ref().filter(|pending| pending.saw_mismatch) {
            record_bbo_settle_duration(counters, received_at, pending.first_observed_at);
            if !was_quarantined {
                match source {
                    ClobBookSource::Websocket => {
                        counters.bbo_recovery_book = counters.bbo_recovery_book.saturating_add(1)
                    }
                    ClobBookSource::RestRepair => {
                        counters.bbo_recovery_rest = counters.bbo_recovery_rest.saturating_add(1)
                    }
                }
            }
        }
        if was_quarantined {
            if let Some(started_at) = self.repair_started_at.remove(&symbol) {
                record_bbo_recovery_duration(counters, received_at, started_at);
            }
            match source {
                ClobBookSource::Websocket => {
                    counters.bbo_recovery_book = counters.bbo_recovery_book.saturating_add(1)
                }
                ClobBookSource::RestRepair => {
                    counters.bbo_recovery_rest = counters.bbo_recovery_rest.saturating_add(1)
                }
            }
        }
        self.degraded_tokens.remove(&symbol);
        self.token_books.insert(symbol.clone(), book);
        // An initial snapshot for the complementary token can be older than
        // the event-level Up snapshot already accepted. Keep the newer event
        // book, but re-emit it once so completion of initial L2 seeding can
        // transition the feed to READY without letting the old Down book win.
        let mut events = Vec::new();
        if let Some(event) = self
            .canonicalize_token(&symbol, local_now)
            .or_else(|| self.canonical_snapshot_for_token(&symbol))
        {
            events.push(event);
        }
        if let Some(event) = self.reconcile_health(
            &symbol,
            match source {
                ClobBookSource::Websocket => "authoritative websocket book",
                ClobBookSource::RestRepair => "authoritative REST repair",
            },
            received_at,
            local_now,
        ) {
            events.push(event);
        }
        Ok(ClobBookApplyOutcome::Applied(events))
    }

    fn finalize_pending_bbo(
        &mut self,
        token: &str,
        finished_at: Instant,
        local_now: u64,
        counters: &mut ClobWireCounters,
        diagnostics: &mut Vec<ClobDiagnostic>,
        emit_diagnostic: bool,
    ) -> (Option<MarketEvent>, Option<String>) {
        let Some(pending) = self.pending_bbo.remove(token) else {
            return (None, None);
        };
        let actual = self
            .token_books
            .get(token)
            .map(ClobLocalBook::top)
            .unwrap_or_default();
        if pending.expected.matches(actual) {
            if pending.saw_mismatch {
                counters.bbo_transient_recoveries =
                    counters.bbo_transient_recoveries.saturating_add(1);
                record_bbo_settle_duration(counters, finished_at, pending.first_observed_at);
                if pending.saw_newer_checkpoint {
                    counters.bbo_recovery_newer_timestamp =
                        counters.bbo_recovery_newer_timestamp.saturating_add(1);
                } else {
                    counters.bbo_recovery_same_timestamp =
                        counters.bbo_recovery_same_timestamp.saturating_add(1);
                }
            }
            if pending.awaiting_tick_change && emit_diagnostic {
                diagnostics.push(ClobDiagnostic {
                    key: "tick_size_change_lag",
                    detail: format!(
                        "token={token} ts={} publication_released_after={}ms",
                        pending.exchange_timestamp_ns,
                        CLOB_BBO_SETTLE_INTERVAL.as_millis(),
                    ),
                });
            }
            if let Some(book) = self.token_books.get_mut(token) {
                book.dirty_since = None;
            }
            return (self.canonicalize_token(token, local_now), None);
        }

        counters.bbo_mismatches = counters.bbo_mismatches.saturating_add(1);
        counters.bbo_repair_requests = counters.bbo_repair_requests.saturating_add(1);
        record_bbo_settle_duration(counters, finished_at, pending.first_observed_at);
        let tick = self.current_ticks.get(&self.market_key(token)).copied();
        if let Some(distance) = bbo_tick_distance(&pending.expected, actual, tick) {
            counters.bbo_tick_distance_samples =
                counters.bbo_tick_distance_samples.saturating_add(1);
            counters.bbo_tick_distance_total =
                counters.bbo_tick_distance_total.saturating_add(distance);
            counters.bbo_tick_distance_max = counters.bbo_tick_distance_max.max(distance);
        }
        if emit_diagnostic {
            diagnostics.push(ClobDiagnostic {
                key: "price_change_bbo_mismatch",
                detail: format!(
                    "token={token} ts={} expected_bid={:?} expected_ask={:?} actual_bid={:?} actual_ask={:?} settled_ms={} frames={:?}",
                    pending.exchange_timestamp_ns,
                    pending.expected.bid,
                    pending.expected.ask,
                    actual.0,
                    actual.1,
                    CLOB_BBO_SETTLE_INTERVAL.as_millis(),
                    pending.frame_summaries,
                ),
            });
        }
        self.quarantined_tokens.insert(token.to_string());
        self.repair_started_at
            .entry(token.to_string())
            .or_insert(finished_at);
        (None, emit_diagnostic.then(|| token.to_string()))
    }

    fn resolve_pending_if_ready(
        &mut self,
        token: &str,
        now: Instant,
        counters: &mut ClobWireCounters,
    ) -> bool {
        let Some(pending) = self.pending_bbo.get(token) else {
            return false;
        };
        let actual = self
            .token_books
            .get(token)
            .map(ClobLocalBook::top)
            .unwrap_or_default();
        if pending.awaiting_tick_change || !pending.expected.matches(actual) {
            return false;
        }
        let pending = self
            .pending_bbo
            .remove(token)
            .expect("pending BBO checked above");
        if pending.saw_mismatch {
            counters.bbo_transient_recoveries = counters.bbo_transient_recoveries.saturating_add(1);
            record_bbo_settle_duration(counters, now, pending.first_observed_at);
            if pending.saw_newer_checkpoint {
                counters.bbo_recovery_newer_timestamp =
                    counters.bbo_recovery_newer_timestamp.saturating_add(1);
            } else {
                counters.bbo_recovery_same_timestamp =
                    counters.bbo_recovery_same_timestamp.saturating_add(1);
            }
        }
        if self.quarantined_tokens.remove(token) {
            counters.bbo_repair_superseded_by_ws =
                counters.bbo_repair_superseded_by_ws.saturating_add(1);
            if let Some(started_at) = self.repair_started_at.remove(token) {
                record_bbo_recovery_duration(counters, now, started_at);
            }
        }
        self.degraded_tokens.remove(token);
        true
    }

    fn apply_tick_size_change(
        &mut self,
        change: &TickSizeChange,
        received_at: Instant,
        local_now: u64,
        counters: &mut ClobWireCounters,
    ) -> Vec<MarketEvent> {
        let Ok(new_tick) = Decimal::from_str(&change.new_tick_size.to_string()) else {
            return Vec::new();
        };
        let Ok(old_tick) = Decimal::from_str(&change.old_tick_size.to_string()) else {
            return Vec::new();
        };
        let key = self.market_key(&change.symbol);
        if self
            .tick_versions
            .get(&key)
            .is_some_and(|timestamp| *timestamp > change.exchange_timestamp_ns)
        {
            return Vec::new();
        }
        if let Some(current_tick) = self.current_ticks.get(&key) {
            let duplicate = *current_tick == new_tick;
            if (!duplicate && *current_tick != old_tick) || new_tick > *current_tick {
                return Vec::new();
            }
        }
        self.current_ticks.insert(key.clone(), new_tick);
        self.tick_versions
            .insert(key.clone(), change.exchange_timestamp_ns);

        let mut release_tokens: Vec<_> = self
            .pending_bbo
            .keys()
            .filter(|token| self.market_key(token) == key)
            .cloned()
            .collect();
        release_tokens.sort();
        let mut events = Vec::new();
        for token in release_tokens {
            let all_levels_on_new_tick = self.token_books.get(&token).is_none_or(|book| {
                book.bids
                    .keys()
                    .chain(book.asks.keys())
                    .all(|price| *price % new_tick == Decimal::ZERO)
            });
            if all_levels_on_new_tick {
                if let Some(pending) = self.pending_bbo.get_mut(&token) {
                    pending.awaiting_tick_change = false;
                }
            }
            if self.resolve_pending_if_ready(&token, received_at, counters) {
                if let Some(book) = self.token_books.get_mut(&token) {
                    book.dirty_since = None;
                }
                if let Some(event) = self.canonicalize_token(&token, local_now) {
                    push_latest_order_book(&mut events, event);
                }
            }
            if let Some(event) = self.reconcile_health(
                &token,
                "tick-size/BBO batch settled",
                received_at,
                local_now,
            ) {
                events.push(event);
            }
        }

        if let Some(pending) = self.pending_quotes.remove(&key) {
            if let Some(event) = self.canonicalize_quote_ready(pending.quote) {
                events.push(event);
            }
        }
        events
    }

    fn next_deferred_deadline(&self) -> Option<Instant> {
        self.pending_bbo
            .values()
            .map(|pending| pending.last_update_at + CLOB_BBO_SETTLE_INTERVAL)
            .chain(
                self.pending_quotes
                    .values()
                    .map(|pending| pending.received_at + CLOB_BBO_SETTLE_INTERVAL),
            )
            .chain(
                self.pending_health_recoveries
                    .values()
                    .map(|pending| pending.due_at),
            )
            .min()
    }

    fn flush_deferred_due(
        &mut self,
        now: Instant,
        local_now: u64,
        active_tokens: &[String],
    ) -> ClobDeferredBatch {
        let _timing = ClobLatencyScope::new("polymarket.ws.clob_bbo_settle");
        let mut batch = ClobDeferredBatch::default();
        let mut tokens: Vec<_> = self
            .pending_bbo
            .iter()
            .filter_map(|(token, pending)| {
                (now.saturating_duration_since(pending.last_update_at) >= CLOB_BBO_SETTLE_INTERVAL)
                    .then_some(token.clone())
            })
            .collect();
        tokens.sort();
        for token in tokens {
            let active = subscribed_token(active_tokens, &token);
            let (event, repair) = self.finalize_pending_bbo(
                &token,
                now,
                local_now,
                &mut batch.wire,
                &mut batch.diagnostics,
                active,
            );
            if let Some(event) = event {
                push_latest_order_book(&mut batch.events, event);
            }
            if let Some(token) = repair.filter(|_| active) {
                batch.repair_tokens.push(token);
            }
            if let Some(event) = self.reconcile_health(
                &token,
                if batch.repair_tokens.last() == Some(&token) {
                    "BBO settle window expired; authoritative repair pending"
                } else {
                    "BBO settled"
                },
                now,
                local_now,
            ) {
                batch.events.push(event);
            }
        }

        let mut quote_keys: Vec<_> = self
            .pending_quotes
            .iter()
            .filter_map(|(key, pending)| {
                (now.saturating_duration_since(pending.received_at) >= CLOB_BBO_SETTLE_INTERVAL)
                    .then_some(key.clone())
            })
            .collect();
        quote_keys.sort();
        for key in quote_keys {
            let Some(pending) = self.pending_quotes.remove(&key) else {
                continue;
            };
            if subscribed_token(active_tokens, &pending.quote.symbol) {
                batch.diagnostics.push(ClobDiagnostic {
                    key: "tick_size_change_lag",
                    detail: format!(
                        "token={} market={key} quote_ts={} publication_released_after={}ms",
                        pending.quote.symbol,
                        pending.quote.exchange_timestamp_ns,
                        CLOB_BBO_SETTLE_INTERVAL.as_millis(),
                    ),
                });
            }
            if let Some(event) = self.canonicalize_quote_ready(pending.quote) {
                batch.events.push(event);
            }
        }
        batch
            .events
            .extend(self.flush_health_recoveries_due(now, local_now));
        batch
    }

    fn apply_price_change(
        &mut self,
        fields: PriceChangeFields<'_>,
        received_at: Instant,
        local_now: u64,
        counters: &mut ClobWireCounters,
        diagnostics: &mut Vec<ClobDiagnostic>,
        active_tokens: &[String],
    ) -> (Vec<MarketEvent>, usize, Vec<String>) {
        let exchange_timestamp_ns = timestamp_value_to_ns(fields.timestamp.as_ref(), local_now);
        let mut immediate = Vec::new();
        let entry_counts: HashMap<String, usize> =
            fields
                .price_changes
                .iter()
                .fold(HashMap::new(), |mut counts, change| {
                    *counts.entry(change.asset_id.to_string()).or_insert(0) += 1;
                    counts
                });
        let mut before: HashMap<String, (Option<Decimal>, Option<Decimal>)> = HashMap::new();
        let mut reported_bbo: HashMap<String, ReportedBbo> = HashMap::new();
        let mut off_tick_tokens: HashSet<String> = HashSet::new();

        for change in fields.price_changes {
            counters.price_change_entries = counters.price_change_entries.saturating_add(1);
            let token = change.asset_id;
            let emit_diagnostic = subscribed_token(active_tokens, &token);
            let Some(price) = change.price.decimal() else {
                counters.ignored = counters.ignored.saturating_add(1);
                if emit_diagnostic {
                    diagnostics.push(ClobDiagnostic {
                        key: "invalid_price_change",
                        detail: format!("token={token} reason=invalid_price"),
                    });
                }
                continue;
            };
            let Some(size) = change.size.decimal() else {
                counters.ignored = counters.ignored.saturating_add(1);
                if emit_diagnostic {
                    diagnostics.push(ClobDiagnostic {
                        key: "invalid_price_change",
                        detail: format!("token={token} reason=invalid_size"),
                    });
                }
                continue;
            };
            if price <= Decimal::ZERO || price >= Decimal::ONE || size < Decimal::ZERO {
                counters.ignored = counters.ignored.saturating_add(1);
                if emit_diagnostic {
                    diagnostics.push(ClobDiagnostic {
                        key: "invalid_price_change",
                        detail: format!("token={token} price={price} size={size}"),
                    });
                }
                continue;
            }
            if !self.price_is_on_current_tick(token.as_ref(), price) {
                off_tick_tokens.insert(token.to_string());
            }
            let Some(current_book) = self.token_books.get(token.as_ref()) else {
                counters.unseeded_deltas = counters.unseeded_deltas.saturating_add(1);
                counters.ignored = counters.ignored.saturating_add(1);
                if emit_diagnostic {
                    diagnostics.push(ClobDiagnostic {
                        key: "unseeded_price_change",
                        detail: format!("token={token} ts={exchange_timestamp_ns}"),
                    });
                }
                continue;
            };
            if exchange_timestamp_ns < current_book.exchange_timestamp_ns {
                counters.ignored = counters.ignored.saturating_add(1);
                if emit_diagnostic {
                    diagnostics.push(ClobDiagnostic {
                        key: "stale_price_change",
                        detail: format!(
                            "token={token} incoming_ts={} current_ts={}",
                            exchange_timestamp_ns, current_book.exchange_timestamp_ns,
                        ),
                    });
                }
                continue;
            }
            let sequence = self.next_sequence();
            let book = self
                .token_books
                .get_mut(token.as_ref())
                .expect("book existence checked above");
            if !before.contains_key(token.as_ref()) {
                before.insert(token.to_string(), book.top());
            }
            let side = change.side.trim();
            let levels = if side.eq_ignore_ascii_case("BUY") {
                &mut book.bids
            } else if side.eq_ignore_ascii_case("SELL") {
                &mut book.asks
            } else {
                counters.ignored = counters.ignored.saturating_add(1);
                if emit_diagnostic {
                    diagnostics.push(ClobDiagnostic {
                        key: "invalid_price_change",
                        detail: format!("token={token} reason=unknown_side side={side}"),
                    });
                }
                continue;
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

            let reported = reported_bbo.entry(token.to_string()).or_default();
            if let Some(value) = change.best_bid.as_ref() {
                match value.decimal() {
                    Some(price) => reported.bid = Some(normalize_reported_bbo(price)),
                    None if emit_diagnostic => diagnostics.push(ClobDiagnostic {
                        key: "invalid_price_change_bbo",
                        detail: format!("token={token} side=bid"),
                    }),
                    None => {}
                }
            }
            if let Some(value) = change.best_ask.as_ref() {
                match value.decimal() {
                    Some(price) => reported.ask = Some(normalize_reported_bbo(price)),
                    None if emit_diagnostic => diagnostics.push(ClobDiagnostic {
                        key: "invalid_price_change_bbo",
                        detail: format!("token={token} side=ask"),
                    }),
                    None => {}
                }
            }
        }

        // The venue's advertised BBO describes a logical microbatch, but that
        // batch can span multiple WebSocket frames with the same millisecond
        // timestamp. Merge expectations by token+timestamp and publish only
        // after the local top agrees (or the short quiet window expires).
        let mut validation_tokens: HashSet<String> = reported_bbo.keys().cloned().collect();
        validation_tokens.extend(off_tick_tokens.iter().cloned());
        let mut validation_tokens: Vec<_> = validation_tokens.into_iter().collect();
        validation_tokens.sort();
        for token in validation_tokens {
            if !before.contains_key(&token) {
                continue;
            }
            let newer_expected = reported_bbo.remove(&token).unwrap_or_default();
            let actual = self
                .token_books
                .get(&token)
                .map(ClobLocalBook::top)
                .unwrap_or_default();
            let off_tick = off_tick_tokens.contains(&token);
            let summary = subscribed_token(active_tokens, &token).then(|| {
                format!(
                    "ts={} entries={} expected_bid={:?} expected_ask={:?} actual_bid={:?} actual_ask={:?}",
                    exchange_timestamp_ns,
                    entry_counts.get(&token).copied().unwrap_or(0),
                    newer_expected.bid,
                    newer_expected.ask,
                    actual.0,
                    actual.1,
                )
            });
            let pending =
                self.pending_bbo
                    .entry(token.clone())
                    .or_insert_with(|| PendingBboCheck {
                        exchange_timestamp_ns,
                        expected: ReportedBbo::default(),
                        first_observed_at: received_at,
                        last_update_at: received_at,
                        saw_mismatch: false,
                        saw_newer_checkpoint: false,
                        frame_summaries: VecDeque::new(),
                        awaiting_tick_change: false,
                    });
            if exchange_timestamp_ns > pending.exchange_timestamp_ns {
                // A newer advertised checkpoint supersedes the unfinished
                // older one. Apply the newer delta first, then validate the
                // latest state; never fail an old checkpoint at this boundary.
                pending.exchange_timestamp_ns = exchange_timestamp_ns;
                pending.expected = newer_expected;
                pending.last_update_at = received_at;
                pending.saw_newer_checkpoint = true;
                pending.awaiting_tick_change = off_tick;
                pending.saw_mismatch |= !pending.expected.matches(actual);
            } else if pending.exchange_timestamp_ns == exchange_timestamp_ns {
                pending.expected.merge(newer_expected);
                pending.last_update_at = received_at;
                pending.awaiting_tick_change |= off_tick;
                pending.saw_mismatch |= !pending.expected.matches(actual);
            }
            if let Some(summary) = summary {
                pending.frame_summaries.push_back(summary);
                while pending.frame_summaries.len() > CLOB_BBO_DIAGNOSTIC_FRAMES {
                    pending.frame_summaries.pop_front();
                }
            }
            let advertised_l1 = if !pending.expected.matches(actual) {
                match (
                    pending.expected.bid.flatten(),
                    pending.expected.ask.flatten(),
                ) {
                    (Some(bid), Some(ask)) if bid < ask => Some((bid, ask)),
                    _ => None,
                }
            } else {
                None
            };
            if self.roles.contains_key(&token) {
                if let Some((bid, ask)) = advertised_l1 {
                    if let (Some(bid_price), Some(ask_price)) = (bid.to_f64(), ask.to_f64()) {
                        let quote = QuoteTick {
                            exchange: Exchange::Polymarket,
                            symbol: token.clone(),
                            bid_price,
                            bid_qty: 0.0,
                            ask_price,
                            ask_qty: 0.0,
                            exchange_timestamp_ns,
                            local_timestamp_ns: local_now,
                        };
                        if let Some(event) = self.canonicalize_quote(quote, received_at) {
                            immediate.push(event);
                        }
                    }
                }
            }
        }

        let mut touched_order: Vec<_> = before
            .keys()
            .filter_map(|token| {
                self.token_books
                    .get(token)
                    .map(|book| (book.wire_sequence, token.clone()))
            })
            .collect();
        touched_order.sort_by_key(|(sequence, _)| *sequence);
        let health_tokens: Vec<String> = touched_order
            .iter()
            .map(|(_, token)| token.clone())
            .collect();
        for (_, token) in touched_order {
            let (top_changed, semantically_valid) = self
                .token_books
                .get(&token)
                .map(|book| {
                    (
                        before.get(&token).copied() != Some(book.top()),
                        book.is_semantically_valid(),
                    )
                })
                .unwrap_or((false, false));
            let _ = self.resolve_pending_if_ready(&token, received_at, counters);
            if top_changed
                && semantically_valid
                && !self.pending_bbo.contains_key(&token)
                && !self.market_is_quarantined(&token)
            {
                if let Some(book) = self.token_books.get_mut(&token) {
                    book.dirty_since = None;
                }
                if let Some(event) = self.canonicalize_token(&token, local_now) {
                    push_latest_order_book(&mut immediate, event);
                }
            }
        }
        let mut reconciled_markets = HashSet::new();
        for token in health_tokens {
            if reconciled_markets.insert(self.market_key(&token)) {
                if let Some(event) = self.reconcile_health(
                    &token,
                    "BBO checkpoint state changed",
                    received_at,
                    local_now,
                ) {
                    immediate.push(event);
                }
            }
        }
        let bbo_change_snapshots = immediate
            .iter()
            .filter(|event| matches!(event, MarketEvent::OrderBook(_)))
            .count();
        let _ = fields.market;
        (immediate, bbo_change_snapshots, Vec::new())
    }

    fn flush_due(&mut self, now: Instant, local_now: u64) -> Vec<MarketEvent> {
        let mut due: Vec<_> = self
            .token_books
            .iter()
            .filter_map(|(token, book)| {
                if self.pending_bbo.contains_key(token) || self.market_is_quarantined(token) {
                    return None;
                }
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

#[cfg(test)]
fn process_clob_frame(
    text: &str,
    books: &mut ClobLocalBooks,
    tokens: &[String],
    received_at: Instant,
    local_now: u64,
) -> ClobParsedBatch {
    let mut parse_buffer = text.as_bytes().to_vec();
    let mut phases = ClobFramePhaseTimings::default();
    process_clob_frame_in_place(
        &mut parse_buffer,
        books,
        tokens,
        tokens,
        received_at,
        local_now,
        &mut phases,
    )
}

fn process_clob_frame_in_place(
    parse_buffer: &mut [u8],
    books: &mut ClobLocalBooks,
    _tokens: &[String],
    active_tokens: &[String],
    received_at: Instant,
    local_now: u64,
    phases: &mut ClobFramePhaseTimings,
) -> ClobParsedBatch {
    let mut batch = ClobParsedBatch::default();
    if parse_buffer.is_empty() {
        return batch;
    }
    let is_array = parse_buffer
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        == Some(b'[');
    let decode_started = crate::latency::Instant::now();
    let frames = if is_array {
        simd_json::serde::from_slice::<Vec<ClobFrame>>(&mut *parse_buffer)
    } else {
        simd_json::serde::from_slice::<ClobFrame>(&mut *parse_buffer).map(|frame| vec![frame])
    };
    phases.simd_json_ns = decode_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
    let frames = match frames {
        Ok(frames) => frames,
        Err(error) => {
            batch.wire.parse_errors = 1;
            batch.diagnostics.push(ClobDiagnostic {
                key: "parse_error",
                detail: format!(
                    "error={} raw={}",
                    error,
                    String::from_utf8_lossy(parse_buffer)
                        .chars()
                        .take(300)
                        .collect::<String>(),
                ),
            });
            return batch;
        }
    };

    let construction_started = crate::latency::Instant::now();
    for frame in frames {
        match frame {
            ClobFrame::Tagged(TaggedMessage::Book(fields)) => {
                batch.wire.books = batch.wire.books.saturating_add(1);
                batch.recognized_topic |= books.token_index(&fields.asset_id).is_some();
                let emit_diagnostic = subscribed_token(active_tokens, &fields.asset_id);
                let diagnostic_token = fields.asset_id.clone();
                let apply_started = crate::latency::Instant::now();
                let apply_result = books.apply_book(
                    fields,
                    received_at,
                    local_now,
                    ClobBookSource::Websocket,
                    &mut batch.wire,
                );
                phases.book_apply_ns =
                    phases.book_apply_ns.saturating_add(
                        apply_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                    );
                match apply_result {
                    Ok(ClobBookApplyOutcome::Applied(events)) => {
                        for event in events {
                            if matches!(event, MarketEvent::OrderBook(_)) {
                                push_latest_order_book(&mut batch.events, event);
                            } else {
                                batch.events.push(event);
                            }
                        }
                    }
                    Ok(ClobBookApplyOutcome::Superseded {
                        incoming_timestamp_ns,
                        current_timestamp_ns,
                    }) => {
                        batch.wire.ignored = batch.wire.ignored.saturating_add(1);
                        if emit_diagnostic {
                            batch.diagnostics.push(ClobDiagnostic {
                                key: "stale_book",
                                detail: format!(
                                    "token={} incoming_ts={incoming_timestamp_ns} current_ts={current_timestamp_ns}",
                                    diagnostic_token,
                                ),
                            });
                        }
                    }
                    Err(detail) => {
                        batch.wire.ignored = batch.wire.ignored.saturating_add(1);
                        if emit_diagnostic {
                            batch.diagnostics.push(ClobDiagnostic {
                                key: "invalid_book",
                                detail,
                            });
                        }
                    }
                }
            }
            ClobFrame::Tagged(TaggedMessage::PriceChange(fields)) => {
                batch.wire.price_changes = batch.wire.price_changes.saturating_add(1);
                batch.recognized_topic |= fields
                    .price_changes
                    .iter()
                    .any(|change| books.token_index(&change.asset_id).is_some());
                let apply_started = crate::latency::Instant::now();
                let (events, bbo_snapshots, repair_tokens) = books.apply_price_change(
                    fields,
                    received_at,
                    local_now,
                    &mut batch.wire,
                    &mut batch.diagnostics,
                    active_tokens,
                );
                phases.price_change_apply_ns =
                    phases.price_change_apply_ns.saturating_add(
                        apply_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                    );
                batch.bbo_change_snapshots =
                    batch.bbo_change_snapshots.saturating_add(bbo_snapshots);
                batch.repair_tokens.extend(repair_tokens);
                for event in events {
                    push_latest_order_book(&mut batch.events, event);
                }
            }
            ClobFrame::Tagged(TaggedMessage::BestBidAsk(fields)) => {
                batch.wire.best_bid_asks = batch.wire.best_bid_asks.saturating_add(1);
                batch.recognized_topic |= books.token_index(&fields.asset_id).is_some();
                let emit_diagnostic = subscribed_token(active_tokens, &fields.asset_id);
                let diagnostic_token = fields.asset_id.clone();
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
                    fields.asset_id.into_owned(),
                    fields.best_bid,
                    fields.best_ask,
                    exchange_timestamp_ns,
                    local_now,
                ) {
                    Some(MarketEvent::Quote(quote)) => {
                        if let Some(event) = books.canonicalize_quote(quote, received_at) {
                            batch.events.push(event);
                        }
                    }
                    _ => {
                        batch.wire.ignored = batch.wire.ignored.saturating_add(1);
                        if emit_diagnostic {
                            batch.diagnostics.push(ClobDiagnostic {
                                key: "invalid_best_bid_ask",
                                detail: format!(
                                    "token={} ts={exchange_timestamp_ns}",
                                    diagnostic_token,
                                ),
                            });
                        }
                    }
                }
            }
            ClobFrame::Tagged(TaggedMessage::Trade(fields)) => {
                batch.wire.trades = batch.wire.trades.saturating_add(1);
                batch.recognized_topic |= books.token_index(&fields.asset_id).is_some();
                match make_trade_event(fields, local_now) {
                    Some(event) => batch.events.push(event),
                    None => batch.wire.ignored = batch.wire.ignored.saturating_add(1),
                }
            }
            ClobFrame::Tagged(TaggedMessage::LastTradePrice(fields)) => {
                batch.wire.last_trade_prices = batch.wire.last_trade_prices.saturating_add(1);
                batch.recognized_topic |= books.token_index(&fields.asset_id).is_some();
                match make_trade_event(fields, local_now) {
                    Some(event) => batch.events.push(event),
                    None => batch.wire.ignored = batch.wire.ignored.saturating_add(1),
                }
            }
            ClobFrame::Tagged(TaggedMessage::TickSizeChange(fields)) => {
                batch.wire.tick_size_changes = batch.wire.tick_size_changes.saturating_add(1);
                batch.recognized_topic |= books.token_index(&fields.asset_id).is_some();
                match make_tick_size_event(fields, local_now) {
                    Some(MarketEvent::TickSizeChange(change)) => {
                        let released = books.apply_tick_size_change(
                            &change,
                            received_at,
                            local_now,
                            &mut batch.wire,
                        );
                        // The strategy must observe the new grid before any
                        // same-batch 0.001 book/quote that was held behind it.
                        batch.events.push(MarketEvent::TickSizeChange(change));
                        for event in released {
                            push_latest_order_book(&mut batch.events, event);
                        }
                    }
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
                // `new_market` and `market_resolved` are expected control
                // frames. They were previously formatted into diagnostic
                // strings even when the sampler suppressed them; count them
                // without allocating. Truly unknown frames share one static
                // key and format a bounded preview only on the anomaly path.
                if !known_ignored {
                    batch.diagnostics.push(ClobDiagnostic {
                        key: "unknown_event",
                        detail: format!(
                            "event_type={} raw={}",
                            event_type,
                            diagnostic_preview(&value),
                        ),
                    });
                }
            }
        }
    }
    let construction_total_ns = construction_started
        .elapsed()
        .as_nanos()
        .min(u64::MAX as u128) as u64;
    phases.event_construction_ns = construction_total_ns
        .saturating_sub(phases.book_apply_ns)
        .saturating_sub(phases.price_change_apply_ns);
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

fn make_trade_event(t: TradeFields<'_>, now: u64) -> Option<MarketEvent> {
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
    let side_text = t.side.trim();
    let side = if side_text.eq_ignore_ascii_case("BUY") {
        Side::Buy
    } else if side_text.eq_ignore_ascii_case("SELL") {
        Side::Sell
    } else {
        return None;
    };
    let exchange_timestamp_ns = timestamp_value_to_ns(t.timestamp.as_ref(), now);
    if exchange_timestamp_ns > now.saturating_add(MAX_PUBLIC_EVENT_FUTURE_SKEW_NS) {
        return None;
    }
    let clean_id = |value: Option<std::borrow::Cow<'_, str>>| {
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
        symbol: t.asset_id.into_owned(),
        exchange_trade_id,
        price,
        quantity,
        side,
        exchange_timestamp_ns,
        local_timestamp_ns: now,
    }))
}

fn make_tick_size_event(t: TickSizeFields<'_>, now: u64) -> Option<MarketEvent> {
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
        symbol: t.asset_id.into_owned(),
        old_tick_size: t.old_tick_size,
        new_tick_size: t.new_tick_size,
        exchange_timestamp_ns,
        local_timestamp_ns: now,
    }))
}

fn make_inline_rtds_event(r: InlineRtdsFields<'_>, local_now: u64) -> Option<MarketEvent> {
    let symbol = r.pair.or(r.symbol).or(r.filter)?.into_owned();
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
        let (event_tx, event_rx) = clob_event_lanes();
        let (ctrl_tx, ctrl_rx) = tokio::sync::mpsc::channel::<WsCtrl>(16);
        self.event_rx = Some(event_rx);
        self.ws_ctrl_tx = Some(ctrl_tx);

        if self.liveness.reconnect_reason().is_some() {
            self.clob_runtime_fallback = true;
        }
        self.liveness.begin_connection(clob_token_count > 0);
        let task = clob_ws_task(
            clob_subscription,
            event_tx,
            ctrl_rx,
            shutdown,
            self.clob_subscribed_once.clone(),
            self.liveness.clone(),
        );
        let join = if self.clob_runtime_fallback {
            warn!("[Polymarket] CLOB reader using general-runtime fallback after external stall");
            crate::async_rt::handle().spawn(task)
        } else {
            crate::async_rt::clob_handle().spawn(task)
        };
        let abort = join.abort_handle();
        self.liveness.install_abort(abort.clone());
        self.clob_task_abort = Some(abort);

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
                            _tick_size: condition.tick_size,
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
                            _tick_size: condition.tick_size,
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
        self.update_liveness_subscription();
        Ok(())
    }

    fn next_event(&mut self) -> Result<Option<MarketEvent>> {
        if let Some(reason) = self.liveness.reconnect_reason() {
            return Err(self.fail_supervised_clob_task(&reason));
        }

        // REST maintenance may discover the next event up to a minute early.
        // Drain that explicit registration channel before normal pending
        // events so strategy routing learns its token ids promptly.
        self.drain_rest_future_events();

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
        if let Some(rx) = &mut self.event_rx {
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
            let _ = tx.try_send(WsCtrl::Shutdown);
        }
        if let Some(abort) = self.clob_task_abort.take() {
            abort.abort();
        }
        self.event_rx = None;
        self.liveness.end_connection();
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
mod clob_event_lane_tests {
    use super::*;

    #[test]
    fn simd_clob_schema_borrows_wire_token_and_decimal_strings() {
        let mut bytes = br#"{"event_type":"price_change","price_changes":[{"asset_id":"resident-token","price":"0.41","size":"2","side":"BUY"}]}"#.to_vec();
        let frame: ClobFrame<'_> = simd_json::serde::from_slice(&mut bytes).unwrap();
        let ClobFrame::Tagged(TaggedMessage::PriceChange(fields)) = frame else {
            panic!("price-change frame expected");
        };
        let change = &fields.price_changes[0];
        assert!(matches!(change.asset_id, std::borrow::Cow::Borrowed(_)));
        assert!(matches!(change.price, WireDecimal::String(std::borrow::Cow::Borrowed(_))));
        assert!(matches!(change.size, WireDecimal::String(std::borrow::Cow::Borrowed(_))));
    }

    fn latency_summary_ns(samples: &mut [u64]) -> (u64, u64, u64, u64) {
        samples.sort_unstable();
        let percentile = |per_mille: usize| {
            let rank = (samples.len() * per_mille).div_ceil(1000).max(1);
            samples[rank.saturating_sub(1).min(samples.len() - 1)]
        };
        (
            percentile(500),
            percentile(990),
            percentile(999),
            *samples.last().unwrap(),
        )
    }

    fn quote(sequence: u64) -> MarketEvent {
        MarketEvent::Quote(QuoteTick {
            exchange: Exchange::Polymarket,
            symbol: "token".to_string(),
            bid_price: 0.4,
            bid_qty: 0.0,
            ask_price: 0.6,
            ask_qty: 0.0,
            exchange_timestamp_ns: sequence,
            local_timestamp_ns: sequence,
        })
    }

    #[test]
    fn replaceable_lane_is_bounded_and_keeps_the_newest_snapshot() {
        let (tx, mut rx) = clob_event_lanes();
        for sequence in 0..(CLOB_REPLACEABLE_EVENT_CAPACITY as u64 + 17) {
            assert!(tx.send(quote(sequence)));
        }
        assert_eq!(tx.replaceable_tx.len(), CLOB_REPLACEABLE_EVENT_CAPACITY);
        assert_eq!(tx.overflow_totals(), (0, 17));

        let mut newest = 0;
        for _ in 0..CLOB_REPLACEABLE_EVENT_CAPACITY {
            let MarketEvent::Quote(quote) = rx.recv_timeout(Duration::ZERO).unwrap() else {
                panic!("replaceable lane returned a non-quote event");
            };
            newest = newest.max(quote.local_timestamp_ns);
        }
        assert_eq!(
            newest,
            CLOB_REPLACEABLE_EVENT_CAPACITY as u64 + 16,
            "the most recent snapshot must survive overflow",
        );
    }

    #[test]
    fn tiered_lanes_preserve_cross_lane_event_order() {
        let (tx, mut rx) = clob_event_lanes();
        assert!(tx.send(quote(1)));
        assert!(tx.send(MarketEvent::Disconnected {
            exchange: Exchange::Polymarket,
            reason: "test".to_string(),
        }));
        assert!(matches!(
            rx.recv_timeout(Duration::ZERO).unwrap(),
            MarketEvent::Quote(_)
        ));
        assert!(matches!(
            rx.recv_timeout(Duration::ZERO).unwrap(),
            MarketEvent::Disconnected { .. }
        ));
    }

    #[test]
    fn critical_lane_is_bounded_and_fails_closed_on_overflow() {
        let (tx, _rx) = clob_event_lanes();
        for _ in 0..CLOB_CRITICAL_EVENT_CAPACITY {
            assert!(tx.send(MarketEvent::Connected {
                exchange: Exchange::Polymarket,
            }));
        }
        assert!(!tx.send(MarketEvent::Connected {
            exchange: Exchange::Polymarket,
        }));
        assert_eq!(tx.overflow_totals().0, 1);
    }

    #[test]
    #[ignore = "focused bounded CLOB bridge latency benchmark"]
    fn benchmark_tiered_lane_send_receive() {
        const EVENTS_PER_LANE: usize = 50_000;
        let (tx, mut rx) = clob_event_lanes();
        let mut critical = Vec::with_capacity(EVENTS_PER_LANE);
        let mut replaceable = Vec::with_capacity(EVENTS_PER_LANE);
        let mut peak_depth = 0usize;

        for sequence in 0..EVENTS_PER_LANE as u64 {
            let started = Instant::now();
            assert!(tx.send(MarketEvent::Connected {
                exchange: Exchange::Polymarket,
            }));
            peak_depth = peak_depth.max(tx.len());
            assert!(matches!(
                rx.recv_timeout(Duration::ZERO).unwrap(),
                MarketEvent::Connected { .. }
            ));
            critical.push(started.elapsed().as_nanos().min(u64::MAX as u128) as u64);

            let started = Instant::now();
            assert!(tx.send(quote(sequence)));
            peak_depth = peak_depth.max(tx.len());
            assert!(matches!(
                rx.recv_timeout(Duration::ZERO).unwrap(),
                MarketEvent::Quote(_)
            ));
            replaceable.push(started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        }

        let critical = latency_summary_ns(&mut critical);
        let replaceable = latency_summary_ns(&mut replaceable);
        let overflows = tx.overflow_totals();
        eprintln!(
            "bounded CLOB bridge boundary (sender enqueue + tier merge dequeue, events_per_lane={EVENTS_PER_LANE}, peak_depth={peak_depth}, critical_overflow={}, replaceable_eviction={}) ns: critical median/p99/p999/max={}/{}/{}/{} replaceable={}/{}/{}/{}",
            overflows.0,
            overflows.1,
            critical.0,
            critical.1,
            critical.2,
            critical.3,
            replaceable.0,
            replaceable.1,
            replaceable.2,
            replaceable.3,
        );
        assert_eq!(overflows, (0, 0));
        assert_eq!(peak_depth, 1);
    }
}

#[cfg(test)]
mod pick_current_event_tests {
    use super::*;

    #[test]
    fn clob_socket_poll_window_records_actionable_gaps() {
        let start = Instant::now();
        let mut last_poll_at = None;
        let mut window = ClobSocketPollWindow::default();

        window.record_poll(&mut last_poll_at, start);
        window.record_poll(&mut last_poll_at, start + Duration::from_millis(10));
        window.record_poll(&mut last_poll_at, start + Duration::from_millis(31));

        assert_eq!(window.poll_calls, 3);
        assert_eq!(window.poll_gap_samples, 2);
        assert_eq!(window.poll_gap_max_us, 21_000);
        assert_eq!(window.poll_gap_over_20ms, 1);
    }

    #[test]
    fn standby_requires_an_observed_recent_data_frame() {
        let now = Instant::now();
        assert!(!clob_standby_is_hot(false, now, now));
        assert!(clob_standby_is_hot(
            true,
            now,
            now + CLOB_STANDBY_MAX_RAW_AGE,
        ));
        assert!(!clob_standby_is_hot(
            true,
            now,
            now + CLOB_STANDBY_MAX_RAW_AGE + Duration::from_nanos(1),
        ));
    }

    #[test]
    fn failover_reseed_is_not_ready_until_every_token_has_a_full_book() {
        let subscription = ClobSubscription {
            tokens: vec!["up".to_string(), "down".to_string()],
            canonical_events: Vec::new(),
        };
        let mut books = ClobLocalBooks::default();
        reset_clob_books_for_failover(&mut books, &subscription, Instant::now());

        assert!(!books.has_all_seeded(&subscription.tokens));
        assert!(books.quarantined_tokens.contains("up"));
        assert!(books.quarantined_tokens.contains("down"));
        assert_eq!(books.repair_started_at.len(), 2);
    }

    #[test]
    fn clob_burst_metrics_retain_microburst_and_unread_high_water() {
        let start = Instant::now();
        let mut metrics = ClobBurstMetrics::new(start);

        metrics.record_frame(start + Duration::from_millis(10), 100);
        metrics.record_frame(start + Duration::from_millis(20), 200);
        metrics.record_socket_probe(
            start + Duration::from_millis(99),
            Some(7),
            Duration::from_micros(3),
        );
        metrics.record_frame(start + Duration::from_millis(110), 50);
        metrics.record_socket_probe(
            start + Duration::from_millis(199),
            Some(11),
            Duration::from_micros(5),
        );
        metrics.finish_window(start + Duration::from_millis(200));

        assert_eq!(metrics.frames, 3);
        assert_eq!(metrics.bytes, 350);
        assert_eq!(metrics.max_frame_bytes, 200);
        assert_eq!(metrics.peak_100ms_frames, 2);
        assert_eq!(metrics.peak_100ms_bytes, 300);
        assert_eq!(metrics.peak_100ms_max_frame_bytes, 200);
        assert_eq!(metrics.kernel_unread_latest, Some(11));
        assert_eq!(metrics.kernel_unread_max, 11);
        assert_eq!(metrics.kernel_unread_samples, 2);
        assert_eq!(metrics.kernel_unread_errors, 0);
        assert_eq!(metrics.socket_probe_max_us, 5);
    }

    #[cfg(unix)]
    #[test]
    fn clob_receive_buffer_is_configurable_and_observable() {
        let (socket, _peer) = std::os::unix::net::UnixStream::pair().unwrap();
        let actual = configure_clob_socket_receive_buffer(Some(socket.as_raw_fd()), 64 * 1024)
            .expect("configure receive buffer");
        assert!(actual >= 64 * 1024, "actual receive buffer={actual}");
    }

    #[cfg(unix)]
    #[test]
    fn clob_socket_unread_probe_reports_kernel_receive_queue_bytes() {
        use std::io::Write;

        let (mut writer, reader) = std::os::unix::net::UnixStream::pair().unwrap();
        writer.write_all(b"abcdef").unwrap();

        assert_eq!(
            sample_socket_unread_bytes(Some(reader.as_raw_fd())),
            Some(6)
        );
    }

    #[test]
    fn external_liveness_keeps_three_heartbeats_independent() {
        let health = PolymarketLiveness::default();
        health
            .connection_started_ns
            .store(1_000_000_000, Ordering::Release);
        health.connected.store(true, Ordering::Release);
        health.active.store(true, Ordering::Release);
        health
            .last_feed_loop_ns
            .store(2_000_000_000, Ordering::Release);
        health.record_raw_frame(5_000_000_000);
        health.record_market_data(4_000_000_000);

        let snapshot = health.snapshot_at(7_500_000_000);
        assert_eq!(snapshot.raw_frame_age_ns, Some(2_500_000_000));
        assert_eq!(snapshot.market_data_age_ns, Some(3_500_000_000));
        assert_eq!(snapshot.feed_loop_age_ns, Some(5_500_000_000));
        assert!(snapshot.first_raw_frame_seen);
    }

    #[test]
    fn recovery_milestones_are_sticky_across_in_worker_reconnects() {
        let health = PolymarketLiveness::default();
        health.mark_connecting();
        health.mark_subscribed();
        health.record_raw_frame(1);
        health.mark_ready();

        health.begin_connection(true);
        let snapshot = health.snapshot_at(2);
        assert!(snapshot.connecting_seen);
        assert!(snapshot.subscribed_seen);
        assert!(snapshot.first_raw_frame_seen);
        assert!(snapshot.ready_seen);
    }

    #[test]
    fn replacement_recovery_clock_pauses_between_events_and_restarts_on_tokens() {
        let health = PolymarketLiveness::default();
        health.begin_connection(true);
        assert!(health.snapshot().recovery_age_ns.is_some());

        health.update_subscription(false, 0);
        assert_eq!(health.snapshot().recovery_age_ns, None);

        health.update_subscription(true, 123);
        assert!(health.snapshot().recovery_age_ns.is_some());
    }

    #[test]
    fn linux_tcp_info_tail_is_parsed_without_libc_field_support() {
        let mut info = [0_u8; LINUX_TCP_INFO_PREFIX_LEN];
        let rcv_space = 256_000_u32;
        let rcv_wnd = 128_000_u32;
        let rcv_ssthresh = 512_000_u32;
        let total_retrans = 17_u32;
        info[LINUX_TCP_INFO_RCV_WSCALE_OFFSET] = 7 << 4;
        info[LINUX_TCP_INFO_RCV_SPACE_OFFSET..LINUX_TCP_INFO_RCV_SPACE_OFFSET + 4]
            .copy_from_slice(&rcv_space.to_ne_bytes());
        info[LINUX_TCP_INFO_RCV_WND_OFFSET..LINUX_TCP_INFO_RCV_WND_OFFSET + 4]
            .copy_from_slice(&rcv_wnd.to_ne_bytes());
        info[LINUX_TCP_INFO_RCV_SSTHRESH_OFFSET..LINUX_TCP_INFO_RCV_SSTHRESH_OFFSET + 4]
            .copy_from_slice(&rcv_ssthresh.to_ne_bytes());
        info[LINUX_TCP_INFO_TOTAL_RETRANS_OFFSET..LINUX_TCP_INFO_TOTAL_RETRANS_OFFSET + 4]
            .copy_from_slice(&total_retrans.to_ne_bytes());

        assert_eq!((info[LINUX_TCP_INFO_RCV_WSCALE_OFFSET] >> 4) & 0x0f, 7);
        assert_eq!(
            linux_tcp_info_u32(
                &info,
                LINUX_TCP_INFO_PREFIX_LEN,
                LINUX_TCP_INFO_TOTAL_RETRANS_OFFSET,
            ),
            Some(total_retrans)
        );
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

    fn mk_binary_event(name: &str, start_secs: u64, end_secs: u64) -> PolymarketEvent {
        let mut event = mk_event(start_secs, end_secs);
        event.id = format!("{name}-event-{start_secs}");
        event.slug = format!("{name}-updown-5m-{start_secs}");
        event.markets.push(PolyMarketInfo {
            id: format!("{name}-market-{start_secs}"),
            question: format!("{name} up or down?"),
            condition_id: format!("{name}-condition-{start_secs}"),
            slug: format!("{name}-market-{start_secs}"),
            clob_token_ids: vec![
                format!("{name}-up-{start_secs}"),
                format!("{name}-down-{start_secs}"),
            ],
            outcomes: vec!["Up".to_string(), "Down".to_string()],
            outcome_prices: vec!["0.5".to_string(), "0.5".to_string()],
            active: true,
            closed: false,
            volume: 0.0,
            liquidity: 0.0,
            tick_size: 0.01,
            order_min_size: 5.0,
            group_item_title: String::new(),
            event_start_time: chrono::DateTime::<chrono::Utc>::from_timestamp(start_secs as i64, 0)
                .unwrap()
                .format("%Y-%m-%dT%H:%M:%SZ")
                .to_string(),
            base_fee: 0,
            fee_schedule: FeeSchedule::default(),
        });
        event
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
        let cache = ArcSwap::from_pointee(HashMap::new());
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
            cache.load().len(),
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
        let cache = ArcSwap::from_pointee(HashMap::new());
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
        cache_gamma_events_at(&cache, "series-1", &[], now_instant);
        assert!(cache.load().is_empty());
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
                    _tick_size: 0.01,
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
            Some(MarketEvent::EventEnd {
                event_id,
                retired_symbols,
                ..
            }) if event_id == "expired" && retired_symbols == &["old-up"]
        ));
        assert!(matches!(
            market.pending_events.get(1),
            Some(MarketEvent::EventStart { .. })
        ));
        assert!(market
            .pending_events
            .iter()
            .any(|event| matches!(event, MarketEvent::Instrument(_))));
    }

    #[test]
    fn maintenance_rest_cache_promotes_event_into_strategy_registration() {
        let started_ns = now_ns();
        let current_end_secs = now().saturating_sub(1);
        let series_id = format!("rest-promotion-{started_ns}");
        let mut next_event = mk_event(current_end_secs, current_end_secs + 300);
        next_event.markets.push(PolyMarketInfo {
            id: "rest-market".to_string(),
            question: "BTC up or down?".to_string(),
            condition_id: "rest-condition".to_string(),
            slug: "rest-market".to_string(),
            clob_token_ids: vec!["rest-up".to_string(), "rest-down".to_string()],
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
        cache_gamma_events(&series_id, std::slice::from_ref(&next_event));

        let mut market = PolymarketMarket::new();
        market.series.push(SeriesState {
            name: "series:btc-up-or-down-5m".to_string(),
            interval_minutes: -1,
            market: MarketState {
                event_id: "expired".to_string(),
                start_ns: current_end_secs
                    .saturating_sub(300)
                    .saturating_mul(1_000_000_000),
                end_ns: current_end_secs.saturating_mul(1_000_000_000),
                symbols: vec![SymbolState {
                    token_id: "old-up".to_string(),
                    _outcome: "Up".to_string(),
                    _condition_id: "old-condition".to_string(),
                    _tick_size: 0.01,
                }],
            },
            series_id: Some(series_id),
            next_retry_ns: 0,
            refresh_fail_count: 0,
            refresh_fail_first_ns: 0,
            refresh_idling_logged: false,
            rotation_refresh: None,
        });

        market.check_rotation().unwrap();

        assert_eq!(market.series[0].market.event_id, next_event.id);
        assert_eq!(market.token_to_series.get("rest-up"), Some(&0));
        assert!(market.pending_events.iter().any(|event| {
            matches!(
                event,
                MarketEvent::Instrument(Instrument::BinaryOption(instrument))
                    if instrument.condition_id == "rest-condition"
            )
        }));
    }

    #[test]
    fn rest_discovery_channel_registers_future_event_without_rotating_clob() {
        let base = now();
        let current_end_secs = base + 120;
        let series_id = format!("future-channel-{}", clob_monotonic_now_ns());
        let next_event = mk_binary_event("btc", current_end_secs, current_end_secs + 300);
        cache_gamma_events(&series_id, std::slice::from_ref(&next_event));

        let mut market = PolymarketMarket::new();
        market.token_to_series.insert("old-up".to_string(), 0);
        market.series.push(SeriesState {
            name: "series:btc-up-or-down-5m".to_string(),
            interval_minutes: -1,
            market: MarketState {
                event_id: "current-event".to_string(),
                start_ns: base.saturating_sub(180).saturating_mul(1_000_000_000),
                end_ns: current_end_secs.saturating_mul(1_000_000_000),
                symbols: vec![SymbolState {
                    token_id: "old-up".to_string(),
                    _outcome: "Up".to_string(),
                    _condition_id: "old-condition".to_string(),
                    _tick_size: 0.01,
                }],
            },
            series_id: Some(series_id.clone()),
            next_retry_ns: 0,
            refresh_fail_count: 0,
            refresh_fail_first_ns: 0,
            refresh_idling_logged: false,
            rotation_refresh: None,
        });
        let (ctrl_tx, mut ctrl_rx) = tokio::sync::mpsc::channel(4);
        market.ws_ctrl_tx = Some(ctrl_tx);

        let discovered = fetch_next_event(&series_id, current_end_secs)
            .unwrap()
            .expect("cached future event");
        assert_eq!(discovered.id, next_event.id);
        market.drain_rest_future_events();

        assert_eq!(market.series[0].market.event_id, "current-event");
        assert_eq!(market.current_tokens(), vec!["old-up".to_string()]);
        let WsCtrl::Prepare(subscription) = ctrl_rx.try_recv().unwrap() else {
            panic!("future discovery must prewarm without activating");
        };
        assert!(subscription.tokens.contains(&"old-up".to_string()));
        for token in &next_event.markets[0].clob_token_ids {
            assert!(subscription.tokens.contains(token));
        }
        assert!(market.pending_events.iter().any(|event| {
            matches!(
                event,
                MarketEvent::Instrument(Instrument::BinaryOption(instrument))
                    if instrument.condition_id == next_event.markets[0].condition_id
            )
        }));
        assert!(
            !market
                .pending_events
                .iter()
                .any(|event| matches!(event, MarketEvent::EventStart { .. })),
            "future registration must not advance recorder/event lifecycle",
        );
    }

    #[test]
    fn future_event_continuity_rejects_a_skipped_five_minute_interval() {
        let start = now() + 300;
        let exact = mk_binary_event("btc", start, start + 300);
        assert_eq!(contiguous_event_error(&exact, start, 300), None);

        let skipped = mk_binary_event("btc", start + 300, start + 600);
        let error = contiguous_event_error(&skipped, start, 300)
            .expect("skipping one event must be explicit");
        assert!(error.contains("future-event continuity gap"));
        assert!(error.contains(&format!("expected_start={start}")));
        assert!(error.contains(&format!("actual_start={}", start + 300)));
    }

    #[test]
    fn rest_discovery_gap_is_not_registered_as_the_next_event() {
        let base = now();
        let current_end_secs = base + 120;
        let series_id = format!("future-gap-{}", clob_monotonic_now_ns());
        let skipped = mk_binary_event("btc", current_end_secs + 300, current_end_secs + 600);
        let mut market = PolymarketMarket::new();
        market.series.push(SeriesState {
            name: "series:btc-up-or-down-5m".to_string(),
            interval_minutes: -1,
            market: MarketState {
                event_id: "current-event".to_string(),
                start_ns: base.saturating_sub(180).saturating_mul(1_000_000_000),
                end_ns: current_end_secs.saturating_mul(1_000_000_000),
                symbols: Vec::new(),
            },
            series_id: Some(series_id.clone()),
            next_retry_ns: 0,
            refresh_fail_count: 0,
            refresh_fail_first_ns: 0,
            refresh_idling_logged: false,
            rotation_refresh: None,
        });

        publish_rest_future_event(&series_id, &skipped);
        market.drain_rest_future_events();
        assert!(market.pending_events.iter().all(|event| {
            !matches!(
                event,
                MarketEvent::Instrument(Instrument::BinaryOption(instrument))
                    if instrument.condition_id == skipped.markets[0].condition_id
            )
        }));
    }

    #[test]
    fn rest_channel_supports_many_consecutive_rotations() {
        let base = now();
        let series_id = format!("continuous-rotation-{}", clob_monotonic_now_ns());
        let mut market = PolymarketMarket::new();
        market.series.push(SeriesState {
            name: "series:btc-up-or-down-5m".to_string(),
            interval_minutes: -1,
            market: MarketState {
                event_id: "seed-event".to_string(),
                start_ns: base.saturating_sub(300).saturating_mul(1_000_000_000),
                end_ns: base.saturating_mul(1_000_000_000),
                symbols: Vec::new(),
            },
            series_id: Some(series_id.clone()),
            next_retry_ns: 0,
            refresh_fail_count: 0,
            refresh_fail_first_ns: 0,
            refresh_idling_logged: false,
            rotation_refresh: None,
        });

        for generation in 0..12_u64 {
            let start_secs = base + generation * 300;
            let event = mk_binary_event("btc", start_secs, start_secs + 300);
            publish_rest_future_event(&series_id, &event);
            market.drain_rest_future_events();
            market.series[0].market.end_ns = now_ns().saturating_sub(1);
            market.check_rotation().unwrap();
            assert_eq!(
                market.series[0].market.event_id, event.id,
                "rotation generation {generation} did not promote the REST event",
            );
            assert_eq!(market.current_tokens().len(), 2);
        }
        assert!(market.series[0].rotation_refresh.is_none());
    }

    #[test]
    fn simultaneous_series_rotations_emit_one_combined_resubscribe() {
        let started_ns = now_ns();
        let make_market = |name: &str| PolyMarketInfo {
            id: format!("{name}-market"),
            question: format!("{name} up or down?"),
            condition_id: format!("{name}-condition"),
            slug: format!("{name}-market"),
            clob_token_ids: vec![format!("{name}-up"), format!("{name}-down")],
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
        };
        let make_event = |name: &str| {
            let mut event = mk_event(now() - 1, now() + 299);
            event.id = format!("{name}-event");
            event.slug = format!("{name}-slug");
            event.markets.push(make_market(name));
            event
        };

        let (btc_tx, btc_rx) = crossbeam_channel::bounded(1);
        let (eth_tx, eth_rx) = crossbeam_channel::bounded(1);
        btc_tx
            .send(Ok(("btc-series-id".to_string(), make_event("btc"))))
            .unwrap();

        let expired_series =
            |name: &str, old_token: &str, rx: crossbeam_channel::Receiver<RotationFetchResult>| {
                SeriesState {
                    name: format!("series:{name}"),
                    interval_minutes: -1,
                    market: MarketState {
                        event_id: format!("old-{name}"),
                        start_ns: started_ns.saturating_sub(300_000_000_000),
                        end_ns: started_ns.saturating_sub(1),
                        symbols: vec![SymbolState {
                            token_id: old_token.to_string(),
                            _outcome: "Up".to_string(),
                            _condition_id: format!("old-{name}-condition"),
                            _tick_size: 0.01,
                        }],
                    },
                    series_id: Some(format!("{name}-series-id")),
                    next_retry_ns: 0,
                    refresh_fail_count: 0,
                    refresh_fail_first_ns: 0,
                    refresh_idling_logged: false,
                    rotation_refresh: Some(RotationRefresh { started_ns, rx }),
                }
            };

        let mut market = PolymarketMarket::new();
        market.series.push(expired_series("btc", "old-btc", btc_rx));
        market.series.push(expired_series("eth", "old-eth", eth_rx));
        let (ctrl_tx, mut ctrl_rx) = tokio::sync::mpsc::channel(16);
        market.ws_ctrl_tx = Some(ctrl_tx);

        market.check_rotation().unwrap();
        assert!(market.clob_resubscribe_pending);
        assert!(matches!(
            ctrl_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));

        eth_tx
            .send(Ok(("eth-series-id".to_string(), make_event("eth"))))
            .unwrap();
        market.check_rotation().unwrap();
        let WsCtrl::Resubscribe(subscription) = ctrl_rx.try_recv().unwrap() else {
            panic!("rotation wave must emit one combined resubscribe");
        };
        assert_eq!(subscription.tokens.len(), 4);
        assert!(subscription.tokens.contains(&"btc-up".to_string()));
        assert!(subscription.tokens.contains(&"btc-down".to_string()));
        assert!(subscription.tokens.contains(&"eth-up".to_string()));
        assert!(subscription.tokens.contains(&"eth-down".to_string()));
        assert!(matches!(
            ctrl_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert!(!market.clob_resubscribe_pending);
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
            tick_size: 0.01,
        }
    }

    fn order_book(event: &MarketEvent) -> &OrderBookSnapshot {
        let MarketEvent::OrderBook(book) = event else {
            panic!("expected order book, got {event:?}");
        };
        book
    }

    fn first_order_book(events: &[MarketEvent]) -> &OrderBookSnapshot {
        events
            .iter()
            .find_map(|event| match event {
                MarketEvent::OrderBook(book) => Some(book),
                _ => None,
            })
            .unwrap_or_else(|| panic!("expected order book in {events:?}"))
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
        assert_eq!(
            batch
                .events
                .iter()
                .filter(|event| matches!(event, MarketEvent::OrderBook(_)))
                .count(),
            1,
            "one latest book per event"
        );
        let current = first_order_book(&batch.events);
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
    fn split_same_timestamp_bbo_recovers_without_warning_or_repair() {
        let tokens = vec!["up".to_string()];
        let mut books = ClobLocalBooks::default();
        let received_at = Instant::now();
        process_clob_frame(
            r#"{"event_type":"book","asset_id":"up","bids":[{"price":"0.44","size":"10"},{"price":"0.43","size":"9"}],"asks":[{"price":"0.45","size":"11"}],"timestamp":"8000"}"#,
            &mut books,
            &tokens,
            received_at,
            16_000_000_000,
        );

        let first = process_clob_frame(
            r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.42","size":"8","side":"BUY","best_bid":"0.42","best_ask":"0.45"}],"timestamp":"8001"}"#,
            &mut books,
            &tokens,
            received_at + Duration::from_micros(50),
            16_001_000_000,
        );
        assert!(first.events.is_empty(), "intermediate top must not escape");
        assert_eq!(first.wire.bbo_mismatches, 0);
        assert!(first.repair_tokens.is_empty());

        let second = process_clob_frame(
            r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.44","size":"0","side":"BUY","best_bid":"0.42","best_ask":"0.45"},{"asset_id":"up","price":"0.43","size":"0","side":"BUY","best_bid":"0.42","best_ask":"0.45"}],"timestamp":"8001"}"#,
            &mut books,
            &tokens,
            received_at + Duration::from_micros(250),
            16_001_000_000,
        );
        assert_eq!(second.wire.bbo_transient_recoveries, 1);
        assert_eq!(second.wire.bbo_mismatches, 0);
        assert!(second.repair_tokens.is_empty());
        assert_eq!(second.events.len(), 1);
        assert_eq!(order_book(&second.events[0]).bids[0].price, 0.42);
    }

    #[test]
    fn persistent_bbo_mismatch_is_quarantined_and_requests_one_repair() {
        let tokens = vec!["up".to_string()];
        let mut books = ClobLocalBooks::default();
        let received_at = Instant::now();
        process_clob_frame(
            r#"{"event_type":"book","asset_id":"up","bids":[{"price":"0.44","size":"10"}],"asks":[{"price":"0.45","size":"11"}],"timestamp":"8100"}"#,
            &mut books,
            &tokens,
            received_at,
            16_100_000_000,
        );
        let first = process_clob_frame(
            r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.42","size":"8","side":"BUY","best_bid":"0.42","best_ask":"0.45"}],"timestamp":"8101"}"#,
            &mut books,
            &tokens,
            received_at + Duration::from_micros(50),
            16_101_000_000,
        );
        assert!(first.events.is_empty());

        let settled = books.flush_deferred_due(
            received_at + CLOB_BBO_SETTLE_INTERVAL + Duration::from_millis(1),
            16_105_000_000,
            &tokens,
        );
        assert!(settled.events.is_empty());
        assert_eq!(settled.wire.bbo_mismatches, 1);
        assert_eq!(settled.wire.bbo_repair_requests, 1);
        assert_eq!(settled.repair_tokens, vec!["up".to_string()]);
        assert!(books.quarantined_tokens.contains("up"));

        let again = books.flush_deferred_due(
            received_at + CLOB_BBO_SETTLE_INTERVAL + Duration::from_millis(2),
            16_106_000_000,
            &tokens,
        );
        assert!(
            again.repair_tokens.is_empty(),
            "repair requests are deduped"
        );
        assert_eq!(again.wire.bbo_mismatches, 0);
    }

    #[test]
    fn newer_timestamp_checkpoint_heals_delayed_delete_without_repair() {
        let tokens = vec!["up".to_string(), "down".to_string()];
        let mut books = ClobLocalBooks::new(&[canonical_event_spec()]);
        let received_at = Instant::now();
        process_clob_frame(
            r#"[{"event_type":"book","asset_id":"up","bids":[{"price":"0.44","size":"10"},{"price":"0.43","size":"9"}],"asks":[{"price":"0.45","size":"11"}],"timestamp":"9000"},{"event_type":"book","asset_id":"down","bids":[{"price":"0.55","size":"11"}],"asks":[{"price":"0.56","size":"10"}],"timestamp":"9000"}]"#,
            &mut books,
            &tokens,
            received_at,
            17_000_000_000,
        );

        let first = process_clob_frame(
            r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.42","size":"8","side":"BUY","best_bid":"0.42","best_ask":"0.45"}],"timestamp":"9001"}"#,
            &mut books,
            &tokens,
            received_at + Duration::from_millis(1),
            17_001_000_000,
        );
        assert!(first.events.iter().any(|event| matches!(
            event,
            MarketEvent::MarketDataHealth(MarketDataHealth {
                state: MarketDataHealthState::Settling,
                ..
            })
        )));
        assert!(
            first.events.iter().any(|event| matches!(
                event,
                MarketEvent::Quote(QuoteTick {
                    bid_price,
                    ask_price,
                    ..
                }) if *bid_price == 0.42 && *ask_price == 0.45
            )),
            "advertised L1 remains available for passive pricing"
        );

        // The deletion arrives with a newer timestamp. It must be applied
        // before deciding whether the older checkpoint failed.
        let healed = process_clob_frame(
            r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.44","size":"0","side":"BUY","best_bid":"0.42","best_ask":"0.45"},{"asset_id":"up","price":"0.43","size":"0","side":"BUY","best_bid":"0.42","best_ask":"0.45"}],"timestamp":"9002"}"#,
            &mut books,
            &tokens,
            received_at + Duration::from_millis(8),
            17_008_000_000,
        );
        assert_eq!(healed.wire.bbo_recovery_newer_timestamp, 1);
        assert_eq!(healed.wire.bbo_mismatches, 0);
        assert!(healed.repair_tokens.is_empty());
        assert_eq!(first_order_book(&healed.events).bids[0].price, 0.42);
        assert!(
            !healed.events.iter().any(|event| matches!(
                event,
                MarketEvent::MarketDataHealth(MarketDataHealth {
                    state: MarketDataHealthState::Healthy,
                    ..
                })
            )),
            "Settling→Healthy recovery is coalesced until stable"
        );
        let stable = books.flush_deferred_due(
            received_at + CLOB_HEALTH_RECOVERY_STABLE_INTERVAL + Duration::from_millis(10),
            17_510_000_000,
            &tokens,
        );
        assert!(
            stable.events.iter().any(|event| matches!(
                event,
                MarketEvent::MarketDataHealth(MarketDataHealth {
                    state: MarketDataHealthState::Healthy,
                    ..
                })
            )),
            "stable Healthy recovery is emitted after the merge window"
        );
    }

    #[test]
    fn stale_rest_repair_is_superseded_not_rejected() {
        let tokens = vec!["up".to_string()];
        let mut books = ClobLocalBooks::default();
        process_clob_frame(
            r#"{"event_type":"book","asset_id":"up","bids":[{"price":"0.40","size":"10"}],"asks":[{"price":"0.60","size":"11"}],"timestamp":"9102"}"#,
            &mut books,
            &tokens,
            Instant::now(),
            17_100_000_000,
        );
        books.quarantined_tokens.insert("up".to_string());
        let fields: BookFields = serde_json::from_str(
            r#"{"asset_id":"up","bids":[{"price":"0.39","size":"10"}],"asks":[{"price":"0.61","size":"11"}],"timestamp":"9101"}"#,
        )
        .unwrap();
        let mut counters = ClobWireCounters::default();
        let outcome = books
            .apply_book(
                fields,
                Instant::now(),
                17_101_000_000,
                ClobBookSource::RestRepair,
                &mut counters,
            )
            .unwrap();
        assert!(matches!(outcome, ClobBookApplyOutcome::Superseded { .. }));
        assert_eq!(counters.bbo_repair_superseded_by_ws, 1);
        assert!(books.quarantined_tokens.contains("up"));
    }

    #[test]
    fn authoritative_repair_records_recovery_and_returns_condition_healthy() {
        let tokens = vec!["up".to_string(), "down".to_string()];
        let mut books = ClobLocalBooks::new(&[canonical_event_spec()]);
        let received_at = Instant::now();
        process_clob_frame(
            r#"[{"event_type":"book","asset_id":"up","bids":[{"price":"0.44","size":"10"}],"asks":[{"price":"0.45","size":"11"}],"timestamp":"9300"},{"event_type":"book","asset_id":"down","bids":[{"price":"0.55","size":"11"}],"asks":[{"price":"0.56","size":"10"}],"timestamp":"9300"}]"#,
            &mut books,
            &tokens,
            received_at,
            17_300_000_000,
        );
        process_clob_frame(
            r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.42","size":"8","side":"BUY","best_bid":"0.42","best_ask":"0.45"}],"timestamp":"9301"}"#,
            &mut books,
            &tokens,
            received_at + Duration::from_millis(1),
            17_301_000_000,
        );
        let repairing = books.flush_deferred_due(
            received_at + Duration::from_millis(52),
            17_352_000_000,
            &tokens,
        );
        assert_eq!(repairing.repair_tokens, vec!["up".to_string()]);

        let fields: BookFields = serde_json::from_str(
            r#"{"asset_id":"up","bids":[{"price":"0.42","size":"8"}],"asks":[{"price":"0.45","size":"11"}],"timestamp":"9302"}"#,
        )
        .unwrap();
        let mut counters = ClobWireCounters::default();
        let outcome = books
            .apply_book(
                fields,
                received_at + Duration::from_millis(60),
                17_360_000_000,
                ClobBookSource::RestRepair,
                &mut counters,
            )
            .unwrap();
        let ClobBookApplyOutcome::Applied(events) = outcome else {
            panic!("repair must apply");
        };
        assert_eq!(counters.bbo_recovery_rest, 1);
        assert_eq!(counters.bbo_recovery_samples, 1);
        assert!(!events.iter().any(|event| matches!(
            event,
            MarketEvent::MarketDataHealth(MarketDataHealth {
                state: MarketDataHealthState::Healthy,
                ..
            })
        )));
        let stable = books.flush_deferred_due(
            received_at
                + Duration::from_millis(60)
                + CLOB_HEALTH_RECOVERY_STABLE_INTERVAL
                + Duration::from_millis(10),
            17_870_000_000,
            &tokens,
        );
        assert!(stable.events.iter().any(|event| matches!(
            event,
            MarketEvent::MarketDataHealth(MarketDataHealth {
                state: MarketDataHealthState::Healthy,
                ..
            })
        )));
    }

    #[test]
    fn bbo_tick_distance_reports_largest_side_gap() {
        let expected = ReportedBbo {
            bid: Some(Some(Decimal::from_str("0.42").unwrap())),
            ask: Some(Some(Decimal::from_str("0.47").unwrap())),
        };
        assert_eq!(
            bbo_tick_distance(
                &expected,
                (
                    Some(Decimal::from_str("0.44").unwrap()),
                    Some(Decimal::from_str("0.46").unwrap()),
                ),
                Some(Decimal::from_str("0.01").unwrap()),
            ),
            Some(2)
        );
    }

    #[test]
    fn degraded_bbo_health_is_condition_scoped() {
        let specs = [
            canonical_event_spec(),
            CanonicalEventSpec {
                condition_id: "condition-2".to_string(),
                up_token: "up-2".to_string(),
                down_token: "down-2".to_string(),
                tick_size: 0.01,
            },
        ];
        let mut books = ClobLocalBooks::new(&specs);
        let tokens = vec![
            "up".to_string(),
            "down".to_string(),
            "up-2".to_string(),
            "down-2".to_string(),
        ];
        process_clob_frame(
            r#"[{"event_type":"book","asset_id":"up","bids":[{"price":"0.40","size":"10"}],"asks":[{"price":"0.60","size":"10"}],"timestamp":"9200"},{"event_type":"book","asset_id":"down","bids":[{"price":"0.40","size":"10"}],"asks":[{"price":"0.60","size":"10"}],"timestamp":"9200"},{"event_type":"book","asset_id":"up-2","bids":[{"price":"0.30","size":"10"}],"asks":[{"price":"0.70","size":"10"}],"timestamp":"9200"},{"event_type":"book","asset_id":"down-2","bids":[{"price":"0.30","size":"10"}],"asks":[{"price":"0.70","size":"10"}],"timestamp":"9200"}]"#,
            &mut books,
            &tokens,
            Instant::now(),
            17_200_000_000,
        );

        let degraded = books
            .mark_repair_failed(
                "up",
                "injected REST failure",
                Instant::now(),
                17_201_000_000,
            )
            .expect("condition must transition");
        let MarketEvent::MarketDataHealth(health) = degraded else {
            panic!("expected health event");
        };
        assert_eq!(health.market_id, "condition");
        assert_eq!(health.state, MarketDataHealthState::Degraded);
        assert!(!health.passive_ready);
        assert!(!health.taker_ready);
        assert_eq!(
            books.desired_health_state("condition-2"),
            Some(MarketDataHealthState::Healthy)
        );
        assert!(!books.market_is_quarantined("up-2"));
    }

    #[test]
    fn tick_narrowing_precedes_release_of_fine_grid_book() {
        let tokens = vec!["up".to_string(), "down".to_string()];
        let mut books = ClobLocalBooks::new(&[canonical_event_spec()]);
        let received_at = Instant::now();
        process_clob_frame(
            r#"[{"event_type":"book","asset_id":"up","bids":[{"price":"0.98","size":"10"}],"asks":[{"price":"0.99","size":"11"}],"timestamp":"8200"},{"event_type":"book","asset_id":"down","bids":[{"price":"0.01","size":"11"}],"asks":[{"price":"0.02","size":"10"}],"timestamp":"8200"}]"#,
            &mut books,
            &tokens,
            received_at,
            16_200_000_000,
        );
        let fine_grid = process_clob_frame(
            r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.99","size":"0","side":"SELL","best_bid":"0.98","best_ask":"0.999"},{"asset_id":"up","price":"0.999","size":"9","side":"SELL","best_bid":"0.98","best_ask":"0.999"}],"timestamp":"8201"}"#,
            &mut books,
            &tokens,
            received_at + Duration::from_micros(50),
            16_201_000_000,
        );
        assert!(
            fine_grid
                .events
                .iter()
                .all(|event| !matches!(event, MarketEvent::OrderBook(_))),
            "0.001 book must wait for its tick transition"
        );
        assert!(fine_grid.events.iter().any(|event| matches!(
            event,
            MarketEvent::MarketDataHealth(MarketDataHealth {
                state: MarketDataHealthState::Settling,
                ..
            })
        )));

        let tick = process_clob_frame(
            r#"{"event_type":"tick_size_change","asset_id":"up","old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"8201"}"#,
            &mut books,
            &tokens,
            received_at + Duration::from_micros(200),
            16_201_000_000,
        );
        assert_eq!(
            tick.events
                .iter()
                .filter(|event| matches!(
                    event,
                    MarketEvent::TickSizeChange(_) | MarketEvent::OrderBook(_)
                ))
                .count(),
            2
        );
        assert!(matches!(tick.events[0], MarketEvent::TickSizeChange(_)));
        assert_eq!(first_order_book(&tick.events).asks[0].price, 0.999);
        assert_eq!(
            books.current_ticks.get("condition"),
            Some(&Decimal::from_str("0.001").unwrap()),
        );
    }

    #[test]
    fn tick_narrowing_precedes_release_of_fine_grid_quote() {
        let tokens = vec!["up".to_string(), "down".to_string()];
        let mut books = ClobLocalBooks::new(&[canonical_event_spec()]);
        let received_at = Instant::now();
        let quote = process_clob_frame(
            r#"{"event_type":"best_bid_ask","asset_id":"up","best_bid":"0.998","best_ask":"0.999","timestamp":"8301"}"#,
            &mut books,
            &tokens,
            received_at,
            16_301_000_000,
        );
        assert!(quote.events.is_empty());

        let tick = process_clob_frame(
            r#"{"event_type":"tick_size_change","asset_id":"down","old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"8301"}"#,
            &mut books,
            &tokens,
            received_at + Duration::from_micros(200),
            16_301_000_000,
        );
        assert_eq!(tick.events.len(), 2);
        assert!(matches!(tick.events[0], MarketEvent::TickSizeChange(_)));
        let MarketEvent::Quote(quote) = &tick.events[1] else {
            panic!("fine-grid quote must follow the tick transition");
        };
        assert_eq!(quote.bid_price, 0.998);
        assert_eq!(quote.ask_price, 0.999);
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
        assert_eq!(
            seed.events
                .iter()
                .filter(|event| matches!(event, MarketEvent::OrderBook(_)))
                .count(),
            1
        );

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
        assert_eq!(batch.diagnostics.len(), 1);
        assert_eq!(batch.diagnostics[0].key, "unknown_event");
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

    #[test]
    fn clob_standby_requires_distinct_peer_ip() {
        let active_v4 = Some("192.0.2.10:443".parse().unwrap());
        let same_v4 = Some("192.0.2.10:8443".parse().unwrap());
        let other_v4 = Some("198.51.100.20:443".parse().unwrap());
        let active_v6 = Some("[2001:db8::10]:443".parse().unwrap());
        let same_v6 = Some("[2001:db8::10]:8443".parse().unwrap());
        let other_v6 = Some("[2001:db8::20]:443".parse().unwrap());

        assert!(!clob_peers_are_anti_affine(active_v4, same_v4));
        assert!(clob_peers_are_anti_affine(active_v4, other_v4));
        assert!(!clob_peers_are_anti_affine(active_v6, same_v6));
        assert!(clob_peers_are_anti_affine(active_v6, other_v6));
        assert!(!clob_peers_are_anti_affine(active_v4, None));
        assert!(!clob_peers_are_anti_affine(None, other_v4));
    }

    #[test]
    fn standby_slow_consumer_backoff_is_jittered_monotonic_and_capped() {
        let first = clob_standby_slow_consumer_delay(1, 7);
        let second = clob_standby_slow_consumer_delay(2, 7);
        let saturated = clob_standby_slow_consumer_delay(u32::MAX, 7);
        assert!(first >= CLOB_STANDBY_RECONNECT_DELAY);
        assert!(second > first);
        assert_eq!(saturated, CLOB_STANDBY_SLOW_CONSUMER_MAX_BACKOFF);
        assert_ne!(
            clob_standby_slow_consumer_delay(1, 7),
            clob_standby_slow_consumer_delay(1, 8),
            "independent lanes must not reconnect on one deterministic boundary",
        );
    }

    #[test]
    fn retired_token_diagnostics_are_filtered_before_publication() {
        let wire_tokens = vec!["current".to_string(), "retired".to_string()];
        let active_tokens = vec!["current".to_string()];
        let mut books = ClobLocalBooks::default();
        let mut retired = br#"{"event_type":"price_change","price_changes":[{"asset_id":"retired","price":"0.40","size":"-1","side":"BUY"}],"timestamp":"1000"}"#.to_vec();
        let mut retired_phases = ClobFramePhaseTimings::default();
        let retired_batch = process_clob_frame_in_place(
            &mut retired,
            &mut books,
            &wire_tokens,
            &active_tokens,
            Instant::now(),
            1_000_000_000,
            &mut retired_phases,
        );
        assert!(retired_batch.diagnostics.is_empty());

        let mut current = br#"{"event_type":"price_change","price_changes":[{"asset_id":"current","price":"0.40","size":"-1","side":"BUY"}],"timestamp":"1000"}"#.to_vec();
        let mut current_phases = ClobFramePhaseTimings::default();
        let current_batch = process_clob_frame_in_place(
            &mut current,
            &mut books,
            &wire_tokens,
            &active_tokens,
            Instant::now(),
            1_000_000_000,
            &mut current_phases,
        );
        assert_eq!(current_batch.diagnostics.len(), 1);
        assert_eq!(current_batch.diagnostics[0].key, "invalid_price_change");
        assert!(current_batch.diagnostics[0].detail.starts_with("token=current "));
    }

    #[test]
    fn bbo_repair_generation_fences_retired_token_requests() {
        let epoch = AtomicU64::new(0);
        let first = advance_clob_repair_generation(&epoch);
        assert!(clob_repair_generation_is_current(&epoch, first));
        let second = advance_clob_repair_generation(&epoch);
        assert!(!clob_repair_generation_is_current(&epoch, first));
        assert!(clob_repair_generation_is_current(&epoch, second));
    }
}
