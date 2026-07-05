//! Polymarket CLOB live order execution.
//!
//! Implements `ExchangeTrade` for submitting and canceling orders via the
//! Polymarket CLOB REST API, with EIP-712 order signing and HMAC request auth.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use log::{info, warn};

use crate::async_rt;
use crate::exchange::ExchangeTrade;
use crate::types::*;
use super::auth::PolyAuth;
use super::live_position::LivePositionManager;
use super::signer::{OrderSigner, SignatureType};

/// CLOB protocol version selector. Threaded through `SharedState` so
/// every signing / POST / auth path can dispatch at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClobVersion {
    V1,
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
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "v1" | "1" => ClobVersion::V1,
            _ => ClobVersion::V2,
        }
    }
    pub fn as_str(&self) -> &'static str {
        match self { ClobVersion::V1 => "v1", ClobVersion::V2 => "v2" }
    }
}

/// Default CLOB host when `api_url_prefix` is unset in config.
/// Post-2026-04-28 cutover this host serves the v2 schema directly;
/// the legacy `clob-v2.polymarket.com` staging hostname was folded
/// into the canonical name. Override via `api_url_prefix` only if
/// you need to point at a non-prod environment.
const DEFAULT_CLOB_BASE_URL: &str = "https://clob.polymarket.com";
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// Internal HTTP error discriminator for callers that need to map errors
/// to specific `OrderStatus` variants (Timeout vs server Status vs Other).
#[derive(Debug)]
pub(crate) enum HttpErr {
    Timeout,
    Status(u16, String),
    Other(String),
}

impl std::fmt::Display for HttpErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpErr::Timeout => write!(f, "timeout"),
            HttpErr::Status(code, body) => write!(f, "status {} ({})", code, body),
            HttpErr::Other(s) => write!(f, "{}", s),
        }
    }
}

impl From<HttpErr> for anyhow::Error {
    fn from(e: HttpErr) -> Self { anyhow!("{}", e) }
}

impl HttpErr {
    /// True when the server's response was either never received (timeout)
    /// or indicates server-side failure (HTTP 5xx). In both cases the order
    /// state is unknown — the server MAY have accepted/cancelled the order
    /// despite the error. Callers should emit timeout-equivalent statuses
    /// (NewOrderTimeout / CancelOrderTimeout) so the orphan reconciler can
    /// resolve state by re-querying.
    ///
    /// HTTP 4xx (other than 425), JSON parse errors, and other Transport
    /// errors are definitive rejections — the request reached the server
    /// and was rejected cleanly, so state is known (no order placed / no
    /// cancel performed).
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
            HttpErr::Other(_) => false,
        }
    }
}

/// Classify a reqwest error into our HttpErr taxonomy.
fn map_reqwest_err(e: reqwest::Error) -> HttpErr {
    if e.is_timeout() || e.is_connect() {
        // connect-timeout is functionally equivalent to a read timeout
        // for our purposes: the server never got to respond.
        HttpErr::Timeout
    } else if let Some(status) = e.status() {
        HttpErr::Status(status.as_u16(), e.to_string())
    } else {
        HttpErr::Other(e.to_string())
    }
}

/// Outcome of mapping a `not_canceled` reason returned by Polymarket
/// CLOB. Three categories: definite Cancelled, definite Filled, or
/// **Uncertain** (server's own wording is ambiguous — both states are
/// possible). Uncertain is handled differently by each caller:
///
///   * First-pass `handle_cancel_reply`: re-route to orphan-cancel
///     state (return `CancelOrderTimeout`) so the orphan reconciler
///     queries `GET /data/order/{oid}` to get an authoritative answer
///     before the strategy's `pending_orders` lock is released.
///   * Reconcile DELETE retry path: commit to Cancelled (server has
///     already been queried once via `fetch_order_status_by_id`; a
///     second deferral would loop indefinitely).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelReasonOutcome {
    /// Server explicitly says the order was cancelled (or never
    /// existed in a state we'd care about).
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
///     waits ~1 reconcile RTT (~150 ms) for `GET /data/order/{oid}`
///     to return MATCHED / CANCELED / 404 authoritatively.
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
///   * Other / unrecognised → **Cancelled** (conservative fallback).
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

fn cancel_not_canceled_outcome(reason: &str) -> CancelReasonOutcome {
    let r = reason.to_ascii_lowercase();
    let not_found = r.contains("not found")
        || r.contains("can't be found")
        || r.contains("cant be found");
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
    // "not found" alone (no "or matched"): server has no record. Fine to
    // commit to Cancelled — there's no fill in flight to wait for.
    if not_found {
        return CancelReasonOutcome::Cancelled;
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
    // "already canceled" / unrecognised — conservative.
    CancelReasonOutcome::Cancelled
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
        o.client_order_id, o.side, label, o.price.unwrap_or(0.0), o.quantity, po,
    )
}

// ════════════════════════════════════════════════════════════════
// Shared State (between trade executor and user_feed)
// ════════════════════════════════════════════════════════════════

/// Tracked order for state reconciliation.
#[derive(Debug, Clone)]
pub(crate) struct TrackedOrder {
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

/// Sliding-window rate limiter.
struct RateLimiter {
    max_per_second: u32,
    timestamps: std::collections::VecDeque<Instant>,
}

impl RateLimiter {
    fn new(max_per_second: u32) -> Self {
        Self {
            max_per_second,
            timestamps: std::collections::VecDeque::new(),
        }
    }

    fn check(&mut self) -> bool {
        let now = Instant::now();
        let cutoff = now - Duration::from_secs(1);
        while self.timestamps.front().map(|t| *t < cutoff).unwrap_or(false) {
            self.timestamps.pop_front();
        }
        if (self.timestamps.len() as u32) < self.max_per_second {
            self.timestamps.push_back(now);
            true
        } else {
            false
        }
    }
}

// ─── Async HTTP dispatch ────────────────────────────────────────────
// All Polymarket REST calls run on the shared tokio runtime
// (`async_rt::handle()`) via the shared `reqwest::Client`
// (HTTP/2, keepalive, multiplexed). No dedicated worker threads — tokio
// schedules the futures on its current_thread runtime. Parallel cancel+
// place is realised by kicking off two `spawn` calls without waiting.

type HttpReply = std::result::Result<serde_json::Value, HttpErr>;

/// Async heartbeat loop. Spawned as a tokio task on the shared runtime
/// at startup. Each tick (every `HEARTBEAT_INTERVAL`) fires **one**
/// `POST /heartbeats` per primary HTTP/2 client across **every** role
/// pool (FAST + CANCEL + RECONCILE + QUERY) so each underlying TCP
/// connection sees traffic before reqwest's `pool_idle_timeout` evicts
/// it. Without this, only the QUERY pool stays warm and the next
/// place / cancel after a quiet stretch pays a TLS+h2 handshake.
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
    let n_clients = async_rt::http_clients_all().len();
    info!(
        "[PolyHeartbeat] Started (interval={}s, transport=h1.1, fan_out={} clients)",
        HEARTBEAT_INTERVAL.as_secs(), n_clients,
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

        // Fan out one ping per client across ALL pools concurrently.
        // Each client has its own TCP connection; using `client.request`
        // directly bypasses `pick_client`'s path-based routing so we
        // can target a specific connection.
        let mut set = tokio::task::JoinSet::new();
        for client in async_rt::http_clients_all() {
            let url_c = url.clone();
            let headers_c = headers.clone();
            set.spawn(async move {
                let mut req = client.request(reqwest::Method::POST, &url_c)
                    .header("Content-Type", "application/json")
                    .body(String::new());
                for (k, v) in headers_c.as_pairs() {
                    req = req.header(k, v);
                }
                req.send().await.map(|r| r.status().as_u16())
            });
        }

        let mut ok_n = 0usize;
        let mut err_n = 0usize;
        let mut first_err: Option<String> = None;
        while let Some(res) = set.join_next().await {
            match res {
                Ok(Ok(status)) if (200..400).contains(&status) => ok_n += 1,
                Ok(Ok(status)) => {
                    err_n += 1;
                    if first_err.is_none() {
                        first_err = Some(format!("HTTP {}", status));
                    }
                }
                Ok(Err(e)) => {
                    err_n += 1;
                    if first_err.is_none() { first_err = Some(e.to_string()); }
                }
                Err(_) => {
                    err_n += 1;
                    if first_err.is_none() { first_err = Some("task cancelled".into()); }
                }
            }
        }

        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        if err_n == 0 {
            log::trace!("[PolyHeartbeat] ok ({} pings, {:.0}ms)", ok_n, elapsed_ms);
            tick_ok += 1;
        } else {
            warn!(
                "[PolyHeartbeat] {}/{} pings failed ({:.0}ms): first_err={}",
                err_n, ok_n + err_n, elapsed_ms,
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

/// Classify a (method, path) as a place / cancel request for the
/// per-request latency CSV (`latency_record`). Returns `None` for
/// everything else (heartbeat, reconcile GET, …) so the record stays
/// focused on order placement + cancellation latency.
fn latency_record_kind(method: &str, path: &str) -> Option<&'static str> {
    match (method, path) {
        ("POST", "/order") | ("POST", "/orders") => Some("place"),
        ("DELETE", "/order") | ("DELETE", "/orders") | ("DELETE", "/cancel-all") => Some("cancel"),
        _ => None,
    }
}

/// Map an HTTP reply to the `status` column of the latency CSV.
fn latency_record_status(reply: &HttpReply) -> String {
    match reply {
        Ok(_) => "ok".to_string(),
        Err(HttpErr::Timeout) => "timeout".to_string(),
        Err(HttpErr::Status(code, _)) => format!("http_{}", code),
        Err(HttpErr::Other(_)) => "error".to_string(),
    }
}

/// Pick the per-role HTTP client for a (method, path) pair. Role isolation
/// ensures a slow query / heartbeat can't back-pressure the hot-path
/// submit via shared h2 stream credits or TCP receive windows — each role
/// owns a distinct TCP connection per host.
///
/// Routing table (all relative to CLOB_BASE_URL):
///   * POST /order, POST /orders        → fast (500 ms)
///   * DELETE /order, /orders, /cancel-all, DELETE *  → cancel (500 ms)
///   * GET /data/order/{id}              → reconcile (1000 ms)
///   * everything else (heartbeats, /trades gap-fill, generic GET / POST)
///                                      → query (5000 ms)
fn pick_client(method: &reqwest::Method, path: &str) -> std::sync::Arc<reqwest::Client> {
    match (method.as_str(), path) {
        ("POST", "/order") | ("POST", "/orders") => async_rt::http_client_fast(),
        ("DELETE", _) => async_rt::http_client_cancel(),
        ("GET", p) if p.starts_with("/data/order/") => async_rt::http_client_reconcile(),
        _ => async_rt::http_client_query(),
    }
}

/// Per-request timeout for `(method, path)`. Returns `Some(d)` for the
/// FAST + CANCEL paths so the session-of-day timeout takes effect; `None`
/// for paths that should keep their client-level timeout (reconcile uses
/// its dedicated 2 s pool, query uses 5 s, neither benefits from
/// session-aware tuning since the upstream stalls hitting them are rare
/// and already absorbed by their longer ceiling).
fn per_request_timeout(method: &reqwest::Method, path: &str) -> Option<std::time::Duration> {
    match (method.as_str(), path) {
        ("POST", "/order") | ("POST", "/orders") => Some(async_rt::current_fast_timeout()),
        ("DELETE", _) => Some(async_rt::current_cancel_timeout()),
        _ => None,
    }
}

/// Hedge delay (ms) for cancel paths. p50 cancel RTT in 2026-04-27
/// live = 31 ms, p95 = 243 ms — at 120 ms we cover the long tail
/// (~15-20% of cancels) while only doubling traffic on slow cases.
/// The hedge skips its own send if the primary already won by then
/// (channel-full check), so on the healthy path it costs only one
/// wakeup of the tokio sleep.
pub(crate) const HEDGE_DELAY_MS_CANCEL: u64 = 120;

/// Hedge delay (ms) for place paths. p50 place RTT in 2026-05-04 live
/// = 29 ms, p95 = 236 ms; the long tail keeps p95 pinned to the 500 ms
/// FAST_TIMEOUT in slow Polymarket-CLOB windows (the 11h22m run had
/// 23.8% NewOrderTimeout). 250 ms is past p95 but well below the
/// timeout, so the hedge only fires on the genuinely slow ~5% — and
/// since Polymarket dedupes by orderID hash (deterministic over the
/// EIP-712-signed body, which is identical between the two legs), a
/// duplicate that lands second simply gets rejected as already-known
/// while the first leg gives us our ack. Net effect: on the slow tail
/// we trade one extra request for an ack 100-300 ms sooner, and we
/// avoid the orphan-reconcile cycle that timeout would otherwise
/// incur.
pub(crate) const HEDGE_DELAY_MS_PLACE: u64 = 250;

/// If `(method, path)` is a hedge-eligible endpoint, return the delay
/// (ms) the hedge leg should sleep before firing; otherwise `None`.
///
/// Eligible:
///   * `DELETE /order`, `DELETE /orders`     → cancel hedge (120 ms)
///   * `POST /order`                          → place hedge (250 ms)
///
/// Excluded:
///   * `DELETE /cancel-all` — heavier session-shutdown / balance-error
///     path that doesn't benefit from racing.
///   * `POST /orders` (batch place) — duplicating a 5-order batch
///     amplifies traffic 10×; single-order place is the common path
///     and gets the targeted treatment.
fn hedge_delay_ms(method: &reqwest::Method, path: &str) -> Option<u64> {
    if method == reqwest::Method::DELETE && (path == "/order" || path == "/orders") {
        Some(HEDGE_DELAY_MS_CANCEL)
    } else if method == reqwest::Method::POST && path == "/order" {
        Some(HEDGE_DELAY_MS_PLACE)
    } else {
        None
    }
}

/// True if a POST /order reply is a server-side dedup rejection (400
/// "Duplicated") — meaning the OTHER leg of the hedged pair already
/// reached the server and created the order. The dedup-rejection
/// reply must NOT be allowed to win the channel race: doing so makes
/// the strategy mark the order Rejected, unregister the coid mapping,
/// and lose track of the actual fill that the winning leg's accepted
/// order goes on to receive (observed 1,033× / 7h17m on
/// 2026-05-05 — every one of those coids subsequently saw an
/// authoritative WS Trade Matched push for the same orderID).
///
/// Polymarket's body wording is `"order <oid> is invalid. Duplicated."`;
/// match on the substring `"Duplicated"` to catch minor server-side
/// rephrasings.
fn is_dedup_reject_post(method: &reqwest::Method, path: &str, reply: &HttpReply) -> bool {
    if method != reqwest::Method::POST || path != "/order" {
        return false;
    }
    matches!(reply, Err(HttpErr::Status(400, body)) if body.contains("Duplicated"))
}

/// Execute a single authenticated POST/DELETE against the CLOB via the
/// shared reqwest client. Returns the parsed JSON body or a mapped
/// HttpErr (Timeout / Status / Other). This is the single entry point
/// — all sync wrappers in `SharedState` call it.
async fn execute_http(
    method: reqwest::Method,
    url: String,
    path: String,
    headers: super::auth::AuthHeaders,
    body: String,
) -> HttpReply {
    // Per-(method,path) role-isolated client (FAST for places, CANCEL for
    // deletes, …) — see `pick_client`.
    let client = pick_client(&method, &path);
    let req_timeout = per_request_timeout(&method, &path);
    let mut req = client.request(method.clone(), &url)
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
        Err(e) => return Err(map_reqwest_err(e)),
    };
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(HttpErr::Status(status.as_u16(), body));
    }
    match resp.json::<serde_json::Value>().await {
        Ok(v) => Ok(v),
        Err(e) => Err(HttpErr::Other(format!("json parse: {}", e))),
    }
}

/// User-feed gap-replay tuning (sourced from `exchanges[polymarket]`). All
/// values in milliseconds; the rewinds are quantised to whole seconds for the
/// second-granular `/trades?after=` API. `Default` matches the historical
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
        Self { interval_ms: 2000, periodic_rewind_ms: 5000, reconnect_rewind_ms: 5000 }
    }
}

/// Shared state between PolymarketTrade and the user_feed WebSocket thread.
pub struct SharedState {
    /// Strategy instance identifier (the `[poly.<id>]` key). Tags each
    /// row in the per-request latency CSV so a single file can hold
    /// multiple instances. `"cli"` for one-off CLI subcommands.
    pub(crate) instance_id: String,
    /// Local order tracking: client_order_id → TrackedOrder
    pub(crate) open_orders: Mutex<HashMap<String, TrackedOrder>>,
    /// client_order_id → Polymarket orderID (hex hash)
    pub coid_to_oid: Mutex<HashMap<String, String>>,
    /// Polymarket orderID → client_order_id
    pub oid_to_coid: Mutex<HashMap<String, String>>,
    /// client_order_id → token_id (outcome asset). Written alongside the
    /// coid↔oid maps at registration and kept for the SAME lifetime, so the
    /// event-expiry sweep can purge an event's mappings by its outcome
    /// tokens. We deliberately KEEP coid↔oid mappings across order-lifecycle
    /// rejects/cancels (a "post-only crosses book" 400 or a cancel can still
    /// be followed by a real fill — the racy reject/cancel-then-fill case)
    /// so a late fill push still resolves its coid instead of arriving
    /// `<unmapped>` and broadcasting to every instance. This map lets us
    /// reclaim that memory per-event at settlement rather than never.
    pub coid_to_token: Mutex<HashMap<String, String>>,
    /// Deferred map-reclaim queue: `(sweep_ns, tokens)` batches recorded at
    /// each event-expiry sweep. The settling event's FINAL matching fills land
    /// ~1-2 s AFTER the sweep cancel (observed: sweep 20:05:00.127, settlement
    /// fills 20:05:01.5), so reclaiming a token's coid↔oid mapping right at the
    /// sweep loses those fills to `<unmapped>`. Instead each batch waits out
    /// `RECLAIM_GRACE_NS` before its mappings are dropped (drained on a later
    /// sweep). Timestamped per-batch so concurrent / back-to-back sweeps of
    /// different markets never reclaim each other's just-settled tokens early.
    pub pending_reclaim: Mutex<Vec<(u64, Vec<String>)>>,
    /// Authentication for REST requests
    pub auth: PolyAuth,
    /// EIP-712 order signer (v1 — pre-2026-04-28-cutover).
    pub signer: OrderSigner,
    /// EIP-712 order signer (v2 — post-cutover). `Some` iff this
    /// `SharedState` was initialised with `clob_version = "v2"`.
    pub signer_v2: Option<super::signer_v2::OrderSignerV2>,
    /// The address that actually owns the orders we place on-book — used
    /// to match incoming fills (WS maker-leg match + REST
    /// `/trades?maker_address=` gap recovery). For POLY_1271 (v2 deposit
    /// wallet) this is the funder/DW, which `with_funder` wrote into
    /// `signer_v2.maker_address`. `signer.maker_address` is the EOA
    /// (`derive_addresses` fixes POLY_1271 to the EOA), so matching fills
    /// against it silently dropped EVERY maker fill — the ledger never
    /// decremented and the strategy over-quoted SELL against phantom
    /// inventory (CLOB `not enough balance`).
    pub order_maker_address: String,
    /// Which CLOB protocol to use for order placement / signing.
    /// "v1" (default) = current behaviour; "v2" = new 2026-04-28
    /// contract & schema. Every `sign_and_build_body` and auth path
    /// dispatches on this.
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
    /// Live position & balance manager (trade-status-based)
    pub live_position: Mutex<LivePositionManager>,
    /// Taker-matched inventory accelerator: HTTP `POST /order` matched fills
    /// recorded here (writer) so the strategy (reader, wired via
    /// `set_taker_matched` in `build_strategies`) reflects them before the WS
    /// `user_feed` push lands. The user feed vacates each entry on arrival.
    /// See [`TakerMatchedInventory`].
    pub taker_matched: std::sync::Arc<super::live_position::TakerMatchedInventory>,
    /// Narrow user-feed health handle shared with the strategy (pause-quoting
    /// signals). See [`UserFeedHealth`]. The user feed writes; the strategy
    /// reads (wired via `set_user_feed_health` in `build_strategies`).
    pub user_feed_health: std::sync::Arc<super::live_position::UserFeedHealth>,
    /// User-feed gap-replay cadence / rewind tuning (from config).
    pub gap_replay: GapReplayConfig,
    rate_limiter: Mutex<RateLimiter>,
    /// Per-INSTANCE balance-error backoff deadlines (wall-clock ns), keyed
    /// by the placing `instance_id`. A future deadline means that instance's
    /// `submit_order` / `batch_submit_orders` / `batch_update_orders`
    /// pre-reject new placements so we stop hammering the server with doomed
    /// submits while a racing cancel releases the server-side allowance (a
    /// prior cancel timed out → orphan → server still reserves funds).
    /// Per-instance (not account-wide) so one strategy hitting `not enough
    /// balance` never pauses a shared-wallet sibling's submits. Absent or
    /// past = not in backoff.
    pub(crate) balance_backoff_until_ns: Mutex<HashMap<String, u64>>,
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
    pub(crate) invalid_token_backoff: Mutex<HashMap<String, (u32, u64)>>,
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
    /// Wall-clock ns until which `reconcile_orphans` short-circuits (returns
    /// empty updates without hitting HTTP) because the server is in a 425
    /// "service not ready" storm. Set by the cancel-reply / reconcile-DELETE
    /// paths when they hit 425; cleared lazily by the deadline.
    ///
    /// Rationale: HTTP 425 means "service is overloaded, retry later". The
    /// reconciler's 500 ms / 1.5 s loop converts that signal into a flood
    /// (live 2026-05-12 13:14–13:37: 1,975 retry log lines for one coid).
    /// During backoff we keep the orphan parked but skip HTTP roundtrips
    /// — the reconciler will naturally re-try after the deadline expires.
    /// Read/write Relaxed.
    pub(crate) http_425_reconcile_backoff_until_ns: std::sync::atomic::AtomicU64,
    /// Per-coid counter for `reconcile_orphans` placement queries that
    /// returned `not_found` from the server. Real Polymarket is
    /// eventually-consistent across CLOB shards: a brand-new order can
    /// be 404 on the read replica for 100s of ms even though the write
    /// replica has it. Live2 evidence (2026-04-30): 66% of "not_found"
    /// reconciles were actually live orders that later traded. Until
    /// the counter for a coid hits `RECONCILE_NOT_FOUND_RETRY_LIMIT`
    /// the reconcile path returns NO update — strategy keeps the coid
    /// orphaned and the next `Signal::ReconcilePolymarket` will retry.
    /// Cleared on any conclusive resolution (MATCHED / FILLED /
    /// CANCELED) so a subsequent unrelated 404 starts fresh.
    pub(crate) reconcile_not_found_attempts: Mutex<HashMap<String, u32>>,

    /// Coids whose cancel was rejected with a `pending/delayed` reason — the
    /// cancel raced the placement, so the order is still being processed and
    /// will shortly be LIVE (not gone). The reconcile cancel-orphan `""`
    /// (not-found) arm treats these as **Uncertain** and keeps retrying the GET
    /// (bounded) until the order converges (LIVE → re-DELETE / MATCHED → Filled
    /// / CANCELED → Cancelled), instead of committing Cancelled on a
    /// not-yet-indexed order. Inserted at the cancel-reply classification sites;
    /// cleared on conclusive resolution via `remove_order`. (Belt-and-suspenders
    /// with the OrderManager resurrection: this avoids even a transient
    /// Cancelled for pending/delayed; resurrection backstops the bounded-retry
    /// give-up tail.)
    pub(crate) pending_delayed_orphans: Mutex<HashSet<String>>,
}

/// Number of consecutive `not_found` GETs we tolerate from the server
/// before giving up on a placement orphan and committing `Rejected`.
/// Sized for Polymarket's read-replica lag — 5 attempts at ≥1.5 s
/// retry interval (= `IN_FLIGHT_TTL_NS`) gives ~7.5 s for the write to
/// propagate, well past the observed 100-300 ms convergence window.
pub(crate) const RECONCILE_NOT_FOUND_RETRY_LIMIT: u32 = 5;

/// Backoff window applied to `reconcile_orphans` when a 425 "service not
/// ready" is observed. 10 s gives the upstream service a chance to drain
/// without us hammering it; the reconciler resumes automatically on next
/// `Signal::ReconcilePolymarket` after the deadline. Observed 425 storms
/// (2026-05-12 13:14–13:21, ~7 min) recover in single-digit minutes, so
/// 10 s × extending-on-repeat reaches a healthy steady state.
pub(crate) const HTTP_425_BACKOFF_NS: u64 = 10_000_000_000;

/// Grace period a settling event's coid↔oid mappings are kept AFTER its
/// expiry sweep before being reclaimed. The event's final matching fills land
/// ~1-2 s after the sweep cancel; 60 s is a generous margin while still
/// reclaiming within the same event cadence (next same-market sweep is minutes
/// away). See `pending_reclaim` field doc.
pub(crate) const RECLAIM_GRACE_NS: u64 = 60_000_000_000;

/// Record this sweep's `tokens` (stamped `now_ns`) and return the token sets of
/// all batches that have aged past `RECLAIM_GRACE_NS` (removed from `queue`).
/// Pure so it's unit-testable. The returned batches' mappings are then dropped
/// via `reclaim_token_mappings`.
fn drain_matured_reclaims(
    queue: &mut Vec<(u64, Vec<String>)>,
    tokens: &[String],
    now_ns: u64,
) -> Vec<Vec<String>> {
    queue.push((now_ns, tokens.to_vec()));
    let mut due = Vec::new();
    queue.retain(|(ts, toks)| {
        if now_ns.saturating_sub(*ts) >= RECLAIM_GRACE_NS {
            due.push(toks.clone());
            false
        } else {
            true
        }
    });
    due
}

/// Remove every coid↔oid / coid↔token entry whose token is in `settling`,
/// keeping all other events' entries. Returns the count reclaimed. Pure (maps
/// passed in) so it's unit-testable without a live `SharedState`. Callers hold
/// all three map locks for the duration.
fn reclaim_token_mappings(
    coid_to_oid: &mut HashMap<String, String>,
    oid_to_coid: &mut HashMap<String, String>,
    coid_to_token: &mut HashMap<String, String>,
    settling: &[String],
) -> usize {
    let settling: std::collections::HashSet<&str> = settling.iter().map(|s| s.as_str()).collect();
    let stale: Vec<String> = coid_to_token.iter()
        .filter(|(_, tok)| settling.contains(tok.as_str()))
        .map(|(coid, _)| coid.clone())
        .collect();
    for coid in &stale {
        if let Some(oid) = coid_to_oid.remove(coid) { oid_to_coid.remove(&oid); }
        coid_to_token.remove(coid);
    }
    stale.len()
}

impl SharedState {
    /// Register a bidirectional order ID mapping plus the order's outcome
    /// token. The token lets `cancel_market_orders` purge this mapping at the
    /// owning event's expiry sweep (the only place coid↔oid mappings are
    /// reclaimed now that lifecycle rejects/cancels KEEP them — see
    /// `coid_to_token` field doc).
    pub fn register_order_id(&self, client_order_id: &str, exchange_order_id: &str, token: &str) {
        self.coid_to_oid.lock().unwrap()
            .insert(client_order_id.to_string(), exchange_order_id.to_string());
        self.oid_to_coid.lock().unwrap()
            .insert(exchange_order_id.to_string(), client_order_id.to_string());
        if !token.is_empty() {
            self.coid_to_token.lock().unwrap()
                .insert(client_order_id.to_string(), token.to_string());
        }
    }

    /// Look up client_order_id from Polymarket orderID.
    pub fn lookup_coid(&self, exchange_order_id: &str) -> Option<String> {
        self.oid_to_coid.lock().unwrap().get(exchange_order_id).cloned()
    }

    /// Drop the order from the **active-order** tracker.
    ///
    /// Deliberately KEEPS the `coid_to_oid` / `oid_to_coid` / `coid_to_token`
    /// maps intact — a delayed fill push for a just-cancelled OR just-rejected
    /// order (cancel-then-fill / racy "post-only crosses book" reject-then-fill)
    /// can still resolve its coid via `oid_to_coid`, so the fill is attributed
    /// to the placing instance instead of arriving `<unmapped>` (empty coid)
    /// and broadcasting to every instance's PositionManager. The maps are
    /// reclaimed per-event by `cancel_market_orders` at the event-expiry sweep
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
        self.open_orders.lock().unwrap().remove(client_order_id);
        // Conclusive resolution — drop any pending/delayed orphan flag so the
        // set never leaks and a future coid reuse starts fresh.
        self.pending_delayed_orphans.lock().unwrap().remove(client_order_id);
    }

    fn check_rate_limit(&self) -> bool {
        self.rate_limiter.lock().unwrap().check()
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
        let map = self.balance_backoff_until_ns.lock().unwrap();
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
        let mut map = self.balance_backoff_until_ns.lock().unwrap();
        let prev = map.insert(instance_id.to_string(), now + Self::BALANCE_BACKOFF_NS);
        // Edge = no prior deadline, or the prior one already expired.
        prev.map_or(true, |p| p < now)
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
        self.invalid_token_backoff.lock().unwrap()
            .get(token).is_some_and(|(_, until)| *until > now)
    }

    /// Record an `invalid token id` reject for `token`: bump its strike count
    /// and, once it reaches `INVALID_TOKEN_STRIKES`, (re)arm the block window.
    /// Returns `true` on the edge (window armed from a non-active state) so the
    /// caller logs exactly once per window.
    pub(crate) fn record_invalid_token(&self, token: &str) -> bool {
        let now = crate::types::now_ns();
        let mut map = self.invalid_token_backoff.lock().unwrap();
        let e = map.entry(token.to_string()).or_insert((0, 0));
        e.0 = e.0.saturating_add(1);
        if e.0 < Self::INVALID_TOKEN_STRIKES { return false; }
        let was_active = e.1 > now;
        e.1 = now + Self::INVALID_TOKEN_BACKOFF_NS;
        !was_active
    }

    /// Clear a token's invalid-token state — an order for it was accepted, so
    /// it's registered and tradeable again.
    pub(crate) fn clear_invalid_token(&self, token: &str) {
        let mut map = self.invalid_token_backoff.lock().unwrap();
        if map.remove(token).is_some() && map.len() > 256 {
            // Opportunistic prune: drop long-expired entries so a long-lived
            // process doesn't accumulate one row per ever-invalid token.
            let now = crate::types::now_ns();
            map.retain(|_, (_, until)| *until > now);
        }
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
        let until = self.http_425_warn_silent_until_ns
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

    /// Record that an HTTP 425 was observed by the cancel-reply or
    /// reconcile-DELETE path, and push `http_425_reconcile_backoff_until_ns`
    /// to `now + HTTP_425_BACKOFF_NS`. Used to gate `reconcile_orphans` so
    /// the bot doesn't hammer the upstream service while it's still
    /// recovering. Idempotent — only advances the deadline, never shortens.
    pub(crate) fn note_http_425_backoff(&self) {
        let now = crate::types::now_ns();
        let new_deadline = now.saturating_add(HTTP_425_BACKOFF_NS);
        // Use a CAS-like fetch_max to ensure we only advance the deadline
        // (relaxed is fine — eventual consistency on the deadline value
        // is acceptable, the loop will retry naturally).
        let cur = self.http_425_reconcile_backoff_until_ns
            .load(std::sync::atomic::Ordering::Relaxed);
        if new_deadline > cur {
            self.http_425_reconcile_backoff_until_ns
                .store(new_deadline, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// True iff a 425 backoff is currently active (set by
    /// `note_http_425_backoff` and not yet expired). When true,
    /// `reconcile_orphans` short-circuits without making HTTP calls.
    pub(crate) fn in_http_425_backoff(&self) -> bool {
        let until = self.http_425_reconcile_backoff_until_ns
            .load(std::sync::atomic::Ordering::Relaxed);
        if until == 0 {
            return false;
        }
        crate::types::now_ns() < until
    }

    /// Dispatch an HTTP request onto the shared async runtime. Returns a
    /// crossbeam Receiver so the caller can poll for the response from a
    /// sync context. Kicks off the reqwest call immediately (non-blocking
    /// from the caller's perspective) — for parallel cancel+place, two
    /// sequential calls of this method are dispatched concurrently on
    /// the runtime (HTTP/2 multiplexes them on a single TCP connection).
    ///
    /// **Hedged retry**: when `(method, path)` is hedge-eligible, a
    /// second identical request is fired on a different pool client
    /// after a path-specific delay iff the primary hasn't replied yet.
    /// First reply wins via a `bounded(1)` channel; the loser's
    /// `try_send` is silently dropped on full/disconnected.
    ///
    /// Eligible paths and their delays (see `hedge_delay_ms`):
    ///   * `DELETE /order`, `DELETE /orders` → 120 ms (cancel)
    ///   * `POST /order`                      → 250 ms (place)
    ///
    /// Idempotency:
    ///   * DELETE: already-cancelled orders return inside `not_canceled`
    ///     with a benign reason — no double-cancel risk.
    ///   * POST: the body is byte-identical between legs and contains
    ///     the EIP-712-signed order params, so both legs hash to the
    ///     same orderID; Polymarket dedupes server-side, the second
    ///     leg lands as already-known. No double-fill risk.
    /// `/cancel-all` is intentionally **not** hedged — it's a
    /// session-shutdown / balance-error escape hatch and the heavier
    /// payload doesn't benefit from racing.
    pub(crate) fn http_call_async(
        &self,
        method: &str,
        path: &str,
        body: &str,
    ) -> crossbeam_channel::Receiver<HttpReply> {
        let method = match method {
            "POST" => reqwest::Method::POST,
            "DELETE" => reqwest::Method::DELETE,
            "GET" => reqwest::Method::GET,
            other => {
                let (tx, rx) = crossbeam_channel::bounded(1);
                let _ = tx.send(Err(HttpErr::Other(format!("unsupported method: {}", other))));
                return rx;
            }
        };
        // Pick a stable stage name by (method, path prefix) for the
        // latency histogram. Falls back to a generic bucket for paths
        // we haven't categorised.
        let stage = http_stage(method.as_str(), path);
        // Per-request latency CSV: classify place / cancel once up-front
        // (None ⇒ not recorded). Both legs tag the winning reply with
        // this instance's id when recording is active.
        let rec_kind = latency_record_kind(method.as_str(), path);
        let t_start = crate::latency::Instant::now();
        let url = format!("{}{}", self.clob_base_url, path);
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);

        // Primary leg: sign + spawn now.
        {
            let headers = self.auth.sign_request(method.as_str(), path, body);
            let path_owned = path.to_string();
            let body_owned = body.to_string();
            let url_owned = url.clone();
            let method_owned = method.clone();
            let tx_a = reply_tx.clone();
            let iid_a = self.instance_id.clone();
            async_rt::handle().spawn(async move {
                let reply = execute_http(method_owned.clone(), url_owned, path_owned.clone(), headers, body_owned).await;
                // POST /order dedup guard: if THIS leg lost the race to
                // server (the hedge created the order first), the server
                // returned 400 "Duplicated" — never let that reply win
                // the channel, otherwise the strategy marks the order
                // Rejected and forgets about the actual fill the winning
                // leg's accepted order will receive. Drop the reply
                // silently and let the OTHER leg's reply (or
                // FAST_TIMEOUT → orphan reconciler) be the truth.
                if is_dedup_reject_post(&method_owned, &path_owned, &reply) {
                    log::debug!("[PolymarketTrade] Primary discarded Duplicated 400 (hedge won race)");
                    return;
                }
                // try_send: succeeds iff this leg won the race.
                // Full (hedge already sent) or Disconnected (caller dropped rx)
                // are both benign — drop silently. Latency only recorded for
                // the winner so the histogram reflects actual user-observed
                // wall-clock RTT (from `t_start`, the moment the caller
                // dispatched the request), not wasted hedge work.
                // Capture the CSV status before `reply` is moved into the
                // channel (only when we'll actually record).
                let rec = rec_kind
                    .filter(|_| crate::latency_record::is_active())
                    .map(|k| (k, latency_record_status(&reply)));
                if tx_a.try_send(reply).is_ok() {
                    crate::latency::record(stage, t_start);
                    if let Some((k, status)) = rec {
                        crate::latency_record::record(
                            &iid_a, k, t_start.elapsed().as_secs_f64() * 1000.0, status,
                        );
                    }
                }
            });
        }

        // Hedge leg: spawn iff this is a hedge-eligible endpoint. Sleeps
        // the path-specific delay (cancel: 120 ms; place: 250 ms) first;
        // if the primary already won (channel full) its `try_send` will
        // fail and we skip the actual HTTP call, saving one round-trip's
        // worth of pool traffic.
        //
        // Place hedging is safe because Polymarket dedupes orders by
        // EIP-712 orderID hash — the same `body` string contains the
        // signed order params, so both legs hash to the same orderID,
        // and the second arrival is rejected as already-known. Auth
        // headers are re-signed (timestamp-bound) but the body itself
        // is byte-identical between legs.
        if let Some(delay_ms) = hedge_delay_ms(&method, path) {
            let auth = self.auth.clone();
            let method_b = method;
            let path_b = path.to_string();
            let body_b = body.to_string();
            let url_b = url;
            let tx_b = reply_tx;
            let iid_b = self.instance_id.clone();
            async_rt::handle().spawn(async move {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                // Channel full ⇒ primary already replied. Skip the hedge —
                // no point spending a request slot on a doomed leg.
                if tx_b.is_full() { return; }
                // Re-sign: Polymarket auth signature is timestamp-bound, so
                // the hedge needs its own headers (re-using primary's headers
                // would risk the server rejecting on stale timestamp).
                let headers_b = auth.sign_request(method_b.as_str(), &path_b, &body_b);
                let reply = execute_http(method_b.clone(), url_b, path_b.clone(), headers_b, body_b).await;
                // POST /order dedup guard (mirror of the primary leg's
                // check): the hedge can't be allowed to win with a 400
                // Duplicated, otherwise the strategy mis-classifies an
                // order the primary actually accepted as Rejected. See
                // `is_dedup_reject_post`.
                if is_dedup_reject_post(&method_b, &path_b, &reply) {
                    log::debug!("[PolymarketTrade] Hedge discarded Duplicated 400 (primary won race) path={}", path_b);
                    return;
                }
                let rec = rec_kind
                    .filter(|_| crate::latency_record::is_active())
                    .map(|k| (k, latency_record_status(&reply)));
                if tx_b.try_send(reply).is_ok() {
                    // Record from `t_start` (caller dispatch time), NOT from
                    // post-sleep `Instant::now()`. The strategy's wall-clock
                    // RTT for a hedge-won request is `delay_ms + hedge_http_rtt`
                    // — recording only the post-sleep portion would understate
                    // p95/p99 in the histogram and bias the BT calibrator
                    // (`sim_latency_calibrate_from`) low by ~delay_ms on the
                    // hedge-won fraction (~5% of slow tail).
                    crate::latency::record(stage, t_start);
                    if let Some((k, status)) = rec {
                        crate::latency_record::record(
                            &iid_b, k, t_start.elapsed().as_secs_f64() * 1000.0, status,
                        );
                    }
                    log::debug!(
                        "[PolymarketTrade] Hedged {} won path={} delay_ms={}",
                        method_b, path_b, delay_ms,
                    );
                }
            });
        }

        reply_rx
    }

    /// Synchronous variant — dispatches and blocks on the reply. Used by
    /// single-op paths (POST /order, DELETE /order). Blocks the calling
    /// thread on a crossbeam recv; the actual I/O work happens on the
    /// tokio runtime thread.
    pub(crate) fn http_call_sync(
        &self,
        method: &str,
        path: &str,
        body: &str,
    ) -> HttpReply {
        self.http_call_async(method, path, body)
            .recv()
            .unwrap_or_else(|_| Err(HttpErr::Other("async reply dropped".to_string())))
    }
}

// ════════════════════════════════════════════════════════════════
// PolymarketTrade
// ════════════════════════════════════════════════════════════════

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
            api_key, api_secret, passphrase, private_key,
            neg_risk, rate_limit_per_second, sig_type,
            ClobVersion::V1,
            "",
            "",
            true,
            "cli",
            "",
            GapReplayConfig::default(),
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
    ) -> Result<Self> {
        let signer = OrderSigner::new(private_key, neg_risk, sig_type)?;
        // Build v2 signer eagerly iff v2 mode — it's tiny (a few keys +
        // strings) and keeps the sign-hot-path branch a simple Option
        // check rather than constructing per-call. For POLY_1271 the
        // deposit-wallet `funder` is the order maker/signer.
        let signer_v2 = if clob_version == ClobVersion::V2 {
            Some(super::signer_v2::OrderSignerV2::new(
                private_key, neg_risk, sig_type, builder_code,
            )?.with_funder(funder))
        } else { None };

        // POLY_ADDRESS must be the signer (EOA) address, matching the API key
        let auth = PolyAuth::new(api_key, api_secret, passphrase, &signer.signer_address)?;

        let clob_base_url = if api_url_prefix.trim().is_empty() {
            DEFAULT_CLOB_BASE_URL.to_string()
        } else {
            api_url_prefix.trim_end_matches('/').to_string()
        };

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
                    info!("[PolymarketTrade] Signer POL balance OK: {:.6} POL on {}",
                        pol, signer.signer_address);
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

        Ok(Self {
            shared: Arc::new(SharedState {
                instance_id: instance_id.to_string(),
                open_orders: Mutex::new(HashMap::new()),
                coid_to_oid: Mutex::new(HashMap::new()),
                oid_to_coid: Mutex::new(HashMap::new()),
                coid_to_token: Mutex::new(HashMap::new()),
                pending_reclaim: Mutex::new(Vec::new()),
                auth,
                signer,
                signer_v2,
                order_maker_address,
                clob_version,
                use_batch_orders,
                clob_base_url,
                live_position: Mutex::new(LivePositionManager::new()),
                taker_matched: std::sync::Arc::new(super::live_position::TakerMatchedInventory::new()),
                user_feed_health: std::sync::Arc::new(super::live_position::UserFeedHealth::new()),
                gap_replay,
                rate_limiter: Mutex::new(RateLimiter::new(rate_limit_per_second.max(1))),
                balance_backoff_until_ns: Mutex::new(HashMap::new()),
                invalid_token_backoff: Mutex::new(HashMap::new()),
                http_425_warn_silent_until_ns: std::sync::atomic::AtomicU64::new(0),
                http_425_reconcile_backoff_until_ns: std::sync::atomic::AtomicU64::new(0),
                reconcile_not_found_attempts: Mutex::new(HashMap::new()),
                pending_delayed_orphans: Mutex::new(HashSet::new()),
            }),
            owner: api_key.to_string(),
            // CLI / first-build route: per-instance trading routes are
            // rebuilt via `from_shared(.., instance_id)`.
            instance_id: String::new(),
            gen_ns_hint: 0,
        })
    }

    /// Spawn a heartbeat thread that, every `HEARTBEAT_INTERVAL`,
    /// fires one `POST /heartbeats` per primary HTTP/2 client across
    /// every role pool (FAST + CANCEL + RECONCILE + QUERY). Two jobs
    /// in one:
    ///   1. Keeps the CLOB session active so the server doesn't
    ///      auto-cancel resting orders.
    ///   2. Touches every TCP connection in every pool so reqwest's
    ///      `pool_idle_timeout` doesn't evict a client that hasn't
    ///      seen business traffic recently — preventing the next real
    ///      place/cancel from paying a fresh TLS+h2 handshake.
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
        let task_handle = async_rt::handle()
            .spawn(heartbeat_loop(auth, shutdown, base_url).instrument(hb_span));
        // Return a std JoinHandle so existing engine shutdown code can
        // .join() it. The handle's thread awaits the tokio task to
        // finish via block_on_runtime — no polling loop.
        std::thread::Builder::new()
            .name("poly-heartbeat-join".into())
            .spawn(move || {
                crate::os_tune::pin_background("poly-heartbeat-join");
                async_rt::block_on_runtime(async move { let _ = task_handle.await; });
            })
            .expect("Failed to spawn heartbeat thread")
    }

    /// Get a clone of the shared state (for user_feed thread).
    pub fn shared_state(&self) -> Arc<SharedState> {
        self.shared.clone()
    }

    /// Pre-warm `n` HTTP connections to `clob.polymarket.com` by firing that
    /// many concurrent `POST /heartbeats` requests. Each establishes a TLS
    /// socket that returns to the shared reqwest pool on response,
    /// so subsequent real orders / cancels don't pay the 100-200ms TLS
    /// handshake cost on the first few batches after startup.
    ///
    /// Uses its own transient spawned threads (not the 2-worker HTTP pool)
    /// because the pool can only run 2 requests concurrently; to fill a
    /// pool of size N we need N concurrent in-flight requests.
    pub fn prewarm_connections(&self) {
        // HTTP/2 multiplexes many in-flight requests over a single TCP
        // connection — "prewarming a pool" is no longer meaningful under
        // the reqwest h2 client. Instead we concurrently warm the three
        // Polymarket hosts we hit on the hot path (clob / data-api /
        // gamma-api) plus the Polygon RPC, so TLS + h2 handshakes complete
        // before the first real request.
        let start = std::time::Instant::now();
        info!("[PolymarketTrade] Pre-warming h1.1 connections (clob/data-api/gamma-api + polygon-rpc)...");

        let auth = self.shared.auth.clone();
        let heartbeat_headers = auth.sign_request("POST", "/heartbeats", "");
        let clob_url = format!("{}{}", self.shared.clob_base_url, "/heartbeats");

        // Warm every primary client against every Polymarket host in
        // parallel. Each primary client owns its own connection pool,
        // so a single GET / POST per (client, host) pair is enough to
        // stand up the TLS + h2 session that subsequent real requests
        // will reuse. Polygon RPC uses the auto (ALPN) client — one
        // warmup is sufficient since it's a single client.
        let primaries = crate::async_rt::http_clients_all().to_vec();
        let n_primaries = primaries.len();
        async_rt::block_on_runtime(async move {
            // Warm the dedicated Polygon RPC client (the one on-chain reads
            // actually use — formerly this warmed the retired ALPN client).
            let rpc = super::onchain_tx::rpc_http_client().clone();
            let h = heartbeat_headers;

            let mut tasks: Vec<tokio::task::JoinHandle<(std::time::Duration, String, std::result::Result<u16, String>)>> =
                Vec::with_capacity(n_primaries * 3 + 1);

            for (idx, primary) in primaries.into_iter().enumerate() {
                // CLOB: signed heartbeat (keeps API key active + warms h2).
                let clob_url = clob_url.clone();
                let h = h.clone();
                let p = primary.clone();
                tasks.push(tokio::spawn(async move {
                    let t0 = std::time::Instant::now();
                    let req = p.request(reqwest::Method::POST, &clob_url)
                        .header("Content-Type", "application/json")
                        .header("POLY_API_KEY", &h.api_key)
                        .header("POLY_ADDRESS", &h.address)
                        .header("POLY_SIGNATURE", &h.signature)
                        .header("POLY_TIMESTAMP", &h.timestamp)
                        .header("POLY_PASSPHRASE", &h.passphrase);
                    let r = req.send().await;
                    (
                        t0.elapsed(),
                        format!("clob[{}]", idx),
                        r.map(|x| x.status().as_u16()).map_err(|e| e.to_string()),
                    )
                }));

                // data-api: cheap unauth GET.
                let p = primary.clone();
                tasks.push(tokio::spawn(async move {
                    let t0 = std::time::Instant::now();
                    let r = p.get("https://data-api.polymarket.com/").send().await;
                    (
                        t0.elapsed(),
                        format!("data-api[{}]", idx),
                        r.map(|x| x.status().as_u16()).map_err(|e| e.to_string()),
                    )
                }));

                // gamma-api: cheap unauth GET.
                let p = primary.clone();
                tasks.push(tokio::spawn(async move {
                    let t0 = std::time::Instant::now();
                    let r = p.get("https://gamma-api.polymarket.com/").send().await;
                    (
                        t0.elapsed(),
                        format!("gamma-api[{}]", idx),
                        r.map(|x| x.status().as_u16()).map_err(|e| e.to_string()),
                    )
                }));
            }

            // Polygon RPC: tiny eth_chainId call via the dedicated RPC client.
            tasks.push(tokio::spawn(async move {
                let t0 = std::time::Instant::now();
                let body = r#"{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}"#;
                let url = std::env::var("POLYGON_RPC")
                    .ok()
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| "https://polygon-rpc.com".to_string());
                let r = rpc.post(&url)
                    .header("Content-Type", "application/json")
                    .body(body)
                    .send().await;
                (
                    t0.elapsed(),
                    "polygon-rpc".to_string(),
                    r.map(|x| x.status().as_u16()).map_err(|e| e.to_string()),
                )
            }));

            for j in tasks {
                if let Ok((dur, host, res)) = j.await {
                    match res {
                        Ok(status) => info!(
                            "[PolymarketTrade] Pre-warm {} → {} ({}ms)",
                            host, status, dur.as_millis(),
                        ),
                        Err(e) => warn!(
                            "[PolymarketTrade] Pre-warm {} failed: {} ({}ms)",
                            host, e, dur.as_millis(),
                        ),
                    }
                }
            }
        });
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

    /// POST variant that distinguishes timeout / status / other errors so
    /// callers can return `OrderStatus::NewOrderTimeout` vs `Rejected`
    /// appropriately. Dispatched through the shared HTTP worker pool.
    fn post_detailed(&self, path: &str, body: &serde_json::Value) -> std::result::Result<serde_json::Value, HttpErr> {
        let body_str = body.to_string();
        self.shared.http_call_sync("POST", path, &body_str)
    }

    /// Cancel ALL open orders on the CLOB (DELETE /cancel-all, no body).
    pub fn cancel_all_orders(&self) {
        let res = self.shared.http_call_sync("DELETE", "/cancel-all", "");
        match res {
            Ok(json) => {
                let canceled = json.get("canceled").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0);
                let not_canceled = json.get("not_canceled").and_then(|v| v.as_object()).map(|o| o.len()).unwrap_or(0);
                info!("[PolymarketTrade] Cancel-all: {} canceled, {} failed", canceled, not_canceled);
            }
            Err(e) => warn!("[PolymarketTrade] Cancel-all failed: {}", e),
        }
        // Clear local tracking (account-wide / shutdown → wipe everything).
        self.shared.open_orders.lock().unwrap().clear();
        self.shared.coid_to_oid.lock().unwrap().clear();
        self.shared.oid_to_coid.lock().unwrap().clear();
        self.shared.coid_to_token.lock().unwrap().clear();
        self.shared.pending_reclaim.lock().unwrap().clear();
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
    pub fn cancel_market_orders(&self, market_condition_id: &str, asset_ids: &[String]) {
        for asset_id in asset_ids {
            if asset_id.is_empty() { continue; }
            let body = serde_json::json!({
                "market": market_condition_id,
                "asset_id": asset_id,
            }).to_string();
            match self.shared.http_call_sync("DELETE", "/cancel-market-orders", &body) {
                Ok(json) => {
                    let canceled = json.get("canceled").and_then(|v| v.as_array()).cloned().unwrap_or_default();
                    let not_canceled = json.get("not_canceled").and_then(|v| v.as_object()).map(|o| o.len()).unwrap_or(0);
                    info!("[PolymarketTrade] Cancel-market market={} asset={}: {} canceled, {} failed",
                        market_condition_id, asset_id, canceled.len(), not_canceled);
                    // Drop local tracking for any canceled order we still
                    // tracked (forgotten orders have no local entry → no-op).
                    if !canceled.is_empty() {
                        let coids: Vec<String> = {
                            let oid_to_coid = self.shared.oid_to_coid.lock().unwrap();
                            canceled.iter()
                                .filter_map(|v| v.as_str())
                                .filter_map(|oid| oid_to_coid.get(oid).cloned())
                                .collect()
                        };
                        for coid in coids {
                            self.shared.remove_order(&coid);
                        }
                    }
                }
                Err(e) => warn!("[PolymarketTrade] Cancel-market market={} asset={} failed: {}",
                    market_condition_id, asset_id, e),
            }
        }

        // Event-expiry reclaim, DEFERRED: enqueue this sweep's settling tokens
        // and reclaim only batches that have aged past `RECLAIM_GRACE_NS`. The
        // settling event's final matching fills land ~1-2 s after this sweep
        // cancel — reclaiming its coid↔oid mappings now would lose them to
        // `<unmapped>`. We KEEP mappings across rejects/cancels (racy
        // crosses-book-reject / cancel-then-fill — see `coid_to_token` field
        // doc); the grace window is the safe point to free the per-event memory.
        let due = {
            let mut q = self.shared.pending_reclaim.lock().unwrap();
            drain_matured_reclaims(&mut q, asset_ids, crate::types::now_ns())
        };
        if !due.is_empty() {
            // Holding all three map locks at once is deadlock-free: no other
            // path in this module ever holds more than one simultaneously.
            let mut coid_to_oid = self.shared.coid_to_oid.lock().unwrap();
            let mut oid_to_coid = self.shared.oid_to_coid.lock().unwrap();
            let mut coid_to_token = self.shared.coid_to_token.lock().unwrap();
            let reclaimed: usize = due.iter()
                .map(|toks| reclaim_token_mappings(&mut coid_to_oid, &mut oid_to_coid, &mut coid_to_token, toks))
                .sum();
            if reclaimed > 0 {
                info!("[PolymarketTrade] Cancel-market market={}: reclaimed {} coid↔oid mapping(s) from {} matured batch(es)",
                    market_condition_id, reclaimed, due.len());
            }
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
            let open = self.shared.open_orders.lock().unwrap();
            let coid_to_oid = self.shared.coid_to_oid.lock().unwrap();
            let mut targets = Vec::with_capacity(open.len());
            for (c, t) in open.iter() {
                // Scope to THIS instance's own orders only — a shared-wallet
                // sibling's resting orders live in the same `open_orders` map
                // but must never be cancelled by our balance-error sweep.
                if t.instance_id != self.instance_id { continue; }
                let in_scope = match side {
                    Side::Buy  => t.side == Side::Buy,
                    Side::Sell => t.side == Side::Sell && t.symbol == symbol,
                };
                if !in_scope { continue; }
                if let Some(oid) = coid_to_oid.get(c) {
                    targets.push((c.clone(), oid.clone()));
                }
            }
            let lbl = match side {
                Side::Buy  => "all-BUYs (USDC pool)",
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
            targets.iter()
                .map(|(_, oid)| serde_json::Value::String(oid.clone()))
                .collect(),
        ).to_string();
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
            let sym = if symbol.len() > 16 { &symbol[..16] } else { symbol };
            warn!(
                "[PolymarketTrade] invalid token id {}... ×{} → {}ms submit backoff for this token (CLOB book not live for this event)",
                sym, SharedState::INVALID_TOKEN_STRIKES, backoff_ms,
            );
        }
    }

    /// DELETE variant, routed through the async reqwest client.
    fn delete_detailed(&self, path: &str, body: &serde_json::Value) -> std::result::Result<serde_json::Value, HttpErr> {
        let body_str = body.to_string();
        self.shared.http_call_sync("DELETE", path, &body_str)
    }

    /// GET from a CLOB endpoint with authentication (used by reconcile path).
    #[allow(dead_code)]
    fn get(&self, path: &str) -> Result<serde_json::Value> {
        self.shared.http_call_sync("GET", path, "")
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
    ) -> Result<(String /* order_hash */, serde_json::Value)> {
        let price = order.price.unwrap_or(0.0);
        if price <= 0.0 || price >= 1.0 {
            return Err(anyhow!("Invalid price: {}", price));
        }

        match self.shared.clob_version {
            ClobVersion::V1 => self.sign_and_build_body_v1(order, price),
            ClobVersion::V2 => self.sign_and_build_body_v2(order, price),
        }
    }

    /// Translate `OrderRequest::order_type` to Polymarket's wire string.
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

    fn sign_and_build_body_v1(
        &self,
        order: &OrderRequest,
        price: f64,
    ) -> Result<(String, serde_json::Value)> {
        let signed = self.shared.signer.build_signed_order(
            &order.symbol,
            price,
            order.quantity,
            order.side,
            order.fee_rate_bps,
        )?;

        let salt_u64: u64 = signed.order.salt.parse::<u128>()
            .map(|v| v as u64).unwrap_or(0);
        let salt_num = serde_json::json!(salt_u64);

        let body = serde_json::json!({
            "owner": self.owner,
            "orderType": Self::poly_order_type_str(order.order_type),
            "postOnly": order.post_only,
            "order": {
                "salt": salt_num,
                "maker": signed.order.maker,
                "signer": signed.order.signer,
                "taker": signed.order.taker,
                "tokenId": signed.order.token_id,
                "makerAmount": signed.order.maker_amount,
                "takerAmount": signed.order.taker_amount,
                "expiration": signed.order.expiration,
                "nonce": signed.order.nonce,
                "feeRateBps": signed.order.fee_rate_bps,
                "side": if order.side == Side::Buy { "BUY" } else { "SELL" },
                "signature": signed.signature,
                "signatureType": signed.order.signature_type,
            }
        });
        Ok((signed.order_hash, body))
    }

    fn sign_and_build_body_v2(
        &self,
        order: &OrderRequest,
        price: f64,
    ) -> Result<(String, serde_json::Value)> {
        let signer_v2 = self.shared.signer_v2.as_ref()
            .ok_or_else(|| anyhow!("clob_version=v2 but signer_v2 is None — constructor bug"))?;
        let signed = signer_v2.build_signed_order_dispatch(
            &order.symbol, price, order.quantity, order.side,
        )?;

        let salt_u64: u64 = signed.order.salt.parse::<u128>()
            .map(|v| v as u64).unwrap_or(0);
        let salt_num = serde_json::json!(salt_u64);

        // v2 wire body — field set + order matches `orderToJsonV2` in
        // clob-client-v2/src/types/ordersV2.ts exactly. No `nonce`, no
        // `feeRateBps` (both removed in v2). `taker` and `expiration`
        // are wire-only (NOT in the signed struct).
        let body = serde_json::json!({
            "owner": self.owner,
            "orderType": Self::poly_order_type_str(order.order_type),
            "postOnly": order.post_only,
            "deferExec": false,
            "order": {
                "salt": salt_num,
                "maker": signed.order.maker,
                "signer": signed.order.signer,
                "taker": signed.order.taker,
                "tokenId": signed.order.token_id,
                "makerAmount": signed.order.maker_amount,
                "takerAmount": signed.order.taker_amount,
                "side": if order.side == Side::Buy { "BUY" } else { "SELL" },
                "signatureType": signed.order.signature_type,
                "timestamp": signed.order.timestamp,
                "expiration": signed.order.expiration,
                "metadata": signed.order.metadata,
                "builder": signed.order.builder,
                "signature": signed.signature,
            }
        });
        Ok((signed.order_hash, body))
    }

    /// Normalise an `orderID` for comparison — Polymarket's API returns
    /// the hex in mixed case (no checksum); we lowercase both sides.
    fn oid_eq(a: &str, b: &str) -> bool {
        a.trim_start_matches("0x").eq_ignore_ascii_case(b.trim_start_matches("0x"))
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
            trade_id: None,
            error: if msg.is_empty() { None } else { Some(msg.to_string()) },
        }
    }

    /// Make a timeout OrderUpdate for a placement whose HTTP call timed
    /// out. The server MAY have accepted the order; strategy should
    /// reconcile — but because we pre-compute and pass along `order_hash`
    /// (the Polymarket `orderID`), reconciliation can query / cancel by
    /// orderID directly via `GET /data/order/{orderID}` or
    /// `DELETE /order/{orderID}` without any salt/price matching.
    fn make_timeout_place(order: &OrderRequest, order_hash: Option<&str>) -> OrderUpdate {
        OrderUpdate {
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
            trade_id: None,
            error: None,
        }
    }

    /// Make a timeout OrderUpdate for a cancel whose HTTP call timed out.
    fn make_timeout_cancel(coid: &str, symbol: &str, side: Side, order_id: Option<String>) -> OrderUpdate {
        OrderUpdate {
            client_order_id: coid.to_string(),
            exchange: Exchange::Polymarket,
            symbol: symbol.to_string(),
            side,
            exchange_order_id: order_id,
            status: OrderStatus::CancelOrderTimeout,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: 0.0,
            avg_fill_price: 0.0,
            timestamp_ns: now_ns(),
            trade_id: None,
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
    ) -> Vec<OrderUpdate> {
        // HTTP 425 backoff gate: if the upstream service signalled "not
        // ready" recently, short-circuit before making more HTTP roundtrips.
        // Orphans stay parked; the orphan_reconciler will fire another
        // Signal::ReconcilePolymarket after its throttle and we'll re-enter
        // here. Once the deadline expires `in_http_425_backoff` returns
        // false and reconciliation resumes naturally.
        //
        // Without this gate a 425 storm gets amplified by the reconciler:
        // live 2026-05-12 13:14–13:37, one coid logged 1,975 retry lines
        // over 23 min because each retry hit 425 → kept-as-orphan →
        // re-emit 500 ms later → repeat.
        if self.shared.in_http_425_backoff() {
            log::debug!(
                "[PolymarketTrade] Reconcile skipped: HTTP 425 backoff active ({} places, {} cancels parked)",
                pending_places.len(), pending_cancels.len(),
            );
            return Vec::new();
        }

        let mut updates: Vec<OrderUpdate> = Vec::new();

        // --- Placements: deterministic per-orderID lookup ---
        if !pending_places.is_empty() {
            info!(
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
                let fetch_result = self.fetch_order_status_by_id(oid);
                // 425 mid-iteration: defer this and remaining orphans rather
                // than commit a false outcome via the `""` (not-found) arm
                // below. Next reconcile call short-circuits on backoff.
                if fetch_result.is_none() && self.shared.in_http_425_backoff() {
                    log::debug!(
                        "[PolymarketTrade] Reconcile placement coid={} orderID={}: fetch deferred (HTTP 425 backoff); keeping orphan",
                        coid, oid,
                    );
                    continue;
                }
                let status_str = fetch_result.unwrap_or_default();
                match status_str.as_str() {
                    "LIVE" => {
                        // Conclusive answer — clear the not_found counter
                        // so a future unrelated 404 starts fresh.
                        self.shared.reconcile_not_found_attempts.lock().unwrap().remove(coid);
                        self.shared.register_order_id(coid, oid, symbol);
                        self.shared.open_orders.lock().unwrap().insert(
                            coid.clone(),
                            TrackedOrder { symbol: symbol.clone(), side: *side, instance_id: self.instance_id.clone() },
                        );
                        info!(
                            "[PolymarketTrade] Reconciled placement coid={} orderID={} → LIVE",
                            coid, oid,
                        );
                        updates.push(OrderUpdate {
                            client_order_id: coid.clone(),
                            exchange: Exchange::Polymarket,
                            symbol: symbol.clone(),
                            side: *side,
                            exchange_order_id: Some(oid.to_string()),
                            status: OrderStatus::Accepted,
                            liquidity: None,
                            filled_quantity: 0.0,
                            remaining_quantity: 0.0,
                            avg_fill_price: *price,
                            timestamp_ns: now_ns(),
                            trade_id: None,
                            error: None,
                        });
                    }
                    "MATCHED" | "FILLED" => {
                        self.shared.reconcile_not_found_attempts.lock().unwrap().remove(coid);
                        self.shared.remove_order(coid);
                        info!(
                            "[PolymarketTrade] Reconciled placement coid={} orderID={} → Filled",
                            coid, oid,
                        );
                        updates.push(OrderUpdate {
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
                            trade_id: None,
                            error: None,
                        });
                    }
                    "CANCELED" | "CANCELLED" => {
                        self.shared.reconcile_not_found_attempts.lock().unwrap().remove(coid);
                        self.shared.remove_order(coid);
                        info!(
                            "[PolymarketTrade] Reconciled placement coid={} orderID={} → Cancelled",
                            coid, oid,
                        );
                        updates.push(OrderUpdate {
                            client_order_id: coid.clone(),
                            exchange: Exchange::Polymarket,
                            symbol: symbol.clone(),
                            side: *side,
                            exchange_order_id: Some(oid.to_string()),
                            status: OrderStatus::Cancelled,
                            liquidity: None,
                            filled_quantity: 0.0,
                            remaining_quantity: 0.0,
                            avg_fill_price: 0.0,
                            timestamp_ns: now_ns(),
                            trade_id: None,
                            error: None,
                        });
                    }
                    "" => {
                        // 404 may be a stale read replica — Polymarket CLOB
                        // is eventually-consistent across shards and a
                        // freshly-accepted order can return not_found from
                        // the read endpoint for hundreds of ms (live2 2026-
                        // 04-30: 66 % of these "rejections" later traded as
                        // ghosts). Tolerate `RECONCILE_NOT_FOUND_RETRY_LIMIT`
                        // attempts before committing Rejected. Each retry
                        // happens via the strategy's next ReconcilePolymarket
                        // signal (orphan stays in the reconciler map; the
                        // 1.5 s in_flight TTL gates re-emission).
                        let attempts = {
                            let mut m = self.shared.reconcile_not_found_attempts.lock().unwrap();
                            let entry = m.entry(coid.clone()).or_insert(0);
                            *entry += 1;
                            *entry
                        };
                        if attempts < RECONCILE_NOT_FOUND_RETRY_LIMIT {
                            warn!(
                                "[PolymarketTrade] Reconcile: placement coid={} orderID={} not found on server (attempt {}/{}) — keeping orphan, retrying",
                                coid, oid, attempts, RECONCILE_NOT_FOUND_RETRY_LIMIT,
                            );
                            // No update emitted — strategy keeps the orphan
                            // and the orphan_reconciler in_flight TTL will
                            // unblock the next dedup'd retry.
                            continue;
                        }
                        warn!(
                            "[PolymarketTrade] Reconcile: placement coid={} orderID={} not found on server (after {} attempts) → Rejected",
                            coid, oid, attempts,
                        );
                        self.shared.reconcile_not_found_attempts.lock().unwrap().remove(coid);
                        self.shared.remove_order(coid);
                        updates.push(OrderUpdate {
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
                            trade_id: None,
                            error: None,
                        });
                    }
                    "INVALID" => {
                        // Polymarket "INVALID" = order failed server-side
                        // validation (signature / expiration / nonce /
                        // already-spent collateral). Definitive terminal
                        // — never going to LIVE/MATCHED. Live evidence
                        // 2026-05-01 06:50: a single INVALID-status coid
                        // looped 2,088 reconcile attempts over 50 min,
                        // wedging the orphan-gate at strategy.rs:3226 →
                        // on_quote early-returned every tick →
                        // poll_pending_snapshots never ran → 11 events
                        // ran with no quoting. Treat exactly like
                        // Rejected so the orphan clears immediately.
                        self.shared.reconcile_not_found_attempts.lock().unwrap().remove(coid);
                        warn!(
                            "[PolymarketTrade] Reconcile: placement coid={} orderID={} status=INVALID → Rejected (server validation failed)",
                            coid, oid,
                        );
                        self.shared.remove_order(coid);
                        updates.push(OrderUpdate {
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
                            trade_id: None,
                            error: Some("server status=INVALID (validation failed)".to_string()),
                        });
                    }
                    other => {
                        // Unknown status — defensive cap: tolerate up to
                        // RECONCILE_NOT_FOUND_RETRY_LIMIT attempts (reusing
                        // the not_found counter since both paths represent
                        // "server response we can't act on definitively"),
                        // then commit Rejected so a future Polymarket-
                        // introduced status can't permanently wedge the
                        // orphan-gate.
                        let attempts = {
                            let mut m = self.shared.reconcile_not_found_attempts.lock().unwrap();
                            let entry = m.entry(coid.clone()).or_insert(0);
                            *entry += 1;
                            *entry
                        };
                        if attempts < RECONCILE_NOT_FOUND_RETRY_LIMIT {
                            warn!(
                                "[PolymarketTrade] Reconcile: placement coid={} orderID={} returned unexpected status '{}' (attempt {}/{}) — keeping as orphan",
                                coid, oid, other, attempts, RECONCILE_NOT_FOUND_RETRY_LIMIT,
                            );
                        } else {
                            warn!(
                                "[PolymarketTrade] Reconcile: placement coid={} orderID={} returned unexpected status '{}' (after {} attempts) → Rejected",
                                coid, oid, other, attempts,
                            );
                            self.shared.reconcile_not_found_attempts.lock().unwrap().remove(coid);
                            self.shared.remove_order(coid);
                            updates.push(OrderUpdate {
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
                                trade_id: None,
                                error: Some(format!("unexpected status '{}' (after {} attempts)", other, attempts)),
                            });
                        }
                    }
                }
            }
        }

        // --- Cancels: query each order by id ---
        for (coid, order_id) in pending_cancels {
            let fetch_result = self.fetch_order_status_by_id(order_id);
            // 425 mid-iteration: the fetch failed and noted a backoff. Keep
            // every remaining orphan parked instead of incorrectly committing
            // Cancelled via the `""` arm below — the next reconcile call
            // will short-circuit on `in_http_425_backoff()` and we'll retry
            // once the deadline expires.
            if fetch_result.is_none() && self.shared.in_http_425_backoff() {
                log::debug!(
                    "[PolymarketTrade] Reconcile cancel coid={} orderID={}: fetch deferred (HTTP 425 backoff); keeping orphan",
                    coid, order_id,
                );
                continue;
            }
            let status_str = fetch_result.unwrap_or_default();
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
                    // The DELETE response tells us the terminal state
                    // directly: `canceled=[orderID]` → Cancelled;
                    // `not_canceled` with "matched" reason → Filled
                    // (the order raced us to the fill in the retry
                    // window). Either way we resolve the orphan in
                    // this reconcile pass instead of waiting another.
                    let body = serde_json::json!({ "orderID": order_id });
                    match self.delete_detailed("/order", &body) {
                        Ok(resp) => {
                            let in_canceled = resp.get("canceled")
                                .and_then(|v| v.as_array())
                                .map(|a| a.iter().filter_map(|v| v.as_str())
                                    .any(|s| s == order_id))
                                .unwrap_or(false);
                            if in_canceled {
                                info!("[PolymarketTrade] Reconcile DELETE retry coid={} orderID={} → Cancelled",
                                    coid, order_id);
                                OrderStatus::Cancelled
                            } else if let Some(nc) = resp.get("not_canceled").and_then(|v| v.as_object()) {
                                if let Some(reason_v) = nc.get(order_id) {
                                    let reason = reason_v.as_str().unwrap_or("");
                                    // This is the reconcile retry path — we
                                    // already queried the server once via
                                    // `fetch_order_status_by_id`. Even
                                    // Uncertain reasons must commit here to
                                    // avoid an infinite re-orphan loop;
                                    // collapse to Cancelled (the trade push,
                                    // if a fill happened, will arrive
                                    // independently via user_feed).
                                    let s = match cancel_not_canceled_outcome(reason) {
                                        CancelReasonOutcome::Cancelled
                                        | CancelReasonOutcome::Uncertain => OrderStatus::Cancelled,
                                        CancelReasonOutcome::Filled => OrderStatus::Filled,
                                    };
                                    info!("[PolymarketTrade] Reconcile DELETE retry coid={} orderID={} → {:?} (reason={})",
                                        coid, order_id, s, reason);
                                    s
                                } else {
                                    // Server didn't mention this orderID at all — stay an orphan.
                                    OrderStatus::CancelOrderTimeout
                                }
                            } else {
                                OrderStatus::CancelOrderTimeout
                            }
                        }
                        Err(e) => {
                            // HTTP 425 specifically signals "service overloaded
                            // — retry later". Open the global backoff so the
                            // next ~10 s of reconcile signals short-circuit
                            // before hitting HTTP. The order stays orphan and
                            // gets re-checked once the deadline expires.
                            if matches!(e, HttpErr::Status(425, _)) {
                                self.shared.note_http_425_backoff();
                            }
                            warn!("[PolymarketTrade] Reconcile DELETE retry coid={} orderID={} HTTP error: {} — keeping as orphan",
                                coid, order_id, e);
                            OrderStatus::CancelOrderTimeout
                        }
                    }
                }
                "MATCHED" | "FILLED" => OrderStatus::Filled,
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
                    // Not found. Default = assume gone → Cancelled. BUT a
                    // pending/delayed orphan is treated as UNCERTAIN, not
                    // cancelled: the cancel raced the placement and
                    // `GET /data/order` just hasn't indexed the order yet.
                    // Keep retrying (orphan stays parked; the orphan_reconciler
                    // re-emits after its in_flight TTL) so a later pass hits the
                    // "LIVE" arm above and re-DELETEs it — instead of committing
                    // Cancelled on an order that goes live ~tens of ms later.
                    // Bounded by RECONCILE_NOT_FOUND_RETRY_LIMIT (≈7.5 s) so a
                    // never-materialising order can't wedge the orphan-gate; the
                    // OrderManager resurrection + layer-2 sweep backstop the cap.
                    // Every other not-found cancel still commits Cancelled
                    // immediately (unchanged).
                    if self.shared.pending_delayed_orphans.lock().unwrap().contains(coid) {
                        let attempts = {
                            let mut m = self.shared.reconcile_not_found_attempts.lock().unwrap();
                            let entry = m.entry(coid.clone()).or_insert(0);
                            *entry += 1;
                            *entry
                        };
                        if attempts < RECONCILE_NOT_FOUND_RETRY_LIMIT {
                            warn!(
                                "[PolymarketTrade] Reconcile cancel coid={} orderID={} pending/delayed not found yet (attempt {}/{}) — uncertain, keeping orphan, retrying",
                                coid, order_id, attempts, RECONCILE_NOT_FOUND_RETRY_LIMIT,
                            );
                            OrderStatus::CancelOrderTimeout
                        } else {
                            warn!(
                                "[PolymarketTrade] Reconcile cancel coid={} orderID={} pending/delayed never materialised (after {} attempts) → Cancelled",
                                coid, order_id, attempts,
                            );
                            self.shared.reconcile_not_found_attempts.lock().unwrap().remove(coid);
                            OrderStatus::Cancelled
                        }
                    } else {
                        OrderStatus::Cancelled // not found → assume gone
                    }
                }
                other => {
                    // Unknown status — defensive retry cap so a future
                    // Polymarket-introduced status can't permanently wedge
                    // the orphan-gate (same pattern as the placement-reconcile
                    // `other` arm). Reuse `reconcile_not_found_attempts` since
                    // both paths represent "server response we can't act on
                    // definitively yet". After the cap commit to Cancelled
                    // (conservative: a cancel DELETE has already been
                    // attempted at least once and we polled `/data/order`).
                    let attempts = {
                        let mut m = self.shared.reconcile_not_found_attempts.lock().unwrap();
                        let entry = m.entry(coid.clone()).or_insert(0);
                        *entry += 1;
                        *entry
                    };
                    if attempts < RECONCILE_NOT_FOUND_RETRY_LIMIT {
                        warn!(
                            "[PolymarketTrade] Reconcile cancel coid={} orderID={} unknown server status '{}' (attempt {}/{}) — keeping as orphan",
                            coid, order_id, other, attempts, RECONCILE_NOT_FOUND_RETRY_LIMIT,
                        );
                        OrderStatus::CancelOrderTimeout
                    } else {
                        warn!(
                            "[PolymarketTrade] Reconcile cancel coid={} orderID={} unknown server status '{}' (after {} attempts) → giving up, committing Cancelled",
                            coid, order_id, other, attempts,
                        );
                        self.shared.reconcile_not_found_attempts.lock().unwrap().remove(coid);
                        OrderStatus::Cancelled
                    }
                }
            };
            if status == OrderStatus::Cancelled || status == OrderStatus::Filled {
                self.shared.remove_order(coid);
                // Clear the defensive-retry counter on conclusive resolution
                // so a later unrelated unknown-status arm for the same coid
                // starts fresh.
                self.shared.reconcile_not_found_attempts.lock().unwrap().remove(coid);
            }
            info!("[PolymarketTrade] Reconcile cancel coid={} orderID={} → {:?} (server={})",
                coid, order_id, status, status_str);
            let tracked = self.shared.open_orders.lock().unwrap().get(coid).cloned();
            let (symbol, side) = tracked
                .map(|t| (t.symbol, t.side))
                .unwrap_or_else(|| (String::new(), Side::Buy));
            updates.push(OrderUpdate {
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
                trade_id: None,
                error: None,
            });
        }

        updates
    }

    /// Query a single order's server status by orderID. Returns the status
    /// string (e.g. "LIVE", "MATCHED", "CANCELED") or None on error /
    /// not found.
    ///
    /// Endpoint: `GET /data/order/{orderID}`. Tried `GET /order/{id}`
    /// briefly (commit 8b4ce1b) on the guess that it was "more modern"
    /// — empirically returns `404 page not found` from clob.polymarket.com
    /// while `/data/order/{id}` returns proper status strings. The
    /// py-clob-client SDK also uses the /data path.
    fn fetch_order_status_by_id(&self, order_id: &str) -> Option<String> {
        let path = format!("/data/order/{}", order_id);
        let json = match self.shared.http_call_sync("GET", &path, "") {
            Ok(j) => j,
            Err(e) => {
                // HTTP 425 from the GET side: the upstream service is in a
                // "not ready" storm. Open the global reconcile backoff so
                // subsequent reconciles short-circuit; the caller will see
                // None here and (defensively) treat it as "no status yet"
                // — see the empty-string handling around the call site.
                if matches!(e, HttpErr::Status(425, _)) {
                    self.shared.note_http_425_backoff();
                }
                warn!("[PolymarketTrade] Reconcile /data/order/{}: {}", order_id, e);
                return None;
            }
        };
        json.get("status").and_then(|v| v.as_str()).map(|s| s.to_string())
    }
}

/// Context captured at cancel-kickoff time, threaded into
/// `handle_cancel_reply` so it can build the OrderUpdate without
/// re-querying internal maps after the recv races.
pub(crate) struct CancelCtx {
    pub local_oid: Option<String>,
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
    ) -> std::result::Result<
        (String, crossbeam_channel::Receiver<HttpReply>),
        OrderUpdate,
    > {
        let (local_oid, body_str) = self.submit_prep(order)?;
        let rx = self.shared.http_call_async("POST", "/order", &body_str);
        Ok((local_oid, rx))
    }

    /// Synchronous prep for a place: rate/balance gate, sign, register the
    /// orderID + `open_orders` entry, and return `(local_oid, body_json)`.
    /// Split out of `submit_kickoff` so the kickoff path can prep then
    /// dispatch separately. Returns the synthetic `OrderUpdate` on a
    /// pre-flight reject (rate-limit / backoff / sign).
    pub(crate) fn submit_prep(
        &mut self,
        order: &OrderRequest,
    ) -> std::result::Result<(String, String), OrderUpdate> {
        if !self.shared.check_rate_limit() {
            return Err(Self::make_rejected(order, "rate limited"));
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
        self.shared.register_order_id(&order.client_order_id, &local_oid, &order.symbol);
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
        self.shared.open_orders.lock().unwrap().insert(
            order.client_order_id.clone(),
            TrackedOrder {
                symbol: order.symbol.clone(),
                side: order.side,
                instance_id: self.instance_id.clone(),
            },
        );

        let sym_short = if order.symbol.len() > 16 { &order.symbol[..16] } else { &order.symbol };
        // `gen_ns` = strategy on_quote emission time (ns) carried on the
        // OrderRequest. Pairs this place with its quote for offline
        // on_quote→dispatch latency analysis (dispatch wall-clock − gen_ns).
        info!("[PolymarketTrade] Submit {} {}... @ {:.3} qty={} coid={} oid={} gen_ns={}",
            order.side, sym_short, order.price.unwrap_or(0.0), order.quantity,
            order.client_order_id, &local_oid[..18.min(local_oid.len())], order.timestamp_ns);
        log::debug!("[PolymarketTrade] Order body: {}", serde_json::to_string_pretty(&body).unwrap_or_default());

        Ok((local_oid, body.to_string()))
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
        let resp = match reply {
            Ok(r) => r,
            Err(e) if e.is_unknown_state() => {
                if self.shared.should_warn_unknown_state(&e) {
                    warn!("[PolymarketTrade] Order unknown state ({}) coid={} oid={} → NewOrderTimeout",
                        e, order.client_order_id, &local_oid[..18.min(local_oid.len())]);
                }
                return Self::make_timeout_place(order, Some(local_oid));
            }
            Err(e) => {
                // Non-timeout HTTP error (e.g. 4xx parse error, transport
                // failure that's NOT classified as unknown_state). The
                // server's state is ambiguous in transport errors but we
                // already commit to "Rejected" semantics here, so clear
                // both `coid_to_oid` and `open_orders` to keep our local
                // tracking consistent — otherwise `handle_balance_error`
                // would later snapshot a phantom coid.
                let err_s = e.to_string();
                if SharedState::is_balance_error(&err_s) {
                    self.handle_balance_error(&order.client_order_id, order.side, &order.symbol);
                } else if SharedState::is_invalid_token_error(&err_s) {
                    self.handle_invalid_token(&order.symbol);
                }
                self.shared.remove_order(&order.client_order_id);
                warn!("[PolymarketTrade] Order failed: {} coid={}", e, order.client_order_id);
                return Self::make_rejected(order, &err_s);
            }
        };

        let success = resp.get("success").and_then(|v| v.as_bool()).unwrap_or(false);
        let order_id = resp.get("orderID").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let status_str = resp.get("status").and_then(|v| v.as_str()).unwrap_or("");
        let error_msg = resp.get("errorMsg").and_then(|v| v.as_str()).unwrap_or("");

        if !success {
            self.shared.remove_order(&order.client_order_id);
            if SharedState::is_balance_error(error_msg) {
                self.handle_balance_error(&order.client_order_id, order.side, &order.symbol);
            } else if SharedState::is_invalid_token_error(error_msg) {
                self.handle_invalid_token(&order.symbol);
            }
            warn!("[PolymarketTrade] Order rejected: {} coid={}", error_msg, order.client_order_id);
            return Self::make_rejected(order, error_msg);
        }
        // Accepted by the server → token is registered/tradeable; clear any
        // invalid-token strikes/backoff for it.
        self.shared.clear_invalid_token(&order.symbol);

        if !order_id.is_empty() && !Self::oid_eq(&order_id, local_oid) {
            warn!(
                "[PolymarketTrade] orderID MISMATCH coid={} local={} server={} — local hash is wrong!",
                order.client_order_id, local_oid, order_id,
            );
            self.shared.register_order_id(&order.client_order_id, &order_id, &order.symbol);
        }

        // `open_orders` was already populated by `submit_kickoff` at
        // sign time; on success there's nothing further to track.

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
        // The brief side-effect: `sync_pending_from_update` removes the
        // BUY-side cash lock on the placeholder, so `available_cash` is
        // briefly overstated by `price × qty` until the WS push books
        // the trade. Bounded to ~300 ms and at most O($10) at typical
        // polymaker sizes — acceptable.
        let (status, filled_quantity, remaining_quantity) = match status_str {
            "matched" => {
                let trade_ids_arr = resp.get("tradeIDs").and_then(|v| v.as_array());
                let trade_ids = trade_ids_arr.map(|a| a.len()).unwrap_or(0);
                // Single-trade taker match: the whole order filled against one
                // maker, so `order.quantity` IS the fill size and the lone
                // `tradeIDs[0]` keys cleanly against the WS taker push (which
                // also keys by plain `trade_id`). Buffer it so local inventory
                // reflects the fill before the (sometimes multi-second,
                // out-of-order) WS push lands. Multi-trade matches are skipped
                // (per-trade size attribution is ambiguous from this response).
                if trade_ids == 1 {
                    if let Some(tid) = trade_ids_arr
                        .and_then(|a| a.first())
                        .and_then(|v| v.as_str())
                    {
                        self.shared.taker_matched.try_add(
                            tid, &order_id, &order.symbol, order.side, order.quantity,
                        );
                    }
                }
                info!("[PolymarketTrade] Matched immediately: orderID={} trades={} \
                       (emitting placeholder Filled so OM removes the order; \
                       ledger updated via WS user_feed)",
                      order_id, trade_ids);
                (OrderStatus::Filled, 0.0, 0.0)
            }
            "delayed" => {
                info!("[PolymarketTrade] Deferred execution: orderID={}", order_id);
                (OrderStatus::Accepted, 0.0, order.quantity)
            }
            _ => (OrderStatus::Accepted, 0.0, order.quantity),
        };

        info!("[PolymarketTrade] Order accepted: orderID={} status={} coid={}",
            order_id, status_str, order.client_order_id);

        OrderUpdate {
            client_order_id: order.client_order_id.clone(),
            exchange: Exchange::Polymarket,
            symbol: order.symbol.clone(),
            side: order.side,
            exchange_order_id: Some(if order_id.is_empty() { local_oid.to_string() } else { order_id }),
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
            trade_id: None,
            error: None,
        }
    }

    /// Look up local state for `coid`, dispatch a `DELETE /order` (or
    /// none if no orderID is mapped), and return:
    ///   * `(ctx, Some(rx))` — request in flight
    ///   * `(ctx, None)`     — nothing to send; emit Cancelled directly.
    pub(crate) fn cancel_kickoff(
        &mut self,
        client_order_id: &str,
    ) -> (CancelCtx, Option<crossbeam_channel::Receiver<HttpReply>>) {
        let (ctx, body) = self.cancel_prep(client_order_id);
        match body {
            Some(body_str) => {
                let rx = self.shared.http_call_async("DELETE", "/order", &body_str);
                (ctx, Some(rx))
            }
            None => (ctx, None),
        }
    }

    /// Synchronous prep for a cancel: resolve the server orderID, build the
    /// `CancelCtx` (for reply handling), and return the DELETE body string
    /// (or `None` when the coid has no local orderID → nothing to send).
    /// Prep half of `cancel_kickoff`: resolve the server orderID + tracked
    /// symbol/side and build the DELETE body (None = nothing to send).
    pub(crate) fn cancel_prep(&mut self, client_order_id: &str) -> (CancelCtx, Option<String>) {
        let order_id = self.shared.coid_to_oid.lock().unwrap()
            .get(client_order_id).cloned();
        let tracked = self.shared.open_orders.lock().unwrap()
            .get(client_order_id).cloned();
        let (symbol, side) = tracked
            .map(|t| (t.symbol, t.side))
            .unwrap_or_else(|| (String::new(), Side::Buy));
        let ctx = CancelCtx { local_oid: order_id.clone(), symbol, side };
        match order_id {
            Some(ref oid) => {
                let oid_short = &oid[..16.min(oid.len())];
                // `gen_ns` = strategy on_quote emission time (ns) of the
                // cancel/replace signal being dispatched (set by the engine on
                // this route just before the call). Pairs this cancel with its
                // quote for offline on_quote→dispatch latency analysis
                // (dispatch wall-clock − gen_ns). 0 = non-quote origin.
                info!("[PolymarketTrade] Cancel request orderID={}... coid={} gen_ns={}",
                    oid_short, client_order_id, self.gen_ns_hint);
                let body_str = serde_json::json!({ "orderID": oid }).to_string();
                (ctx, Some(body_str))
            }
            None => {
                info!("[PolymarketTrade] Cancel coid={} — no orderID locally, nothing to send", client_order_id);
                (ctx, None)
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
        let CancelCtx { local_oid, symbol, side } = ctx;
        let mut should_remove = true;
        let mut timed_out = false;
        let mut ok_status = OrderStatus::Cancelled;

        if let Some(reply) = reply {
            let oid_ref = local_oid.as_deref().unwrap_or("");
            let oid_short = &oid_ref[..16.min(oid_ref.len())];
            match reply {
                Ok(resp) => {
                    let canceled_n = resp.get("canceled").and_then(|v| v.as_array())
                        .map(|a| a.len()).unwrap_or(0);
                    let not_canceled = resp.get("not_canceled").and_then(|v| v.as_object());
                    let nc_n = not_canceled.map(|o| o.len()).unwrap_or(0);
                    info!(
                        "[PolymarketTrade] Cancel result orderID={}... coid={} canceled={} not_canceled={}",
                        oid_short, client_order_id, canceled_n, nc_n,
                    );
                    if let Some(nc) = not_canceled {
                        for (id, reason) in nc {
                            let reason_str = reason.as_str().unwrap_or("");
                            info!("[PolymarketTrade] Cancel rejected: {} reason={} coid={}",
                                id, reason_str, client_order_id);
                            if Some(id.as_str()) == local_oid.as_deref() {
                                match cancel_not_canceled_outcome(reason_str) {
                                    CancelReasonOutcome::Cancelled => ok_status = OrderStatus::Cancelled,
                                    CancelReasonOutcome::Filled    => ok_status = OrderStatus::Filled,
                                    CancelReasonOutcome::Uncertain => {
                                        // "order can't be found - already
                                        // canceled or matched" — server
                                        // can't disambiguate. Park as
                                        // orphan-cancel so the reconciler
                                        // queries `GET /data/order/{oid}`
                                        // for an authoritative answer
                                        // before strategy releases the
                                        // pending_orders lock.
                                        // pending/delayed = cancel raced the
                                        // placement; flag so the reconcile
                                        // not-found arm treats it as Uncertain
                                        // (keeps retrying) instead of committing
                                        // Cancelled (the forgotten-order race).
                                        if is_pending_delayed_reason(reason_str) {
                                            self.shared.pending_delayed_orphans
                                                .lock().unwrap()
                                                .insert(client_order_id.to_string());
                                        }
                                        info!("[PolymarketTrade] Cancel reply uncertain (reason={}) coid={} → orphan",
                                            reason_str, client_order_id);
                                        should_remove = false;
                                        timed_out = true;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) if e.is_unknown_state() => {
                    // 425 falls through here too (per `is_unknown_state`); the
                    // dedup helper suppresses repeats of 425 storms within 5
                    // min. Timeouts / 5xx always WARN.
                    if self.shared.should_warn_unknown_state(&e) {
                        warn!("[PolymarketTrade] Cancel unknown state ({}) coid={} orderID={}... → CancelOrderTimeout",
                            e, client_order_id, oid_short);
                    }
                    // HTTP 425 specifically signals "service overloaded, retry
                    // later" — open a global backoff so subsequent
                    // `reconcile_orphans` skips its DELETE retries until the
                    // upstream service recovers. The order still becomes an
                    // orphan (we have no way to know if the cancel was
                    // received) but we won't hammer the server with retries.
                    if matches!(e, HttpErr::Status(425, _)) {
                        self.shared.note_http_425_backoff();
                    }
                    should_remove = false;
                    timed_out = true;
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

        if timed_out {
            return Self::make_timeout_cancel(client_order_id, &symbol, side, local_oid);
        }
        if should_remove {
            self.shared.remove_order(client_order_id);
        }
        let status = if should_remove { ok_status } else { OrderStatus::Accepted };

        OrderUpdate {
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
            trade_id: None,
            error: None,
        }
    }
}

impl ExchangeTrade for PolymarketTrade {
    fn submit_order(&mut self, order: &OrderRequest) -> Result<OrderUpdate> {
        let (local_oid, rx) = match self.submit_kickoff(order) {
            Ok(v) => v,
            Err(update) => return Ok(update),
        };
        let reply = rx.recv()
            .unwrap_or_else(|_| Err(HttpErr::Other("async reply dropped".to_string())));
        Ok(self.handle_submit_reply(order, &local_oid, reply))
    }

    fn cancel_order(&mut self, exchange: Exchange, client_order_id: &str) -> Result<OrderUpdate> {
        let (ctx, rx_opt) = self.cancel_kickoff(client_order_id);
        let reply = rx_opt.map(|rx| rx.recv()
            .unwrap_or_else(|_| Err(HttpErr::Other("async reply dropped".to_string()))));
        Ok(self.handle_cancel_reply(exchange, client_order_id, ctx, reply))
    }

    fn cancel_all(&mut self, exchange: Exchange, symbol: &str) -> Result<Vec<OrderUpdate>> {
        // Collect all open order IDs for this symbol
        let mut order_ids: Vec<String> = Vec::new();
        let mut coids: Vec<String> = Vec::new();
        {
            let open = self.shared.open_orders.lock().unwrap();
            let coid_to_oid = self.shared.coid_to_oid.lock().unwrap();
            for (coid, tracked) in open.iter() {
                if tracked.symbol == symbol {
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

        info!("[PolymarketTrade] Cancel all request: {} orders for {}", order_ids.len(), symbol);

        // Batch cancel (up to 3000)
        let body = serde_json::Value::Array(
            order_ids.iter().map(|id| serde_json::Value::String(id.clone())).collect()
        );
        match self.delete_detailed("/orders", &body) {
            Ok(resp) => {
                let canceled_n = resp.get("canceled").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0);
                let nc_n = resp.get("not_canceled").and_then(|v| v.as_object()).map(|o| o.len()).unwrap_or(0);
                info!("[PolymarketTrade] Cancel all result for {}: canceled={} not_canceled={}",
                    symbol, canceled_n, nc_n);
            }
            Err(e) => warn!("[PolymarketTrade] Cancel all HTTP error: {}", e),
        }

        // Remove from tracking and build updates
        let mut updates = Vec::new();
        for coid in &coids {
            let tracked = self.shared.open_orders.lock().unwrap()
                .get(coid).cloned();
            self.shared.remove_order(coid);
            updates.push(OrderUpdate {
                client_order_id: coid.clone(),
                exchange,
                symbol: symbol.to_string(),
                side: tracked.map(|t| t.side).unwrap_or(Side::Buy),
                exchange_order_id: None,
                status: OrderStatus::Cancelled,
                liquidity: None,
                filled_quantity: 0.0,
                remaining_quantity: 0.0,
                avg_fill_price: 0.0,
                timestamp_ns: now_ns(),
                trade_id: None,
                error: None,
            });
        }

        Ok(updates)
    }

    fn batch_submit_orders(&mut self, _market_id: &str, orders: &[OrderRequest]) -> Result<Vec<OrderUpdate>> {
        // Balance-backoff short-circuit (see `submit_order` for rationale).
        if self.shared.in_balance_backoff(&self.instance_id) {
            return Ok(orders.iter()
                .map(|o| Self::make_rejected(o, "balance backoff"))
                .collect());
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
            let mut waiters: Vec<(usize, String, crossbeam_channel::Receiver<HttpReply>)>
                = Vec::with_capacity(orders.len());
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
                let reply = rx.recv()
                    .unwrap_or_else(|_| Err(HttpErr::Other("async reply dropped".to_string())));
                slots[idx] = Some(self.handle_submit_reply(&orders[idx], &local_oid, reply));
            }
            for slot in slots {
                if let Some(u) = slot { updates.push(u); }
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
                match self.sign_and_build_body(o) {
                    Ok((order_hash, b)) => {
                        // Pre-register BEFORE the HTTP call so the map
                        // survives a timeout / dropped ack. Same
                        // open_orders insert as `submit_kickoff` —
                        // makes the map the single source of truth
                        // for "may be live on the server".
                        self.shared.register_order_id(&o.client_order_id, &order_hash, &o.symbol);
                        self.shared.open_orders.lock().unwrap().insert(
                            o.client_order_id.clone(),
                            TrackedOrder {
                                symbol: o.symbol.clone(),
                                side: o.side,
                                instance_id: self.instance_id.clone(),
                            },
                        );
                        signed_hashes.push(order_hash);
                        bodies.push(b);
                        body_to_chunk.push(idx);
                    }
                    Err(e) => {
                        warn!(
                            "[PolymarketTrade] sign failed coid={}: {} — skipping",
                            o.client_order_id, e,
                        );
                        all_updates.push(Self::make_rejected(o, &e.to_string()));
                    }
                }
            }

            if bodies.is_empty() { continue; }

            // Single order → POST /order with the single object; multiple →
            // POST /orders with an array. POST /order returns the order
            // object directly; POST /orders returns an array of per-order
            // results. Normalize both into `responses: Vec<Value>` below.
            let (path, body) = if bodies.len() == 1 {
                ("/order", bodies[0].clone())
            } else {
                ("/orders", serde_json::Value::Array(bodies.clone()))
            };
            let chunk_coids: Vec<String> = chunk.iter()
                .map(|o| o.client_order_id.clone()).collect();
            let details: Vec<String> = chunk.iter()
                .map(|o| format_order_brief(o))
                .collect();
            info!(
                "[PolymarketTrade] Submit request: {} orders [{}]",
                bodies.len(), details.join(", "),
            );
            match self.post_detailed(path, &body) {
                Ok(resp) => {
                    let responses: Vec<serde_json::Value> = if resp.is_array() {
                        resp.as_array().cloned().unwrap_or_default()
                    } else {
                        vec![resp]
                    };
                    let mut accepted_coids: Vec<String> = Vec::new();
                    let mut rejected_coids: Vec<String> = Vec::new();
                    for (i, r) in responses.iter().enumerate() {
                        if i >= bodies.len() { break; }
                        // Response[i] pairs with bodies[i] / signed_orders[i];
                        // the chunk entry is chunk[body_to_chunk[i]].
                        let order = &chunk[body_to_chunk[i]];
                        let local_oid = &signed_hashes[i];
                        let success = r.get("success").and_then(|v| v.as_bool()).unwrap_or(false);
                        let order_id = r.get("orderID").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let status_str = r.get("status").and_then(|v| v.as_str()).unwrap_or("");
                        let error_msg = r.get("errorMsg").and_then(|v| v.as_str()).unwrap_or("");

                        if success && !order_id.is_empty() {
                            accepted_coids.push(order.client_order_id.clone());
                            // Cross-check vs our pre-computed hash — if the
                            // server's orderID disagrees, our local hash
                            // algorithm has drifted; re-register under the
                            // server's value so cancel-by-id still works.
                            if !Self::oid_eq(&order_id, local_oid) {
                                warn!(
                                    "[PolymarketTrade] orderID MISMATCH coid={} local={} server={}",
                                    order.client_order_id, local_oid, order_id,
                                );
                                self.shared.register_order_id(&order.client_order_id, &order_id, &order.symbol);
                            }
                            // open_orders already populated at sign time.
                        } else {
                            rejected_coids.push(order.client_order_id.clone());
                            // Drop local active-order tracking (`open_orders`)
                            // but KEEP the coid↔orderID mapping: a crosses-book
                            // reject can still be matched by the server, so a
                            // late fill must still resolve its coid (the map is
                            // reclaimed at the event-expiry sweep).
                            self.shared.remove_order(&order.client_order_id);
                            if SharedState::is_balance_error(error_msg) {
                                self.handle_balance_error(&order.client_order_id, order.side, &order.symbol);
                            }
                            warn!(
                                "[PolymarketTrade] Submit rejected: coid={} err=\"{}\" status={}",
                                order.client_order_id, error_msg, status_str,
                            );
                        }

                        all_updates.push(OrderUpdate {
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
                            status: if success { OrderStatus::Accepted } else { OrderStatus::Rejected },
                            liquidity: None,
                            filled_quantity: 0.0,
                            remaining_quantity: order.quantity,
                            avg_fill_price: 0.0,
                            timestamp_ns: now_ns(),
                            trade_id: None,
                            error: None,
                        });
                    }
                    info!(
                        "[PolymarketTrade] Submit result: accepted={:?} rejected={:?}",
                        accepted_coids, rejected_coids,
                    );
                }
                Err(e) if e.is_unknown_state() => {
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
                        all_updates.push(Self::make_timeout_place(order, Some(oh)));
                    }
                }
                Err(e) => {
                    // HTTP 4xx or other definitive error — the server rejected
                    // the batch cleanly (no order placed). Emit Rejected for
                    // all orders in the chunk.
                    let err_s = e.to_string();
                    if SharedState::is_balance_error(&err_s) {
                        // Use the first chunk order's side+symbol as the
                        // representative for the targeted-cancel scope.
                        // Polymarket batches sent by the strategy are
                        // typically uniform side/symbol (one outcome's
                        // BIDs or one outcome's ASKs), so first-order is
                        // a faithful sample. record_balance_error()
                        // de-dupes if multiple orders trigger.
                        if let Some(first) = chunk.first() {
                            self.handle_balance_error(&first.client_order_id, first.side, &first.symbol);
                        }
                    } else if SharedState::is_invalid_token_error(&err_s) {
                        if let Some(first) = chunk.first() {
                            self.handle_invalid_token(&first.symbol);
                        }
                    }
                    warn!("[PolymarketTrade] Submit failed: {} coids={:?}", e, chunk_coids);
                    // Clear local active-order tracking for every chunk
                    // member — committed to Rejected, so leaving sign-time
                    // `open_orders` entries behind would mislead the next
                    // `handle_balance_error` snapshot. `remove_order` KEEPS
                    // the coid↔oid map: an HTTP error is ambiguous (the
                    // chunk may have landed) so a late fill must still map.
                    for order in chunk {
                        self.shared.remove_order(&order.client_order_id);
                        all_updates.push(Self::make_rejected(order, &err_s));
                    }
                }
            }
        }
        Ok(all_updates)
    }

    fn batch_cancel_orders(&mut self, exchange: Exchange, _market_id: &str, client_order_ids: &[String]) -> Result<Vec<OrderUpdate>> {
        // Single-endpoint mode: kickoff every `DELETE /order` first so
        // they fly concurrently over the h2 connection, then drain the
        // receivers. Same pattern as `batch_submit_orders`.
        if !self.shared.use_batch_orders {
            let mut waiters: Vec<(usize, CancelCtx, Option<crossbeam_channel::Receiver<HttpReply>>)>
                = Vec::with_capacity(client_order_ids.len());
            for (idx, coid) in client_order_ids.iter().enumerate() {
                let (ctx, rx_opt) = self.cancel_kickoff(coid);
                waiters.push((idx, ctx, rx_opt));
            }
            let mut updates: Vec<OrderUpdate> = Vec::with_capacity(client_order_ids.len());
            for (idx, ctx, rx_opt) in waiters {
                let reply = rx_opt.map(|rx| rx.recv()
                    .unwrap_or_else(|_| Err(HttpErr::Other("async reply dropped".to_string()))));
                updates.push(self.handle_cancel_reply(
                    exchange, &client_order_ids[idx], ctx, reply,
                ));
            }
            return Ok(updates);
        }
        let mut order_ids: Vec<String> = Vec::new();
        let mut sent_coids: Vec<String> = Vec::new();
        let mut unmapped_coids: Vec<String> = Vec::new();
        {
            let map = self.shared.coid_to_oid.lock().unwrap();
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
                (
                    "/order",
                    serde_json::json!({ "orderID": order_ids[0] }),
                )
            } else {
                (
                    "/orders",
                    serde_json::Value::Array(
                        order_ids.iter().map(|id| serde_json::Value::String(id.clone())).collect()
                    ),
                )
            };
            if unmapped_coids.is_empty() {
                info!(
                    "[PolymarketTrade] Cancel request: {} orders coids={:?}",
                    sent_coids.len(), sent_coids,
                );
            } else {
                info!(
                    "[PolymarketTrade] Cancel request: {} orders coids={:?} (+ {} unmapped coids={:?})",
                    sent_coids.len(), sent_coids,
                    unmapped_coids.len(), unmapped_coids,
                );
            }
            // Per-coid outcome map. On Ok: fill from `canceled` +
            // `not_canceled` (matched → Filled, other reasons → Cancelled).
            // On unknown_state: all coids → CancelOrderTimeout.
            // On other Err: all coids → Cancelled.
            let mut per_coid_outcome: std::collections::HashMap<String, OrderStatus>
                = std::collections::HashMap::new();
            let fallback_outcome: OrderStatus = match self.delete_detailed(path, &body) {
                Ok(resp) => {
                    let oid_to_coid = self.shared.oid_to_coid.lock().unwrap().clone();
                    let canceled_oids: Vec<String> = resp.get("canceled")
                        .and_then(|v| v.as_array())
                        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                        .unwrap_or_default();
                    let not_canceled = resp.get("not_canceled").and_then(|v| v.as_object());
                    for oid in &canceled_oids {
                        if let Some(coid) = oid_to_coid.get(oid) {
                            per_coid_outcome.insert(coid.clone(), OrderStatus::Cancelled);
                        }
                    }
                    let canceled_coids: Vec<String> = canceled_oids.iter()
                        .map(|oid| oid_to_coid.get(oid).cloned().unwrap_or_default()).collect();
                    let not_canceled_coids: Vec<String> = not_canceled
                        .map(|m| m.keys()
                            .map(|oid| oid_to_coid.get(oid).cloned().unwrap_or_default())
                            .collect())
                        .unwrap_or_default();
                    info!(
                        "[PolymarketTrade] Cancel result: canceled={:?} not_canceled={:?}",
                        canceled_coids, not_canceled_coids,
                    );
                    if let Some(nc) = not_canceled {
                        for (id, reason) in nc {
                            let coid = oid_to_coid.get(id).cloned().unwrap_or_default();
                            let reason_str = reason.as_str().unwrap_or("");
                            info!(
                                "[PolymarketTrade] Cancel rejected: orderID={} reason={} coid={}",
                                id, reason_str, coid,
                            );
                            if !coid.is_empty() {
                                let s = match cancel_not_canceled_outcome(reason_str) {
                                    CancelReasonOutcome::Cancelled => OrderStatus::Cancelled,
                                    CancelReasonOutcome::Filled    => OrderStatus::Filled,
                                    // Defer to orphan reconcile — same
                                    // rationale as `handle_cancel_reply`:
                                    // server can't disambiguate, ask
                                    // GET /data/order/{oid} before
                                    // releasing pending_orders lock.
                                    CancelReasonOutcome::Uncertain => {
                                        if is_pending_delayed_reason(reason_str) {
                                            self.shared.pending_delayed_orphans
                                                .lock().unwrap().insert(coid.clone());
                                        }
                                        OrderStatus::CancelOrderTimeout
                                    }
                                };
                                per_coid_outcome.insert(coid, s);
                            }
                        }
                    }
                    // Coids the caller sent but that the server didn't
                    // mention (neither canceled nor not_canceled) default
                    // to Cancelled — server treated them as gone.
                    OrderStatus::Cancelled
                }
                Err(e) if e.is_unknown_state() => {
                    if self.shared.should_warn_unknown_state(&e) {
                        warn!(
                            "[PolymarketTrade] Cancel unknown state ({}) coids={:?} → CancelOrderTimeout",
                            e, client_order_ids,
                        );
                    }
                    OrderStatus::CancelOrderTimeout
                }
                Err(e) => {
                    warn!("[PolymarketTrade] Cancel HTTP error: {} coids={:?}", e, client_order_ids);
                    OrderStatus::Cancelled
                }
            };
            let mut updates = Vec::new();
            for coid in client_order_ids {
                let tracked = self.shared.open_orders.lock().unwrap()
                    .get(coid).cloned();
                let order_id = self.shared.coid_to_oid.lock().unwrap().get(coid).cloned();
                let outcome = per_coid_outcome.get(coid).copied().unwrap_or(fallback_outcome);
                // Drop local tracking for terminal outcomes; keep for
                // CancelOrderTimeout so the orphan reconciler can re-query.
                if matches!(outcome, OrderStatus::Cancelled | OrderStatus::Filled) {
                    self.shared.remove_order(coid);
                }
                updates.push(OrderUpdate {
                    client_order_id: coid.clone(),
                    exchange,
                    symbol: tracked.as_ref().map(|t| t.symbol.clone()).unwrap_or_default(),
                    side: tracked.map(|t| t.side).unwrap_or(Side::Buy),
                    exchange_order_id: order_id,
                    status: outcome,
                    liquidity: None,
                    filled_quantity: 0.0,
                    remaining_quantity: 0.0,
                    avg_fill_price: 0.0,
                    timestamp_ns: now_ns(),
                    trade_id: None,
                    error: None,
                });
            }
            return Ok(updates);
        }

        // No orderIDs to cancel (either all unmapped / already gone). Emit
        // Cancelled for every coid so strategy can release locks.
        let mut updates = Vec::new();
        for coid in client_order_ids {
            let tracked = self.shared.open_orders.lock().unwrap()
                .get(coid).cloned();
            self.shared.remove_order(coid);
            updates.push(OrderUpdate {
                client_order_id: coid.clone(),
                exchange,
                symbol: tracked.as_ref().map(|t| t.symbol.clone()).unwrap_or_default(),
                side: tracked.map(|t| t.side).unwrap_or(Side::Buy),
                exchange_order_id: None,
                status: OrderStatus::Cancelled,
                liquidity: None,
                filled_quantity: 0.0,
                remaining_quantity: 0.0,
                avg_fill_price: 0.0,
                timestamp_ns: now_ns(),
                trade_id: None,
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

        // Balance-backoff short-circuit on the PLACE side only — let
        // the cancels still dispatch. The strategy's coid-specific
        // cancels (issued by its own quote-tick decision) are
        // independent of the targeted batch DELETE we fired in
        // `handle_balance_error`; both need to land for local state
        // and the server's allowance pool to converge. Pre-reject
        // every place during the 200 ms window so doomed submits
        // don't get hammered while the cancels race to land.
        if self.shared.in_balance_backoff(&self.instance_id) && !place_orders.is_empty() {
            let mut pre: Vec<OrderUpdate> = place_orders.iter()
                .map(|o| Self::make_rejected(o, "balance backoff"))
                .collect();
            // Still process cancels — recurse into the cancel-only path.
            let rest = self.batch_update_orders(
                exchange, _market_id, cancel_client_order_ids, &[]
            )?;
            pre.extend(rest);
            return Ok(pre);
        }

        // Per-token invalid-token backoff: pre-reject only the places whose
        // token is gated (CLOB book not live for that event), keep the rest +
        // cancels. Per-token, so concurrent events with valid tokens proceed.
        // (Single-endpoint mode also re-checks per order in `submit_prep`;
        // this entry filter additionally covers true-batch `POST /orders`.)
        if place_orders.iter().any(|o| self.shared.in_invalid_token_backoff(&o.symbol)) {
            let (blocked, allowed): (Vec<OrderRequest>, Vec<OrderRequest>) =
                place_orders.iter().cloned()
                    .partition(|o| self.shared.in_invalid_token_backoff(&o.symbol));
            let mut pre: Vec<OrderUpdate> = blocked.iter()
                .map(|o| Self::make_rejected(o, "invalid token backoff"))
                .collect();
            let rest = self.batch_update_orders(
                exchange, _market_id, cancel_client_order_ids, &allowed
            )?;
            pre.extend(rest);
            return Ok(pre);
        }

        // Single-endpoint mode (`use_batch_orders=false`).
        //
        // FULLY CONCURRENT dispatch — cancels AND places kicked off together,
        // no ordering between them: cancels ride the CANCEL pool, places the
        // FAST pool (disjoint h1.1 connections), so every request of a
        // two-leg replace is on the wire immediately after signing and the
        // signal→wire delay is just the per-request prep. Critical path ≈
        // max(single RTT).
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
            let mut updates: Vec<OrderUpdate> = Vec::with_capacity(
                cancel_client_order_ids.len() + place_orders.len(),
            );

            // ── Cancel side: kickoff all, drain after places kicked off ─
            let mut cancel_waiters: Vec<(usize, CancelCtx, Option<crossbeam_channel::Receiver<HttpReply>>)>
                = Vec::with_capacity(cancel_client_order_ids.len());
            for (idx, coid) in cancel_client_order_ids.iter().enumerate() {
                let (ctx, rx_opt) = self.cancel_kickoff(coid);
                cancel_waiters.push((idx, ctx, rx_opt));
            }

            // ── Place side: kickoff all (interleaved on the wire with
            //    the cancels above) ────────────────────────────────────
            let mut place_waiters: Vec<(usize, String, crossbeam_channel::Receiver<HttpReply>)>
                = Vec::with_capacity(place_orders.len());
            let mut place_slots: Vec<Option<OrderUpdate>>
                = (0..place_orders.len()).map(|_| None).collect();
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
                let reply = rx_opt.map(|rx| rx.recv()
                    .unwrap_or_else(|_| Err(HttpErr::Other("async reply dropped".to_string()))));
                updates.push(self.handle_cancel_reply(
                    exchange, &cancel_client_order_ids[idx], ctx, reply,
                ));
            }
            for (idx, local_oid, rx) in place_waiters {
                let reply = rx.recv()
                    .unwrap_or_else(|_| Err(HttpErr::Other("async reply dropped".to_string())));
                place_slots[idx] = Some(self.handle_submit_reply(
                    &place_orders[idx], &local_oid, reply,
                ));
            }
            for slot in place_slots {
                if let Some(u) = slot { updates.push(u); }
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
        {
            let map = self.shared.coid_to_oid.lock().unwrap();
            for coid in cancel_client_order_ids {
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
                    cancel_order_ids.iter().map(|id| serde_json::Value::String(id.clone())).collect()
                ).to_string();
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
            match self.sign_and_build_body(o) {
                Ok((order_hash, b)) => {
                    self.shared.register_order_id(&o.client_order_id, &order_hash, &o.symbol);
                    // Same sign-time open_orders insert as `submit_kickoff`
                    // and `batch_submit_orders` so all submit paths share
                    // the "open_orders = may be on server" invariant.
                    self.shared.open_orders.lock().unwrap().insert(
                        o.client_order_id.clone(),
                        TrackedOrder {
                            symbol: o.symbol.clone(),
                            side: o.side,
                            instance_id: self.instance_id.clone(),
                        },
                    );
                    place_signed.push(order_hash);
                    place_bodies.push(b);
                    place_body_to_chunk.push(idx);
                }
                Err(e) => {
                    warn!(
                        "[PolymarketTrade] sign failed coid={}: {} — skipping",
                        o.client_order_id, e,
                    );
                    place_sign_failures.push(Self::make_rejected(o, &e.to_string()));
                }
            }
        }
        // Decide place endpoint: /order for 1 order body, /orders for >1.
        let place_req: Option<(&'static str, String)> = match place_bodies.len() {
            0 => None,
            1 => Some(("/order", place_bodies[0].to_string())),
            _ => Some(("/orders", serde_json::Value::Array(place_bodies.clone()).to_string())),
        };

        // ─── Dispatch both async ────────────────────────────────────────
        let place_coids: Vec<String> = place_chunk.iter()
            .map(|o| o.client_order_id.clone()).collect();
        // Captured at cancel-dispatch time so a later "Submit rejected"
        // log line can report `cancel_dispatched_ms_ago` — distinguishes
        // a balance-reject caused by genuine phantom server state from
        // one caused by a cancel/submit race within this batch call
        // (cancel not yet landed when submit hit the server).
        let batch_start_ns = now_ns();
        let batch_cancel_coids: Vec<String> = cancel_client_order_ids.to_vec();
        let cancel_rx = cancel_req.as_ref().map(|(path, body)| {
            if unmapped_coids.is_empty() {
                info!(
                    "[PolymarketTrade] Cancel request: {} orders coids={:?}",
                    sent_coids.len(), sent_coids,
                );
            } else {
                info!(
                    "[PolymarketTrade] Cancel request: {} orders coids={:?} (+ {} unmapped coids={:?})",
                    sent_coids.len(), sent_coids,
                    unmapped_coids.len(), unmapped_coids,
                );
            }
            self.shared.http_call_async("DELETE", path, body)
        });
        let place_rx = place_req.as_ref().map(|(path, body)| {
            let details: Vec<String> = place_chunk.iter()
                .map(|o| format_order_brief(o))
                .collect();
            info!(
                "[PolymarketTrade] Submit request: {} orders [{}]",
                place_chunk.len(), details.join(", "),
            );
            self.shared.http_call_async("POST", path, body)
        });

        let mut updates: Vec<OrderUpdate> = Vec::new();

        // ─── Await + parse cancel ───────────────────────────────────────
        // Resolve the outcome even when we skipped the HTTP request (i.e.
        // none of the requested coids had an orderID mapping — typically
        // because the original NewOrder was rejected by the server and
        // never reached a placed state). Those coids still need an
        // OrderUpdate emitted so strategy can clear its in_flight /
        // OrderManager state; without this they'd sit in in_flight until
        // the 5s TTL sweep.
        // `Some((fallback_outcome, per_coid_overrides))` — the fallback
        // applies to coids the server didn't mention; per-coid overrides
        // come from `canceled` / `not_canceled` (the latter with a
        // "matched" reason is mapped to Filled, see `cancel_not_canceled_outcome`).
        let cancel_outcome: Option<(OrderStatus, std::collections::HashMap<String, OrderStatus>)> = match cancel_rx {
            None if !cancel_client_order_ids.is_empty() => {
                info!(
                    "[PolymarketTrade] Cancel request: 0 orders ({} unmapped coids={:?}) → Cancelled locally",
                    unmapped_coids.len(), unmapped_coids,
                );
                Some((OrderStatus::Cancelled, std::collections::HashMap::new()))
            }
            None => None,
            Some(rx) => {
                // Classify the response so we can emit the right OrderStatus for
                // each coid:
                //   - Ok                     → Cancelled (normal success;
                //                              not_canceled reasons are logged)
                //   - Err::is_unknown_state  → CancelOrderTimeout (timeout or
                //                              HTTP 5xx — server state unknown,
                //                              orphan reconciler will verify)
                //   - Err (other / 4xx)      → Cancelled (server rejected cleanly;
                //                              order is typically "not found /
                //                              already gone")
                // Build a per-coid outcome map; on Ok responses use
                // `canceled`/`not_canceled` to distinguish plain cancels
                // from fills that raced ahead of our cancel. On errors
                // every coid gets the fallback outcome.
                let mut per_coid_outcome: std::collections::HashMap<String, OrderStatus>
                    = std::collections::HashMap::new();
                let fallback = match rx.recv().unwrap_or_else(|_| Err(HttpErr::Other("reply dropped".into()))) {
                    Ok(resp) => {
                        // Both /order and /orders return { canceled: [...], not_canceled: {...} }.
                        let oid_to_coid = self.shared.oid_to_coid.lock().unwrap().clone();
                        let canceled_oids: Vec<String> = resp.get("canceled")
                            .and_then(|v| v.as_array())
                            .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                            .unwrap_or_default();
                        let not_canceled = resp.get("not_canceled").and_then(|v| v.as_object());
                        for oid in &canceled_oids {
                            if let Some(coid) = oid_to_coid.get(oid) {
                                per_coid_outcome.insert(coid.clone(), OrderStatus::Cancelled);
                            }
                        }
                        let canceled_coids: Vec<String> = canceled_oids.iter()
                            .map(|oid| oid_to_coid.get(oid).cloned().unwrap_or_default()).collect();
                        let not_canceled_coids: Vec<String> = not_canceled
                            .map(|m| m.keys()
                                .map(|oid| oid_to_coid.get(oid).cloned().unwrap_or_default())
                                .collect())
                            .unwrap_or_default();
                        info!(
                            "[PolymarketTrade] Cancel result: canceled={:?} not_canceled={:?}",
                            canceled_coids, not_canceled_coids,
                        );
                        if let Some(nc) = not_canceled {
                            for (id, reason) in nc {
                                let coid = oid_to_coid.get(id).cloned().unwrap_or_default();
                                let reason_str = reason.as_str().unwrap_or("");
                                info!(
                                    "[PolymarketTrade] Cancel rejected: orderID={} reason={} coid={}",
                                    id, reason_str, coid,
                                );
                                if !coid.is_empty() {
                                    let s = match cancel_not_canceled_outcome(reason_str) {
                                        CancelReasonOutcome::Cancelled => OrderStatus::Cancelled,
                                        CancelReasonOutcome::Filled    => OrderStatus::Filled,
                                        // Same orphan-defer treatment as the
                                        // single-cancel path — wait for
                                        // GET /data/order/{oid} before
                                        // releasing pending_orders lock.
                                        CancelReasonOutcome::Uncertain => {
                                            if is_pending_delayed_reason(reason_str) {
                                                self.shared.pending_delayed_orphans
                                                    .lock().unwrap().insert(coid.clone());
                                            }
                                            OrderStatus::CancelOrderTimeout
                                        }
                                    };
                                    per_coid_outcome.insert(coid, s);
                                }
                            }
                        }
                        OrderStatus::Cancelled
                    }
                    Err(e) if e.is_unknown_state() => {
                        if self.shared.should_warn_unknown_state(&e) {
                            warn!(
                                "[PolymarketTrade] Cancel unknown state ({}) coids={:?} → CancelOrderTimeout",
                                e, cancel_client_order_ids,
                            );
                        }
                        OrderStatus::CancelOrderTimeout
                    }
                    Err(e) => {
                        warn!("[PolymarketTrade] Cancel HTTP error: {} coids={:?}", e, cancel_client_order_ids);
                        OrderStatus::Cancelled
                    }
                };
                Some((fallback, per_coid_outcome))
            }
        };
        if let Some((fallback_outcome, per_coid_outcome)) = cancel_outcome {
            for coid in cancel_client_order_ids {
                let tracked = self.shared.open_orders.lock().unwrap().get(coid).cloned();
                let order_id = self.shared.coid_to_oid.lock().unwrap().get(coid).cloned();
                let outcome = per_coid_outcome.get(coid).copied().unwrap_or(fallback_outcome);
                // Drop local tracking for terminal (Cancelled / Filled)
                // outcomes — keep for CancelOrderTimeout so the orphan
                // reconciler can re-query by orderID.
                if matches!(outcome, OrderStatus::Cancelled | OrderStatus::Filled) {
                    self.shared.remove_order(coid);
                }
                updates.push(OrderUpdate {
                    client_order_id: coid.clone(),
                    exchange,
                    symbol: tracked.as_ref().map(|t| t.symbol.clone()).unwrap_or_default(),
                    side: tracked.map(|t| t.side).unwrap_or(Side::Buy),
                    exchange_order_id: order_id,
                    status: outcome,
                    liquidity: None,
                    filled_quantity: 0.0,
                    remaining_quantity: 0.0,
                    avg_fill_price: 0.0,
                    timestamp_ns: now_ns(),
                    trade_id: None,
                    error: None,
                });
            }
        }

        // ─── Await + parse place ────────────────────────────────────────
        if let Some(rx) = place_rx {
            match rx.recv().unwrap_or_else(|_| Err(HttpErr::Other("reply dropped".into()))) {
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
                    for (i, r) in responses.iter().enumerate() {
                        if i >= place_bodies.len() { break; }
                        let order = &place_chunk[place_body_to_chunk[i]];
                        let local_oid = &place_signed[i];
                        let success = r.get("success").and_then(|v| v.as_bool()).unwrap_or(false);
                        let order_id = r.get("orderID").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let status_str = r.get("status").and_then(|v| v.as_str()).unwrap_or("");
                        let error_msg = r.get("errorMsg").and_then(|v| v.as_str()).unwrap_or("");
                        if success && !order_id.is_empty() {
                            accepted_coids.push(order.client_order_id.clone());
                            if !Self::oid_eq(&order_id, local_oid) {
                                warn!(
                                    "[PolymarketTrade] orderID MISMATCH coid={} local={} server={}",
                                    order.client_order_id, local_oid, order_id,
                                );
                                self.shared.register_order_id(&order.client_order_id, &order_id, &order.symbol);
                            }
                            // open_orders already populated at sign time.
                        } else {
                            rejected_coids.push(order.client_order_id.clone());
                            // Drop local active-order tracking (`open_orders`)
                            // but KEEP the coid↔orderID mapping: a crosses-book
                            // reject can still be matched by the server, so a
                            // late fill must still resolve its coid (the map is
                            // reclaimed at the event-expiry sweep).
                            self.shared.remove_order(&order.client_order_id);
                            let is_balance_err = SharedState::is_balance_error(error_msg);
                            if is_balance_err {
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
                                let elapsed_ms = (now_ns().saturating_sub(batch_start_ns)) / 1_000_000;
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
                                self.handle_balance_error(&order.client_order_id, order.side, &order.symbol);
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
                        let rejected_price = if !success { order.price.unwrap_or(0.0) } else { 0.0 };
                        let err_field = if !success && !error_msg.is_empty() {
                            Some(error_msg.to_string())
                        } else { None };
                        updates.push(OrderUpdate {
                            client_order_id: order.client_order_id.clone(),
                            exchange: Exchange::Polymarket,
                            symbol: order.symbol.clone(),
                            side: order.side,
                            exchange_order_id: Some(if order_id.is_empty() {
                                local_oid.clone()
                            } else {
                                order_id
                            }),
                            status: if success { OrderStatus::Accepted } else { OrderStatus::Rejected },
                            liquidity: None,
                            filled_quantity: 0.0,
                            remaining_quantity: order.quantity,
                            avg_fill_price: rejected_price,
                            timestamp_ns: now_ns(),
                            trade_id: None,
                            error: err_field,
                        });
                    }
                    info!(
                        "[PolymarketTrade] Submit result: accepted={:?} rejected={:?}",
                        accepted_coids, rejected_coids,
                    );
                }
                Err(e) if e.is_unknown_state() => {
                    // Timeout, HTTP 5xx, or 425 — server state is unknown.
                    // Emit NewOrderTimeout with the pre-computed orderID so
                    // the strategy can cancel / status-query by orderID
                    // directly, no open-order scan needed.
                    if self.shared.should_warn_unknown_state(&e) {
                        warn!(
                            "[PolymarketTrade] Submit unknown state ({}) coids={:?} → NewOrderTimeout",
                            e, place_coids,
                        );
                    }
                    for (i, oh) in place_signed.iter().enumerate() {
                        let order = &place_chunk[place_body_to_chunk[i]];
                        updates.push(Self::make_timeout_place(order, Some(oh)));
                    }
                }
                Err(e) => {
                    // HTTP 4xx or other definitive error — server rejected
                    // the request cleanly (no order placed). Strategy's
                    // OrderManager will mark these as Rejected locally and
                    // stop issuing cancels for them.
                    let err_s = e.to_string();
                    if SharedState::is_balance_error(&err_s) {
                        // Pick the first place_chunk order (mapped via
                        // place_body_to_chunk) as the targeted-cancel
                        // scope representative. See `batch_submit_orders`
                        // comment for the uniformity rationale.
                        if let Some(&first_idx) = place_body_to_chunk.first() {
                            let first = &place_chunk[first_idx];
                            self.handle_balance_error(&first.client_order_id, first.side, &first.symbol);
                        }
                    } else if SharedState::is_invalid_token_error(&err_s) {
                        if let Some(&first_idx) = place_body_to_chunk.first() {
                            self.handle_invalid_token(&place_chunk[first_idx].symbol);
                        }
                    }
                    warn!("[PolymarketTrade] Submit failed: {} coids={:?}", e, place_coids);
                    // Clear sign-time `open_orders` entries — same
                    // rationale as `batch_submit_orders` HTTP-error path.
                    // `remove_order` KEEPS the coid↔oid map (ambiguous
                    // HTTP error → orders may be live → late fill must map).
                    for (i, _signed) in place_signed.iter().enumerate() {
                        let order = &place_chunk[place_body_to_chunk[i]];
                        self.shared.remove_order(&order.client_order_id);
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

        Ok(updates)
    }

    fn name(&self) -> &str {
        "polymarket-live"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            oid_to_coid.insert(oid.into(), coid.into());
            coid_to_token.insert(coid.into(), tok.into());
        }

        let n = reclaim_token_mappings(
            &mut coid_to_oid, &mut oid_to_coid, &mut coid_to_token,
            &["AUP".to_string(), "ADN".to_string()],
        );
        assert_eq!(n, 2, "both event-A coids reclaimed");
        // Event A fully purged from all three maps.
        assert!(!coid_to_oid.contains_key("c1") && !coid_to_oid.contains_key("c2"));
        assert!(!oid_to_coid.contains_key("0xa1") && !oid_to_coid.contains_key("0xa2"));
        assert!(!coid_to_token.contains_key("c1"));
        // Event B untouched — its late fill can still map.
        assert_eq!(coid_to_oid.get("c3").map(String::as_str), Some("0xb1"));
        assert_eq!(oid_to_coid.get("0xb1").map(String::as_str), Some("c3"));
        assert_eq!(coid_to_token.get("c3").map(String::as_str), Some("BUP"));
    }

    // Deferral: a token swept now is NOT reclaimed immediately (its settlement
    // fills are still in flight); it matures only after RECLAIM_GRACE_NS. A
    // back-to-back sweep of a different market must not reclaim the first
    // market's just-settled tokens early.
    #[test]
    fn drain_matured_reclaims_defers_until_grace() {
        let mut q: Vec<(u64, Vec<String>)> = Vec::new();
        let t0 = 1_000_000_000_000u64;

        // Sweep A at t0 → nothing matured yet (A's fills still landing).
        let due = drain_matured_reclaims(&mut q, &["AUP".into(), "ADN".into()], t0);
        assert!(due.is_empty(), "A not reclaimed at its own sweep");

        // Sweep B 1s later (concurrent market) → still nothing matured;
        // A must NOT be reclaimed this soon.
        let due = drain_matured_reclaims(&mut q, &["BUP".into()], t0 + 1_000_000_000);
        assert!(due.is_empty(), "A not reclaimed 1s later");
        assert_eq!(q.len(), 2);

        // Sweep C past A's grace → A matures and drains; B/C still held.
        let due = drain_matured_reclaims(&mut q, &["CUP".into()], t0 + RECLAIM_GRACE_NS + 1);
        assert_eq!(due, vec![vec!["AUP".to_string(), "ADN".to_string()]]);
        assert_eq!(q.len(), 2, "B and C remain pending");
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
    fn cancel_not_canceled_outcome_handles_case_and_variants() {
        // Case-insensitive.
        assert_eq!(
            cancel_not_canceled_outcome("MATCHED ORDERS CAN'T BE CANCELED"),
            CancelReasonOutcome::Filled,
        );
        // "cant" (no apostrophe) variant — defensive against server typo.
        assert_eq!(
            cancel_not_canceled_outcome("order cant be found"),
            CancelReasonOutcome::Cancelled,
        );
        // Plain "not found" without "or matched" → no fill in flight,
        // safe to commit to Cancelled.
        assert_eq!(
            cancel_not_canceled_outcome("order not found"),
            CancelReasonOutcome::Cancelled,
        );
    }

    #[test]
    fn cancel_not_canceled_outcome_unrecognised_falls_back_to_cancelled() {
        // Unknown reason → conservative Cancelled (releases lock, no
        // infinite reconcile churn). Same behaviour as the previous
        // `cancel_not_canceled_status`.
        assert_eq!(
            cancel_not_canceled_outcome("server explosion - try again later"),
            CancelReasonOutcome::Cancelled,
        );
        assert_eq!(
            cancel_not_canceled_outcome(""),
            CancelReasonOutcome::Cancelled,
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
        assert!(is_pending_delayed_reason("can't be canceled because it is pending/delayed"));
        assert!(is_pending_delayed_reason("order is DELAYED, cannot cancel")); // case-insensitive
        assert!(is_pending_delayed_reason("order still processing"));
        assert!(!is_pending_delayed_reason("order can't be found - already canceled or matched"));
        assert!(!is_pending_delayed_reason("matched orders can't be canceled"));
        assert!(!is_pending_delayed_reason("the order is already canceled"));
        assert!(!is_pending_delayed_reason(""));
        // Consistent with the classifier: pending/delayed → Uncertain.
        for r in ["pending/delayed", "DELAYED", "processing"] {
            assert_eq!(cancel_not_canceled_outcome(r), CancelReasonOutcome::Uncertain);
        }
    }

    /// `is_unknown_state` must classify HTTP 425 as unknown_state so the
    /// cancel-reply path routes through CancelOrderTimeout + sets the
    /// reconcile backoff, instead of falling through to "definite reject".
    /// Regression guard for Bug #3.
    #[test]
    fn http_425_classified_as_unknown_state() {
        assert!(
            HttpErr::Status(425, "Too Early".to_string()).is_unknown_state(),
            "425 must route through unknown_state so the cancel path treats it as transient"
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
            "4xx (non-425) is a definitive client rejection — must NOT be unknown_state"
        );
        assert!(
            !HttpErr::Status(404, "not found".to_string()).is_unknown_state(),
            "404 is a definitive answer — must NOT be unknown_state"
        );
    }

    /// The 425 backoff window must be monotonic: once a longer deadline
    /// is set, a subsequent note must not shorten it. This guards the
    /// `note_http_425_backoff` implementation's `store-only-if-greater`
    /// rule directly via the AtomicU64 field (avoids building a full
    /// SharedState, which requires real auth keys / signer / etc.).
    /// Regression guard for Bug #2 cascade amplification.
    #[test]
    fn http_425_backoff_field_is_monotonic() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let backoff = AtomicU64::new(0);
        let now: u64 = 1_000_000_000_000; // arbitrary wall-clock proxy

        // First 425: store now + HTTP_425_BACKOFF_NS (10 s).
        let d1 = now.saturating_add(HTTP_425_BACKOFF_NS);
        let cur = backoff.load(Ordering::Relaxed);
        if d1 > cur { backoff.store(d1, Ordering::Relaxed); }
        assert_eq!(backoff.load(Ordering::Relaxed), d1);

        // Operator-style bump: extend to +60 s for a sustained storm.
        backoff.store(now.saturating_add(60_000_000_000), Ordering::Relaxed);
        let bumped = backoff.load(Ordering::Relaxed);
        assert!(bumped > d1);

        // Another 425 arrives — the `store-only-if-greater` rule must
        // NOT pull the deadline backwards from the +60s bump.
        let d2 = now.saturating_add(HTTP_425_BACKOFF_NS);
        let cur2 = backoff.load(Ordering::Relaxed);
        if d2 > cur2 { backoff.store(d2, Ordering::Relaxed); }
        let after = backoff.load(Ordering::Relaxed);
        assert_eq!(after, bumped,
            "note_http_425_backoff must only ADVANCE the deadline (never shorten). bumped={bumped} got={after}");
    }

    /// `HTTP_425_BACKOFF_NS` must be long enough to break a tight reconcile
    /// loop (≥ 500 ms throttle × few iterations) but short enough that
    /// recovery is responsive when the upstream comes back. Lock the
    /// chosen value at 10 s.
    #[test]
    fn http_425_backoff_constant_is_in_sane_range() {
        // ≥ 5 s ensures the reconciler's 500 ms throttle gets at least
        // 10 missed iterations between 425s — well past the storm window.
        assert!(HTTP_425_BACKOFF_NS >= 5_000_000_000,
            "HTTP_425_BACKOFF_NS = {} ns is too short to suppress the reconcile cascade", HTTP_425_BACKOFF_NS);
        // ≤ 60 s keeps recovery responsive once Polymarket recovers.
        assert!(HTTP_425_BACKOFF_NS <= 60_000_000_000,
            "HTTP_425_BACKOFF_NS = {} ns is too long; degrades real-time responsiveness", HTTP_425_BACKOFF_NS);
    }
}
