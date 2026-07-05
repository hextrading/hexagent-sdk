//! Async runtime infrastructure for latency-sensitive I/O paths.
//!
//! Architecture:
//!   - Single `tokio::runtime::Runtime` with `current_thread` scheduler
//!   - Runtime runs on its own dedicated OS thread (not the main thread)
//!   - Latency-critical I/O (Polymarket HTTP order submit, WS feeds) is
//!     spawned onto this runtime via `spawn` / `block_on_runtime`
//!   - Sync callers bridge via `block_on_runtime` (a `oneshot::channel`
//!     round-trip); async callers use the handle directly
//!   - Strategy engine and other CPU-bound threads stay sync, they just
//!     dispatch I/O onto the async side
//!
//! Why current_thread?
//!   - Deterministic, no work-stealing surprises for tail latency
//!   - HTTP/2 multiplexes on a single connection, one scheduler thread is plenty
//!   - Locks-free (no Send required on futures that stay local)
//!
//! Shared globals:
//!   - `RUNTIME_HANDLE`: `OnceLock` of the tokio Handle
//!   - Four role-specific HTTP/2 client pools, each with its own per-request
//!     deadline tuned to the endpoint it serves. Role isolation prevents a
//!     slow query from consuming h2 stream credits / TCP receive windows
//!     on the hot-path submit connection.
//!       * `HTTP_CLIENTS_FAST`    — POST /order, POST /orders. 500 ms
//!         timeout. Tail requests past 500 ms are almost certainly stale
//!         (p50 ≈ 30 ms against the CLOB); failing fast lets the orphan
//!         reconciler engage sooner than a 1.5 s ceiling allows.
//!       * `HTTP_CLIENTS_CANCEL`  — DELETE /order, /orders, /cancel-all.
//!         500 ms timeout. Same reasoning — a cancel that hasn't landed
//!         at 500 ms is racing against a fill, and we'd rather surface the
//!         timeout so reconcile can retry.
//!       * `HTTP_CLIENTS_RECONCILE` — orphan-path GET /data/order/{id}
//!         and reconcile DELETE retries. 1000 ms timeout. Larger than the
//!         hot-path budget so that a brief server wobble doesn't re-orphan
//!         the order we're trying to resolve.
//!       * `HTTP_CLIENTS_QUERY`   — everything else: data-api snapshots,
//!         gamma-api, /positions, /trades gap-fill, wallet relayer,
//!         heartbeats. 5 s timeout — these responses can be large
//!         (positions, open orders) and aren't latency-critical.
//!
//!     Round-robin within each pool spreads concurrent traffic across
//!     distinct TCP connections so packet loss on one doesn't stall others.
//!   - `HTTP_CLIENT_AUTO`: ALPN-negotiating single client for endpoints
//!     that don't speak h2 prior-knowledge (public Polygon RPCs).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use tokio::runtime::{Builder, Handle};
use tokio::sync::oneshot;

static RUNTIME_HANDLE: OnceLock<Handle> = OnceLock::new();

/// Number of HTTP/2 clients per role. Each client owns its own connection
/// pool → in practice one h2 TCP connection per host, per client.
/// Round-robin picking at dispatch time spreads concurrent requests across
/// multiple underlying TCP streams so that TCP-level head-of-line blocking
/// (a single lost packet stalling an entire h2 connection) impacts only a
/// fraction of in-flight work.
// Pool sizes are deliberately small. Each client owns its own h2 TCP
// connection; round-robin across the pool only matters for TCP-level
// HOL avoidance under burst. polymaker quotes ≤2 orders per tick, so
// 2 client / 2 conn per role is plenty — any larger and the rarely-
// rotated clients sit idle long enough to get pool-evicted (see
// `pool_idle_timeout` below) and pay handshake on the next round-robin
// pick. The heartbeat loop fans out across ALL pools per tick, so
// every client stays warm regardless of business traffic.
const FAST_CLIENT_POOL_SIZE: usize = 2;      // submit /order, /orders
const CANCEL_CLIENT_POOL_SIZE: usize = 2;    // DELETE /order, /cancel-all
const RECONCILE_CLIENT_POOL_SIZE: usize = 2; // orphan GET + DELETE retry
const QUERY_CLIENT_POOL_SIZE: usize = 2;     // snapshots, gap-fill, heartbeats

/// Per-role per-request timeouts.
///
/// FAST / CANCEL: 500 ms is intentionally aggressive. The strategy quotes
/// at 100 ms cadence with an in-flight gate, so a slow ack stalls
/// emission for the whole RTT — orphaning at 500 ms and letting the
/// reconciler chase resolution outperforms waiting for a stale ack the
/// strategy will then immediately want to cancel anyway.
///
/// RECONCILE: 2000 ms (was 1000 ms). Reconcile is non-hot-path — its job
/// is to get an authoritative answer for orphans (NewOrderTimeout /
/// CancelOrderTimeout) the FAST path gave up on. The 11h22m
/// 2026-05-04 live run had 16,068 NewOrderTimeouts; the orphan
/// reconciler resolved 95% of them, but its 5-attempt × 1 s budget
/// truncated against the same 500 ms-ish server-side tail that the
/// FAST path hit. Doubling the per-attempt window costs nothing on
/// the happy path (the GET completes in well under 100 ms) and gives
/// the reconciler a much better chance of seeing the order's actual
/// terminal state on its first 1-2 retries during slow CLOB windows.
/// Hard ceiling used to build the FAST and CANCEL client pools. Per-request
/// timeout (chosen by `current_fast_timeout()` / `current_cancel_timeout()`) is
/// always ≤ this value, so the client-level deadline never short-circuits
/// a longer per-request override. Sized for the worst session (`us_am`,
/// 1500 ms) plus a small safety margin.
///
/// History: until 2026-05-12 the FAST/CANCEL clients used a flat 500 ms
/// ceiling. Across live5/6/7 (~62 h), 85-94 % of "timeouts" were orders
/// the server HAD accepted — our 500 ms cut off the upstream RTT tail
/// (observed up to 987 ms via the 5 s-timeout GET pool). Bumping the
/// pool ceiling here + applying session-aware per-request overrides lets
/// us catch those responses synchronously instead of triggering orphan
/// reconcile churn.
const FAST_CLIENT_TIMEOUT_CEILING: Duration = Duration::from_millis(2000);
const CANCEL_CLIENT_TIMEOUT_CEILING: Duration = Duration::from_millis(2000);
/// Fallback FAST/CANCEL per-request timeout used before session-aware
/// values have been initialised (i.e. backtest / paper / tests that never
/// call `init_http_timeout`). Matches the historical 500 ms behaviour
/// so anything that hasn't opted in keeps working as before.
const FAST_DEFAULT_TIMEOUT_MS: u64 = 500;
const CANCEL_DEFAULT_TIMEOUT_MS: u64 = 500;
const RECONCILE_TIMEOUT: Duration = Duration::from_millis(2000);
const QUERY_TIMEOUT: Duration = Duration::from_secs(5);

/// Flat per-request timeout for FAST + CANCEL client pools.
///
/// AtomicU64 of milliseconds. `0` = uninitialised → callers use
/// `FAST_DEFAULT_TIMEOUT_MS` / `CANCEL_DEFAULT_TIMEOUT_MS`. Set once at
/// boot via `init_http_timeout`; subsequent reads use Relaxed (the
/// value is a small int updated only at startup).
///
/// History: 2026-05-12 split this into 4 UTC-session buckets after
/// live5/6/7 RTT analysis suggested ~3× variance across sessions.
/// Operational experience showed the simpler flat ceiling produces
/// equivalent results without the configuration sprawl, so the
/// session split was folded back into a single knob.
static HTTP_TIMEOUT_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn load_timeout_or(fallback_ms: u64) -> u64 {
    use std::sync::atomic::Ordering;
    let v = HTTP_TIMEOUT_MS.load(Ordering::Relaxed);
    if v == 0 { fallback_ms } else { v }
}

/// Initialise the flat per-request timeout (in ms) for the FAST +
/// CANCEL client pools. Must be called once at startup, before the
/// first `http_call_async` dispatch, otherwise calls fall back to the
/// `*_DEFAULT_TIMEOUT_MS` constants (500 ms each).
///
/// Idempotent — repeated calls overwrite. The pool clients are built
/// with `FAST_CLIENT_TIMEOUT_CEILING` / `CANCEL_CLIENT_TIMEOUT_CEILING`
/// (2000 ms), so any value ≤ 2000 ms is honoured per-request via
/// `RequestBuilder::timeout`. Values > 2000 ms are clamped to the
/// ceiling and logged.
pub fn init_http_timeout(ms: u64) {
    use std::sync::atomic::Ordering;
    let ceiling = FAST_CLIENT_TIMEOUT_CEILING.as_millis() as u64;
    let clamped = if ms == 0 {
        0
    } else if ms > ceiling {
        log::warn!(
            "[async_rt] http_timeout = {} ms exceeds client ceiling {} ms — clamped",
            ms, ceiling,
        );
        ceiling
    } else {
        ms
    };
    HTTP_TIMEOUT_MS.store(clamped, Ordering::Relaxed);
    log::info!(
        "[async_rt] http_timeout: {} ms (ceiling={}ms, default fallback=500ms)",
        ms, ceiling,
    );
}

/// Per-request FAST (POST /order) timeout. Falls back to
/// `FAST_DEFAULT_TIMEOUT_MS` when `init_http_timeout` hasn't been
/// called.
pub fn current_fast_timeout() -> Duration {
    Duration::from_millis(load_timeout_or(FAST_DEFAULT_TIMEOUT_MS))
}

/// Per-request CANCEL (DELETE /order) timeout. Currently shares the
/// same table as FAST — cancels and places see the same upstream
/// backpressure.
pub fn current_cancel_timeout() -> Duration {
    Duration::from_millis(load_timeout_or(CANCEL_DEFAULT_TIMEOUT_MS))
}

/// Pool of HTTP/2 clients for hot-path order submits (POST /order, /orders).
static HTTP_CLIENTS_FAST: OnceLock<Vec<Arc<reqwest::Client>>> = OnceLock::new();
/// Round-robin counter for `http_client_fast()` picks.
static HTTP_CLIENT_FAST_COUNTER: AtomicUsize = AtomicUsize::new(0);
/// Pool of HTTP/2 clients dedicated to DELETE (cancel) traffic.
static HTTP_CLIENTS_CANCEL: OnceLock<Vec<Arc<reqwest::Client>>> = OnceLock::new();
/// Round-robin counter for `http_client_cancel()` picks.
static HTTP_CLIENT_CANCEL_COUNTER: AtomicUsize = AtomicUsize::new(0);
/// Pool of HTTP/2 clients for orphan reconciliation (GET /data/order/{id},
/// reconcile DELETE retries). Isolated so a slow reconcile can't back up the
/// fast or cancel submit paths.
static HTTP_CLIENTS_RECONCILE: OnceLock<Vec<Arc<reqwest::Client>>> = OnceLock::new();
/// Round-robin counter for `http_client_reconcile()` picks.
static HTTP_CLIENT_RECONCILE_COUNTER: AtomicUsize = AtomicUsize::new(0);
/// Pool of HTTP/2 clients for non-hot-path queries (data-api /positions,
/// /trades gap-fill, gamma-api metadata, wallet relayer, heartbeats).
static HTTP_CLIENTS_QUERY: OnceLock<Vec<Arc<reqwest::Client>>> = OnceLock::new();
/// Round-robin counter for `http_client()` / `http_client_query()` picks.
static HTTP_CLIENT_QUERY_COUNTER: AtomicUsize = AtomicUsize::new(0);
/// Auto-negotiating client (ALPN h2/h1) for endpoints that may not speak
/// HTTP/2 prior-knowledge (e.g. some public Polygon RPCs).
static HTTP_CLIENT_AUTO: OnceLock<Arc<reqwest::Client>> = OnceLock::new();
static HTTP_CLIENT_H1: OnceLock<Arc<reqwest::Client>> = OnceLock::new();

/// Spawn the async runtime on a dedicated OS thread. Idempotent — safe to
/// call multiple times; only the first call has effect. Must be invoked
/// once during process startup (e.g. at the top of `main`).
pub fn init() -> Result<()> {
    if RUNTIME_HANDLE.get().is_some() {
        return Ok(());
    }
    // Spawn the runtime on a dedicated thread. The thread owns the
    // `Runtime` value, which it keeps alive by parking on
    // `runtime.block_on(pending_future)` so the runtime never shuts down
    // until process exit.
    let (handle_tx, handle_rx) = std::sync::mpsc::sync_channel::<Handle>(1);
    std::thread::Builder::new()
        .name("hexbot-async-rt".into())
        .spawn(move || {
            // Pin this thread to the async-RT core and raise to SCHED_FIFO.
            // Must happen BEFORE building the tokio runtime so the runtime's
            // internal timer thread / blocking pool inherit reasonable
            // defaults (they stay SCHED_OTHER; this thread drives the
            // current_thread scheduler which hosts all hot-path futures).
            crate::os_tune::pin_async_rt("hexbot-async-rt");
            let rt = match Builder::new_current_thread()
                .enable_all()
                .thread_name("hexbot-async-rt")
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("Failed to build tokio runtime: {}", e);
                    return;
                }
            };
            let handle = rt.handle().clone();
            let _ = handle_tx.send(handle);
            // Park the runtime on a future that never resolves — keeps
            // the runtime alive for the rest of the process.
            rt.block_on(futures_util::future::pending::<()>());
        })
        .context("spawn async runtime thread")?;

    let handle = handle_rx.recv().context("receive runtime handle")?;

    // Build each role's client pool — each client keeps its own connection
    // pool (and thus its own h2 TCP connection per host) so that TCP-level
    // HOL on one connection doesn't stall concurrent requests dispatched
    // through another, AND so that e.g. a slow /positions query can't
    // freeze the POST /order path.
    // FAST and CANCEL pools are built with a HIGH ceiling (2 s) so
    // per-request `current_fast_timeout()` / `current_cancel_timeout()`
    // can override DOWN to the session-of-day value. The client-level
    // timeout only fires if no per-request override is applied (e.g.
    // backtest / paper paths that don't call `init_http_timeout`),
    // in which case we want a reasonable upper bound rather than a
    // permanent stall.
    let mut fast_clients = Vec::with_capacity(FAST_CLIENT_POOL_SIZE);
    for _ in 0..FAST_CLIENT_POOL_SIZE {
        fast_clients.push(Arc::new(build_http_client(FAST_CLIENT_TIMEOUT_CEILING)?));
    }
    HTTP_CLIENTS_FAST.set(fast_clients)
        .map_err(|_| anyhow!("HTTP fast clients already initialised"))?;

    let mut cancel_clients = Vec::with_capacity(CANCEL_CLIENT_POOL_SIZE);
    for _ in 0..CANCEL_CLIENT_POOL_SIZE {
        cancel_clients.push(Arc::new(build_http_client(CANCEL_CLIENT_TIMEOUT_CEILING)?));
    }
    HTTP_CLIENTS_CANCEL.set(cancel_clients)
        .map_err(|_| anyhow!("HTTP cancel clients already initialised"))?;

    let mut reconcile_clients = Vec::with_capacity(RECONCILE_CLIENT_POOL_SIZE);
    for _ in 0..RECONCILE_CLIENT_POOL_SIZE {
        reconcile_clients.push(Arc::new(build_http_client(RECONCILE_TIMEOUT)?));
    }
    HTTP_CLIENTS_RECONCILE.set(reconcile_clients)
        .map_err(|_| anyhow!("HTTP reconcile clients already initialised"))?;

    let mut query_clients = Vec::with_capacity(QUERY_CLIENT_POOL_SIZE);
    for _ in 0..QUERY_CLIENT_POOL_SIZE {
        query_clients.push(Arc::new(build_http_client(QUERY_TIMEOUT)?));
    }
    HTTP_CLIENTS_QUERY.set(query_clients)
        .map_err(|_| anyhow!("HTTP query clients already initialised"))?;

    HTTP_CLIENT_AUTO.set(Arc::new(build_http_client_auto()?))
        .map_err(|_| anyhow!("auto HTTP client already initialised"))?;
    HTTP_CLIENT_H1.set(Arc::new(build_http_client_h1()?))
        .map_err(|_| anyhow!("h1 HTTP client already initialised"))?;
    RUNTIME_HANDLE.set(handle)
        .map_err(|_| anyhow!("runtime handle already initialised"))?;
    Ok(())
}

/// Get the runtime handle. Panics if `init()` hasn't been called.
pub fn handle() -> &'static Handle {
    RUNTIME_HANDLE.get().expect("async_rt::init() not called")
}

/// Get one of the fast (hot-path submit) HTTP/2 clients, round-robin across
/// the pool. Use for POST /order and POST /orders where a stale response
/// past 500 ms is actively harmful (a live quote racing the market).
pub fn http_client_fast() -> Arc<reqwest::Client> {
    let clients = HTTP_CLIENTS_FAST.get().expect("async_rt::init() not called");
    let i = HTTP_CLIENT_FAST_COUNTER.fetch_add(1, Ordering::Relaxed) % clients.len();
    clients[i].clone()
}

/// Get one of the dedicated cancel HTTP/2 clients, round-robin across the
/// cancel pool. Use this for `DELETE` requests so cancel traffic can't
/// share stream credits / flow-control windows with placements.
pub fn http_client_cancel() -> Arc<reqwest::Client> {
    let clients = HTTP_CLIENTS_CANCEL.get().expect("async_rt::init() not called");
    let i = HTTP_CLIENT_CANCEL_COUNTER.fetch_add(1, Ordering::Relaxed) % clients.len();
    clients[i].clone()
}

/// Get one of the reconcile HTTP/2 clients. Used by the orphan reconciler
/// — GET /data/order/{id} and DELETE /order retries on timed-out cancels.
/// Separate pool so a slow reconcile can't back-pressure the fast submit
/// path via shared h2 stream credits.
pub fn http_client_reconcile() -> Arc<reqwest::Client> {
    let clients = HTTP_CLIENTS_RECONCILE.get().expect("async_rt::init() not called");
    let i = HTTP_CLIENT_RECONCILE_COUNTER.fetch_add(1, Ordering::Relaxed) % clients.len();
    clients[i].clone()
}

/// Get one of the query HTTP/2 clients. Default client for non-hot-path
/// callers — data-api snapshots, /positions, /trades gap-fill, wallet
/// relayer, heartbeats, generic GETs. 5 s timeout tolerates larger
/// responses.
pub fn http_client_query() -> Arc<reqwest::Client> {
    let clients = HTTP_CLIENTS_QUERY.get().expect("async_rt::init() not called");
    let i = HTTP_CLIENT_QUERY_COUNTER.fetch_add(1, Ordering::Relaxed) % clients.len();
    clients[i].clone()
}

/// Backwards-compatible alias for `http_client_query()`. Legacy callers
/// (position.rs, wallet.rs, user_feed.rs gap-fill, `blocking_get_text`)
/// all do non-hot-path queries that want the 5 s budget; the four new
/// role getters are preferred in new code.
pub fn http_client() -> Arc<reqwest::Client> {
    http_client_query()
}

/// All HTTP/2 clients across every role as an owned Vec. Intended for
/// prewarm paths that need to establish h2 + TLS state on *every* client,
/// not just one.
pub fn http_clients_all() -> Vec<Arc<reqwest::Client>> {
    let mut all = HTTP_CLIENTS_FAST.get().expect("async_rt::init() not called").clone();
    all.extend(HTTP_CLIENTS_CANCEL.get().expect("async_rt::init() not called").iter().cloned());
    all.extend(HTTP_CLIENTS_RECONCILE.get().expect("async_rt::init() not called").iter().cloned());
    all.extend(HTTP_CLIENTS_QUERY.get().expect("async_rt::init() not called").iter().cloned());
    all
}

/// Get the auto-negotiating (ALPN) HTTP client. Use for endpoints that may
/// not speak HTTP/2 prior-knowledge (public Polygon RPCs, etc).
pub fn http_client_auto() -> Arc<reqwest::Client> {
    HTTP_CLIENT_AUTO.get().expect("async_rt::init() not called").clone()
}

/// Get the HTTP/1.1-only client. Use for endpoints whose HTTP/2 frontend is
/// broken server-side: Aster's `fapi` returns spurious `-2019 Margin is
/// insufficient` for signed orders sent over h2 while the byte-identical
/// request succeeds over h1.1 (verified with curl --http1.1 vs --http2,
/// 2026-07-05). ALPN would happily negotiate h2 there, so this client
/// disables it outright.
pub fn http_client_h1() -> Arc<reqwest::Client> {
    HTTP_CLIENT_H1.get().expect("async_rt::init() not called").clone()
}

/// Sync convenience: GET `url` as text via the shared h2 client and runtime.
/// Intended for lightweight, infrequent REST fetches (event metadata, series
/// resolution, etc). Hot paths should use `http_client()` + `block_on_runtime`
/// directly so they can batch work.
pub fn blocking_get_text(url: &str) -> Result<String> {
    let url = url.to_string();
    let client = http_client_query();
    block_on_runtime(async move {
        let resp = client.get(&url).send().await
            .map_err(|e| anyhow!("GET {} failed: {}", url, e))?;
        let status = resp.status();
        let body = resp.text().await
            .map_err(|e| anyhow!("GET {} read body: {}", url, e))?;
        if !status.is_success() {
            return Err(anyhow!("GET {} returned {}", url, status));
        }
        Ok(body)
    })
}

/// Same as `blocking_get_text` but retries on transient failures with
/// exponential backoff. Retries on:
///   * network / connection errors (couldn't even reach the server)
///   * HTTP 408 / 425 / 429 / 5xx responses
///   * body-read errors mid-response
/// Does NOT retry 4xx client errors other than 408/425/429 — those
/// indicate a malformed request that re-issuing won't fix.
///
/// Backoff schedule: `base_backoff_ms × 2^attempt` for attempts 0..N-1
/// (capped at 30 s per sleep). With defaults `attempts=5,
/// base_backoff_ms=200` total wait before giving up is roughly
/// 200 + 400 + 800 + 1600 + 3200 ≈ 6.2 s — long enough to ride out
/// brief gamma-api 500s without permanently stalling the caller.
pub fn blocking_get_text_retry(
    url: &str,
    attempts: u32,
    base_backoff_ms: u64,
) -> Result<String> {
    let url = url.to_string();
    let client = http_client_query();
    let attempts = attempts.max(1);
    block_on_runtime(async move {
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
                                // body-read error is transient — fall through to retry
                            }
                        }
                    } else if is_retriable_status(status) && i + 1 < attempts {
                        last_err = Some(anyhow!("GET {} returned {}", url, status));
                        // fall through to backoff
                    } else {
                        // non-retriable status, OR last attempt — return now
                        return Err(anyhow!("GET {} returned {}", url, status));
                    }
                }
                Err(e) => {
                    last_err = Some(anyhow!("GET {} failed: {}", url, e));
                    // Network / connect error is always retriable until we
                    // run out of attempts.
                }
            }
            // Backoff before next attempt (skip after final attempt).
            if i + 1 < attempts {
                let exp = i.min(7); // cap shift so we don't overflow
                let delay_ms = base_backoff_ms.saturating_mul(1u64 << exp).min(30_000);
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("GET {} failed after {} attempts", url, attempts)))
    })
}

/// HTTP status codes that warrant a retry. 408 (Request Timeout), 425
/// (Too Early), 429 (Too Many Requests), and any 5xx are considered
/// transient — the same request issued a moment later may succeed.
/// Other 4xx codes signal a permanent client problem and should NOT
/// be retried.
fn is_retriable_status(status: reqwest::StatusCode) -> bool {
    match status.as_u16() {
        408 | 425 | 429 => true,
        s if (500..600).contains(&s) => true,
        _ => false,
    }
}

/// Bridge sync → async: run the future on the runtime and block the
/// calling thread until it resolves. Uses a `oneshot` channel to ferry
/// the result off the runtime thread, so the calling thread doesn't
/// consume runtime scheduler cycles.
pub fn block_on_runtime<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = oneshot::channel::<T>();
    handle().spawn(async move {
        let val = fut.await;
        let _ = tx.send(val);
    });
    // Block on the oneshot. If the runtime is down (shutdown), rx errors
    // and we panic — callers catch or propagate.
    rx.blocking_recv().expect("async task dropped before sending")
}

/// Build an HTTP/2 client tuned for CLOB / data-api / gamma-api.
///
/// The per-request `timeout` is the only knob that varies across roles;
/// everything else (TLS, h2 keepalives, connection pool) is identical so
/// each role's TCP connections behave the same once warm.
///
/// Config rationale:
///   * `http2_prior_knowledge()` — all Polymarket hosts support h2;
///     skipping ALPN saves one RTT on first connect.
///   * `http2_adaptive_window(true)` — hyper dynamically enlarges the
///     stream / connection receive windows under load. Without this,
///     larger responses (open-order snapshots, book replays) can stall
///     on flow control even when bandwidth is free.
///   * `http2_keep_alive_interval(5s)` — PINGs every 5 s so a dead peer
///     is noticed within the PING-ACK timeout instead of waiting for
///     the next real request to time out.
///   * `http2_keep_alive_timeout(5s)` — PING-ACK deadline; exceeding it
///     kills the connection so the pool reconnects rather than stalling
///     on a silently-dropped session.
///   * `http2_keep_alive_while_idle(true)` — PINGs even when no streams
///     are open, so the pool doesn't let an idle h2 connection rot.
///   * `pool_idle_timeout(300s)` + `pool_max_idle_per_host(4)` — reqwest
///     evicts a connection from the pool after this many seconds of no
///     in-flight requests. h2 PINGs at the protocol layer keep the
///     socket alive but do NOT reset the pool's idle timer, so during
///     quiet periods a connection can be alive at the wire but evicted
///     at the pool. 5 min is comfortably longer than any plausible
///     trading-quiet stretch within an event; combined with the
///     heartbeat loop fanning out across every pool every 10 s, no
///     client should ever pay handshake cost on a real order.
///   * `tcp_nodelay(true)` — disable Nagle; we send small frames and
///     want them out immediately.
///   * `tcp_keepalive(30s)` — NAT / LB friendly.
///   * `timeout` — per-role; callers pass the deadline that matches the
///     endpoint's latency budget (fast=500ms, cancel=500ms,
///     reconcile=1000ms, query=5000ms).
///   * `connect_timeout(500ms)` — handshake must complete promptly; we
///     prewarm at startup so this is only hit on reconnect / dead pool.
fn build_http_client(timeout: Duration) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .http2_prior_knowledge()
        .http2_adaptive_window(true)
        .http2_keep_alive_interval(Duration::from_secs(5))
        .http2_keep_alive_timeout(Duration::from_secs(5))
        .http2_keep_alive_while_idle(true)
        .pool_idle_timeout(Duration::from_secs(300))
        .pool_max_idle_per_host(4)
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true)
        .timeout(timeout)
        .connect_timeout(Duration::from_millis(500))
        .build()
        .context("build reqwest client")
}

/// Build an ALPN-negotiating HTTP client (h2 or h1 per server support).
///
/// Used for Polygon RPC endpoints — many public RPCs still only speak HTTP/1.1,
/// so `http2_prior_knowledge` would cause an immediate protocol error. The pool
/// / timeout config mirrors the primary client so we still get connection reuse
/// and TLS session caching.
/// Build an HTTP/1.1-only client (h2 disabled even if the server offers it
/// via ALPN). See `http_client_h1` for why Aster needs this.
fn build_http_client_h1() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .http1_only()
        .pool_idle_timeout(Duration::from_secs(300))
        .pool_max_idle_per_host(4)
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true)
        .timeout(Duration::from_secs(5))
        .connect_timeout(Duration::from_millis(800))
        .build()
        .context("build h1 reqwest client")
}

fn build_http_client_auto() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(300))
        .pool_max_idle_per_host(4)
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true)
        .timeout(Duration::from_secs(5))
        .connect_timeout(Duration::from_millis(800))
        .build()
        .context("build auto reqwest client")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `init_http_timeout` must clamp values above the FAST client
    /// pool ceiling so a misconfiguration can't silently exceed what the
    /// pool can actually honour. `0` means "use default" and should
    /// bypass clamping. Idempotent: latest call wins.
    #[test]
    fn init_http_timeout_clamps_above_ceiling() {
        use std::sync::atomic::Ordering;
        let ceiling = FAST_CLIENT_TIMEOUT_CEILING.as_millis() as u64;

        // Overlong value gets clamped to the ceiling.
        init_http_timeout(999_999);
        assert_eq!(
            HTTP_TIMEOUT_MS.load(Ordering::Relaxed),
            ceiling,
            "999_999 must be clamped to FAST_CLIENT_TIMEOUT_CEILING={}", ceiling,
        );

        // Normal values pass through.
        init_http_timeout(1000);
        assert_eq!(HTTP_TIMEOUT_MS.load(Ordering::Relaxed), 1000);

        // 0 means "use default fallback" — stored as 0, fallback
        // applied at lookup time. Doesn't get clamped to ceiling.
        init_http_timeout(0);
        assert_eq!(HTTP_TIMEOUT_MS.load(Ordering::Relaxed), 0);
    }

    /// `load_timeout_or` fallback: a zero-stored value means
    /// "http_timeout not yet initialised" and the helper must
    /// substitute the supplied default. Non-zero stored values pass
    /// through.
    #[test]
    fn http_timeout_load_falls_back_when_zero() {
        use std::sync::atomic::Ordering;
        // Reset to 0 so the test sees the uninitialised state regardless
        // of test ordering.
        HTTP_TIMEOUT_MS.store(0, Ordering::Relaxed);
        assert_eq!(load_timeout_or(500), 500, "0 must fall back to default");
        HTTP_TIMEOUT_MS.store(750, Ordering::Relaxed);
        assert_eq!(load_timeout_or(500), 750, "non-zero must pass through");
        HTTP_TIMEOUT_MS.store(2000, Ordering::Relaxed);
        assert_eq!(load_timeout_or(500), 2000, "non-zero (at ceiling) must pass through");
    }
}

