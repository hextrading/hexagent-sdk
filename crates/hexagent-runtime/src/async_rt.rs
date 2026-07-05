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
//!   - Four role-specific HTTP/1.1 client pools (see [`crate::http1_pool`]),
//!     each with its own per-request deadline tuned to the endpoint it
//!     serves. h1.1 has no multiplexing, so role isolation — a slow query
//!     can never occupy the connection an order needs — comes from the
//!     pools being disjoint sets of TCP connections. (Formerly h2
//!     prior-knowledge pools; Aster's h2 frontend rejects signed requests
//!     with spurious -2019s, and splitting transports per venue buys
//!     nothing, so all REST pools are h1.1 now.)
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
// Pool sizes now live in `crate::http1_pool` (config/instance-scaled).

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
/// Fallback FAST/CANCEL per-request timeout used before session-aware
/// values have been initialised (i.e. backtest / paper / tests that never
/// call `init_http_timeout`). Matches the historical 500 ms behaviour
/// so anything that hasn't opted in keeps working as before.
const FAST_DEFAULT_TIMEOUT_MS: u64 = 500;
const CANCEL_DEFAULT_TIMEOUT_MS: u64 = 500;

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

    // Role-separated HTTP/1.1 client pools — construction, sizing (config /
    // instance-count scaled via `http1_pool::set_sizes`), round-robin and
    // keep-warm all live in `crate::http1_pool`; the getters below delegate.
    // FAST and CANCEL pools keep a HIGH client-level ceiling (2 s) so
    // per-request `current_fast_timeout()` / `current_cancel_timeout()`
    // can override DOWN to the configured value.
    crate::http1_pool::init_pools()?;

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
    crate::http1_pool::client(crate::http1_pool::Role::Fast)
}

/// Get one of the dedicated cancel HTTP/2 clients, round-robin across the
/// cancel pool. Use this for `DELETE` requests so cancel traffic can't
/// share stream credits / flow-control windows with placements.
pub fn http_client_cancel() -> Arc<reqwest::Client> {
    crate::http1_pool::client(crate::http1_pool::Role::Cancel)
}

/// Get one of the reconcile HTTP/2 clients. Used by the orphan reconciler
/// — GET /data/order/{id} and DELETE /order retries on timed-out cancels.
/// Separate pool so a slow reconcile can't back-pressure the fast submit
/// path via shared h2 stream credits.
pub fn http_client_reconcile() -> Arc<reqwest::Client> {
    crate::http1_pool::client(crate::http1_pool::Role::Reconcile)
}

/// Get one of the query HTTP/2 clients. Default client for non-hot-path
/// callers — data-api snapshots, /positions, /trades gap-fill, wallet
/// relayer, heartbeats, generic GETs. 5 s timeout tolerates larger
/// responses.
pub fn http_client_query() -> Arc<reqwest::Client> {
    crate::http1_pool::client(crate::http1_pool::Role::Query)
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
    crate::http1_pool::clients_all()
}

/// Get the HTTP/1.1-only client. Use for endpoints whose HTTP/2 frontend is
/// broken server-side: Aster's `fapi` returns spurious `-2019 Margin is
/// insufficient` for signed orders sent over h2 while the byte-identical
/// request succeeds over h1.1 (verified with curl --http1.1 vs --http2,
/// 2026-07-05). ALPN would happily negotiate h2 there, so this client
/// disables it outright.
pub fn http_client_h1() -> Arc<reqwest::Client> {
    crate::http1_pool::client(crate::http1_pool::Role::Query)
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

