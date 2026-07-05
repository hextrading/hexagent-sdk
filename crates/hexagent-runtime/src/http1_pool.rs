//! Role-separated **HTTP/1.1** client pools with prewarm + keep-warm.
//!
//! Shared transport layer for every REST venue (Polymarket CLOB, Aster
//! fapi, …). Replaces the former HTTP/2 prior-knowledge pools in
//! `async_rt` — Aster's h2 frontend is outright broken for signed
//! requests (spurious `-2019`), and running one venue on h1.1 and
//! another on h2 splits the transport story for no benefit, so both
//! venues now share this module (the ALPN `http_client_auto` remains
//! for Hyperliquid / Polygon RPC).
//!
//! ## Model
//!
//! HTTP/1.1 has **no multiplexing**: one connection carries exactly one
//! in-flight request. Concurrency therefore equals warm-connection
//! count, which this module makes explicit:
//!
//! * Each role (`Fast` / `Cancel` / `Reconcile` / `Query`) owns a pool
//!   of N independent `reqwest::Client`s. Each client keeps its own
//!   small connection pool per host, so N clients ≈ N warm connections
//!   per host per role.
//! * Round-robin dispatch spreads a burst (e.g. a two-leg replace: two
//!   places + cancels) across distinct clients → distinct TCP
//!   connections → no head-of-line queueing.
//! * Role isolation means a slow query can never occupy the connection
//!   an order placement needs — on h1.1 this is *the* isolation
//!   mechanism, stream credits don't exist.
//!
//! ## Sizing
//!
//! Warm concurrent capacity must cover the peak simultaneous request
//! burst. With M strategy instances quoting two legs each, a
//! synchronized replace wave is ~2·M places and ~2·M cancels, so
//! [`PoolSizes::for_instances`] scales FAST/CANCEL as `2·M` (clamped to
//! [2, 16]). Call [`set_sizes`] **before** `async_rt::init()`; later
//! calls are ignored (pools are built once). `HEXBOT_HTTP_POOL_FAST` /
//! `_CANCEL` / `_RECONCILE` / `_QUERY` env vars override individually.
//!
//! ## Warmth
//!
//! * **Prewarm**: [`spawn_keep_warm`] fires an immediate warm round so
//!   TCP+TLS handshakes complete before the first real order.
//! * **Keep-warm**: the same task then re-warms every client for its
//!   host on an interval, covering quiet stretches (feed gates, closed
//!   sessions) where business traffic alone would let connections go
//!   idle/evicted. Polymarket's signed `/heartbeats` loop (which also
//!   keeps the API key active) already fans out across
//!   [`clients_all`], so it doubles as this venue's keep-warm — only
//!   venues without such a loop (Aster) need `spawn_keep_warm`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};

// ── Per-role client-level timeout ceilings ────────────────────────
// FAST/CANCEL are ceilings only: the per-request deadline is chosen by
// `async_rt::current_fast_timeout()` / `current_cancel_timeout()` and is
// always ≤ these values. RECONCILE/QUERY use the client-level value as-is.
const FAST_TIMEOUT_CEILING: Duration = Duration::from_millis(2000);
const CANCEL_TIMEOUT_CEILING: Duration = Duration::from_millis(2000);
const RECONCILE_TIMEOUT: Duration = Duration::from_millis(2000);
const QUERY_TIMEOUT: Duration = Duration::from_secs(5);

/// Request roles, mirroring the former `async_rt` pool split.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    /// POST /order, /orders — hot-path placements.
    Fast,
    /// DELETE /order, /orders, /cancel-all.
    Cancel,
    /// Orphan reconciliation (GET order state + DELETE retries).
    Reconcile,
    /// Everything else: snapshots, metadata, listenKey, heartbeats.
    Query,
}

/// Number of clients (≈ warm connections per host) per role.
#[derive(Clone, Copy, Debug)]
pub struct PoolSizes {
    pub fast: usize,
    pub cancel: usize,
    pub reconcile: usize,
    pub query: usize,
}

impl Default for PoolSizes {
    fn default() -> Self {
        Self { fast: 2, cancel: 2, reconcile: 2, query: 2 }
    }
}

impl PoolSizes {
    /// Scale for `n` concurrently-quoting strategy instances: a
    /// synchronized two-leg replace wave needs ~2·n placements and ~2·n
    /// cancels in flight at once. Clamped to [2, 16] per role — beyond
    /// that, rarely-rotated clients would sit cold between picks.
    pub fn for_instances(n: usize) -> Self {
        let hot = (2 * n.max(1)).clamp(2, 16);
        Self {
            fast: hot,
            cancel: hot,
            reconcile: n.clamp(2, 8),
            query: n.clamp(2, 8),
        }
    }

    /// Apply `HEXBOT_HTTP_POOL_*` env overrides (each var optional).
    pub fn with_env_overrides(mut self) -> Self {
        fn ov(name: &str, cur: usize) -> usize {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|n| (1..=64).contains(n))
                .unwrap_or(cur)
        }
        self.fast = ov("HEXBOT_HTTP_POOL_FAST", self.fast);
        self.cancel = ov("HEXBOT_HTTP_POOL_CANCEL", self.cancel);
        self.reconcile = ov("HEXBOT_HTTP_POOL_RECONCILE", self.reconcile);
        self.query = ov("HEXBOT_HTTP_POOL_QUERY", self.query);
        self
    }
}

static SIZES: OnceLock<PoolSizes> = OnceLock::new();

/// Set pool sizes. Must be called **before** `async_rt::init()` builds the
/// pools; later calls are ignored (first write wins). Returns whether the
/// value was applied.
pub fn set_sizes(sizes: PoolSizes) -> bool {
    SIZES.set(sizes.with_env_overrides()).is_ok()
}

fn sizes() -> PoolSizes {
    *SIZES.get_or_init(|| PoolSizes::default().with_env_overrides())
}

// ── Pools ─────────────────────────────────────────────────────────

struct Pools {
    fast: Vec<Arc<reqwest::Client>>,
    cancel: Vec<Arc<reqwest::Client>>,
    reconcile: Vec<Arc<reqwest::Client>>,
    query: Vec<Arc<reqwest::Client>>,
}

static POOLS: OnceLock<Pools> = OnceLock::new();
static RR_FAST: AtomicUsize = AtomicUsize::new(0);
static RR_CANCEL: AtomicUsize = AtomicUsize::new(0);
static RR_RECONCILE: AtomicUsize = AtomicUsize::new(0);
static RR_QUERY: AtomicUsize = AtomicUsize::new(0);

/// Build an HTTP/1.1-only client. `pool_max_idle_per_host = 2` keeps the
/// client's primary connection plus one burst spare; the long
/// `pool_idle_timeout` is a backstop — keep-warm traffic normally touches
/// every client well inside it.
fn build_h1_client(timeout: Duration) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .http1_only()
        .pool_idle_timeout(Duration::from_secs(300))
        .pool_max_idle_per_host(2)
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true)
        .timeout(timeout)
        // Covers DNS + TCP + TLS. Cold-start measurements against Aster
        // showed DNS ~650 ms + TLS ~950 ms under startup CPU load — an
        // 800 ms budget timed the very first requests out (positionRisk /
        // exchangeInfo before any prewarm had run). Steady-state requests
        // never pay this (prewarm + keep-warm hold connections open); the
        // generous budget only applies to genuine reconnects.
        .connect_timeout(Duration::from_millis(2000))
        .build()
        .context("build h1 reqwest client")
}

fn build_role(n: usize, timeout: Duration) -> Result<Vec<Arc<reqwest::Client>>> {
    let mut v = Vec::with_capacity(n);
    for _ in 0..n {
        v.push(Arc::new(build_h1_client(timeout)?));
    }
    Ok(v)
}

/// Build all pools. Called once from `async_rt::init()`.
pub(crate) fn init_pools() -> Result<()> {
    let s = sizes();
    let pools = Pools {
        fast: build_role(s.fast, FAST_TIMEOUT_CEILING)?,
        cancel: build_role(s.cancel, CANCEL_TIMEOUT_CEILING)?,
        reconcile: build_role(s.reconcile, RECONCILE_TIMEOUT)?,
        query: build_role(s.query, QUERY_TIMEOUT)?,
    };
    POOLS
        .set(pools)
        .map_err(|_| anyhow::anyhow!("http1_pool already initialised"))?;
    log::info!(
        "[http1_pool] initialised (h1.1) fast={} cancel={} reconcile={} query={}",
        s.fast, s.cancel, s.reconcile, s.query,
    );
    Ok(())
}

fn pools() -> &'static Pools {
    POOLS.get().expect("async_rt::init() not called")
}

/// Round-robin client for `role`.
pub fn client(role: Role) -> Arc<reqwest::Client> {
    let p = pools();
    let (list, ctr) = match role {
        Role::Fast => (&p.fast, &RR_FAST),
        Role::Cancel => (&p.cancel, &RR_CANCEL),
        Role::Reconcile => (&p.reconcile, &RR_RECONCILE),
        Role::Query => (&p.query, &RR_QUERY),
    };
    let i = ctr.fetch_add(1, Ordering::Relaxed) % list.len();
    list[i].clone()
}

/// All clients across every role — for prewarm / keep-warm fan-out that
/// must touch *every* underlying connection, not just one pick.
pub fn clients_all() -> Vec<Arc<reqwest::Client>> {
    let p = pools();
    let mut all = p.fast.clone();
    all.extend(p.cancel.iter().cloned());
    all.extend(p.reconcile.iter().cloned());
    all.extend(p.query.iter().cloned());
    all
}

// ── Prewarm + keep-warm ───────────────────────────────────────────

/// Spawn a warm task for `host`: an **immediate** round (prewarm — TCP+TLS
/// up before the first real request) and then one cheap `GET warm_url` per
/// client every `interval`, so every connection stays established through
/// quiet stretches. `warm_url` should be a free, unauthenticated endpoint
/// (e.g. Aster's `/fapi/v3/time`).
///
/// Venues with their own signed heartbeat fan-out (Polymarket) don't need
/// this. One task per host — repeated calls for the same label are refused.
pub fn spawn_keep_warm(label: &'static str, warm_url: String, interval: Duration) {
    use std::sync::Mutex;
    static SPAWNED: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());
    {
        let mut spawned = SPAWNED.lock().unwrap();
        if spawned.contains(&label) {
            return;
        }
        spawned.push(label);
    }
    crate::async_rt::handle().spawn(async move {
        let mut first = true;
        loop {
            if !first {
                tokio::time::sleep(interval).await;
            }
            let t0 = std::time::Instant::now();
            let clients = clients_all();
            let n = clients.len();
            let mut set = tokio::task::JoinSet::new();
            for c in clients {
                let url = warm_url.clone();
                set.spawn(async move { c.get(&url).send().await.map(|r| r.status().as_u16()) });
            }
            let mut ok = 0usize;
            let mut first_err: Option<String> = None;
            while let Some(res) = set.join_next().await {
                match res {
                    Ok(Ok(status)) if (200..400).contains(&status) => ok += 1,
                    Ok(Ok(status)) => {
                        if first_err.is_none() { first_err = Some(format!("HTTP {}", status)); }
                    }
                    Ok(Err(e)) => {
                        if first_err.is_none() { first_err = Some(e.to_string()); }
                    }
                    Err(e) => {
                        if first_err.is_none() { first_err = Some(e.to_string()); }
                    }
                }
            }
            if first {
                log::info!(
                    "[http1_pool] {} prewarm: {}/{} connections up in {:.0}ms{}",
                    label, ok, n, t0.elapsed().as_secs_f64() * 1000.0,
                    first_err.as_deref().map(|e| format!(" (first err: {})", e)).unwrap_or_default(),
                );
                first = false;
            } else if ok < n {
                log::warn!(
                    "[http1_pool] {} keep-warm: {}/{} ok ({})",
                    label, ok, n, first_err.as_deref().unwrap_or("?"),
                );
            } else {
                log::trace!("[http1_pool] {} keep-warm: {}/{} ok", label, ok, n);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_scale_with_instances() {
        let s1 = PoolSizes::for_instances(1);
        assert_eq!((s1.fast, s1.cancel), (2, 2));
        let s3 = PoolSizes::for_instances(3);
        assert_eq!((s3.fast, s3.cancel), (6, 6)); // 2 legs × 3 instances
        let s20 = PoolSizes::for_instances(20);
        assert_eq!(s20.fast, 16); // clamped
        assert_eq!(s20.reconcile, 8);
    }

    #[test]
    fn default_sizes_are_two() {
        let d = PoolSizes::default();
        assert_eq!((d.fast, d.cancel, d.reconcile, d.query), (2, 2, 2, 2));
    }
}
