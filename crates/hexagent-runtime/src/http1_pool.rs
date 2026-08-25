//! Role-separated **HTTP/1.1** client pools with prewarm + keep-warm.
//!
//! Shared transport layer for **all** REST traffic (Polymarket CLOB,
//! Aster fapi, Hyperliquid, Lighter, Hexmarket, Chainlink, …).
//! Replaces the former HTTP/2 prior-knowledge pools and the ALPN
//! auto-negotiating client in `async_rt` — Aster's h2 frontend is
//! outright broken for signed requests (spurious `-2019`), h2 buys
//! nothing over per-role h1.1 pools, and ALPN could silently negotiate
//! h2 wherever a server offers it. HTTP/2 is gone from the codebase;
//! endpoints needing bespoke timeouts (public Polygon RPCs) build a
//! standalone h1.1 client via [`build_client`].
//!
//! ## Model
//!
//! HTTP/1.1 has **no multiplexing**: one connection carries exactly one
//! in-flight request. Concurrency therefore equals warm-connection
//! count, which this module makes explicit:
//!
//! * Two fixed process-global fallback pools: **fallback-order** (4 slots)
//!   and **query** (4 slots).
//! * One pool group per Polymarket account. If N strategy instances share an
//!   account, that group owns **4·N placement** slots, **4·N cancellation**
//!   slots, **2·N reconcile** slots, and **2 gap-replay** slots. Instance IDs are mapped to
//!   their account before admission, so shared-wallet instances borrow the
//!   same warm capacity without duplicating physical pools.
//! * Round-robin dispatch spreads a burst (e.g. a two-leg replace: two
//!   places + cancels) across distinct clients → distinct TCP
//!   connections → no head-of-line queueing.
//! * Pool isolation still guarantees a slow query/replay can never
//!   occupy a connection an order op needs — on h1.1 this is *the*
//!   isolation mechanism, stream credits don't exist. Placement and cancel
//!   therefore also have independent physical pools: slow cancel finality can
//!   never consume a warm connection reserved for a fresh quote.
//!
//! ## Sizing
//!
//! Warm concurrent capacity must cover the peak simultaneous request burst.
//! Account groups scale directly from their registered instance count; global
//! fallback/query capacity never scales with strategy count. Call
//! [`set_global_sizes`] **before** `async_rt::init()` to override the fixed
//! defaults. `HEXBOT_HTTP_POOL_FALLBACK_ORDER` and
//! `HEXBOT_HTTP_POOL_QUERY` override the two global pools individually.
//!
//! ## Warmth
//!
//! * **Prewarm**: venue startup explicitly establishes TCP+TLS state before
//!   the first real order.
//! * **Keep-warm**: an activity-aware scheduler revisits one eligible slot at
//!   a time, spreading one full sweep over the configured interval. A slot is
//!   eligible only after 30 s without any business or keep-warm request, and
//!   every probe owns the slot's admission permit. Per-pool and process-wide
//!   guards cap probes at one and two respectively. Both live venues spawn it:
//!   Polymarket against
//!   `{clob}/time` (engine, after the SharedStates are built) and Aster
//!   against `/fapi/v3/time`. (Polymarket's `/heartbeats` loop does NOT
//!   fan out — it sends one request per 10 s on a single Query slot, so
//!   it keeps the API key active but not the pool warm.)
//! * **Repair**: business requests and keep-warm probes report transport
//!   outcomes through [`PooledClient`]. Two consecutive failures quarantine
//!   the exact slot, build a fresh client in the background, prewarm it, and
//!   only then atomically return it to admission.

use arc_swap::ArcSwap;
use crossbeam_channel::{Receiver, Sender};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
#[cfg(test)]
use std::sync::Mutex;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

// ── Per-pool client-level timeout ceilings ────────────────────────
// ORDER (Fast and Cancel use separate account pools) is a ceiling only: the per-request deadline
// is chosen by `async_rt::current_fast_timeout()` /
// `current_cancel_timeout()` and is always ≤ this value. QUERY and the
// account reconcile pools use the larger 5 s client ceiling; reconcile's
// historical 2 s deadline is preserved per-request (`GET /data/order/…`
// override in the trade dispatch).
const ORDER_TIMEOUT_CEILING: Duration = Duration::from_millis(2000);
const QUERY_TIMEOUT_CEILING: Duration = Duration::from_secs(5);
const GAP_REPLAY_TIMEOUT: Duration = Duration::from_secs(5);
const KEEP_WARM_IDLE: Duration = Duration::from_secs(30);
const KEEP_WARM_TIMEOUT: Duration = Duration::from_millis(500);
const KEEP_WARM_GLOBAL_LIMIT: usize = 2;

static ACTIVITY_EPOCH: OnceLock<Instant> = OnceLock::new();
static KEEP_WARM_INFLIGHT: AtomicUsize = AtomicUsize::new(0);

fn activity_now_ns() -> u64 {
    // The positive base keeps unit tests able to install an artificial
    // "older than 30 s" timestamp without waiting for the process uptime to
    // cross that boundary. Only differences are observed in production.
    let elapsed = ACTIVITY_EPOCH
        .get_or_init(Instant::now)
        .elapsed()
        .as_nanos();
    let base = KEEP_WARM_IDLE.as_nanos();
    base.saturating_add(elapsed).min(u64::MAX as u128) as u64
}

fn keep_warm_tick(full_sweep: Duration, n_slots: usize) -> Duration {
    let n_slots = n_slots.max(1) as u128;
    let nanos = (full_sweep.as_nanos() / n_slots).clamp(1, u64::MAX as u128);
    Duration::from_nanos(nanos as u64)
}

/// Request roles. Fast, Cancel, Reconcile, and GapReplay have physically
/// isolated account pools. Query is process-global.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Role {
    /// POST /order, /orders — hot-path placements.
    Fast,
    /// DELETE /order, /orders, /cancel-all.
    Cancel,
    /// Orphan reconciliation (GET order state + DELETE retries).
    Reconcile,
    /// Everything else: snapshots, metadata, listenKey, heartbeats.
    Query,
    /// Authenticated Polymarket `/trades` gap recovery. Physically isolated
    /// from Query so replay stalls cannot consume ordinary query connections.
    GapReplay,
}

/// Number of clients (≈ warm connections per host) in the two process-global
/// fallback pools.
#[derive(Clone, Copy, Debug)]
pub struct GlobalPoolSizes {
    /// Fallback for place/cancel paths that have no account admission permit.
    pub fallback_order: usize,
    /// Ordinary queries plus reconcile fallback traffic.
    pub query: usize,
}

impl Default for GlobalPoolSizes {
    fn default() -> Self {
        Self {
            fallback_order: 4,
            query: 4,
        }
    }
}

impl GlobalPoolSizes {
    /// Apply global-pool env overrides (each var optional). The old ORDER and
    /// MISC names remain aliases so an existing deployment does not silently
    /// change capacity during rollout.
    pub fn with_env_overrides(mut self) -> Self {
        fn ov(primary: &str, legacy: &str, cur: usize) -> usize {
            [primary, legacy]
                .into_iter()
                .find_map(|name| {
                    std::env::var(name)
                        .ok()
                        .and_then(|v| v.parse::<usize>().ok())
                        .filter(|n| (1..=64).contains(n))
                })
                .unwrap_or(cur)
        }
        self.fallback_order = ov(
            "HEXBOT_HTTP_POOL_FALLBACK_ORDER",
            "HEXBOT_HTTP_POOL_ORDER",
            self.fallback_order,
        );
        self.query = ov(
            "HEXBOT_HTTP_POOL_QUERY",
            "HEXBOT_HTTP_POOL_MISC",
            self.query,
        );
        self
    }
}

static SIZES: OnceLock<GlobalPoolSizes> = OnceLock::new();

/// Set pool sizes. Must be called **before** `async_rt::init()` builds the
/// pools; later calls are ignored (first write wins). Returns whether the
/// value was applied.
pub fn set_global_sizes(sizes: GlobalPoolSizes) -> bool {
    SIZES.set(sizes.with_env_overrides()).is_ok()
}

fn sizes() -> GlobalPoolSizes {
    *SIZES.get_or_init(|| GlobalPoolSizes::default().with_env_overrides())
}

// ── Pools ─────────────────────────────────────────────────────────

struct Pools {
    /// Process-global fallback for Fast + Cancel.
    fallback_order: RolePool,
    /// Process-global Query + reconcile fallback.
    query: RolePool,
}

static POOLS: OnceLock<Pools> = OnceLock::new();

/// Build an HTTP/1.1-only client. `pool_max_idle_per_host = 2` keeps the
/// client's primary connection plus one burst spare; the long
/// `pool_idle_timeout` is a backstop — keep-warm traffic normally touches
/// every client well inside it.
/// Build a standalone HTTP/1.1-only client with custom timeouts, for
/// endpoints whose latency profile doesn't fit the shared pools (e.g.
/// public Polygon RPCs: multi-second JSON-RPC round trips). Same h1.1 /
/// keepalive / nodelay stance as the pool clients.
pub fn build_client(total_timeout: Duration, connect_timeout: Duration) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .http1_only()
        .pool_idle_timeout(Duration::from_secs(300))
        .pool_max_idle_per_host(2)
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true)
        .timeout(total_timeout)
        .connect_timeout(connect_timeout)
        .build()
        .context("build custom h1 reqwest client")
}

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

/// Build all pools. Called once from `async_rt::init()`.
pub(crate) fn init_pools() -> Result<()> {
    let s = sizes();
    let pools = Pools {
        fallback_order: RolePool::new(s.fallback_order, ORDER_TIMEOUT_CEILING, Role::Fast)?,
        query: RolePool::new(s.query, QUERY_TIMEOUT_CEILING, Role::Query)?,
    };
    POOLS
        .set(pools)
        .map_err(|_| anyhow::anyhow!("http1_pool already initialised"))?;
    log::info!(
        "[http1_pool] global pools initialised (h1.1) fallback_order={} query={}",
        s.fallback_order,
        s.query,
    );
    Ok(())
}

fn pools() -> &'static Pools {
    POOLS.get().expect("async_rt::init() not called")
}

/// Round-robin client for `role`.
pub fn client(role: Role) -> Arc<reqwest::Client> {
    global_role(role).exempt_client()
}

/// Round-robin global-role client plus its slot health handle.
pub fn pooled_client(role: Role) -> PooledClient {
    global_role(role).exempt_pooled_client()
}

/// All clients across every role — for startup prewarm fan-out that must
/// touch *every* underlying connection, not just one pick. Steady-state
/// keep-warm uses exact-slot permits instead.
pub fn clients_all() -> Vec<Arc<reqwest::Client>> {
    let p = pools();
    let mut all = p.fallback_order.clients();
    all.extend(p.query.clients());
    // Include every account admission pool so startup prewarm reaches the
    // actual place/cancel/reconcile/replay connections, not just fallbacks.
    if let Some(registry) = ACCOUNT_POOLS.get() {
        for account in registry.by_account.values() {
            for rp in [
                &account.fast,
                &account.cancel,
                &account.reconcile,
                &account.gap_replay,
            ] {
                all.extend(rp.slots.iter().map(|s| s.client.load_full()));
            }
        }
    }
    all
}

/// All current clients plus health handles. Polymarket heartbeat uses this
/// form so keep-warm transport failures feed the same rebuild machinery as
/// business requests.
pub fn pooled_clients_all() -> Vec<PooledClient> {
    let p = pools();
    let mut all = p.fallback_order.pooled_clients();
    all.extend(p.query.pooled_clients());
    if let Some(registry) = ACCOUNT_POOLS.get() {
        for account in registry.by_account.values() {
            for rp in [
                &account.fast,
                &account.cancel,
                &account.reconcile,
                &account.gap_replay,
            ] {
                all.extend(rp.pooled_clients());
            }
        }
    }
    all
}

// ── Prewarm + keep-warm ───────────────────────────────────────────

/// Result of concurrently prewarming every client in one global role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrewarmReport {
    pub total: usize,
    pub ok: usize,
    pub first_error: Option<String>,
}

fn global_role(role: Role) -> &'static RolePool {
    let p = pools();
    match role {
        Role::Fast | Role::Cancel => &p.fallback_order,
        Role::Reconcile | Role::Query => &p.query,
        Role::GapReplay => panic!("GapReplay is account-scoped; use an account pool API"),
    }
}

/// Per-logical-role counter index inside a merged pool: primary role
/// (Fast / Reconcile / GapReplay) → 0, secondary (Cancel / Query) → 1.
fn role_ctr_index(role: Role) -> usize {
    match role {
        Role::Fast | Role::Reconcile | Role::GapReplay => 0,
        Role::Cancel | Role::Query => 1,
    }
}

fn global_role_clients(role: Role) -> Vec<Arc<reqwest::Client>> {
    global_role(role).clients()
}

/// Return only the process-global clients for `role`.
///
/// Unlike [`all_clients`], this intentionally excludes per-instance
/// admission pools. It is useful for low-volume query hosts where warming
/// every place/cancel client would create a needless request burst.
pub fn clients_for_role(role: Role) -> Vec<Arc<reqwest::Client>> {
    global_role_clients(role)
}

/// Concurrently establish TCP+TLS state for every client in a global role.
pub async fn prewarm_role(role: Role, warm_url: &str) -> PrewarmReport {
    let clients = global_role_clients(role);
    prewarm_clients(&format!("global/{role:?}"), clients, warm_url).await
}

async fn prewarm_clients(
    label: &str,
    clients: Vec<Arc<reqwest::Client>>,
    warm_url: &str,
) -> PrewarmReport {
    let total = clients.len();
    let mut set = tokio::task::JoinSet::new();
    for client in clients {
        let url = warm_url.to_string();
        set.spawn(async move {
            let response = client.get(url).send().await?;
            let status = response.status().as_u16();
            response.bytes().await?;
            Ok::<u16, reqwest::Error>(status)
        });
    }
    let mut ok = 0usize;
    let mut first_error = None;
    while let Some(result) = set.join_next().await {
        match result {
            Ok(Ok(status)) if (200..400).contains(&status) => ok += 1,
            Ok(Ok(status)) => {
                if first_error.is_none() {
                    first_error = Some(format!("HTTP {}", status));
                }
            }
            Ok(Err(error)) => {
                if first_error.is_none() {
                    first_error = Some(error.to_string());
                }
            }
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error.to_string());
                }
            }
        }
    }
    log::info!(
        "[http1_pool] {} prewarm: {}/{} connections up{}",
        label,
        ok,
        total,
        first_error
            .as_deref()
            .map(|e| format!(" (first err: {})", e))
            .unwrap_or_default(),
    );
    PrewarmReport {
        total,
        ok,
        first_error,
    }
}

#[derive(Clone, Copy)]
struct KeepWarmTarget {
    pool: &'static RolePool,
    slot: usize,
}

impl KeepWarmTarget {
    fn eligible_at(&self, now_ns: u64) -> bool {
        let last = self.pool.slots[self.slot]
            .last_activity_ns
            .load(Ordering::Acquire);
        now_ns.saturating_sub(last) >= KEEP_WARM_IDLE.as_nanos() as u64
    }

    fn try_acquire(&self) -> Option<KeepWarmLease> {
        if !self.eligible_at(activity_now_ns()) {
            return None;
        }
        let global = GlobalKeepWarmLease::try_acquire()?;
        if self
            .pool
            .keep_warm_inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            drop(global);
            return None;
        }
        // Recheck after taking both concurrency guards. A business request
        // could have completed between the initial eligibility read and here.
        if !self.eligible_at(activity_now_ns()) {
            self.pool.keep_warm_inflight.store(false, Ordering::Release);
            drop(global);
            return None;
        }
        match self.pool.try_acquire_slot(self.slot) {
            Some(permit) => Some(KeepWarmLease {
                permit: Some(permit),
                pool_inflight: self.pool.keep_warm_inflight.clone(),
                _global: global,
            }),
            None => {
                self.pool.keep_warm_inflight.store(false, Ordering::Release);
                drop(global);
                None
            }
        }
    }
}

struct GlobalKeepWarmLease;

impl GlobalKeepWarmLease {
    fn try_acquire() -> Option<Self> {
        let mut current = KEEP_WARM_INFLIGHT.load(Ordering::Acquire);
        loop {
            if current >= KEEP_WARM_GLOBAL_LIMIT {
                return None;
            }
            match KEEP_WARM_INFLIGHT.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(Self),
                Err(observed) => current = observed,
            }
        }
    }
}

impl Drop for GlobalKeepWarmLease {
    fn drop(&mut self) {
        KEEP_WARM_INFLIGHT.fetch_sub(1, Ordering::AcqRel);
    }
}

struct KeepWarmLease {
    permit: Option<Permit>,
    pool_inflight: Arc<AtomicBool>,
    _global: GlobalKeepWarmLease,
}

impl KeepWarmLease {
    fn pooled_client(&self) -> PooledClient {
        self.permit
            .as_ref()
            .expect("keep-warm permit missing")
            .pooled_client()
    }
}

impl Drop for KeepWarmLease {
    fn drop(&mut self) {
        // Release the slot first. A new keep-warm for this pool must not pass
        // its pool guard until business admission can observe the free slot.
        drop(self.permit.take());
        self.pool_inflight.store(false, Ordering::Release);
    }
}

fn keep_warm_targets() -> Vec<KeepWarmTarget> {
    let p = pools();
    let mut targets = Vec::new();
    for pool in [&p.fallback_order, &p.query] {
        targets.extend((0..pool.slots.len()).map(|slot| KeepWarmTarget { pool, slot }));
    }
    if let Some(registry) = ACCOUNT_POOLS.get() {
        let mut ids: Vec<&String> = registry.by_account.keys().collect();
        ids.sort();
        for id in ids {
            let account = &registry.by_account[id];
            for pool in [
                &account.fast,
                &account.cancel,
                &account.reconcile,
                &account.gap_replay,
            ] {
                targets.extend((0..pool.slots.len()).map(|slot| KeepWarmTarget { pool, slot }));
            }
        }
    }
    targets
}

/// Spawn an activity-aware warm task for `host`.
///
/// Startup keeps its one-shot prewarm fan-out because it runs before live
/// order flow. Steady-state probes are different: `full_sweep` is divided by
/// the total slot count and every tick selects at most one slot that has seen
/// no request for 30 s. The probe owns the exact slot permit, uses a 500 ms
/// deadline, and is bounded to one in-flight probe per pool and two globally.
/// `warm_url` should be a cheap unauthenticated endpoint such as `/time`.
/// Repeated calls for the same label are refused.
pub fn spawn_keep_warm(label: &'static str, warm_url: String, full_sweep: Duration) {
    use std::sync::Mutex;
    static SPAWNED: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());
    {
        let mut spawned = SPAWNED.lock().unwrap();
        if spawned.contains(&label) {
            return;
        }
        spawned.push(label);
    }
    // Order runtime, not the general one: connections this task creates
    // (or re-creates after idle eviction) must register with the order
    // reactor so hot-path requests reusing them aren't woken through the
    // busy feed reactor.
    crate::async_rt::order_handle().spawn(async move {
        let targets = keep_warm_targets();
        if targets.is_empty() {
            log::warn!("[http1_pool] {} keep-warm disabled: no slots", label);
            return;
        }

        // Preserve the historical one-shot startup prewarm. This is not a
        // steady-state keep-warm probe: venue startup invokes it before live
        // order flow, so establishing all TCP+TLS connections concurrently is
        // intentional. Once it completes, every periodic request below is
        // permit-bound and concurrency-limited.
        let t0 = Instant::now();
        let clients = clients_all();
        let n = clients.len();
        let mut set = tokio::task::JoinSet::new();
        for client in clients {
            let url = warm_url.clone();
            set.spawn(async move {
                let response = client.get(&url).send().await?;
                let status = response.status().as_u16();
                response.bytes().await?;
                Ok::<u16, reqwest::Error>(status)
            });
        }
        let mut ok = 0usize;
        let mut first_error = None;
        while let Some(result) = set.join_next().await {
            match result {
                Ok(Ok(status)) if (200..400).contains(&status) => ok += 1,
                Ok(Ok(status)) => {
                    if first_error.is_none() { first_error = Some(format!("HTTP {}", status)); }
                }
                Ok(Err(error)) => {
                    if first_error.is_none() { first_error = Some(error.to_string()); }
                }
                Err(error) => {
                    if first_error.is_none() { first_error = Some(error.to_string()); }
                }
            }
        }
        let prewarm_completed_ns = activity_now_ns();
        for target in &targets {
            target.pool.slots[target.slot]
                .last_activity_ns
                .store(prewarm_completed_ns, Ordering::Release);
        }
        log::info!(
            "[http1_pool] {} prewarm: {}/{} connections up in {:.0}ms{}",
            label,
            ok,
            n,
            t0.elapsed().as_secs_f64() * 1000.0,
            first_error.as_deref()
                .map(|error| format!(" (first err: {})", error))
                .unwrap_or_default(),
        );

        let tick = keep_warm_tick(full_sweep, targets.len());
        log::info!(
            "[http1_pool] {} keep-warm scheduler: slots={} sweep_ms={} tick_ms={:.3} idle_s={} timeout_ms={} per_pool=1 global={}",
            label,
            targets.len(),
            full_sweep.as_millis(),
            tick.as_secs_f64() * 1000.0,
            KEEP_WARM_IDLE.as_secs(),
            KEEP_WARM_TIMEOUT.as_millis(),
            KEEP_WARM_GLOBAL_LIMIT,
        );

        let mut cursor = 0usize;
        loop {
            tokio::time::sleep(tick).await;
            let now_ns = activity_now_ns();
            let mut dispatched = false;
            for offset in 0..targets.len() {
                let index = (cursor + offset) % targets.len();
                let target = targets[index];
                if !target.eligible_at(now_ns) {
                    continue;
                }
                let Some(lease) = target.try_acquire() else {
                    continue;
                };
                cursor = (index + 1) % targets.len();
                dispatched = true;
                let url = warm_url.clone();
                tokio::spawn(async move {
                    let client = lease.pooled_client();
                    let outcome = async {
                        let response = client
                            .client()
                            .get(&url)
                            .timeout(KEEP_WARM_TIMEOUT)
                            .send()
                            .await?;
                        let status = response.status().as_u16();
                        response.bytes().await?;
                        Ok::<u16, reqwest::Error>(status)
                    }.await;
                    match outcome {
                        Ok(status) => {
                            client.note_transport_success();
                            if (200..400).contains(&status) {
                                log::trace!(
                                    "[http1_pool] {} keep-warm ok role={:?} slot={} status={}",
                                    label, target.pool.role, target.slot, status,
                                );
                            } else {
                                log::warn!(
                                    "[http1_pool] {} keep-warm HTTP {} role={:?} slot={}",
                                    label, status, target.pool.role, target.slot,
                                );
                            }
                        }
                        Err(error) => {
                            client.note_transport_failure(url.clone());
                            log::warn!(
                                "[http1_pool] {} keep-warm failed role={:?} slot={}: {}",
                                label, target.pool.role, target.slot, error,
                            );
                        }
                    }
                    drop(lease);
                });
                break;
            }
            if !dispatched {
                // Move the scan origin even when every eligible target was
                // busy or concurrency-limited, avoiding a permanently hot
                // first candidate once capacity returns.
                cursor = (cursor + 1) % targets.len();
            }
        }
    });
}

// ══════════════════════════════════════════════════════════════════
// Per-account admission control
// ══════════════════════════════════════════════════════════════════
//
// The role pools above are process-global and shared across strategy
// instances; a request just round-robins a client and, if that client's
// warm connection is busy, hyper opens a **cold** TCP+TLS connection.
// Under overlapping waves that produces cold-connection storms exactly
// when the endpoint is already slow.
//
// This layer replaces "round-robin + hope" with explicit admission control,
// per (account, role). Instance IDs resolve to their owning account before a
// hot request is admitted:
//
//   * Each warm connection is a `Slot` with an exclusive `busy` flag.
//     `try_acquire` hands out at most one in-flight request per slot, so
//     a slot's single warm connection is **never double-dispatched** —
//     no concurrency-driven cold connection is ever opened.
//   * Shared-wallet instances intentionally share order/reconcile capacity.
//     Different accounts remain physically isolated.
//   * Placement may use `try_acquire` and hold the quote when all slots
//     are busy. Cancellation uses `acquire`: it retains the request and is
//     woken immediately by permit release instead of waiting for a quote tick.
//   * `exempt_client` is the escape hatch for must-complete traffic
//     (heartbeat / cancel-all): it always returns a client
//     WITHOUT a permit, accepting a possible cold connection because
//     *completing* the request matters more than avoiding one.

/// One connection slot: a warm h1.1 client + an in-use flag. Held by at
/// most one in-flight request at a time.
struct Slot {
    client: Arc<ArcSwap<reqwest::Client>>,
    busy: Arc<AtomicBool>,
    /// True exactly while this free slot has one token in the role lane.
    /// This fences concurrent permit-drop and quarantine-release publication.
    available_token: Arc<AtomicBool>,
    last_activity_ns: Arc<AtomicU64>,
    quarantined: Arc<AtomicBool>,
    transport_failures: Arc<AtomicUsize>,
    generation: Arc<AtomicU64>,
    last_rebuild_ns: Arc<AtomicU64>,
    /// Fixed storage reused by consecutive requests admitted on this slot.
    /// The slot permit guarantees a single writer and prevents reuse until
    /// completion has consumed the trace.
    attempt_trace: Arc<AttemptTraceSlot>,
    timeout: Duration,
}

#[derive(Clone)]
struct ConnectionHealth {
    role: Role,
    slot: usize,
    generation_at_pick: u64,
    slot_client: Arc<ArcSwap<reqwest::Client>>,
    busy: Arc<AtomicBool>,
    available_token: Arc<AtomicBool>,
    quarantined: Arc<AtomicBool>,
    transport_failures: Arc<AtomicUsize>,
    generation: Arc<AtomicU64>,
    last_rebuild_ns: Arc<AtomicU64>,
    available_tx: Sender<usize>,
    timeout: Duration,
}

impl ConnectionHealth {
    fn is_current(&self) -> bool {
        self.generation.load(Ordering::Acquire) == self.generation_at_pick
    }

    fn note_transport_success(&self) {
        if self.is_current() {
            self.transport_failures.store(0, Ordering::Release);
        }
    }

    fn claim_rebuild(&self, threshold: usize, cooldown: Duration) -> Option<usize> {
        if !self.is_current() {
            return None;
        }
        let failures = self.transport_failures.fetch_add(1, Ordering::AcqRel) + 1;
        if failures < threshold.max(1) {
            return None;
        }
        let now_ns = activity_now_ns();
        let cooldown_ns = cooldown.as_nanos().min(u64::MAX as u128) as u64;
        loop {
            let last_ns = self.last_rebuild_ns.load(Ordering::Acquire);
            if last_ns != 0 && now_ns.saturating_sub(last_ns) < cooldown_ns {
                return None;
            }
            if self
                .last_rebuild_ns
                .compare_exchange(last_ns, now_ns, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
        self.quarantined.store(true, Ordering::Release);
        Some(failures)
    }

    fn build_replacement(&self) -> Result<Arc<reqwest::Client>> {
        Ok(Arc::new(build_h1_client(self.timeout)?))
    }

    fn install_replacement(
        &self,
        replacement: Arc<reqwest::Client>,
        failures: usize,
    ) -> Option<u64> {
        if !self.is_current() {
            return None;
        }
        self.slot_client.store(replacement);
        self.transport_failures.store(0, Ordering::Release);
        let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        self.release_quarantine();
        log::warn!(
            "[http1_pool] role={:?} slot={} replaced unhealthy client after {} consecutive transport failures generation={}",
            self.role, self.slot, failures, generation,
        );
        Some(generation)
    }

    fn release_quarantine(&self) {
        self.quarantined.store(false, Ordering::Release);
        publish_slot_token(
            self.slot,
            &self.busy,
            &self.quarantined,
            &self.available_token,
            &self.available_tx,
        );
    }

    async fn rebuild_and_prewarm(self, prewarm_url: String, failures: usize) {
        let candidate = match self.build_replacement() {
            Ok(client) => client,
            Err(error) => {
                self.release_quarantine();
                log::warn!(
                    "[http1_pool] role={:?} slot={} client rebuild failed after {} transport failures: {}",
                    self.role, self.slot, failures, error,
                );
                return;
            }
        };
        let prewarm_result = async {
            let response = candidate
                .get(&prewarm_url)
                .send()
                .await
                .map_err(|error| error.to_string())?;
            let status = response.status();
            if !status.is_success() {
                return Err(format!("HTTP {}", status));
            }
            response.bytes().await.map_err(|error| error.to_string())?;
            Ok::<(), String>(())
        }
        .await;
        match prewarm_result {
            Ok(()) => {
                if let Some(generation) = self.install_replacement(candidate, failures) {
                    log::info!(
                        "[http1_pool] role={:?} slot={} rebuilt and prewarmed generation={} url={}",
                        self.role,
                        self.slot,
                        generation,
                        prewarm_url,
                    );
                }
            }
            Err(error) => {
                self.release_quarantine();
                log::warn!(
                    "[http1_pool] role={:?} slot={} replacement prewarm failed: {}; keeping generation={} url={}",
                    self.role, self.slot, error, self.generation_at_pick, prewarm_url,
                );
            }
        }
    }
}

/// A selected pool client with a generation-fenced health handle.
///
/// Callers report only transport success/failure; HTTP status and JSON-shape
/// errors prove the socket carried bytes and therefore count as transport
/// success. Rebuild runs in the background and installs only after prewarm.
#[derive(Clone)]
pub struct PooledClient {
    client: Arc<reqwest::Client>,
    health: ConnectionHealth,
    attempt_trace: Arc<AttemptTraceSlot>,
    exclusive_admission: bool,
}

static NEXT_HTTP_ATTEMPT_ID: AtomicU64 = AtomicU64::new(1);

struct AttemptTraceSlot {
    role: Role,
    slot: usize,
    attempt_id: AtomicU64,
    signal_ns: AtomicU64,
    prep_ns: AtomicU64,
    signed_ns: AtomicU64,
    account_recorded_ns: AtomicU64,
    dispatched_ns: AtomicU64,
}

impl AttemptTraceSlot {
    fn new(role: Role, slot: usize) -> Self {
        Self {
            role,
            slot,
            attempt_id: AtomicU64::new(0),
            signal_ns: AtomicU64::new(0),
            prep_ns: AtomicU64::new(0),
            signed_ns: AtomicU64::new(0),
            account_recorded_ns: AtomicU64::new(0),
            dispatched_ns: AtomicU64::new(0),
        }
    }
}

/// Allocation-free handle to the trace record preallocated for one admission
/// slot. A handle remains valid until its owning permit is released.
#[derive(Clone)]
pub struct AttemptTraceHandle {
    attempt_id: u64,
    slot: Arc<AttemptTraceSlot>,
}

/// Stable copy of one HTTP attempt's timestamps. The snapshot is keyed by the
/// monotonic attempt ID, never by a client order ID.
#[derive(Clone, Copy, Debug)]
pub struct AttemptTraceSnapshot {
    pub attempt_id: u64,
    pub role: Role,
    pub slot: usize,
    pub signal_ns: u64,
    pub prep_ns: u64,
    pub signed_ns: u64,
    pub account_recorded_ns: u64,
    pub dispatched_ns: u64,
}

impl AttemptTraceHandle {
    pub fn attempt_id(&self) -> u64 {
        self.attempt_id
    }

    /// Mark the instant at which the prepared request entered the async HTTP
    /// runtime. This is the last write before the completion owner reads it.
    pub fn mark_dispatched(&self, timestamp_ns: u64) {
        debug_assert_eq!(
            self.slot.attempt_id.load(Ordering::Acquire),
            self.attempt_id,
            "attempt slot reused before permit release"
        );
        self.slot
            .dispatched_ns
            .store(timestamp_ns, Ordering::Release);
    }

    pub fn snapshot(&self) -> Option<AttemptTraceSnapshot> {
        if self.slot.attempt_id.load(Ordering::Acquire) != self.attempt_id {
            return None;
        }
        Some(AttemptTraceSnapshot {
            attempt_id: self.attempt_id,
            role: self.slot.role,
            slot: self.slot.slot,
            signal_ns: self.slot.signal_ns.load(Ordering::Relaxed),
            prep_ns: self.slot.prep_ns.load(Ordering::Relaxed),
            signed_ns: self.slot.signed_ns.load(Ordering::Relaxed),
            account_recorded_ns: self.slot.account_recorded_ns.load(Ordering::Relaxed),
            dispatched_ns: self.slot.dispatched_ns.load(Ordering::Acquire),
        })
    }
}

impl PooledClient {
    pub fn client(&self) -> &Arc<reqwest::Client> {
        &self.client
    }

    pub fn role(&self) -> Role {
        self.attempt_trace.role
    }

    pub fn slot(&self) -> usize {
        self.attempt_trace.slot
    }

    /// Allocate the process-monotonic identity used by non-admission HTTP
    /// paths. Admission-owned order attempts should call [`Self::begin_attempt`]
    /// so the same ID is also published in the slot trace.
    pub fn allocate_attempt_id(&self) -> u64 {
        NEXT_HTTP_ATTEMPT_ID.fetch_add(1, Ordering::Relaxed)
    }

    /// Start a trace in this admission slot's preallocated storage. This is
    /// valid only for a client obtained from an exclusive [`Permit`].
    pub fn begin_attempt(
        &self,
        signal_ns: u64,
        prep_ns: u64,
        signed_ns: u64,
        account_recorded_ns: u64,
    ) -> AttemptTraceHandle {
        debug_assert!(
            self.exclusive_admission,
            "attempt trace requires an admission-owned client"
        );
        let attempt_id = self.allocate_attempt_id();
        self.attempt_trace.attempt_id.store(0, Ordering::Relaxed);
        self.attempt_trace
            .signal_ns
            .store(signal_ns, Ordering::Relaxed);
        self.attempt_trace.prep_ns.store(prep_ns, Ordering::Relaxed);
        self.attempt_trace
            .signed_ns
            .store(signed_ns, Ordering::Relaxed);
        self.attempt_trace
            .account_recorded_ns
            .store(account_recorded_ns, Ordering::Relaxed);
        self.attempt_trace.dispatched_ns.store(0, Ordering::Relaxed);
        self.attempt_trace
            .attempt_id
            .store(attempt_id, Ordering::Release);
        AttemptTraceHandle {
            attempt_id,
            slot: Arc::clone(&self.attempt_trace),
        }
    }

    pub fn note_transport_success(&self) {
        self.health.note_transport_success();
    }

    /// Record a transport failure. On the second consecutive failure this
    /// quarantines the slot and starts background rebuild + prewarm. Returns
    /// true when this call claimed the rebuild.
    pub fn note_transport_failure(&self, prewarm_url: String) -> bool {
        let Some(failures) = self.health.claim_rebuild(2, Duration::from_secs(30)) else {
            return false;
        };
        let health = self.health.clone();
        // Order runtime for the same reason as keep-warm: the rebuilt
        // slot's fresh connections must be owned by the order reactor.
        crate::async_rt::order_handle().spawn(async move {
            health.rebuild_and_prewarm(prewarm_url, failures).await;
        });
        true
    }
}

/// Admission permit: owns an exclusive slot's client for the duration of
/// one request. Dropping it frees the slot for the next request.
pub struct Permit {
    role: Role,
    slot: usize,
    acquired_generation: u64,
    flag: Arc<AtomicBool>,
    available_token: Arc<AtomicBool>,
    last_activity_ns: Arc<AtomicU64>,
    client: Arc<reqwest::Client>,
    slot_client: Arc<ArcSwap<reqwest::Client>>,
    quarantined: Arc<AtomicBool>,
    transport_failures: Arc<AtomicUsize>,
    generation: Arc<AtomicU64>,
    last_rebuild_ns: Arc<AtomicU64>,
    attempt_trace: Arc<AttemptTraceSlot>,
    available_tx: Sender<usize>,
    timeout: Duration,
}

impl Permit {
    /// The reserved client — dispatch the request on this.
    pub fn client(&self) -> &Arc<reqwest::Client> {
        &self.client
    }

    pub fn role(&self) -> Role {
        self.role
    }
    pub fn slot(&self) -> usize {
        self.slot
    }
    pub fn generation(&self) -> u64 {
        self.acquired_generation
    }

    fn health(&self, generation_at_pick: u64) -> ConnectionHealth {
        ConnectionHealth {
            role: self.role,
            slot: self.slot,
            generation_at_pick,
            slot_client: self.slot_client.clone(),
            busy: self.flag.clone(),
            available_token: self.available_token.clone(),
            quarantined: self.quarantined.clone(),
            transport_failures: self.transport_failures.clone(),
            generation: self.generation.clone(),
            last_rebuild_ns: self.last_rebuild_ns.clone(),
            available_tx: self.available_tx.clone(),
            timeout: self.timeout,
        }
    }

    pub fn pooled_client(&self) -> PooledClient {
        PooledClient {
            client: self.client.clone(),
            health: self.health(self.acquired_generation),
            attempt_trace: Arc::clone(&self.attempt_trace),
            exclusive_admission: true,
        }
    }

    /// Current client installed in this slot. Reconcile callers use this
    /// accessor because a long batch can rebuild the slot between order GETs.
    pub fn current_client(&self) -> Arc<reqwest::Client> {
        self.slot_client.load_full()
    }

    pub fn current_pooled_client(&self) -> PooledClient {
        let generation = self.generation.load(Ordering::Acquire);
        PooledClient {
            client: self.current_client(),
            health: self.health(generation),
            attempt_trace: Arc::clone(&self.attempt_trace),
            exclusive_admission: true,
        }
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        // Eligibility is measured from request completion. Every permit user
        // (business and keep-warm) refreshes the same slot activity clock.
        self.last_activity_ns
            .store(activity_now_ns(), Ordering::Release);
        self.flag.store(false, Ordering::Release);
        publish_slot_token(
            self.slot,
            &self.flag,
            &self.quarantined,
            &self.available_token,
            &self.available_tx,
        );
    }
}

/// Publish one connection-owner token at most once. The bounded role lane is
/// the ownership transfer: receiving a token grants the sole right to move
/// that physical slot to busy, and permit drop returns it.
fn publish_slot_token(
    slot: usize,
    busy: &AtomicBool,
    quarantined: &AtomicBool,
    available_token: &AtomicBool,
    available_tx: &Sender<usize>,
) {
    if busy.load(Ordering::Acquire) || quarantined.load(Ordering::Acquire) {
        return;
    }
    if available_token
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
        && available_tx.try_send(slot).is_err()
    {
        // A full bounded lane indicates an invariant violation. Fail closed:
        // clear the publication bit so a later owner transition can recover.
        available_token.store(false, Ordering::Release);
    }
}

/// A pool of N slots for one (instance, role). Concurrency ceiling = N =
/// warm-connection count.
struct RolePool {
    /// Pool identity — the PRIMARY logical role (Fast for the order
    /// pool, Reconcile for misc). Used for permit tagging + repair logs.
    role: Role,
    slots: Vec<Slot>,
    rr: AtomicUsize, // round-robin cursor for exempt (no-permit) picks
    available_tx: Sender<usize>,
    available_rx: Receiver<usize>,
    keep_warm_inflight: Arc<AtomicBool>,
    // Admission counters per LOGICAL role sharing this pool:
    // index 0 = primary (Fast / Reconcile / GapReplay), 1 = secondary
    // (Cancel / Query). Slots are shared; the split keeps the
    // `[admission]` per-role observability intact across the merge.
    acquires: [AtomicU64; 2],
    skips: [AtomicU64; 2],
    waits: [AtomicU64; 2],
}

impl RolePool {
    fn new(n: usize, timeout: Duration, role: Role) -> Result<Self> {
        let n = n.max(1);
        let (available_tx, available_rx) = crossbeam_channel::bounded(n);
        let mut slots = Vec::with_capacity(n);
        for slot in 0..n {
            slots.push(Slot {
                client: Arc::new(ArcSwap::from(Arc::new(build_h1_client(timeout)?))),
                busy: Arc::new(AtomicBool::new(false)),
                available_token: Arc::new(AtomicBool::new(true)),
                last_activity_ns: Arc::new(AtomicU64::new(activity_now_ns())),
                quarantined: Arc::new(AtomicBool::new(false)),
                transport_failures: Arc::new(AtomicUsize::new(0)),
                generation: Arc::new(AtomicU64::new(0)),
                last_rebuild_ns: Arc::new(AtomicU64::new(0)),
                attempt_trace: Arc::new(AttemptTraceSlot::new(role, slot)),
                timeout,
            });
            available_tx
                .try_send(slot)
                .expect("fresh connection owner lane must have capacity");
        }
        Ok(Self {
            role,
            slots,
            rr: AtomicUsize::new(0),
            available_tx,
            available_rx,
            keep_warm_inflight: Arc::new(AtomicBool::new(false)),
            acquires: [AtomicU64::new(0), AtomicU64::new(0)],
            skips: [AtomicU64::new(0), AtomicU64::new(0)],
            waits: [AtomicU64::new(0), AtomicU64::new(0)],
        })
    }

    /// Reserve a free slot exclusively, or `None` if all are busy (caller
    /// SKIPS — no cold connection is opened). Binds permit → slot → warm
    /// connection so the connection is never used by two requests at once.
    #[cfg(test)]
    fn try_acquire(&self) -> Option<Permit> {
        self.try_acquire_as(0)
    }

    /// `try_acquire` counting against logical-role counter `ctr`
    /// (see `role_ctr_index`).
    fn try_acquire_as(&self, ctr: usize) -> Option<Permit> {
        self.try_acquire_inner(true, ctr)
    }

    /// Retain a must-dispatch request until a warm slot becomes available.
    /// Permit release wakes one waiter immediately, so callers do not poll or
    /// fall back to a strategy cadence. Unlike `try_acquire`, contention here
    /// is counted as a wait rather than a shed business operation.
    #[cfg(test)]
    fn acquire(&self) -> Permit {
        self.acquire_as(0)
    }

    /// `acquire` counting against logical-role counter `ctr`.
    fn acquire_as(&self, ctr: usize) -> Permit {
        if let Some(permit) = self.try_acquire_inner(false, ctr) {
            return permit;
        }
        self.waits[ctr].fetch_add(1, Ordering::Relaxed);
        loop {
            let slot = self
                .available_rx
                .recv()
                .expect("connection owner lane disconnected");
            if let Some(permit) = self.claim_received_slot(slot) {
                self.acquires[ctr].fetch_add(1, Ordering::Relaxed);
                return permit;
            }
        }
    }

    fn try_acquire_inner(&self, count_skip: bool, ctr: usize) -> Option<Permit> {
        for _ in 0..self.slots.len() {
            let Ok(slot) = self.available_rx.try_recv() else {
                break;
            };
            if let Some(permit) = self.claim_received_slot(slot) {
                self.acquires[ctr].fetch_add(1, Ordering::Relaxed);
                return Some(permit);
            }
        }
        if count_skip {
            self.skips[ctr].fetch_add(1, Ordering::Relaxed);
        }
        None
    }

    /// Reserve one exact slot without touching business admission counters.
    /// Used by keep-warm so round-robin eligibility cannot silently bind the
    /// probe to a different, recently active connection.
    fn try_acquire_slot(&self, slot: usize) -> Option<Permit> {
        if slot >= self.slots.len() {
            return None;
        }
        for _ in 0..self.slots.len() {
            let Ok(candidate) = self.available_rx.try_recv() else {
                return None;
            };
            if candidate == slot {
                return self.claim_received_slot(candidate);
            }
            let state = &self.slots[candidate];
            state.available_token.store(false, Ordering::Release);
            publish_slot_token(
                candidate,
                &state.busy,
                &state.quarantined,
                &state.available_token,
                &self.available_tx,
            );
        }
        None
    }

    fn claim_received_slot(&self, slot: usize) -> Option<Permit> {
        let s = self.slots.get(slot)?;
        s.available_token.store(false, Ordering::Release);
        if s.quarantined.load(Ordering::Acquire) {
            return None;
        }
        if s.busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return None;
        }
        // Mark request start as well as completion. This closes the small
        // window where another scheduler observes an old timestamp while the
        // newly acquired slot has not begun socket I/O yet.
        s.last_activity_ns
            .store(activity_now_ns(), Ordering::Release);
        let acquired_generation = s.generation.load(Ordering::Acquire);
        Some(Permit {
            role: self.role,
            slot,
            acquired_generation,
            flag: s.busy.clone(),
            available_token: s.available_token.clone(),
            last_activity_ns: s.last_activity_ns.clone(),
            client: s.client.load_full(),
            slot_client: s.client.clone(),
            quarantined: s.quarantined.clone(),
            transport_failures: s.transport_failures.clone(),
            generation: s.generation.clone(),
            last_rebuild_ns: s.last_rebuild_ns.clone(),
            attempt_trace: Arc::clone(&s.attempt_trace),
            available_tx: self.available_tx.clone(),
            timeout: s.timeout,
        })
    }

    /// Exempt path: a client via round-robin WITHOUT a permit. Always
    /// returns — may cold-connect if every warm connection is busy. For
    /// heartbeat / cancel-all only. Keep-warm must use an exact-slot permit.
    fn exempt_client(&self) -> Arc<reqwest::Client> {
        self.exempt_pooled_client().client
    }

    fn pooled_client_for_slot(&self, slot: usize) -> PooledClient {
        let state = &self.slots[slot];
        let generation = state.generation.load(Ordering::Acquire);
        PooledClient {
            client: state.client.load_full(),
            health: ConnectionHealth {
                role: self.role,
                slot,
                generation_at_pick: generation,
                slot_client: state.client.clone(),
                busy: state.busy.clone(),
                available_token: state.available_token.clone(),
                quarantined: state.quarantined.clone(),
                transport_failures: state.transport_failures.clone(),
                generation: state.generation.clone(),
                last_rebuild_ns: state.last_rebuild_ns.clone(),
                available_tx: self.available_tx.clone(),
                timeout: state.timeout,
            },
            attempt_trace: Arc::clone(&state.attempt_trace),
            exclusive_admission: false,
        }
    }

    fn exempt_pooled_client(&self) -> PooledClient {
        let start = self.rr.fetch_add(1, Ordering::Relaxed);
        for offset in 0..self.slots.len() {
            let slot = (start + offset) % self.slots.len();
            if !self.slots[slot].quarantined.load(Ordering::Acquire) {
                self.slots[slot]
                    .last_activity_ns
                    .store(activity_now_ns(), Ordering::Release);
                return self.pooled_client_for_slot(slot);
            }
        }
        // Must-complete traffic retains the historical escape hatch when all
        // slots are quarantined; its outcome still feeds health tracking.
        let slot = start % self.slots.len();
        self.slots[slot]
            .last_activity_ns
            .store(activity_now_ns(), Ordering::Release);
        self.pooled_client_for_slot(slot)
    }

    fn clients(&self) -> Vec<Arc<reqwest::Client>> {
        self.slots.iter().map(|s| s.client.load_full()).collect()
    }

    fn pooled_clients(&self) -> Vec<PooledClient> {
        (0..self.slots.len())
            .map(|slot| self.pooled_client_for_slot(slot))
            .collect()
    }

    /// (acquires, skips, waits, busy_now) for logical-role counter
    /// `ctr`. `busy_now` counts the SHARED slots — the same value is
    /// reported for both roles of a merged pool.
    fn stats_as(&self, ctr: usize) -> (u64, u64, u64, usize) {
        let busy = self
            .slots
            .iter()
            .filter(|s| s.busy.load(Ordering::Relaxed))
            .count();
        (
            self.acquires[ctr].load(Ordering::Relaxed),
            self.skips[ctr].load(Ordering::Relaxed),
            self.waits[ctr].load(Ordering::Relaxed),
            busy,
        )
    }

    /// Primary-role stats (single-role pools: GapReplay, tests).
    fn stats(&self) -> (u64, u64, u64, usize) {
        self.stats_as(0)
    }
}

pub const ACCOUNT_FAST_SLOTS_PER_INSTANCE: usize = 4;
pub const ACCOUNT_CANCEL_SLOTS_PER_INSTANCE: usize = 4;
/// Backward-compatible name for placement capacity. Account cancellation now
/// has a separate pool of the same size.
pub const ACCOUNT_ORDER_SLOTS_PER_INSTANCE: usize = ACCOUNT_FAST_SLOTS_PER_INSTANCE;
pub const ACCOUNT_RECONCILE_SLOTS_PER_INSTANCE: usize = 2;
pub const ACCOUNT_GAP_REPLAY_SLOTS: usize = 2;

/// Deterministic capacity derived from the number of instances sharing one
/// Polymarket account.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AccountPoolSizes {
    pub fast: usize,
    pub cancel: usize,
    pub reconcile: usize,
    pub gap_replay: usize,
}

impl AccountPoolSizes {
    pub fn for_instances(instance_count: usize) -> Self {
        Self {
            fast: ACCOUNT_FAST_SLOTS_PER_INSTANCE * instance_count,
            cancel: ACCOUNT_CANCEL_SLOTS_PER_INSTANCE * instance_count,
            reconcile: ACCOUNT_RECONCILE_SLOTS_PER_INSTANCE * instance_count,
            gap_replay: ACCOUNT_GAP_REPLAY_SLOTS,
        }
    }
}

/// Physical pools for one account. Placement cannot be starved by retained
/// cancellations (or vice versa); reconciliation and gap replay remain
/// isolated from both hot lanes and process-global queries.
struct AccountPools {
    fast: RolePool,
    cancel: RolePool,
    reconcile: RolePool,
    gap_replay: RolePool,
}

impl AccountPools {
    fn role(&self, role: Role) -> Option<&RolePool> {
        match role {
            Role::Fast => Some(&self.fast),
            Role::Cancel => Some(&self.cancel),
            Role::Reconcile => Some(&self.reconcile),
            Role::GapReplay => Some(&self.gap_replay),
            Role::Query => None,
        }
    }
}

struct AccountPoolRegistry {
    by_account: HashMap<String, AccountPools>,
    instance_to_account: HashMap<String, String>,
}

static ACCOUNT_POOLS: OnceLock<AccountPoolRegistry> = OnceLock::new();

/// Build account-level admission pools and the instance→account routing map.
/// `accounts[account_id]` is the complete set of enabled instance IDs sharing
/// that wallet. The fixed sizing policy is 4·N placement, 4·N cancel, 2·N
/// reconcile and 2 gap replay slots per account.
pub fn init_account_pools(accounts: &HashMap<String, Vec<String>>) -> Result<()> {
    let registry = build_account_pool_registry(accounts)?;
    let account_count = registry.by_account.len();
    let instance_count = registry.instance_to_account.len();
    ACCOUNT_POOLS
        .set(registry)
        .map_err(|_| anyhow::anyhow!("account pools already initialised"))?;
    log::info!(
        "[http1_pool] account pools initialised: {} account(s), {} instance(s)",
        account_count,
        instance_count,
    );
    Ok(())
}

fn build_account_pool_registry(
    accounts: &HashMap<String, Vec<String>>,
) -> Result<AccountPoolRegistry> {
    let mut by_account = HashMap::with_capacity(accounts.len());
    let mut instance_to_account = HashMap::new();
    let mut account_ids: Vec<&String> = accounts.keys().collect();
    account_ids.sort();
    for account_id in account_ids {
        if account_id.is_empty() {
            anyhow::bail!("account pool id must not be empty");
        }
        let mut instances = accounts[account_id].clone();
        instances.retain(|id| !id.is_empty());
        instances.sort();
        instances.dedup();
        if instances.is_empty() {
            anyhow::bail!("account `{}` has no strategy instances", account_id);
        }
        for instance_id in &instances {
            if let Some(previous) =
                instance_to_account.insert(instance_id.clone(), account_id.clone())
            {
                anyhow::bail!(
                    "instance `{}` is assigned to both `{}` and `{}`",
                    instance_id,
                    previous,
                    account_id,
                );
            }
        }
        let sizes = AccountPoolSizes::for_instances(instances.len());
        by_account.insert(
            account_id.clone(),
            AccountPools {
                fast: RolePool::new(sizes.fast, ORDER_TIMEOUT_CEILING, Role::Fast)?,
                cancel: RolePool::new(sizes.cancel, ORDER_TIMEOUT_CEILING, Role::Cancel)?,
                reconcile: RolePool::new(sizes.reconcile, QUERY_TIMEOUT_CEILING, Role::Reconcile)?,
                gap_replay: RolePool::new(sizes.gap_replay, GAP_REPLAY_TIMEOUT, Role::GapReplay)?,
            },
        );
        log::info!(
            "[http1_pool] account={} instances={} fast={} cancel={} reconcile={} gap_replay={}",
            account_id,
            instances.len(),
            sizes.fast,
            sizes.cancel,
            sizes.reconcile,
            sizes.gap_replay,
        );
    }
    Ok(AccountPoolRegistry {
        by_account,
        instance_to_account,
    })
}

/// True once account-level admission has been configured.
pub fn account_pools_ready() -> bool {
    ACCOUNT_POOLS.get().is_some()
}

/// Fixed physical connection slots that must be owned by the execution
/// actors.  The manifest is sorted so startup binds every slot
/// deterministically before any business request can borrow it.
pub fn account_execution_slot_manifest() -> Vec<(String, Role, usize)> {
    let Some(registry) = ACCOUNT_POOLS.get() else {
        return Vec::new();
    };
    let mut account_ids: Vec<&String> = registry.by_account.keys().collect();
    account_ids.sort();
    let mut manifest = Vec::new();
    for account_id in account_ids {
        let pools = &registry.by_account[account_id];
        for role in [Role::Fast, Role::Cancel, Role::Reconcile] {
            let slots = pools
                .role(role)
                .expect("execution role must have an account pool")
                .slots
                .len();
            manifest.extend((0..slots).map(|slot| (account_id.clone(), role, slot)));
        }
    }
    manifest
}

/// Bind one exact physical slot to its long-lived execution owner.  This is a
/// startup-only ownership transfer: callers retain the returned permit for the
/// actor lifetime and use `current_pooled_client()` for each sequential
/// request.  Taking the same slot twice fails closed.
pub fn take_account_execution_slot(account_id: &str, role: Role, slot: usize) -> Option<Permit> {
    account_by_id(account_id)?
        .role(role)?
        .try_acquire_slot(slot)
}

fn account_for_instance(instance: &str) -> Option<&'static AccountPools> {
    let registry = ACCOUNT_POOLS.get()?;
    let account_id = registry.instance_to_account.get(instance)?;
    registry.by_account.get(account_id)
}

fn account_by_id(account_id: &str) -> Option<&'static AccountPools> {
    ACCOUNT_POOLS.get()?.by_account.get(account_id)
}

/// Admission control: map `instance` to its account and reserve a warm
/// account connection for the requested hot-path role.
///   * `Some(permit)` → dispatch on `permit.client()`, release on drop.
///   * `None`         → all warm connections busy OR unknown instance;
///                      the caller must SKIP (no cold connection).
pub fn try_acquire(instance: &str, role: Role) -> Option<Permit> {
    account_for_instance(instance)?
        .role(role)?
        .try_acquire_as(role_ctr_index(role))
}

/// Event-driven admission for a request that must not be dropped. Returns
/// `None` only for an unknown instance/role; ordinary saturation waits for the
/// next permit release and dispatches immediately.
pub fn acquire(instance: &str, role: Role) -> Option<Permit> {
    Some(
        account_for_instance(instance)?
            .role(role)?
            .acquire_as(role_ctr_index(role)),
    )
}

/// Best-effort account-slot borrow for batch/must-complete paths that do not
/// carry an engine permit. Saturation is not counted as a shed request because
/// the caller immediately uses the process-global fallback-order pool. This is
/// deliberately non-blocking: a completion callback may still hold its
/// original permit while scheduling a defensive cancel.
pub fn try_borrow_account(account_id: &str, role: Role) -> Option<Permit> {
    account_by_id(account_id)?
        .role(role)?
        .try_acquire_inner(false, role_ctr_index(role))
}

/// Reserve a slot from one account's physically isolated gap-replay pool.
pub fn try_acquire_account(account_id: &str, role: Role) -> Option<Permit> {
    account_by_id(account_id)?
        .role(role)?
        .try_acquire_as(role_ctr_index(role))
}

/// Prewarm exactly one account's two gap-replay connections.
pub async fn prewarm_account_gap_replay(account_id: &str, warm_url: &str) -> PrewarmReport {
    let clients = account_by_id(account_id)
        .map(|account| account.gap_replay.clients())
        .unwrap_or_default();
    prewarm_clients(
        &format!("account={account_id}/GapReplay"),
        clients,
        warm_url,
    )
    .await
}

/// Exempt dispatch for must-complete traffic (heartbeat / cancel-all): never
/// blocked by admission, may cold-connect. Keep-warm deliberately does not use
/// this escape hatch. Falls back to the process-global pool when the instance
/// is unknown or the role is Query.
pub fn exempt_client(instance: &str, role: Role) -> Arc<reqwest::Client> {
    if let Some(account) = account_for_instance(instance) {
        if let Some(pool) = account.role(role) {
            return pool.exempt_client();
        }
    }
    client(role)
}

/// Observability snapshot: `(account, role, acquires, skips, waits, busy_now)`
/// sorted by account then role.
pub fn admission_stats() -> Vec<(String, Role, u64, u64, u64, usize)> {
    let mut out = Vec::new();
    if let Some(registry) = ACCOUNT_POOLS.get() {
        let mut ids: Vec<&String> = registry.by_account.keys().collect();
        ids.sort();
        for id in ids {
            let p = &registry.by_account[id];
            for role in [Role::Fast, Role::Cancel, Role::Reconcile] {
                let (a, s, w, b) = p.role(role).unwrap().stats_as(role_ctr_index(role));
                out.push((id.clone(), role, a, s, w, b));
            }
        }
    }
    out
}

/// One account's GapReplay counters and per-slot health:
/// `(acquires, skips, busy, [(slot, generation, failures, quarantined)])`.
pub fn gap_replay_stats(
    account_id: &str,
) -> Option<(u64, u64, usize, Vec<(usize, u64, usize, bool)>)> {
    let pool = &account_by_id(account_id)?.gap_replay;
    let (acquires, skips, _, busy) = pool.stats();
    let slots = pool
        .slots
        .iter()
        .enumerate()
        .map(|(i, s)| {
            (
                i,
                s.generation.load(Ordering::Acquire),
                s.transport_failures.load(Ordering::Acquire),
                s.quarantined.load(Ordering::Acquire),
            )
        })
        .collect();
    Some((acquires, skips, busy, slots))
}

/// GapReplay stats for every configured account, sorted by account ID.
pub fn all_gap_replay_stats() -> Vec<(String, u64, u64, usize, Vec<(usize, u64, usize, bool)>)> {
    let Some(registry) = ACCOUNT_POOLS.get() else {
        return Vec::new();
    };
    let mut ids: Vec<&String> = registry.by_account.keys().collect();
    ids.sort();
    ids.into_iter()
        .filter_map(|account_id| {
            gap_replay_stats(account_id).map(|(acquires, skips, busy, slots)| {
                (account_id.clone(), acquires, skips, busy, slots)
            })
        })
        .collect()
}

/// Sum of all account placement and cancel slots. Completion drainers cover
/// both kinds of already-fired requests while their permit remains held.
pub fn total_account_order_capacity() -> usize {
    ACCOUNT_POOLS
        .get()
        .map(|registry| {
            registry
                .by_account
                .values()
                .map(|account| account.fast.slots.len() + account.cancel.slots.len())
                .sum()
        })
        .unwrap_or(0)
}

/// Sum of physically isolated cancel slots. The retained-cancel worker count
/// should track this lane, not placement capacity.
pub fn total_account_cancel_capacity() -> usize {
    ACCOUNT_POOLS
        .get()
        .map(|registry| {
            registry
                .by_account
                .values()
                .map(|account| account.cancel.slots.len())
                .sum()
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_sizes_scale_with_instances() {
        let s1 = AccountPoolSizes::for_instances(1);
        assert_eq!(
            (s1.fast, s1.cancel, s1.reconcile, s1.gap_replay),
            (4, 4, 2, 2)
        );
        let s3 = AccountPoolSizes::for_instances(3);
        assert_eq!(
            (s3.fast, s3.cancel, s3.reconcile, s3.gap_replay),
            (12, 12, 6, 2)
        );
        let s20 = AccountPoolSizes::for_instances(20);
        assert_eq!(
            (s20.fast, s20.cancel, s20.reconcile, s20.gap_replay),
            (80, 80, 40, 2)
        );
    }

    #[test]
    fn global_default_sizes_are_fixed() {
        let d = GlobalPoolSizes::default();
        assert_eq!((d.fallback_order, d.query), (4, 4));
    }

    #[test]
    fn shared_account_instances_resolve_to_one_sized_pool_group() {
        let accounts = HashMap::from([
            (
                "wallet-a".to_string(),
                vec![
                    "btc-2".to_string(),
                    "btc-1".to_string(),
                    "btc-1".to_string(),
                ],
            ),
            ("wallet-b".to_string(), vec!["eth-1".to_string()]),
        ]);
        let registry = build_account_pool_registry(&accounts).unwrap();
        assert_eq!(registry.instance_to_account["btc-1"], "wallet-a");
        assert_eq!(registry.instance_to_account["btc-2"], "wallet-a");
        assert_eq!(registry.instance_to_account["eth-1"], "wallet-b");

        let wallet_a = &registry.by_account["wallet-a"];
        assert_eq!(wallet_a.fast.slots.len(), 8);
        assert_eq!(wallet_a.cancel.slots.len(), 8);
        assert_eq!(wallet_a.reconcile.slots.len(), 4);
        assert_eq!(wallet_a.gap_replay.slots.len(), 2);
        let wallet_b = &registry.by_account["wallet-b"];
        assert_eq!(wallet_b.fast.slots.len(), 4);
        assert_eq!(wallet_b.cancel.slots.len(), 4);
        assert_eq!(wallet_b.reconcile.slots.len(), 2);
        assert_eq!(wallet_b.gap_replay.slots.len(), 2);
    }

    #[test]
    fn instance_cannot_belong_to_two_account_pool_groups() {
        let accounts = HashMap::from([
            ("wallet-a".to_string(), vec!["btc".to_string()]),
            ("wallet-b".to_string(), vec!["btc".to_string()]),
        ]);
        let error = build_account_pool_registry(&accounts).err().unwrap();
        assert!(error.to_string().contains("assigned to both"));
    }

    #[test]
    fn keep_warm_tick_spreads_one_sweep_across_all_slots() {
        assert_eq!(
            keep_warm_tick(Duration::from_secs(20), 1),
            Duration::from_secs(20)
        );
        assert_eq!(
            keep_warm_tick(Duration::from_secs(20), 45),
            Duration::from_nanos(444_444_444),
        );
    }

    #[test]
    fn keep_warm_requires_thirty_seconds_without_any_slot_activity() {
        let pool = Box::leak(Box::new(pool(1)));
        let target = KeepWarmTarget { pool, slot: 0 };
        let last = 10_000u64;
        pool.slots[0]
            .last_activity_ns
            .store(last, Ordering::Release);
        let idle_ns = KEEP_WARM_IDLE.as_nanos() as u64;
        assert!(!target.eligible_at(last + idle_ns - 1));
        assert!(target.eligible_at(last + idle_ns));

        pool.slots[0].last_activity_ns.store(0, Ordering::Release);
        let permit = pool.try_acquire().unwrap();
        let at_start = pool.slots[0].last_activity_ns.load(Ordering::Acquire);
        assert!(at_start > 0, "business acquisition refreshes activity");
        drop(permit);
        assert!(
            pool.slots[0].last_activity_ns.load(Ordering::Acquire) >= at_start,
            "permit release refreshes activity at request completion",
        );
    }

    #[test]
    fn keep_warm_enforces_exact_slot_pool_and_global_limits() {
        static TEST_LOCK: Mutex<()> = Mutex::new(());
        let _serial = TEST_LOCK.lock().unwrap();
        assert_eq!(KEEP_WARM_INFLIGHT.load(Ordering::Acquire), 0);

        let first_pool = Box::leak(Box::new(pool(2)));
        let second_pool = Box::leak(Box::new(pool(1)));
        let third_pool = Box::leak(Box::new(pool(1)));
        for pool in [&*first_pool, &*second_pool, &*third_pool] {
            for slot in &pool.slots {
                slot.last_activity_ns.store(0, Ordering::Release);
            }
        }

        let first = KeepWarmTarget {
            pool: first_pool,
            slot: 0,
        }
        .try_acquire()
        .expect("first global keep-warm lease");
        assert!(first_pool.slots[0].busy.load(Ordering::Acquire));
        assert!(
            KeepWarmTarget {
                pool: first_pool,
                slot: 1
            }
            .try_acquire()
            .is_none(),
            "one pool permits only one concurrent keep-warm",
        );

        let second = KeepWarmTarget {
            pool: second_pool,
            slot: 0,
        }
        .try_acquire()
        .expect("second global keep-warm lease");
        assert_eq!(KEEP_WARM_INFLIGHT.load(Ordering::Acquire), 2);
        assert!(
            KeepWarmTarget {
                pool: third_pool,
                slot: 0
            }
            .try_acquire()
            .is_none(),
            "the process-wide keep-warm limit is two",
        );

        drop(first);
        let third = KeepWarmTarget {
            pool: third_pool,
            slot: 0,
        }
        .try_acquire()
        .expect("global capacity returns after lease release");
        drop(second);
        drop(third);
        assert_eq!(KEEP_WARM_INFLIGHT.load(Ordering::Acquire), 0);
        assert!(!first_pool.keep_warm_inflight.load(Ordering::Acquire));
        assert!(!first_pool.slots[0].busy.load(Ordering::Acquire));
    }

    // ── admission control ──────────────────────────────────────────

    fn pool(n: usize) -> RolePool {
        RolePool::new(n, Duration::from_millis(500), Role::Fast).unwrap()
    }

    fn account(n: usize) -> AccountPools {
        AccountPools {
            fast: pool(n),
            cancel: RolePool::new(n, Duration::from_millis(500), Role::Cancel).unwrap(),
            reconcile: RolePool::new(n, Duration::from_millis(500), Role::Reconcile).unwrap(),
            gap_replay: RolePool::new(
                ACCOUNT_GAP_REPLAY_SLOTS,
                Duration::from_secs(5),
                Role::GapReplay,
            )
            .unwrap(),
        }
    }

    #[test]
    fn admission_acquire_exhaust_release() {
        let p = pool(2);
        let a = p.try_acquire();
        let b = p.try_acquire();
        assert!(a.is_some() && b.is_some(), "first 2 acquires succeed");
        assert!(
            p.try_acquire().is_none(),
            "3rd acquire on a size-2 pool must skip"
        );

        let (acquires, skips, waits, busy) = p.stats();
        assert_eq!(busy, 2, "both slots busy");
        assert_eq!(acquires, 2);
        assert_eq!(skips, 1, "one skip recorded");
        assert_eq!(waits, 0);

        drop(a); // release one slot
        assert!(
            p.try_acquire().is_some(),
            "a released slot must be reusable — no cold connection needed"
        );
    }

    #[test]
    fn admission_never_double_uses_a_slot() {
        // The core no-cold-connection guarantee: a size-N pool hands out
        // at most N concurrent permits, so N warm connections are never
        // over-subscribed.
        let p = pool(3);
        let held: Vec<_> = (0..3).map(|_| p.try_acquire()).collect();
        assert!(held.iter().all(|x| x.is_some()));
        for _ in 0..10 {
            assert!(p.try_acquire().is_none(), "never exceed N in-flight");
        }
        assert_eq!(p.stats().3, 3, "exactly N busy");
    }

    #[test]
    fn admission_attempt_trace_is_slot_owned_and_attempt_keyed() {
        let p = pool(1);
        let first_permit = p.try_acquire().unwrap();
        let first = first_permit.pooled_client().begin_attempt(10, 20, 30, 40);
        first.mark_dispatched(50);
        let snapshot = first.snapshot().unwrap();
        assert_eq!(snapshot.role, Role::Fast);
        assert_eq!(snapshot.slot, 0);
        assert_eq!(
            (
                snapshot.signal_ns,
                snapshot.prep_ns,
                snapshot.signed_ns,
                snapshot.account_recorded_ns,
                snapshot.dispatched_ns,
            ),
            (10, 20, 30, 40, 50),
        );

        drop(first_permit);
        let second_permit = p.try_acquire().unwrap();
        let second = second_permit.pooled_client().begin_attempt(60, 70, 80, 90);
        second.mark_dispatched(100);
        assert_ne!(
            first.attempt_id, second.attempt_id,
            "attempt IDs must be process-monotonic"
        );
        assert!(
            first.snapshot().is_none(),
            "an old handle detects admission-slot reuse instead of reading another attempt"
        );
    }

    #[test]
    fn spillover_probe_does_not_report_a_shed_business_request() {
        let p = pool(1);
        let _held = p.try_acquire().unwrap();
        assert!(p.try_acquire_inner(false, 0).is_none());
        let (acquires, skips, waits, busy) = p.stats();
        assert_eq!((acquires, skips, waits, busy), (1, 0, 0, 1));
    }

    #[test]
    fn retained_acquire_wakes_on_permit_release_without_polling() {
        let p = Arc::new(pool(1));
        let held = p.try_acquire().unwrap();
        let waiter_pool = p.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            let permit = waiter_pool.acquire();
            tx.send(permit.slot()).unwrap();
        });

        assert!(
            rx.recv_timeout(Duration::from_millis(20)).is_err(),
            "retained acquire must wait while the only slot is busy",
        );
        drop(held);
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            0,
            "permit release must wake the retained cancel immediately",
        );
        waiter.join().unwrap();
        let (acquires, skips, waits, busy) = p.stats();
        assert_eq!((acquires, skips, waits, busy), (2, 0, 1, 0));
    }

    #[test]
    fn transported_response_resets_the_slot_failure_streak() {
        let p = pool(1);
        let permit = p.try_acquire().unwrap();
        let health = permit.health(permit.generation());
        assert!(
            health.claim_rebuild(2, Duration::from_secs(30)).is_none(),
            "the first failure stays below the rebuild threshold",
        );
        health.note_transport_success();
        assert_eq!(
            permit.transport_failures.load(Ordering::Acquire),
            0,
            "a transported HTTP response resets the failure streak",
        );
    }

    #[test]
    fn gap_replay_rebuild_policy_requires_threshold_and_cooldown() {
        let p = RolePool::new(1, Duration::from_secs(5), Role::GapReplay).unwrap();
        let permit = p.try_acquire().unwrap();
        let health = permit.health(permit.generation());
        assert!(health.claim_rebuild(2, Duration::from_secs(30)).is_none());
        assert!(health.claim_rebuild(2, Duration::from_secs(30)).is_some());
        assert!(health.claim_rebuild(2, Duration::from_secs(30)).is_none());
        permit.last_rebuild_ns.store(
            activity_now_ns().saturating_sub(Duration::from_secs(31).as_nanos() as u64),
            Ordering::Release,
        );
        assert!(health.claim_rebuild(2, Duration::from_secs(30)).is_some());
    }

    #[test]
    fn gap_replay_role_rotates_slots_and_tracks_generation() {
        let p = RolePool::new(2, Duration::from_secs(5), Role::GapReplay).unwrap();
        let first = p.try_acquire().unwrap();
        assert_eq!(
            (first.role(), first.slot(), first.generation()),
            (Role::GapReplay, 0, 0)
        );
        drop(first);
        let second = p.try_acquire().unwrap();
        assert_eq!((second.slot(), second.generation()), (1, 0));
    }

    #[test]
    fn quarantined_slot_returns_only_after_replacement_install() {
        let p = RolePool::new(1, Duration::from_secs(2), Role::Fast).unwrap();
        let permit = p.try_acquire().unwrap();
        let original = permit.client().clone();
        let health = permit.health(permit.generation());
        let failures = health
            .claim_rebuild(1, Duration::from_secs(30))
            .expect("first failure reaches the test threshold");
        drop(permit);
        assert!(
            p.try_acquire().is_none(),
            "a slot claimed for repair must stay out of admission",
        );

        // Production calls `install_replacement` only after the candidate's
        // async prewarm request and body drain have succeeded.
        let candidate = health.build_replacement().unwrap();
        health.install_replacement(candidate, failures).unwrap();

        let repaired = p.try_acquire().expect("prewarmed slot returns to service");
        assert_eq!(repaired.generation(), 1);
        assert!(
            !Arc::ptr_eq(&original, repaired.client()),
            "the repaired slot must own a fresh reqwest connection pool",
        );
    }

    #[test]
    fn admission_isolated_between_accounts() {
        let a = account(1);
        let b = account(1);
        let held = a.role(Role::Fast).unwrap().try_acquire();
        assert!(held.is_some());
        assert!(
            a.role(Role::Fast).unwrap().try_acquire().is_none(),
            "account A's placement pool is exhausted"
        );
        assert!(
            b.role(Role::Fast).unwrap().try_acquire().is_some(),
            "account B must be unaffected by account A's exhaustion"
        );
    }

    #[test]
    fn admission_fast_and_cancel_are_physically_isolated() {
        let i = account(1);
        // A retained cancel must never consume the last placement slot, and a
        // placement wave must never delay cancellation admission.
        let _fast = i.role(Role::Fast).unwrap().try_acquire().unwrap();
        assert!(
            i.role(Role::Fast).unwrap().try_acquire().is_none(),
            "placement pool exhausted"
        );
        let _cancel = i.role(Role::Cancel).unwrap().try_acquire().unwrap();
        assert!(
            i.role(Role::Cancel).unwrap().try_acquire().is_none(),
            "cancel pool exhausted independently"
        );
        let _rec = i.role(Role::Reconcile).unwrap().try_acquire().unwrap();
        assert!(i.role(Role::Query).is_none());
        assert!(i.role(Role::GapReplay).is_some());
    }

    #[test]
    fn role_pool_keeps_per_logical_role_counters() {
        let p = pool(2);
        let held = p.try_acquire_as(0).unwrap(); // e.g. a place
        let _cxl = p.try_acquire_as(1).unwrap(); // e.g. a cancel
        assert!(p.try_acquire_as(1).is_none(), "shared slots exhausted");
        let (a0, s0, _, busy) = p.stats_as(0);
        let (a1, s1, _, _) = p.stats_as(1);
        assert_eq!((a0, s0), (1, 0), "role-0 counters untouched by role-1 skip");
        assert_eq!((a1, s1), (1, 1), "role-1 sees its own acquire + skip");
        assert_eq!(busy, 2, "busy_now reflects the shared slots");
        drop(held);
        assert!(
            p.try_acquire_as(1).is_some(),
            "released slot serves either role"
        );
    }

    #[test]
    fn exempt_client_never_blocks() {
        let p = pool(1);
        let _held = p.try_acquire().unwrap(); // the only slot is busy
        assert!(p.try_acquire().is_none(), "admission is exhausted");
        // Exempt traffic (heartbeat / cancel-all) must still get a client
        // even when every warm connection is busy. Keep-warm is not exempt.
        let _c1 = p.exempt_client();
        let _c2 = p.exempt_client();
        // (returns without panicking is the assertion; may cold-connect
        //  on actual send, which is the accepted trade for must-complete
        //  traffic.)
    }
}
