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
//! * Each role (`Fast` / `Cancel` / `Reconcile` / `Query` / `GapReplay`) owns a pool
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
//! `_CANCEL` / `_RECONCILE` / `_QUERY` / `_GAP_REPLAY` env vars override
//! individually.
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
//!   [`pooled_clients_all`], so it doubles as this venue's keep-warm — only
//!   venues without such a loop (Aster) need `spawn_keep_warm`.
//! * **Repair**: business requests and keep-warm probes report transport
//!   outcomes through [`PooledClient`]. Two consecutive failures quarantine
//!   the exact slot, build a fresh client in the background, prewarm it, and
//!   only then atomically return it to admission.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

// ── Per-role client-level timeout ceilings ────────────────────────
// FAST/CANCEL are ceilings only: the per-request deadline is chosen by
// `async_rt::current_fast_timeout()` / `current_cancel_timeout()` and is
// always ≤ these values. RECONCILE/QUERY use the client-level value as-is.
const FAST_TIMEOUT_CEILING: Duration = Duration::from_millis(2000);
const CANCEL_TIMEOUT_CEILING: Duration = Duration::from_millis(2000);
const RECONCILE_TIMEOUT: Duration = Duration::from_millis(2000);
const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
const GAP_REPLAY_TIMEOUT: Duration = Duration::from_secs(5);

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
    /// Authenticated Polymarket `/trades` gap recovery. Physically isolated
    /// from Query so replay stalls cannot consume ordinary query connections.
    GapReplay,
}

/// Number of clients (≈ warm connections per host) per role.
#[derive(Clone, Copy, Debug)]
pub struct PoolSizes {
    pub fast: usize,
    pub cancel: usize,
    pub reconcile: usize,
    pub query: usize,
    pub gap_replay: usize,
}

impl Default for PoolSizes {
    fn default() -> Self {
        Self {
            fast: 2, cancel: 2, reconcile: 2, query: 2, gap_replay: 2,
        }
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
            // One primary + one failover per configured instance. Account
            // count is <= instance count, so multi-wallet replay is covered.
            gap_replay: hot,
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
        self.gap_replay = ov("HEXBOT_HTTP_POOL_GAP_REPLAY", self.gap_replay);
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
    fast: RolePool,
    cancel: RolePool,
    reconcile: RolePool,
    query: RolePool,
    gap_replay: RolePool,
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
        fast: RolePool::new(s.fast, FAST_TIMEOUT_CEILING, Role::Fast)?,
        cancel: RolePool::new(s.cancel, CANCEL_TIMEOUT_CEILING, Role::Cancel)?,
        reconcile: RolePool::new(s.reconcile, RECONCILE_TIMEOUT, Role::Reconcile)?,
        query: RolePool::new(s.query, QUERY_TIMEOUT, Role::Query)?,
        gap_replay: RolePool::new(s.gap_replay, GAP_REPLAY_TIMEOUT, Role::GapReplay)?,
    };
    POOLS
        .set(pools)
        .map_err(|_| anyhow::anyhow!("http1_pool already initialised"))?;
    log::info!(
        "[http1_pool] initialised (h1.1) fast={} cancel={} reconcile={} query={} gap_replay={}",
        s.fast, s.cancel, s.reconcile, s.query, s.gap_replay,
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

/// All clients across every role — for prewarm / keep-warm fan-out that
/// must touch *every* underlying connection, not just one pick.
pub fn clients_all() -> Vec<Arc<reqwest::Client>> {
    let p = pools();
    let mut all = p.fast.clients();
    all.extend(p.cancel.clients());
    all.extend(p.reconcile.clients());
    all.extend(p.query.clients());
    all.extend(p.gap_replay.clients());
    // Include the per-instance admission-pool connections so prewarm /
    // keep-warm / heartbeat fan-out holds them open too — they carry real
    // order traffic but go idle between quote waves, and a cold reserved
    // connection would defeat the whole point of admission control.
    if let Some(m) = INSTANCE_POOLS.get() {
        for inst in m.values() {
            for rp in [&inst.fast, &inst.cancel, &inst.reconcile, &inst.query] {
                all.extend(
                    rp.slots
                        .iter()
                        .map(|s| s.client.read().unwrap().clone()),
                );
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
    let mut all = p.fast.pooled_clients();
    all.extend(p.cancel.pooled_clients());
    all.extend(p.reconcile.pooled_clients());
    all.extend(p.query.pooled_clients());
    all.extend(p.gap_replay.pooled_clients());
    if let Some(m) = INSTANCE_POOLS.get() {
        for inst in m.values() {
            for rp in [&inst.fast, &inst.cancel, &inst.reconcile, &inst.query] {
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
        Role::Fast => &p.fast,
        Role::Cancel => &p.cancel,
        Role::Reconcile => &p.reconcile,
        Role::Query => &p.query,
        Role::GapReplay => &p.gap_replay,
    }
}

fn global_role_clients(role: Role) -> Vec<Arc<reqwest::Client>> {
    global_role(role).clients()
}

/// Concurrently establish TCP+TLS state for every client in a global role.
/// Gap replay calls this before its first `/trades` request.
pub async fn prewarm_role(role: Role, warm_url: &str) -> PrewarmReport {
    let clients = global_role_clients(role);
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
    log::info!(
        "[http1_pool] role={:?} prewarm: {}/{} connections up{}",
        role, ok, total,
        first_error.as_deref()
            .map(|e| format!(" (first err: {})", e))
            .unwrap_or_default(),
    );
    PrewarmReport { total, ok, first_error }
}

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
                set.spawn(async move {
                    let response = c.get(&url).send().await?;
                    let status = response.status().as_u16();
                    response.bytes().await?;
                    Ok::<u16, reqwest::Error>(status)
                });
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

// ══════════════════════════════════════════════════════════════════
// Per-instance admission control
// ══════════════════════════════════════════════════════════════════
//
// The role pools above are process-global and shared across strategy
// instances; a request just round-robins a client and, if that client's
// warm connection is busy, hyper opens a **cold** TCP+TLS connection.
// Under wave-overlap or hedge doubling that produces cold-connection
// storms exactly when the endpoint is already slow.
//
// This layer replaces "round-robin + hope" with explicit admission
// control, per (instance, role):
//
//   * Each warm connection is a `Slot` with an exclusive `busy` flag.
//     `try_acquire` hands out at most one in-flight request per slot, so
//     a slot's single warm connection is **never double-dispatched** —
//     no concurrency-driven cold connection is ever opened.
//   * Pools are **per-instance**: instance A exhausting its Fast pool
//     cannot starve or preempt instance B. Roles are independent too
//     (a saturated Cancel pool never blocks a Fast placement).
//   * When all slots are busy, `try_acquire` returns `None` and the
//     caller **skips** (holds the quote) rather than cold-connecting.
//   * `exempt_client` is the escape hatch for must-complete traffic
//     (heartbeat / keep-warm / cancel-all): it always returns a client
//     WITHOUT a permit, accepting a possible cold connection because
//     *completing* the request matters more than avoiding one.

/// One connection slot: a warm h1.1 client + an in-use flag. Held by at
/// most one in-flight request at a time.
struct Slot {
    client: Arc<RwLock<Arc<reqwest::Client>>>,
    busy: Arc<AtomicBool>,
    quarantined: Arc<AtomicBool>,
    transport_failures: Arc<AtomicUsize>,
    generation: Arc<AtomicU64>,
    last_rebuild: Arc<Mutex<Option<Instant>>>,
    timeout: Duration,
}

#[derive(Clone)]
struct ConnectionHealth {
    role: Role,
    slot: usize,
    generation_at_pick: u64,
    slot_client: Arc<RwLock<Arc<reqwest::Client>>>,
    quarantined: Arc<AtomicBool>,
    transport_failures: Arc<AtomicUsize>,
    generation: Arc<AtomicU64>,
    last_rebuild: Arc<Mutex<Option<Instant>>>,
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
        let now = Instant::now();
        let mut last = self.last_rebuild.lock().unwrap();
        if last.map(|t| now.duration_since(t) < cooldown).unwrap_or(false) {
            return None;
        }
        *last = Some(now);
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
        *self.slot_client.write().unwrap() = replacement;
        self.transport_failures.store(0, Ordering::Release);
        let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        self.quarantined.store(false, Ordering::Release);
        log::warn!(
            "[http1_pool] role={:?} slot={} replaced unhealthy client after {} consecutive transport failures generation={}",
            self.role, self.slot, failures, generation,
        );
        Some(generation)
    }

    async fn rebuild_and_prewarm(self, prewarm_url: String, failures: usize) {
        let candidate = match self.build_replacement() {
            Ok(client) => client,
            Err(error) => {
                self.quarantined.store(false, Ordering::Release);
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
        }.await;
        match prewarm_result {
            Ok(()) => {
                if let Some(generation) =
                    self.install_replacement(candidate, failures)
                {
                    log::info!(
                        "[http1_pool] role={:?} slot={} rebuilt and prewarmed generation={} url={}",
                        self.role, self.slot, generation, prewarm_url,
                    );
                }
            }
            Err(error) => {
                self.quarantined.store(false, Ordering::Release);
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
}

impl PooledClient {
    pub fn client(&self) -> &Arc<reqwest::Client> {
        &self.client
    }

    pub fn note_transport_success(&self) {
        self.health.note_transport_success();
    }

    /// Record a transport failure. On the second consecutive failure this
    /// quarantines the slot and starts background rebuild + prewarm. Returns
    /// true when this call claimed the rebuild.
    pub fn note_transport_failure(&self, prewarm_url: String) -> bool {
        let Some(failures) =
            self.health.claim_rebuild(2, Duration::from_secs(30))
        else {
            return false;
        };
        let health = self.health.clone();
        crate::async_rt::handle().spawn(async move {
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
    client: Arc<reqwest::Client>,
    slot_client: Arc<RwLock<Arc<reqwest::Client>>>,
    quarantined: Arc<AtomicBool>,
    transport_failures: Arc<AtomicUsize>,
    generation: Arc<AtomicU64>,
    last_rebuild: Arc<Mutex<Option<Instant>>>,
    timeout: Duration,
}

impl Permit {
    /// The reserved client — dispatch the request on this.
    pub fn client(&self) -> &Arc<reqwest::Client> {
        &self.client
    }

    pub fn role(&self) -> Role { self.role }
    pub fn slot(&self) -> usize { self.slot }
    pub fn generation(&self) -> u64 { self.acquired_generation }

    fn health(&self, generation_at_pick: u64) -> ConnectionHealth {
        ConnectionHealth {
            role: self.role,
            slot: self.slot,
            generation_at_pick,
            slot_client: self.slot_client.clone(),
            quarantined: self.quarantined.clone(),
            transport_failures: self.transport_failures.clone(),
            generation: self.generation.clone(),
            last_rebuild: self.last_rebuild.clone(),
            timeout: self.timeout,
        }
    }

    pub fn pooled_client(&self) -> PooledClient {
        PooledClient {
            client: self.client.clone(),
            health: self.health(self.acquired_generation),
        }
    }

    /// Current client installed in this slot. Reconcile callers use this
    /// accessor because a long batch can rebuild the slot between order GETs.
    pub fn current_client(&self) -> Arc<reqwest::Client> {
        self.slot_client.read().unwrap().clone()
    }

    pub fn current_pooled_client(&self) -> PooledClient {
        let generation = self.generation.load(Ordering::Acquire);
        PooledClient {
            client: self.current_client(),
            health: self.health(generation),
        }
    }

}

impl Drop for Permit {
    fn drop(&mut self) {
        // Release the slot. `Release` pairs with the `Acquire` in the
        // next `try_acquire`'s successful CAS.
        self.flag.store(false, Ordering::Release);
    }
}

/// A pool of N slots for one (instance, role). Concurrency ceiling = N =
/// warm-connection count.
struct RolePool {
    role: Role,
    slots: Vec<Slot>,
    rr: AtomicUsize, // round-robin cursor for exempt (no-permit) picks
    acquires: AtomicU64,
    skips: AtomicU64,
    hedge_acquires: AtomicU64,
    hedge_skips: AtomicU64,
}

impl RolePool {
    fn new(n: usize, timeout: Duration, role: Role) -> Result<Self> {
        let n = n.max(1);
        let mut slots = Vec::with_capacity(n);
        for _ in 0..n {
            slots.push(Slot {
                client: Arc::new(RwLock::new(Arc::new(build_h1_client(timeout)?))),
                busy: Arc::new(AtomicBool::new(false)),
                quarantined: Arc::new(AtomicBool::new(false)),
                transport_failures: Arc::new(AtomicUsize::new(0)),
                generation: Arc::new(AtomicU64::new(0)),
                last_rebuild: Arc::new(Mutex::new(None)),
                timeout,
            });
        }
        Ok(Self {
            role,
            slots,
            rr: AtomicUsize::new(0),
            acquires: AtomicU64::new(0),
            skips: AtomicU64::new(0),
            hedge_acquires: AtomicU64::new(0),
            hedge_skips: AtomicU64::new(0),
        })
    }

    /// Reserve a free slot exclusively, or `None` if all are busy (caller
    /// SKIPS — no cold connection is opened). Binds permit → slot → warm
    /// connection so the connection is never used by two requests at once.
    fn try_acquire(&self) -> Option<Permit> {
        self.try_acquire_counted(false)
    }

    /// Optional duplicate/hedge acquisition. It uses the same slots but keeps
    /// separate observability counters: failure here only suppresses the
    /// duplicate request and must not be reported as a dropped primary.
    fn try_acquire_hedge(&self) -> Option<Permit> {
        self.try_acquire_counted(true)
    }

    fn try_acquire_counted(&self, hedge: bool) -> Option<Permit> {
        let start = self.rr.fetch_add(1, Ordering::Relaxed);
        for offset in 0..self.slots.len() {
            let slot = (start + offset) % self.slots.len();
            let s = &self.slots[slot];
            if s.quarantined.load(Ordering::Acquire) {
                continue;
            }
            if s.busy
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                if hedge {
                    self.hedge_acquires.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.acquires.fetch_add(1, Ordering::Relaxed);
                }
                let acquired_generation = s.generation.load(Ordering::Acquire);
                return Some(Permit {
                    role: self.role,
                    slot,
                    acquired_generation,
                    flag: s.busy.clone(),
                    client: s.client.read().unwrap().clone(),
                    slot_client: s.client.clone(),
                    quarantined: s.quarantined.clone(),
                    transport_failures: s.transport_failures.clone(),
                    generation: s.generation.clone(),
                    last_rebuild: s.last_rebuild.clone(),
                    timeout: s.timeout,
                });
            }
        }
        if hedge {
            self.hedge_skips.fetch_add(1, Ordering::Relaxed);
        } else {
            self.skips.fetch_add(1, Ordering::Relaxed);
        }
        None
    }

    /// Exempt path: a client via round-robin WITHOUT a permit. Always
    /// returns — may cold-connect if every warm connection is busy. For
    /// heartbeat / keep-warm / cancel-all only.
    fn exempt_client(&self) -> Arc<reqwest::Client> {
        self.exempt_pooled_client().client
    }

    fn pooled_client_for_slot(&self, slot: usize) -> PooledClient {
        let state = &self.slots[slot];
        let generation = state.generation.load(Ordering::Acquire);
        PooledClient {
            client: state.client.read().unwrap().clone(),
            health: ConnectionHealth {
                role: self.role,
                slot,
                generation_at_pick: generation,
                slot_client: state.client.clone(),
                quarantined: state.quarantined.clone(),
                transport_failures: state.transport_failures.clone(),
                generation: state.generation.clone(),
                last_rebuild: state.last_rebuild.clone(),
                timeout: state.timeout,
            },
        }
    }

    fn exempt_pooled_client(&self) -> PooledClient {
        let start = self.rr.fetch_add(1, Ordering::Relaxed);
        for offset in 0..self.slots.len() {
            let slot = (start + offset) % self.slots.len();
            if !self.slots[slot].quarantined.load(Ordering::Acquire) {
                return self.pooled_client_for_slot(slot);
            }
        }
        // Must-complete traffic retains the historical escape hatch when all
        // slots are quarantined; its outcome still feeds health tracking.
        self.pooled_client_for_slot(start % self.slots.len())
    }

    fn clients(&self) -> Vec<Arc<reqwest::Client>> {
        self.slots.iter()
            .map(|s| s.client.read().unwrap().clone())
            .collect()
    }

    fn pooled_clients(&self) -> Vec<PooledClient> {
        (0..self.slots.len())
            .map(|slot| self.pooled_client_for_slot(slot))
            .collect()
    }

    /// (primary_acquires, primary_skips, hedge_acquires, hedge_skips,
    /// busy_now) for observability.
    fn stats(&self) -> (u64, u64, u64, u64, usize) {
        let busy = self
            .slots
            .iter()
            .filter(|s| s.busy.load(Ordering::Relaxed))
            .count();
        (
            self.acquires.load(Ordering::Relaxed),
            self.skips.load(Ordering::Relaxed),
            self.hedge_acquires.load(Ordering::Relaxed),
            self.hedge_skips.load(Ordering::Relaxed),
            busy,
        )
    }
}

/// Independent Fast/Cancel/Reconcile/Query pools for one instance.
struct InstancePools {
    fast: RolePool,
    cancel: RolePool,
    reconcile: RolePool,
    query: RolePool,
}

impl InstancePools {
    fn role(&self, role: Role) -> Option<&RolePool> {
        match role {
            Role::Fast => Some(&self.fast),
            Role::Cancel => Some(&self.cancel),
            Role::Reconcile => Some(&self.reconcile),
            Role::Query => Some(&self.query),
            Role::GapReplay => None,
        }
    }
}

static INSTANCE_POOLS: OnceLock<HashMap<String, InstancePools>> = OnceLock::new();

/// Build per-(instance, role) admission-controlled pools. `sizes` is the
/// **per-instance** slot count for each role (not the global `2·M`
/// figure). Each instance gets fully independent pools — no cross-instance
/// sharing or preemption. Call once before use; later calls are ignored
/// (first write wins) and return `Err`.
pub fn init_instance_pools(instances: &[String], sizes: PoolSizes) -> Result<()> {
    let mut map = HashMap::with_capacity(instances.len());
    for id in instances {
        map.insert(
            id.clone(),
            InstancePools {
                fast: RolePool::new(sizes.fast, FAST_TIMEOUT_CEILING, Role::Fast)?,
                cancel: RolePool::new(sizes.cancel, CANCEL_TIMEOUT_CEILING, Role::Cancel)?,
                reconcile: RolePool::new(sizes.reconcile, RECONCILE_TIMEOUT, Role::Reconcile)?,
                query: RolePool::new(sizes.query, QUERY_TIMEOUT, Role::Query)?,
            },
        );
    }
    INSTANCE_POOLS
        .set(map)
        .map_err(|_| anyhow::anyhow!("instance pools already initialised"))?;
    log::info!(
        "[http1_pool] per-instance pools initialised: {} instance(s) × (fast={} cancel={} reconcile={} query={})",
        instances.len(),
        sizes.fast,
        sizes.cancel,
        sizes.reconcile,
        sizes.query,
    );
    Ok(())
}

/// True once `init_instance_pools` has run (per-instance admission is
/// active). Callers fall back to the shared global pool when this is
/// false (e.g. non-poly venues, paper/BT shims).
pub fn instance_pools_ready() -> bool {
    INSTANCE_POOLS.get().is_some()
}

/// Admission control: reserve a warm connection for `(instance, role)`.
///   * `Some(permit)` → dispatch on `permit.client()`, release on drop.
///   * `None`         → all warm connections busy OR unknown instance;
///                      the caller must SKIP (no cold connection).
pub fn try_acquire(instance: &str, role: Role) -> Option<Permit> {
    INSTANCE_POOLS.get()?.get(instance)?.role(role)?.try_acquire()
}

/// Admission control for an optional duplicate/hedge request. A failed hedge
/// acquisition is tracked separately because the primary request is already
/// in flight and no business operation was dropped.
pub fn try_acquire_hedge(instance: &str, role: Role) -> Option<Permit> {
    INSTANCE_POOLS
        .get()?
        .get(instance)?
        .role(role)?
        .try_acquire_hedge()
}

/// Reserve a process-global GapReplay slot with the same exclusive
/// no-cold-connect admission guarantee as per-instance order traffic.
pub fn try_acquire_global(role: Role) -> Option<Permit> {
    match role {
        Role::GapReplay => pools().gap_replay.try_acquire(),
        _ => None,
    }
}

/// Exempt dispatch for must-complete traffic (heartbeat / keep-warm /
/// cancel-all): never blocked by admission, may cold-connect. Falls back
/// to the shared global pool when the instance is unknown / pools not yet
/// initialised.
pub fn exempt_client(instance: &str, role: Role) -> Arc<reqwest::Client> {
    if let Some(p) = INSTANCE_POOLS.get().and_then(|m| m.get(instance)) {
        if let Some(pool) = p.role(role) {
            return pool.exempt_client();
        }
    }
    client(role)
}

/// Observability snapshot: `(instance, role, primary_acquires, primary_skips,
/// hedge_acquires, hedge_skips, busy_now)` sorted by instance then role. Empty
/// until `init_instance_pools` runs.
pub fn admission_stats() -> Vec<(String, Role, u64, u64, u64, u64, usize)> {
    let mut out = Vec::new();
    if let Some(m) = INSTANCE_POOLS.get() {
        let mut ids: Vec<&String> = m.keys().collect();
        ids.sort();
        for id in ids {
            let p = &m[id];
            for role in [Role::Fast, Role::Cancel, Role::Reconcile, Role::Query] {
                let (a, s, ha, hs, b) = p.role(role).unwrap().stats();
                out.push((id.clone(), role, a, s, ha, hs, b));
            }
        }
    }
    out
}

/// GapReplay pool counters and per-slot health:
/// `(acquires, skips, busy, [(slot, generation, failures, quarantined)])`.
pub fn gap_replay_stats() -> (u64, u64, usize, Vec<(usize, u64, usize, bool)>) {
    let pool = &pools().gap_replay;
    let (acquires, skips, _, _, busy) = pool.stats();
    let slots = pool.slots.iter().enumerate()
        .map(|(i, s)| (
            i,
            s.generation.load(Ordering::Acquire),
            s.transport_failures.load(Ordering::Acquire),
            s.quarantined.load(Ordering::Acquire),
        ))
        .collect();
    (acquires, skips, busy, slots)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_scale_with_instances() {
        let s1 = PoolSizes::for_instances(1);
        assert_eq!((s1.fast, s1.cancel, s1.gap_replay), (2, 2, 2));
        let s3 = PoolSizes::for_instances(3);
        assert_eq!((s3.fast, s3.cancel, s3.gap_replay), (6, 6, 6));
        let s20 = PoolSizes::for_instances(20);
        assert_eq!(s20.fast, 16); // clamped
        assert_eq!(s20.reconcile, 8);
        assert_eq!(s20.gap_replay, 16);
    }

    #[test]
    fn default_sizes_are_two() {
        let d = PoolSizes::default();
        assert_eq!(
            (d.fast, d.cancel, d.reconcile, d.query, d.gap_replay),
            (2, 2, 2, 2, 2),
        );
    }

    // ── admission control ──────────────────────────────────────────

    fn pool(n: usize) -> RolePool {
        RolePool::new(n, Duration::from_millis(500), Role::Fast).unwrap()
    }

    fn inst(n: usize) -> InstancePools {
        InstancePools {
            fast: pool(n),
            cancel: pool(n),
            reconcile: pool(n),
            query: pool(n),
        }
    }

    #[test]
    fn admission_acquire_exhaust_release() {
        let p = pool(2);
        let a = p.try_acquire();
        let b = p.try_acquire();
        assert!(a.is_some() && b.is_some(), "first 2 acquires succeed");
        assert!(p.try_acquire().is_none(), "3rd acquire on a size-2 pool must skip");

        let (acquires, skips, hedge_acquires, hedge_skips, busy) = p.stats();
        assert_eq!(busy, 2, "both slots busy");
        assert_eq!(acquires, 2);
        assert_eq!(skips, 1, "one skip recorded");
        assert_eq!(hedge_acquires, 0);
        assert_eq!(hedge_skips, 0);

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
        assert_eq!(p.stats().4, 3, "exactly N busy");
    }

    #[test]
    fn admission_hedge_stats_are_separate_from_primary() {
        let p = pool(2);
        let primary = p.try_acquire();
        let hedge = p.try_acquire_hedge();
        assert!(primary.is_some() && hedge.is_some());
        assert!(p.try_acquire_hedge().is_none());
        assert!(p.try_acquire().is_none());

        let (acquires, skips, hedge_acquires, hedge_skips, busy) = p.stats();
        assert_eq!((acquires, skips), (1, 1));
        assert_eq!((hedge_acquires, hedge_skips), (1, 1));
        assert_eq!(busy, 2);
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
        *permit.last_rebuild.lock().unwrap() =
            Some(Instant::now() - Duration::from_secs(31));
        assert!(health.claim_rebuild(2, Duration::from_secs(30)).is_some());
    }

    #[test]
    fn gap_replay_role_rotates_slots_and_tracks_generation() {
        let p = RolePool::new(2, Duration::from_secs(5), Role::GapReplay).unwrap();
        let first = p.try_acquire().unwrap();
        assert_eq!((first.role(), first.slot(), first.generation()),
                   (Role::GapReplay, 0, 0));
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
    fn admission_per_instance_isolation() {
        let a = inst(1);
        let b = inst(1);
        let held = a.role(Role::Fast).unwrap().try_acquire();
        assert!(held.is_some());
        assert!(
            a.role(Role::Fast).unwrap().try_acquire().is_none(),
            "instance A's Fast pool is exhausted"
        );
        assert!(
            b.role(Role::Fast).unwrap().try_acquire().is_some(),
            "instance B must be unaffected by A's exhaustion — no cross-instance preemption"
        );
    }

    #[test]
    fn admission_role_isolation() {
        let i = inst(1);
        let _fast = i.role(Role::Fast).unwrap().try_acquire().unwrap();
        assert!(
            i.role(Role::Fast).unwrap().try_acquire().is_none(),
            "Fast exhausted"
        );
        assert!(
            i.role(Role::Cancel).unwrap().try_acquire().is_some(),
            "Cancel pool is independent of Fast — a full place pool never blocks a cancel"
        );
        assert!(i.role(Role::Reconcile).unwrap().try_acquire().is_some());
        assert!(i.role(Role::Query).unwrap().try_acquire().is_some());
        assert!(i.role(Role::GapReplay).is_none());
    }

    #[test]
    fn exempt_client_never_blocks() {
        let p = pool(1);
        let _held = p.try_acquire().unwrap(); // the only slot is busy
        assert!(p.try_acquire().is_none(), "admission is exhausted");
        // Exempt traffic (heartbeat / keep-warm / cancel-all) must still
        // get a client even when every warm connection is busy.
        let _c1 = p.exempt_client();
        let _c2 = p.exempt_client();
        // (returns without panicking is the assertion; may cold-connect
        //  on actual send, which is the accepted trade for must-complete
        //  traffic.)
    }
}
