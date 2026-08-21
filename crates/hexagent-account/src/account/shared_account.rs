//! Account-scoped physical/virtual bookkeeping for shared-wallet strategies.
//!
//! One [`SharedAccount`] is owned by one exchange account. It is the
//! admission-control source of truth shared by every strategy instance on the
//! wallet: physical funds/positions are the hard ceiling, while each
//! instance's weighted virtual balance/inventory is its private ceiling.

use arc_swap::{ArcSwap, ArcSwapOption};
use hexagent_types::types::{AuthoritativeOrderAudit, BinaryOption, OrderStatus, Side};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, RwLockWriteGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const EPS: f64 = 1e-9;
/// Polymarket settles quote notional in six-decimal USDC units. The CLOB can
/// therefore report an average fill price a fraction beyond the submitted
/// limit when the integer quote amount is divided back by the matched shares.
/// Compare order limits in quote-notional space and permit at most one atomic
/// quote unit.
const QUOTE_CURRENCY_ATOMIC_UNIT: f64 = 1e-6;
const RECONCILIATION_UNIT: f64 = 1e-6;
const INITIAL_TOKEN_BARRIER_TIMEOUT_MS: u64 = 10_000;
const MANUAL_RISK_BLOCKER: &str = "manual";
const MAINTENANCE_ATTRIBUTION_RISK_BLOCKER_PREFIX: &str = "maintenance_attribution:";
const TRADE_PERSISTENCE_RISK_BLOCKER: &str = "account_persistence:trade";
const FEE_ATTRIBUTION_RISK_BLOCKER_PREFIX: &str = "fee_attribution:";
/// Settled-event FIFO eviction may race a pinned gap replay by many hours.
/// Keep a lightweight, durable ownership proof long after the full order and
/// trade rows have been compacted so an already-applied fill remains an
/// attributable no-op instead of becoming an `unowned trade`.
const RETIRED_TRADE_TOMBSTONE_TTL_MS: u64 = 90 * 24 * 60 * 60 * 1_000;
const MAX_RETIRED_TRADE_TOMBSTONES: usize = 100_000;
const PERSISTENCE_WAL_VERSION: u32 = 1;
const ROUTE_SHARD_COUNT: usize = 64;
const RECENT_VIRTUAL_TRADE_MUTATIONS: usize = 65_536;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct LockMonitoringSnapshot {
    pub wait_last_us: u64,
    pub wait_max_us: u64,
    pub hold_last_us: u64,
    pub hold_max_us: u64,
    pub acquisitions: u64,
}

#[derive(Debug, Default)]
struct LockLatencyMetrics {
    wait_last_us: AtomicU64,
    wait_max_us: AtomicU64,
    hold_last_us: AtomicU64,
    hold_max_us: AtomicU64,
    acquisitions: AtomicU64,
}

impl LockLatencyMetrics {
    fn acquired(&self, wait_started: Instant) -> HeldLockMetric<'_> {
        let acquired_at = Instant::now();
        let wait_us = acquired_at
            .saturating_duration_since(wait_started)
            .as_micros()
            .min(u64::MAX as u128) as u64;
        self.wait_last_us.store(wait_us, Ordering::Relaxed);
        self.wait_max_us.fetch_max(wait_us, Ordering::Relaxed);
        self.acquisitions.fetch_add(1, Ordering::Relaxed);
        HeldLockMetric {
            metrics: self,
            acquired_at,
        }
    }

    fn snapshot(&self) -> LockMonitoringSnapshot {
        LockMonitoringSnapshot {
            wait_last_us: self.wait_last_us.load(Ordering::Relaxed),
            wait_max_us: self.wait_max_us.load(Ordering::Relaxed),
            hold_last_us: self.hold_last_us.load(Ordering::Relaxed),
            hold_max_us: self.hold_max_us.load(Ordering::Relaxed),
            acquisitions: self.acquisitions.load(Ordering::Relaxed),
        }
    }
}

struct HeldLockMetric<'a> {
    metrics: &'a LockLatencyMetrics,
    acquired_at: Instant,
}

impl Drop for HeldLockMetric<'_> {
    fn drop(&mut self) {
        let hold_us = self.acquired_at.elapsed().as_micros().min(u64::MAX as u128) as u64;
        self.metrics.hold_last_us.store(hold_us, Ordering::Relaxed);
        self.metrics
            .hold_max_us
            .fetch_max(hold_us, Ordering::Relaxed);
    }
}

/// A fixed-size ownership index keeps unrelated strategy instances from
/// contending on one account-wide route map. The shard hash is process-local;
/// route entries themselves are rebuilt from the durable lifecycle ledger.
#[derive(Debug)]
struct ShardedRouteMap {
    shards: Box<[RwLock<HashMap<String, String>>]>,
}

impl ShardedRouteMap {
    fn new() -> Self {
        let shards = (0..ROUTE_SHARD_COUNT)
            .map(|_| RwLock::new(HashMap::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { shards }
    }

    fn shard_index(key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize % ROUTE_SHARD_COUNT
    }

    fn get(&self, key: &str) -> Option<String> {
        self.shards[Self::shard_index(key)]
            .read()
            .unwrap()
            .get(key)
            .cloned()
    }

    fn try_get(&self, key: &str) -> Result<Option<String>, ()> {
        self.shards[Self::shard_index(key)]
            .try_read()
            .map(|shard| shard.get(key).cloned())
            .map_err(|_| ())
    }

    fn write_shard(&self, key: &str) -> std::sync::RwLockWriteGuard<'_, HashMap<String, String>> {
        self.shards[Self::shard_index(key)].write().unwrap()
    }

    fn insert(&self, key: String, owner: String) {
        self.write_shard(&key).insert(key, owner);
    }

    fn remove(&self, key: &str) -> Option<String> {
        self.write_shard(key).remove(key)
    }

    fn retain_owner_keys(&self, owner: &str, desired: &HashSet<String>) {
        for shard in &self.shards {
            shard
                .write()
                .unwrap()
                .retain(|key, route_owner| route_owner != owner || desired.contains(key));
        }
    }

    fn retain_owners(&self, owners: &HashSet<String>) {
        for shard in &self.shards {
            shard
                .write()
                .unwrap()
                .retain(|_, route_owner| owners.contains(route_owner));
        }
    }

    fn keys(&self) -> Vec<String> {
        self.shards
            .iter()
            .flat_map(|shard| shard.read().unwrap().keys().cloned().collect::<Vec<_>>())
            .collect()
    }
}

/// Canonical lookup form for Polymarket order hashes. The CLOB has returned
/// the same hash with mixed hex casing and, on some paths, without the `0x`
/// prefix. Keep the original string on [`OrderOwnership`] for API/audit use,
/// but use this form for every ownership-map key and equality check.
pub fn normalize_order_id(order_id: &str) -> String {
    order_id
        .trim()
        .strip_prefix("0x")
        .or_else(|| order_id.trim().strip_prefix("0X"))
        .unwrap_or(order_id.trim())
        .to_ascii_lowercase()
}

fn legacy_orphan_order_coid(reason: &str) -> Option<String> {
    const MARKER: &str = "runtime mapping but no ledger row coid `";
    let (_, tail) = reason.split_once(MARKER)?;
    let (coid, _) = tail.split_once('`')?;
    let coid = coid.trim();
    (!coid.is_empty()).then(|| coid.to_string())
}

fn fill_violates_limit(side: Side, limit_price: f64, fill_price: f64, fill_quantity: f64) -> bool {
    let adverse_price = match side {
        Side::Buy => fill_price - limit_price,
        Side::Sell => limit_price - fill_price,
    };
    if adverse_price <= 0.0 {
        return false;
    }
    let adverse_notional = adverse_price * fill_quantity;
    let arithmetic_slack =
        (fill_price.abs().max(limit_price.abs()) * fill_quantity).max(1.0) * f64::EPSILON * 8.0;
    adverse_notional > QUOTE_CURRENCY_ATOMIC_UNIT + arithmetic_slack
}

fn fixed_point_trade_price_tolerance(reference_price: f64, quantity: f64) -> f64 {
    let arithmetic_slack = reference_price.abs().max(1.0) * f64::EPSILON * 8.0;
    if quantity.is_finite() && quantity > 0.0 {
        (QUOTE_CURRENCY_ATOMIC_UNIT / quantity).max(arithmetic_slack)
    } else {
        arithmetic_slack
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct InstanceAccountSnapshot {
    pub instance_id: String,
    pub weight: f64,
    /// Monotonic generation of the virtual trade ledger represented by this
    /// snapshot. Strategies use it to avoid double-applying late fills.
    pub ledger_generation: u64,
    pub cash: f64,
    pub positions: HashMap<String, f64>,
    pub reserved_cash: f64,
    pub reserved_positions: HashMap<String, f64>,
}

/// Immutable, lock-free view of account-wide authoritative binary outcomes.
/// Writers publish a new `Arc` only when the generation advances; quote and
/// watchdog callbacks therefore never enter the aggregate account lock merely
/// to compare settlement generations.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SettledTokenValuesSnapshot {
    pub generation: u64,
    pub values: HashMap<String, f64>,
}

/// Durable checkpoint for a strategy-owned sidecar whose file is committed
/// before this marker advances. `recovery_payload` is intentionally opaque to
/// the account crate: the owning strategy can use it to reconstruct a missing
/// sidecar, then reconcile that snapshot with the account's durable trades.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableSidecarCheckpoint {
    pub generation: u64,
    pub expected_entries: usize,
    pub recovery_payload: String,
}

/// One event/token scope currently traded by an instance.  These scopes are
/// the authoritative ownership filter used when a cold account snapshot is
/// split into virtual inventories.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenInterest {
    pub instance_id: String,
    pub condition_id: String,
    pub up_token_id: String,
    pub down_token_id: String,
    /// Static configured series shared by every instance expected to register
    /// the same event tokens. Used only as a cold-start allocation barrier.
    #[serde(default)]
    pub scope_key: String,
    /// Retired events remain fetchable for a short grace so delayed platform
    /// redemption is observed on-chain before the scope is discarded.
    #[serde(default)]
    pub retire_after_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ExternalAdjustment {
    pub operation_id: String,
    pub instance_id: String,
    pub cash_delta: f64,
    pub position_deltas: HashMap<String, f64>,
    pub recorded_at_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CashAllocationMigration {
    pub operation_id: String,
    pub target_weights: BTreeMap<String, f64>,
    pub cash_before: BTreeMap<String, f64>,
    pub cash_after: BTreeMap<String, f64>,
    pub recorded_at_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct AccountMonitoringSnapshot {
    pub account_id: String,
    pub seeded: bool,
    pub physical_cash: f64,
    pub virtual_cash: f64,
    pub unallocated_cash: f64,
    pub physical_positions: HashMap<String, f64>,
    pub virtual_positions: HashMap<String, f64>,
    pub unallocated_positions: HashMap<String, f64>,
    pub provisional_position_owners: HashMap<String, String>,
    pub reserved_cash: f64,
    pub reserved_positions: HashMap<String, f64>,
    pub uncertain: bool,
    pub uncertain_reason: Option<String>,
    pub uncertain_since_ms: Option<u64>,
    pub instances: Vec<InstanceAccountSnapshot>,
    pub gap_replay_last_pages: u64,
    pub gap_replay_max_pages: u64,
    pub gap_replay_total_pages: u64,
    pub maintenance_queue_last_wait_ms: u64,
    pub maintenance_queue_max_wait_ms: u64,
    pub maintenance_queue_jobs: u64,
    pub pending_maintenance_operations: usize,
    pub recovery_pending_orders: usize,
    /// Ordinary terminal cancels retaining their full reservation until an
    /// order-specific size-matched audit completes. They do not globally block
    /// unrelated instances sharing the account.
    pub routine_cancel_audits: usize,
    /// Lightweight durable ownership proofs retained after settled history is
    /// economically compacted.
    pub retired_trade_ownership_tombstones: usize,
    /// Exact retired-trade replays that removed their matching ownership
    /// anomaly and caused the account invariants to be recomputed.
    pub verified_trade_replay_recoveries: u64,
    pub persistence_path: Option<PathBuf>,
    pub persistence_error: Option<String>,
    pub persistence_writes: u64,
    pub persistence_write_last_us: u64,
    pub persistence_write_max_us: u64,
    pub persistence_flushes: u64,
    pub persistence_flush_last_us: u64,
    pub persistence_flush_max_us: u64,
    /// Time spent waiting for the account-wide control/state lock. Ordinary
    /// virtual-account order/trade paths do not contribute because they never
    /// acquire this lock.
    pub account_lock_wait_last_us: u64,
    pub account_lock_wait_max_us: u64,
    /// Time the account-wide control transaction held the lock.
    pub account_lock_hold_last_us: u64,
    pub account_lock_hold_max_us: u64,
    pub account_lock_acquisitions: u64,
    /// Reservation fast-path lock timings, split by lock class so a route-map
    /// collision cannot be mistaken for account-ledger contention.
    pub reservation_control_lock: LockMonitoringSnapshot,
    pub reservation_coid_route_lock: LockMonitoringSnapshot,
    pub reservation_oid_route_lock: LockMonitoringSnapshot,
    pub reservation_lifecycle_lock: LockMonitoringSnapshot,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AccountAvailability {
    pub virtual_cash: f64,
    pub physical_cash: f64,
    pub effective_cash: f64,
    pub virtual_position: f64,
    pub physical_position: f64,
    pub effective_position: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderOwnership {
    pub account_id: String,
    pub instance_id: String,
    pub client_order_id: String,
    pub order_id: String,
    pub token_id: String,
    pub side: Side,
    pub quantity: f64,
    pub filled_quantity: f64,
    /// Authoritative terminal `size_matched`. A cancelled order retains only
    /// the still-unobserved portion of this quantity while trades replay.
    #[serde(default)]
    pub terminal_matched_quantity: Option<f64>,
    /// Exact base trade IDs named by the latest authoritative order audit.
    /// Once present, order recovery can fetch only the missing trades instead
    /// of issuing a second order GET to rediscover the same audit.
    #[serde(default)]
    pub terminal_trade_ids: Vec<String>,
    /// Distinguishes an authoritative empty trade set (`size_matched=0`) from
    /// legacy/cancel paths that know a matched quantity but not the complete
    /// associated ID set.
    #[serde(default)]
    pub terminal_trade_ids_authoritative: bool,
    pub price: f64,
    #[serde(default)]
    pub fee_rate_bps: u32,
    pub reserved_cash: f64,
    pub reserved_quantity: f64,
    pub status: OrderStatus,
}

/// Durable recovery input for an order lifecycle event whose process-local
/// oid/coid route survived long enough to receive the event, but whose
/// instance lifecycle row did not.  New anomalies persist this structured
/// hint; startup also synthesizes it from the legacy human-readable reason so
/// ledgers written before the structured field was introduced remain
/// repairable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedOrphanOrderAnomaly {
    pub anomaly_key: String,
    pub order_id: String,
    pub client_order_id: Option<String>,
    /// Asset identity retained from the rejected authenticated lifecycle row.
    /// This allows startup repair to use the durable event-settlement audit
    /// even after the original order/route rows have disappeared.
    pub token_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeOwnership {
    pub account_id: String,
    pub instance_id: String,
    pub trade_key: String,
    pub client_order_id: String,
    pub order_id: String,
    pub token_id: String,
    pub side: Side,
    pub quantity: f64,
    pub price: f64,
    pub status: String,
}

fn validate_owned_trade_replay(
    prior: &TradeOwnership,
    client_order_id: &str,
    order_id: &str,
    token_id: &str,
    side: Side,
    quantity: f64,
    price: f64,
) -> Result<(), String> {
    let normalized_order_id = normalize_order_id(order_id);
    if (!client_order_id.is_empty() && prior.client_order_id != client_order_id)
        || normalized_order_id.is_empty()
        || normalize_order_id(&prior.order_id) != normalized_order_id
        || prior.token_id != token_id
        || prior.side != side
    {
        return Err(format!(
            "trade `{}` lifecycle ownership changed incoming=(coid=`{client_order_id}`,oid=`{order_id}`,token=`{token_id}`,side={side:?}) stored=(coid=`{}`,oid=`{}`,token=`{}`,side={:?})",
            prior.trade_key, prior.client_order_id, prior.order_id, prior.token_id, prior.side,
        ));
    }
    let quantity_tolerance = 1e-8_f64.max(prior.quantity.abs() * 1e-8);
    let price_tolerance = fixed_point_trade_price_tolerance(prior.price, prior.quantity);
    if (prior.quantity - quantity).abs() > quantity_tolerance
        || (prior.price - price).abs() > price_tolerance
    {
        return Err(format!(
            "trade `{}` lifecycle economics changed incoming=(quantity={quantity},price={price}) stored=(quantity={},price={})",
            prior.trade_key, prior.quantity, prior.price,
        ));
    }
    Ok(())
}

/// Result of applying one private-trade lifecycle edge to the durable account.
/// Ownership/accounting validation is deliberately separate from persistence
/// confirmation: a slow fsync must not make callers suppress an already-owned
/// fill, but admission remains risk-off until the scheduled generation lands.
#[derive(Debug, Clone, PartialEq)]
pub enum TradeTransitionResult {
    Applied(TradeOwnership),
    AppliedButPersistencePending(TradeOwnership),
    /// The trade is durably owned and its economic/lifecycle edge was already
    /// applied. Callers must treat this as a valid business event without
    /// broadcasting another fill downstream.
    OwnedNoop(TradeOwnership),
    OwnedNoopButPersistencePending(TradeOwnership),
    Rejected,
}

impl TradeTransitionResult {
    pub fn ownership(&self) -> Option<&TradeOwnership> {
        match self {
            Self::Applied(ownership)
            | Self::AppliedButPersistencePending(ownership)
            | Self::OwnedNoop(ownership)
            | Self::OwnedNoopButPersistencePending(ownership) => Some(ownership),
            Self::Rejected => None,
        }
    }

    pub fn persistence_pending(&self) -> bool {
        matches!(
            self,
            Self::AppliedButPersistencePending(_) | Self::OwnedNoopButPersistencePending(_)
        )
    }

    pub fn owned_noop(&self) -> bool {
        matches!(
            self,
            Self::OwnedNoop(_) | Self::OwnedNoopButPersistencePending(_)
        )
    }
}

enum VirtualTradeAttempt {
    Applied(TradeOwnership),
    OwnedNoop(TradeOwnership),
    /// Unknown/conflicting ownership and retired-history recovery remain cold
    /// control-plane operations so they can install durable anomalies.
    Fallback,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RestoredTrade {
    pub ownership: TradeOwnership,
    pub booked: bool,
    pub usdc_fee: f64,
    pub shares_fee: f64,
    pub virtual_fee_booked: bool,
    pub is_maker: bool,
    pub match_time_secs: u64,
    pub ledger_generation: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReservationError {
    AccountNotSeeded,
    AccountUncertain,
    AccountInstanceBlocked {
        instance_id: String,
        client_order_ids: Vec<String>,
    },
    PersistenceUnavailable(String),
    UnknownInstance(String),
    DuplicateClientOrderId(String),
    InvalidOrder(String),
    InsufficientVirtualCash {
        required: f64,
        available: f64,
    },
    InsufficientPhysicalCash {
        required: f64,
        available: f64,
    },
    InsufficientVirtualPosition {
        token: String,
        required: f64,
        available: f64,
    },
    InsufficientPhysicalPosition {
        token: String,
        required: f64,
        available: f64,
    },
}

/// Edge result for terminal order notifications. Repeated Filled lifecycle
/// rows are common; callers should log only `NewlyPending` as a new audit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillAuditPendingTransition {
    NotTracked,
    Resolved,
    NewlyPending,
    AlreadyPending,
}

impl FillAuditPendingTransition {
    pub fn pending(self) -> bool {
        matches!(self, Self::NewlyPending | Self::AlreadyPending)
    }

    pub fn newly_pending(self) -> bool {
        self == Self::NewlyPending
    }
}

impl std::fmt::Display for ReservationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AccountNotSeeded => write!(f, "shared account has no physical snapshot"),
            Self::AccountUncertain => write!(f, "shared account is awaiting physical reconciliation"),
            Self::AccountInstanceBlocked { instance_id, client_order_ids } => write!(
                f,
                "shared-account instance order-audit metadata pending: instance={instance_id} coids=[{}]",
                client_order_ids.join(","),
            ),
            Self::PersistenceUnavailable(error) => {
                write!(f, "shared-account ledger persistence unavailable: {error}")
            }
            Self::UnknownInstance(id) => write!(f, "unknown shared-account instance `{id}`"),
            Self::DuplicateClientOrderId(id) => write!(f, "duplicate client_order_id `{id}`"),
            Self::InvalidOrder(reason) => write!(f, "invalid order reservation: {reason}"),
            Self::InsufficientVirtualCash { required, available } => write!(
                f, "insufficient instance virtual cash: required={required:.6} available={available:.6}"
            ),
            Self::InsufficientPhysicalCash { required, available } => write!(
                f, "insufficient account physical cash: required={required:.6} available={available:.6}"
            ),
            Self::InsufficientVirtualPosition { token, required, available } => write!(
                f, "insufficient instance virtual position for {token}: required={required:.6} available={available:.6}"
            ),
            Self::InsufficientPhysicalPosition { token, required, available } => write!(
                f, "insufficient account physical position for {token}: required={required:.6} available={available:.6}"
            ),
        }
    }
}

impl std::error::Error for ReservationError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InstanceLedger {
    weight: f64,
    cash: f64,
    positions: HashMap<String, f64>,
    /// Reservations owned by order lifecycle rows only. Maintenance coverage
    /// lives in the operation-scoped fields below and cannot be released by an
    /// order terminal transition.
    reserved_cash: f64,
    reserved_positions: HashMap<String, f64>,
    #[serde(default)]
    maintenance_reserved_cash: f64,
    #[serde(default)]
    maintenance_reserved_positions: HashMap<String, f64>,
    /// Zero identifies a pre operation-scoped ledger whose `reserved_*`
    /// aggregates included both orders and maintenance operations.
    #[serde(default)]
    reservation_scope_version: u8,
    #[serde(default)]
    token_interests: BTreeMap<String, TokenInterest>,
    #[serde(default)]
    market_scopes: HashSet<String>,
}

/// Immutable economic root captured immediately after the first authoritative
/// wallet snapshot is allocated across the configured instances. Every later
/// cash/position mutation must be reproducible from durable journal roots.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct EconomicBalance {
    cash: f64,
    positions: HashMap<String, f64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct AccountEconomicState {
    physical_cash: f64,
    physical_positions: HashMap<String, f64>,
    instances: BTreeMap<String, EconomicBalance>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct AccountSeedBaseline {
    captured_at_ms: u64,
    physical_cash: f64,
    physical_positions: HashMap<String, f64>,
    instances: BTreeMap<String, EconomicBalance>,
    /// Version-1 ledgers predate immutable seed roots. Their first upgraded
    /// load derives an equivalent synthetic root by reversing every durable
    /// economic effect, then persists it exactly once.
    #[serde(default)]
    legacy_derived: bool,
}

impl InstanceLedger {
    fn new(weight: f64) -> Self {
        Self {
            weight,
            cash: 0.0,
            positions: HashMap::new(),
            reserved_cash: 0.0,
            reserved_positions: HashMap::new(),
            maintenance_reserved_cash: 0.0,
            maintenance_reserved_positions: HashMap::new(),
            reservation_scope_version: 1,
            token_interests: BTreeMap::new(),
            market_scopes: HashSet::new(),
        }
    }

    fn total_reserved_cash(&self) -> f64 {
        self.reserved_cash + self.maintenance_reserved_cash
    }

    fn total_reserved_position(&self, token: &str) -> f64 {
        self.reserved_positions.get(token).copied().unwrap_or(0.0)
            + self
                .maintenance_reserved_positions
                .get(token)
                .copied()
                .unwrap_or(0.0)
    }

    fn total_reserved_positions(&self) -> HashMap<String, f64> {
        let mut total = self.reserved_positions.clone();
        for (token, quantity) in &self.maintenance_reserved_positions {
            *total.entry(token.clone()).or_insert(0.0) += *quantity;
        }
        total
    }
}

/// Lock-free f64 cell used by the per-instance admission quota. Account
/// balances are finite and non-negative at every public mutation boundary, so
/// bitwise CAS gives us a small, dependency-free atomic counter.
#[derive(Debug, Default)]
struct AtomicF64(AtomicU64);

impl AtomicF64 {
    fn new(value: f64) -> Self {
        Self(AtomicU64::new(value.to_bits()))
    }

    fn load(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::Acquire))
    }

    fn store(&self, value: f64) {
        self.0.store(value.to_bits(), Ordering::Release);
    }

    fn add(&self, delta: f64) -> f64 {
        let mut current_bits = self.0.load(Ordering::Acquire);
        loop {
            let current = f64::from_bits(current_bits);
            let next = (current + delta).max(0.0);
            match self.0.compare_exchange_weak(
                current_bits,
                next.to_bits(),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return next,
                Err(observed) => current_bits = observed,
            }
        }
    }

    fn ensure_at_least(&self, minimum: f64) -> f64 {
        let mut current_bits = self.0.load(Ordering::Acquire);
        loop {
            let current = f64::from_bits(current_bits);
            if current + EPS >= minimum {
                return current;
            }
            match self.0.compare_exchange_weak(
                current_bits,
                minimum.to_bits(),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return minimum,
                Err(observed) => current_bits = observed,
            }
        }
    }

    fn try_reserve(&self, limit: &AtomicF64, amount: f64) -> Result<f64, f64> {
        let mut current_bits = self.0.load(Ordering::Acquire);
        loop {
            let current = f64::from_bits(current_bits);
            let available = (limit.load() - current).max(0.0);
            if amount > available + EPS {
                return Err(available);
            }
            let next = current + amount;
            match self.0.compare_exchange_weak(
                current_bits,
                next.to_bits(),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(available - amount),
                Err(observed) => current_bits = observed,
            }
        }
    }
}

#[derive(Debug)]
struct VirtualPositionQuota {
    balance: AtomicF64,
    reserved: AtomicF64,
}

impl VirtualPositionQuota {
    fn new(balance: f64, reserved: f64) -> Self {
        Self {
            balance: AtomicF64::new(balance),
            reserved: AtomicF64::new(reserved),
        }
    }
}

#[derive(Debug, Default)]
struct VirtualLifecycle {
    orders: HashMap<String, OrderOwnership>,
    trades: HashMap<String, AppliedTrade>,
    recovery_pending_orders: HashSet<String>,
    startup_query_repair_orders: HashSet<String>,
    routine_cancel_audits: HashSet<String>,
    fee_attribution_pending: HashSet<String>,
    /// Rare malformed terminal audits still install an account-wide ownership
    /// anomaly. This local marker lets a later valid audit detect and repair
    /// that cold-path state without consulting the aggregate on every edge.
    cancel_audit_anomalies: HashSet<String>,
    sidecar_checkpoint: Option<DurableSidecarCheckpoint>,
    /// Bounded, per-instance publication hints for private trades that land
    /// while a cold account transaction is materializing its aggregate copy.
    /// The payload stays typed and tiny; the control-plane publisher uses it
    /// to merge only the touched order/trade/token instead of overwriting the
    /// hot shard or copying the whole account.
    recent_trade_mutations: VecDeque<VirtualTradeMutationHint>,
}

#[derive(Debug, Clone)]
struct VirtualTradeMutationHint {
    epoch: u64,
    trade_key: String,
    client_order_id: String,
    token_id: String,
}

/// Runtime account shard owned by one strategy instance. Ordinary order
/// admission and lifecycle transitions never acquire `SharedAccount::state`;
/// only this shard's metadata mutex and atomic economic counters are touched.
#[derive(Debug)]
struct VirtualAccount {
    instance_id: String,
    weight: AtomicF64,
    cash: AtomicF64,
    reserved_cash: AtomicF64,
    maintenance_reserved_cash: AtomicF64,
    positions: RwLock<HashMap<String, Arc<VirtualPositionQuota>>>,
    maintenance_reserved_positions: RwLock<HashMap<String, f64>>,
    token_interests: Mutex<BTreeMap<String, TokenInterest>>,
    market_scopes: Mutex<HashSet<String>>,
    lifecycle: Mutex<VirtualLifecycle>,
    reservation_publish: Mutex<()>,
    /// Published after a new order row and both route entries are installed.
    /// Cold account snapshots use it to retain reservations admitted while a
    /// control transaction was in progress.
    reservation_epoch: AtomicU64,
    /// Advances after every private-trade mutation. Unlike
    /// `reservation_epoch`, this is paired with typed touched-key hints so a
    /// concurrent cold publisher can preserve both unrelated control changes
    /// and the just-applied fill without taking `control_gate` on the feed.
    trade_epoch: AtomicU64,
}

impl VirtualAccount {
    fn new(instance_id: String, ledger: &InstanceLedger) -> Self {
        let positions = ledger
            .positions
            .iter()
            .map(|(token, balance)| {
                (
                    token.clone(),
                    Arc::new(VirtualPositionQuota::new(
                        *balance,
                        ledger.reserved_positions.get(token).copied().unwrap_or(0.0),
                    )),
                )
            })
            .chain(
                ledger
                    .reserved_positions
                    .iter()
                    .filter(|(token, _)| !ledger.positions.contains_key(*token))
                    .map(|(token, reserved)| {
                        (
                            token.clone(),
                            Arc::new(VirtualPositionQuota::new(0.0, *reserved)),
                        )
                    }),
            )
            .collect();
        Self {
            instance_id,
            weight: AtomicF64::new(ledger.weight),
            cash: AtomicF64::new(ledger.cash),
            reserved_cash: AtomicF64::new(ledger.reserved_cash),
            maintenance_reserved_cash: AtomicF64::new(ledger.maintenance_reserved_cash),
            positions: RwLock::new(positions),
            maintenance_reserved_positions: RwLock::new(
                ledger.maintenance_reserved_positions.clone(),
            ),
            token_interests: Mutex::new(ledger.token_interests.clone()),
            market_scopes: Mutex::new(ledger.market_scopes.clone()),
            lifecycle: Mutex::new(VirtualLifecycle::default()),
            reservation_publish: Mutex::new(()),
            reservation_epoch: AtomicU64::new(0),
            trade_epoch: AtomicU64::new(0),
        }
    }

    fn position(&self, token: &str) -> Arc<VirtualPositionQuota> {
        if let Some(position) = self.positions.read().unwrap().get(token).cloned() {
            return position;
        }
        self.positions
            .write()
            .unwrap()
            .entry(token.to_string())
            .or_insert_with(|| Arc::new(VirtualPositionQuota::new(0.0, 0.0)))
            .clone()
    }

    fn ledger_snapshot(&self) -> InstanceLedger {
        let positions = self.positions.read().unwrap();
        let mut balances = HashMap::with_capacity(positions.len());
        let mut reserved_positions = HashMap::with_capacity(positions.len());
        for (token, quota) in positions.iter() {
            let balance = quota.balance.load();
            let reserved = quota.reserved.load();
            // Preserve zero-valued token keys. Several ownership/redeem paths
            // use presence as the durable proof that an instance owns the
            // token scope even after its economic quantity reaches zero.
            balances.insert(token.clone(), balance);
            reserved_positions.insert(token.clone(), reserved);
        }
        InstanceLedger {
            weight: self.weight.load(),
            cash: self.cash.load(),
            positions: balances,
            reserved_cash: self.reserved_cash.load(),
            reserved_positions,
            maintenance_reserved_cash: self.maintenance_reserved_cash.load(),
            maintenance_reserved_positions: self
                .maintenance_reserved_positions
                .read()
                .unwrap()
                .clone(),
            reservation_scope_version: 1,
            token_interests: self.token_interests.lock().unwrap().clone(),
            market_scopes: self.market_scopes.lock().unwrap().clone(),
        }
    }

    fn replace_ledger(&self, ledger: &InstanceLedger) {
        self.weight.store(ledger.weight);
        self.cash.store(ledger.cash);
        self.reserved_cash.store(ledger.reserved_cash);
        self.maintenance_reserved_cash
            .store(ledger.maintenance_reserved_cash);
        let mut positions = self.positions.write().unwrap();
        positions.clear();
        for token in ledger
            .positions
            .keys()
            .chain(ledger.reserved_positions.keys())
        {
            positions.entry(token.clone()).or_insert_with(|| {
                Arc::new(VirtualPositionQuota::new(
                    ledger.positions.get(token).copied().unwrap_or(0.0),
                    ledger.reserved_positions.get(token).copied().unwrap_or(0.0),
                ))
            });
        }
        *self.maintenance_reserved_positions.write().unwrap() =
            ledger.maintenance_reserved_positions.clone();
        *self.token_interests.lock().unwrap() = ledger.token_interests.clone();
        *self.market_scopes.lock().unwrap() = ledger.market_scopes.clone();
    }

    fn adjust_reservation(&self, token: &str, cash_delta: f64, position_delta: f64) {
        if cash_delta.abs() > EPS {
            self.reserved_cash.add(cash_delta);
        }
        if position_delta.abs() > EPS {
            self.position(token).reserved.add(position_delta);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AppliedTrade {
    ownership: TradeOwnership,
    booked: bool,
    /// Whether the base cash/token delta has reached the physical wallet view.
    /// Old v1 ledgers updated physical state at MATCHED, so a missing field in
    /// an existing JSON snapshot must deserialize as true. New transitions set
    /// this explicitly and advance it only at MINED/CONFIRMED.
    #[serde(default = "default_true")]
    physical_booked: bool,
    /// Taker fees are reported in USDC for SELL and shares for BUY. They are
    /// attached after the strategy resolves the market fee curve, so each has
    /// its own idempotent virtual/physical lifecycle flags.
    #[serde(default)]
    usdc_fee: f64,
    #[serde(default)]
    shares_fee: f64,
    #[serde(default)]
    virtual_fee_booked: bool,
    #[serde(default)]
    physical_fee_booked: bool,
    failed: bool,
    /// A FAILED lifecycle remains a tombstone so stale MATCHED messages cannot
    /// resurrect it. Retained for backward-compatible ledger decoding.
    #[serde(default)]
    failure_reconciled: bool,
    /// Private-feed role used to reproduce fee attribution after restart.
    #[serde(default)]
    is_maker: Option<bool>,
    #[serde(default)]
    match_time_secs: u64,
    /// Last virtual cash/position mutation caused by this trade.
    #[serde(default)]
    ledger_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MaintenanceOperationKind {
    Split,
    Merge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MaintenanceOperationStatus {
    Reserved,
    Submitted,
    Uncertain,
    Confirmed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceOperation {
    pub operation_id: String,
    pub kind: MaintenanceOperationKind,
    pub condition_id: String,
    pub up_token_id: String,
    pub down_token_id: String,
    pub allocations: BTreeMap<String, f64>,
    pub tx_id: Option<String>,
    pub status: MaintenanceOperationStatus,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TokenFeeConfig {
    rate: f64,
    exponent: f64,
}

/// Cross-instance ownership of the late-fill audit retained by each strategy's
/// settled-event FIFO. An empty `instances` set means every strategy has
/// evicted the event, but account cleanup is still waiting for a non-revisable
/// order/trade terminal state.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SettledAuditReference {
    condition_id: String,
    asset_ids: BTreeSet<String>,
    instances: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RiskBlocker {
    reason: String,
    since_ms: u64,
}

/// Identity retained after a terminal trade's economics have been folded into
/// `compacted_economic_effects`. It deliberately contains no state that may be
/// booked again: a matching replay can only prove ownership and no-op; a
/// mismatch remains a sticky ownership anomaly. The immutable ownership and
/// quantity still prove a surviving parent order's derived `filled_quantity`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetiredTradeOwnershipTombstone {
    ownership: TradeOwnership,
    #[serde(default)]
    is_maker: Option<bool>,
    /// A terminal private-feed row whose account address, settled token and
    /// unique historical instance owner were proved after its original order
    /// and trade rows had already aged out. It is intentionally economic-free:
    /// the authoritative wallet snapshot already contains the old trade.
    #[serde(default)]
    authenticated_terminal_noop: bool,
    retired_at_ms: u64,
}

/// Economic-free proof that an authenticated order lookup found a terminal
/// order with an authoritative zero matched quantity and empty trade set.
/// Keeping the proof prevents a late replay of the same private order event
/// from recreating an account-wide blocker after its original lifecycle row
/// has already been retired.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetiredOrderAuditTombstone {
    order_id: String,
    #[serde(default)]
    client_order_id: Option<String>,
    status: OrderStatus,
    original_size: f64,
    /// Historical order lookups can return authoritative not-found after the
    /// exchange has compacted the row.  When a complete authenticated trade
    /// sweep plus event-settlement proof shows that the oid never filled, the
    /// original size is no longer recoverable. Such a tombstone covers any
    /// later zero-fill lifecycle size, but never a matched lifecycle.
    #[serde(default)]
    covers_any_zero_fill_size: bool,
    size_matched: f64,
    #[serde(default)]
    associate_trades: Vec<String>,
    evidence: String,
    audited_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrphanOrderAnomalyHint {
    /// Preserve the authenticated wire spelling (including a possible `0x`
    /// prefix) for the authoritative REST lookup.
    order_id: String,
    #[serde(default)]
    client_order_id: Option<String>,
    #[serde(default)]
    token_id: Option<String>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SharedAccountState {
    seeded: bool,
    /// Immutable root for cold replay of every virtual and physical balance.
    #[serde(default)]
    seed_baseline: Option<AccountSeedBaseline>,
    /// Terminal trade rows are pruned after the settled-event FIFO expires.
    /// Fold their already-validated net effects here so history stays bounded
    /// without making the immutable seed baseline mutable.
    #[serde(default)]
    compacted_economic_effects: AccountEconomicState,
    physical_cash: f64,
    physical_positions: HashMap<String, f64>,
    unallocated_cash: f64,
    unallocated_positions: HashMap<String, f64>,
    /// First-start wallet positions whose event scope is not known yet receive
    /// one deterministic owner instead of disappearing into unallocated state.
    /// The assignment survives restart and remains explicitly provisional
    /// until an operator or later ownership migration resolves it.
    #[serde(default)]
    provisional_position_owners: HashMap<String, String>,
    instances: BTreeMap<String, InstanceLedger>,
    orders: HashMap<String, OrderOwnership>,
    oid_to_coid: HashMap<String, String>,
    trades: HashMap<String, AppliedTrade>,
    #[serde(default)]
    retired_trade_ownership_tombstones: HashMap<String, RetiredTradeOwnershipTombstone>,
    #[serde(default)]
    retired_order_audit_tombstones: HashMap<String, RetiredOrderAuditTombstone>,
    #[serde(default)]
    verified_trade_replay_recoveries: u64,
    /// Advances only when the virtual trade/fee ledger changes.
    #[serde(default)]
    ledger_generation: u64,
    uncertain: bool,
    #[serde(default)]
    uncertain_reason: Option<String>,
    #[serde(default)]
    uncertain_since_ms: Option<u64>,
    /// Risk gates owned by subsystems outside account reconciliation. A normal
    /// balance/trade recomputation cannot clear these; only the source that set
    /// a key may remove it after proving recovery.
    #[serde(default)]
    risk_blockers: BTreeMap<String, RiskBlocker>,
    #[serde(default)]
    external_adjustments: HashMap<String, ExternalAdjustment>,
    #[serde(default)]
    internal_adjustment_sequence: u64,
    #[serde(default)]
    gap_replay_last_pages: u64,
    #[serde(default)]
    gap_replay_max_pages: u64,
    #[serde(default)]
    gap_replay_total_pages: u64,
    #[serde(default)]
    maintenance_queue_last_wait_ms: u64,
    #[serde(default)]
    maintenance_queue_max_wait_ms: u64,
    #[serde(default)]
    maintenance_queue_jobs: u64,
    /// Authoritative settlement value for tokens whose event outcome is known.
    /// Platform-side cash/token deltas may be inferred as redeem only for a
    /// registered winner (value=1), never merely because quantities match.
    #[serde(default)]
    settled_token_values: HashMap<String, f64>,
    /// Monotonic generation for settlement-outcome propagation across
    /// strategy instances and process restarts.
    #[serde(default)]
    settled_token_values_generation: u64,
    /// Persisted per-token fee curves make cold trade replay independent of a
    /// live strategy EventContext.
    #[serde(default)]
    token_fee_configs: HashMap<String, TokenFeeConfig>,
    /// Durable shared reference count for strategy settled-event FIFOs.
    #[serde(default)]
    settled_audit_references: BTreeMap<String, SettledAuditReference>,
    /// Taker trades seen before their fee curve is available remain a sticky
    /// admission blocker rather than silently booking zero fee.
    #[serde(default)]
    fee_attribution_pending: HashSet<String>,
    /// Orders whose exchange terminal state/fill audit has not yet been proved,
    /// including both restored orders and runtime Filled-before-trade races.
    /// These are sticky recovery records: an otherwise matching wallet
    /// snapshot must not clear them. Only rows still lacking authoritative
    /// order metadata block their owner instance; exact private-trade replay
    /// can continue concurrently with admission.
    #[serde(default)]
    recovery_pending_orders: HashSet<String>,
    /// Orders whose otherwise-invalid reservation was conservatively rebuilt
    /// from durable FAILED-trade roots. This marker survives a crash so only
    /// those exact rows remain behind the authoritative startup-query gate.
    #[serde(default)]
    startup_query_repair_orders: HashSet<String>,
    /// Successful ordinary cancels awaiting an authoritative cumulative
    /// `size_matched` read. Kept separate from true trade recovery so normal
    /// cancellation does not pause the shared account.
    #[serde(default)]
    routine_cancel_audits: HashSet<String>,
    /// Persisted ownership for an instance no longer present in config cannot
    /// be silently reassigned without moving that instance's PnL/inventory.
    #[serde(default)]
    instance_registry_issue: Option<String>,
    /// A configured member/weight change on an already-seeded account must be
    /// acknowledged by an explicit, durable cash-budget migration. Token
    /// inventory is deliberately excluded: trade ownership remains immutable.
    #[serde(default)]
    allocation_migration_required: Option<String>,
    #[serde(default)]
    cash_allocation_migrations: BTreeMap<String, CashAllocationMigration>,
    /// Ownership/invariant failures are durable admission blockers. A clean
    /// wallet snapshot cannot prove that a private trade was attributed to
    /// the right instance, so these are cleared only by a correct replay or
    /// the explicit repair API.
    #[serde(default)]
    ownership_anomalies: BTreeMap<String, String>,
    /// normalized order id -> authenticated wire id plus optional client id.
    /// This is deliberately separate from `oid_to_coid`: the latter is an
    /// active lifecycle index, while this map is recovery provenance for a row
    /// that is known to be absent.
    #[serde(default)]
    orphan_order_anomaly_hints: BTreeMap<String, OrphanOrderAnomalyHint>,
    /// Server match_time for private trades whose order ownership could not yet
    /// be resolved. Gap replay must never advance its `after` lower bound past
    /// the earliest row in this durable set.
    #[serde(default)]
    unresolved_trade_match_times: BTreeMap<String, u64>,
    /// Durable split/merge operation journal. Reservations and operation state
    /// change under the same ledger mutex so restart recovery never has to
    /// guess whether an aggregate reservation belongs to an on-chain submit.
    #[serde(default)]
    maintenance_ops: BTreeMap<String, MaintenanceOperation>,
    /// Strategy-owned sidecar commit markers. A non-zero marker makes a missing
    /// sidecar an integrity failure rather than an indistinguishable cold start.
    #[serde(default)]
    sidecar_checkpoints: BTreeMap<String, DurableSidecarCheckpoint>,
    /// Snapshot generations only order concurrent fan-out inside this
    /// process. They deliberately do not survive restart because the fetch
    /// generation counter also restarts.
    #[serde(skip)]
    last_physical_snapshot_generation: u64,
    #[serde(skip)]
    startup_snapshot_applied_this_process: bool,
    #[serde(skip)]
    initial_token_barrier_started_ms: Option<u64>,
    #[serde(skip)]
    initial_token_barrier_degraded_members: Vec<String>,
}

const PERSISTENCE_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedAccount {
    version: u32,
    account_id: String,
    /// Highest WAL generation folded into this snapshot. Legacy snapshots
    /// deserialize as generation zero and are upgraded on the next startup.
    #[serde(default)]
    persistence_generation: u64,
    state: SharedAccountState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum PersistenceWalChange {
    Set {
        path: Vec<String>,
        value: serde_json::Value,
    },
    Remove {
        path: Vec<String>,
    },
    SetInsert {
        path: Vec<String>,
        value: serde_json::Value,
    },
    SetRemove {
        path: Vec<String>,
        value: serde_json::Value,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistenceWalRecord {
    version: u32,
    account_id: String,
    generation: u64,
    changes: Vec<PersistenceWalChange>,
}

#[derive(Debug, Clone)]
struct PersistenceJob {
    generation: u64,
    payload: PersistenceJobPayload,
}

#[derive(Debug, Clone)]
enum PersistenceJobPayload {
    /// Compatibility fallback for cold/complex mutations. The writer snapshots
    /// those under the account lock.
    FullSnapshot,
    Changes(Vec<PersistenceWalChange>),
    /// Raw reservation data is converted to JSON only on the WAL writer. This
    /// keeps path allocation and serde work off the signed-to-dispatch lane.
    Reservation(ReservationPersistenceDelta),
    /// Raw order/status/audit lifecycle mutation. HTTP response, cancel and
    /// background cleanup paths only clone their one owned row under the
    /// instance lock; WAL paths/serde stay on the writer.
    VirtualLifecycle(VirtualLifecyclePersistenceDelta),
    /// Raw virtual-trade data is converted to JSON only on the WAL writer.
    /// The private-feed worker captures a bounded set of owned values while
    /// holding the instance lifecycle shard, then releases that shard before
    /// any path allocation or serde work occurs.
    VirtualTrade(VirtualTradePersistenceDelta),
}

#[derive(Debug, Clone)]
struct ReservationPersistenceDelta {
    instance_id: String,
    client_order_id: String,
    order: OrderOwnership,
    reserved_cash: f64,
    reserved_position: f64,
}

#[derive(Debug, Clone)]
struct VirtualLifecyclePersistenceDelta {
    instance_id: String,
    reserved_cash: f64,
    client_order_id: String,
    order: Option<OrderOwnership>,
    reserved_position: Option<(String, f64)>,
    recovery_pending: bool,
    startup_query_repair: bool,
    routine_cancel_audit: bool,
}

#[derive(Debug, Clone)]
struct VirtualTradePersistenceDelta {
    instance_id: String,
    cash: f64,
    reserved_cash: f64,
    token_id: String,
    position: f64,
    reserved_position: f64,
    client_order_id: String,
    order: Option<OrderOwnership>,
    trade_key: String,
    trade: Option<AppliedTrade>,
    fee_attribution_pending: bool,
    recovery_pending: bool,
    routine_cancel_audit: bool,
    ledger_generation: u64,
}

#[derive(Debug)]
enum PersistenceSignal {
    Wake,
    Shutdown,
}

#[derive(Debug)]
struct PersistenceProgress {
    completed_generation: u64,
    last_error: Option<String>,
    writes: u64,
    write_last_us: u64,
    write_max_us: u64,
}

/// Single-writer incremental WAL. Hot order/trade mutations enqueue typed
/// entry deltas while they already hold the account mutex; the writer never
/// re-locks or clones the full account for those generations. Cold/complex
/// mutations retain a full-snapshot fallback until their domain gets a typed
/// delta. Filesystem I/O is always confined to this thread.
#[derive(Debug)]
struct AccountPersistence {
    path: PathBuf,
    _lock_file: std::fs::File,
    pending: Arc<Mutex<Vec<PersistenceJob>>>,
    wake: std::sync::mpsc::SyncSender<PersistenceSignal>,
    next_generation: Arc<AtomicU64>,
    progress: Arc<(Mutex<PersistenceProgress>, Condvar)>,
    flushes: AtomicU64,
    flush_last_us: AtomicU64,
    flush_max_us: AtomicU64,
    writer: Option<std::thread::JoinHandle<()>>,
}

impl AccountPersistence {
    fn enqueue(&self, payload: PersistenceJobPayload) {
        // Generation assignment and queue insertion are one critical section.
        // Route sharding permits concurrent instance writers; assigning the
        // generation before taking this lock could publish [N+1, N] and make
        // the WAL writer replay absolute virtual-account counters backwards.
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let generation = self.next_generation.fetch_add(1, Ordering::AcqRel) + 1;
        pending.push(PersistenceJob {
            generation,
            payload,
        });
        drop(pending);
        let _ = self.wake.try_send(PersistenceSignal::Wake);
    }

    fn start(
        path: PathBuf,
        account_id: String,
        state: Arc<Mutex<SharedAccountState>>,
        initial_generation: u64,
    ) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                format!("create ledger directory {}: {error}", parent.display())
            })?;
        }
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let lock_path = PathBuf::from(lock_path);
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|error| {
                format!("open account ledger lock {}: {error}", lock_path.display())
            })?;
        fs2::FileExt::try_lock_exclusive(&lock_file).map_err(|error| {
            format!(
                "account ledger {} is already open by another process: {error}",
                path.display(),
            )
        })?;

        // Startup is the only full-snapshot commit. It folds every recovered
        // WAL record and any schema/reconciliation migration into one atomic
        // image, then resets the incremental log before live workers start.
        let initial_state = state.lock().unwrap().clone();
        let initial_snapshot = PersistedAccount {
            version: PERSISTENCE_VERSION,
            account_id: account_id.clone(),
            persistence_generation: initial_generation,
            state: initial_state.clone(),
        };
        write_persisted_account(&path, &initial_snapshot)?;
        reset_persistence_wal(&path)?;
        log::info!(
            "[shared_account] account={} persistence=incremental_wal snapshot={} wal={} generation={}",
            account_id,
            path.display(),
            persistence_wal_path(&path).display(),
            initial_generation,
        );

        let progress = Arc::new((
            Mutex::new(PersistenceProgress {
                completed_generation: initial_generation,
                last_error: None,
                writes: 0,
                write_last_us: 0,
                write_max_us: 0,
            }),
            Condvar::new(),
        ));
        let next_generation = Arc::new(AtomicU64::new(initial_generation));
        let pending = Arc::new(Mutex::new(Vec::<PersistenceJob>::new()));
        let (wake, rx) = std::sync::mpsc::sync_channel::<PersistenceSignal>(1);
        let thread_pending = Arc::clone(&pending);
        let thread_progress = Arc::clone(&progress);
        let thread_path = path.clone();
        let thread_state = Arc::clone(&state);
        let mut durable_state = serde_json::to_value(initial_state).map_err(|error| {
            format!(
                "serialize account ledger WAL baseline {}: {error}",
                path.display()
            )
        })?;
        let writer = std::thread::Builder::new()
            .name(format!(
                "account-ledger-{}",
                path.file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("writer")
            ))
            .spawn(move || {
                hexagent_runtime::os_tune::pin_background("account-ledger-writer");
                let mut durable_wal_len = 0u64;
                while let Ok(signal) = rx.recv() {
                    loop {
                        // Detach the batch so producers append to a fresh Vec
                        // while JSON and disk work proceeds.
                        let mut jobs = {
                            let mut pending = thread_pending
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                            std::mem::take(&mut *pending)
                        };
                        let Some(last) = jobs.last() else {
                            break;
                        };
                        let generation = last.generation;
                        let started = std::time::Instant::now();
                        let result = (|| -> Result<serde_json::Value, String> {
                            let last_full_snapshot = jobs.iter().rposition(|job| {
                                matches!(job.payload, PersistenceJobPayload::FullSnapshot)
                            });
                            let (changes, next_state) = if let Some(full_index) = last_full_snapshot
                            {
                                // schedule_persist() runs while the same state
                                // mutex is held, so once the writer acquires it
                                // this clone includes every queued generation.
                                let snapshot = thread_state
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                                    .clone();
                                let mut next_state =
                                    serde_json::to_value(snapshot).map_err(|error| {
                                        format!(
                                            "serialize account ledger WAL fallback {}: {error}",
                                            thread_path.display()
                                        )
                                    })?;
                                // Virtual-account hot deltas scheduled after
                                // the latest cold snapshot are not present in
                                // `thread_state`. Replay only that suffix on top
                                // of the snapshot; earlier absolute deltas were
                                // already folded by the cold control transaction.
                                for job in jobs.iter().skip(full_index + 1) {
                                    for change in materialize_persistence_job(job)? {
                                        apply_persistence_wal_change(&mut next_state, change)?;
                                    }
                                }
                                (
                                    persistence_json_diff(&durable_state, &next_state),
                                    next_state,
                                )
                            } else {
                                let mut changes = Vec::new();
                                for job in &jobs {
                                    changes.extend(materialize_persistence_job(job)?);
                                }
                                // Validate paths against a detached JSON value.
                                // This can be CPU-heavy for a large ledger, but
                                // never holds the account mutex or delays order
                                // admission/private-feed processing.
                                let mut next_state = durable_state.clone();
                                for change in changes.iter().cloned() {
                                    apply_persistence_wal_change(&mut next_state, change)?;
                                }
                                (changes, next_state)
                            };
                            if !changes.is_empty() {
                                append_persistence_wal(
                                    &thread_path,
                                    &PersistenceWalRecord {
                                        version: PERSISTENCE_WAL_VERSION,
                                        account_id: account_id.clone(),
                                        generation,
                                        changes,
                                    },
                                    &mut durable_wal_len,
                                )?;
                            }
                            Ok(next_state)
                        })();
                        let error = match result {
                            Ok(next_state) => {
                                // Consume directly; avoid cloning the complete
                                // 10-13 MB durable JSON tree a second time.
                                durable_state = next_state;
                                None
                            }
                            Err(error) => {
                                // Failed jobs must stay ahead of work queued
                                // while this batch was being written.
                                let mut pending = thread_pending
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                                jobs.append(&mut *pending);
                                *pending = jobs;
                                Some(error)
                            }
                        };
                        let elapsed_us = started.elapsed().as_micros().min(u64::MAX as u128) as u64;
                        let (lock, cv) = &*thread_progress;
                        let mut progress =
                            lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                        if error.is_none() {
                            progress.completed_generation =
                                progress.completed_generation.max(generation);
                        }
                        progress.last_error = error;
                        progress.writes = progress.writes.saturating_add(1);
                        progress.write_last_us = elapsed_us;
                        progress.write_max_us = progress.write_max_us.max(elapsed_us);
                        cv.notify_all();
                        if progress.last_error.is_some() {
                            break;
                        }
                    }
                    if matches!(signal, PersistenceSignal::Shutdown) {
                        break;
                    }
                }
            })
            .map_err(|error| format!("spawn account ledger writer: {error}"))?;
        Ok(Self {
            path,
            _lock_file: lock_file,
            pending,
            wake,
            next_generation,
            progress,
            flushes: AtomicU64::new(0),
            flush_last_us: AtomicU64::new(0),
            flush_max_us: AtomicU64::new(0),
            writer: Some(writer),
        })
    }

    fn schedule(&self) {
        self.enqueue(PersistenceJobPayload::FullSnapshot);
    }

    fn schedule_delta(&self, changes: Vec<PersistenceWalChange>) {
        if changes.is_empty() {
            return;
        }
        self.enqueue(PersistenceJobPayload::Changes(changes));
    }

    fn schedule_reservation(&self, delta: ReservationPersistenceDelta) {
        self.enqueue(PersistenceJobPayload::Reservation(delta));
    }

    fn schedule_virtual_lifecycle(&self, delta: VirtualLifecyclePersistenceDelta) {
        self.enqueue(PersistenceJobPayload::VirtualLifecycle(delta));
    }

    fn schedule_virtual_trade(&self, delta: VirtualTradePersistenceDelta) {
        self.enqueue(PersistenceJobPayload::VirtualTrade(delta));
    }

    fn scheduled_generation(&self) -> u64 {
        self.next_generation.load(Ordering::Acquire)
    }

    fn generation_is_durable(&self, generation: u64) -> bool {
        if generation == 0 {
            return true;
        }
        self.progress
            .0
            .lock()
            .map(|progress| {
                progress.completed_generation >= generation && progress.last_error.is_none()
            })
            .unwrap_or(false)
    }

    fn flush(&self, timeout: Duration) -> Result<(), String> {
        let started = std::time::Instant::now();
        let record_latency = || {
            let elapsed_us = started.elapsed().as_micros().min(u64::MAX as u128) as u64;
            self.flushes.fetch_add(1, Ordering::Relaxed);
            self.flush_last_us.store(elapsed_us, Ordering::Relaxed);
            self.flush_max_us.fetch_max(elapsed_us, Ordering::Relaxed);
        };
        let target = self.next_generation.load(Ordering::Relaxed);
        if target == 0 {
            record_latency();
            return Ok(());
        }
        let _ = self.wake.try_send(PersistenceSignal::Wake);
        let (lock, cv) = &*self.progress;
        let progress = lock.lock().unwrap();
        let (progress, wait) = cv
            .wait_timeout_while(progress, timeout, |p| {
                p.completed_generation < target && p.last_error.is_none()
            })
            .map_err(|_| "account ledger writer progress lock poisoned".to_string())?;
        if progress.completed_generation < target && wait.timed_out() {
            record_latency();
            return Err(format!(
                "timed out persisting generation {target} to {}",
                self.path.display()
            ));
        }
        if let Some(error) = &progress.last_error {
            record_latency();
            return Err(error.clone());
        }
        record_latency();
        Ok(())
    }

    fn last_error(&self) -> Option<String> {
        self.progress
            .0
            .lock()
            .ok()
            .and_then(|p| p.last_error.clone())
    }

    fn is_current(&self) -> Result<bool, String> {
        let target = self.scheduled_generation();
        let progress = self
            .progress
            .0
            .lock()
            .map_err(|_| "account ledger writer progress lock poisoned".to_string())?;
        if let Some(error) = &progress.last_error {
            return Err(error.clone());
        }
        Ok(progress.completed_generation >= target)
    }

    fn metrics(&self) -> (u64, u64, u64, u64, u64, u64) {
        let (writes, write_last_us, write_max_us) = self
            .progress
            .0
            .lock()
            .map(|progress| {
                (
                    progress.writes,
                    progress.write_last_us,
                    progress.write_max_us,
                )
            })
            .unwrap_or_default();
        (
            writes,
            write_last_us,
            write_max_us,
            self.flushes.load(Ordering::Relaxed),
            self.flush_last_us.load(Ordering::Relaxed),
            self.flush_max_us.load(Ordering::Relaxed),
        )
    }
}

impl Drop for AccountPersistence {
    fn drop(&mut self) {
        // A blocking shutdown send cannot lose a coalesced wake: if the queue
        // is full, the writer first consumes that wake and drains the latest
        // generation before receiving Shutdown.
        let _ = self.wake.send(PersistenceSignal::Shutdown);
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
    }
}

fn persistence_wal_path(path: &Path) -> PathBuf {
    let mut wal = path.as_os_str().to_os_string();
    wal.push(".wal");
    PathBuf::from(wal)
}

fn persistence_checksum(bytes: &[u8]) -> u64 {
    // FNV-1a is deliberately simple and stable across Rust releases. This is
    // corruption/torn-write detection, not an authenticity boundary.
    let mut checksum = 0xcbf29ce484222325u64;
    for byte in bytes {
        checksum ^= u64::from(*byte);
        checksum = checksum.wrapping_mul(0x100000001b3);
    }
    checksum
}

fn persistence_json_diff(
    durable: &serde_json::Value,
    next: &serde_json::Value,
) -> Vec<PersistenceWalChange> {
    fn visit(
        durable: &serde_json::Value,
        next: &serde_json::Value,
        path: &mut Vec<String>,
        changes: &mut Vec<PersistenceWalChange>,
    ) {
        if durable == next {
            return;
        }
        match (durable.as_object(), next.as_object()) {
            (Some(durable), Some(next)) => {
                let keys: BTreeSet<&String> = durable.keys().chain(next.keys()).collect();
                for key in keys {
                    path.push(key.clone());
                    match (durable.get(key), next.get(key)) {
                        (Some(before), Some(after)) => visit(before, after, path, changes),
                        (None, Some(value)) => changes.push(PersistenceWalChange::Set {
                            path: path.clone(),
                            value: value.clone(),
                        }),
                        (Some(_), None) => {
                            changes.push(PersistenceWalChange::Remove { path: path.clone() })
                        }
                        (None, None) => unreachable!("union key must exist in one object"),
                    }
                    path.pop();
                }
            }
            _ => changes.push(PersistenceWalChange::Set {
                path: path.clone(),
                value: next.clone(),
            }),
        }
    }

    let mut changes = Vec::new();
    visit(durable, next, &mut Vec::new(), &mut changes);
    changes
}

fn persistence_wal_set<T: Serialize>(
    changes: &mut Vec<PersistenceWalChange>,
    path: impl IntoIterator<Item = String>,
    value: &T,
) -> Result<(), String> {
    let path: Vec<String> = path.into_iter().collect();
    let value = serde_json::to_value(value).map_err(|error| {
        format!(
            "serialize typed account WAL delta {}: {error}",
            path.join("/")
        )
    })?;
    changes.push(PersistenceWalChange::Set { path, value });
    Ok(())
}

fn persistence_wal_map_entry<T: Serialize>(
    changes: &mut Vec<PersistenceWalChange>,
    map: &str,
    key: &str,
    value: Option<&T>,
) -> Result<(), String> {
    let path = vec![map.to_string(), key.to_string()];
    if let Some(value) = value {
        persistence_wal_set(changes, path, value)
    } else {
        changes.push(PersistenceWalChange::Remove { path });
        Ok(())
    }
}

fn persistence_wal_set_membership<T: Serialize>(
    changes: &mut Vec<PersistenceWalChange>,
    set: &str,
    value: &T,
    present: bool,
) -> Result<(), String> {
    let value = serde_json::to_value(value)
        .map_err(|error| format!("serialize typed account WAL set member {set}: {error}"))?;
    let path = vec![set.to_string()];
    changes.push(if present {
        PersistenceWalChange::SetInsert { path, value }
    } else {
        PersistenceWalChange::SetRemove { path, value }
    });
    Ok(())
}

fn materialize_persistence_job(job: &PersistenceJob) -> Result<Vec<PersistenceWalChange>, String> {
    match &job.payload {
        PersistenceJobPayload::FullSnapshot => {
            Err("full snapshot persistence job cannot be materialized as a typed delta".to_string())
        }
        PersistenceJobPayload::Changes(changes) => Ok(changes.clone()),
        PersistenceJobPayload::Reservation(delta) => {
            let mut changes = Vec::with_capacity(7);
            persistence_wal_set(
                &mut changes,
                [
                    "instances".to_string(),
                    delta.instance_id.clone(),
                    "reserved_cash".to_string(),
                ],
                &delta.reserved_cash,
            )?;
            persistence_wal_map_entry(
                &mut changes,
                "orders",
                &delta.client_order_id,
                Some(&delta.order),
            )?;
            persistence_wal_set(
                &mut changes,
                [
                    "instances".to_string(),
                    delta.instance_id.clone(),
                    "reserved_positions".to_string(),
                    delta.order.token_id.clone(),
                ],
                &delta.reserved_position,
            )?;
            persistence_wal_map_entry(
                &mut changes,
                "oid_to_coid",
                &normalize_order_id(&delta.order.order_id),
                Some(&delta.client_order_id),
            )?;
            // A newly reserved order cannot yet be in any audit set. Emitting
            // explicit removes makes replay deterministic if a coid is ever
            // reused after an operator-led repair.
            for set in [
                "recovery_pending_orders",
                "startup_query_repair_orders",
                "routine_cancel_audits",
            ] {
                persistence_wal_set_membership(&mut changes, set, &delta.client_order_id, false)?;
            }
            Ok(changes)
        }
        PersistenceJobPayload::VirtualLifecycle(delta) => {
            let mut changes = Vec::with_capacity(8);
            persistence_wal_set(
                &mut changes,
                [
                    "instances".to_string(),
                    delta.instance_id.clone(),
                    "reserved_cash".to_string(),
                ],
                &delta.reserved_cash,
            )?;
            persistence_wal_map_entry(
                &mut changes,
                "orders",
                &delta.client_order_id,
                delta.order.as_ref(),
            )?;
            if let Some((token_id, reserved_position)) = &delta.reserved_position {
                persistence_wal_set(
                    &mut changes,
                    [
                        "instances".to_string(),
                        delta.instance_id.clone(),
                        "reserved_positions".to_string(),
                        token_id.clone(),
                    ],
                    reserved_position,
                )?;
                let normalized = delta
                    .order
                    .as_ref()
                    .map(|order| normalize_order_id(&order.order_id))
                    .unwrap_or_default();
                persistence_wal_map_entry(
                    &mut changes,
                    "oid_to_coid",
                    &normalized,
                    delta.order.as_ref().map(|order| &order.client_order_id),
                )?;
            }
            for (set, present) in [
                ("recovery_pending_orders", delta.recovery_pending),
                ("startup_query_repair_orders", delta.startup_query_repair),
                ("routine_cancel_audits", delta.routine_cancel_audit),
            ] {
                persistence_wal_set_membership(&mut changes, set, &delta.client_order_id, present)?;
            }
            Ok(changes)
        }
        PersistenceJobPayload::VirtualTrade(delta) => {
            let mut changes = Vec::with_capacity(10);
            persistence_wal_set(
                &mut changes,
                [
                    "instances".to_string(),
                    delta.instance_id.clone(),
                    "cash".to_string(),
                ],
                &delta.cash,
            )?;
            persistence_wal_set(
                &mut changes,
                [
                    "instances".to_string(),
                    delta.instance_id.clone(),
                    "reserved_cash".to_string(),
                ],
                &delta.reserved_cash,
            )?;
            persistence_wal_set(
                &mut changes,
                [
                    "instances".to_string(),
                    delta.instance_id.clone(),
                    "positions".to_string(),
                    delta.token_id.clone(),
                ],
                &delta.position,
            )?;
            persistence_wal_set(
                &mut changes,
                [
                    "instances".to_string(),
                    delta.instance_id.clone(),
                    "reserved_positions".to_string(),
                    delta.token_id.clone(),
                ],
                &delta.reserved_position,
            )?;
            persistence_wal_map_entry(
                &mut changes,
                "orders",
                &delta.client_order_id,
                delta.order.as_ref(),
            )?;
            persistence_wal_map_entry(
                &mut changes,
                "trades",
                &delta.trade_key,
                delta.trade.as_ref(),
            )?;
            persistence_wal_set_membership(
                &mut changes,
                "fee_attribution_pending",
                &delta.trade_key,
                delta.fee_attribution_pending,
            )?;
            persistence_wal_set_membership(
                &mut changes,
                "recovery_pending_orders",
                &delta.client_order_id,
                delta.recovery_pending,
            )?;
            persistence_wal_set_membership(
                &mut changes,
                "routine_cancel_audits",
                &delta.client_order_id,
                delta.routine_cancel_audit,
            )?;
            persistence_wal_set(
                &mut changes,
                ["ledger_generation".to_string()],
                &delta.ledger_generation,
            )?;
            Ok(changes)
        }
    }
}

fn apply_persistence_wal_change(
    state: &mut serde_json::Value,
    change: PersistenceWalChange,
) -> Result<(), String> {
    let set_change = match &change {
        PersistenceWalChange::SetInsert { path, value } => Some((true, path, value)),
        PersistenceWalChange::SetRemove { path, value } => Some((false, path, value)),
        _ => None,
    };
    if let Some((insert, path, value)) = set_change {
        let mut target = state;
        for component in path {
            target = target
                .as_object_mut()
                .and_then(|object| object.get_mut(component))
                .ok_or_else(|| {
                    format!(
                        "account ledger WAL set path has missing/non-object parent: {}",
                        path.join("/")
                    )
                })?;
        }
        let array = target.as_array_mut().ok_or_else(|| {
            format!(
                "account ledger WAL set path is not an array: {}",
                path.join("/")
            )
        })?;
        if insert {
            if !array.contains(value) {
                array.push(value.clone());
            }
        } else {
            array.retain(|member| member != value);
        }
        return Ok(());
    }
    let (path, value) = match change {
        PersistenceWalChange::Set { path, value } => (path, Some(value)),
        PersistenceWalChange::Remove { path } => (path, None),
        PersistenceWalChange::SetInsert { .. } | PersistenceWalChange::SetRemove { .. } => {
            unreachable!("set membership changes handled above")
        }
    };
    if path.is_empty() {
        let Some(value) = value else {
            return Err("account ledger WAL cannot remove the state root".to_string());
        };
        *state = value;
        return Ok(());
    }

    let (key, parents) = path.split_last().expect("empty WAL path handled above");
    let mut target = state;
    for component in parents {
        target = target
            .as_object_mut()
            .and_then(|object| object.get_mut(component))
            .ok_or_else(|| {
                format!(
                    "account ledger WAL path has missing/non-object parent: {}",
                    path.join("/")
                )
            })?;
    }
    let object = target.as_object_mut().ok_or_else(|| {
        format!(
            "account ledger WAL path parent is not an object: {}",
            path.join("/")
        )
    })?;
    if let Some(value) = value {
        object.insert(key.clone(), value);
    } else {
        // Typed jobs may coalesce repeated removal of the same map entry before
        // the writer drains them. Removal is intentionally idempotent.
        object.remove(key);
    }
    Ok(())
}

fn append_persistence_wal(
    path: &Path,
    record: &PersistenceWalRecord,
    durable_wal_len: &mut u64,
) -> Result<(), String> {
    use std::io::{Seek as _, Write as _};

    let payload = serde_json::to_vec(record).map_err(|error| {
        format!(
            "serialize account ledger WAL {}: {error}",
            persistence_wal_path(path).display()
        )
    })?;
    let header = format!("{} {:016x} ", payload.len(), persistence_checksum(&payload));
    let mut frame = Vec::with_capacity(header.len() + payload.len() + 1);
    frame.extend_from_slice(header.as_bytes());
    frame.extend_from_slice(&payload);
    frame.push(b'\n');

    let wal_path = persistence_wal_path(path);
    let mut wal = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&wal_path)
        .map_err(|error| format!("open account ledger WAL {}: {error}", wal_path.display()))?;
    // A prior failed write may have left an incomplete frame. Rewind to the
    // last fsync acknowledged by this writer before appending its replacement.
    wal.set_len(*durable_wal_len).map_err(|error| {
        format!(
            "truncate account ledger WAL {} to {}: {error}",
            wal_path.display(),
            *durable_wal_len
        )
    })?;
    wal.seek(std::io::SeekFrom::Start(*durable_wal_len))
        .map_err(|error| format!("seek account ledger WAL {}: {error}", wal_path.display()))?;
    wal.write_all(&frame)
        .map_err(|error| format!("append account ledger WAL {}: {error}", wal_path.display()))?;
    wal.sync_data()
        .map_err(|error| format!("sync account ledger WAL {}: {error}", wal_path.display()))?;
    *durable_wal_len = durable_wal_len.saturating_add(frame.len() as u64);
    Ok(())
}

#[derive(Debug, Clone)]
struct IncompleteMaintenanceCashWalEvidence {
    generation: u64,
    operation_id: String,
    cash_corrections: BTreeMap<String, f64>,
}

#[derive(Debug, Clone)]
struct StaleVirtualTradeSnapshotWalEvidence {
    generation: u64,
    instance_id: String,
    cash_correction: f64,
    position_corrections: BTreeMap<String, f64>,
}

fn json_value_at_path<'a>(
    state: &'a serde_json::Value,
    path: &[String],
) -> Option<&'a serde_json::Value> {
    path.iter()
        .try_fold(state, |value, component| value.as_object()?.get(component))
}

fn economic_effects_match(left: &AccountEconomicState, right: &AccountEconomicState) -> bool {
    let mut instance_ids: HashSet<String> = left.instances.keys().cloned().collect();
    instance_ids.extend(right.instances.keys().cloned());
    instance_ids.into_iter().all(|instance_id| {
        let left = left.instances.get(&instance_id);
        let right = right.instances.get(&instance_id);
        let left_cash = left.map_or(0.0, |balance| balance.cash);
        let right_cash = right.map_or(0.0, |balance| balance.cash);
        if (left_cash - right_cash).abs() > reconciliation_tolerance(left_cash, right_cash) {
            return false;
        }
        let mut tokens = HashSet::new();
        if let Some(balance) = left {
            tokens.extend(balance.positions.keys().cloned());
        }
        if let Some(balance) = right {
            tokens.extend(balance.positions.keys().cloned());
        }
        tokens.into_iter().all(|token| {
            let left = left
                .and_then(|balance| balance.positions.get(&token))
                .copied()
                .unwrap_or(0.0);
            let right = right
                .and_then(|balance| balance.positions.get(&token))
                .copied()
                .unwrap_or(0.0);
            (left - right).abs() <= reconciliation_tolerance(left, right)
        })
    })
}

/// Detect the old full-account snapshot race from durable WAL evidence. A
/// lifecycle-only trade advance (for example MATCHED -> MINED) has identical
/// immutable virtual economics. Therefore a typed virtual-trade frame that
/// changes its owner's cash or token balance while making only such trade
/// advances proves that a stale cold snapshot was published over the owner
/// shard before this frame was captured.
fn stale_virtual_trade_snapshot_wal_evidence(
    record: &PersistenceWalRecord,
    state_before: &serde_json::Value,
) -> Result<Vec<StaleVirtualTradeSnapshotWalEvidence>, String> {
    if record.changes.iter().any(|change| {
        let path = match change {
            PersistenceWalChange::Set { path, .. }
            | PersistenceWalChange::Remove { path }
            | PersistenceWalChange::SetInsert { path, .. }
            | PersistenceWalChange::SetRemove { path, .. } => path,
        };
        matches!(
            path.first().map(String::as_str),
            Some(
                "maintenance_ops"
                    | "external_adjustments"
                    | "compacted_economic_effects"
                    | "cash_allocation_migrations"
            )
        )
    }) {
        return Ok(Vec::new());
    }

    let mut lifecycle_only_scopes = BTreeSet::<(String, String)>::new();
    let mut saw_trade_change = false;
    for change in &record.changes {
        let PersistenceWalChange::Set { path, value } = change else {
            if matches!(change, PersistenceWalChange::Remove { path } if path.first().is_some_and(|component| component == "trades"))
            {
                return Ok(Vec::new());
            }
            continue;
        };
        if path.len() != 2 || path[0] != "trades" {
            continue;
        }
        saw_trade_change = true;
        let Some(previous_value) = json_value_at_path(state_before, path) else {
            // A newly booked or restored trade is an economic mutation, not
            // proof of the lifecycle-only stale-snapshot failure.
            return Ok(Vec::new());
        };
        let previous: AppliedTrade =
            serde_json::from_value(previous_value.clone()).map_err(|error| {
                format!(
                    "decode prior trade `{}` before WAL generation {}: {error}",
                    path[1], record.generation,
                )
            })?;
        let next: AppliedTrade = serde_json::from_value(value.clone()).map_err(|error| {
            format!(
                "decode trade `{}` in WAL generation {}: {error}",
                path[1], record.generation,
            )
        })?;
        if previous.ownership.instance_id != next.ownership.instance_id
            || previous.ownership.token_id != next.ownership.token_id
            || !economic_effects_match(
                &trade_economic_effect(&previous),
                &trade_economic_effect(&next),
            )
        {
            return Ok(Vec::new());
        }
        lifecycle_only_scopes.insert((
            next.ownership.instance_id.clone(),
            next.ownership.token_id.clone(),
        ));
    }
    if !saw_trade_change {
        return Ok(Vec::new());
    }

    let mut evidence = Vec::new();
    for (instance_id, token_id) in lifecycle_only_scopes {
        let cash_path = vec![
            "instances".to_string(),
            instance_id.clone(),
            "cash".to_string(),
        ];
        let position_path = vec![
            "instances".to_string(),
            instance_id.clone(),
            "positions".to_string(),
            token_id.clone(),
        ];
        let last_set_number = |expected_path: &[String]| {
            record.changes.iter().rev().find_map(|change| match change {
                PersistenceWalChange::Set { path, value } if path == expected_path => {
                    value.as_f64()
                }
                _ => None,
            })
        };
        let correction = |path: &[String]| -> Result<f64, String> {
            let Some(next) = last_set_number(path) else {
                return Ok(0.0);
            };
            let previous = json_value_at_path(state_before, path)
                .and_then(serde_json::Value::as_f64)
                .ok_or_else(|| {
                    format!(
                        "WAL generation {} has no prior numeric value for `{}`",
                        record.generation,
                        path.join("/"),
                    )
                })?;
            Ok(previous - next)
        };
        let cash_correction = correction(&cash_path)?;
        let position_correction = correction(&position_path)?;
        if cash_correction.abs() <= EPS && position_correction.abs() <= EPS {
            continue;
        }
        evidence.push(StaleVirtualTradeSnapshotWalEvidence {
            generation: record.generation,
            instance_id,
            cash_correction,
            position_corrections: BTreeMap::from([(token_id, position_correction)]),
        });
    }
    Ok(evidence)
}

fn incomplete_maintenance_cash_wal_evidence(
    record: &PersistenceWalRecord,
    state: &serde_json::Value,
) -> Result<Vec<IncompleteMaintenanceCashWalEvidence>, String> {
    let confirmed_operations: BTreeSet<String> = record
        .changes
        .iter()
        .filter_map(|change| match change {
            PersistenceWalChange::Set { path, value }
                if path.len() == 3
                    && path[0] == "maintenance_ops"
                    && path[2] == "status"
                    && value == "Confirmed" =>
            {
                Some(path[1].clone())
            }
            _ => None,
        })
        .collect();
    if confirmed_operations.is_empty() {
        return Ok(Vec::new());
    }
    let changed = |expected: &[&str]| {
        record.changes.iter().any(|change| match change {
            PersistenceWalChange::Set { path, .. } | PersistenceWalChange::Remove { path } => {
                path.iter().map(String::as_str).eq(expected.iter().copied())
            }
            PersistenceWalChange::SetInsert { .. } | PersistenceWalChange::SetRemove { .. } => {
                false
            }
        })
    };
    let mut evidence = Vec::new();
    for operation_id in confirmed_operations {
        let operation_value = state
            .get("maintenance_ops")
            .and_then(|operations| operations.get(&operation_id))
            .ok_or_else(|| {
                format!(
                    "maintenance confirmation WAL generation {} has no operation `{operation_id}`",
                    record.generation,
                )
            })?;
        let operation: MaintenanceOperation = serde_json::from_value(operation_value.clone())
            .map_err(|error| {
                format!(
                    "decode maintenance operation `{operation_id}` after WAL generation {}: {error}",
                    record.generation,
                )
            })?;
        let physical_complete = changed(&["physical_cash"])
            && changed(&["physical_positions", &operation.up_token_id])
            && changed(&["physical_positions", &operation.down_token_id]);
        if !physical_complete {
            continue;
        }
        let direction = match operation.kind {
            MaintenanceOperationKind::Split => -1.0,
            MaintenanceOperationKind::Merge => 1.0,
        };
        let mut cash_corrections = BTreeMap::new();
        for (instance_id, amount) in &operation.allocations {
            let positions_complete = changed(&[
                "instances",
                instance_id,
                "positions",
                &operation.up_token_id,
            ]) && changed(&[
                "instances",
                instance_id,
                "positions",
                &operation.down_token_id,
            ]);
            let reservation_complete = match operation.kind {
                MaintenanceOperationKind::Split => {
                    changed(&["instances", instance_id, "maintenance_reserved_cash"])
                }
                MaintenanceOperationKind::Merge => {
                    changed(&[
                        "instances",
                        instance_id,
                        "maintenance_reserved_positions",
                        &operation.up_token_id,
                    ]) && changed(&[
                        "instances",
                        instance_id,
                        "maintenance_reserved_positions",
                        &operation.down_token_id,
                    ])
                }
            };
            if positions_complete
                && reservation_complete
                && !changed(&["instances", instance_id, "cash"])
            {
                cash_corrections.insert(instance_id.clone(), direction * *amount);
            }
        }
        if !cash_corrections.is_empty() {
            evidence.push(IncompleteMaintenanceCashWalEvidence {
                generation: record.generation,
                operation_id,
                cash_corrections,
            });
        }
    }
    Ok(evidence)
}

fn repair_incomplete_maintenance_cash_from_wal(
    account_id: &str,
    state: &mut SharedAccountState,
    evidence: &[IncompleteMaintenanceCashWalEvidence],
) -> bool {
    if evidence.is_empty() || !state.seeded {
        return false;
    }
    let Ok(replayed) = replay_account_economics(state) else {
        return false;
    };
    let current = current_account_economics(state);
    let mut corrections = BTreeMap::<String, f64>::new();
    for item in evidence {
        for (instance_id, correction) in &item.cash_corrections {
            *corrections.entry(instance_id.clone()).or_insert(0.0) += *correction;
        }
    }
    let mut instance_ids: BTreeSet<String> = current.instances.keys().cloned().collect();
    instance_ids.extend(replayed.instances.keys().cloned());
    instance_ids.extend(corrections.keys().cloned());
    for instance_id in &instance_ids {
        let current_balance = current.instances.get(instance_id);
        let replayed_balance = replayed.instances.get(instance_id);
        let empty = HashMap::new();
        if compare_economic_positions(
            &format!("instance `{instance_id}` positions during maintenance WAL recovery"),
            current_balance.map_or(&empty, |balance| &balance.positions),
            replayed_balance.map_or(&empty, |balance| &balance.positions),
        )
        .is_err()
        {
            return false;
        }
        let stored_cash = current_balance.map_or(0.0, |balance| balance.cash);
        let expected_cash = replayed_balance.map_or(0.0, |balance| balance.cash);
        let correction = corrections.get(instance_id).copied().unwrap_or(0.0);
        if (stored_cash + correction - expected_cash).abs()
            > reconciliation_tolerance(stored_cash + correction, expected_cash)
        {
            return false;
        }
    }
    for (instance_id, correction) in corrections {
        let Some(instance) = state.instances.get_mut(&instance_id) else {
            return false;
        };
        instance.cash += correction;
    }
    let recovered: Vec<String> = evidence
        .iter()
        .map(|item| format!("{}@{}", item.operation_id, item.generation))
        .collect();
    log::warn!(
        "[shared_account] account={} repaired incomplete maintenance WAL cash publication operations={:?}",
        account_id,
        recovered,
    );
    true
}

fn repair_stale_virtual_trade_snapshots_from_wal(
    account_id: &str,
    state: &mut SharedAccountState,
    evidence: &[StaleVirtualTradeSnapshotWalEvidence],
) -> bool {
    if evidence.is_empty() || !state.seeded {
        return false;
    }
    let Ok(replayed) = replay_account_economics(state) else {
        return false;
    };
    let current = current_account_economics(state);
    let mut cash_corrections = BTreeMap::<String, f64>::new();
    let mut position_corrections = BTreeMap::<String, BTreeMap<String, f64>>::new();
    for item in evidence {
        *cash_corrections
            .entry(item.instance_id.clone())
            .or_insert(0.0) += item.cash_correction;
        let instance = position_corrections
            .entry(item.instance_id.clone())
            .or_default();
        for (token, correction) in &item.position_corrections {
            *instance.entry(token.clone()).or_insert(0.0) += *correction;
        }
    }

    // The immutable seed plus durable economic roots remains authoritative,
    // but recovery is allowed only when the WAL-proven stale publications
    // explain every replay mismatch exactly. Any extra mismatch still fails
    // closed in validate_persisted_state.
    let mut instance_ids: HashSet<String> = current.instances.keys().cloned().collect();
    instance_ids.extend(replayed.instances.keys().cloned());
    instance_ids.extend(cash_corrections.keys().cloned());
    instance_ids.extend(position_corrections.keys().cloned());
    for instance_id in &instance_ids {
        let stored = current.instances.get(instance_id);
        let expected = replayed.instances.get(instance_id);
        let stored_cash = stored.map_or(0.0, |balance| balance.cash);
        let expected_cash = expected.map_or(0.0, |balance| balance.cash);
        let corrected_cash =
            stored_cash + cash_corrections.get(instance_id).copied().unwrap_or(0.0);
        if (corrected_cash - expected_cash).abs()
            > reconciliation_tolerance(corrected_cash, expected_cash)
        {
            return false;
        }
        let mut tokens = HashSet::new();
        if let Some(balance) = stored {
            tokens.extend(balance.positions.keys().cloned());
        }
        if let Some(balance) = expected {
            tokens.extend(balance.positions.keys().cloned());
        }
        if let Some(corrections) = position_corrections.get(instance_id) {
            tokens.extend(corrections.keys().cloned());
        }
        for token in tokens {
            let stored_position = stored
                .and_then(|balance| balance.positions.get(&token))
                .copied()
                .unwrap_or(0.0);
            let expected_position = expected
                .and_then(|balance| balance.positions.get(&token))
                .copied()
                .unwrap_or(0.0);
            let corrected_position = stored_position
                + position_corrections
                    .get(instance_id)
                    .and_then(|corrections| corrections.get(&token))
                    .copied()
                    .unwrap_or(0.0);
            if (corrected_position - expected_position).abs()
                > reconciliation_tolerance(corrected_position, expected_position)
            {
                return false;
            }
        }
    }

    for (instance_id, correction) in cash_corrections {
        let Some(instance) = state.instances.get_mut(&instance_id) else {
            return false;
        };
        instance.cash += correction;
    }
    for (instance_id, corrections) in position_corrections {
        let Some(instance) = state.instances.get_mut(&instance_id) else {
            return false;
        };
        for (token, correction) in corrections {
            *instance.positions.entry(token).or_insert(0.0) += correction;
        }
    }
    recompute_reconciliation(state, "stale virtual trade WAL snapshot recovery");
    let recovered: Vec<String> = evidence
        .iter()
        .map(|item| format!("{}@{}", item.instance_id, item.generation))
        .collect();
    log::warn!(
        "[shared_account] account={} repaired stale virtual-trade WAL snapshot(s) evidence={:?}",
        account_id,
        recovered,
    );
    true
}

fn replay_persistence_wal(path: &Path, persisted: &mut PersistedAccount) -> Result<(), String> {
    use std::io::BufRead as _;

    let wal_path = persistence_wal_path(path);
    let wal = match std::fs::File::open(&wal_path) {
        Ok(wal) => wal,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(format!(
                "open account ledger WAL {}: {error}",
                wal_path.display()
            ));
        }
    };
    let mut state = serde_json::to_value(&persisted.state).map_err(|error| {
        format!(
            "serialize account ledger state before WAL replay {}: {error}",
            wal_path.display()
        )
    })?;
    let snapshot_generation = persisted.persistence_generation;
    let mut applied_generation = snapshot_generation;
    let mut incomplete_maintenance_cash = Vec::new();
    let mut stale_virtual_trade_snapshots = Vec::new();
    let mut reader = std::io::BufReader::new(wal);
    let mut line_number = 0usize;
    loop {
        let mut frame = Vec::new();
        let bytes_read = reader
            .read_until(b'\n', &mut frame)
            .map_err(|error| format!("read account ledger WAL {}: {error}", wal_path.display()))?;
        if bytes_read == 0 {
            break;
        }
        line_number += 1;
        if frame.last() != Some(&b'\n') {
            log::warn!(
                "[shared_account] ignoring torn final WAL frame path={} line={} bytes={}",
                wal_path.display(),
                line_number,
                frame.len(),
            );
            break;
        }
        frame.pop();
        let Some(first_space) = frame.iter().position(|byte| *byte == b' ') else {
            return Err(format!(
                "invalid account ledger WAL frame {}:{}: missing length delimiter",
                wal_path.display(),
                line_number
            ));
        };
        let Some(second_relative) = frame[first_space + 1..]
            .iter()
            .position(|byte| *byte == b' ')
        else {
            return Err(format!(
                "invalid account ledger WAL frame {}:{}: missing checksum delimiter",
                wal_path.display(),
                line_number
            ));
        };
        let second_space = first_space + 1 + second_relative;
        let expected_len = std::str::from_utf8(&frame[..first_space])
            .ok()
            .and_then(|text| text.parse::<usize>().ok())
            .ok_or_else(|| {
                format!(
                    "invalid account ledger WAL frame {}:{}: bad payload length",
                    wal_path.display(),
                    line_number
                )
            })?;
        let expected_checksum = std::str::from_utf8(&frame[first_space + 1..second_space])
            .ok()
            .and_then(|text| u64::from_str_radix(text, 16).ok())
            .ok_or_else(|| {
                format!(
                    "invalid account ledger WAL frame {}:{}: bad checksum",
                    wal_path.display(),
                    line_number
                )
            })?;
        let payload = &frame[second_space + 1..];
        if payload.len() != expected_len {
            return Err(format!(
                "invalid account ledger WAL frame {}:{}: payload length {} != {}",
                wal_path.display(),
                line_number,
                payload.len(),
                expected_len
            ));
        }
        if persistence_checksum(payload) != expected_checksum {
            return Err(format!(
                "invalid account ledger WAL frame {}:{}: checksum mismatch",
                wal_path.display(),
                line_number
            ));
        }
        let record: PersistenceWalRecord = serde_json::from_slice(payload).map_err(|error| {
            format!(
                "parse account ledger WAL frame {}:{}: {error}",
                wal_path.display(),
                line_number
            )
        })?;
        if record.version != PERSISTENCE_WAL_VERSION {
            return Err(format!(
                "unsupported account ledger WAL version {} in {}:{} (expected {})",
                record.version,
                wal_path.display(),
                line_number,
                PERSISTENCE_WAL_VERSION
            ));
        }
        if record.account_id != persisted.account_id {
            return Err(format!(
                "account ledger WAL {}:{} belongs to `{}`, not `{}`",
                wal_path.display(),
                line_number,
                record.account_id,
                persisted.account_id
            ));
        }
        // A crash after snapshot rename but before WAL truncation leaves old
        // frames behind. The snapshot generation makes them idempotently stale.
        if record.generation <= snapshot_generation {
            continue;
        }
        if record.generation <= applied_generation {
            return Err(format!(
                "non-monotonic account ledger WAL generation {} after {} in {}:{}",
                record.generation,
                applied_generation,
                wal_path.display(),
                line_number
            ));
        }
        stale_virtual_trade_snapshots
            .extend(stale_virtual_trade_snapshot_wal_evidence(&record, &state)?);
        for change in record.changes.iter().cloned() {
            apply_persistence_wal_change(&mut state, change).map_err(|error| {
                format!(
                    "apply account ledger WAL frame {}:{}: {error}",
                    wal_path.display(),
                    line_number
                )
            })?;
        }
        incomplete_maintenance_cash
            .extend(incomplete_maintenance_cash_wal_evidence(&record, &state)?);
        applied_generation = record.generation;
    }
    if applied_generation > snapshot_generation {
        persisted.state = serde_json::from_value(state).map_err(|error| {
            format!(
                "decode account ledger state after WAL replay {}: {error}",
                wal_path.display()
            )
        })?;
        persisted.persistence_generation = applied_generation;
    }
    repair_incomplete_maintenance_cash_from_wal(
        &persisted.account_id,
        &mut persisted.state,
        &incomplete_maintenance_cash,
    );
    repair_stale_virtual_trade_snapshots_from_wal(
        &persisted.account_id,
        &mut persisted.state,
        &stale_virtual_trade_snapshots,
    );
    Ok(())
}

fn reset_persistence_wal(path: &Path) -> Result<(), String> {
    let wal_path = persistence_wal_path(path);
    let wal = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&wal_path)
        .map_err(|error| format!("reset account ledger WAL {}: {error}", wal_path.display()))?;
    wal.sync_all().map_err(|error| {
        format!(
            "sync reset account ledger WAL {}: {error}",
            wal_path.display()
        )
    })?;
    let parent = wal_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("sync WAL directory {}: {error}", parent.display()))
}

fn write_persisted_account(path: &Path, snapshot: &PersistedAccount) -> Result<(), String> {
    let bytes = serde_json::to_vec(snapshot)
        .map_err(|error| format!("serialize account ledger {}: {error}", path.display()))?;
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    {
        use std::io::Write as _;
        let mut file = std::fs::File::create(&tmp)
            .map_err(|error| format!("create {}: {error}", tmp.display()))?;
        file.write_all(&bytes)
            .map_err(|error| format!("write {}: {error}", tmp.display()))?;
        file.sync_all()
            .map_err(|error| format!("sync {}: {error}", tmp.display()))?;
    }
    std::fs::rename(&tmp, path)
        .map_err(|error| format!("rename {} -> {}: {error}", tmp.display(), path.display()))?;
    // fsyncing only the file does not make the rename durable across sudden
    // power loss. Persist the parent directory entry as the final commit step.
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("sync ledger directory {}: {error}", parent.display()))
}

/// Thread-safe account ledger shared by every strategy instance on one wallet.
#[derive(Debug)]
pub struct SharedAccount {
    account_id: String,
    state: Arc<Mutex<SharedAccountState>>,
    /// Cold account-wide mutations (wallet snapshots, maintenance and explicit
    /// allocation migrations) take the write side. Ordinary order paths take
    /// only a shared guard and then mutate one virtual account shard.
    control_gate: RwLock<()>,
    virtual_accounts: RwLock<BTreeMap<String, Arc<VirtualAccount>>>,
    coid_routes: ShardedRouteMap,
    oid_routes: ShardedRouteMap,
    /// Read-mostly ownership index for private-fill ids. This is deliberately
    /// separate from the physical ledger lock: it prevents one exchange trade
    /// from being booked into two instance shards without serializing unrelated
    /// fills on `SharedAccount::state`.
    trade_routes: ShardedRouteMap,
    /// Only anomalous trade ids are present. Exact replays for those ids take
    /// the cold audit path until a verified replay clears the durable anomaly.
    anomalous_trade_keys: RwLock<HashSet<String>>,
    /// Exact private-event anomaly keys mirrored from the durable account.
    /// Successful ordinary events can skip the account-wide reconciliation
    /// lock unless their own key is known to need repair.
    anomalous_private_event_keys: RwLock<HashSet<String>>,
    /// Read-mostly membership mirror for sticky risk blockers.  Normal
    /// private fills clear a trade-scoped blocker defensively even though one
    /// was almost never installed.  Missing in this set is authoritative, so
    /// that common no-op never materialises/clones the aggregate account.
    risk_blocker_sources_fast: RwLock<HashSet<String>>,
    /// Only trade ids whose replay watermark still needs cold-ledger repair
    /// are present. A normal private fill therefore misses in one route shard
    /// instead of taking `control_gate` and cloning/publishing the full
    /// account state merely to discover there is nothing to remove.
    unresolved_trade_keys: ShardedRouteMap,
    /// Serializes the rare mark/resolve transition so a corrected replay
    /// cannot race between the fast anomaly index and its durable row.
    private_anomaly_transition: Mutex<()>,
    /// Fee curves are account-scoped control data but read on every taker-fill
    /// hot path. The cold account transaction refreshes this read-mostly copy.
    token_fee_configs_fast: RwLock<HashMap<String, TokenFeeConfig>>,
    seeded_fast: AtomicBool,
    uncertain_fast: AtomicBool,
    admission_fast: AtomicBool,
    passive_admission_fast: AtomicBool,
    /// Monotonic for one process: once the startup wallet snapshot is applied,
    /// hot readiness checks remain a single acquire load until restart.
    startup_snapshot_applied_fast: AtomicBool,
    /// Cold startup wallet reconciliation must wait while any trade or
    /// maintenance lifecycle can still advance physical economics. Strategy
    /// workers read this without entering the aggregate account transaction.
    startup_snapshot_deferred_fast: AtomicBool,
    /// Account-wide outcome map published by the cold control plane.
    settled_token_values_fast: ArcSwap<SettledTokenValuesSnapshot>,
    settled_token_values_generation_fast: AtomicU64,
    uncertain_reason_fast: ArcSwapOption<String>,
    ledger_generation_fast: AtomicU64,
    /// Cached after startup and every settled-history GC transaction. Reading
    /// monitoring data must not walk tens of thousands of tombstones while
    /// holding the account-wide control/state lock.
    retired_trade_tombstone_count_fast: AtomicUsize,
    /// Empty settled-audit references that may be ready for terminal-history
    /// retirement.  Keeping this worklist separate from the durable map means
    /// a GC wakeup never scans every historical event merely to discover that
    /// no cleanup is pending.
    settled_gc_candidates: Mutex<BTreeMap<String, BTreeSet<String>>>,
    settled_gc_candidate_count_fast: AtomicUsize,
    /// Lock-free token membership snapshot for the terminal private-event
    /// path. Candidate publication is rare and runs outside account apply;
    /// terminal trade messages only load this immutable set.
    settled_gc_candidate_tokens_fast: ArcSwap<HashSet<String>>,
    persistence: Option<AccountPersistence>,
    /// Highest account-persistence generation containing a trade mutation.
    /// Trade ingestion never waits for this generation: subsequent admission
    /// paths inspect writer progress non-blockingly, install the source-owned
    /// blocker only after an actual writer failure, and clear it once a later
    /// generation is durable.
    trade_persistence_pending_generation: AtomicU64,
    /// Edge flag for the source-owned persistence blocker. Without this, the
    /// first quote after every successfully persisted trade took the cold
    /// account lock merely to discover there was no blocker to clear.
    trade_persistence_blocker_active: AtomicBool,
    /// Edge-triggered wakeup for the account-scoped order-audit worker. The
    /// generation prevents missed notifications between the worker's health
    /// snapshot and its wait call.
    order_audit_wakeup: (Mutex<u64>, Condvar),
    account_lock_wait_last_us: AtomicU64,
    account_lock_wait_max_us: AtomicU64,
    account_lock_hold_last_us: AtomicU64,
    account_lock_hold_max_us: AtomicU64,
    account_lock_acquisitions: AtomicU64,
    reservation_control_lock: LockLatencyMetrics,
    reservation_coid_route_lock: LockLatencyMetrics,
    reservation_oid_route_lock: LockLatencyMetrics,
    reservation_lifecycle_lock: LockLatencyMetrics,
}

struct AccountStateGuard<'a> {
    account: &'a SharedAccount,
    _control: RwLockWriteGuard<'a, ()>,
    state: MutexGuard<'a, SharedAccountState>,
    instance_scope: Option<String>,
    reservation_epochs: BTreeMap<String, u64>,
    trade_epochs: BTreeMap<String, u64>,
    acquired_at: Instant,
}

impl Deref for AccountStateGuard<'_> {
    type Target = SharedAccountState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl DerefMut for AccountStateGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl Drop for AccountStateGuard<'_> {
    fn drop(&mut self) {
        if let Some(instance_id) = self.instance_scope.as_deref() {
            self.account.sync_state_to_virtual_account(
                &mut self.state,
                instance_id,
                self.reservation_epochs.get(instance_id).copied(),
                self.trade_epochs.get(instance_id).copied(),
            );
        } else {
            self.account.sync_state_to_virtual_accounts(
                &mut self.state,
                Some(&self.reservation_epochs),
                Some(&self.trade_epochs),
            );
        }
        let hold_us = self.acquired_at.elapsed().as_micros().min(u64::MAX as u128) as u64;
        self.account
            .account_lock_hold_last_us
            .store(hold_us, Ordering::Relaxed);
        self.account
            .account_lock_hold_max_us
            .fetch_max(hold_us, Ordering::Relaxed);
        self.account.publish_control_snapshots(&self.state);
    }
}

impl SharedAccount {
    fn effective_settled_token_values_generation(state: &SharedAccountState) -> u64 {
        if state.settled_token_values.is_empty() {
            state.settled_token_values_generation
        } else {
            // Ledgers written before this field existed load it as zero.
            state.settled_token_values_generation.max(1)
        }
    }

    /// Publish read-mostly control state after a cold transaction. This runs
    /// while the transaction still owns the state guard, so readers observing
    /// a new generation also observe the matching immutable values map.
    fn publish_control_snapshots(&self, state: &SharedAccountState) {
        self.startup_snapshot_applied_fast.store(
            state.startup_snapshot_applied_this_process,
            Ordering::Release,
        );
        self.startup_snapshot_deferred_fast.store(
            has_unsettled_trade_lifecycle(state) || has_unsettled_maintenance_operation(state),
            Ordering::Release,
        );
        let current_reason = self.uncertain_reason_fast.load_full();
        if current_reason.as_deref().map(String::as_str) != state.uncertain_reason.as_deref() {
            self.uncertain_reason_fast
                .store(state.uncertain_reason.clone().map(Arc::new));
        }
        let generation = Self::effective_settled_token_values_generation(state);
        if self
            .settled_token_values_generation_fast
            .load(Ordering::Acquire)
            != generation
        {
            self.settled_token_values_fast
                .store(Arc::new(SettledTokenValuesSnapshot {
                    generation,
                    values: state.settled_token_values.clone(),
                }));
            self.settled_token_values_generation_fast
                .store(generation, Ordering::Release);
        }
    }

    #[inline]
    fn mark_virtual_fee_pending(&self) {
        self.uncertain_fast.store(true, Ordering::Release);
        self.admission_fast.store(false, Ordering::Release);
        // Missing taker attribution is the one degraded mode in which passive
        // maker admission remains safe.
        self.passive_admission_fast
            .store(self.seeded_fast.load(Ordering::Acquire), Ordering::Release);
    }

    pub fn new(account_id: impl Into<String>) -> Self {
        let account = Self {
            account_id: account_id.into(),
            state: Arc::new(Mutex::new(SharedAccountState::default())),
            control_gate: RwLock::new(()),
            virtual_accounts: RwLock::new(BTreeMap::new()),
            coid_routes: ShardedRouteMap::new(),
            oid_routes: ShardedRouteMap::new(),
            trade_routes: ShardedRouteMap::new(),
            anomalous_trade_keys: RwLock::new(HashSet::new()),
            anomalous_private_event_keys: RwLock::new(HashSet::new()),
            risk_blocker_sources_fast: RwLock::new(HashSet::new()),
            unresolved_trade_keys: ShardedRouteMap::new(),
            private_anomaly_transition: Mutex::new(()),
            token_fee_configs_fast: RwLock::new(HashMap::new()),
            seeded_fast: AtomicBool::new(false),
            uncertain_fast: AtomicBool::new(false),
            admission_fast: AtomicBool::new(false),
            passive_admission_fast: AtomicBool::new(false),
            startup_snapshot_applied_fast: AtomicBool::new(false),
            startup_snapshot_deferred_fast: AtomicBool::new(false),
            settled_token_values_fast: ArcSwap::from_pointee(SettledTokenValuesSnapshot::default()),
            settled_token_values_generation_fast: AtomicU64::new(0),
            uncertain_reason_fast: ArcSwapOption::empty(),
            ledger_generation_fast: AtomicU64::new(0),
            retired_trade_tombstone_count_fast: AtomicUsize::new(0),
            settled_gc_candidates: Mutex::new(BTreeMap::new()),
            settled_gc_candidate_count_fast: AtomicUsize::new(0),
            settled_gc_candidate_tokens_fast: ArcSwap::from_pointee(HashSet::new()),
            persistence: None,
            trade_persistence_pending_generation: AtomicU64::new(0),
            trade_persistence_blocker_active: AtomicBool::new(false),
            order_audit_wakeup: (Mutex::new(0), Condvar::new()),
            account_lock_wait_last_us: AtomicU64::new(0),
            account_lock_wait_max_us: AtomicU64::new(0),
            account_lock_hold_last_us: AtomicU64::new(0),
            account_lock_hold_max_us: AtomicU64::new(0),
            account_lock_acquisitions: AtomicU64::new(0),
            reservation_control_lock: LockLatencyMetrics::default(),
            reservation_coid_route_lock: LockLatencyMetrics::default(),
            reservation_oid_route_lock: LockLatencyMetrics::default(),
            reservation_lifecycle_lock: LockLatencyMetrics::default(),
        };
        let mut state = SharedAccountState::default();
        account.sync_state_to_virtual_accounts(&mut state, None, None);
        account
    }

    /// Open (or create) a durable account ledger. A corrupt, unsupported, or
    /// account-mismatched file fails startup rather than silently discarding
    /// ownership and reservations.
    pub fn new_persistent(
        account_id: impl Into<String>,
        path: impl Into<PathBuf>,
    ) -> Result<Self, String> {
        Self::new_persistent_inner(account_id.into(), path.into(), false)
    }

    /// Open a durable live-trading ledger while admitting only the narrow
    /// class of under-reservations that can be repaired by an authoritative
    /// per-order CLOB lookup. The repair first restores the worst-case
    /// reservation from durable order/FAILED-trade roots and marks the order
    /// for startup recovery. Callers must query and resolve every id returned
    /// by [`Self::startup_query_repair_pending_order_ids`] before allowing
    /// live strategy workers to start.
    pub fn new_persistent_for_query_repair(
        account_id: impl Into<String>,
        path: impl Into<PathBuf>,
    ) -> Result<Self, String> {
        Self::new_persistent_inner(account_id.into(), path.into(), true)
    }

    fn new_persistent_inner(
        account_id: String,
        path: PathBuf,
        allow_query_repair: bool,
    ) -> Result<Self, String> {
        let (state, initial_generation, startup_aggregate_repairs) = if path.exists() {
            let bytes = std::fs::read(&path)
                .map_err(|error| format!("read account ledger {}: {error}", path.display()))?;
            let mut persisted: PersistedAccount = serde_json::from_slice(&bytes)
                .map_err(|error| format!("parse account ledger {}: {error}", path.display()))?;
            if persisted.version != PERSISTENCE_VERSION {
                return Err(format!(
                    "unsupported account ledger version {} in {} (expected {})",
                    persisted.version,
                    path.display(),
                    PERSISTENCE_VERSION,
                ));
            }
            if persisted.account_id != account_id {
                return Err(format!(
                    "account ledger {} belongs to `{}`, not `{}`",
                    path.display(),
                    persisted.account_id,
                    account_id,
                ));
            }
            replay_persistence_wal(&path, &mut persisted)?;
            let initial_generation = persisted.persistence_generation;
            let mut state = persisted.state;
            if !allow_query_repair && !state.startup_query_repair_orders.is_empty() {
                let mut pending: Vec<String> =
                    state.startup_query_repair_orders.iter().cloned().collect();
                pending.sort();
                return Err(format!(
                    "account ledger {} has unfinished authoritative startup query repair(s): coids={pending:?}",
                    path.display(),
                ));
            }
            let persisted_uncertainty = state
                .uncertain
                .then(|| (state.uncertain_reason.clone(), state.uncertain_since_ms));
            // `oid_to_coid` used to preserve the API's exact casing/prefix.
            // Rebuild it from durable orders so old ledgers are migrated to
            // canonical keys and cannot lose attribution after restart.
            let mut normalized = HashMap::with_capacity(state.orders.len());
            for order in state.orders.values() {
                let oid = normalize_order_id(&order.order_id);
                if oid.is_empty() {
                    return Err(format!(
                        "account ledger {} has empty order id for coid `{}`",
                        path.display(),
                        order.client_order_id,
                    ));
                }
                if let Some(other) = normalized.insert(oid.clone(), order.client_order_id.clone()) {
                    if other != order.client_order_id {
                        return Err(format!(
                            "account ledger {} maps normalized order id `{}` to both `{}` and `{}`",
                            path.display(),
                            oid,
                            other,
                            order.client_order_id,
                        ));
                    }
                }
            }
            state.oid_to_coid = normalized;
            let pending_maintenance: Vec<String> = state
                .maintenance_ops
                .values()
                .filter(|operation| {
                    matches!(
                        operation.status,
                        MaintenanceOperationStatus::Reserved
                            | MaintenanceOperationStatus::Submitted
                            | MaintenanceOperationStatus::Uncertain
                    )
                })
                .map(|operation| operation.operation_id.clone())
                .collect();
            if !pending_maintenance.is_empty() {
                set_uncertain(
                    &mut state,
                    format!(
                        "maintenance recovery pending after restart: operation_ids=[{}]",
                        pending_maintenance.join(","),
                    ),
                );
            }
            let terminal_failed_migrated = normalize_terminal_failed_state(&mut state);
            if terminal_failed_migrated {
                recompute_reconciliation(&mut state, "terminal FAILED ledger migration");
            }
            if state.seeded && state.seed_baseline.is_none() {
                state.seed_baseline = Some(derive_legacy_seed_baseline(&state));
                log::warn!(
                    "[shared_account] account={} upgraded legacy ledger with a synthetic immutable seed baseline",
                    account_id,
                );
            }
            // Older SDK callers could book a trade before supplying its
            // maker/taker role. Preserve the economic row, but make that
            // intermediate state explicitly recoverable and risk-off instead
            // of letting `restored_trades()` panic or rejecting the ledger on
            // the next process start.
            let unresolved_roles: Vec<String> = state
                .trades
                .iter()
                .filter(|(_, trade)| trade.is_maker.is_none())
                .map(|(trade_key, _)| trade_key.clone())
                .collect();
            let mut role_migrated = false;
            for trade_key in unresolved_roles {
                role_migrated |= state.fee_attribution_pending.insert(trade_key);
            }
            if role_migrated {
                recompute_reconciliation(&mut state, "legacy trade-role attribution migration");
            }
            // A crash can leave an economically-booked trade at MATCHED while
            // its MINED/CONFIRMED/FAILED edge is still outstanding.  The
            // ordinary gap-replay watermark may already be newer than that
            // trade, so reconstruct a durable rewind anchor from the trade row
            // itself.  Until finality arrives, startup wallet snapshots must
            // remain deferred: applying one could book the same physical fill
            // twice when the terminal lifecycle is replayed later.
            let pending_finality_anchors: Vec<(String, u64)> = state
                .trades
                .iter()
                .filter(|(_, trade)| {
                    trade.booked
                        && !trade.failed
                        && (!trade.physical_booked
                            || (trade.virtual_fee_booked && !trade.physical_fee_booked))
                        && trade.match_time_secs > 0
                })
                .map(|(trade_key, trade)| (trade_key.clone(), trade.match_time_secs))
                .collect();
            let mut restored_finality_anchors = 0usize;
            for (trade_key, match_time_secs) in pending_finality_anchors {
                if state
                    .unresolved_trade_match_times
                    .insert(trade_key, match_time_secs)
                    != Some(match_time_secs)
                {
                    restored_finality_anchors = restored_finality_anchors.saturating_add(1);
                }
            }
            if restored_finality_anchors > 0 {
                log::warn!(
                    "[shared_account] account={} restored {} pending trade-finality replay anchor(s) from durable MATCHED lifecycle state",
                    account_id,
                    restored_finality_anchors,
                );
            }
            // A persisted trade-persistence blocker proves that the snapshot
            // containing both the trade and the blocker reached disk. The new
            // process can therefore clear only this source-owned blocker; all
            // other subsystem blockers remain fail-closed across restart.
            if state
                .risk_blockers
                .remove(TRADE_PERSISTENCE_RISK_BLOCKER)
                .is_some()
            {
                recompute_reconciliation(&mut state, "durable trade-persistence blocker recovery");
            }
            if allow_query_repair {
                let (query_orders, repair_mutated) =
                    repair_failed_trade_under_reservations_for_query(&mut state)?;
                if repair_mutated {
                    recompute_reconciliation(
                        &mut state,
                        "startup FAILED-trade reservation query repair",
                    );
                }
                if !query_orders.is_empty() {
                    log::warn!(
                        "[shared_account] account={} admitted {} FAILED-trade order(s) only for authoritative startup query after conservatively restoring reservations: coids={:?}",
                        account_id,
                        query_orders.len(),
                        query_orders,
                    );
                }
            }
            // Rebuild startup recovery admission before validating resource
            // availability. An order can legitimately outlive the event's
            // wallet position while its crash-durable reservation is still
            // present. Rejecting that temporary deficit here makes the
            // order-specific recovery path unreachable. The derived-order
            // checks below still prove that every accepted reservation comes
            // from a durable order root.
            let startup_deficit_orders: Vec<String> = state
                .orders
                .iter()
                .filter(|(_, order)| order_has_startup_reservation_deficit(&state, order))
                .map(|(coid, _)| coid.clone())
                .collect();
            let recovered_before = state.recovery_pending_orders.len();
            state.recovery_pending_orders.extend(startup_deficit_orders);
            if state.recovery_pending_orders.len() != recovered_before {
                recompute_reconciliation(&mut state, "startup order recovery rebuild");
                log::warn!(
                    "[shared_account] account={} restored {} potentially-live order(s) into owner-instance startup recovery before ledger validation",
                    account_id,
                    state.recovery_pending_orders.len(),
                );
            }
            // `uncertain` used to be persisted for an ordinary negative
            // wallet-vs-virtual residual. Wallet differences are accounting
            // observations, not structural admission blockers: keep their
            // exact signed values in `unallocated_*`, but re-evaluate old
            // scalar uncertainty under the current blocker-only policy.
            if let Some((previous_reason, previous_since_ms)) = persisted_uncertainty {
                recompute_reconciliation(&mut state, "persisted uncertainty migration");
                if !state.uncertain {
                    log::warn!(
                        "[shared_account] account={} reset legacy persisted uncertainty reason={:?} since_ms={:?}; preserving wallet residual unallocated_cash={:+.6} unallocated_positions={:?}",
                        account_id,
                        previous_reason,
                        previous_since_ms,
                        state.unallocated_cash,
                        state.unallocated_positions,
                    );
                }
            }
            // Typed instance deltas intentionally do not rewrite aggregate
            // reconciliation leaves on the fill thread. Rebuild those derived
            // leaves after WAL replay, before validating and exposing state.
            recompute_reconciliation(&mut state, "startup typed-delta reconciliation");
            let startup_aggregate_repairs = repair_under_reserved_instance_aggregates(&mut state)
                .map_err(|error| {
                format!("invalid account ledger {}: {error}", path.display())
            })?;
            validate_persisted_state(&account_id, &state)
                .map_err(|error| format!("invalid account ledger {}: {error}", path.display()))?;
            (state, initial_generation, startup_aggregate_repairs)
        } else {
            let wal_path = persistence_wal_path(&path);
            if wal_path.exists() {
                return Err(format!(
                    "account ledger snapshot {} is missing while WAL {} exists",
                    path.display(),
                    wal_path.display()
                ));
            }
            (SharedAccountState::default(), 0, Vec::new())
        };
        let initial_retired_trade_tombstones = state
            .retired_trade_ownership_tombstones
            .values()
            .filter(|tombstone| retired_trade_tombstone_is_live(tombstone, wall_clock_ms()))
            .count();
        let initial_unresolved_trade_keys: Vec<String> =
            state.unresolved_trade_match_times.keys().cloned().collect();
        let initial_risk_blocker_sources = state.risk_blockers.keys().cloned().collect();
        let initial_settled_gc_candidates: BTreeMap<String, BTreeSet<String>> = state
            .settled_audit_references
            .iter()
            .filter(|(_, reference)| reference.instances.is_empty())
            .map(|(condition_id, reference)| (condition_id.clone(), reference.asset_ids.clone()))
            .collect();
        let initial_settled_gc_candidate_count = initial_settled_gc_candidates.len();
        let initial_settled_gc_candidate_tokens = initial_settled_gc_candidates
            .values()
            .flat_map(|tokens| tokens.iter().cloned())
            .collect();
        let initial_settled_generation = Self::effective_settled_token_values_generation(&state);
        let initial_settled_values = state.settled_token_values.clone();
        let state = Arc::new(Mutex::new(state));
        let persistence = AccountPersistence::start(
            path,
            account_id.clone(),
            Arc::clone(&state),
            initial_generation,
        )?;
        if !startup_aggregate_repairs.is_empty() {
            log::warn!(
                "[shared_account] account={} startup repaired under-reserved instance aggregate(s) from durable roots before admission: {:?}",
                account_id,
                startup_aggregate_repairs,
            );
        }
        let account = Self {
            account_id,
            state,
            control_gate: RwLock::new(()),
            virtual_accounts: RwLock::new(BTreeMap::new()),
            coid_routes: ShardedRouteMap::new(),
            oid_routes: ShardedRouteMap::new(),
            trade_routes: ShardedRouteMap::new(),
            anomalous_trade_keys: RwLock::new(HashSet::new()),
            anomalous_private_event_keys: RwLock::new(HashSet::new()),
            risk_blocker_sources_fast: RwLock::new(initial_risk_blocker_sources),
            unresolved_trade_keys: ShardedRouteMap::new(),
            private_anomaly_transition: Mutex::new(()),
            token_fee_configs_fast: RwLock::new(HashMap::new()),
            seeded_fast: AtomicBool::new(false),
            uncertain_fast: AtomicBool::new(false),
            admission_fast: AtomicBool::new(false),
            passive_admission_fast: AtomicBool::new(false),
            startup_snapshot_applied_fast: AtomicBool::new(false),
            startup_snapshot_deferred_fast: AtomicBool::new(false),
            settled_token_values_fast: ArcSwap::from_pointee(SettledTokenValuesSnapshot {
                generation: initial_settled_generation,
                values: initial_settled_values,
            }),
            settled_token_values_generation_fast: AtomicU64::new(initial_settled_generation),
            uncertain_reason_fast: ArcSwapOption::empty(),
            ledger_generation_fast: AtomicU64::new(0),
            retired_trade_tombstone_count_fast: AtomicUsize::new(initial_retired_trade_tombstones),
            settled_gc_candidates: Mutex::new(initial_settled_gc_candidates),
            settled_gc_candidate_count_fast: AtomicUsize::new(initial_settled_gc_candidate_count),
            settled_gc_candidate_tokens_fast: ArcSwap::from_pointee(
                initial_settled_gc_candidate_tokens,
            ),
            persistence: Some(persistence),
            trade_persistence_pending_generation: AtomicU64::new(0),
            trade_persistence_blocker_active: AtomicBool::new(false),
            order_audit_wakeup: (Mutex::new(0), Condvar::new()),
            account_lock_wait_last_us: AtomicU64::new(0),
            account_lock_wait_max_us: AtomicU64::new(0),
            account_lock_hold_last_us: AtomicU64::new(0),
            account_lock_hold_max_us: AtomicU64::new(0),
            account_lock_acquisitions: AtomicU64::new(0),
            reservation_control_lock: LockLatencyMetrics::default(),
            reservation_coid_route_lock: LockLatencyMetrics::default(),
            reservation_oid_route_lock: LockLatencyMetrics::default(),
            reservation_lifecycle_lock: LockLatencyMetrics::default(),
        };
        {
            let _state = account.lock_state();
        }
        for trade_key in initial_unresolved_trade_keys {
            account
                .unresolved_trade_keys
                .insert(trade_key, String::new());
        }
        Ok(account)
    }

    fn lock_state(&self) -> AccountStateGuard<'_> {
        let wait_started = Instant::now();
        let control = self.control_gate.write().unwrap();
        let mut state = self.state.lock().unwrap();
        let acquired_at = Instant::now();
        let wait_us = wait_started.elapsed().as_micros().min(u64::MAX as u128) as u64;
        self.account_lock_wait_last_us
            .store(wait_us, Ordering::Relaxed);
        self.account_lock_wait_max_us
            .fetch_max(wait_us, Ordering::Relaxed);
        self.account_lock_acquisitions
            .fetch_add(1, Ordering::Relaxed);
        // Capture before copying the virtual shards. A reservation that lands
        // after this point either appears in the copy or advances the epoch,
        // causing the guard's final publication step to merge it explicitly.
        let (reservation_epochs, trade_epochs) = {
            let accounts = self.virtual_accounts.read().unwrap();
            let reservation_epochs = accounts
                .iter()
                .map(|(instance_id, account)| {
                    (
                        instance_id.clone(),
                        account.reservation_epoch.load(Ordering::Acquire),
                    )
                })
                .collect();
            let trade_epochs = accounts
                .iter()
                .map(|(instance_id, account)| {
                    (
                        instance_id.clone(),
                        account.trade_epoch.load(Ordering::Acquire),
                    )
                })
                .collect();
            (reservation_epochs, trade_epochs)
        };
        self.sync_virtual_accounts_to_state(&mut state);
        // Account snapshots/reconcile workers own aggregate reconciliation.
        // Keeping this on the already-cold control transaction means hot fills
        // never pay for an account-wide scan while readers see fresh residuals.
        recompute_reconciliation(&mut state, "control-plane aggregate refresh");
        AccountStateGuard {
            account: self,
            _control: control,
            state,
            instance_scope: None,
            reservation_epochs,
            trade_epochs,
            acquired_at,
        }
    }

    fn lock_state_for_instance(&self, instance_id: &str) -> AccountStateGuard<'_> {
        let wait_started = Instant::now();
        let control = self.control_gate.write().unwrap();
        let mut state = self.state.lock().unwrap();
        let acquired_at = Instant::now();
        let wait_us = wait_started.elapsed().as_micros().min(u64::MAX as u128) as u64;
        self.account_lock_wait_last_us
            .store(wait_us, Ordering::Relaxed);
        self.account_lock_wait_max_us
            .fetch_max(wait_us, Ordering::Relaxed);
        self.account_lock_acquisitions
            .fetch_add(1, Ordering::Relaxed);
        let (reservation_epochs, trade_epochs) = self
            .virtual_account(instance_id)
            .map(|account| {
                (
                    BTreeMap::from([(
                        instance_id.to_string(),
                        account.reservation_epoch.load(Ordering::Acquire),
                    )]),
                    BTreeMap::from([(
                        instance_id.to_string(),
                        account.trade_epoch.load(Ordering::Acquire),
                    )]),
                )
            })
            .unwrap_or_default();
        self.sync_virtual_account_to_state(&mut state, instance_id);
        AccountStateGuard {
            account: self,
            _control: control,
            state,
            instance_scope: Some(instance_id.to_string()),
            reservation_epochs,
            trade_epochs,
            acquired_at,
        }
    }

    fn virtual_account(&self, instance_id: &str) -> Option<Arc<VirtualAccount>> {
        self.virtual_accounts
            .read()
            .unwrap()
            .get(instance_id)
            .cloned()
    }

    fn virtual_account_for_coid(&self, client_order_id: &str) -> Option<Arc<VirtualAccount>> {
        let instance_id = self.coid_routes.get(client_order_id)?;
        self.virtual_account(&instance_id)
    }

    fn sync_virtual_accounts_to_state(&self, state: &mut SharedAccountState) {
        let accounts = self.virtual_accounts.read().unwrap();
        if accounts.is_empty() {
            return;
        }

        let mut instances = BTreeMap::new();
        let mut orders = HashMap::new();
        let mut trades = HashMap::new();
        let mut recovery_pending_orders = HashSet::new();
        let mut startup_query_repair_orders = HashSet::new();
        let mut routine_cancel_audits = HashSet::new();
        let mut fee_attribution_pending = HashSet::new();
        let mut known_sidecars = HashMap::new();
        for (instance_id, account) in accounts.iter() {
            // Private-trade economics and the matching lifecycle root are
            // mutated while holding this owner-local mutex. Take it before
            // reading the atomic ledger so a cold full-account snapshot cannot
            // pair a pre-trade balance with a post-trade row (or vice versa).
            let lifecycle = account.lifecycle.lock().unwrap();
            instances.insert(instance_id.clone(), account.ledger_snapshot());
            orders.extend(lifecycle.orders.clone());
            trades.extend(lifecycle.trades.clone());
            recovery_pending_orders.extend(lifecycle.recovery_pending_orders.clone());
            startup_query_repair_orders.extend(lifecycle.startup_query_repair_orders.clone());
            routine_cancel_audits.extend(lifecycle.routine_cancel_audits.clone());
            fee_attribution_pending.extend(lifecycle.fee_attribution_pending.clone());
            if let Some(checkpoint) = lifecycle.sidecar_checkpoint.clone() {
                known_sidecars.insert(instance_id.clone(), checkpoint);
            }
        }
        state.instances = instances;
        state.orders = orders;
        state.oid_to_coid = state
            .orders
            .iter()
            .map(|(coid, order)| (normalize_order_id(&order.order_id), coid.clone()))
            .collect();
        state.trades = trades;
        state.recovery_pending_orders = recovery_pending_orders;
        state.startup_query_repair_orders = startup_query_repair_orders;
        state.routine_cancel_audits = routine_cancel_audits;
        state.fee_attribution_pending = fee_attribution_pending;
        state.ledger_generation = state
            .ledger_generation
            .max(self.ledger_generation_fast.load(Ordering::Acquire));
        // Preserve legacy/non-instance sidecars, but make every registered
        // instance's virtual checkpoint authoritative, including `None`.
        state
            .sidecar_checkpoints
            .retain(|instance_id, _| !accounts.contains_key(instance_id));
        for (instance_id, checkpoint) in known_sidecars {
            state.sidecar_checkpoints.insert(instance_id, checkpoint);
        }
    }

    fn sync_virtual_account_to_state(&self, state: &mut SharedAccountState, instance_id: &str) {
        let Some(account) = self.virtual_account(instance_id) else {
            return;
        };
        let lifecycle = account.lifecycle.lock().unwrap();
        let mut owned: HashSet<String> = state
            .orders
            .iter()
            .filter(|(_, order)| order.instance_id == instance_id)
            .map(|(coid, _)| coid.clone())
            .collect();
        owned.extend(lifecycle.orders.keys().cloned());
        state
            .instances
            .insert(instance_id.to_string(), account.ledger_snapshot());
        state
            .orders
            .retain(|_, order| order.instance_id != instance_id);
        state.orders.extend(lifecycle.orders.clone());
        state
            .trades
            .retain(|_, trade| trade.ownership.instance_id != instance_id);
        state.trades.extend(lifecycle.trades.clone());
        state
            .recovery_pending_orders
            .retain(|coid| !owned.contains(coid));
        state
            .recovery_pending_orders
            .extend(lifecycle.recovery_pending_orders.clone());
        state
            .startup_query_repair_orders
            .retain(|coid| !owned.contains(coid));
        state
            .startup_query_repair_orders
            .extend(lifecycle.startup_query_repair_orders.clone());
        state
            .routine_cancel_audits
            .retain(|coid| !owned.contains(coid));
        state
            .routine_cancel_audits
            .extend(lifecycle.routine_cancel_audits.clone());
        state.fee_attribution_pending.retain(|trade_key| {
            state
                .trades
                .get(trade_key)
                .is_none_or(|trade| trade.ownership.instance_id != instance_id)
        });
        state
            .fee_attribution_pending
            .extend(lifecycle.fee_attribution_pending.clone());
        state.ledger_generation = state
            .ledger_generation
            .max(self.ledger_generation_fast.load(Ordering::Acquire));
        if let Some(checkpoint) = lifecycle.sidecar_checkpoint.clone() {
            state
                .sidecar_checkpoints
                .insert(instance_id.to_string(), checkpoint);
        } else {
            state.sidecar_checkpoints.remove(instance_id);
        }
        state.oid_to_coid = state
            .orders
            .iter()
            .map(|(coid, order)| (normalize_order_id(&order.order_id), coid.clone()))
            .collect();
    }

    fn merge_concurrent_reservations(
        state: &mut SharedAccountState,
        account: &VirtualAccount,
        lifecycle: &VirtualLifecycle,
        observed_epoch: Option<u64>,
    ) {
        if observed_epoch
            .is_none_or(|observed| observed == account.reservation_epoch.load(Ordering::Acquire))
        {
            return;
        }
        let missing: Vec<OrderOwnership> = lifecycle
            .orders
            .iter()
            .filter(|(coid, _)| !state.orders.contains_key(*coid))
            .map(|(_, order)| order.clone())
            .collect();
        if missing.is_empty() {
            return;
        }
        let Some(instance) = state.instances.get_mut(&account.instance_id) else {
            return;
        };
        for order in missing {
            instance.reserved_cash += order.reserved_cash;
            if order.reserved_quantity > EPS {
                *instance
                    .reserved_positions
                    .entry(order.token_id.clone())
                    .or_insert(0.0) += order.reserved_quantity;
            }
            state.oid_to_coid.insert(
                normalize_order_id(&order.order_id),
                order.client_order_id.clone(),
            );
            state.orders.insert(order.client_order_id.clone(), order);
        }
    }

    /// Fold private-trade mutations that committed after a cold transaction's
    /// initial shard snapshot into that transaction before it republishes.
    /// Hot fills remain completely outside `control_gate`; only the cold
    /// publisher pays for this touched-key merge.
    fn merge_concurrent_trade_mutations(
        state: &mut SharedAccountState,
        account: &VirtualAccount,
        lifecycle: &mut VirtualLifecycle,
        observed_epoch: Option<u64>,
    ) -> bool {
        let Some(observed_epoch) = observed_epoch else {
            return false;
        };
        let current_epoch = account.trade_epoch.load(Ordering::Acquire);
        if observed_epoch == current_epoch {
            while lifecycle
                .recent_trade_mutations
                .front()
                .is_some_and(|hint| hint.epoch <= current_epoch)
            {
                lifecycle.recent_trade_mutations.pop_front();
            }
            return false;
        }
        let hints = lifecycle
            .recent_trade_mutations
            .iter()
            .filter(|hint| hint.epoch > observed_epoch)
            .collect::<Vec<_>>();
        let complete = hints
            .first()
            .is_some_and(|hint| hint.epoch == observed_epoch.saturating_add(1))
            && hints.last().is_some_and(|hint| hint.epoch == current_epoch);
        let (coids, trade_keys, tokens) = if complete {
            (
                hints
                    .iter()
                    .map(|hint| hint.client_order_id.clone())
                    .collect::<HashSet<_>>(),
                hints
                    .iter()
                    .map(|hint| hint.trade_key.clone())
                    .collect::<HashSet<_>>(),
                hints
                    .iter()
                    .map(|hint| hint.token_id.clone())
                    .collect::<HashSet<_>>(),
            )
        } else {
            // A transaction spanning more than the retained hint window is
            // exceptional. Fail safe by preserving the entire hot shard; the
            // asynchronous physical snapshot will converge any same-instance
            // control adjustment on its next pass.
            log::warn!(
                "[shared_account] instance={} private trade merge hint window overrun observed_epoch={} current_epoch={}; preserving full hot shard",
                account.instance_id,
                observed_epoch,
                current_epoch,
            );
            (
                lifecycle.orders.keys().cloned().collect(),
                lifecycle.trades.keys().cloned().collect(),
                account.positions.read().unwrap().keys().cloned().collect(),
            )
        };

        let mut concurrent_economics = AccountEconomicState::default();
        for trade_key in &trade_keys {
            if let Some(trade) = state.trades.get(trade_key) {
                add_economic_state(
                    &mut concurrent_economics,
                    &trade_economic_effect(trade),
                    -1.0,
                );
            }
            if let Some(trade) = lifecycle.trades.get(trade_key) {
                add_economic_state(
                    &mut concurrent_economics,
                    &trade_economic_effect(trade),
                    1.0,
                );
            }
        }
        if let Some(instance) = state.instances.get_mut(&account.instance_id) {
            let economic_delta = concurrent_economics.instances.get(&account.instance_id);
            // Compose the concurrent owner-shard trade root with the cold
            // transaction instead of replacing absolute cash/positions. An
            // absolute overwrite loses a simultaneous split/merge debit even
            // though the operation status and token legs persist.
            instance.cash += economic_delta.map_or(0.0, |delta| delta.cash);
            instance.reserved_cash = account.reserved_cash.load();
            let positions = account.positions.read().unwrap();
            for token in &tokens {
                if let Some(position) = positions.get(token) {
                    *instance.positions.entry(token.clone()).or_insert(0.0) += economic_delta
                        .and_then(|delta| delta.positions.get(token))
                        .copied()
                        .unwrap_or(0.0);
                    instance
                        .reserved_positions
                        .insert(token.clone(), position.reserved.load());
                }
            }
        }
        for coid in &coids {
            if let Some(previous) = state.orders.get(coid) {
                let previous_oid = normalize_order_id(&previous.order_id);
                if state
                    .oid_to_coid
                    .get(&previous_oid)
                    .is_some_and(|mapped| mapped == coid)
                {
                    state.oid_to_coid.remove(&previous_oid);
                }
            }
            if let Some(order) = lifecycle.orders.get(coid) {
                state.oid_to_coid.insert(
                    normalize_order_id(&order.order_id),
                    order.client_order_id.clone(),
                );
                state.orders.insert(coid.clone(), order.clone());
            } else {
                state.orders.remove(coid);
            }
            if lifecycle.recovery_pending_orders.contains(coid) {
                state.recovery_pending_orders.insert(coid.clone());
            } else {
                state.recovery_pending_orders.remove(coid);
            }
            if lifecycle.routine_cancel_audits.contains(coid) {
                state.routine_cancel_audits.insert(coid.clone());
            } else {
                state.routine_cancel_audits.remove(coid);
            }
        }
        for trade_key in &trade_keys {
            if let Some(trade) = lifecycle.trades.get(trade_key) {
                state.trades.insert(trade_key.clone(), trade.clone());
            } else {
                state.trades.remove(trade_key);
            }
            if lifecycle.fee_attribution_pending.contains(trade_key) {
                state.fee_attribution_pending.insert(trade_key.clone());
            } else {
                state.fee_attribution_pending.remove(trade_key);
            }
        }
        state.ledger_generation = state.ledger_generation.max(
            lifecycle
                .trades
                .values()
                .map(|trade| trade.ledger_generation)
                .max()
                .unwrap_or(0),
        );
        while lifecycle
            .recent_trade_mutations
            .front()
            .is_some_and(|hint| hint.epoch <= current_epoch)
        {
            lifecycle.recent_trade_mutations.pop_front();
        }
        true
    }

    fn record_virtual_trade_mutation(
        account: &VirtualAccount,
        lifecycle: &mut VirtualLifecycle,
        trade_key: &str,
        client_order_id: &str,
        token_id: &str,
    ) {
        let epoch = account
            .trade_epoch
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        lifecycle
            .recent_trade_mutations
            .push_back(VirtualTradeMutationHint {
                epoch,
                trade_key: trade_key.to_string(),
                client_order_id: client_order_id.to_string(),
                token_id: token_id.to_string(),
            });
        while lifecycle.recent_trade_mutations.len() > RECENT_VIRTUAL_TRADE_MUTATIONS {
            lifecycle.recent_trade_mutations.pop_front();
        }
    }

    fn sync_state_to_virtual_accounts(
        &self,
        state: &mut SharedAccountState,
        reservation_epochs: Option<&BTreeMap<String, u64>>,
        trade_epochs: Option<&BTreeMap<String, u64>>,
    ) {
        let ledgers: Vec<(String, InstanceLedger)> = state
            .instances
            .iter()
            .map(|(instance_id, ledger)| (instance_id.clone(), ledger.clone()))
            .collect();
        // Keep the account-map writer scoped to membership publication only.
        // Reservation lookup must never wait for the subsequent per-instance
        // snapshot copies performed by a cold control transaction.
        let accounts: Vec<(String, Arc<VirtualAccount>)> = {
            let mut accounts = self.virtual_accounts.write().unwrap();
            accounts.retain(|instance_id, _| state.instances.contains_key(instance_id));
            ledgers
                .iter()
                .map(|(instance_id, ledger)| {
                    let account = accounts
                        .entry(instance_id.clone())
                        .or_insert_with(|| {
                            Arc::new(VirtualAccount::new(instance_id.clone(), ledger))
                        })
                        .clone();
                    (instance_id.clone(), account)
                })
                .collect()
        };
        let mut merged_concurrent_trade = false;
        for (instance_id, account) in &accounts {
            let _publication = account.reservation_publish.lock().unwrap();
            let mut lifecycle = account.lifecycle.lock().unwrap();
            Self::merge_concurrent_reservations(
                state,
                &account,
                &lifecycle,
                reservation_epochs.and_then(|epochs| epochs.get(instance_id).copied()),
            );
            merged_concurrent_trade |= Self::merge_concurrent_trade_mutations(
                state,
                &account,
                &mut lifecycle,
                trade_epochs.and_then(|epochs| epochs.get(instance_id).copied()),
            );
            if let Some(ledger) = state.instances.get(instance_id) {
                account.replace_ledger(ledger);
            }
            lifecycle.orders = state
                .orders
                .iter()
                .filter(|(_, order)| order.instance_id == *instance_id)
                .map(|(coid, order)| (coid.clone(), order.clone()))
                .collect();
            lifecycle.trades = state
                .trades
                .iter()
                .filter(|(_, trade)| trade.ownership.instance_id == *instance_id)
                .map(|(trade_key, trade)| (trade_key.clone(), trade.clone()))
                .collect();
            lifecycle.recovery_pending_orders = state
                .recovery_pending_orders
                .iter()
                .filter(|coid| lifecycle.orders.contains_key(*coid))
                .cloned()
                .collect();
            lifecycle.startup_query_repair_orders = state
                .startup_query_repair_orders
                .iter()
                .filter(|coid| lifecycle.orders.contains_key(*coid))
                .cloned()
                .collect();
            lifecycle.routine_cancel_audits = state
                .routine_cancel_audits
                .iter()
                .filter(|coid| lifecycle.orders.contains_key(*coid))
                .cloned()
                .collect();
            lifecycle.fee_attribution_pending = state
                .fee_attribution_pending
                .iter()
                .filter(|trade_key| lifecycle.trades.contains_key(*trade_key))
                .cloned()
                .collect();
            lifecycle.cancel_audit_anomalies = state
                .ownership_anomalies
                .keys()
                .filter_map(|key| key.strip_prefix("order_cancel_audit:"))
                .filter(|coid| lifecycle.orders.contains_key(*coid))
                .map(str::to_string)
                .collect();
            lifecycle.sidecar_checkpoint = state.sidecar_checkpoints.get(instance_id).cloned();
        }
        if merged_concurrent_trade {
            recompute_reconciliation(state, "concurrent private trade publication merge");
        }

        // Never clear a live route index before rebuilding it. Private events
        // resolve through these maps without the cold control gate; a
        // clear-then-insert window previously produced false "runtime mapping
        // but no ledger row" ownership failures. Settled GC removes truly
        // retired routes explicitly, so snapshot convergence only needs
        // idempotent upserts here.
        let route_snapshots: Vec<_> = accounts
            .iter()
            .map(|(instance_id, account)| {
                let lifecycle = account.lifecycle.lock().unwrap();
                let coids: HashSet<String> = lifecycle.orders.keys().cloned().collect();
                let oids: HashSet<String> = lifecycle
                    .orders
                    .values()
                    .map(|order| normalize_order_id(&order.order_id))
                    .collect();
                let trade_keys: HashSet<String> = lifecycle.trades.keys().cloned().collect();
                let trade_epoch = account.trade_epoch.load(Ordering::Acquire);
                (instance_id.clone(), coids, oids, trade_keys, trade_epoch)
            })
            .collect();
        let live_owners: HashSet<String> = route_snapshots
            .iter()
            .map(|(instance_id, _, _, _, _)| instance_id.clone())
            .collect();
        for (instance_id, coids, oids, trade_keys, trade_epoch) in route_snapshots {
            for coid in &coids {
                self.coid_routes.insert(coid.clone(), instance_id.clone());
            }
            for oid in &oids {
                self.oid_routes.insert(oid.clone(), instance_id.clone());
            }
            for trade_key in &trade_keys {
                self.trade_routes
                    .insert(trade_key.clone(), instance_id.clone());
            }
            // Publish desired keys first, then prune keys no longer present in
            // this owner's shard. Readers therefore never observe a valid
            // order without a route while settled GC still removes tombstones.
            self.coid_routes.retain_owner_keys(&instance_id, &coids);
            self.oid_routes.retain_owner_keys(&instance_id, &oids);
            if accounts
                .iter()
                .find(|(candidate, _)| candidate == &instance_id)
                .is_some_and(|(_, account)| {
                    account.trade_epoch.load(Ordering::Acquire) == trade_epoch
                })
            {
                self.trade_routes
                    .retain_owner_keys(&instance_id, &trade_keys);
            }
        }
        self.coid_routes.retain_owners(&live_owners);
        self.oid_routes.retain_owners(&live_owners);
        self.trade_routes.retain_owners(&live_owners);
        *self.anomalous_trade_keys.write().unwrap() = state
            .ownership_anomalies
            .keys()
            .filter_map(|key| key.strip_prefix("trade:"))
            .map(str::to_string)
            .collect();
        *self.anomalous_private_event_keys.write().unwrap() = state
            .ownership_anomalies
            .keys()
            .filter_map(|key| key.strip_prefix("private_event:"))
            .map(str::to_string)
            .collect();
        let seeded = state.seeded;
        let passive = seeded && (!state.uncertain || fee_degradation_is_only_uncertainty(state));
        self.seeded_fast.store(seeded, Ordering::Release);
        self.uncertain_fast
            .store(state.uncertain, Ordering::Release);
        self.admission_fast
            .store(seeded && !state.uncertain, Ordering::Release);
        self.passive_admission_fast
            .store(passive, Ordering::Release);
        self.ledger_generation_fast
            .store(state.ledger_generation, Ordering::Release);
        *self.token_fee_configs_fast.write().unwrap() = state.token_fee_configs.clone();
    }

    fn sync_state_to_virtual_account(
        &self,
        state: &mut SharedAccountState,
        instance_id: &str,
        observed_reservation_epoch: Option<u64>,
        observed_trade_epoch: Option<u64>,
    ) {
        let Some(ledger) = state.instances.get(instance_id).cloned() else {
            return;
        };
        let account = {
            let mut accounts = self.virtual_accounts.write().unwrap();
            accounts
                .entry(instance_id.to_string())
                .or_insert_with(|| Arc::new(VirtualAccount::new(instance_id.to_string(), &ledger)))
                .clone()
        };
        let _publication = account.reservation_publish.lock().unwrap();
        let mut lifecycle = account.lifecycle.lock().unwrap();
        Self::merge_concurrent_reservations(
            state,
            &account,
            &lifecycle,
            observed_reservation_epoch,
        );
        let merged_concurrent_trade = Self::merge_concurrent_trade_mutations(
            state,
            &account,
            &mut lifecycle,
            observed_trade_epoch,
        );
        if let Some(ledger) = state.instances.get(instance_id) {
            account.replace_ledger(ledger);
        }
        lifecycle.orders = state
            .orders
            .iter()
            .filter(|(_, order)| order.instance_id == instance_id)
            .map(|(coid, order)| (coid.clone(), order.clone()))
            .collect();
        lifecycle.trades = state
            .trades
            .iter()
            .filter(|(_, trade)| trade.ownership.instance_id == instance_id)
            .map(|(trade_key, trade)| (trade_key.clone(), trade.clone()))
            .collect();
        lifecycle.recovery_pending_orders = state
            .recovery_pending_orders
            .iter()
            .filter(|coid| lifecycle.orders.contains_key(*coid))
            .cloned()
            .collect();
        lifecycle.startup_query_repair_orders = state
            .startup_query_repair_orders
            .iter()
            .filter(|coid| lifecycle.orders.contains_key(*coid))
            .cloned()
            .collect();
        lifecycle.routine_cancel_audits = state
            .routine_cancel_audits
            .iter()
            .filter(|coid| lifecycle.orders.contains_key(*coid))
            .cloned()
            .collect();
        lifecycle.fee_attribution_pending = state
            .fee_attribution_pending
            .iter()
            .filter(|trade_key| lifecycle.trades.contains_key(*trade_key))
            .cloned()
            .collect();
        lifecycle.cancel_audit_anomalies = state
            .ownership_anomalies
            .keys()
            .filter_map(|key| key.strip_prefix("order_cancel_audit:"))
            .filter(|coid| lifecycle.orders.contains_key(*coid))
            .map(str::to_string)
            .collect();
        lifecycle.sidecar_checkpoint = state.sidecar_checkpoints.get(instance_id).cloned();
        if merged_concurrent_trade {
            recompute_reconciliation(state, "concurrent private trade publication merge");
        }
        let coids: HashSet<String> = lifecycle.orders.keys().cloned().collect();
        let oids: HashSet<String> = lifecycle
            .orders
            .values()
            .map(|order| normalize_order_id(&order.order_id))
            .collect();
        let trade_keys: HashSet<String> = lifecycle.trades.keys().cloned().collect();
        let trade_epoch = account.trade_epoch.load(Ordering::Acquire);
        drop(lifecycle);

        // Route retirement is explicit and event-scoped. Destructively
        // removing every route for an instance makes lock-free private reads
        // observe a transient hole while this snapshot is republished.
        for coid in &coids {
            self.coid_routes
                .insert(coid.clone(), instance_id.to_string());
        }
        for oid in &oids {
            self.oid_routes.insert(oid.clone(), instance_id.to_string());
        }
        for trade_key in &trade_keys {
            self.trade_routes
                .insert(trade_key.clone(), instance_id.to_string());
        }
        self.coid_routes.retain_owner_keys(instance_id, &coids);
        self.oid_routes.retain_owner_keys(instance_id, &oids);
        if account.trade_epoch.load(Ordering::Acquire) == trade_epoch {
            self.trade_routes
                .retain_owner_keys(instance_id, &trade_keys);
        }
        *self.anomalous_trade_keys.write().unwrap() = state
            .ownership_anomalies
            .keys()
            .filter_map(|key| key.strip_prefix("trade:"))
            .map(str::to_string)
            .collect();
        *self.anomalous_private_event_keys.write().unwrap() = state
            .ownership_anomalies
            .keys()
            .filter_map(|key| key.strip_prefix("private_event:"))
            .map(str::to_string)
            .collect();
        self.seeded_fast.store(state.seeded, Ordering::Release);
        self.uncertain_fast
            .store(state.uncertain, Ordering::Release);
        let passive =
            state.seeded && (!state.uncertain || fee_degradation_is_only_uncertainty(state));
        self.admission_fast
            .store(state.seeded && !state.uncertain, Ordering::Release);
        self.passive_admission_fast
            .store(passive, Ordering::Release);
        self.ledger_generation_fast
            .store(state.ledger_generation, Ordering::Release);
        *self.token_fee_configs_fast.write().unwrap() = state.token_fee_configs.clone();
    }

    /// Query-repair orders that still lack authoritative terminal/live
    /// resolution. An empty result is the live-startup admission condition.
    pub fn startup_query_repair_pending_order_ids(&self) -> Vec<String> {
        let _control = self.control_gate.read().unwrap();
        let accounts = self.virtual_accounts.read().unwrap();
        let mut pending: Vec<String> = accounts
            .values()
            .flat_map(|account| {
                account
                    .lifecycle
                    .lock()
                    .unwrap()
                    .startup_query_repair_orders
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect();
        pending.sort();
        pending
    }

    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    fn notify_order_audit_worker(&self) {
        let (generation, wake) = &self.order_audit_wakeup;
        let mut generation = generation.lock().unwrap();
        *generation = generation.wrapping_add(1);
        wake.notify_all();
    }

    /// Current edge generation for the background order-audit worker.
    pub fn order_audit_generation(&self) -> u64 {
        *self.order_audit_wakeup.0.lock().unwrap()
    }

    /// Wait until new audit work is queued or the bounded shutdown poll
    /// expires. Comparing the generation while holding the same mutex closes
    /// the snapshot-to-sleep missed-wakeup race.
    pub fn wait_for_order_audit_work(&self, observed: u64, timeout: Duration) -> u64 {
        let (generation, wake) = &self.order_audit_wakeup;
        let mut current = generation.lock().unwrap();
        if *current == observed {
            current = wake.wait_timeout(current, timeout).unwrap().0;
        }
        *current
    }

    fn schedule_persist(&self, _state: &SharedAccountState) {
        if let Some(persistence) = &self.persistence {
            persistence.schedule();
        }
    }

    fn schedule_typed_persist(
        &self,
        state: &SharedAccountState,
        changes: Result<Vec<PersistenceWalChange>, String>,
    ) {
        let Some(persistence) = &self.persistence else {
            return;
        };
        match changes {
            Ok(changes) if !changes.is_empty() => persistence.schedule_delta(changes),
            Ok(_) => {}
            Err(error) => {
                log::error!(
                    "[shared_account] account={} typed WAL delta capture failed; falling back to full snapshot: {}",
                    self.account_id,
                    error,
                );
                let _ = state;
                persistence.schedule();
            }
        }
    }

    fn schedule_virtual_changes(
        &self,
        instance_id: &str,
        changes: Result<Vec<PersistenceWalChange>, String>,
    ) {
        let Some(persistence) = &self.persistence else {
            return;
        };
        match changes {
            Ok(changes) if !changes.is_empty() => {
                persistence.schedule_delta(changes);
            }
            Ok(_) => {}
            Err(error) => {
                // A hot-shard serialization failure cannot safely fall back to
                // the cold aggregate because the latter is deliberately only
                // eventually mirrored. Fail admission closed and let startup
                // reconciliation recover the exchange state.
                self.admission_fast.store(false, Ordering::Release);
                self.passive_admission_fast.store(false, Ordering::Release);
                log::error!(
                    "[shared_account] account={} instance={} virtual-account WAL capture failed; admission disabled: {}",
                    self.account_id,
                    instance_id,
                    error,
                );
            }
        }
    }

    fn schedule_virtual_lifecycle_persist(
        &self,
        account: &VirtualAccount,
        lifecycle: &VirtualLifecycle,
        client_order_id: &str,
    ) {
        let Some(persistence) = &self.persistence else {
            return;
        };
        let order = lifecycle.orders.get(client_order_id).cloned();
        let reserved_position = order.as_ref().map(|order| {
            let reserved = account
                .positions
                .read()
                .unwrap()
                .get(&order.token_id)
                .map(|position| position.reserved.load())
                .unwrap_or(0.0);
            (order.token_id.clone(), reserved)
        });
        persistence.schedule_virtual_lifecycle(VirtualLifecyclePersistenceDelta {
            instance_id: account.instance_id.clone(),
            reserved_cash: account.reserved_cash.load(),
            client_order_id: client_order_id.to_string(),
            order,
            reserved_position,
            recovery_pending: lifecycle.recovery_pending_orders.contains(client_order_id),
            startup_query_repair: lifecycle
                .startup_query_repair_orders
                .contains(client_order_id),
            routine_cancel_audit: lifecycle.routine_cancel_audits.contains(client_order_id),
        });
    }

    fn schedule_virtual_sidecar_persist(
        &self,
        account: &VirtualAccount,
        checkpoint: Option<&DurableSidecarCheckpoint>,
    ) {
        if self.persistence.is_none() {
            return;
        }
        let instance_id = account.instance_id.as_str();
        let changes = (|| -> Result<Vec<PersistenceWalChange>, String> {
            let mut changes = Vec::with_capacity(1);
            persistence_wal_map_entry(
                &mut changes,
                "sidecar_checkpoints",
                instance_id,
                checkpoint,
            )?;
            Ok(changes)
        })();
        self.schedule_virtual_changes(instance_id, changes);
    }

    fn schedule_virtual_rebind_persist(
        &self,
        account: &VirtualAccount,
        lifecycle: &VirtualLifecycle,
        client_order_id: &str,
        old_order_id: &str,
    ) {
        if self.persistence.is_none() {
            return;
        }
        let instance_id = account.instance_id.as_str();
        let changes = (|| -> Result<Vec<PersistenceWalChange>, String> {
            let mut changes = Vec::with_capacity(4);
            let order = lifecycle.orders.get(client_order_id);
            persistence_wal_map_entry(&mut changes, "orders", client_order_id, order)?;
            let old_normalized = normalize_order_id(old_order_id);
            let new_normalized = order
                .map(|order| normalize_order_id(&order.order_id))
                .unwrap_or_default();
            if !old_normalized.is_empty() && old_normalized != new_normalized {
                changes.push(PersistenceWalChange::Remove {
                    path: vec!["oid_to_coid".to_string(), old_normalized],
                });
            }
            if !new_normalized.is_empty() {
                persistence_wal_map_entry(
                    &mut changes,
                    "oid_to_coid",
                    &new_normalized,
                    Some(&client_order_id.to_string()),
                )?;
            }
            Ok(changes)
        })();
        self.schedule_virtual_changes(instance_id, changes);
    }

    fn schedule_virtual_trade_persist(
        &self,
        account: &VirtualAccount,
        lifecycle: &VirtualLifecycle,
        trade_key: &str,
        client_order_id: &str,
        token_id: &str,
    ) {
        let Some(persistence) = &self.persistence else {
            return;
        };
        let position = account.position(token_id);
        persistence.schedule_virtual_trade(VirtualTradePersistenceDelta {
            instance_id: account.instance_id.clone(),
            cash: account.cash.load(),
            reserved_cash: account.reserved_cash.load(),
            token_id: token_id.to_string(),
            position: position.balance.load(),
            reserved_position: position.reserved.load(),
            client_order_id: client_order_id.to_string(),
            order: lifecycle.orders.get(client_order_id).cloned(),
            trade_key: trade_key.to_string(),
            trade: lifecycle.trades.get(trade_key).cloned(),
            fee_attribution_pending: lifecycle.fee_attribution_pending.contains(trade_key),
            recovery_pending: lifecycle.recovery_pending_orders.contains(client_order_id),
            routine_cancel_audit: lifecycle.routine_cancel_audits.contains(client_order_id),
            ledger_generation: self.ledger_generation_fast.load(Ordering::Acquire),
        });
    }

    fn schedule_settled_prune_persist(
        &self,
        state: &SharedAccountState,
        outcomes: &[SettledPruneOutcome],
        retired_conditions: &[String],
    ) {
        if self.persistence.is_none() {
            return;
        }
        let changes = (|| -> Result<Vec<PersistenceWalChange>, String> {
            let mut changes = Vec::new();
            for outcome in outcomes {
                for (coid, order_id) in &outcome.orders {
                    persistence_wal_map_entry(
                        &mut changes,
                        "orders",
                        coid,
                        state.orders.get(coid),
                    )?;
                    let normalized = normalize_order_id(order_id);
                    persistence_wal_map_entry(
                        &mut changes,
                        "oid_to_coid",
                        &normalized,
                        state.oid_to_coid.get(&normalized),
                    )?;
                }
                for trade_key in &outcome.trades {
                    persistence_wal_map_entry(
                        &mut changes,
                        "trades",
                        trade_key,
                        state.trades.get(trade_key),
                    )?;
                    persistence_wal_map_entry(
                        &mut changes,
                        "retired_trade_ownership_tombstones",
                        trade_key,
                        state.retired_trade_ownership_tombstones.get(trade_key),
                    )?;
                    persistence_wal_set_membership(
                        &mut changes,
                        "fee_attribution_pending",
                        trade_key,
                        state.fee_attribution_pending.contains(trade_key),
                    )?;
                }
                for trade_key in &outcome.expired_tombstones {
                    persistence_wal_map_entry::<RetiredTradeOwnershipTombstone>(
                        &mut changes,
                        "retired_trade_ownership_tombstones",
                        trade_key,
                        None,
                    )?;
                }
                for token in &outcome.fee_tokens {
                    persistence_wal_map_entry(
                        &mut changes,
                        "token_fee_configs",
                        token,
                        state.token_fee_configs.get(token),
                    )?;
                }
            }
            if outcomes.iter().any(|outcome| !outcome.trades.is_empty()) {
                persistence_wal_set(
                    &mut changes,
                    ["compacted_economic_effects".to_string()],
                    &state.compacted_economic_effects,
                )?;
            }
            for condition_id in retired_conditions {
                persistence_wal_map_entry(
                    &mut changes,
                    "settled_audit_references",
                    condition_id,
                    state.settled_audit_references.get(condition_id),
                )?;
            }
            Ok(changes)
        })();
        self.schedule_typed_persist(state, changes);
    }

    fn clear_cancel_audit_anomaly(&self, client_order_id: &str) {
        let mut state = self.lock_state();
        if state
            .ownership_anomalies
            .remove(&format!("order_cancel_audit:{client_order_id}"))
            .is_some()
        {
            recompute_reconciliation(&mut state, "valid terminal order audit");
            self.schedule_persist(&state);
        }
    }

    /// Capture the exact account entries a private trade transition can touch.
    /// Large historical maps are updated by key; no complete `trades`,
    /// `orders`, or `instances` collection is cloned on the user-feed thread.
    fn schedule_trade_persist(
        &self,
        state: &SharedAccountState,
        trade_key: &str,
        client_order_id: &str,
        order_id: &str,
        token_id: &str,
    ) {
        if self.persistence.is_none() {
            return;
        }
        let changes = (|| -> Result<Vec<PersistenceWalChange>, String> {
            let normalized_order_id = normalize_order_id(order_id);
            let resolved_coid = state
                .oid_to_coid
                .get(&normalized_order_id)
                .map(String::as_str)
                .or_else(|| (!client_order_id.is_empty()).then_some(client_order_id))
                .unwrap_or_default();
            let instance_id = state
                .orders
                .get(resolved_coid)
                .map(|order| order.instance_id.as_str())
                .or_else(|| {
                    state
                        .trades
                        .get(trade_key)
                        .map(|trade| trade.ownership.instance_id.as_str())
                });
            let mut changes = Vec::with_capacity(20);
            persistence_wal_set(
                &mut changes,
                ["physical_cash".to_string()],
                &state.physical_cash,
            )?;
            persistence_wal_set(
                &mut changes,
                ["unallocated_cash".to_string()],
                &state.unallocated_cash,
            )?;
            persistence_wal_map_entry(
                &mut changes,
                "physical_positions",
                token_id,
                state.physical_positions.get(token_id),
            )?;
            persistence_wal_set(
                &mut changes,
                ["unallocated_positions".to_string()],
                &state.unallocated_positions,
            )?;
            persistence_wal_set(
                &mut changes,
                ["provisional_position_owners".to_string()],
                &state.provisional_position_owners,
            )?;
            if let Some(instance_id) = instance_id {
                persistence_wal_map_entry(
                    &mut changes,
                    "instances",
                    instance_id,
                    state.instances.get(instance_id),
                )?;
            }
            if !resolved_coid.is_empty() {
                persistence_wal_map_entry(
                    &mut changes,
                    "orders",
                    resolved_coid,
                    state.orders.get(resolved_coid),
                )?;
            }
            if !normalized_order_id.is_empty() {
                persistence_wal_map_entry(
                    &mut changes,
                    "oid_to_coid",
                    &normalized_order_id,
                    state.oid_to_coid.get(&normalized_order_id),
                )?;
            }
            if !trade_key.is_empty() {
                persistence_wal_map_entry(
                    &mut changes,
                    "trades",
                    trade_key,
                    state.trades.get(trade_key),
                )?;
                persistence_wal_map_entry(
                    &mut changes,
                    "retired_trade_ownership_tombstones",
                    trade_key,
                    state.retired_trade_ownership_tombstones.get(trade_key),
                )?;
                persistence_wal_map_entry(
                    &mut changes,
                    "unresolved_trade_match_times",
                    trade_key,
                    state.unresolved_trade_match_times.get(trade_key),
                )?;
            }
            let anomaly_key = if trade_key.is_empty() {
                format!("trade:<missing>:{order_id}")
            } else {
                format!("trade:{trade_key}")
            };
            persistence_wal_map_entry(
                &mut changes,
                "ownership_anomalies",
                &anomaly_key,
                state.ownership_anomalies.get(&anomaly_key),
            )?;
            persistence_wal_set(
                &mut changes,
                ["fee_attribution_pending".to_string()],
                &state.fee_attribution_pending,
            )?;
            persistence_wal_set(
                &mut changes,
                ["recovery_pending_orders".to_string()],
                &state.recovery_pending_orders,
            )?;
            persistence_wal_set(
                &mut changes,
                ["routine_cancel_audits".to_string()],
                &state.routine_cancel_audits,
            )?;
            persistence_wal_set(
                &mut changes,
                ["verified_trade_replay_recoveries".to_string()],
                &state.verified_trade_replay_recoveries,
            )?;
            persistence_wal_set(
                &mut changes,
                ["ledger_generation".to_string()],
                &state.ledger_generation,
            )?;
            persistence_wal_set(&mut changes, ["uncertain".to_string()], &state.uncertain)?;
            persistence_wal_set(
                &mut changes,
                ["uncertain_reason".to_string()],
                &state.uncertain_reason,
            )?;
            persistence_wal_set(
                &mut changes,
                ["uncertain_since_ms".to_string()],
                &state.uncertain_since_ms,
            )?;
            Ok(changes)
        })();
        self.schedule_typed_persist(state, changes);
    }

    pub fn flush_persistence(&self, timeout: Duration) -> Result<(), String> {
        self.persistence
            .as_ref()
            .map_or(Ok(()), |p| p.flush(timeout))
    }

    /// Non-blocking persistence health probe for event-loop callers. Returns
    /// `Ok(false)` while the single writer is catching up and `Err` only after
    /// the writer has observed a real persistence failure.
    pub fn persistence_is_current(&self) -> Result<bool, String> {
        self.persistence
            .as_ref()
            .map_or(Ok(true), AccountPersistence::is_current)
    }

    fn refresh_trade_persistence_blocker(&self) {
        let generation = self
            .trade_persistence_pending_generation
            .load(Ordering::Acquire);
        if generation == 0 {
            return;
        }
        let Some(persistence) = self.persistence.as_ref() else {
            self.trade_persistence_pending_generation
                .store(0, Ordering::Release);
            return;
        };
        if let Some(error) = persistence.last_error() {
            let reason = format!("trade generation {generation} is not durable: {error}");
            if self
                .trade_persistence_blocker_active
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.set_risk_blocker(TRADE_PERSISTENCE_RISK_BLOCKER, reason);
            }
            return;
        }
        let durable = persistence.generation_is_durable(generation);
        if !durable {
            return;
        }
        if self
            .trade_persistence_pending_generation
            .compare_exchange(generation, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            && self
                .trade_persistence_blocker_active
                .swap(false, Ordering::AcqRel)
        {
            self.clear_risk_blocker(TRADE_PERSISTENCE_RISK_BLOCKER);
        }
    }

    /// Record the newest trade generation without waiting for the WAL writer.
    /// The user-feed thread calls this immediately after applying an economic
    /// transition; admission observes writer failure through
    /// `refresh_trade_persistence_blocker` instead of imposing an fsync barrier
    /// on every private event.
    fn track_trade_persistence_generation(&self) {
        let generation = self
            .persistence
            .as_ref()
            .map_or(0, AccountPersistence::scheduled_generation);
        if generation == 0 {
            return;
        }
        self.trade_persistence_pending_generation
            .fetch_max(generation, Ordering::AcqRel);
        self.refresh_trade_persistence_blocker();
    }

    fn flush_admission_persistence(&self) -> Result<(), ReservationError> {
        self.flush_persistence(Duration::from_millis(250))
            .map_err(ReservationError::PersistenceUnavailable)
    }

    fn ensure_admission_persistence(&self) -> Result<(), ReservationError> {
        if self
            .persistence
            .as_ref()
            .and_then(AccountPersistence::last_error)
            .is_some()
        {
            let state = self.lock_state();
            self.schedule_persist(&state);
            drop(state);
            self.flush_admission_persistence()?;
        }
        Ok(())
    }

    pub fn persistence_path(&self) -> Option<&Path> {
        self.persistence.as_ref().map(|p| p.path.as_path())
    }

    pub fn sidecar_checkpoint(&self, sidecar_id: &str) -> Option<DurableSidecarCheckpoint> {
        let control = self.control_gate.read().unwrap();
        if let Some(account) = self.virtual_account(sidecar_id) {
            return account.lifecycle.lock().unwrap().sidecar_checkpoint.clone();
        }
        drop(control);
        self.lock_state()
            .sidecar_checkpoints
            .get(sidecar_id)
            .cloned()
    }

    /// Advance a sidecar marker only after the owning strategy has fsynced the
    /// corresponding generation. Older completions are harmless because the
    /// asynchronous sidecar writer may coalesce or finish out of callback order.
    pub fn record_sidecar_checkpoint(
        &self,
        sidecar_id: &str,
        checkpoint: DurableSidecarCheckpoint,
    ) -> Result<bool, String> {
        if sidecar_id.trim().is_empty()
            || checkpoint.generation == 0
            || checkpoint.expected_entries == 0
            || checkpoint.recovery_payload.trim().is_empty()
        {
            return Err(
                "sidecar checkpoint requires id, generation, entries and recovery payload"
                    .to_string(),
            );
        }
        let control = self.control_gate.read().unwrap();
        if let Some(account) = self.virtual_account(sidecar_id) {
            let mut lifecycle = account.lifecycle.lock().unwrap();
            if lifecycle
                .sidecar_checkpoint
                .as_ref()
                .is_some_and(|existing| existing.generation >= checkpoint.generation)
            {
                return Ok(false);
            }
            lifecycle.sidecar_checkpoint = Some(checkpoint);
            self.schedule_virtual_sidecar_persist(&account, lifecycle.sidecar_checkpoint.as_ref());
            return Ok(true);
        }
        drop(control);
        let mut state = self.lock_state();
        if state
            .sidecar_checkpoints
            .get(sidecar_id)
            .is_some_and(|existing| existing.generation >= checkpoint.generation)
        {
            return Ok(false);
        }
        state
            .sidecar_checkpoints
            .insert(sidecar_id.to_string(), checkpoint);
        self.schedule_persist(&state);
        Ok(true)
    }

    /// Replace a checkpoint whose opaque payload the owning subsystem has
    /// independently proved invalid.  This compare-and-swap escape hatch is
    /// deliberately separate from the monotonic fast path above: a corrupt
    /// marker may advertise `u64::MAX`, so generation ordering alone can never
    /// repair it with a valid lower-generation recovery payload.
    pub fn repair_sidecar_checkpoint(
        &self,
        sidecar_id: &str,
        invalid_generation: u64,
        replacement: DurableSidecarCheckpoint,
    ) -> Result<bool, String> {
        if sidecar_id.trim().is_empty()
            || invalid_generation == 0
            || replacement.generation == 0
            || replacement.expected_entries == 0
            || replacement.recovery_payload.trim().is_empty()
        {
            return Err(
                "sidecar checkpoint repair requires id, observed generation, entries and recovery payload"
                    .to_string(),
            );
        }
        let control = self.control_gate.read().unwrap();
        if let Some(account) = self.virtual_account(sidecar_id) {
            let mut lifecycle = account.lifecycle.lock().unwrap();
            if lifecycle
                .sidecar_checkpoint
                .as_ref()
                .is_none_or(|existing| existing.generation != invalid_generation)
            {
                return Ok(false);
            }
            lifecycle.sidecar_checkpoint = Some(replacement);
            self.schedule_virtual_sidecar_persist(&account, lifecycle.sidecar_checkpoint.as_ref());
            return Ok(true);
        }
        drop(control);
        let mut state = self.lock_state();
        let Some(existing) = state.sidecar_checkpoints.get(sidecar_id) else {
            return Ok(false);
        };
        if existing.generation != invalid_generation {
            return Ok(false);
        }
        state
            .sidecar_checkpoints
            .insert(sidecar_id.to_string(), replacement);
        self.schedule_persist(&state);
        Ok(true)
    }

    /// Register an instance before the first physical snapshot. Non-positive
    /// and non-finite weights are normalized to the default equal weight 1.0.
    /// Once an account has been seeded, a new member or changed weight never
    /// silently reallocates PnL: admission becomes fail-closed until an explicit
    /// [`Self::migrate_cash_allocation`] operation is durably recorded.
    pub fn register_instance(&self, instance_id: &str, weight: f64) {
        if instance_id.is_empty() {
            return;
        }
        let weight = if weight.is_finite() && weight > 0.0 {
            weight
        } else {
            1.0
        };
        let mut state = self.lock_state();
        let previous = state
            .instances
            .get(instance_id)
            .map(|instance| instance.weight);
        state
            .instances
            .entry(instance_id.to_string())
            .and_modify(|instance| instance.weight = weight)
            .or_insert_with(|| InstanceLedger::new(weight));
        if state.seeded && previous.is_none_or(|old| (old - weight).abs() > EPS) {
            state.allocation_migration_required = Some(match previous {
                Some(old) => format!(
                    "configured weight changed for instance `{instance_id}`: {old:.6} -> {weight:.6}; explicit cash allocation migration required"
                ),
                None => format!(
                    "configured instance `{instance_id}` joined an already-seeded account; explicit cash allocation migration required"
                ),
            });
            recompute_reconciliation(&mut state, "account membership changed");
        }
        self.schedule_persist(&state);
    }

    pub fn register_market_scope(&self, instance_id: &str, scope_key: &str) {
        let scope_key = scope_key.trim().to_ascii_lowercase();
        if instance_id.is_empty() || scope_key.is_empty() {
            return;
        }
        let mut state = self.lock_state();
        let Some(instance) = state.instances.get_mut(instance_id) else {
            set_ownership_anomaly(
                &mut state,
                format!("market_scope:{instance_id}:{scope_key}"),
                format!("cannot register market scope `{scope_key}` for unknown instance `{instance_id}`"),
            );
            self.schedule_persist(&state);
            return;
        };
        if instance.market_scopes.insert(scope_key) {
            self.schedule_persist(&state);
        }
    }

    /// Explicitly redistribute only virtual USDC across the configured account
    /// members. Token positions, trades and order ownership never move. The
    /// operation id is durable and idempotent so an operator can safely retry a
    /// startup after an fsync or process failure.
    pub fn migrate_cash_allocation(
        &self,
        operation_id: &str,
        target_weights: &BTreeMap<String, f64>,
    ) -> Result<CashAllocationMigration, ReservationError> {
        if operation_id.trim().is_empty() || target_weights.is_empty() {
            return Err(ReservationError::InvalidOrder(
                "cash allocation migration requires a non-empty operation id and target weights"
                    .into(),
            ));
        }
        if target_weights.iter().any(|(instance_id, weight)| {
            instance_id.is_empty() || !weight.is_finite() || *weight <= 0.0
        }) {
            return Err(ReservationError::InvalidOrder(
                "cash allocation migration weights must have non-empty instance ids and finite positive values"
                    .into(),
            ));
        }

        let mut state = self.lock_state();
        if let Some(existing) = state.cash_allocation_migrations.get(operation_id) {
            return if existing.target_weights == *target_weights {
                Ok(existing.clone())
            } else {
                Err(ReservationError::InvalidOrder(format!(
                    "cash allocation migration id `{operation_id}` was already used with different weights"
                )))
            };
        }
        if !state.seeded {
            return Err(ReservationError::AccountNotSeeded);
        }
        if target_weights
            .keys()
            .any(|instance_id| !state.instances.contains_key(instance_id))
        {
            return Err(ReservationError::InvalidOrder(
                "cash allocation migration references an unregistered instance".into(),
            ));
        }
        let has_reservations = state.instances.values().any(|instance| {
            instance.total_reserved_cash() > EPS
                || instance
                    .total_reserved_positions()
                    .values()
                    .any(|qty| *qty > EPS)
        });
        let has_live_orders = state.orders.values().any(|order| {
            !matches!(
                order.status,
                OrderStatus::Cancelled | OrderStatus::Rejected | OrderStatus::Filled
            )
        });
        if has_reservations
            || has_live_orders
            || !state.recovery_pending_orders.is_empty()
            || !state.routine_cancel_audits.is_empty()
            || has_unsettled_trade_lifecycle(&state)
            || has_unsettled_maintenance_operation(&state)
        {
            return Err(ReservationError::InvalidOrder(
                "cash allocation migration requires no live reservations, orders, trades, or maintenance operations"
                    .into(),
            ));
        }

        let total_weight: f64 = target_weights.values().sum();
        let total_cash: f64 = state.instances.values().map(|instance| instance.cash).sum();
        let cash_before = state
            .instances
            .iter()
            .map(|(id, instance)| (id.clone(), instance.cash))
            .collect();
        for (instance_id, instance) in &mut state.instances {
            if let Some(weight) = target_weights.get(instance_id) {
                instance.weight = *weight;
                instance.cash = total_cash * *weight / total_weight;
            } else {
                instance.cash = 0.0;
            }
        }
        let cash_after = state
            .instances
            .iter()
            .map(|(id, instance)| (id.clone(), instance.cash))
            .collect();
        let migration = CashAllocationMigration {
            operation_id: operation_id.to_string(),
            target_weights: target_weights.clone(),
            cash_before,
            cash_after,
            recorded_at_ms: wall_clock_ms(),
        };
        state
            .cash_allocation_migrations
            .insert(operation_id.to_string(), migration.clone());
        state.allocation_migration_required = None;
        recompute_reconciliation(&mut state, "explicit cash allocation migration");
        self.schedule_persist(&state);
        drop(state);
        self.flush_admission_persistence()?;
        Ok(migration)
    }

    /// Register the event/token scope traded by one instance. Token inventory
    /// is never cold-allocated to a sibling that did not register the token.
    pub fn register_token_interest(
        &self,
        instance_id: &str,
        condition_id: &str,
        up_token_id: &str,
        down_token_id: &str,
    ) -> Result<(), ReservationError> {
        self.register_token_interest_scoped(
            instance_id,
            condition_id,
            up_token_id,
            down_token_id,
            "",
        )
    }

    pub fn register_token_interest_scoped(
        &self,
        instance_id: &str,
        condition_id: &str,
        up_token_id: &str,
        down_token_id: &str,
        scope_key: &str,
    ) -> Result<(), ReservationError> {
        if instance_id.is_empty()
            || condition_id.is_empty()
            || up_token_id.is_empty()
            || down_token_id.is_empty()
        {
            return Err(ReservationError::InvalidOrder(
                "token interest requires instance/condition/up/down identifiers".into(),
            ));
        }
        let mut state = self.lock_state();
        let Some(instance) = state.instances.get_mut(instance_id) else {
            return Err(ReservationError::UnknownInstance(instance_id.into()));
        };
        let requested_scope = scope_key.trim().to_ascii_lowercase();
        let scope_key = if requested_scope.is_empty() && instance.market_scopes.len() == 1 {
            instance
                .market_scopes
                .iter()
                .next()
                .cloned()
                .unwrap_or_default()
        } else {
            requested_scope
        };
        instance.token_interests.insert(
            condition_id.to_string(),
            TokenInterest {
                instance_id: instance_id.to_string(),
                condition_id: condition_id.to_string(),
                up_token_id: up_token_id.to_string(),
                down_token_id: down_token_id.to_string(),
                scope_key,
                retire_after_ms: None,
            },
        );
        // Never redistribute an already-seeded ledger here. Live startup
        // registers every configured instance before the first fetch; a scope
        // added later must not rewrite cash, PnL, or trade-owned inventory.
        self.schedule_persist(&state);
        Ok(())
    }

    pub fn token_interests(&self) -> Vec<TokenInterest> {
        let mut state = self.lock_state();
        let now_ms = wall_clock_ms();
        // Keep every owned historical token in the explicit ERC-1155 and
        // settlement-query scope until physical and virtual quantities both
        // reach zero. Settlement can happen while the process is stopped, so
        // winner proof may not exist in the ledger yet and Data API may omit
        // the already-auto-redeemed rows entirely.
        let mut owned_tokens_requiring_zero: HashSet<String> = state
            .physical_positions
            .iter()
            .filter(|(_, qty)| **qty > EPS)
            .map(|(token, _)| token.clone())
            .collect();
        owned_tokens_requiring_zero.extend(
            state
                .instances
                .values()
                .flat_map(|instance| instance.positions.iter())
                .filter(|(_, qty)| **qty > EPS)
                .map(|(token, _)| token.clone()),
        );
        let mut pruned = false;
        for instance in state.instances.values_mut() {
            let before = instance.token_interests.len();
            instance.token_interests.retain(|_, interest| {
                interest
                    .retire_after_ms
                    .is_none_or(|deadline| deadline > now_ms)
                    || owned_tokens_requiring_zero.contains(&interest.up_token_id)
                    || owned_tokens_requiring_zero.contains(&interest.down_token_id)
            });
            pruned |= instance.token_interests.len() != before;
        }
        let mut interests: Vec<TokenInterest> = state
            .instances
            .values()
            .flat_map(|instance| instance.token_interests.values().cloned())
            .collect();
        let mut known: HashSet<(String, String, String, String)> = interests
            .iter()
            .map(|interest| {
                (
                    interest.instance_id.clone(),
                    interest.condition_id.clone(),
                    interest.up_token_id.clone(),
                    interest.down_token_id.clone(),
                )
            })
            .collect();
        // A recovered split can predate the strategy's durable event-interest
        // registry. Keep its confirmed operation root as a read-only wallet
        // query scope until both physical and virtual legs are observed at
        // zero. This lets a later platform auto-redeem close historical
        // inventory without resurrecting the event in strategy runtime state.
        for operation in state
            .maintenance_ops
            .values()
            .filter(|operation| operation.status == MaintenanceOperationStatus::Confirmed)
        {
            let physical_outstanding = [&operation.up_token_id, &operation.down_token_id]
                .into_iter()
                .any(|token| {
                    state
                        .physical_positions
                        .get(token)
                        .is_some_and(|quantity| *quantity > EPS)
                });
            for instance_id in operation.allocations.keys() {
                let Some(instance) = state.instances.get(instance_id) else {
                    continue;
                };
                let virtual_outstanding = [&operation.up_token_id, &operation.down_token_id]
                    .into_iter()
                    .any(|token| {
                        instance
                            .positions
                            .get(token)
                            .is_some_and(|quantity| *quantity > EPS)
                    });
                let identity = (
                    instance_id.clone(),
                    operation.condition_id.clone(),
                    operation.up_token_id.clone(),
                    operation.down_token_id.clone(),
                );
                if (physical_outstanding || virtual_outstanding) && known.insert(identity) {
                    interests.push(TokenInterest {
                        instance_id: instance_id.clone(),
                        condition_id: operation.condition_id.clone(),
                        up_token_id: operation.up_token_id.clone(),
                        down_token_id: operation.down_token_id.clone(),
                        scope_key: instance
                            .market_scopes
                            .iter()
                            .next()
                            .filter(|_| instance.market_scopes.len() == 1)
                            .cloned()
                            .unwrap_or_default(),
                        retire_after_ms: Some(0),
                    });
                }
            }
        }
        if pruned {
            self.schedule_persist(&state);
        }
        interests
    }

    /// Whether the durable account ledger proves that the event owning this
    /// token has already ended. Any one of these monotonic markers is enough:
    /// the event entered the settled audit FIFO, its token interest was
    /// retired by the strategy, or an authoritative 0/1 outcome was recorded.
    /// This is intentionally narrower than merely having historical inventory.
    pub fn token_event_has_ended(&self, token_id: &str) -> bool {
        if token_id.trim().is_empty() {
            return false;
        }
        let state = self.lock_state();
        state.settled_token_values.contains_key(token_id)
            || state
                .settled_audit_references
                .values()
                .any(|reference| reference.asset_ids.iter().any(|token| token == token_id))
            || state.instances.values().any(|instance| {
                instance.token_interests.values().any(|interest| {
                    interest.retire_after_ms.is_some()
                        && (interest.up_token_id == token_id || interest.down_token_id == token_id)
                })
            })
    }

    /// Retire a finished/abandoned event after a ten-minute reconciliation
    /// grace. Existing virtual positions retain their direct instance ownership;
    /// their on-chain/settlement query scope expires only after inventory is
    /// authoritatively observed at zero.
    pub fn retire_token_interest(&self, instance_id: &str, condition_id: &str) {
        let mut state = self.lock_state();
        if let Some(instance) = state.instances.get_mut(instance_id) {
            if let Some(interest) = instance.token_interests.get_mut(condition_id) {
                interest.retire_after_ms = Some(wall_clock_ms().saturating_add(10 * 60 * 1000));
            }
        }
        self.schedule_persist(&state);
    }

    /// Idempotently retain one settled event's late-fill audit for an instance.
    /// The reference is persisted in the shared account ledger so process
    /// restart cannot let one instance clean history still promised by another.
    pub fn retain_settled_event_audit(
        &self,
        instance_id: &str,
        condition_id: &str,
        asset_ids: &[String],
    ) -> Result<(), ReservationError> {
        let normalized = validate_settled_audit_identity(instance_id, condition_id, asset_ids)?;
        {
            let mut state = self.lock_state();
            if !state.instances.contains_key(instance_id) {
                return Err(ReservationError::UnknownInstance(instance_id.to_string()));
            }
            match state.settled_audit_references.get_mut(condition_id) {
                Some(reference) => {
                    if reference.asset_ids != normalized {
                        return Err(ReservationError::InvalidOrder(format!(
                            "settled audit token identity changed for condition `{condition_id}`",
                        )));
                    }
                    reference.instances.insert(instance_id.to_string());
                }
                None => {
                    state.settled_audit_references.insert(
                        condition_id.to_string(),
                        SettledAuditReference {
                            condition_id: condition_id.to_string(),
                            asset_ids: normalized,
                            instances: BTreeSet::from([instance_id.to_string()]),
                        },
                    );
                }
            }
            let changes = (|| -> Result<Vec<PersistenceWalChange>, String> {
                let mut changes = Vec::with_capacity(1);
                persistence_wal_map_entry(
                    &mut changes,
                    "settled_audit_references",
                    condition_id,
                    state.settled_audit_references.get(condition_id),
                )?;
                Ok(changes)
            })();
            self.schedule_typed_persist(&state, changes);
        }
        let mut candidates = self.settled_gc_candidates.lock().unwrap();
        candidates.remove(condition_id);
        self.publish_settled_gc_candidates(&candidates);
        Ok(())
    }

    /// Release one instance's FIFO reference. The empty reference remains as a
    /// durable cleanup request while any order/trade can still be revised.
    pub fn release_settled_event_audit(
        &self,
        instance_id: &str,
        condition_id: &str,
        asset_ids: &[String],
    ) -> Result<(), ReservationError> {
        let normalized = validate_settled_audit_identity(instance_id, condition_id, asset_ids)?;
        let candidate_tokens = {
            let mut state = self.lock_state();
            let Some(reference) = state.settled_audit_references.get_mut(condition_id) else {
                // A failed-event eviction never entered the settled FIFO. Keep this
                // idempotent and conservative: instance-scoped rows are still pruned
                // by the caller, but no account-global history is destroyed.
                return Ok(());
            };
            if reference.asset_ids != normalized {
                return Err(ReservationError::InvalidOrder(format!(
                    "settled audit retirement token mismatch for condition `{condition_id}`",
                )));
            }
            reference.instances.remove(instance_id);
            let candidate_tokens = reference
                .instances
                .is_empty()
                .then(|| reference.asset_ids.clone());
            let changes = (|| -> Result<Vec<PersistenceWalChange>, String> {
                let mut changes = Vec::with_capacity(1);
                persistence_wal_map_entry(
                    &mut changes,
                    "settled_audit_references",
                    condition_id,
                    state.settled_audit_references.get(condition_id),
                )?;
                Ok(changes)
            })();
            self.schedule_typed_persist(&state, changes);
            candidate_tokens
        };
        if let Some(candidate_tokens) = candidate_tokens {
            let mut candidates = self.settled_gc_candidates.lock().unwrap();
            candidates.insert(condition_id.to_string(), candidate_tokens);
            self.publish_settled_gc_candidates(&candidates);
        }
        Ok(())
    }

    /// Claim a bounded batch of zero-reference events whose durable audit is
    /// fully terminal. Callers run this from the private-feed GC lane; bounding
    /// each transaction prevents a long settled backlog from monopolising the
    /// account control lock.
    pub fn finalize_ready_settled_audit_retirements(&self) -> Vec<HashSet<String>> {
        const SETTLED_GC_EVENTS_PER_BATCH: usize = 4;
        // Keep candidates published while the cold check runs. A terminal
        // trade racing this transaction can then still enqueue a follow-up
        // wake; removing a claimed key up front created a missed-wakeup gap.
        let claimed: Vec<String> = self
            .settled_gc_candidates
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        if claimed.is_empty() {
            return Vec::new();
        }
        let mut state = self.lock_state();
        let mut inactive = Vec::new();
        let ready: Vec<(String, HashSet<String>)> = claimed
            .into_iter()
            .filter_map(|condition_id| {
                let Some(reference) = state.settled_audit_references.get(&condition_id) else {
                    inactive.push(condition_id);
                    return None;
                };
                if !reference.instances.is_empty() {
                    inactive.push(condition_id);
                    return None;
                }
                let tokens: HashSet<String> = reference.asset_ids.iter().cloned().collect();
                if settled_audit_has_revisable_rows(&state, &tokens) {
                    None
                } else {
                    Some((condition_id, tokens))
                }
            })
            .take(SETTLED_GC_EVENTS_PER_BATCH)
            .collect();
        if ready.is_empty() {
            drop(state);
            if !inactive.is_empty() {
                let mut candidates = self.settled_gc_candidates.lock().unwrap();
                for condition_id in inactive {
                    candidates.remove(&condition_id);
                }
                self.publish_settled_gc_candidates(&candidates);
            }
            return Vec::new();
        }
        let mut retired = Vec::with_capacity(ready.len());
        let mut outcomes = Vec::with_capacity(ready.len());
        let mut retired_conditions = Vec::with_capacity(ready.len());
        for (condition_id, tokens) in ready {
            outcomes.push(prune_terminal_history_locked(&mut state, None, &tokens));
            state.settled_audit_references.remove(&condition_id);
            retired_conditions.push(condition_id.clone());
            inactive.push(condition_id);
            retired.push(tokens);
        }
        self.retired_trade_tombstone_count_fast.store(
            state.retired_trade_ownership_tombstones.len(),
            Ordering::Relaxed,
        );
        self.schedule_settled_prune_persist(&state, &outcomes, &retired_conditions);
        drop(state);
        if !inactive.is_empty() {
            let mut candidates = self.settled_gc_candidates.lock().unwrap();
            for condition_id in inactive {
                candidates.remove(&condition_id);
            }
            self.publish_settled_gc_candidates(&candidates);
        }
        retired
    }

    /// Cheap edge used by the exchange worker before sending a coalesced GC
    /// wakeup. An acquire load replaces the previous account-wide scan on
    /// every private trade lifecycle message.
    pub fn has_settled_gc_candidates(&self) -> bool {
        self.settled_gc_candidate_count_fast.load(Ordering::Acquire) > 0
    }

    pub fn has_settled_gc_candidate_for_token(&self, token_id: &str) -> bool {
        !token_id.is_empty()
            && self.has_settled_gc_candidates()
            && self
                .settled_gc_candidate_tokens_fast
                .load()
                .contains(token_id)
    }

    fn publish_settled_gc_candidates(&self, candidates: &BTreeMap<String, BTreeSet<String>>) {
        let tokens = candidates
            .values()
            .flat_map(|candidate_tokens| candidate_tokens.iter().cloned())
            .collect();
        self.settled_gc_candidate_tokens_fast
            .store(Arc::new(tokens));
        self.settled_gc_candidate_count_fast
            .store(candidates.len(), Ordering::Release);
    }

    /// Validate the final configured membership after every instance sharing
    /// this account has registered. Persisted owners missing from config keep
    /// their ledger rows for late attribution, but make admission fail closed
    /// until an explicit external ownership migration is recorded.
    pub fn reconcile_configured_instances(&self, configured: &HashSet<String>) {
        let mut state = self.lock_state();
        let mut stale = Vec::new();
        for (instance_id, instance) in &state.instances {
            if configured.contains(instance_id) {
                continue;
            }
            let owned_cash = instance.cash.abs() > EPS || instance.total_reserved_cash() > EPS;
            let owned_positions = instance.positions.values().any(|qty| qty.abs() > EPS)
                || instance
                    .reserved_positions
                    .values()
                    .any(|qty| qty.abs() > EPS);
            let owned_orders = state.orders.values().any(|order| {
                order.instance_id == *instance_id
                    && matches!(
                        order.status,
                        OrderStatus::Pending
                            | OrderStatus::Accepted
                            | OrderStatus::PartiallyFilled
                            | OrderStatus::NewOrderTimeout
                            | OrderStatus::CancelOrderTimeout
                            | OrderStatus::CancelUncertain
                    )
            });
            if owned_cash || owned_positions || owned_orders {
                stale.push(instance_id.clone());
            }
        }
        stale.sort();
        state.instance_registry_issue = (!stale.is_empty()).then(|| {
            format!(
                "persisted instance ownership missing from config: [{}]; explicit migration required",
                stale.join(","),
            )
        });
        recompute_reconciliation(&mut state, "instance registry reconciliation");
        self.schedule_persist(&state);
    }

    pub fn record_settled_token_values(&self, values: &HashMap<String, f64>) {
        let mut state = self.lock_state();
        let effective_generation = if state.settled_token_values.is_empty() {
            state.settled_token_values_generation
        } else {
            state.settled_token_values_generation.max(1)
        };
        let mut changed = false;
        for (token, value) in values {
            if !token.is_empty() && value.is_finite() && (*value == 0.0 || *value == 1.0) {
                if state.settled_token_values.get(token) != Some(value) {
                    state.settled_token_values.insert(token.clone(), *value);
                    changed = true;
                }
            }
        }
        if changed {
            state.settled_token_values_generation = effective_generation.saturating_add(1).max(1);
            // A restart snapshot can arrive before the market outcome lookup.
            // Retry the same conservative inference while holding the account
            // lock so the outcome and any resulting virtual redemption are
            // persisted as one state transition.
            try_attribute_binary_redeem(&mut state);
            self.schedule_persist(&state);
        }
    }

    /// Apply one event's authoritative outcomes and retire its token-interest
    /// scope in a single ordered control-plane transaction. Strategy callbacks
    /// enqueue this operation; the account worker owns aggregate mutation,
    /// reconciliation and persistence.
    pub fn record_settlement_and_retire(
        &self,
        instance_id: &str,
        condition_id: &str,
        values: &HashMap<String, f64>,
    ) -> Result<(), ReservationError> {
        if instance_id.trim().is_empty() || condition_id.trim().is_empty() {
            return Err(ReservationError::InvalidOrder(
                "settlement mutation requires instance and condition identifiers".into(),
            ));
        }
        let mut state = self.lock_state();
        if !state.instances.contains_key(instance_id) {
            return Err(ReservationError::UnknownInstance(instance_id.to_string()));
        }
        let effective_generation = Self::effective_settled_token_values_generation(&state);
        let mut changed = false;
        for (token, value) in values {
            if !token.is_empty()
                && value.is_finite()
                && (*value == 0.0 || *value == 1.0)
                && state.settled_token_values.get(token) != Some(value)
            {
                state.settled_token_values.insert(token.clone(), *value);
                changed = true;
            }
        }
        if changed {
            state.settled_token_values_generation = effective_generation.saturating_add(1).max(1);
            try_attribute_binary_redeem(&mut state);
        }
        if let Some(interest) = state
            .instances
            .get_mut(instance_id)
            .and_then(|instance| instance.token_interests.get_mut(condition_id))
        {
            interest.retire_after_ms = Some(wall_clock_ms().saturating_add(10 * 60 * 1000));
            changed = true;
        }
        if changed {
            self.schedule_persist(&state);
        }
        Ok(())
    }

    /// Account-wide authoritative outcome snapshot. Strategies compare the
    /// generation before revising active and retained settled-event baselines.
    /// Kept for compatibility; latency-sensitive callers should retain the
    /// `Arc` returned by [`Self::settled_token_values_snapshot_arc`].
    pub fn settled_token_values_snapshot(&self) -> (u64, HashMap<String, f64>) {
        let snapshot = self.settled_token_values_snapshot_arc();
        (snapshot.generation, snapshot.values.clone())
    }

    pub fn settled_token_values_snapshot_arc(&self) -> Arc<SettledTokenValuesSnapshot> {
        self.settled_token_values_fast.load_full()
    }

    /// Persist the exchange fee curve for every outcome token in one event.
    /// Any cold-replayed taker trade that was waiting on this metadata is
    /// completed immediately and admission resumes only after all such fees
    /// have been attributed.
    pub fn register_token_fee_config(
        &self,
        token_ids: &[String],
        rate: f64,
        exponent: f64,
    ) -> Result<(), ReservationError> {
        if token_ids.is_empty()
            || token_ids.iter().any(|token| token.is_empty())
            || BinaryOption::validate_polymarket_fee_curve(rate, exponent, 0).is_err()
        {
            return Err(ReservationError::InvalidOrder(
                "token fee config requires tokens and a valid Polymarket v2 fee curve".into(),
            ));
        }
        let token_set: HashSet<&str> = token_ids.iter().map(String::as_str).collect();
        let mut state = self.lock_state();
        let next_config = TokenFeeConfig { rate, exponent };
        let revised_tokens: HashSet<String> = token_ids
            .iter()
            .filter(|token| {
                state
                    .token_fee_configs
                    .get(*token)
                    .is_some_and(|current| !fee_configs_equal(current, &next_config))
            })
            .cloned()
            .collect();

        // A curve swap cannot be made atomic while any affected trade still
        // lacks its role. Leave the old curve and account economics untouched;
        // the role-pending blocker remains the owner of admission.
        if let Some((trade_key, _)) = state.trades.iter().find(|(_, trade)| {
            revised_tokens.contains(&trade.ownership.token_id) && trade.is_maker.is_none()
        }) {
            return Err(ReservationError::InvalidOrder(format!(
                "cannot revise token fee curve while trade `{trade_key}` has unresolved maker/taker role"
            )));
        }

        for token in token_ids {
            state
                .token_fee_configs
                .insert(token.clone(), next_config.clone());
        }

        // Reprice every already-attributed execution before exposing the new
        // curve. Pending rows remain zero/unbooked and are handled by the retry
        // loop below. Trade-derived economics belong to virtual shards; the
        // authoritative wallet snapshot remains the sole physical-ledger writer.
        let attributed: Vec<(String, AppliedTrade)> = state
            .trades
            .iter()
            .filter(|(_, trade)| revised_tokens.contains(&trade.ownership.token_id))
            .filter(|(_, trade)| {
                trade.virtual_fee_booked
                    || trade.physical_fee_booked
                    || trade.failed
                    || trade.usdc_fee > EPS
                    || trade.shares_fee > EPS
            })
            .map(|(trade_key, trade)| (trade_key.clone(), trade.clone()))
            .collect();
        let mut generation_updates = Vec::new();
        for (trade_key, previous) in attributed {
            let is_maker = previous
                .is_maker
                .expect("fee-curve revision preflight rejected unresolved role");
            let (next_usdc, next_shares) = configured_fee_amounts(
                &previous.ownership,
                is_maker,
                (!is_maker).then_some(&next_config),
            );
            let usdc_delta = previous.usdc_fee - next_usdc;
            let shares_delta = previous.shares_fee - next_shares;
            let changed = usdc_delta.abs() > EPS || shares_delta.abs() > EPS;
            if previous.virtual_fee_booked && changed {
                if let Some(instance) = state.instances.get_mut(&previous.ownership.instance_id) {
                    instance.cash += usdc_delta;
                    *instance
                        .positions
                        .entry(previous.ownership.token_id.clone())
                        .or_insert(0.0) += shares_delta;
                }
                generation_updates.push(trade_key.clone());
            }
            if let Some(trade) = state.trades.get_mut(&trade_key) {
                trade.usdc_fee = next_usdc;
                trade.shares_fee = next_shares;
            }
        }
        for trade_key in generation_updates {
            advance_trade_ledger_generation(&mut state, &trade_key);
        }
        let retry: Vec<(String, OrderStatus, bool)> = state
            .fee_attribution_pending
            .iter()
            .filter_map(|trade_key| {
                let trade = state.trades.get(trade_key)?;
                if !token_set.contains(trade.ownership.token_id.as_str()) {
                    return None;
                }
                let status = match trade.ownership.status.as_str() {
                    "FAILED" => OrderStatus::Failed,
                    "CONFIRMED" => OrderStatus::Filled,
                    _ => OrderStatus::PartiallyFilled,
                };
                Some((trade_key.clone(), status, trade.is_maker?))
            })
            .collect();
        recompute_reconciliation(&mut state, "token fee curve registration/revision");
        self.schedule_persist(&state);
        drop(state);
        for (trade_key, status, is_maker) in retry {
            let _ = self.apply_configured_trade_fee(&trade_key, status, is_maker);
        }
        Ok(())
    }

    pub fn active_tokens(&self) -> HashSet<String> {
        self.token_interests()
            .into_iter()
            .flat_map(|interest| [interest.up_token_id, interest.down_token_id])
            .collect()
    }

    /// Apply the account snapshot used to establish this process's startup
    /// baseline. Later calls in the same process are ignored.
    pub fn apply_physical_snapshot(
        &self,
        cash: f64,
        positions: HashMap<String, f64>,
    ) -> Result<bool, String> {
        let mut authoritative_tokens: HashSet<String> = positions.keys().cloned().collect();
        let state = self.lock_state();
        authoritative_tokens.extend(state.physical_positions.keys().cloned());
        authoritative_tokens.extend(
            state
                .instances
                .values()
                .flat_map(|instance| instance.positions.keys().cloned()),
        );
        drop(state);
        self.apply_scoped_physical_snapshot(cash, positions, authoritative_tokens)
    }

    /// Apply a startup cash snapshot plus a token-scoped position view.
    /// Tokens outside `authoritative_tokens` retain their last physical value;
    /// this prevents a BTC-only RPC result from zeroing ETH/SOL inventory on a
    /// shared wallet. Callers should include every active account token they
    /// actually queried, including queried tokens whose balance is zero.
    pub fn apply_scoped_physical_snapshot(
        &self,
        cash: f64,
        positions: HashMap<String, f64>,
        authoritative_tokens: HashSet<String>,
    ) -> Result<bool, String> {
        self.apply_scoped_physical_snapshot_inner(None, cash, positions, authoritative_tokens)
    }

    /// Apply one account-level startup generation at most once.
    pub fn apply_scoped_physical_snapshot_versioned(
        &self,
        generation: u64,
        cash: f64,
        positions: HashMap<String, f64>,
        authoritative_tokens: HashSet<String>,
    ) -> Result<bool, String> {
        self.apply_scoped_physical_snapshot_inner(
            Some(generation),
            cash,
            positions,
            authoritative_tokens,
        )
    }

    fn apply_scoped_physical_snapshot_inner(
        &self,
        generation: Option<u64>,
        cash: f64,
        positions: HashMap<String, f64>,
        authoritative_tokens: HashSet<String>,
    ) -> Result<bool, String> {
        validate_physical_snapshot(cash, &positions, &authoritative_tokens)?;
        let mut state = self.lock_state();
        if state.startup_snapshot_applied_this_process {
            return Ok(false);
        }
        if !state.seeded {
            let missing = missing_initial_token_interest_owners(&state, &authoritative_tokens);
            if !missing.is_empty() {
                let now_ms = wall_clock_ms();
                let started_ms = *state.initial_token_barrier_started_ms.get_or_insert(now_ms);
                if now_ms.saturating_sub(started_ms) < INITIAL_TOKEN_BARRIER_TIMEOUT_MS {
                    log::info!(
                        "[shared_account] account={} initial allocation waiting for token-interest barrier: {}",
                        self.account_id, missing.join(", "),
                    );
                    return Ok(false);
                }
                state.initial_token_barrier_degraded_members = missing.clone();
                log::warn!(
                    "[shared_account] account={} token-interest barrier timed out after {}ms; seeding healthy members and degrading missing members: {}",
                    self.account_id, INITIAL_TOKEN_BARRIER_TIMEOUT_MS, missing.join(", "),
                );
            }
        }
        if let Some(generation) = generation {
            if generation == 0 || generation <= state.last_physical_snapshot_generation {
                return Ok(false);
            }
        }
        let positions = positions
            .into_iter()
            .filter(|(_, qty)| *qty > EPS)
            .collect::<HashMap<_, _>>();
        if !state.seeded {
            state.seeded = true;
            state.startup_snapshot_applied_this_process = true;
            if let Some(generation) = generation {
                state.last_physical_snapshot_generation = generation;
            }
            state.physical_cash = cash;
            state.physical_positions = positions
                .iter()
                .filter(|(token, _)| authoritative_tokens.contains(*token))
                .map(|(token, qty)| (token.clone(), *qty))
                .collect();
            redistribute_all(&mut state);
            state.seed_baseline = Some(capture_seed_baseline(&state, false));
            self.schedule_persist(&state);
            return Ok(true);
        }

        // A wallet snapshot has no trade ids. Applying it while a MATCHED trade
        // is still waiting for MINED/CONFIRMED creates an unavoidable race: the
        // snapshot may already contain that settlement, and the later lifecycle
        // edge would then apply the same physical delta a second time. Do not
        // guess individual trade finality from aggregate wallet equality. Keep
        // the trade-driven physical ledger unchanged and let the next snapshot
        // retry after every pending lifecycle has resolved.
        if has_unsettled_trade_lifecycle(&state) || has_unsettled_maintenance_operation(&state) {
            return Ok(false);
        }

        // Deferred snapshots must remain retryable with the same generation.
        if let Some(generation) = generation {
            state.last_physical_snapshot_generation = generation;
        }

        state.physical_cash = cash;
        for token in &authoritative_tokens {
            let physical = positions.get(token).copied().unwrap_or(0.0);
            if physical > EPS {
                state.physical_positions.insert(token.clone(), physical);
            } else {
                state.physical_positions.remove(token);
            }
        }
        recompute_reconciliation(&mut state, "authoritative physical snapshot");
        if mark_failed_trades_reconciled_by_snapshot(&mut state, &authoritative_tokens) {
            recompute_reconciliation(&mut state, "authoritative physical snapshot");
        }
        try_attribute_binary_redeem(&mut state);
        state.startup_snapshot_applied_this_process = true;
        self.schedule_persist(&state);
        Ok(true)
    }

    pub fn is_seeded(&self) -> bool {
        self.seeded_fast.load(Ordering::Acquire)
    }
    pub fn startup_snapshot_applied(&self) -> bool {
        self.startup_snapshot_applied_fast.load(Ordering::Acquire)
    }

    pub fn startup_snapshot_deferred_by_pending_lifecycle(&self) -> bool {
        self.startup_snapshot_deferred_fast.load(Ordering::Acquire)
    }

    /// Attribute only a proven 1:1 platform redemption. This deliberately
    /// does not turn a runtime wallet observation into a position snapshot.
    pub fn observe_platform_binary_redeem(
        &self,
        observed_cash: f64,
        observed_positions: &HashMap<String, f64>,
        authoritative_tokens: &HashSet<String>,
    ) -> bool {
        if !observed_cash.is_finite()
            || observed_cash < 0.0
            || observed_positions
                .values()
                .any(|qty| !qty.is_finite() || *qty < 0.0)
        {
            return false;
        }
        let mut state = self.lock_state();
        if !state.seeded
            || has_unsettled_trade_lifecycle(&state)
            || has_unsettled_maintenance_operation(&state)
        {
            return false;
        }
        let cash_delta = observed_cash - state.physical_cash;
        if cash_delta <= EPS {
            return false;
        }

        let mut removed = Vec::new();
        let mut conditions = BTreeSet::new();
        for token in authoritative_tokens {
            let prior = state.physical_positions.get(token).copied().unwrap_or(0.0);
            let observed = observed_positions.get(token).copied().unwrap_or(0.0);
            let delta = observed - prior;
            if delta.abs() <= reconciliation_tolerance(prior, observed) {
                continue;
            }
            let Some((condition_id, value)) = proven_binary_token_value(&state, token) else {
                return false;
            };
            if delta < 0.0 {
                removed.push((token.clone(), -delta, value));
                conditions.insert(condition_id);
            } else {
                return false;
            }
        }
        if removed.is_empty() {
            return false;
        }
        let removed_total: f64 = removed.iter().map(|(_, qty, _)| qty).sum();
        let expected_payout: f64 = removed.iter().map(|(_, qty, value)| qty * value).sum();
        let tolerance = 0.02_f64.max(removed_total.abs().max(cash_delta.abs()) * 0.001);
        if expected_payout <= EPS || cash_delta + tolerance < expected_payout {
            return false;
        }
        // Polymarket can settle a redeem a few cents below its proven binary
        // payout (for example, because the platform deducted a redeem fee).
        // The physical side already contains the authoritative wallet delta,
        // so cap virtual attribution at that amount and spread the shortfall
        // pro rata across every winning token/owner. Positive excess cash is
        // intentionally left unallocated, preserving the existing deposit
        // safety boundary.
        let attributed_payout = cash_delta.min(expected_payout);
        let payout_scale = attributed_payout / expected_payout;
        let residual_cash = cash_delta - expected_payout;
        for (token, qty, _) in &removed {
            let virtual_total: f64 = state
                .instances
                .values()
                .map(|instance| instance.positions.get(token).copied().unwrap_or(0.0))
                .sum();
            if virtual_total + tolerance < *qty {
                return false;
            }
        }

        state.physical_cash += cash_delta;
        let mut attributed = Vec::new();
        for (token, qty, value) in &removed {
            let physical = state.physical_positions.entry(token.clone()).or_insert(0.0);
            *physical = (*physical - *qty).max(0.0);
            let virtual_total: f64 = state
                .instances
                .values()
                .map(|instance| instance.positions.get(token).copied().unwrap_or(0.0))
                .sum();
            if virtual_total <= EPS {
                continue;
            }
            for (instance_id, instance) in &mut state.instances {
                let owned = instance.positions.get(token).copied().unwrap_or(0.0);
                if owned <= EPS {
                    continue;
                }
                let burned = (owned * *qty / virtual_total).min(owned);
                *instance.positions.entry(token.clone()).or_insert(0.0) -= burned;
                let instance_cash_delta = burned * *value * payout_scale;
                instance.cash += instance_cash_delta;
                attributed.push((
                    instance_id.clone(),
                    token.clone(),
                    burned,
                    instance_cash_delta,
                ));
            }
        }
        for (instance_id, token, burned, instance_cash_delta) in attributed {
            record_internal_external_adjustment(
                &mut state,
                "observed_platform_redeem",
                &instance_id,
                instance_cash_delta,
                HashMap::from([(token, -burned)]),
            );
        }
        recompute_reconciliation(&mut state, "platform automatic binary redeem");
        self.schedule_persist(&state);
        log::info!(
            "[shared_account] attributed platform automatic redeem account={} payout={:.6} attributed_payout={:.6} payout_scale={:.9} observed_cash_delta={:.6} residual_cash={:+.6} conditions={:?} removed={:?}",
            self.account_id,
            expected_payout,
            attributed_payout,
            payout_scale,
            cash_delta,
            residual_cash,
            conditions,
            removed,
        );
        true
    }
    pub fn is_uncertain(&self) -> bool {
        self.refresh_trade_persistence_blocker();
        self.persistence
            .as_ref()
            .and_then(AccountPersistence::last_error)
            .is_some()
            || self.uncertain_fast.load(Ordering::Acquire)
    }

    /// Lock-free diagnostic reason paired with the most recently published
    /// control transaction. Admission decisions use dedicated atomics; this
    /// view is for logging/monitoring and must never force quote callbacks to
    /// materialize the aggregate ledger.
    pub fn uncertain_reason_snapshot(&self) -> Option<Arc<String>> {
        self.uncertain_reason_fast.load_full()
    }

    /// True when every current admission failure is limited to delayed fee
    /// attribution/rebuild. Base trade quantities, ownership and physical vs
    /// virtual reconciliation are still valid in this state, so passive
    /// maker-only orders can remain available without admitting new taker fee
    /// exposure.
    pub fn is_fee_degraded_only(&self) -> bool {
        self.refresh_trade_persistence_blocker();
        if self
            .persistence
            .as_ref()
            .and_then(AccountPersistence::last_error)
            .is_some()
        {
            return false;
        }
        self.passive_admission_fast.load(Ordering::Acquire)
            && !self.admission_fast.load(Ordering::Acquire)
    }

    /// Quote-side readiness for maker-only paths. All ordinary uncertainty
    /// remains fail-closed; the sole exception is fee-only degradation.
    pub fn passive_order_admission_allowed(&self) -> bool {
        self.refresh_trade_persistence_blocker();
        if self
            .persistence
            .as_ref()
            .and_then(AccountPersistence::last_error)
            .is_some()
        {
            return false;
        }
        self.passive_admission_fast.load(Ordering::Acquire)
    }
    pub fn mark_uncertain(&self) {
        self.mark_uncertain_with_reason("unspecified account uncertainty");
    }

    pub fn mark_uncertain_with_reason(&self, reason: impl Into<String>) {
        self.set_risk_blocker(MANUAL_RISK_BLOCKER, reason);
    }

    /// Set a sticky, source-owned admission blocker. Reconciliation may update
    /// balances and its own derived blockers, but it cannot remove this key.
    pub fn set_risk_blocker(&self, source: &str, reason: impl Into<String>) {
        let source = source.trim();
        if source.is_empty() {
            return;
        }
        let reason = reason.into();
        let reason = if reason.trim().is_empty() {
            "unspecified subsystem risk".to_string()
        } else {
            reason
        };
        let mut state = self.lock_state();
        let since_ms = state
            .risk_blockers
            .get(source)
            .map_or_else(wall_clock_ms, |blocker| blocker.since_ms);
        state.risk_blockers.insert(
            source.to_string(),
            RiskBlocker {
                reason: reason.clone(),
                since_ms,
            },
        );
        self.risk_blocker_sources_fast
            .write()
            .unwrap()
            .insert(source.to_string());
        set_uncertain(&mut state, format!("{source}: {reason}"));
        self.schedule_persist(&state);
    }

    /// Clear exactly one subsystem blocker, then re-evaluate every remaining
    /// derived account invariant. Callers cannot accidentally reopen admission
    /// for a different source.
    pub fn clear_risk_blocker(&self, source: &str) -> bool {
        let source = source.trim();
        if source.is_empty()
            || !self
                .risk_blocker_sources_fast
                .read()
                .unwrap()
                .contains(source)
        {
            return false;
        }
        let mut state = self.lock_state();
        if state.risk_blockers.remove(source).is_none() {
            self.risk_blocker_sources_fast
                .write()
                .unwrap()
                .remove(source);
            return false;
        }
        self.risk_blocker_sources_fast
            .write()
            .unwrap()
            .remove(source);
        recompute_reconciliation(&mut state, "risk blocker cleared");
        self.schedule_persist(&state);
        true
    }

    /// Mark potentially-live orders restored from disk. Their durable order
    /// reservations continue to cover quoting while recovery runs; unrelated
    /// balance-changing maintenance remains fail-closed until authoritative
    /// terminal metadata arrives.
    pub fn begin_order_recovery<'a>(&self, client_order_ids: impl IntoIterator<Item = &'a str>) {
        let _control = self.control_gate.read().unwrap();
        let mut newly_pending = false;
        for client_order_id in client_order_ids.into_iter().filter(|id| !id.is_empty()) {
            let Some(account) = self.virtual_account_for_coid(client_order_id) else {
                continue;
            };
            let mut lifecycle = account.lifecycle.lock().unwrap();
            if !lifecycle.orders.contains_key(client_order_id) {
                continue;
            }
            if lifecycle
                .recovery_pending_orders
                .insert(client_order_id.to_string())
            {
                newly_pending = true;
                self.schedule_virtual_lifecycle_persist(&account, &lifecycle, client_order_id);
            }
        }
        if newly_pending {
            self.notify_order_audit_worker();
        }
    }

    pub fn finish_order_recovery(&self, client_order_id: &str) {
        let _control = self.control_gate.read().unwrap();
        let Some(account) = self.virtual_account_for_coid(client_order_id) else {
            return;
        };
        let mut lifecycle = account.lifecycle.lock().unwrap();
        let recovery_removed = lifecycle.recovery_pending_orders.remove(client_order_id);
        let query_repair_removed = lifecycle
            .startup_query_repair_orders
            .remove(client_order_id);
        if recovery_removed || query_repair_removed {
            self.schedule_virtual_lifecycle_persist(&account, &lifecycle, client_order_id);
        }
    }

    pub fn recovery_pending_order_ids(&self) -> Vec<String> {
        let _control = self.control_gate.read().unwrap();
        let accounts = self.virtual_accounts.read().unwrap();
        let mut pending: Vec<String> = accounts
            .values()
            .flat_map(|account| {
                account
                    .lifecycle
                    .lock()
                    .unwrap()
                    .recovery_pending_orders
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect();
        pending.sort();
        pending
    }

    /// All orders still participating in order/trade recovery. Entries that
    /// lack authoritative terminal metadata block balance-changing maintenance
    /// for their owner instance; quote admission continues under retained
    /// reservations while the worker retries metadata and exact private trades.
    pub fn pending_order_audit_ids(&self) -> Vec<String> {
        let _control = self.control_gate.read().unwrap();
        let accounts = self.virtual_accounts.read().unwrap();
        let mut pending = HashSet::new();
        for account in accounts.values() {
            let lifecycle = account.lifecycle.lock().unwrap();
            pending.extend(lifecycle.recovery_pending_orders.iter().cloned());
            pending.extend(lifecycle.routine_cancel_audits.iter().cloned());
        }
        let mut pending: Vec<String> = pending.into_iter().collect();
        pending.sort();
        pending
    }

    /// Diagnostic for terminal orders still missing a complete authoritative
    /// order audit, scoped to their owner instance. Quote admission does not
    /// use this signal because the original reservation already represents
    /// worst-case exposure; balance-changing maintenance remains blocked.
    pub fn order_audit_instance_blocker(&self, instance_id: &str) -> Option<Vec<String>> {
        let _control = self.control_gate.read().unwrap();
        let account = self.virtual_account(instance_id)?;
        let lifecycle = account.lifecycle.lock().unwrap();
        let mut pending: Vec<String> = lifecycle
            .recovery_pending_orders
            .iter()
            .filter(|coid| {
                lifecycle
                    .orders
                    .get(*coid)
                    .is_some_and(|order| !order.terminal_trade_ids_authoritative)
            })
            .cloned()
            .collect();
        if pending.is_empty() {
            None
        } else {
            pending.sort();
            Some(pending)
        }
    }

    /// Attribute an externally-confirmed wallet operation atomically to both
    /// the physical account ledger and one instance's virtual ledger.
    pub fn attribute_external_adjustment(
        &self,
        operation_id: &str,
        instance_id: &str,
        cash_delta: f64,
        position_deltas: HashMap<String, f64>,
    ) -> Result<ExternalAdjustment, ReservationError> {
        if operation_id.is_empty()
            || !cash_delta.is_finite()
            || position_deltas.values().any(|delta| !delta.is_finite())
        {
            return Err(ReservationError::InvalidOrder(
                "external adjustment requires an operation_id and finite deltas".into(),
            ));
        }
        let mut state = self.lock_state();
        if let Some(existing) = state.external_adjustments.get(operation_id) {
            if existing.instance_id == instance_id
                && (existing.cash_delta - cash_delta).abs() <= EPS
                && existing.position_deltas == position_deltas
            {
                return Ok(existing.clone());
            }
            return Err(ReservationError::InvalidOrder(format!(
                "external operation_id `{operation_id}` was already used with a different attribution"
            )));
        }
        let Some(instance_before) = state.instances.get(instance_id) else {
            return Err(ReservationError::UnknownInstance(instance_id.into()));
        };
        if instance_before.cash + cash_delta < -EPS || state.physical_cash + cash_delta < -EPS {
            return Err(ReservationError::InvalidOrder(format!(
                "external adjustment would make account or instance `{instance_id}` cash negative"
            )));
        }
        for (token, delta) in &position_deltas {
            let virtual_current = instance_before.positions.get(token).copied().unwrap_or(0.0);
            let physical_current = state.physical_positions.get(token).copied().unwrap_or(0.0);
            if virtual_current + delta < -EPS || physical_current + delta < -EPS {
                return Err(ReservationError::InvalidOrder(format!(
                    "external adjustment would make account or instance `{instance_id}` token `{token}` negative"
                )));
            }
        }
        state.physical_cash = (state.physical_cash + cash_delta).max(0.0);
        for (token, delta) in &position_deltas {
            let entry = state.physical_positions.entry(token.clone()).or_insert(0.0);
            *entry = (*entry + *delta).max(0.0);
        }
        state.physical_positions.retain(|_, qty| *qty > EPS);
        let instance = state.instances.get_mut(instance_id).unwrap();
        instance.cash = (instance.cash + cash_delta).max(0.0);
        for (token, delta) in &position_deltas {
            let entry = instance.positions.entry(token.clone()).or_insert(0.0);
            *entry = (*entry + *delta).max(0.0);
        }
        let adjustment = ExternalAdjustment {
            operation_id: operation_id.to_string(),
            instance_id: instance_id.to_string(),
            cash_delta,
            position_deltas,
            recorded_at_ms: wall_clock_ms(),
        };
        state
            .external_adjustments
            .insert(operation_id.to_string(), adjustment.clone());
        recompute_reconciliation(
            &mut state,
            &format!("external operation `{operation_id}` awaiting physical reconciliation"),
        );
        self.schedule_persist(&state);
        Ok(adjustment)
    }

    pub fn record_gap_replay_pages(&self, pages: usize) {
        let mut state = self.lock_state();
        let pages = pages as u64;
        state.gap_replay_last_pages = pages;
        state.gap_replay_max_pages = state.gap_replay_max_pages.max(pages);
        state.gap_replay_total_pages = state.gap_replay_total_pages.saturating_add(pages);
    }

    pub fn record_maintenance_queue_wait(&self, wait: Duration) {
        let mut state = self.lock_state();
        let wait_ms = wait.as_millis().min(u64::MAX as u128) as u64;
        state.maintenance_queue_last_wait_ms = wait_ms;
        state.maintenance_queue_max_wait_ms = state.maintenance_queue_max_wait_ms.max(wait_ms);
        state.maintenance_queue_jobs = state.maintenance_queue_jobs.saturating_add(1);
    }

    pub fn monitoring_snapshot(&self) -> AccountMonitoringSnapshot {
        self.refresh_trade_persistence_blocker();
        let state = self.lock_state();
        let persistence_error = self
            .persistence
            .as_ref()
            .and_then(AccountPersistence::last_error);
        let persistence_metrics = self
            .persistence
            .as_ref()
            .map(AccountPersistence::metrics)
            .unwrap_or_default();
        let mut virtual_positions = HashMap::<String, f64>::new();
        let mut reserved_positions = HashMap::<String, f64>::new();
        let mut instances = Vec::with_capacity(state.instances.len());
        for (instance_id, instance) in &state.instances {
            for (token, qty) in &instance.positions {
                *virtual_positions.entry(token.clone()).or_insert(0.0) += *qty;
            }
            for (token, qty) in &instance.reserved_positions {
                *reserved_positions.entry(token.clone()).or_insert(0.0) += *qty;
            }
            for (token, qty) in &instance.maintenance_reserved_positions {
                *reserved_positions.entry(token.clone()).or_insert(0.0) += *qty;
            }
            instances.push(InstanceAccountSnapshot {
                instance_id: instance_id.clone(),
                weight: instance.weight,
                ledger_generation: state.ledger_generation,
                cash: instance.cash,
                positions: instance.positions.clone(),
                reserved_cash: instance.total_reserved_cash(),
                reserved_positions: instance.total_reserved_positions(),
            });
        }
        AccountMonitoringSnapshot {
            account_id: self.account_id.clone(),
            seeded: state.seeded,
            physical_cash: state.physical_cash,
            virtual_cash: state.instances.values().map(|instance| instance.cash).sum(),
            unallocated_cash: state.unallocated_cash,
            physical_positions: state.physical_positions.clone(),
            virtual_positions,
            unallocated_positions: state.unallocated_positions.clone(),
            provisional_position_owners: state.provisional_position_owners.clone(),
            reserved_cash: state
                .instances
                .values()
                .map(InstanceLedger::total_reserved_cash)
                .sum(),
            reserved_positions,
            uncertain: state.uncertain || persistence_error.is_some(),
            uncertain_reason: persistence_error
                .as_ref()
                .map(|error| format!("account ledger persistence error: {error}"))
                .or_else(|| state.uncertain_reason.clone()),
            uncertain_since_ms: state.uncertain_since_ms,
            instances,
            gap_replay_last_pages: state.gap_replay_last_pages,
            gap_replay_max_pages: state.gap_replay_max_pages,
            gap_replay_total_pages: state.gap_replay_total_pages,
            maintenance_queue_last_wait_ms: state.maintenance_queue_last_wait_ms,
            maintenance_queue_max_wait_ms: state.maintenance_queue_max_wait_ms,
            maintenance_queue_jobs: state.maintenance_queue_jobs,
            pending_maintenance_operations: state
                .maintenance_ops
                .values()
                .filter(|operation| {
                    matches!(
                        operation.status,
                        MaintenanceOperationStatus::Reserved
                            | MaintenanceOperationStatus::Submitted
                            | MaintenanceOperationStatus::Uncertain
                    )
                })
                .count(),
            recovery_pending_orders: state.recovery_pending_orders.len(),
            routine_cancel_audits: state.routine_cancel_audits.len(),
            retired_trade_ownership_tombstones: self
                .retired_trade_tombstone_count_fast
                .load(Ordering::Relaxed),
            verified_trade_replay_recoveries: state.verified_trade_replay_recoveries,
            persistence_path: self.persistence.as_ref().map(|p| p.path.clone()),
            persistence_error,
            persistence_writes: persistence_metrics.0,
            persistence_write_last_us: persistence_metrics.1,
            persistence_write_max_us: persistence_metrics.2,
            persistence_flushes: persistence_metrics.3,
            persistence_flush_last_us: persistence_metrics.4,
            persistence_flush_max_us: persistence_metrics.5,
            account_lock_wait_last_us: self.account_lock_wait_last_us.load(Ordering::Relaxed),
            account_lock_wait_max_us: self.account_lock_wait_max_us.load(Ordering::Relaxed),
            account_lock_hold_last_us: self.account_lock_hold_last_us.load(Ordering::Relaxed),
            account_lock_hold_max_us: self.account_lock_hold_max_us.load(Ordering::Relaxed),
            account_lock_acquisitions: self.account_lock_acquisitions.load(Ordering::Relaxed),
            reservation_control_lock: self.reservation_control_lock.snapshot(),
            reservation_coid_route_lock: self.reservation_coid_route_lock.snapshot(),
            reservation_oid_route_lock: self.reservation_oid_route_lock.snapshot(),
            reservation_lifecycle_lock: self.reservation_lifecycle_lock.snapshot(),
        }
    }

    pub fn orders(&self) -> Vec<OrderOwnership> {
        let accounts = self.virtual_accounts.read().unwrap();
        accounts
            .values()
            .flat_map(|account| {
                account
                    .lifecycle
                    .lock()
                    .unwrap()
                    .orders
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub fn instance_snapshot(&self, instance_id: &str) -> Option<InstanceAccountSnapshot> {
        let account = self.virtual_account(instance_id)?;
        let instance = account.ledger_snapshot();
        Some(InstanceAccountSnapshot {
            instance_id: instance_id.to_string(),
            weight: instance.weight,
            ledger_generation: self.ledger_generation_fast.load(Ordering::Acquire),
            cash: instance.cash,
            positions: instance.positions,
            reserved_cash: instance.reserved_cash + instance.maintenance_reserved_cash,
            reserved_positions: {
                let mut total = instance.reserved_positions;
                for (token, quantity) in instance.maintenance_reserved_positions {
                    *total.entry(token).or_insert(0.0) += quantity;
                }
                total
            },
        })
    }

    pub fn availability(&self, instance_id: &str, token: &str) -> Option<AccountAvailability> {
        self.availability_with_fee_policy(instance_id, token, false)
    }

    /// Availability for a route that will emit maker-only orders. Fee-only
    /// degradation does not zero the otherwise authoritative base inventory;
    /// every other uncertainty source keeps the normal fail-closed result.
    pub fn passive_quote_availability(
        &self,
        instance_id: &str,
        token: &str,
    ) -> Option<AccountAvailability> {
        self.availability_with_fee_policy(instance_id, token, true)
    }

    fn availability_with_fee_policy(
        &self,
        instance_id: &str,
        token: &str,
        allow_fee_degraded: bool,
    ) -> Option<AccountAvailability> {
        self.refresh_trade_persistence_blocker();
        let persistence_failed = self
            .persistence
            .as_ref()
            .and_then(AccountPersistence::last_error)
            .is_some();
        let account = self.virtual_account(instance_id)?;
        // A negative unexplained reconciliation delta means the physical
        // wallet is below the sum of the virtual ledgers. Fail closed until
        // reconciliation instead of letting one instance consume another's
        // allocation.
        // Order/trade audit recovery is not an availability gate: the durable
        // order reservation already excludes its worst-case cash or shares.
        let admission_allowed = if allow_fee_degraded {
            self.passive_admission_fast.load(Ordering::Acquire)
        } else {
            self.admission_fast.load(Ordering::Acquire)
        };
        if persistence_failed || !admission_allowed {
            return Some(AccountAvailability {
                virtual_cash: 0.0,
                physical_cash: 0.0,
                effective_cash: 0.0,
                virtual_position: 0.0,
                physical_position: 0.0,
                effective_position: 0.0,
            });
        }
        // The wallet's physical ceiling was already split into immutable
        // instance quotas at seed/migration time. Ordinary quotes therefore
        // need only this instance's atomic counters; no cross-instance sum or
        // account-wide lock participates in availability.
        let virtual_cash = (account.cash.load()
            - account.reserved_cash.load()
            - account.maintenance_reserved_cash.load())
        .max(0.0);
        let position = account.position(token);
        let maintenance_reserved = account
            .maintenance_reserved_positions
            .read()
            .unwrap()
            .get(token)
            .copied()
            .unwrap_or(0.0);
        let virtual_position =
            (position.balance.load() - position.reserved.load() - maintenance_reserved).max(0.0);
        Some(AccountAvailability {
            virtual_cash,
            physical_cash: virtual_cash,
            effective_cash: virtual_cash,
            virtual_position,
            physical_position: virtual_position,
            effective_position: virtual_position,
        })
    }

    /// Record order ownership without performing account admission.
    ///
    /// The strategy instance owns its balance/inventory admission and pending
    /// reservations. The execution layer retains this durable mirror only so
    /// authenticated order/trade messages can be routed and replayed after a
    /// restart. Nominal reservations are recorded for recovery, but they are
    /// never compared with shared cash or inventory here.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_order_ownership(
        &self,
        instance_id: &str,
        client_order_id: &str,
        order_id: &str,
        token_id: &str,
        side: Side,
        quantity: f64,
        price: f64,
        fee_rate_bps: u32,
    ) -> Result<OrderOwnership, ReservationError> {
        if instance_id.is_empty()
            || client_order_id.is_empty()
            || order_id.is_empty()
            || token_id.is_empty()
            || !quantity.is_finite()
            || quantity <= 0.0
            || !price.is_finite()
            || price <= 0.0
        {
            return Err(ReservationError::InvalidOrder(
                "instance/coid/oid/token must be present and quantity/price must be positive"
                    .into(),
            ));
        }
        let reserved_cash = if side == Side::Buy {
            quantity * price * (1.0 + fee_rate_bps as f64 / 10_000.0)
        } else {
            0.0
        };
        if !reserved_cash.is_finite() {
            return Err(ReservationError::InvalidOrder(
                "order reservation is not finite".into(),
            ));
        }
        let reserved_quantity = if side == Side::Sell { quantity } else { 0.0 };
        let ownership = OrderOwnership {
            account_id: self.account_id.clone(),
            instance_id: instance_id.to_string(),
            client_order_id: client_order_id.to_string(),
            order_id: order_id.to_string(),
            token_id: token_id.to_string(),
            side,
            quantity,
            filled_quantity: 0.0,
            terminal_matched_quantity: None,
            terminal_trade_ids: Vec::new(),
            terminal_trade_ids_authoritative: false,
            price,
            fee_rate_bps,
            reserved_cash,
            reserved_quantity,
            status: OrderStatus::Pending,
        };
        Ok(ownership)
    }

    /// Compatibility entry point for cold/legacy callers. Live execution uses
    /// [`Self::prepare_order_ownership`] and publishes the returned typed row
    /// to its single-writer persistence actor after installing local routing.
    #[allow(clippy::too_many_arguments)]
    pub fn record_order_without_admission(
        &self,
        instance_id: &str,
        client_order_id: &str,
        order_id: &str,
        token_id: &str,
        side: Side,
        quantity: f64,
        price: f64,
        fee_rate_bps: u32,
    ) -> Result<OrderOwnership, ReservationError> {
        let ownership = self.prepare_order_ownership(
            instance_id,
            client_order_id,
            order_id,
            token_id,
            side,
            quantity,
            price,
            fee_rate_bps,
        )?;
        self.backfill_order_ownership(&ownership)
            .ok_or_else(|| ReservationError::DuplicateClientOrderId(client_order_id.to_string()))
    }

    /// Reserve an order and bind both locally-known identifiers before the
    /// network POST. This legacy account-wide admission API remains available
    /// to callers that explicitly need shared-wallet arbitration.
    pub fn reserve_order(
        &self,
        instance_id: &str,
        client_order_id: &str,
        order_id: &str,
        token_id: &str,
        side: Side,
        quantity: f64,
        price: f64,
        fee_rate_bps: u32,
    ) -> Result<OrderOwnership, ReservationError> {
        self.reserve_order_with_fee_policy(
            instance_id,
            client_order_id,
            order_id,
            token_id,
            side,
            quantity,
            price,
            fee_rate_bps,
            false,
        )
    }

    /// Reserve an exchange-enforced maker-only order while fee attribution is
    /// the account's sole degraded input. Callers must prove the request is
    /// post-only before using this entry point; taker-capable paths must use
    /// [`Self::reserve_order`].
    pub fn reserve_passive_order(
        &self,
        instance_id: &str,
        client_order_id: &str,
        order_id: &str,
        token_id: &str,
        side: Side,
        quantity: f64,
        price: f64,
        fee_rate_bps: u32,
    ) -> Result<OrderOwnership, ReservationError> {
        self.reserve_order_with_fee_policy(
            instance_id,
            client_order_id,
            order_id,
            token_id,
            side,
            quantity,
            price,
            fee_rate_bps,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn reserve_order_with_fee_policy(
        &self,
        instance_id: &str,
        client_order_id: &str,
        order_id: &str,
        token_id: &str,
        side: Side,
        quantity: f64,
        price: f64,
        fee_rate_bps: u32,
        allow_fee_degraded: bool,
    ) -> Result<OrderOwnership, ReservationError> {
        self.refresh_trade_persistence_blocker();
        if client_order_id.is_empty()
            || order_id.is_empty()
            || token_id.is_empty()
            || !quantity.is_finite()
            || quantity <= 0.0
            || !price.is_finite()
            || price <= 0.0
        {
            return Err(ReservationError::InvalidOrder(
                "coid/oid/token must be present and quantity/price must be positive".into(),
            ));
        }
        if !self.seeded_fast.load(Ordering::Acquire) {
            return Err(ReservationError::AccountNotSeeded);
        }
        let admission_allowed = if allow_fee_degraded {
            self.passive_admission_fast.load(Ordering::Acquire)
        } else {
            self.admission_fast.load(Ordering::Acquire)
        };
        if !admission_allowed {
            return Err(ReservationError::AccountUncertain);
        }
        let account = self
            .virtual_account(instance_id)
            .ok_or_else(|| ReservationError::UnknownInstance(instance_id.into()))?;
        // Reservation is entirely instance-scoped. The narrow publication
        // mutex only coordinates this shard with the final snapshot-copy
        // phase; it never waits for an account control transaction.
        let _publication = account.reservation_publish.lock().unwrap();
        let normalized_order_id = normalize_order_id(order_id);
        let lifecycle_wait = Instant::now();
        let mut lifecycle = account.lifecycle.lock().unwrap();
        let lifecycle_metric = self.reservation_lifecycle_lock.acquired(lifecycle_wait);
        let coid_wait = Instant::now();
        let mut coid_routes = self.coid_routes.write_shard(client_order_id);
        let coid_metric = self.reservation_coid_route_lock.acquired(coid_wait);
        let oid_wait = Instant::now();
        let mut oid_routes = self.oid_routes.write_shard(&normalized_order_id);
        let oid_metric = self.reservation_oid_route_lock.acquired(oid_wait);
        if let Some(existing) = lifecycle.orders.get(client_order_id) {
            if normalize_order_id(&existing.order_id) == normalize_order_id(order_id)
                && existing.instance_id == instance_id
            {
                // The original reservation and WAL delta already own this
                // exact order. Retrying must be a true no-op: re-enqueueing an
                // identical delta used to wake the writer and recreate the
                // account-wide clone contention this fast path removes.
                return Ok(existing.clone());
            }
            return Err(ReservationError::DuplicateClientOrderId(
                client_order_id.into(),
            ));
        }
        if coid_routes.contains_key(client_order_id) {
            return Err(ReservationError::DuplicateClientOrderId(
                client_order_id.into(),
            ));
        }
        if oid_routes.contains_key(&normalized_order_id) {
            return Err(ReservationError::InvalidOrder(format!(
                "order_id `{order_id}` is already owned by another order",
            )));
        }

        // Fee reserve is deliberately conservative: actual Polymarket fees
        // are price-shaped and no larger than this simple notional ceiling.
        let reserve_cash = if side == Side::Buy {
            quantity * price * (1.0 + fee_rate_bps as f64 / 10_000.0)
        } else {
            0.0
        };
        let reserve_qty = if side == Side::Sell { quantity } else { 0.0 };

        if reserve_cash > 0.0 {
            account
                .reserved_cash
                .try_reserve(&account.cash, reserve_cash)
                .map_err(|available| ReservationError::InsufficientVirtualCash {
                    required: reserve_cash,
                    available,
                })?;
        }
        if reserve_qty > 0.0 {
            let position = account.position(token_id);
            if let Err(available) = position
                .reserved
                .try_reserve(&position.balance, reserve_qty)
            {
                if reserve_cash > 0.0 {
                    account.reserved_cash.add(-reserve_cash);
                }
                return Err(ReservationError::InsufficientVirtualPosition {
                    token: token_id.into(),
                    required: reserve_qty,
                    available,
                });
            }
        }
        let ownership = OrderOwnership {
            account_id: self.account_id.clone(),
            instance_id: instance_id.into(),
            client_order_id: client_order_id.into(),
            order_id: order_id.into(),
            token_id: token_id.into(),
            side,
            quantity,
            filled_quantity: 0.0,
            terminal_matched_quantity: None,
            terminal_trade_ids: Vec::new(),
            terminal_trade_ids_authoritative: false,
            price,
            fee_rate_bps,
            reserved_cash: reserve_cash,
            reserved_quantity: reserve_qty,
            status: OrderStatus::Pending,
        };
        lifecycle
            .orders
            .insert(client_order_id.into(), ownership.clone());
        coid_routes.insert(client_order_id.into(), instance_id.into());
        oid_routes.insert(normalized_order_id, instance_id.into());
        account.reservation_epoch.fetch_add(1, Ordering::Release);
        let persisted_reserved_cash = account.reserved_cash.load();
        let persisted_reserved_position = account
            .positions
            .read()
            .unwrap()
            .get(token_id)
            .map(|position| position.reserved.load())
            .unwrap_or(0.0);

        // Queue the raw command while the instance lifecycle is still held so
        // two reservations for the same virtual account cannot publish their
        // absolute counter snapshots out of order. The queued payload contains
        // no JSON paths or serialized values; the writer constructs those.
        if let Some(persistence) = &self.persistence {
            persistence.schedule_reservation(ReservationPersistenceDelta {
                instance_id: instance_id.to_string(),
                client_order_id: client_order_id.to_string(),
                order: ownership.clone(),
                reserved_cash: persisted_reserved_cash,
                reserved_position: persisted_reserved_position,
            });
        }

        // End every admission lock before allocating WAL paths or invoking
        // serde. The writer materializes this raw command into typed JSON.
        drop(oid_metric);
        drop(oid_routes);
        drop(coid_metric);
        drop(coid_routes);
        drop(lifecycle_metric);
        drop(lifecycle);
        // Deliberately asynchronous: startup exchange reconciliation is the
        // recovery authority for the crash window between POST acceptance and
        // this WAL generation reaching disk. In-process reservations remain
        // atomic across strategies, while order submission never waits for
        // typed-delta construction, serialization, append, or fsync.
        Ok(ownership)
    }

    pub fn rebind_order_id(&self, client_order_id: &str, order_id: &str) -> bool {
        if client_order_id.is_empty() || order_id.is_empty() {
            return false;
        }
        let control = self.control_gate.read().unwrap();
        if let Some(account) = self.virtual_account_for_coid(client_order_id) {
            let mut lifecycle = account.lifecycle.lock().unwrap();
            let Some(old_order_id) = lifecycle
                .orders
                .get(client_order_id)
                .map(|order| order.order_id.clone())
            else {
                drop(lifecycle);
                drop(control);
                return self.record_order_binding_anomaly(
                    client_order_id,
                    format!("cannot bind oid `{order_id}` to unknown coid `{client_order_id}`"),
                );
            };
            let normalized = normalize_order_id(order_id);
            let normalized_old = normalize_order_id(&old_order_id);
            if normalized_old == normalized {
                // Local signing already bound the exact exchange hash at
                // reservation time. The placement ACK commonly repeats it;
                // this is a strict no-op: no recompute and no persistence.
                return true;
            }
            let conflicts = lifecycle.orders.iter().any(|(coid, order)| {
                coid != client_order_id && normalize_order_id(&order.order_id) == normalized
            });
            let mut new_oid_routes = self.oid_routes.write_shard(&normalized);
            let routed_elsewhere = new_oid_routes
                .get(&normalized)
                .is_some_and(|owner| owner != &account.instance_id);
            if conflicts || routed_elsewhere {
                drop(new_oid_routes);
                drop(lifecycle);
                drop(control);
                return self.record_order_binding_anomaly(
                    client_order_id,
                    format!(
                        "oid `{order_id}` ownership conflict while rebinding coid `{client_order_id}`"
                    ),
                );
            }
            lifecycle
                .orders
                .get_mut(client_order_id)
                .expect("order checked above")
                .order_id = order_id.into();
            new_oid_routes.insert(normalized, account.instance_id.clone());
            drop(new_oid_routes);
            self.oid_routes.remove(&normalized_old);
            self.schedule_virtual_rebind_persist(
                &account,
                &lifecycle,
                client_order_id,
                &old_order_id,
            );
            return true;
        }
        drop(control);
        let mut state = self.lock_state();
        let Some(old_order_id) = state
            .orders
            .get(client_order_id)
            .map(|order| order.order_id.clone())
        else {
            set_ownership_anomaly(
                &mut state,
                format!("order_binding:{client_order_id}"),
                format!("cannot bind oid `{order_id}` to unknown coid `{client_order_id}`"),
            );
            self.schedule_persist(&state);
            return false;
        };
        let normalized = normalize_order_id(order_id);
        if let Some(other) = state.oid_to_coid.get(&normalized).cloned() {
            if other != client_order_id {
                set_ownership_anomaly(
                    &mut state,
                    format!("order_binding:{client_order_id}"),
                    format!(
                        "oid `{order_id}` ownership conflict: coid `{client_order_id}` vs `{other}`"
                    ),
                );
                self.schedule_persist(&state);
                return false;
            }
        }
        let normalized_old = normalize_order_id(&old_order_id);
        if let Some(other) = state.oid_to_coid.get(&normalized_old).cloned() {
            if other != client_order_id {
                set_ownership_anomaly(
                    &mut state,
                    format!("order_binding:{client_order_id}"),
                    format!(
                        "stored oid `{old_order_id}` ownership conflict: coid `{client_order_id}` vs `{other}`"
                    ),
                );
                self.schedule_persist(&state);
                return false;
            }
        }
        state.oid_to_coid.remove(&normalized_old);
        if let Some(order) = state.orders.get_mut(client_order_id) {
            order.order_id = order_id.into();
        }
        state.oid_to_coid.insert(normalized, client_order_id.into());
        state
            .ownership_anomalies
            .remove(&format!("order_binding:{client_order_id}"));
        recompute_reconciliation(&mut state, "corrected order ownership mapping");
        self.schedule_persist(&state);
        true
    }

    fn record_order_binding_anomaly(&self, client_order_id: &str, reason: String) -> bool {
        let mut state = self.lock_state();
        set_ownership_anomaly(
            &mut state,
            format!("order_binding:{client_order_id}"),
            reason,
        );
        self.schedule_persist(&state);
        false
    }

    /// Explicit operator repair hook for an ownership anomaly after external
    /// audit. Runtime code should prefer a correct order/trade replay, which
    /// clears its own anomaly automatically.
    pub fn ownership_anomalies(&self) -> BTreeMap<String, String> {
        self.lock_state().ownership_anomalies.clone()
    }

    pub fn mark_private_event_anomaly(&self, payload_key: &str, reason: impl Into<String>) {
        if payload_key.is_empty() {
            return;
        }
        let _transition = self.private_anomaly_transition.lock().unwrap();
        if self
            .anomalous_private_event_keys
            .read()
            .unwrap()
            .contains(payload_key)
        {
            return;
        }
        let mut state = self.lock_state();
        set_ownership_anomaly(
            &mut state,
            format!("private_event:{payload_key}"),
            reason.into(),
        );
        self.schedule_persist(&state);
    }

    /// Persist an order-specific recovery hint together with the ordinary
    /// private-event anomaly.  The hint is not an active route and is never
    /// consulted by live attribution; it exists solely so a later process can
    /// audit the oid even when the lifecycle row and runtime maps are absent.
    pub fn mark_private_order_event_anomaly(
        &self,
        order_id: &str,
        client_order_id: Option<&str>,
        reason: impl Into<String>,
    ) {
        self.mark_private_order_event_anomaly_with_token(order_id, client_order_id, None, reason);
    }

    /// Structured variant used by the private feed. Keeping the authenticated
    /// asset id makes event settlement an order-independent startup-repair
    /// authority if the CLOB later compacts the oid lookup row.
    pub fn mark_private_order_event_anomaly_with_token(
        &self,
        order_id: &str,
        client_order_id: Option<&str>,
        token_id: Option<&str>,
        reason: impl Into<String>,
    ) {
        let normalized = normalize_order_id(order_id);
        if normalized.is_empty() {
            return;
        }
        let payload_key = format!("order:{normalized}");
        let _transition = self.private_anomaly_transition.lock().unwrap();
        let mut state = self.lock_state();
        // A prior authenticated zero-fill terminal audit is stronger than a
        // delayed lifecycle replay.  Never recreate the historical blocker.
        if state
            .retired_order_audit_tombstones
            .contains_key(&normalized)
        {
            return;
        }
        state.orphan_order_anomaly_hints.insert(
            normalized.clone(),
            OrphanOrderAnomalyHint {
                order_id: order_id.trim().to_string(),
                client_order_id: client_order_id
                    .map(str::trim)
                    .filter(|coid| !coid.is_empty())
                    .map(str::to_string),
                token_id: token_id
                    .map(str::trim)
                    .filter(|token| !token.is_empty())
                    .map(str::to_string),
            },
        );
        set_ownership_anomaly(
            &mut state,
            format!("private_event:{payload_key}"),
            reason.into(),
        );
        self.schedule_persist(&state);
    }

    /// Enumerate persisted order anomalies independently of active order rows.
    /// Legacy ledgers embedded the coid only in the reason text; preserve a
    /// narrow parser for that exact format so they can be repaired once and
    /// rewritten with structured provenance.
    pub fn persisted_orphan_order_anomalies(&self) -> Vec<PersistedOrphanOrderAnomaly> {
        let state = self.lock_state();
        let mut anomalies = Vec::new();
        for (anomaly_key, reason) in &state.ownership_anomalies {
            let Some(order_id) = anomaly_key.strip_prefix("private_event:order:") else {
                continue;
            };
            let normalized = normalize_order_id(order_id);
            if normalized.is_empty()
                || state
                    .retired_order_audit_tombstones
                    .contains_key(&normalized)
            {
                continue;
            }
            let hint = state.orphan_order_anomaly_hints.get(&normalized);
            let client_order_id = hint
                .and_then(|hint| hint.client_order_id.clone())
                .or_else(|| legacy_orphan_order_coid(reason));
            anomalies.push(PersistedOrphanOrderAnomaly {
                anomaly_key: anomaly_key.clone(),
                order_id: hint
                    .map(|hint| hint.order_id.clone())
                    .unwrap_or_else(|| order_id.trim().to_string()),
                client_order_id,
                token_id: hint.and_then(|hint| hint.token_id.clone()),
            });
        }
        anomalies.sort_by(|left, right| {
            normalize_order_id(&left.order_id).cmp(&normalize_order_id(&right.order_id))
        });
        anomalies
    }

    /// Retain an authenticated, economic-free terminal proof and clear only
    /// its exact private-order anomaly.  Orders with any matched quantity or
    /// trade id are intentionally rejected: those require ownership rebuild
    /// plus exact trade replay and must remain fail-closed if that cannot be
    /// completed.
    pub fn record_terminal_orphan_order_audit(
        &self,
        order_id: &str,
        client_order_id: Option<&str>,
        status: OrderStatus,
        original_size: f64,
        size_matched: f64,
        associate_trades: &[String],
        evidence: &str,
    ) -> bool {
        let normalized = normalize_order_id(order_id);
        if normalized.is_empty()
            || !matches!(status, OrderStatus::Cancelled | OrderStatus::Rejected)
            || !original_size.is_finite()
            || original_size <= 0.0
            || !size_matched.is_finite()
            || size_matched.abs() > 1e-9
            || !associate_trades.is_empty()
            || evidence.trim().is_empty()
        {
            return false;
        }
        let _transition = self.private_anomaly_transition.lock().unwrap();
        let mut state = self.lock_state();
        if state
            .orders
            .values()
            .any(|order| normalize_order_id(&order.order_id) == normalized)
        {
            return false;
        }
        state.retired_order_audit_tombstones.insert(
            normalized.clone(),
            RetiredOrderAuditTombstone {
                order_id: normalized.clone(),
                client_order_id: client_order_id
                    .map(str::trim)
                    .filter(|coid| !coid.is_empty())
                    .map(str::to_string),
                status,
                original_size,
                covers_any_zero_fill_size: false,
                size_matched,
                associate_trades: Vec::new(),
                evidence: evidence.to_string(),
                audited_at_ms: wall_clock_ms(),
            },
        );
        state.ownership_anomalies.retain(|key, _| {
            key.strip_prefix("private_event:order:")
                .is_none_or(|order_id| normalize_order_id(order_id) != normalized)
        });
        state.orphan_order_anomaly_hints.remove(&normalized);
        recompute_reconciliation(&mut state, "authenticated terminal orphan order audit");
        self.schedule_persist(&state);
        true
    }

    /// Retire a compacted historical oid after callers have independently
    /// proved both sides of the absence: the authenticated order lookup is
    /// not-found and a complete account trade/event-settlement audit contains
    /// no fill for this oid. Unlike the ordinary tombstone, original quantity
    /// is intentionally unknown; the replay guard therefore covers only
    /// zero-fill lifecycle rows.
    pub fn record_authoritative_absent_orphan_order_audit(
        &self,
        order_id: &str,
        client_order_id: Option<&str>,
        evidence: &str,
    ) -> bool {
        let normalized = normalize_order_id(order_id);
        if normalized.is_empty() || evidence.trim().is_empty() {
            return false;
        }
        let _transition = self.private_anomaly_transition.lock().unwrap();
        let mut state = self.lock_state();
        if state
            .orders
            .values()
            .any(|order| normalize_order_id(&order.order_id) == normalized)
        {
            return false;
        }
        state.retired_order_audit_tombstones.insert(
            normalized.clone(),
            RetiredOrderAuditTombstone {
                order_id: normalized.clone(),
                client_order_id: client_order_id
                    .map(str::trim)
                    .filter(|coid| !coid.is_empty())
                    .map(str::to_string),
                status: OrderStatus::Cancelled,
                original_size: 0.0,
                covers_any_zero_fill_size: true,
                size_matched: 0.0,
                associate_trades: Vec::new(),
                evidence: evidence.to_string(),
                audited_at_ms: wall_clock_ms(),
            },
        );
        state.ownership_anomalies.retain(|key, _| {
            key.strip_prefix("private_event:order:")
                .is_none_or(|order_id| normalize_order_id(order_id) != normalized)
        });
        state.orphan_order_anomaly_hints.remove(&normalized);
        recompute_reconciliation(
            &mut state,
            "authenticated absent historical orphan order audit",
        );
        self.schedule_persist(&state);
        true
    }

    /// True only when a late private lifecycle row is covered by a retained
    /// zero-fill terminal audit.  This is the replay guard that keeps a
    /// repaired historical oid from re-entering `ownership_anomalies`.
    pub fn retired_order_audit_covers(
        &self,
        order_id: &str,
        original_size: f64,
        size_matched: f64,
    ) -> bool {
        let normalized = normalize_order_id(order_id);
        let state = self.lock_state();
        state
            .retired_order_audit_tombstones
            .get(&normalized)
            .is_some_and(|audit| {
                (audit.covers_any_zero_fill_size
                    || (audit.original_size - original_size).abs()
                        <= 1e-9_f64.max(audit.original_size.abs().max(original_size.abs()) * 1e-8))
                    && size_matched.abs() <= 1e-9
                    && audit.size_matched.abs() <= 1e-9
            })
    }

    pub fn resolve_private_event_anomaly(&self, payload_key: &str) {
        if payload_key.is_empty() {
            return;
        }
        let _transition = self.private_anomaly_transition.lock().unwrap();
        if !self
            .anomalous_private_event_keys
            .read()
            .unwrap()
            .contains(payload_key)
        {
            return;
        }
        let mut state = self.lock_state();
        if state
            .ownership_anomalies
            .remove(&format!("private_event:{payload_key}"))
            .is_some()
        {
            if let Some(order_id) = payload_key.strip_prefix("order:") {
                state
                    .orphan_order_anomaly_hints
                    .remove(&normalize_order_id(order_id));
            }
            recompute_reconciliation(&mut state, "corrected private event replay");
            self.schedule_persist(&state);
        }
    }

    pub fn mark_unresolved_trade_match_time(&self, trade_key: &str, match_time_secs: u64) {
        if trade_key.is_empty() || match_time_secs == 0 {
            return;
        }
        let mut state = self.lock_state();
        if state
            .unresolved_trade_match_times
            .get(trade_key)
            .is_some_and(|existing| *existing == match_time_secs)
        {
            return;
        }
        state
            .unresolved_trade_match_times
            .insert(trade_key.to_string(), match_time_secs);
        self.schedule_persist(&state);
        self.unresolved_trade_keys
            .insert(trade_key.to_string(), String::new());
    }

    pub fn resolve_unresolved_trade_match_time(&self, trade_key: &str) {
        if trade_key.is_empty() {
            return;
        }
        // Ordinary owned trades have no unresolved anchor. Keep that dominant
        // path completely out of the account-wide control/state lock.
        if self.unresolved_trade_keys.remove(trade_key).is_none() {
            return;
        }
        let mut state = self.lock_state();
        if state
            .unresolved_trade_match_times
            .remove(trade_key)
            .is_some()
        {
            self.schedule_persist(&state);
        }
    }

    pub fn earliest_unresolved_trade_match_time(&self) -> Option<u64> {
        self.lock_state()
            .unresolved_trade_match_times
            .values()
            .copied()
            .min()
    }

    pub fn repair_ownership_anomaly(&self, anomaly_key: &str) -> bool {
        let mut state = self.lock_state();
        if state.ownership_anomalies.remove(anomaly_key).is_none() {
            return false;
        }
        recompute_reconciliation(&mut state, "explicit ownership repair");
        self.schedule_persist(&state);
        true
    }

    pub fn order_owner_by_coid(&self, client_order_id: &str) -> Option<String> {
        self.coid_routes.get(client_order_id)
    }

    pub fn order_owner_by_oid(&self, order_id: &str) -> Option<String> {
        self.oid_routes.get(&normalize_order_id(order_id))
    }

    /// Resolve a persisted client-order-id hint against currently configured
    /// virtual shards without publishing an active route.  Instance ids may
    /// themselves contain dashes, so choose the longest registered `id-`
    /// prefix rather than splitting at the first dash.
    pub fn instance_id_for_historical_coid(&self, client_order_id: &str) -> Option<String> {
        let accounts = self.virtual_accounts.read().unwrap();
        accounts
            .keys()
            .filter(|instance_id| {
                client_order_id
                    .strip_prefix(instance_id.as_str())
                    .is_some_and(|suffix| suffix.starts_with('-'))
            })
            .max_by_key(|instance_id| instance_id.len())
            .cloned()
    }

    pub fn order(&self, client_order_id: &str) -> Option<OrderOwnership> {
        if let Some(account) = self.virtual_account_for_coid(client_order_id) {
            if let Some(order) = account
                .lifecycle
                .lock()
                .unwrap()
                .orders
                .get(client_order_id)
                .cloned()
            {
                return Some(order);
            }
        }
        // Snapshot publication is intentionally lock-free to private readers.
        // If a route is temporarily absent/stale, scan the small instance map
        // and repair both indexes instead of turning a valid event into an
        // account-wide ownership anomaly.
        let accounts: Vec<Arc<VirtualAccount>> = self
            .virtual_accounts
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect();
        for account in accounts {
            let order = account
                .lifecycle
                .lock()
                .unwrap()
                .orders
                .get(client_order_id)
                .cloned();
            if let Some(order) = order {
                self.coid_routes
                    .insert(client_order_id.to_string(), account.instance_id.clone());
                self.oid_routes.insert(
                    normalize_order_id(&order.order_id),
                    account.instance_id.clone(),
                );
                return Some(order);
            }
        }
        None
    }

    /// Recover an ownership row by exchange order id when a retired runtime
    /// route receives a late private lifecycle replay. This is a cold-path
    /// fallback: the normal private owner route uses its immutable OID index.
    /// Merely finding the row does not clear an anomaly; callers must first
    /// validate the event's token/economics, then use
    /// [`Self::reconcile_order_route`] to acknowledge the exact evidence.
    pub fn order_by_oid(&self, order_id: &str) -> Option<OrderOwnership> {
        let normalized = normalize_order_id(order_id);
        if normalized.is_empty() {
            return None;
        }
        let accounts: Vec<Arc<VirtualAccount>> = self
            .virtual_accounts
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect();
        let mut recovered: Option<(String, OrderOwnership)> = None;
        for account in accounts {
            let lifecycle = account.lifecycle.lock().unwrap();
            for order in lifecycle
                .orders
                .values()
                .filter(|order| normalize_order_id(&order.order_id) == normalized)
            {
                if recovered.as_ref().is_some_and(|(_, existing)| {
                    existing.client_order_id != order.client_order_id
                        || existing.instance_id != order.instance_id
                }) {
                    return None;
                }
                recovered = Some((account.instance_id.clone(), order.clone()));
            }
        }
        let (instance_id, order) = recovered?;
        self.coid_routes
            .insert(order.client_order_id.clone(), instance_id.clone());
        self.oid_routes.insert(normalized, instance_id);
        Some(order)
    }

    /// Cheap instance-shard predicate used by the asynchronous executor
    /// cleanup worker after a private fill. It intentionally does not enter
    /// `control_gate`: the order row, exact terminal ids, applied trades and
    /// reservation residuals all live in the same virtual lifecycle shard.
    pub fn filled_order_ready_for_retirement(&self, client_order_id: &str) -> bool {
        let Some(account) = self.virtual_account_for_coid(client_order_id) else {
            return false;
        };
        let lifecycle = account.lifecycle.lock().unwrap();
        let Some(order) = lifecycle.orders.get(client_order_id) else {
            return false;
        };
        order.status == OrderStatus::Filled
            && if order.terminal_trade_ids_authoritative {
                terminal_order_audit_complete_virtual(&lifecycle, client_order_id)
            } else {
                order.reserved_cash <= EPS && order.reserved_quantity <= EPS
            }
    }

    /// Resolve an order through its durable instance shard and repair any
    /// transient route hole. A successful authoritative reconcile also clears
    /// the exact private-event anomaly that previously blocked admission.
    pub fn reconcile_order_route(
        &self,
        client_order_id: &str,
        order_id: &str,
    ) -> Option<OrderOwnership> {
        let order = self.order(client_order_id)?;
        if normalize_order_id(&order.order_id) != normalize_order_id(order_id) {
            return None;
        }
        self.resolve_private_event_anomaly(&format!("order:{}", normalize_order_id(order_id)));
        Some(order)
    }

    /// Restore a missing instance lifecycle row from the complete ownership
    /// mirror captured at reservation publication. The operation is
    /// idempotent and raises reservation counters only to the conservative
    /// minimum implied by all retained orders, so it cannot double-reserve.
    pub fn backfill_order_ownership(&self, ownership: &OrderOwnership) -> Option<OrderOwnership> {
        if ownership.account_id != self.account_id
            || ownership.client_order_id.is_empty()
            || ownership.order_id.is_empty()
        {
            return None;
        }
        let account = self.virtual_account(&ownership.instance_id)?;
        let _publication = account.reservation_publish.lock().unwrap();
        let mut lifecycle = account.lifecycle.lock().unwrap();
        let normalized = normalize_order_id(&ownership.order_id);
        let existing = lifecycle.orders.get(&ownership.client_order_id).cloned();
        if existing.as_ref().is_some_and(|existing| {
            normalize_order_id(&existing.order_id) != normalized
                || existing.instance_id != ownership.instance_id
        }) {
            return None;
        }
        let mut coid_routes = self.coid_routes.write_shard(&ownership.client_order_id);
        let mut oid_routes = self.oid_routes.write_shard(&normalized);
        if coid_routes
            .get(&ownership.client_order_id)
            .is_some_and(|owner| owner != &ownership.instance_id)
            || oid_routes
                .get(&normalized)
                .is_some_and(|owner| owner != &ownership.instance_id)
        {
            return None;
        }
        let inserted = existing.is_none();
        if inserted {
            lifecycle
                .orders
                .insert(ownership.client_order_id.clone(), ownership.clone());
            let minimum_cash: f64 = lifecycle
                .orders
                .values()
                .map(|order| order.reserved_cash)
                .sum();
            account.reserved_cash.ensure_at_least(minimum_cash);
            let minimum_position: f64 = lifecycle
                .orders
                .values()
                .filter(|order| order.token_id == ownership.token_id)
                .map(|order| order.reserved_quantity)
                .sum();
            account
                .position(&ownership.token_id)
                .reserved
                .ensure_at_least(minimum_position);
        }
        coid_routes.insert(
            ownership.client_order_id.clone(),
            ownership.instance_id.clone(),
        );
        oid_routes.insert(normalized.clone(), ownership.instance_id.clone());
        if inserted {
            account.reservation_epoch.fetch_add(1, Ordering::Release);
            self.schedule_virtual_lifecycle_persist(
                &account,
                &lifecycle,
                &ownership.client_order_id,
            );
        }
        drop(oid_routes);
        drop(coid_routes);
        drop(lifecycle);
        drop(_publication);
        self.resolve_private_event_anomaly(&format!("order:{normalized}"));
        Some(existing.unwrap_or_else(|| ownership.clone()))
    }

    pub fn mark_order_status(&self, client_order_id: &str, status: OrderStatus) {
        let _ = self.mark_order_status_effective(client_order_id, status);
    }

    /// Apply a local lifecycle update and return the state that actually won.
    /// Callers that emit an OrderUpdate can use this to avoid forwarding a
    /// stale HTTP acknowledgement after a sticky terminal status.
    pub fn mark_order_status_effective(
        &self,
        client_order_id: &str,
        status: OrderStatus,
    ) -> Option<OrderStatus> {
        let account = self.virtual_account_for_coid(client_order_id)?;
        let mut lifecycle = account.lifecycle.lock().unwrap();
        if let Some(current_status) = lifecycle
            .orders
            .get(client_order_id)
            .map(|order| order.status)
        {
            // REST placement acknowledgements can arrive after the private
            // feed has already advanced the order. FAILED/FILLED are sticky;
            // PartiallyFilled is also monotonic against the weaker Accepted
            // state because an observed match cannot be undone by a late ACK.
            if (matches!(current_status, OrderStatus::Failed | OrderStatus::Filled)
                && status != current_status)
                || (current_status == OrderStatus::PartiallyFilled
                    && status == OrderStatus::Accepted)
                || (matches!(
                    current_status,
                    OrderStatus::Cancelled | OrderStatus::Rejected
                ) && matches!(
                    status,
                    OrderStatus::Pending
                        | OrderStatus::ExecutorRejected
                        | OrderStatus::NewOrderTimeout
                        | OrderStatus::CancelOrderTimeout
                        | OrderStatus::CancelUncertain
                ))
            {
                return Some(current_status);
            }

            // Exchange lifecycle pushes are not ordered. If a cancellation
            // is followed by an authoritative live status, restore the
            // reservation from the durable order economics before exposing
            // Accepted to callers. Merely changing `status` would leave the
            // account with released collateral while the order is live.
            if current_status == OrderStatus::Cancelled
                && matches!(status, OrderStatus::Accepted | OrderStatus::PartiallyFilled)
            {
                let (token_id, old_cash, old_qty, desired_cash, desired_qty) = {
                    let order = lifecycle
                        .orders
                        .get_mut(client_order_id)
                        .expect("order status read above");
                    order.status = status;
                    order.terminal_matched_quantity = None;
                    order.terminal_trade_ids.clear();
                    order.terminal_trade_ids_authoritative = false;
                    let old_cash = order.reserved_cash;
                    let old_qty = order.reserved_quantity;
                    let (desired_cash, desired_qty) = desired_order_reservation(order);
                    order.reserved_cash = desired_cash;
                    order.reserved_quantity = desired_qty;
                    (
                        order.token_id.clone(),
                        old_cash,
                        old_qty,
                        desired_cash,
                        desired_qty,
                    )
                };
                account.adjust_reservation(
                    &token_id,
                    desired_cash - old_cash,
                    desired_qty - old_qty,
                );
                lifecycle.recovery_pending_orders.remove(client_order_id);
                lifecycle.routine_cancel_audits.remove(client_order_id);
                let clear_cancel_anomaly = lifecycle.cancel_audit_anomalies.remove(client_order_id);
                Self::record_virtual_trade_mutation(
                    &account,
                    &mut lifecycle,
                    "",
                    client_order_id,
                    &token_id,
                );
                self.schedule_virtual_lifecycle_persist(&account, &lifecycle, client_order_id);
                drop(lifecycle);
                if clear_cancel_anomaly {
                    self.clear_cancel_audit_anomaly(client_order_id);
                }
                return Some(status);
            } else {
                let token_id = lifecycle
                    .orders
                    .get_mut(client_order_id)
                    .expect("order status read above")
                    .token_id
                    .clone();
                lifecycle
                    .orders
                    .get_mut(client_order_id)
                    .expect("order status read above")
                    .status = status;
                Self::record_virtual_trade_mutation(
                    &account,
                    &mut lifecycle,
                    "",
                    client_order_id,
                    &token_id,
                );
            }
            self.schedule_virtual_lifecycle_persist(&account, &lifecycle, client_order_id);
            return Some(status);
        }
        None
    }

    /// A terminal exchange order status does not prove that every fill leg has
    /// reached the trade ledger. Preserve any unconsumed reservation and enter
    /// the sticky audit gate until those fills are booked. The edge result lets
    /// callers suppress duplicate WARNs from repeated Filled lifecycle rows.
    pub fn mark_filled_pending_audit(&self, client_order_id: &str) -> FillAuditPendingTransition {
        let _control = self.control_gate.read().unwrap();
        let Some(account) = self.virtual_account_for_coid(client_order_id) else {
            return FillAuditPendingTransition::NotTracked;
        };
        let mut lifecycle = account.lifecycle.lock().unwrap();
        let Some(order) = lifecycle.orders.get_mut(client_order_id) else {
            return FillAuditPendingTransition::NotTracked;
        };
        order.status = OrderStatus::Filled;
        let has_exact_terminal_audit = order.terminal_trade_ids_authoritative;
        let residual_pending = order.reserved_cash > EPS || order.reserved_quantity > EPS;
        let pending = if has_exact_terminal_audit {
            !terminal_order_audit_complete_virtual(&lifecycle, client_order_id)
        } else {
            residual_pending
        };
        let already_pending = lifecycle.recovery_pending_orders.contains(client_order_id);
        lifecycle.routine_cancel_audits.remove(client_order_id);
        if pending {
            lifecycle
                .recovery_pending_orders
                .insert(client_order_id.to_string());
        }
        self.schedule_virtual_lifecycle_persist(&account, &lifecycle, client_order_id);
        let transition = if !pending {
            FillAuditPendingTransition::Resolved
        } else if already_pending {
            FillAuditPendingTransition::AlreadyPending
        } else {
            FillAuditPendingTransition::NewlyPending
        };
        if transition.newly_pending() {
            self.notify_order_audit_worker();
        }
        transition
    }

    /// Commit a complete authenticated order audit in the same ledger
    /// transaction as the terminal target, exact associated trade IDs and
    /// reservation resize. Recovery can then fetch only missing trade rows;
    /// it never needs another order GET for metadata already observed here.
    pub fn apply_authoritative_order_audit(
        &self,
        client_order_id: &str,
        status: OrderStatus,
        audit: &AuthoritativeOrderAudit,
    ) -> Result<FillAuditPendingTransition, String> {
        if !matches!(status, OrderStatus::Filled | OrderStatus::Cancelled) {
            return Err(format!(
                "authoritative terminal audit requires Filled/Cancelled, got {status:?}",
            ));
        }
        let original = audit
            .original_size
            .as_deref()
            .ok_or_else(|| "authoritative audit omitted original_size".to_string())?
            .parse::<f64>()
            .map_err(|_| "authoritative audit has invalid original_size".to_string())?;
        let matched = audit
            .size_matched
            .as_deref()
            .ok_or_else(|| "authoritative audit omitted size_matched".to_string())?
            .parse::<f64>()
            .map_err(|_| "authoritative audit has invalid size_matched".to_string())?;
        let mut trade_ids = audit.associate_trades.clone();
        if trade_ids.iter().any(|id| id.trim().is_empty()) {
            return Err("authoritative audit contains an empty trade id".to_string());
        }
        trade_ids.sort();
        if trade_ids.windows(2).any(|ids| ids[0] == ids[1]) {
            return Err("authoritative audit contains duplicate trade ids".to_string());
        }

        let control = self.control_gate.read().unwrap();
        let Some(account) = self.virtual_account_for_coid(client_order_id) else {
            return Ok(FillAuditPendingTransition::NotTracked);
        };
        let mut lifecycle = account.lifecycle.lock().unwrap();
        let Some(existing) = lifecycle.orders.get(client_order_id).cloned() else {
            return Ok(FillAuditPendingTransition::NotTracked);
        };
        let tolerance = existing.quantity.abs().max(1.0) * 1e-8;
        if !original.is_finite()
            || (original - existing.quantity).abs() > tolerance
            || !matched.is_finite()
            || matched < -tolerance
            || matched > existing.quantity + tolerance
            || matched + tolerance < existing.filled_quantity
            || (matched > tolerance && trade_ids.is_empty())
        {
            return Err(format!(
                "authoritative audit disagrees with owned order coid={client_order_id} original={original} matched={matched} owned_quantity={} filled={}",
                existing.quantity, existing.filled_quantity,
            ));
        }

        let already_pending = lifecycle.recovery_pending_orders.contains(client_order_id);
        let (token_id, old_cash, old_qty, desired_cash, desired_qty) = {
            let order = lifecycle
                .orders
                .get_mut(client_order_id)
                .expect("checked above");
            order.status = status;
            order.terminal_matched_quantity = Some(matched.clamp(0.0, order.quantity));
            order.terminal_trade_ids = trade_ids;
            order.terminal_trade_ids_authoritative = true;
            let old_cash = order.reserved_cash;
            let old_qty = order.reserved_quantity;
            let (desired_cash, desired_qty) = desired_order_reservation(order);
            order.reserved_cash = desired_cash;
            order.reserved_quantity = desired_qty;
            (
                order.token_id.clone(),
                old_cash,
                old_qty,
                desired_cash,
                desired_qty,
            )
        };
        account.adjust_reservation(&token_id, desired_cash - old_cash, desired_qty - old_qty);
        lifecycle.routine_cancel_audits.remove(client_order_id);

        let complete = terminal_order_audit_complete_virtual(&lifecycle, client_order_id);
        if complete {
            release_virtual_order_reservation(&account, &mut lifecycle, client_order_id);
            lifecycle.recovery_pending_orders.remove(client_order_id);
        } else {
            lifecycle
                .recovery_pending_orders
                .insert(client_order_id.to_string());
        }
        let clear_cancel_anomaly = lifecycle.cancel_audit_anomalies.remove(client_order_id);
        self.schedule_virtual_lifecycle_persist(&account, &lifecycle, client_order_id);

        let transition = if complete {
            FillAuditPendingTransition::Resolved
        } else if already_pending {
            FillAuditPendingTransition::AlreadyPending
        } else {
            FillAuditPendingTransition::NewlyPending
        };
        if transition.newly_pending() {
            self.notify_order_audit_worker();
        }
        drop(lifecycle);
        drop(control);
        if clear_cancel_anomaly {
            self.clear_cancel_audit_anomaly(client_order_id);
        }
        Ok(transition)
    }

    pub fn terminal_order_audit_complete(&self, client_order_id: &str) -> bool {
        let _control = self.control_gate.read().unwrap();
        let Some(account) = self.virtual_account_for_coid(client_order_id) else {
            return false;
        };
        let complete = terminal_order_audit_complete_virtual(
            &account.lifecycle.lock().unwrap(),
            client_order_id,
        );
        complete
    }

    pub fn mark_cancelled_pending_trade_audit(
        &self,
        client_order_id: &str,
        size_matched: f64,
    ) -> bool {
        let control = self.control_gate.read().unwrap();
        let Some(account) = self.virtual_account_for_coid(client_order_id) else {
            return false;
        };
        let mut lifecycle = account.lifecycle.lock().unwrap();
        let already_pending = lifecycle.recovery_pending_orders.contains(client_order_id);
        lifecycle.routine_cancel_audits.remove(client_order_id);
        let Some(existing) = lifecycle.orders.get(client_order_id) else {
            return false;
        };
        let quantity = existing.quantity;
        let filled = existing.filled_quantity;
        let tolerance = 1e-8_f64.max(quantity.abs() * 1e-8);
        if !size_matched.is_finite()
            || size_matched < -tolerance
            || size_matched > quantity + tolerance
            || size_matched + tolerance < filled
        {
            lifecycle
                .cancel_audit_anomalies
                .insert(client_order_id.to_string());
            drop(lifecycle);
            drop(control);
            let mut state = self.lock_state();
            set_ownership_anomaly(
                &mut state,
                format!("order_cancel_audit:{client_order_id}"),
                format!("invalid cancellation audit coid={client_order_id} size_matched={size_matched} filled={filled} quantity={quantity}"),
            );
            self.schedule_persist(&state);
            return true;
        }
        let order = lifecycle
            .orders
            .get_mut(client_order_id)
            .expect("checked above");
        order.status = OrderStatus::Cancelled;
        order.terminal_matched_quantity = Some(size_matched.clamp(0.0, quantity));
        order.terminal_trade_ids.clear();
        order.terminal_trade_ids_authoritative = false;
        let token_id = order.token_id.clone();
        let old_cash = order.reserved_cash;
        let old_qty = order.reserved_quantity;
        let (desired_cash, desired_qty) = desired_order_reservation(order);
        order.reserved_cash = desired_cash;
        order.reserved_quantity = desired_qty;
        account.adjust_reservation(&token_id, desired_cash - old_cash, desired_qty - old_qty);
        let pending = desired_cash > EPS || desired_qty > EPS;
        if pending {
            lifecycle
                .recovery_pending_orders
                .insert(client_order_id.to_string());
        } else {
            lifecycle.recovery_pending_orders.remove(client_order_id);
        }
        let clear_cancel_anomaly = lifecycle.cancel_audit_anomalies.remove(client_order_id);
        self.schedule_virtual_lifecycle_persist(&account, &lifecycle, client_order_id);
        if pending && !already_pending {
            self.notify_order_audit_worker();
        }
        drop(lifecycle);
        drop(control);
        if clear_cancel_anomaly {
            self.clear_cancel_audit_anomaly(client_order_id);
        }
        pending
    }

    /// DELETE acknowledgements have no matched quantity; preserve the full
    /// residual lock until an order-specific audit arrives, without globally
    /// pausing unrelated instances on the same account.
    pub fn mark_cancelled_pending_audit(&self, client_order_id: &str) -> bool {
        let _control = self.control_gate.read().unwrap();
        let Some(account) = self.virtual_account_for_coid(client_order_id) else {
            return false;
        };
        let mut lifecycle = account.lifecycle.lock().unwrap();
        let Some(order) = lifecycle.orders.get_mut(client_order_id) else {
            return false;
        };
        order.status = OrderStatus::Cancelled;
        order.terminal_matched_quantity = None;
        order.terminal_trade_ids.clear();
        order.terminal_trade_ids_authoritative = false;
        lifecycle.recovery_pending_orders.remove(client_order_id);
        lifecycle
            .startup_query_repair_orders
            .remove(client_order_id);
        let newly_pending = lifecycle
            .routine_cancel_audits
            .insert(client_order_id.to_string());
        self.schedule_virtual_lifecycle_persist(&account, &lifecycle, client_order_id);
        if newly_pending {
            self.notify_order_audit_worker();
        }
        true
    }

    /// Release the still-unfilled reservation after an authoritative terminal
    /// order outcome. Ownership is retained for late fill attribution.
    pub fn release_order(&self, client_order_id: &str, status: OrderStatus) {
        let _control = self.control_gate.read().unwrap();
        let Some(account) = self.virtual_account_for_coid(client_order_id) else {
            return;
        };
        let mut lifecycle = account.lifecycle.lock().unwrap();
        let Some(mut order) = lifecycle.orders.remove(client_order_id) else {
            return;
        };
        account.adjust_reservation(
            &order.token_id,
            -order.reserved_cash,
            -order.reserved_quantity,
        );
        order.reserved_cash = 0.0;
        order.reserved_quantity = 0.0;
        order.status = status;
        lifecycle.orders.insert(client_order_id.into(), order);
        lifecycle.routine_cancel_audits.remove(client_order_id);
        self.schedule_virtual_lifecycle_persist(&account, &lifecycle, client_order_id);
    }

    pub fn release_all_orders(&self) {
        let coids = self.coid_routes.keys();
        for coid in coids {
            self.release_order(&coid, OrderStatus::Cancelled);
        }
    }

    /// Atomically reserve each instance's requested cash for one aggregated
    /// on-chain split. Reuses the same reserved-cash counters as orders, so
    /// split and order admission cannot race on either virtual or physical
    /// funds.
    pub fn reserve_split_allocations(
        &self,
        allocations: &HashMap<String, f64>,
    ) -> Result<(), ReservationError> {
        self.ensure_admission_persistence()?;
        let mut state = self.lock_state();
        if !state.seeded {
            return Err(ReservationError::AccountNotSeeded);
        }
        if state.uncertain {
            return Err(ReservationError::AccountUncertain);
        }
        reject_allocation_audit_blockers(&state, allocations)?;
        let total: f64 = allocations.values().copied().sum();
        let total_reserved_cash: f64 = state
            .instances
            .values()
            .map(InstanceLedger::total_reserved_cash)
            .sum();
        let physical_available = (state.physical_cash - total_reserved_cash).max(0.0);
        if total > physical_available + EPS {
            return Err(ReservationError::InsufficientPhysicalCash {
                required: total,
                available: physical_available,
            });
        }
        for (instance_id, amount) in allocations {
            let Some(instance) = state.instances.get(instance_id) else {
                return Err(ReservationError::UnknownInstance(instance_id.clone()));
            };
            let available = (instance.cash - instance.total_reserved_cash()).max(0.0);
            if *amount > available + EPS {
                return Err(ReservationError::InsufficientVirtualCash {
                    required: *amount,
                    available,
                });
            }
        }
        for (instance_id, amount) in allocations {
            state
                .instances
                .get_mut(instance_id)
                .expect("validated")
                .reserved_cash += *amount;
        }
        self.schedule_persist(&state);
        drop(state);
        if let Err(error) = self.flush_admission_persistence() {
            self.release_split_allocations(allocations);
            return Err(error);
        }
        Ok(())
    }

    pub fn release_split_allocations(&self, allocations: &HashMap<String, f64>) {
        let mut state = self.lock_state();
        for (instance_id, amount) in allocations {
            if let Some(instance) = state.instances.get_mut(instance_id) {
                instance.reserved_cash = (instance.reserved_cash - *amount).max(0.0);
            }
        }
        self.schedule_persist(&state);
    }

    /// Commit a previously-reserved aggregate split after on-chain
    /// confirmation, preserving each instance's exact requested amount.
    pub fn confirm_reserved_split(
        &self,
        up_token: &str,
        down_token: &str,
        allocations: &HashMap<String, f64>,
    ) -> Result<(), ReservationError> {
        let mut state = self.lock_state();
        let total: f64 = allocations.values().copied().sum();
        if total > state.physical_cash + EPS {
            return Err(ReservationError::InsufficientPhysicalCash {
                required: total,
                available: state.physical_cash,
            });
        }
        for (instance_id, amount) in allocations {
            let Some(instance) = state.instances.get(instance_id) else {
                return Err(ReservationError::UnknownInstance(instance_id.clone()));
            };
            if *amount > instance.cash + EPS || *amount > instance.reserved_cash + EPS {
                return Err(ReservationError::InsufficientVirtualCash {
                    required: *amount,
                    available: instance.cash.min(instance.reserved_cash),
                });
            }
        }
        state.physical_cash -= total;
        *state
            .physical_positions
            .entry(up_token.into())
            .or_insert(0.0) += total;
        *state
            .physical_positions
            .entry(down_token.into())
            .or_insert(0.0) += total;
        for (instance_id, amount) in allocations {
            let instance = state.instances.get_mut(instance_id).expect("validated");
            instance.reserved_cash = (instance.reserved_cash - *amount).max(0.0);
            instance.cash -= *amount;
            *instance.positions.entry(up_token.into()).or_insert(0.0) += *amount;
            *instance.positions.entry(down_token.into()).or_insert(0.0) += *amount;
        }
        // This legacy direct API has no maintenance-operation row. Preserve
        // the same immutable replay guarantee through an internal durable
        // adjustment rather than leaving an unrooted balance mutation.
        for (instance_id, amount) in allocations {
            record_internal_external_adjustment(
                &mut state,
                "direct_split",
                instance_id,
                -*amount,
                HashMap::from([
                    (up_token.to_string(), *amount),
                    (down_token.to_string(), *amount),
                ]),
            );
        }
        recompute_reconciliation(&mut state, "confirmed split");
        self.schedule_persist(&state);
        Ok(())
    }

    /// Apply confirmed redeem legs from the account-wide maintenance worker.
    /// Each token's collateral payout is allocated in proportion to the
    /// virtual quantity owned by each instance immediately before the burn.
    pub fn apply_redeemed_legs(&self, legs: &[(String, f64, f64)]) -> Result<(), ReservationError> {
        let mut state = self.lock_state();
        if !state.seeded {
            return Err(ReservationError::AccountNotSeeded);
        }
        if state.uncertain {
            return Err(ReservationError::AccountUncertain);
        }
        for (token, requested_qty, requested_payout) in legs {
            let physical_before = state.physical_positions.get(token).copied().unwrap_or(0.0);
            let removed = finite_nonnegative(*requested_qty).min(physical_before);
            if removed <= EPS {
                continue;
            }
            let virtual_total: f64 = state
                .instances
                .values()
                .map(|instance| instance.positions.get(token).copied().unwrap_or(0.0))
                .sum();
            if virtual_total <= EPS {
                set_uncertain(
                    &mut state,
                    format!("redeem token `{token}` has physical quantity but no virtual owner"),
                );
                self.schedule_persist(&state);
                return Err(ReservationError::InvalidOrder(format!(
                    "redeem token {token} has physical quantity but no virtual owner"
                )));
            }
            let payout = finite_nonnegative(*requested_payout)
                * if *requested_qty > EPS {
                    removed / *requested_qty
                } else {
                    0.0
                };
            let ownership_scale = removed / virtual_total;
            let mut attributed = Vec::new();
            for (instance_id, instance) in &mut state.instances {
                let owned = instance.positions.get(token).copied().unwrap_or(0.0);
                if owned <= EPS {
                    continue;
                }
                let burned = (owned * ownership_scale).min(owned);
                let share = burned / removed;
                *instance.positions.entry(token.clone()).or_insert(0.0) -= burned;
                let cash_delta = payout * share;
                instance.cash += cash_delta;
                attributed.push((instance_id.clone(), burned, cash_delta));
            }
            for (instance_id, burned, cash_delta) in attributed {
                record_internal_external_adjustment(
                    &mut state,
                    "confirmed_redeem",
                    &instance_id,
                    cash_delta,
                    HashMap::from([(token.clone(), -burned)]),
                );
            }
            *state.physical_positions.entry(token.clone()).or_insert(0.0) -= removed;
            state.physical_cash += payout;
        }
        recompute_reconciliation(&mut state, "confirmed redeem");
        self.schedule_persist(&state);
        Ok(())
    }

    /// Reserve equal Up+Down quantities per instance for one aggregated
    /// merge. This uses the same position locks as SELL orders.
    pub fn reserve_merge_allocations(
        &self,
        up_token: &str,
        down_token: &str,
        allocations: &HashMap<String, f64>,
    ) -> Result<(), ReservationError> {
        self.ensure_admission_persistence()?;
        let mut state = self.lock_state();
        if !state.seeded {
            return Err(ReservationError::AccountNotSeeded);
        }
        if state.uncertain {
            return Err(ReservationError::AccountUncertain);
        }
        reject_allocation_audit_blockers(&state, allocations)?;
        let total: f64 = allocations.values().copied().sum();
        for token in [up_token, down_token] {
            let physical_reserved: f64 = state
                .instances
                .values()
                .map(|instance| {
                    instance
                        .reserved_positions
                        .get(token)
                        .copied()
                        .unwrap_or(0.0)
                })
                .sum();
            let physical_available = (state.physical_positions.get(token).copied().unwrap_or(0.0)
                - physical_reserved)
                .max(0.0);
            if total > physical_available + EPS {
                return Err(ReservationError::InsufficientPhysicalPosition {
                    token: token.into(),
                    required: total,
                    available: physical_available,
                });
            }
            for (instance_id, amount) in allocations {
                let Some(instance) = state.instances.get(instance_id) else {
                    return Err(ReservationError::UnknownInstance(instance_id.clone()));
                };
                let available = (instance.positions.get(token).copied().unwrap_or(0.0)
                    - instance
                        .reserved_positions
                        .get(token)
                        .copied()
                        .unwrap_or(0.0))
                .max(0.0);
                if *amount > available + EPS {
                    return Err(ReservationError::InsufficientVirtualPosition {
                        token: token.into(),
                        required: *amount,
                        available,
                    });
                }
            }
        }
        for (instance_id, amount) in allocations {
            let instance = state.instances.get_mut(instance_id).expect("validated");
            *instance
                .reserved_positions
                .entry(up_token.into())
                .or_insert(0.0) += *amount;
            *instance
                .reserved_positions
                .entry(down_token.into())
                .or_insert(0.0) += *amount;
        }
        self.schedule_persist(&state);
        drop(state);
        if let Err(error) = self.flush_admission_persistence() {
            self.release_merge_allocations(up_token, down_token, allocations);
            return Err(error);
        }
        Ok(())
    }

    pub fn release_merge_allocations(
        &self,
        up_token: &str,
        down_token: &str,
        allocations: &HashMap<String, f64>,
    ) {
        let mut state = self.lock_state();
        for (instance_id, amount) in allocations {
            if let Some(instance) = state.instances.get_mut(instance_id) {
                for token in [up_token, down_token] {
                    let reserved = instance
                        .reserved_positions
                        .entry(token.into())
                        .or_insert(0.0);
                    *reserved = (*reserved - *amount).max(0.0);
                }
            }
        }
        self.schedule_persist(&state);
    }

    pub fn confirm_reserved_merge(
        &self,
        up_token: &str,
        down_token: &str,
        allocations: &HashMap<String, f64>,
    ) -> Result<(), ReservationError> {
        let mut state = self.lock_state();
        let total: f64 = allocations.values().copied().sum();
        for token in [up_token, down_token] {
            let physical = state.physical_positions.get(token).copied().unwrap_or(0.0);
            if total > physical + EPS {
                return Err(ReservationError::InsufficientPhysicalPosition {
                    token: token.into(),
                    required: total,
                    available: physical,
                });
            }
        }
        for (instance_id, amount) in allocations {
            let Some(instance) = state.instances.get(instance_id) else {
                return Err(ReservationError::UnknownInstance(instance_id.clone()));
            };
            for token in [up_token, down_token] {
                let owned = instance.positions.get(token).copied().unwrap_or(0.0);
                let reserved = instance
                    .reserved_positions
                    .get(token)
                    .copied()
                    .unwrap_or(0.0);
                if *amount > owned.min(reserved) + EPS {
                    return Err(ReservationError::InsufficientVirtualPosition {
                        token: token.into(),
                        required: *amount,
                        available: owned.min(reserved),
                    });
                }
            }
        }
        state.physical_cash += total;
        *state
            .physical_positions
            .entry(up_token.into())
            .or_insert(0.0) -= total;
        *state
            .physical_positions
            .entry(down_token.into())
            .or_insert(0.0) -= total;
        for (instance_id, amount) in allocations {
            let instance = state.instances.get_mut(instance_id).expect("validated");
            instance.cash += *amount;
            for token in [up_token, down_token] {
                *instance.positions.entry(token.into()).or_insert(0.0) -= *amount;
                let reserved = instance
                    .reserved_positions
                    .entry(token.into())
                    .or_insert(0.0);
                *reserved = (*reserved - *amount).max(0.0);
            }
        }
        for (instance_id, amount) in allocations {
            record_internal_external_adjustment(
                &mut state,
                "direct_merge",
                instance_id,
                *amount,
                HashMap::from([
                    (up_token.to_string(), -*amount),
                    (down_token.to_string(), -*amount),
                ]),
            );
        }
        recompute_reconciliation(&mut state, "confirmed merge");
        self.schedule_persist(&state);
        Ok(())
    }

    pub fn apply_merge_allocations(
        &self,
        up_token: &str,
        down_token: &str,
        allocations: &HashMap<String, f64>,
    ) -> Result<(), ReservationError> {
        self.reserve_merge_allocations(up_token, down_token, allocations)?;
        match self.confirm_reserved_merge(up_token, down_token, allocations) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.release_merge_allocations(up_token, down_token, allocations);
                Err(error)
            }
        }
    }

    pub fn reserve_maintenance_operation(
        &self,
        operation_id: &str,
        kind: MaintenanceOperationKind,
        condition_id: &str,
        up_token_id: &str,
        down_token_id: &str,
        allocations: &HashMap<String, f64>,
    ) -> Result<(), ReservationError> {
        self.ensure_admission_persistence()?;
        if operation_id.is_empty()
            || condition_id.is_empty()
            || up_token_id.is_empty()
            || down_token_id.is_empty()
            || allocations.is_empty()
            || allocations
                .values()
                .any(|amount| !amount.is_finite() || *amount <= 0.0)
        {
            return Err(ReservationError::InvalidOrder(
                "invalid maintenance operation identity/tokens/allocations".to_string(),
            ));
        }
        let mut state = self.lock_state();
        if let Some(existing) = state.maintenance_ops.get(operation_id) {
            let expected_allocations: BTreeMap<String, f64> = allocations
                .iter()
                .map(|(instance, amount)| (instance.clone(), *amount))
                .collect();
            if existing.kind == kind
                && existing.condition_id == condition_id
                && existing.up_token_id == up_token_id
                && existing.down_token_id == down_token_id
                && existing.allocations == expected_allocations
            {
                return Ok(());
            }
            return Err(ReservationError::InvalidOrder(format!(
                "maintenance operation id `{operation_id}` was reused with different intent"
            )));
        }
        if !state.seeded {
            return Err(ReservationError::AccountNotSeeded);
        }
        if state.uncertain {
            return Err(ReservationError::AccountUncertain);
        }
        reject_allocation_audit_blockers(&state, allocations)?;
        let total: f64 = allocations.values().copied().sum();
        match kind {
            MaintenanceOperationKind::Split => {
                let total_reserved_cash: f64 = state
                    .instances
                    .values()
                    .map(InstanceLedger::total_reserved_cash)
                    .sum();
                let physical_available = (state.physical_cash - total_reserved_cash).max(0.0);
                if total > physical_available + EPS {
                    return Err(ReservationError::InsufficientPhysicalCash {
                        required: total,
                        available: physical_available,
                    });
                }
                for (instance_id, amount) in allocations {
                    let Some(instance) = state.instances.get(instance_id) else {
                        return Err(ReservationError::UnknownInstance(instance_id.clone()));
                    };
                    let available = (instance.cash - instance.total_reserved_cash()).max(0.0);
                    if *amount > available + EPS {
                        return Err(ReservationError::InsufficientVirtualCash {
                            required: *amount,
                            available,
                        });
                    }
                }
                for (instance_id, amount) in allocations {
                    state
                        .instances
                        .get_mut(instance_id)
                        .expect("validated")
                        .maintenance_reserved_cash += *amount;
                }
            }
            MaintenanceOperationKind::Merge => {
                for token in [up_token_id, down_token_id] {
                    let total_reserved: f64 = state
                        .instances
                        .values()
                        .map(|instance| instance.total_reserved_position(token))
                        .sum();
                    let physical = state.physical_positions.get(token).copied().unwrap_or(0.0);
                    let physical_available = (physical - total_reserved).max(0.0);
                    if total > physical_available + EPS {
                        return Err(ReservationError::InsufficientPhysicalPosition {
                            token: token.to_string(),
                            required: total,
                            available: physical_available,
                        });
                    }
                    for (instance_id, amount) in allocations {
                        let Some(instance) = state.instances.get(instance_id) else {
                            return Err(ReservationError::UnknownInstance(instance_id.clone()));
                        };
                        let available = (instance.positions.get(token).copied().unwrap_or(0.0)
                            - instance.total_reserved_position(token))
                        .max(0.0);
                        if *amount > available + EPS {
                            return Err(ReservationError::InsufficientVirtualPosition {
                                token: token.to_string(),
                                required: *amount,
                                available,
                            });
                        }
                    }
                }
                for (instance_id, amount) in allocations {
                    let instance = state.instances.get_mut(instance_id).expect("validated");
                    for token in [up_token_id, down_token_id] {
                        *instance
                            .maintenance_reserved_positions
                            .entry(token.to_string())
                            .or_insert(0.0) += *amount;
                    }
                }
            }
        }
        let now_ms = wall_clock_ms();
        state.maintenance_ops.insert(
            operation_id.to_string(),
            MaintenanceOperation {
                operation_id: operation_id.to_string(),
                kind,
                condition_id: condition_id.to_string(),
                up_token_id: up_token_id.to_string(),
                down_token_id: down_token_id.to_string(),
                allocations: allocations
                    .iter()
                    .map(|(instance, amount)| (instance.clone(), *amount))
                    .collect(),
                tx_id: None,
                status: MaintenanceOperationStatus::Reserved,
                created_at_ms: now_ms,
                updated_at_ms: now_ms,
                detail: None,
            },
        );
        self.schedule_persist(&state);
        drop(state);
        if let Err(error) = self.flush_admission_persistence() {
            self.fail_maintenance_operation(
                operation_id,
                format!("reservation persistence: {error}"),
            );
            return Err(error);
        }
        Ok(())
    }

    pub fn mark_maintenance_operation_submitted(
        &self,
        operation_id: &str,
        tx_id: &str,
    ) -> Result<(), String> {
        if tx_id.is_empty() {
            return Err("maintenance submission returned an empty tx id".to_string());
        }
        let mut state = self.lock_state();
        let Some(operation) = state.maintenance_ops.get_mut(operation_id) else {
            return Err(format!("unknown maintenance operation `{operation_id}`"));
        };
        if matches!(operation.status, MaintenanceOperationStatus::Confirmed) {
            return Ok(());
        }
        if matches!(operation.status, MaintenanceOperationStatus::Failed) {
            return Err(format!(
                "maintenance operation `{operation_id}` already failed"
            ));
        }
        if operation
            .tx_id
            .as_deref()
            .is_some_and(|existing| existing != tx_id)
        {
            return Err(format!(
                "maintenance operation `{operation_id}` tx id conflict: stored={} incoming={tx_id}",
                operation.tx_id.as_deref().unwrap_or_default(),
            ));
        }
        operation.tx_id = Some(tx_id.to_string());
        operation.status = MaintenanceOperationStatus::Submitted;
        operation.updated_at_ms = wall_clock_ms();
        self.schedule_persist(&state);
        Ok(())
    }

    pub fn mark_maintenance_operation_uncertain(
        &self,
        operation_id: &str,
        detail: impl Into<String>,
    ) {
        let detail = detail.into();
        let mut state = self.lock_state();
        if let Some(operation) = state.maintenance_ops.get_mut(operation_id) {
            operation.status = MaintenanceOperationStatus::Uncertain;
            operation.updated_at_ms = wall_clock_ms();
            operation.detail = Some(detail.clone());
        }
        set_uncertain(
            &mut state,
            format!("maintenance operation `{operation_id}` finality uncertain: {detail}"),
        );
        self.schedule_persist(&state);
    }

    /// Keep a confirmed-chain/virtual-attribution failure owned by the exact
    /// maintenance operation. A later proof may clear this source without
    /// touching an unrelated manual or subsystem blocker.
    pub fn mark_maintenance_attribution_uncertain(
        &self,
        operation_id: &str,
        detail: impl Into<String>,
    ) {
        self.set_risk_blocker(
            &format!("{MAINTENANCE_ATTRIBUTION_RISK_BLOCKER_PREFIX}{operation_id}"),
            detail,
        );
    }

    /// Remove only attribution blockers whose durable operation is already
    /// Confirmed. This also migrates the precise legacy `manual` reason emitted
    /// by older SDKs before maintenance blockers became operation-scoped.
    pub fn repair_confirmed_maintenance_risk_blockers(&self) -> usize {
        let mut state = self.lock_state();
        let cleared = clear_confirmed_maintenance_risk_blockers(&mut state);
        if cleared.is_empty() {
            return 0;
        }
        let mut fast_sources = self.risk_blocker_sources_fast.write().unwrap();
        for source in &cleared {
            fast_sources.remove(source);
        }
        drop(fast_sources);
        recompute_reconciliation(&mut state, "confirmed maintenance blocker recovery");
        self.schedule_persist(&state);
        cleared.len()
    }

    pub fn pending_maintenance_operations(&self) -> Vec<MaintenanceOperation> {
        self.lock_state()
            .maintenance_ops
            .values()
            .filter(|operation| {
                matches!(
                    operation.status,
                    MaintenanceOperationStatus::Reserved
                        | MaintenanceOperationStatus::Submitted
                        | MaintenanceOperationStatus::Uncertain
                )
            })
            .cloned()
            .collect()
    }

    pub fn maintenance_operation(&self, operation_id: &str) -> Option<MaintenanceOperation> {
        self.lock_state().maintenance_ops.get(operation_id).cloned()
    }

    pub fn fail_maintenance_operation(&self, operation_id: &str, detail: impl Into<String>) {
        let detail = detail.into();
        let mut state = self.lock_state();
        let Some(existing) = state.maintenance_ops.get(operation_id).cloned() else {
            return;
        };
        if matches!(
            existing.status,
            MaintenanceOperationStatus::Confirmed | MaintenanceOperationStatus::Failed
        ) {
            return;
        }
        for (instance_id, amount) in &existing.allocations {
            if let Some(instance) = state.instances.get_mut(instance_id) {
                match existing.kind {
                    MaintenanceOperationKind::Split => {
                        instance.maintenance_reserved_cash =
                            (instance.maintenance_reserved_cash - *amount).max(0.0);
                    }
                    MaintenanceOperationKind::Merge => {
                        for token in [&existing.up_token_id, &existing.down_token_id] {
                            let reserved = instance
                                .maintenance_reserved_positions
                                .entry(token.clone())
                                .or_insert(0.0);
                            *reserved = (*reserved - *amount).max(0.0);
                        }
                    }
                }
            }
        }
        if let Some(operation) = state.maintenance_ops.get_mut(operation_id) {
            operation.status = MaintenanceOperationStatus::Failed;
            operation.updated_at_ms = wall_clock_ms();
            operation.detail = Some(detail);
        }
        recompute_reconciliation(&mut state, "maintenance operation failed");
        self.schedule_persist(&state);
    }

    pub fn confirm_maintenance_operation(
        &self,
        operation_id: &str,
    ) -> Result<(), ReservationError> {
        let mut state = self.lock_state();
        let Some(existing) = state.maintenance_ops.get(operation_id).cloned() else {
            return Err(ReservationError::InvalidOrder(format!(
                "unknown maintenance operation `{operation_id}`"
            )));
        };
        if existing.status == MaintenanceOperationStatus::Confirmed {
            return Ok(());
        }
        if existing.status == MaintenanceOperationStatus::Failed {
            return Err(ReservationError::InvalidOrder(format!(
                "maintenance operation `{operation_id}` already failed"
            )));
        }
        if existing.tx_id.as_deref().is_none_or(str::is_empty) {
            return Err(ReservationError::InvalidOrder(format!(
                "maintenance operation `{operation_id}` has no persisted tx id"
            )));
        }
        let total: f64 = existing.allocations.values().copied().sum();
        match existing.kind {
            MaintenanceOperationKind::Split => {
                if total > state.physical_cash + EPS {
                    return Err(ReservationError::InsufficientPhysicalCash {
                        required: total,
                        available: state.physical_cash,
                    });
                }
                for (instance_id, amount) in &existing.allocations {
                    let Some(instance) = state.instances.get(instance_id) else {
                        return Err(ReservationError::UnknownInstance(instance_id.clone()));
                    };
                    if *amount > instance.cash.min(instance.maintenance_reserved_cash) + EPS {
                        return Err(ReservationError::InsufficientVirtualCash {
                            required: *amount,
                            available: instance.cash.min(instance.maintenance_reserved_cash),
                        });
                    }
                }
                state.physical_cash -= total;
                *state
                    .physical_positions
                    .entry(existing.up_token_id.clone())
                    .or_insert(0.0) += total;
                *state
                    .physical_positions
                    .entry(existing.down_token_id.clone())
                    .or_insert(0.0) += total;
                for (instance_id, amount) in &existing.allocations {
                    let instance = state.instances.get_mut(instance_id).expect("validated");
                    instance.maintenance_reserved_cash =
                        (instance.maintenance_reserved_cash - *amount).max(0.0);
                    instance.cash -= *amount;
                    *instance
                        .positions
                        .entry(existing.up_token_id.clone())
                        .or_insert(0.0) += *amount;
                    *instance
                        .positions
                        .entry(existing.down_token_id.clone())
                        .or_insert(0.0) += *amount;
                }
            }
            MaintenanceOperationKind::Merge => {
                for token in [&existing.up_token_id, &existing.down_token_id] {
                    let physical = state.physical_positions.get(token).copied().unwrap_or(0.0);
                    if total > physical + EPS {
                        return Err(ReservationError::InsufficientPhysicalPosition {
                            token: token.clone(),
                            required: total,
                            available: physical,
                        });
                    }
                }
                for (instance_id, amount) in &existing.allocations {
                    let Some(instance) = state.instances.get(instance_id) else {
                        return Err(ReservationError::UnknownInstance(instance_id.clone()));
                    };
                    for token in [&existing.up_token_id, &existing.down_token_id] {
                        let owned = instance.positions.get(token).copied().unwrap_or(0.0);
                        let reserved = instance
                            .maintenance_reserved_positions
                            .get(token)
                            .copied()
                            .unwrap_or(0.0);
                        if *amount > owned.min(reserved) + EPS {
                            return Err(ReservationError::InsufficientVirtualPosition {
                                token: token.clone(),
                                required: *amount,
                                available: owned.min(reserved),
                            });
                        }
                    }
                }
                state.physical_cash += total;
                *state
                    .physical_positions
                    .entry(existing.up_token_id.clone())
                    .or_insert(0.0) -= total;
                *state
                    .physical_positions
                    .entry(existing.down_token_id.clone())
                    .or_insert(0.0) -= total;
                for (instance_id, amount) in &existing.allocations {
                    let instance = state.instances.get_mut(instance_id).expect("validated");
                    instance.cash += *amount;
                    for token in [&existing.up_token_id, &existing.down_token_id] {
                        *instance.positions.entry(token.clone()).or_insert(0.0) -= *amount;
                        let reserved = instance
                            .maintenance_reserved_positions
                            .entry(token.clone())
                            .or_insert(0.0);
                        *reserved = (*reserved - *amount).max(0.0);
                    }
                }
            }
        }
        if let Some(operation) = state.maintenance_ops.get_mut(operation_id) {
            operation.status = MaintenanceOperationStatus::Confirmed;
            operation.updated_at_ms = wall_clock_ms();
            operation.detail = None;
        }
        let cleared = clear_confirmed_maintenance_risk_blockers(&mut state);
        if !cleared.is_empty() {
            let mut fast_sources = self.risk_blocker_sources_fast.write().unwrap();
            for source in cleared {
                fast_sources.remove(&source);
            }
        }
        recompute_reconciliation(&mut state, "confirmed maintenance operation");
        self.schedule_persist(&state);
        Ok(())
    }

    /// Attribute one user-feed trade leg. MATCHED/MINED/CONFIRMED book once;
    /// FAILED reverses that one booking. Replayed lifecycle messages are
    /// idempotent by `trade_key`.
    pub fn apply_trade_transition(
        &self,
        trade_key: &str,
        status: &str,
        client_order_id: &str,
        order_id: &str,
        token_id: &str,
        side: Side,
        quantity: f64,
        price: f64,
    ) -> Option<TradeOwnership> {
        let mut persistence_required = false;
        let mut owned_noop = false;
        self.apply_trade_transition_inner(
            trade_key,
            status,
            client_order_id,
            order_id,
            token_id,
            side,
            quantity,
            price,
            None,
            &mut persistence_required,
            &mut owned_noop,
        )
    }

    pub fn apply_trade_transition_with_context(
        &self,
        trade_key: &str,
        status: &str,
        client_order_id: &str,
        order_id: &str,
        token_id: &str,
        side: Side,
        quantity: f64,
        price: f64,
        is_maker: bool,
        match_time_secs: u64,
    ) -> TradeTransitionResult {
        let mut persistence_required = false;
        let mut owned_noop = false;
        let applied = self.apply_trade_transition_inner(
            trade_key,
            status,
            client_order_id,
            order_id,
            token_id,
            side,
            quantity,
            price,
            Some((is_maker, match_time_secs)),
            &mut persistence_required,
            &mut owned_noop,
        );
        let Some(ownership) = applied else {
            return TradeTransitionResult::Rejected;
        };
        if persistence_required {
            self.track_trade_persistence_generation();
        }
        if owned_noop {
            TradeTransitionResult::OwnedNoop(ownership)
        } else {
            TradeTransitionResult::Applied(ownership)
        }
    }

    /// Recover a terminal historical private trade after both its order row
    /// and original trade tombstone are absent. The caller must have proved
    /// that the authenticated account address owns this exact maker/taker leg.
    /// We additionally require a known settlement value and one unique
    /// historical instance owner for the token. No economics are applied: a
    /// startup wallet snapshot already includes this aged trade and settlement.
    pub fn record_authenticated_terminal_trade_noop(
        &self,
        trade_key: &str,
        status: &str,
        order_id: &str,
        token_id: &str,
        side: Side,
        quantity: f64,
        price: f64,
        is_maker: bool,
    ) -> TradeTransitionResult {
        let normalized = status
            .trim_start_matches("TRADE_STATUS_")
            .to_ascii_uppercase();
        let anomaly_key = if trade_key.is_empty() {
            format!("trade:<missing>:{order_id}")
        } else {
            format!("trade:{trade_key}")
        };
        let mut state = self.lock_state();
        let reject = |state: &mut SharedAccountState, reason: String| {
            set_ownership_anomaly(state, anomaly_key.clone(), reason);
        };
        if !matches!(normalized.as_str(), "CONFIRMED" | "FAILED")
            || trade_key.trim().is_empty()
            || order_id.trim().is_empty()
            || token_id.trim().is_empty()
            || !quantity.is_finite()
            || quantity <= 0.0
            || !price.is_finite()
            || price <= 0.0
            || price > 1.0 + 1e-8
        {
            reject(
                &mut state,
                format!(
                    "authenticated historical trade `{trade_key}` is not a valid terminal edge"
                ),
            );
            self.schedule_trade_persist(&state, trade_key, "", order_id, token_id);
            return TradeTransitionResult::Rejected;
        }
        if state.trades.contains_key(trade_key)
            || state
                .retired_trade_ownership_tombstones
                .contains_key(trade_key)
        {
            reject(
                &mut state,
                format!(
                    "authenticated historical trade `{trade_key}` conflicts with an existing durable trade proof"
                ),
            );
            self.schedule_trade_persist(&state, trade_key, "", order_id, token_id);
            return TradeTransitionResult::Rejected;
        }
        if !state.settled_token_values.contains_key(token_id) {
            reject(
                &mut state,
                format!(
                    "authenticated historical trade `{trade_key}` token `{token_id}` has no durable settlement proof"
                ),
            );
            self.schedule_trade_persist(&state, trade_key, "", order_id, token_id);
            return TradeTransitionResult::Rejected;
        }

        // A zero position key survives settlement and is stronger ownership
        // evidence than a temporarily shared interest registration. Fall back
        // to interests only when no instance ledger retains the token key.
        let position_owners: Vec<String> = state
            .instances
            .iter()
            .filter(|(_, instance)| instance.positions.contains_key(token_id))
            .map(|(instance_id, _)| instance_id.clone())
            .collect();
        let candidate_owners = if position_owners.is_empty() {
            state
                .instances
                .iter()
                .filter(|(_, instance)| {
                    instance.token_interests.values().any(|interest| {
                        interest.up_token_id == token_id || interest.down_token_id == token_id
                    })
                })
                .map(|(instance_id, _)| instance_id.clone())
                .collect::<Vec<_>>()
        } else {
            position_owners
        };
        if candidate_owners.len() != 1 {
            reject(
                &mut state,
                format!(
                    "authenticated historical trade `{trade_key}` token `{token_id}` has {} candidate instance owners",
                    candidate_owners.len(),
                ),
            );
            self.schedule_trade_persist(&state, trade_key, "", order_id, token_id);
            return TradeTransitionResult::Rejected;
        }

        let ownership = TradeOwnership {
            account_id: self.account_id.clone(),
            instance_id: candidate_owners[0].clone(),
            trade_key: trade_key.to_string(),
            client_order_id: String::new(),
            order_id: order_id.to_string(),
            token_id: token_id.to_string(),
            side,
            quantity,
            price,
            status: normalized,
        };
        let retired_at_ms = wall_clock_ms();
        state.retired_trade_ownership_tombstones.insert(
            trade_key.to_string(),
            RetiredTradeOwnershipTombstone {
                ownership: ownership.clone(),
                is_maker: Some(is_maker),
                authenticated_terminal_noop: true,
                retired_at_ms,
            },
        );
        prune_retired_trade_ownership_tombstones(&mut state, retired_at_ms);
        self.retired_trade_tombstone_count_fast.store(
            state.retired_trade_ownership_tombstones.len(),
            Ordering::Relaxed,
        );
        let was_uncertain = state.uncertain;
        state.ownership_anomalies.remove(&anomaly_key);
        state.unresolved_trade_match_times.remove(trade_key);
        self.unresolved_trade_keys.remove(trade_key);
        state.verified_trade_replay_recoveries =
            state.verified_trade_replay_recoveries.saturating_add(1);
        recompute_reconciliation(&mut state, "authenticated terminal historical trade no-op");
        let reopened = was_uncertain && !state.uncertain;
        log::info!(
            "[shared_account] authenticated terminal trade recovered as durable no-op account={} trade={} oid={} token={} instance={} maker={} reopened_admission={}",
            self.account_id,
            trade_key,
            order_id,
            token_id,
            ownership.instance_id,
            is_maker,
            reopened,
        );
        // This rare historical path also prunes expired ownership tombstones;
        // retain the cold full-state fallback so those removals are durable.
        self.schedule_persist(&state);
        drop(state);
        self.track_trade_persistence_generation();
        TradeTransitionResult::OwnedNoop(ownership)
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_trade_transition_virtual(
        &self,
        trade_key: &str,
        normalized: &str,
        lifecycle_rank: u8,
        client_order_id: &str,
        order_id: &str,
        token_id: &str,
        side: Side,
        quantity: f64,
        price: f64,
        trade_context: Option<(bool, u64)>,
        instance_id: &str,
    ) -> VirtualTradeAttempt {
        if trade_key.is_empty()
            || !quantity.is_finite()
            || quantity <= 0.0
            || !price.is_finite()
            || price <= 0.0
            || price > 1.0 + 1e-8
        {
            return VirtualTradeAttempt::Fallback;
        }
        let Some(account) = self.virtual_account(instance_id) else {
            return VirtualTradeAttempt::Fallback;
        };
        let mut lifecycle = account.lifecycle.lock().unwrap();
        let normalized_order_id = normalize_order_id(order_id);
        let existing = lifecycle.trades.get(trade_key).cloned();

        // A terminal/non-advancing replay is resolved by the trade id before
        // consulting its parent order. Settled cleanup may retire order roots
        // independently while the active trade proof is still present.
        if let Some(applied) = existing.as_ref() {
            let prior_rank = match applied.ownership.status.as_str() {
                "MATCHED" => 1,
                "MINED" => 2,
                "CONFIRMED" | "FAILED" => 3,
                _ => 0,
            };
            if applied.failed
                || applied.ownership.status == "CONFIRMED"
                || lifecycle_rank <= prior_rank
            {
                if validate_owned_trade_replay(
                    &applied.ownership,
                    client_order_id,
                    order_id,
                    token_id,
                    side,
                    quantity,
                    price,
                )
                .is_err()
                {
                    return VirtualTradeAttempt::Fallback;
                }
                let mut changed = false;
                if let Some((is_maker, match_time_secs)) = trade_context {
                    if applied.is_maker.is_some_and(|stored| stored != is_maker) {
                        return VirtualTradeAttempt::Fallback;
                    }
                    if let Some(trade) = lifecycle.trades.get_mut(trade_key) {
                        if trade.match_time_secs < match_time_secs {
                            trade.match_time_secs = match_time_secs;
                            changed = true;
                        }
                    }
                    let config = (!is_maker)
                        .then(|| {
                            self.token_fee_configs_fast
                                .read()
                                .unwrap()
                                .get(token_id)
                                .cloned()
                        })
                        .flatten();
                    match apply_trade_fee_transition_virtual(
                        &account,
                        &mut lifecycle,
                        trade_key,
                        if normalized == "FAILED" {
                            OrderStatus::Failed
                        } else {
                            OrderStatus::PartiallyFilled
                        },
                        is_maker,
                        config.as_ref(),
                    ) {
                        Ok(fee_changed) => changed |= fee_changed,
                        Err(_) => return VirtualTradeAttempt::Fallback,
                    }
                    if lifecycle.fee_attribution_pending.contains(trade_key) {
                        self.mark_virtual_fee_pending();
                    }
                }
                if changed {
                    Self::record_virtual_trade_mutation(
                        &account,
                        &mut lifecycle,
                        trade_key,
                        &applied.ownership.client_order_id,
                        token_id,
                    );
                    self.schedule_virtual_trade_persist(
                        &account,
                        &lifecycle,
                        trade_key,
                        &applied.ownership.client_order_id,
                        token_id,
                    );
                }
                return VirtualTradeAttempt::OwnedNoop(applied.ownership.clone());
            }
        }

        let resolved_coid = if !client_order_id.is_empty() {
            client_order_id.to_string()
        } else {
            lifecycle
                .orders
                .iter()
                .find(|(_, order)| normalize_order_id(&order.order_id) == normalized_order_id)
                .map(|(coid, _)| coid.clone())
                .unwrap_or_default()
        };
        let Some(order) = lifecycle.orders.get(&resolved_coid).cloned() else {
            return VirtualTradeAttempt::Fallback;
        };
        if order.instance_id != instance_id
            || normalized_order_id.is_empty()
            || normalize_order_id(&order.order_id) != normalized_order_id
            || order.token_id != token_id
            || order.side != side
        {
            return VirtualTradeAttempt::Fallback;
        }
        if let Some(applied) = existing.as_ref() {
            if validate_owned_trade_replay(
                &applied.ownership,
                &resolved_coid,
                order_id,
                token_id,
                side,
                quantity,
                price,
            )
            .is_err()
            {
                return VirtualTradeAttempt::Fallback;
            }
            if let Some((is_maker, _)) = trade_context {
                if applied.is_maker.is_some_and(|stored| stored != is_maker) {
                    return VirtualTradeAttempt::Fallback;
                }
                let config = (!is_maker)
                    .then(|| {
                        self.token_fee_configs_fast
                            .read()
                            .unwrap()
                            .get(token_id)
                            .cloned()
                    })
                    .flatten();
                if is_maker || config.is_some() {
                    let (expected_usdc, expected_shares) =
                        configured_fee_amounts(&applied.ownership, is_maker, config.as_ref());
                    if (applied.usdc_fee > EPS && (applied.usdc_fee - expected_usdc).abs() > EPS)
                        || (applied.shares_fee > EPS
                            && (applied.shares_fee - expected_shares).abs() > EPS)
                    {
                        return VirtualTradeAttempt::Fallback;
                    }
                }
            }
        }
        let quantity_tolerance = 1e-8_f64.max(order.quantity.abs() * 1e-8);
        if fill_violates_limit(side, order.price, price, quantity)
            || (existing.is_none()
                && order.filled_quantity + quantity > order.quantity + quantity_tolerance)
        {
            return VirtualTradeAttempt::Fallback;
        }

        // Claim the immutable trade-id owner only after every order invariant
        // has passed and before changing economics. The index has its own tiny
        // critical section and never touches the physical account ledger.
        {
            let mut routes = self.trade_routes.write_shard(trade_key);
            match routes.get(trade_key) {
                Some(owner) if owner != instance_id => return VirtualTradeAttempt::Fallback,
                Some(_) => {}
                None => {
                    routes.insert(trade_key.to_string(), instance_id.to_string());
                }
            }
        }

        let already_booked = existing.as_ref().is_some_and(|trade| trade.booked);
        let settlement_observed = existing.as_ref().is_some_and(|trade| trade.physical_booked);
        let is_failed = normalized == "FAILED";
        let reaches_settlement = matches!(normalized, "MINED" | "CONFIRMED");
        let should_book = !is_failed && !already_booked;
        let should_mark_settled = !is_failed && reaches_settlement && !settlement_observed;
        let should_reverse = is_failed && already_booked;
        let should_reverse_settled = is_failed && settlement_observed;

        // A lifecycle-only advance still persists the new status, but it does
        // not mutate the global physical wallet view. Wallet snapshots and the
        // reconcile worker own that control-plane state.
        if existing.is_some()
            && !should_book
            && !should_mark_settled
            && !should_reverse
            && !should_reverse_settled
        {
            let mut ownership = existing
                .as_ref()
                .expect("existing checked above")
                .ownership
                .clone();
            if let Some(trade) = lifecycle.trades.get_mut(trade_key) {
                if !trade.failed {
                    trade.ownership.status = normalized.to_string();
                    ownership.status = normalized.to_string();
                }
                if let Some((_, match_time_secs)) = trade_context {
                    trade.match_time_secs = trade.match_time_secs.max(match_time_secs);
                }
            }
            if let Some((is_maker, _)) = trade_context {
                let config = (!is_maker)
                    .then(|| {
                        self.token_fee_configs_fast
                            .read()
                            .unwrap()
                            .get(token_id)
                            .cloned()
                    })
                    .flatten();
                if apply_trade_fee_transition_virtual(
                    &account,
                    &mut lifecycle,
                    trade_key,
                    if is_failed {
                        OrderStatus::Failed
                    } else {
                        OrderStatus::PartiallyFilled
                    },
                    is_maker,
                    config.as_ref(),
                )
                .is_err()
                {
                    return VirtualTradeAttempt::Fallback;
                }
                if lifecycle.fee_attribution_pending.contains(trade_key) {
                    self.mark_virtual_fee_pending();
                }
            }
            Self::record_virtual_trade_mutation(
                &account,
                &mut lifecycle,
                trade_key,
                &resolved_coid,
                token_id,
            );
            self.schedule_virtual_trade_persist(
                &account,
                &lifecycle,
                trade_key,
                &resolved_coid,
                token_id,
            );
            return VirtualTradeAttempt::Applied(ownership);
        }

        if should_book || should_reverse {
            let sign = if side == Side::Buy { 1.0 } else { -1.0 };
            let multiplier = if should_reverse { -1.0 } else { 1.0 };
            account.cash.add(-sign * quantity * price * multiplier);
            account
                .position(token_id)
                .balance
                .add(sign * quantity * multiplier);
        }

        let mut order_fully_filled = false;
        if should_book {
            let (reservation_token, cash_delta, quantity_delta) = {
                let order = lifecycle
                    .orders
                    .get_mut(&resolved_coid)
                    .expect("owned order checked above");
                let cancellation_audit_pending = order.status == OrderStatus::Cancelled;
                let old_cash = order.reserved_cash;
                let old_quantity = order.reserved_quantity;
                order.filled_quantity = (order.filled_quantity + quantity).min(order.quantity);
                let fill_target = order
                    .terminal_matched_quantity
                    .unwrap_or(order.quantity)
                    .min(order.quantity);
                if order.filled_quantity + EPS >= fill_target {
                    if order.terminal_matched_quantity.is_none() && !cancellation_audit_pending {
                        order.status = OrderStatus::Filled;
                    }
                    order_fully_filled = true;
                } else if order.terminal_matched_quantity.is_none() && !cancellation_audit_pending {
                    order.status = OrderStatus::PartiallyFilled;
                }
                let (desired_cash, desired_quantity) = desired_order_reservation(order);
                order.reserved_cash = desired_cash;
                order.reserved_quantity = desired_quantity;
                (
                    order.token_id.clone(),
                    desired_cash - old_cash,
                    desired_quantity - old_quantity,
                )
            };
            account.adjust_reservation(&reservation_token, cash_delta, quantity_delta);
        }

        if is_failed {
            let (reservation_token, cash_delta, quantity_delta, recovery_pending) = {
                let order = lifecycle
                    .orders
                    .get_mut(&resolved_coid)
                    .expect("owned order checked above");
                if should_reverse {
                    order.filled_quantity = (order.filled_quantity - quantity).max(0.0);
                    if order.status == OrderStatus::Cancelled
                        && !order.terminal_trade_ids_authoritative
                    {
                        if let Some(target) = order.terminal_matched_quantity.as_mut() {
                            *target = (*target - quantity).max(order.filled_quantity);
                        }
                    }
                }
                let cancellation_audit_pending = order.status == OrderStatus::Cancelled
                    && order.terminal_matched_quantity.is_none();
                let off_book = order.status == OrderStatus::Rejected;
                if should_reverse && !off_book && order.status != OrderStatus::Cancelled {
                    order.status = if order.filled_quantity > EPS {
                        OrderStatus::PartiallyFilled
                    } else {
                        OrderStatus::Accepted
                    };
                }
                let (desired_cash, desired_quantity) = if off_book {
                    (0.0, 0.0)
                } else {
                    desired_order_reservation(order)
                };
                let cash_delta = desired_cash - order.reserved_cash;
                let quantity_delta = desired_quantity - order.reserved_quantity;
                order.reserved_cash = desired_cash;
                order.reserved_quantity = desired_quantity;
                (
                    order.token_id.clone(),
                    cash_delta,
                    quantity_delta,
                    cancellation_audit_pending
                        || (order.status == OrderStatus::Cancelled
                            && (desired_cash > EPS || desired_quantity > EPS)),
                )
            };
            account.adjust_reservation(&reservation_token, cash_delta, quantity_delta);
            if recovery_pending {
                lifecycle
                    .recovery_pending_orders
                    .insert(resolved_coid.clone());
            } else {
                lifecycle.recovery_pending_orders.remove(&resolved_coid);
            }
        }

        let ownership = TradeOwnership {
            account_id: self.account_id.clone(),
            instance_id: instance_id.to_string(),
            trade_key: trade_key.to_string(),
            client_order_id: resolved_coid.clone(),
            order_id: order_id.to_string(),
            token_id: token_id.to_string(),
            side,
            quantity,
            price,
            status: normalized.to_string(),
        };
        lifecycle.trades.insert(
            trade_key.to_string(),
            AppliedTrade {
                ownership: ownership.clone(),
                booked: should_book || (already_booked && !should_reverse),
                // Retain the durable field for schema compatibility; it now
                // means settlement finality observed, not that a trade thread
                // directly mutated the account-global physical totals.
                physical_booked: should_mark_settled
                    || (settlement_observed && !should_reverse_settled),
                usdc_fee: existing.as_ref().map_or(0.0, |trade| trade.usdc_fee),
                shares_fee: existing.as_ref().map_or(0.0, |trade| trade.shares_fee),
                virtual_fee_booked: existing
                    .as_ref()
                    .is_some_and(|trade| trade.virtual_fee_booked),
                physical_fee_booked: existing
                    .as_ref()
                    .is_some_and(|trade| trade.physical_fee_booked)
                    && !should_reverse_settled,
                failed: is_failed,
                failure_reconciled: is_failed
                    || existing
                        .as_ref()
                        .is_some_and(|trade| trade.failure_reconciled),
                is_maker: trade_context
                    .map(|(is_maker, _)| is_maker)
                    .or_else(|| existing.as_ref().and_then(|trade| trade.is_maker)),
                match_time_secs: trade_context.map_or_else(
                    || existing.as_ref().map_or(0, |trade| trade.match_time_secs),
                    |(_, match_time_secs)| match_time_secs,
                ),
                ledger_generation: existing.as_ref().map_or(0, |trade| trade.ledger_generation),
            },
        );

        let mut fee_economics_changed = false;
        if let Some((is_maker, _)) = trade_context {
            let config = (!is_maker)
                .then(|| {
                    self.token_fee_configs_fast
                        .read()
                        .unwrap()
                        .get(token_id)
                        .cloned()
                })
                .flatten();
            match apply_trade_fee_transition_virtual(
                &account,
                &mut lifecycle,
                trade_key,
                if is_failed {
                    OrderStatus::Failed
                } else {
                    OrderStatus::PartiallyFilled
                },
                is_maker,
                config.as_ref(),
            ) {
                Ok(changed) => fee_economics_changed = changed,
                Err(_) => return VirtualTradeAttempt::Fallback,
            }
        } else {
            lifecycle
                .fee_attribution_pending
                .insert(trade_key.to_string());
            self.mark_virtual_fee_pending();
        }

        if terminal_order_audit_complete_virtual(&lifecycle, &resolved_coid) {
            release_virtual_order_reservation(&account, &mut lifecycle, &resolved_coid);
            lifecycle.recovery_pending_orders.remove(&resolved_coid);
        } else if order_fully_filled
            && !lifecycle
                .orders
                .get(&resolved_coid)
                .is_some_and(|order| order.terminal_trade_ids_authoritative)
        {
            lifecycle.recovery_pending_orders.remove(&resolved_coid);
        }
        if should_book || should_reverse || fee_economics_changed {
            let generation = self
                .ledger_generation_fast
                .fetch_add(1, Ordering::AcqRel)
                .saturating_add(1)
                .max(1);
            if let Some(trade) = lifecycle.trades.get_mut(trade_key) {
                trade.ledger_generation = generation;
            }
        }
        Self::record_virtual_trade_mutation(
            &account,
            &mut lifecycle,
            trade_key,
            &resolved_coid,
            token_id,
        );
        self.schedule_virtual_trade_persist(
            &account,
            &lifecycle,
            trade_key,
            &resolved_coid,
            token_id,
        );
        VirtualTradeAttempt::Applied(ownership)
    }

    fn apply_trade_transition_inner(
        &self,
        trade_key: &str,
        status: &str,
        client_order_id: &str,
        order_id: &str,
        token_id: &str,
        side: Side,
        quantity: f64,
        price: f64,
        trade_context: Option<(bool, u64)>,
        persistence_required: &mut bool,
        owned_noop: &mut bool,
    ) -> Option<TradeOwnership> {
        let mut normalized = status
            .trim_start_matches("TRADE_STATUS_")
            .to_ascii_uppercase();
        if normalized == "MATCHED_NOT_BROADCASTED" {
            normalized = "MATCHED".to_string();
        }
        if normalized == "RETRYING" {
            return None;
        }
        let lifecycle_rank = match normalized.as_str() {
            "MATCHED" => 1,
            "MINED" => 2,
            "CONFIRMED" | "FAILED" => 3,
            _ => return None,
        };
        let coid_scope = self.coid_routes.get(client_order_id);
        let oid_scope = self.oid_routes.get(&normalize_order_id(order_id));
        let trade_scope = self.trade_routes.get(trade_key);
        let anomalous_trade = self
            .anomalous_trade_keys
            .read()
            .unwrap()
            .contains(trade_key);
        // A cross-instance runtime/durable binding disagreement is an anomaly,
        // not a valid single-shard transition. Materialize the cold aggregate
        // only for that rare reject path so the original conflict proof is
        // preserved; ordinary owned trades still sync one virtual account.
        let mut routed_instances = [trade_scope, coid_scope, oid_scope].into_iter().flatten();
        let first_scope = routed_instances.next();
        let route_conflict = first_scope.as_ref().is_some_and(|first| {
            routed_instances.any(|candidate| candidate.as_str() != first.as_str())
        });
        let instance_scope = if anomalous_trade || route_conflict {
            None
        } else {
            first_scope
        };
        if let Some(instance_id) = instance_scope.as_deref() {
            match self.apply_trade_transition_virtual(
                trade_key,
                &normalized,
                lifecycle_rank,
                client_order_id,
                order_id,
                token_id,
                side,
                quantity,
                price,
                trade_context,
                instance_id,
            ) {
                VirtualTradeAttempt::Applied(ownership) => {
                    *persistence_required = true;
                    return Some(ownership);
                }
                VirtualTradeAttempt::OwnedNoop(ownership) => {
                    *owned_noop = true;
                    return Some(ownership);
                }
                VirtualTradeAttempt::Fallback => {}
            }
        }
        let mut state = if let Some(instance_id) = instance_scope.as_deref() {
            self.lock_state_for_instance(instance_id)
        } else {
            self.lock_state()
        };
        let schedule_trade_persist = |state: &SharedAccountState| {
            self.schedule_trade_persist(state, trade_key, client_order_id, order_id, token_id);
        };
        let anomaly_key = if trade_key.is_empty() {
            format!("trade:<missing>:{order_id}")
        } else {
            format!("trade:{trade_key}")
        };
        if trade_key.is_empty()
            || !quantity.is_finite()
            || quantity <= 0.0
            || !price.is_finite()
            || price <= 0.0
            || price > 1.0 + 1e-8
        {
            set_ownership_anomaly(
                &mut state,
                anomaly_key,
                format!(
                    "invalid trade `{trade_key}` numeric payload quantity={quantity} price={price}"
                ),
            );
            schedule_trade_persist(&state);
            return None;
        }
        let normalized_order_id = normalize_order_id(order_id);
        let existing = state.trades.get(trade_key).cloned();

        // Resolve already-applied durable rows before consulting order maps.
        // Settled cleanup may retire oid/coid roots independently; replay
        // idempotence must therefore be rooted in the trade id itself.
        if let Some(applied) = existing.as_ref() {
            let prior_rank = match applied.ownership.status.as_str() {
                "MATCHED" => 1,
                "MINED" => 2,
                "CONFIRMED" | "FAILED" => 3,
                _ => 0,
            };
            if applied.failed
                || applied.ownership.status == "CONFIRMED"
                || lifecycle_rank <= prior_rank
            {
                if let Err(reason) = validate_owned_trade_replay(
                    &applied.ownership,
                    client_order_id,
                    order_id,
                    token_id,
                    side,
                    quantity,
                    price,
                ) {
                    set_ownership_anomaly(&mut state, anomaly_key.clone(), reason);
                    schedule_trade_persist(&state);
                    return None;
                }
                let was_uncertain = state.uncertain;
                let recovered = state.ownership_anomalies.remove(&anomaly_key).is_some();
                let mut changed = recovered;
                if let Some((is_maker, match_time_secs)) = trade_context {
                    let role_changed = applied.is_maker != Some(is_maker);
                    let match_time_changed = applied.match_time_secs < match_time_secs;
                    let fee_pending = state.fee_attribution_pending.contains(trade_key);
                    if let Some(trade) = state.trades.get_mut(trade_key) {
                        if match_time_changed {
                            trade.match_time_secs = match_time_secs;
                        }
                    }
                    changed |= match_time_changed;
                    if role_changed || fee_pending {
                        let uncertainty_before = (
                            state.uncertain,
                            state.uncertain_reason.clone(),
                            state.uncertain_since_ms,
                        );
                        let fee_status = if normalized == "FAILED" {
                            OrderStatus::Failed
                        } else {
                            OrderStatus::PartiallyFilled
                        };
                        let fee_changed = apply_configured_trade_fee_locked(
                            &mut state, trade_key, fee_status, is_maker,
                        );
                        let uncertainty_changed = uncertainty_before
                            != (
                                state.uncertain,
                                state.uncertain_reason.clone(),
                                state.uncertain_since_ms,
                            );
                        changed |= role_changed || fee_changed || uncertainty_changed;
                    }
                }
                if changed {
                    recompute_reconciliation(&mut state, "corrected trade ownership replay");
                    if recovered {
                        state.verified_trade_replay_recoveries =
                            state.verified_trade_replay_recoveries.saturating_add(1);
                        log::info!(
                            "[shared_account] verified durable trade replay account={} trade={} coid={} source=active reopened_admission={}",
                            self.account_id, trade_key,
                            applied.ownership.client_order_id,
                            was_uncertain && !state.uncertain,
                        );
                    }
                    schedule_trade_persist(&state);
                    *persistence_required = true;
                }
                *owned_noop = true;
                return Some(applied.ownership.clone());
            }
        }

        // Full terminal rows are economically compacted, but their seven-day
        // ownership tombstones survive. Exact identity/economic agreement is
        // sufficient proof that this replay is already applied.
        if existing.is_none() {
            let retired = state
                .retired_trade_ownership_tombstones
                .get(trade_key)
                .filter(|tombstone| retired_trade_tombstone_is_live(tombstone, wall_clock_ms()))
                .cloned();
            if let Some(tombstone) = retired {
                let validation = validate_owned_trade_replay(
                    &tombstone.ownership, client_order_id, order_id, token_id,
                    side, quantity, price,
                ).and_then(|()| {
                    if trade_context.is_some_and(|(is_maker, _)| {
                        tombstone.is_maker.is_some_and(|stored| stored != is_maker)
                    }) {
                        Err(format!(
                            "trade `{trade_key}` retired ownership role changed incoming_maker={} stored_maker={}",
                            trade_context.map(|(is_maker, _)| is_maker).unwrap_or(false),
                            tombstone.is_maker.unwrap_or(false),
                        ))
                    } else { Ok(()) }
                });
                if let Err(reason) = validation {
                    set_ownership_anomaly(&mut state, anomaly_key.clone(), reason);
                    schedule_trade_persist(&state);
                    return None;
                }
                let was_uncertain = state.uncertain;
                let recovered = state.ownership_anomalies.remove(&anomaly_key).is_some();
                if recovered {
                    recompute_reconciliation(&mut state, "verified retired trade ownership replay");
                    state.verified_trade_replay_recoveries =
                        state.verified_trade_replay_recoveries.saturating_add(1);
                    let reopened = was_uncertain && !state.uncertain;
                    log::info!(
                        "[shared_account] verified retired trade replay account={} trade={} coid={} reopened_admission={}",
                        self.account_id, trade_key,
                        tombstone.ownership.client_order_id, reopened,
                    );
                    schedule_trade_persist(&state);
                    *persistence_required = true;
                }
                *owned_noop = true;
                return Some(tombstone.ownership);
            }
        }

        let durable_coid = state.oid_to_coid.get(&normalized_order_id).cloned();
        if !client_order_id.is_empty()
            && durable_coid
                .as_deref()
                .is_some_and(|coid| coid != client_order_id)
        {
            set_ownership_anomaly(
                &mut state,
                anomaly_key.clone(),
                format!(
                    "trade `{trade_key}` ownership mapping conflict oid=`{order_id}` runtime_coid=`{client_order_id}` durable_coid=`{}`",
                    durable_coid.as_deref().unwrap_or_default(),
                ),
            );
            schedule_trade_persist(&state);
            return None;
        }
        let resolved_coid = durable_coid
            .or_else(|| (!client_order_id.is_empty()).then(|| client_order_id.to_string()))
            .unwrap_or_default();
        let Some(order) = state.orders.get(&resolved_coid).cloned() else {
            set_ownership_anomaly(
                &mut state,
                anomaly_key.clone(),
                format!("unowned trade `{trade_key}` coid=`{resolved_coid}` oid=`{order_id}`"),
            );
            schedule_trade_persist(&state);
            return None;
        };
        let stored_order_id = normalize_order_id(&order.order_id);
        if normalized_order_id.is_empty()
            || stored_order_id != normalized_order_id
            || order.token_id != token_id
            || order.side != side
        {
            set_ownership_anomaly(
                &mut state,
                anomaly_key.clone(),
                format!(
                    "trade `{trade_key}` order invariant mismatch coid=`{resolved_coid}` incoming=(oid=`{order_id}`,token=`{token_id}`,side={side:?}) stored=(oid=`{}`,token=`{}`,side={:?})",
                    order.order_id, order.token_id, order.side,
                ),
            );
            schedule_trade_persist(&state);
            return None;
        }
        let instance_id = order.instance_id;

        if let Some(applied) = existing.as_ref() {
            let prior = &applied.ownership;
            if prior.client_order_id != resolved_coid
                || normalize_order_id(&prior.order_id) != normalized_order_id
                || prior.token_id != token_id
                || prior.side != side
            {
                set_ownership_anomaly(
                    &mut state,
                    anomaly_key.clone(),
                    format!(
                        "trade `{trade_key}` lifecycle ownership changed incoming=(coid=`{resolved_coid}`,oid=`{order_id}`,token=`{token_id}`,side={side:?}) stored=(coid=`{}`,oid=`{}`,token=`{}`,side={:?})",
                        prior.client_order_id, prior.order_id, prior.token_id, prior.side,
                    ),
                );
                schedule_trade_persist(&state);
                return None;
            }
            let quantity_tolerance = 1e-8_f64.max(prior.quantity.abs() * 1e-8);
            let price_tolerance = fixed_point_trade_price_tolerance(prior.price, prior.quantity);
            if (prior.quantity - quantity).abs() > quantity_tolerance
                || (prior.price - price).abs() > price_tolerance
            {
                set_ownership_anomaly(
                    &mut state,
                    anomaly_key.clone(),
                    format!(
                        "trade `{trade_key}` lifecycle economics changed incoming=(quantity={quantity},price={price}) stored=(quantity={},price={})",
                        prior.quantity, prior.price,
                    ),
                );
                schedule_trade_persist(&state);
                return None;
            }
        }
        let violates_limit = fill_violates_limit(side, order.price, price, quantity);
        let quantity_tolerance = 1e-8_f64.max(order.quantity.abs() * 1e-8);
        let exceeds_order_quantity = existing.is_none()
            && order.filled_quantity + quantity > order.quantity + quantity_tolerance;
        if violates_limit || exceeds_order_quantity {
            set_ownership_anomaly(
                &mut state,
                anomaly_key.clone(),
                format!(
                    "trade `{trade_key}` violates owned order bounds side={side:?} fill=(quantity={quantity},price={price}) order=(filled={},quantity={},limit={})",
                    order.filled_quantity, order.quantity, order.price,
                ),
            );
            schedule_trade_persist(&state);
            return None;
        }
        state.ownership_anomalies.remove(&anomaly_key);
        recompute_reconciliation(&mut state, "corrected trade ownership replay");
        let already_booked = existing.as_ref().map(|trade| trade.booked).unwrap_or(false);
        let physical_booked = existing
            .as_ref()
            .map(|trade| trade.physical_booked)
            .unwrap_or(false);
        let is_failed = normalized == "FAILED";
        let reaches_physical = matches!(normalized.as_str(), "MINED" | "CONFIRMED");
        let should_book = !is_failed && !already_booked;
        let should_book_physical = !is_failed && reaches_physical && !physical_booked;
        let should_reverse = is_failed && already_booked;
        let should_reverse_physical = is_failed && physical_booked;
        if existing.is_some()
            && !should_book
            && !should_book_physical
            && !should_reverse
            && !should_reverse_physical
        {
            if let Some(applied) = state.trades.get_mut(trade_key) {
                if !applied.failed {
                    applied.ownership.status = normalized.clone();
                }
                let ownership = applied.ownership.clone();
                if let Some((_, match_time_secs)) = trade_context {
                    applied.match_time_secs = applied.match_time_secs.max(match_time_secs);
                }
                if let Some((is_maker, _)) = trade_context {
                    let fee_status = if normalized == "FAILED" {
                        OrderStatus::Failed
                    } else {
                        OrderStatus::PartiallyFilled
                    };
                    let _ = apply_configured_trade_fee_locked(
                        &mut state, trade_key, fee_status, is_maker,
                    );
                }
                schedule_trade_persist(&state);
                *persistence_required = true;
                return Some(ownership);
            }
        }

        if should_book || should_reverse {
            let sign = if side == Side::Buy { 1.0 } else { -1.0 };
            let cash_delta = -sign * quantity * price;
            let position_delta = sign * quantity;
            let multiplier = if should_reverse { -1.0 } else { 1.0 };
            if let Some(instance) = state.instances.get_mut(&instance_id) {
                instance.cash += cash_delta * multiplier;
                *instance.positions.entry(token_id.into()).or_insert(0.0) +=
                    position_delta * multiplier;
            }
        }
        // `physical_booked` below records settlement finality only. Even this
        // rare anomaly-recovery path leaves physical totals to wallet snapshots.
        let mut order_fully_filled = false;
        if should_book {
            let (cash_delta, qty_delta, reservation_token) = if let Some(order) =
                state.orders.get_mut(&resolved_coid)
            {
                let cancellation_audit_pending = order.status == OrderStatus::Cancelled;
                let old_cash = order.reserved_cash;
                let old_qty = order.reserved_quantity;
                order.filled_quantity = (order.filled_quantity + quantity).min(order.quantity);
                let fill_target = order
                    .terminal_matched_quantity
                    .unwrap_or(order.quantity)
                    .min(order.quantity);
                if order.filled_quantity + EPS >= fill_target {
                    if order.terminal_matched_quantity.is_none() && !cancellation_audit_pending {
                        order.status = OrderStatus::Filled;
                    }
                    order_fully_filled = true;
                } else if order.terminal_matched_quantity.is_none() && !cancellation_audit_pending {
                    order.status = OrderStatus::PartiallyFilled;
                }
                // Recompute from the effective remaining quantity after every
                // fill so the pro-rata BUY fee buffer is released too.
                let (desired_cash, desired_qty) = desired_order_reservation(order);
                order.reserved_cash = desired_cash;
                order.reserved_quantity = desired_qty;
                (
                    desired_cash - old_cash,
                    desired_qty - old_qty,
                    order.token_id.clone(),
                )
            } else {
                (0.0, 0.0, token_id.to_string())
            };
            if let Some(instance) = state.instances.get_mut(&instance_id) {
                instance.reserved_cash = (instance.reserved_cash + cash_delta).max(0.0);
                if qty_delta.abs() > EPS {
                    let reserved = instance
                        .reserved_positions
                        .entry(reservation_token)
                        .or_insert(0.0);
                    *reserved = (*reserved + qty_delta).max(0.0);
                }
            }
        }
        if is_failed {
            // FAILED is terminal for this trade, not for the parent order.
            // Restore the worst-case residual reservation; normal order
            // lifecycle/cancel handling proves when the parent is off-book.
            let reservation_delta = if let Some(order) = state.orders.get_mut(&resolved_coid) {
                if should_reverse {
                    order.filled_quantity = (order.filled_quantity - quantity).max(0.0);
                    // An authoritative cancellation's size_matched includes this
                    // trade while it is MATCHED. Once that trade reaches FAILED,
                    // it is terminal and can never consume the cancelled parent
                    // again, so remove it from the cancellation audit target.
                    // Other, not-yet-delivered trade legs remain represented by
                    // the residual target and therefore keep their reservation.
                    if order.status == OrderStatus::Cancelled
                        && !order.terminal_trade_ids_authoritative
                    {
                        if let Some(target) = order.terminal_matched_quantity.as_mut() {
                            *target = (*target - quantity).max(order.filled_quantity);
                        }
                    }
                }
                let cancellation_audit_pending = order.status == OrderStatus::Cancelled
                    && order.terminal_matched_quantity.is_none();
                let off_book = order.status == OrderStatus::Rejected;
                if should_reverse && !off_book && order.status != OrderStatus::Cancelled {
                    order.status = if order.filled_quantity > EPS {
                        OrderStatus::PartiallyFilled
                    } else {
                        OrderStatus::Accepted
                    };
                }
                let (desired_cash, desired_qty) = if off_book {
                    (0.0, 0.0)
                } else {
                    desired_order_reservation(order)
                };
                let cash_delta = desired_cash - order.reserved_cash;
                let qty_delta = desired_qty - order.reserved_quantity;
                order.reserved_cash = desired_cash;
                order.reserved_quantity = desired_qty;
                let recovery_pending = cancellation_audit_pending
                    || (order.status == OrderStatus::Cancelled
                        && (desired_cash > EPS || desired_qty > EPS));
                (
                    cash_delta,
                    qty_delta,
                    order.token_id.clone(),
                    recovery_pending,
                )
            } else {
                (0.0, 0.0, token_id.to_string(), false)
            };
            if let Some(instance) = state.instances.get_mut(&instance_id) {
                instance.reserved_cash = (instance.reserved_cash + reservation_delta.0).max(0.0);
                if reservation_delta.1.abs() > EPS {
                    let reserved = instance
                        .reserved_positions
                        .entry(reservation_delta.2)
                        .or_insert(0.0);
                    *reserved = (*reserved + reservation_delta.1).max(0.0);
                }
            }
            if reservation_delta.3 {
                state.recovery_pending_orders.insert(resolved_coid.clone());
            } else {
                state.recovery_pending_orders.remove(&resolved_coid);
            }
        }
        let ownership = TradeOwnership {
            account_id: self.account_id.clone(),
            instance_id,
            trade_key: trade_key.into(),
            client_order_id: resolved_coid.clone(),
            order_id: order_id.into(),
            token_id: token_id.into(),
            side,
            quantity,
            price,
            status: normalized,
        };
        state.trades.insert(
            trade_key.into(),
            AppliedTrade {
                ownership: ownership.clone(),
                booked: should_book || (already_booked && !should_reverse),
                physical_booked: should_book_physical
                    || (physical_booked && !should_reverse_physical),
                usdc_fee: existing.as_ref().map(|trade| trade.usdc_fee).unwrap_or(0.0),
                shares_fee: existing
                    .as_ref()
                    .map(|trade| trade.shares_fee)
                    .unwrap_or(0.0),
                virtual_fee_booked: existing
                    .as_ref()
                    .is_some_and(|trade| trade.virtual_fee_booked),
                physical_fee_booked: existing
                    .as_ref()
                    .is_some_and(|trade| trade.physical_fee_booked),
                failed: is_failed,
                failure_reconciled: is_failed
                    || existing
                        .as_ref()
                        .is_some_and(|trade| trade.failure_reconciled),
                is_maker: trade_context
                    .map(|(maker, _)| maker)
                    .or_else(|| existing.as_ref().and_then(|trade| trade.is_maker)),
                match_time_secs: trade_context.map(|(_, ts)| ts).unwrap_or_else(|| {
                    existing
                        .as_ref()
                        .map(|trade| trade.match_time_secs)
                        .unwrap_or(0)
                }),
                ledger_generation: existing
                    .as_ref()
                    .map(|trade| trade.ledger_generation)
                    .unwrap_or(0),
            },
        );
        let exact_terminal_audit_complete =
            terminal_order_audit_complete_locked(&state, &resolved_coid);
        let has_exact_terminal_audit = state
            .orders
            .get(&resolved_coid)
            .is_some_and(|order| order.terminal_trade_ids_authoritative);
        if exact_terminal_audit_complete {
            release_order_reservation_locked(&mut state, &resolved_coid);
            state.recovery_pending_orders.remove(&resolved_coid);
        } else if order_fully_filled && !has_exact_terminal_audit {
            state.recovery_pending_orders.remove(&resolved_coid);
        }
        if state
            .trades
            .get(trade_key)
            .is_some_and(|trade| trade.is_maker.is_none())
        {
            // Legacy/two-phase callers are allowed to establish ownership
            // before they know liquidity role, but that intermediate state is
            // durable and admission-blocking until `apply_configured_trade_fee`
            // supplies the role. It is never a healthy, restart-invalid row.
            state.fee_attribution_pending.insert(trade_key.to_string());
        }
        if let Some((is_maker, _)) = trade_context {
            let fee_status = if is_failed {
                OrderStatus::Failed
            } else {
                OrderStatus::PartiallyFilled
            };
            let _ = apply_configured_trade_fee_locked(&mut state, trade_key, fee_status, is_maker);
        }
        if should_book || should_reverse {
            advance_trade_ledger_generation(&mut state, trade_key);
        }
        recompute_reconciliation(&mut state, "trade lifecycle transition");
        schedule_trade_persist(&state);
        *persistence_required = true;
        Some(ownership)
    }

    /// Apply a private trade's fee from the durable token curve. Maker fills
    /// are explicitly zero-fee. A missing taker curve is sticky risk-off and
    /// is retried automatically by `register_token_fee_config`.
    pub fn apply_configured_trade_fee(
        &self,
        trade_key: &str,
        status: OrderStatus,
        is_maker: bool,
    ) -> bool {
        let mut state = self.lock_state();
        let Some(existing) = state.trades.get(trade_key).cloned() else {
            set_uncertain(
                &mut state,
                format!("fee attribution missing owned trade `{trade_key}`"),
            );
            self.schedule_persist(&state);
            return false;
        };
        if existing.is_maker.is_some_and(|stored| stored != is_maker) {
            set_uncertain(
                &mut state,
                format!(
                    "trade role replay mismatch trade={trade_key} stored_maker={:?} replay_maker={is_maker}",
                    existing.is_maker,
                ),
            );
            self.schedule_persist(&state);
            return false;
        }
        if let Some(trade) = state.trades.get_mut(trade_key) {
            trade.is_maker = Some(is_maker);
        }
        let config = (!is_maker)
            .then(|| {
                state
                    .token_fee_configs
                    .get(&existing.ownership.token_id)
                    .cloned()
            })
            .flatten();
        if !is_maker && config.is_none() {
            state.fee_attribution_pending.insert(trade_key.to_string());
            recompute_reconciliation(&mut state, "missing token fee config");
            self.schedule_persist(&state);
            return false;
        }
        self.schedule_persist(&state);
        drop(state);

        let notional = config.map_or(0.0, |config| {
            let price = existing.ownership.price.clamp(0.0, 1.0);
            existing.ownership.quantity
                * config.rate
                * (price * (1.0 - price)).max(0.0).powf(config.exponent)
        });
        let (usdc_fee, shares_fee) = if is_maker {
            (0.0, 0.0)
        } else if existing.ownership.side == Side::Buy {
            let shares = if existing.ownership.price > EPS {
                notional / existing.ownership.price
            } else {
                0.0
            };
            (0.0, shares)
        } else {
            (notional, 0.0)
        };
        self.apply_trade_fee_transition(trade_key, status, usdc_fee, shares_fee)
    }

    /// Attach the strategy-resolved taker fee to an already-owned private
    /// trade. Virtual risk changes at MATCHED and FAILED reverses it. Physical
    /// totals are updated only by authoritative wallet snapshots/reconciliation.
    pub fn apply_trade_fee_transition(
        &self,
        trade_key: &str,
        status: OrderStatus,
        usdc_fee: f64,
        shares_fee: f64,
    ) -> bool {
        if trade_key.is_empty()
            || !usdc_fee.is_finite()
            || usdc_fee < 0.0
            || !shares_fee.is_finite()
            || shares_fee < 0.0
        {
            return false;
        }
        let mut state = self.lock_state();
        let Some(existing) = state.trades.get(trade_key).cloned() else {
            set_uncertain(
                &mut state,
                format!("fee attribution missing owned trade `{trade_key}`"),
            );
            self.schedule_persist(&state);
            return false;
        };
        let Some(is_maker) = existing.is_maker else {
            state.fee_attribution_pending.insert(trade_key.to_string());
            recompute_reconciliation(&mut state, "trade role attribution pending");
            self.schedule_persist(&state);
            return false;
        };
        let config = (!is_maker)
            .then(|| state.token_fee_configs.get(&existing.ownership.token_id))
            .flatten();
        if !is_maker && config.is_none() {
            state.fee_attribution_pending.insert(trade_key.to_string());
            recompute_reconciliation(&mut state, "missing token fee config");
            self.schedule_persist(&state);
            return false;
        }
        let (expected_usdc, expected_shares) =
            configured_fee_amounts(&existing.ownership, is_maker, config);
        if (usdc_fee - expected_usdc).abs()
            > reconciliation_tolerance(usdc_fee, expected_usdc).max(EPS)
            || (shares_fee - expected_shares).abs()
                > reconciliation_tolerance(shares_fee, expected_shares).max(EPS)
        {
            set_uncertain(
                &mut state,
                format!(
                    "trade fee disagrees with durable role/curve trade={trade_key} incoming=({usdc_fee:.8},{shares_fee:.8}) expected=({expected_usdc:.8},{expected_shares:.8})"
                ),
            );
            self.schedule_persist(&state);
            return false;
        }
        let fee_changed = (existing.usdc_fee > EPS && (existing.usdc_fee - usdc_fee).abs() > EPS)
            || (existing.shares_fee > EPS && (existing.shares_fee - shares_fee).abs() > EPS);
        if fee_changed {
            set_uncertain(
                &mut state,
                format!(
                    "fee replay mismatch trade={trade_key} stored=({:.8},{:.8}) replay=({usdc_fee:.8},{shares_fee:.8})",
                    existing.usdc_fee, existing.shares_fee,
                ),
            );
            self.schedule_persist(&state);
            return false;
        }

        let effective_usdc_fee = if existing.usdc_fee > EPS {
            existing.usdc_fee
        } else {
            usdc_fee
        };
        let effective_shares_fee = if existing.shares_fee > EPS {
            existing.shares_fee
        } else {
            shares_fee
        };
        let upgrades_zero_fee = existing.usdc_fee <= EPS
            && existing.shares_fee <= EPS
            && (effective_usdc_fee > EPS || effective_shares_fee > EPS);
        let is_failed = status == OrderStatus::Failed || existing.failed;
        let book_virtual = !is_failed && (!existing.virtual_fee_booked || upgrades_zero_fee);
        let reverse_virtual = is_failed && existing.virtual_fee_booked;
        let multiplier = |book: bool, reverse: bool| {
            if book {
                1.0
            } else if reverse {
                -1.0
            } else {
                0.0
            }
        };
        let virtual_multiplier = multiplier(book_virtual, reverse_virtual);
        if virtual_multiplier != 0.0 {
            if let Some(instance) = state.instances.get_mut(&existing.ownership.instance_id) {
                instance.cash -= effective_usdc_fee * virtual_multiplier;
                *instance
                    .positions
                    .entry(existing.ownership.token_id.clone())
                    .or_insert(0.0) -= effective_shares_fee * virtual_multiplier;
            }
        }
        let settlement_observed = !is_failed && existing.physical_booked;
        if let Some(trade) = state.trades.get_mut(trade_key) {
            trade.usdc_fee = effective_usdc_fee;
            trade.shares_fee = effective_shares_fee;
            trade.virtual_fee_booked =
                book_virtual || (existing.virtual_fee_booked && !reverse_virtual);
            trade.physical_fee_booked = settlement_observed;
        }
        if virtual_multiplier != 0.0 {
            advance_trade_ledger_generation(&mut state, trade_key);
        }
        state.fee_attribution_pending.remove(trade_key);
        recompute_reconciliation(&mut state, "trade fee lifecycle transition");
        self.schedule_persist(&state);
        true
    }

    /// Apply one confirmed aggregate split and preserve per-instance intent
    /// attribution: amount cash becomes the same amount of each outcome token.
    pub fn apply_split_allocations(
        &self,
        up_token: &str,
        down_token: &str,
        allocations: &HashMap<String, f64>,
    ) -> Result<(), ReservationError> {
        self.reserve_split_allocations(allocations)?;
        match self.confirm_reserved_split(up_token, down_token, allocations) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.release_split_allocations(allocations);
                Err(error)
            }
        }
    }

    pub fn trades(&self) -> Vec<TradeOwnership> {
        self.lock_state()
            .trades
            .values()
            .map(|trade| trade.ownership.clone())
            .collect()
    }

    /// Resolve either a live durable trade row or a still-live compacted
    /// ownership tombstone. This is used before role classification so a
    /// delayed taker replay remains attributable after oid mappings retire.
    pub fn trade_ownership(&self, trade_key: &str) -> Option<TradeOwnership> {
        if trade_key.is_empty() {
            return None;
        }
        if let Some(instance_id) = self.trade_routes.get(trade_key) {
            if let Some(account) = self.virtual_account(&instance_id) {
                if let Some(ownership) = account
                    .lifecycle
                    .lock()
                    .unwrap()
                    .trades
                    .get(trade_key)
                    .map(|trade| trade.ownership.clone())
                {
                    return Some(ownership);
                }
            }
        }
        // Compacted terminal rows intentionally no longer live in an
        // instance shard. Only that historical tombstone fallback pays for a
        // cold aggregate transaction; ordinary terminal private updates are
        // served by the route + one lifecycle shard above.
        let state = self.lock_state();
        state
            .trades
            .get(trade_key)
            .map(|trade| trade.ownership.clone())
            .or_else(|| {
                let tombstone = state.retired_trade_ownership_tombstones.get(trade_key)?;
                retired_trade_tombstone_is_live(tombstone, wall_clock_ms())
                    .then(|| tombstone.ownership.clone())
            })
    }

    /// Non-blocking terminal high-water lookup for the authenticated private
    /// owner route. A contended shard is deliberately reported as "not yet
    /// resolved": routing the event again is safe because StrategyAccount and
    /// the cold ledger both deduplicate by trade id, whereas waiting for the
    /// cold lifecycle writer would add its scheduling tail to private apply.
    pub fn trade_status_matches_nonblocking(&self, trade_key: &str, status: &str) -> bool {
        if trade_key.is_empty() || status.is_empty() {
            return false;
        }
        let instance_id = match self.trade_routes.try_get(trade_key) {
            Ok(instance_id) => instance_id,
            Err(()) => return false,
        };
        if let Some(instance_id) = instance_id {
            let account = match self.virtual_accounts.try_read() {
                Ok(accounts) => accounts.get(&instance_id).cloned(),
                Err(_) => return false,
            };
            let Some(account) = account else {
                return false;
            };
            return account
                .lifecycle
                .try_lock()
                .ok()
                .and_then(|lifecycle| {
                    lifecycle
                        .trades
                        .get(trade_key)
                        .map(|trade| trade.ownership.status.eq_ignore_ascii_case(status))
                })
                .unwrap_or(false);
        }
        let Ok(state) = self.state.try_lock() else {
            return false;
        };
        state
            .trades
            .get(trade_key)
            .map(|trade| trade.ownership.status.eq_ignore_ascii_case(status))
            .or_else(|| {
                state
                    .retired_trade_ownership_tombstones
                    .get(trade_key)
                    .map(|tombstone| {
                        tombstone.ownership.status.eq_ignore_ascii_case(status)
                            && retired_trade_tombstone_is_live(tombstone, wall_clock_ms())
                    })
            })
            .unwrap_or(false)
    }

    pub fn restored_trades(&self) -> Vec<RestoredTrade> {
        self.lock_state()
            .trades
            .values()
            .filter_map(|trade| {
                Some(RestoredTrade {
                    ownership: trade.ownership.clone(),
                    booked: trade.booked,
                    usdc_fee: if trade.virtual_fee_booked {
                        trade.usdc_fee
                    } else {
                        0.0
                    },
                    shares_fee: if trade.virtual_fee_booked {
                        trade.shares_fee
                    } else {
                        0.0
                    },
                    virtual_fee_booked: trade.virtual_fee_booked,
                    is_maker: trade.is_maker?,
                    match_time_secs: trade.match_time_secs,
                    ledger_generation: trade.ledger_generation,
                })
            })
            .collect()
    }

    /// Return only one instance's trades for a bounded token scope. Seed and
    /// fee-rebuild workers use this instead of cloning/scanning the aggregate
    /// account ledger; the lookup takes only the instance lifecycle shard.
    pub fn restored_trades_for_instance_tokens(
        &self,
        instance_id: &str,
        token_ids: &HashSet<String>,
    ) -> Vec<RestoredTrade> {
        if instance_id.trim().is_empty() || token_ids.is_empty() {
            return Vec::new();
        }
        let Some(account) = self.virtual_account(instance_id) else {
            return Vec::new();
        };
        let lifecycle = account.lifecycle.lock().unwrap();
        lifecycle
            .trades
            .values()
            .filter(|trade| token_ids.contains(&trade.ownership.token_id))
            .filter_map(|trade| {
                Some(RestoredTrade {
                    ownership: trade.ownership.clone(),
                    booked: trade.booked,
                    usdc_fee: if trade.virtual_fee_booked {
                        trade.usdc_fee
                    } else {
                        0.0
                    },
                    shares_fee: if trade.virtual_fee_booked {
                        trade.shares_fee
                    } else {
                        0.0
                    },
                    virtual_fee_booked: trade.virtual_fee_booked,
                    is_maker: trade.is_maker?,
                    match_time_secs: trade.match_time_secs,
                    ledger_generation: trade.ledger_generation,
                })
            })
            .collect()
    }

    /// Exact, instance-sharded generation lookup for one private trade.
    /// Unlike `restored_trades`, this never materializes the aggregate account
    /// or clones unrelated trade rows.
    pub fn trade_ledger_generation(&self, instance_id: &str, trade_key: &str) -> Option<u64> {
        if instance_id.trim().is_empty() || trade_key.trim().is_empty() {
            return None;
        }
        let account = self.virtual_account(instance_id)?;
        let lifecycle = account.lifecycle.lock().unwrap();
        lifecycle
            .trades
            .get(trade_key)
            .map(|trade| trade.ledger_generation)
    }

    /// Bound the durable per-event ownership history after the executor's
    /// late-fill mapping grace has elapsed. Potentially-live/FAILED orders and
    /// nonterminal trades are retained; only fully terminal rows for the
    /// retired token scope are removed.
    pub fn prune_terminal_history(&self, tokens: &HashSet<String>) -> (usize, usize) {
        self.prune_terminal_history_scoped(None, tokens)
    }

    /// Instance-scoped settled-FIFO retirement. Multiple strategies may share
    /// one physical wallet and even the same event tokens; one instance's FIFO
    /// eviction must never erase a sibling's still-revisable ownership rows.
    pub fn prune_terminal_history_for_instance(
        &self,
        instance_id: &str,
        tokens: &HashSet<String>,
    ) -> (usize, usize) {
        if instance_id.is_empty() {
            return (0, 0);
        }
        self.prune_terminal_history_scoped(Some(instance_id), tokens)
    }

    fn prune_terminal_history_scoped(
        &self,
        instance_id: Option<&str>,
        tokens: &HashSet<String>,
    ) -> (usize, usize) {
        if tokens.is_empty() {
            return (0, 0);
        }
        let mut state = self.lock_state();
        let outcome = prune_terminal_history_locked(&mut state, instance_id, tokens);
        self.retired_trade_tombstone_count_fast.store(
            state.retired_trade_ownership_tombstones.len(),
            Ordering::Relaxed,
        );
        let pruned_orders = outcome.orders.len();
        let pruned_trades = outcome.trades.len();
        if !outcome.is_empty() {
            self.schedule_settled_prune_persist(&state, &[outcome], &[]);
        }
        (pruned_orders, pruned_trades)
    }
}

fn validate_settled_audit_identity(
    instance_id: &str,
    condition_id: &str,
    asset_ids: &[String],
) -> Result<BTreeSet<String>, ReservationError> {
    let normalized: BTreeSet<String> = asset_ids
        .iter()
        .map(|token| token.trim())
        .filter(|token| !token.is_empty())
        .map(str::to_string)
        .collect();
    if instance_id.trim().is_empty()
        || condition_id.trim().is_empty()
        || normalized.len() != asset_ids.len()
    {
        return Err(ReservationError::InvalidOrder(
            "settled audit reference requires instance/condition and distinct non-empty tokens"
                .to_string(),
        ));
    }
    Ok(normalized)
}

fn settled_audit_has_revisable_rows(state: &SharedAccountState, tokens: &HashSet<String>) -> bool {
    state.orders.iter().any(|(coid, order)| {
        tokens.contains(&order.token_id)
            && (state.recovery_pending_orders.contains(coid)
                || state.routine_cancel_audits.contains(coid)
                || order.reserved_cash > EPS
                || order.reserved_quantity > EPS
                || !matches!(
                    order.status,
                    OrderStatus::Cancelled
                        | OrderStatus::Rejected
                        | OrderStatus::Filled
                        | OrderStatus::Failed
                ))
    }) || state.trades.iter().any(|(trade_key, trade)| {
        tokens.contains(&trade.ownership.token_id)
            && !trade.failed
            && (trade.ownership.status != "CONFIRMED"
                || trade.is_maker.is_none()
                || state.fee_attribution_pending.contains(trade_key))
    })
}

#[derive(Debug, Default)]
struct SettledPruneOutcome {
    orders: Vec<(String, String)>,
    trades: Vec<String>,
    fee_tokens: Vec<String>,
    expired_tombstones: Vec<String>,
}

impl SettledPruneOutcome {
    fn is_empty(&self) -> bool {
        self.orders.is_empty()
            && self.trades.is_empty()
            && self.fee_tokens.is_empty()
            && self.expired_tombstones.is_empty()
    }
}

fn prune_terminal_history_locked(
    state: &mut SharedAccountState,
    instance_id: Option<&str>,
    tokens: &HashSet<String>,
) -> SettledPruneOutcome {
    // A terminal order is still the durable ownership root for every late
    // MINED/CONFIRMED/FAILED edge. Retain it (and its oid mapping) until all
    // trades that reference it are terminal and their fee attribution is
    // complete.
    let protected_coids: HashSet<String> = state
        .trades
        .iter()
        .filter(|(trade_key, trade)| {
            !trade.failed
                && (trade.ownership.status != "CONFIRMED"
                    || state.fee_attribution_pending.contains(*trade_key))
        })
        .map(|(_, trade)| trade.ownership.client_order_id.clone())
        .collect();
    let stale_orders: Vec<(String, String)> = state
        .orders
        .iter()
        .filter(|(coid, order)| {
            tokens.contains(&order.token_id)
                && instance_id.is_none_or(|expected| order.instance_id == expected)
                && !protected_coids.contains(*coid)
                && !state.recovery_pending_orders.contains(*coid)
                && !state.routine_cancel_audits.contains(*coid)
                && order.reserved_cash <= EPS
                && order.reserved_quantity <= EPS
                && matches!(
                    order.status,
                    OrderStatus::Cancelled
                        | OrderStatus::Rejected
                        | OrderStatus::Filled
                        | OrderStatus::Failed
                )
        })
        .map(|(coid, order)| (coid.clone(), order.order_id.clone()))
        .collect();
    for (coid, oid) in &stale_orders {
        state.orders.remove(coid);
        state.oid_to_coid.remove(&normalize_order_id(oid));
    }
    let stale_trades: Vec<String> = state
        .trades
        .iter()
        .filter(|(trade_key, trade)| {
            tokens.contains(&trade.ownership.token_id)
                && instance_id.is_none_or(|expected| trade.ownership.instance_id == expected)
                && !state.fee_attribution_pending.contains(*trade_key)
                && (trade.failed || trade.ownership.status == "CONFIRMED")
        })
        .map(|(trade_key, _)| trade_key.clone())
        .collect();
    let retired_at_ms = wall_clock_ms();
    for trade_key in &stale_trades {
        if let Some(trade) = state.trades.remove(trade_key) {
            state.retired_trade_ownership_tombstones.insert(
                trade_key.clone(),
                RetiredTradeOwnershipTombstone {
                    ownership: trade.ownership.clone(),
                    is_maker: trade.is_maker,
                    authenticated_terminal_noop: false,
                    retired_at_ms,
                },
            );
            add_economic_state(
                &mut state.compacted_economic_effects,
                &trade_economic_effect(&trade),
                1.0,
            );
        }
        state.fee_attribution_pending.remove(trade_key);
    }
    let expired_tombstones = prune_retired_trade_ownership_tombstones(state, retired_at_ms);
    let protected_fee_tokens: HashSet<String> = state
        .trades
        .iter()
        .filter(|(trade_key, trade)| {
            !trade.failed
                && (trade.ownership.status != "CONFIRMED"
                    || state.fee_attribution_pending.contains(*trade_key))
        })
        .map(|(_, trade)| trade.ownership.token_id.clone())
        .collect();
    // Fee curves are token-global within the shared wallet. Instance-scoped
    // retirement cannot prove that no sibling FIFO still references the token.
    let fee_tokens = if instance_id.is_none() {
        tokens
            .iter()
            .filter(|token| !protected_fee_tokens.contains(*token))
            .filter_map(|token| state.token_fee_configs.remove(token).map(|_| token.clone()))
            .collect()
    } else {
        Vec::new()
    };
    SettledPruneOutcome {
        orders: stale_orders,
        trades: stale_trades,
        fee_tokens,
        expired_tombstones,
    }
}

impl Drop for SharedAccount {
    fn drop(&mut self) {
        if let Err(error) = self.flush_persistence(Duration::from_secs(2)) {
            log::error!(
                "[shared_account] account={} final ledger flush failed: {}",
                self.account_id,
                error,
            );
        }
    }
}

fn finite_nonnegative(value: f64) -> f64 {
    if value.is_finite() {
        value.max(0.0)
    } else {
        0.0
    }
}

fn wall_clock_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

fn retired_trade_tombstone_is_live(
    tombstone: &RetiredTradeOwnershipTombstone,
    now_ms: u64,
) -> bool {
    now_ms.saturating_sub(tombstone.retired_at_ms) <= RETIRED_TRADE_TOMBSTONE_TTL_MS
}

fn prune_retired_trade_ownership_tombstones(
    state: &mut SharedAccountState,
    now_ms: u64,
) -> Vec<String> {
    let mut removed: Vec<String> = state
        .retired_trade_ownership_tombstones
        .iter()
        .filter(|(_, tombstone)| !retired_trade_tombstone_is_live(tombstone, now_ms))
        .map(|(trade_key, _)| trade_key.clone())
        .collect();
    for trade_key in &removed {
        state.retired_trade_ownership_tombstones.remove(trade_key);
    }
    let excess = state
        .retired_trade_ownership_tombstones
        .len()
        .saturating_sub(MAX_RETIRED_TRADE_TOMBSTONES);
    if excess == 0 {
        return removed;
    }
    let mut oldest: Vec<(u64, String)> = state
        .retired_trade_ownership_tombstones
        .iter()
        .map(|(trade_key, tombstone)| (tombstone.retired_at_ms, trade_key.clone()))
        .collect();
    oldest.sort_unstable();
    for (_, trade_key) in oldest.into_iter().take(excess) {
        state.retired_trade_ownership_tombstones.remove(&trade_key);
        removed.push(trade_key);
    }
    removed
}

fn advance_trade_ledger_generation(state: &mut SharedAccountState, trade_key: &str) -> u64 {
    state.ledger_generation = state.ledger_generation.saturating_add(1).max(1);
    let generation = state.ledger_generation;
    if let Some(trade) = state.trades.get_mut(trade_key) {
        trade.ledger_generation = generation;
    }
    generation
}

fn set_uncertain(state: &mut SharedAccountState, reason: String) {
    state.uncertain = true;
    state.uncertain_reason = Some(reason);
    state.uncertain_since_ms.get_or_insert_with(wall_clock_ms);
}

fn clear_uncertain(state: &mut SharedAccountState) {
    state.uncertain = false;
    state.uncertain_reason = None;
    state.uncertain_since_ms = None;
}

fn has_unsettled_trade_lifecycle(state: &SharedAccountState) -> bool {
    state.trades.values().any(|trade| {
        trade.booked
            && !trade.failed
            && (!trade.physical_booked || (trade.virtual_fee_booked && !trade.physical_fee_booked))
    })
}

fn has_unsettled_maintenance_operation(state: &SharedAccountState) -> bool {
    state.maintenance_ops.values().any(|operation| {
        matches!(
            operation.status,
            MaintenanceOperationStatus::Reserved
                | MaintenanceOperationStatus::Submitted
                | MaintenanceOperationStatus::Uncertain
        )
    })
}

fn clear_confirmed_maintenance_risk_blockers(state: &mut SharedAccountState) -> Vec<String> {
    let confirmed: Vec<(&str, &str)> = state
        .maintenance_ops
        .values()
        .filter(|operation| operation.status == MaintenanceOperationStatus::Confirmed)
        .map(|operation| {
            (
                operation.operation_id.as_str(),
                operation.condition_id.as_str(),
            )
        })
        .collect();
    let mut cleared: Vec<String> = confirmed
        .iter()
        .map(|(operation_id, _)| {
            format!("{MAINTENANCE_ATTRIBUTION_RISK_BLOCKER_PREFIX}{operation_id}")
        })
        .filter(|source| state.risk_blockers.remove(source).is_some())
        .collect();

    let legacy_manual_is_confirmed =
        state
            .risk_blockers
            .get(MANUAL_RISK_BLOCKER)
            .is_some_and(|blocker| {
                confirmed.iter().any(|(_, condition_id)| {
                    blocker.reason.starts_with(&format!(
                        "confirmed maintenance split attribution failed cid={condition_id}: "
                    ))
                })
            });
    if legacy_manual_is_confirmed {
        state.risk_blockers.remove(MANUAL_RISK_BLOCKER);
        cleared.push(MANUAL_RISK_BLOCKER.to_string());
    }
    cleared
}

fn fee_configs_equal(left: &TokenFeeConfig, right: &TokenFeeConfig) -> bool {
    left.rate.to_bits() == right.rate.to_bits()
        && left.exponent.to_bits() == right.exponent.to_bits()
}

fn configured_fee_amounts(
    ownership: &TradeOwnership,
    is_maker: bool,
    config: Option<&TokenFeeConfig>,
) -> (f64, f64) {
    if is_maker {
        return (0.0, 0.0);
    }
    let Some(config) = config else {
        return (0.0, 0.0);
    };
    let price = ownership.price.clamp(0.0, 1.0);
    let notional =
        ownership.quantity * config.rate * (price * (1.0 - price)).max(0.0).powf(config.exponent);
    match ownership.side {
        Side::Buy => (
            0.0,
            if ownership.price > EPS {
                notional / ownership.price
            } else {
                0.0
            },
        ),
        Side::Sell => (notional, 0.0),
    }
}

fn apply_trade_fee_transition_virtual(
    account: &VirtualAccount,
    lifecycle: &mut VirtualLifecycle,
    trade_key: &str,
    status: OrderStatus,
    is_maker: bool,
    config: Option<&TokenFeeConfig>,
) -> Result<bool, String> {
    let existing = lifecycle
        .trades
        .get(trade_key)
        .cloned()
        .ok_or_else(|| format!("fee attribution missing owned trade `{trade_key}`"))?;
    if existing.is_maker.is_some_and(|stored| stored != is_maker) {
        return Err(format!(
            "trade role replay mismatch trade={trade_key} stored_maker={:?} replay_maker={is_maker}",
            existing.is_maker,
        ));
    }
    if let Some(trade) = lifecycle.trades.get_mut(trade_key) {
        trade.is_maker = Some(is_maker);
    }
    if !is_maker && config.is_none() {
        lifecycle
            .fee_attribution_pending
            .insert(trade_key.to_string());
        return Ok(existing.is_maker != Some(is_maker));
    }
    let (usdc_fee, shares_fee) = configured_fee_amounts(&existing.ownership, is_maker, config);
    if (existing.usdc_fee > EPS && (existing.usdc_fee - usdc_fee).abs() > EPS)
        || (existing.shares_fee > EPS && (existing.shares_fee - shares_fee).abs() > EPS)
    {
        return Err(format!(
            "fee replay mismatch trade={trade_key} stored=({:.8},{:.8}) replay=({usdc_fee:.8},{shares_fee:.8})",
            existing.usdc_fee, existing.shares_fee,
        ));
    }
    let effective_usdc = if existing.usdc_fee > EPS {
        existing.usdc_fee
    } else {
        usdc_fee
    };
    let effective_shares = if existing.shares_fee > EPS {
        existing.shares_fee
    } else {
        shares_fee
    };
    let upgrades_zero = existing.usdc_fee <= EPS
        && existing.shares_fee <= EPS
        && (effective_usdc > EPS || effective_shares > EPS);
    let failed = status == OrderStatus::Failed || existing.failed;
    let book_virtual = !failed && (!existing.virtual_fee_booked || upgrades_zero);
    let reverse_virtual = failed && existing.virtual_fee_booked;
    let virtual_multiplier = if book_virtual {
        1.0
    } else if reverse_virtual {
        -1.0
    } else {
        0.0
    };
    if virtual_multiplier != 0.0 {
        account.cash.add(-effective_usdc * virtual_multiplier);
        account
            .position(&existing.ownership.token_id)
            .balance
            .add(-effective_shares * virtual_multiplier);
    }
    let settled_fee = !failed && existing.physical_booked;
    if let Some(trade) = lifecycle.trades.get_mut(trade_key) {
        trade.usdc_fee = effective_usdc;
        trade.shares_fee = effective_shares;
        trade.virtual_fee_booked =
            book_virtual || (existing.virtual_fee_booked && !reverse_virtual);
        // Settlement bookkeeping is local metadata. The wallet snapshot owns
        // physical_cash/physical_positions and consumes no trade-thread lock.
        trade.physical_fee_booked = settled_fee;
    }
    lifecycle.fee_attribution_pending.remove(trade_key);
    Ok(virtual_multiplier != 0.0
        || existing.is_maker != Some(is_maker)
        || existing.physical_fee_booked != settled_fee)
}

fn apply_configured_trade_fee_locked(
    state: &mut SharedAccountState,
    trade_key: &str,
    status: OrderStatus,
    is_maker: bool,
) -> bool {
    let Some(existing) = state.trades.get(trade_key).cloned() else {
        set_uncertain(
            state,
            format!("fee attribution missing owned trade `{trade_key}`"),
        );
        return false;
    };
    if existing.is_maker.is_some_and(|stored| stored != is_maker) {
        set_uncertain(state, format!(
            "trade role replay mismatch trade={trade_key} stored_maker={:?} replay_maker={is_maker}",
            existing.is_maker,
        ));
        return false;
    }
    if let Some(trade) = state.trades.get_mut(trade_key) {
        trade.is_maker = Some(is_maker);
    }
    let config = (!is_maker)
        .then(|| {
            state
                .token_fee_configs
                .get(&existing.ownership.token_id)
                .cloned()
        })
        .flatten();
    if !is_maker && config.is_none() {
        state.fee_attribution_pending.insert(trade_key.to_string());
        recompute_reconciliation(state, "missing token fee config");
        return false;
    }
    let (usdc_fee, shares_fee) =
        configured_fee_amounts(&existing.ownership, is_maker, config.as_ref());
    apply_trade_fee_transition_locked(state, trade_key, status, usdc_fee, shares_fee)
}

fn apply_trade_fee_transition_locked(
    state: &mut SharedAccountState,
    trade_key: &str,
    status: OrderStatus,
    usdc_fee: f64,
    shares_fee: f64,
) -> bool {
    let Some(existing) = state.trades.get(trade_key).cloned() else {
        set_uncertain(
            state,
            format!("fee attribution missing owned trade `{trade_key}`"),
        );
        return false;
    };
    if (existing.usdc_fee > EPS && (existing.usdc_fee - usdc_fee).abs() > EPS)
        || (existing.shares_fee > EPS && (existing.shares_fee - shares_fee).abs() > EPS)
    {
        set_uncertain(state, format!(
            "fee replay mismatch trade={trade_key} stored=({:.8},{:.8}) replay=({usdc_fee:.8},{shares_fee:.8})",
            existing.usdc_fee, existing.shares_fee,
        ));
        return false;
    }
    let effective_usdc = if existing.usdc_fee > EPS {
        existing.usdc_fee
    } else {
        usdc_fee
    };
    let effective_shares = if existing.shares_fee > EPS {
        existing.shares_fee
    } else {
        shares_fee
    };
    let upgrades_zero = existing.usdc_fee <= EPS
        && existing.shares_fee <= EPS
        && (effective_usdc > EPS || effective_shares > EPS);
    let failed = status == OrderStatus::Failed || existing.failed;
    let book_virtual = !failed && (!existing.virtual_fee_booked || upgrades_zero);
    let reverse_virtual = failed && existing.virtual_fee_booked;
    let multiplier = |book, reverse| {
        if book {
            1.0
        } else if reverse {
            -1.0
        } else {
            0.0
        }
    };
    let virtual_multiplier = multiplier(book_virtual, reverse_virtual);
    if virtual_multiplier != 0.0 {
        if let Some(instance) = state.instances.get_mut(&existing.ownership.instance_id) {
            instance.cash -= effective_usdc * virtual_multiplier;
            *instance
                .positions
                .entry(existing.ownership.token_id.clone())
                .or_insert(0.0) -= effective_shares * virtual_multiplier;
        }
    }
    let settlement_observed = !failed && existing.physical_booked;
    if let Some(trade) = state.trades.get_mut(trade_key) {
        trade.usdc_fee = effective_usdc;
        trade.shares_fee = effective_shares;
        trade.virtual_fee_booked =
            book_virtual || (existing.virtual_fee_booked && !reverse_virtual);
        trade.physical_fee_booked = settlement_observed;
    }
    if virtual_multiplier != 0.0 {
        advance_trade_ledger_generation(state, trade_key);
    }
    state.fee_attribution_pending.remove(trade_key);
    recompute_reconciliation(state, "trade fee lifecycle transition");
    true
}

fn instance_pending_order_ids(state: &SharedAccountState, instance_id: &str) -> Vec<String> {
    state
        .recovery_pending_orders
        .iter()
        .filter(|coid| {
            state
                .orders
                .get(*coid)
                .is_some_and(|order| order.instance_id == instance_id)
        })
        .cloned()
        .collect()
}

fn instance_pending_order_ids_requiring_metadata(
    state: &SharedAccountState,
    instance_id: &str,
) -> Vec<String> {
    instance_pending_order_ids(state, instance_id)
        .into_iter()
        .filter(|coid| {
            state
                .orders
                .get(coid)
                .is_some_and(|order| !order.terminal_trade_ids_authoritative)
        })
        .collect()
}

fn reject_instance_audit_blocker(
    state: &SharedAccountState,
    instance_id: &str,
) -> Result<(), ReservationError> {
    let mut client_order_ids = instance_pending_order_ids_requiring_metadata(state, instance_id);
    if client_order_ids.is_empty() {
        return Ok(());
    }
    client_order_ids.sort();
    Err(ReservationError::AccountInstanceBlocked {
        instance_id: instance_id.to_string(),
        client_order_ids,
    })
}

fn reject_allocation_audit_blockers(
    state: &SharedAccountState,
    allocations: &HashMap<String, f64>,
) -> Result<(), ReservationError> {
    let mut instance_ids: Vec<&str> = allocations.keys().map(String::as_str).collect();
    instance_ids.sort_unstable();
    for instance_id in instance_ids {
        reject_instance_audit_blocker(state, instance_id)?;
    }
    Ok(())
}

fn terminal_trade_id_matches(trade_key: &str, base_trade_id: &str) -> bool {
    trade_key == base_trade_id
        || trade_key
            .strip_prefix(base_trade_id)
            .is_some_and(|suffix| suffix.starts_with(':'))
}

fn terminal_order_audit_complete_locked(state: &SharedAccountState, client_order_id: &str) -> bool {
    let Some(order) = state.orders.get(client_order_id) else {
        return false;
    };
    let Some(target) = order.terminal_matched_quantity else {
        return false;
    };
    if !order.terminal_trade_ids_authoritative {
        return false;
    }
    if order.terminal_trade_ids.is_empty() {
        return target <= EPS;
    }
    let covered = order
        .terminal_trade_ids
        .iter()
        .try_fold(0.0, |covered, expected_id| {
            state
                .trades
                .values()
                .find(|trade| {
                    trade.ownership.client_order_id == client_order_id
                        && terminal_trade_id_matches(&trade.ownership.trade_key, expected_id)
                        && (trade.booked || trade.failed)
                })
                .map(|trade| covered + trade.ownership.quantity)
        });
    let Some(covered) = covered else {
        return false;
    };
    let tolerance = target.abs().max(1.0) * 1e-8;
    (covered - target).abs() <= tolerance
}

fn terminal_order_audit_complete_virtual(
    lifecycle: &VirtualLifecycle,
    client_order_id: &str,
) -> bool {
    let Some(order) = lifecycle.orders.get(client_order_id) else {
        return false;
    };
    let Some(target) = order.terminal_matched_quantity else {
        return false;
    };
    if !order.terminal_trade_ids_authoritative {
        return false;
    }
    if order.terminal_trade_ids.is_empty() {
        return target <= EPS;
    }
    let covered = order
        .terminal_trade_ids
        .iter()
        .try_fold(0.0, |covered, expected_id| {
            lifecycle
                .trades
                .values()
                .find(|trade| {
                    trade.ownership.client_order_id == client_order_id
                        && terminal_trade_id_matches(&trade.ownership.trade_key, expected_id)
                        && (trade.booked || trade.failed)
                })
                .map(|trade| covered + trade.ownership.quantity)
        });
    let Some(covered) = covered else {
        return false;
    };
    let tolerance = target.abs().max(1.0) * 1e-8;
    (covered - target).abs() <= tolerance
}

fn release_virtual_order_reservation(
    account: &VirtualAccount,
    lifecycle: &mut VirtualLifecycle,
    client_order_id: &str,
) {
    let Some((token_id, reserved_cash, reserved_quantity)) =
        lifecycle.orders.get_mut(client_order_id).map(|order| {
            let reservation = (
                order.token_id.clone(),
                order.reserved_cash,
                order.reserved_quantity,
            );
            order.reserved_cash = 0.0;
            order.reserved_quantity = 0.0;
            reservation
        })
    else {
        return;
    };
    account.adjust_reservation(&token_id, -reserved_cash, -reserved_quantity);
}

fn release_order_reservation_locked(state: &mut SharedAccountState, client_order_id: &str) {
    let Some((instance_id, token_id, reserved_cash, reserved_quantity)) =
        state.orders.get_mut(client_order_id).map(|order| {
            let reservation = (
                order.instance_id.clone(),
                order.token_id.clone(),
                order.reserved_cash,
                order.reserved_quantity,
            );
            order.reserved_cash = 0.0;
            order.reserved_quantity = 0.0;
            reservation
        })
    else {
        return;
    };
    if let Some(instance) = state.instances.get_mut(&instance_id) {
        instance.reserved_cash = (instance.reserved_cash - reserved_cash).max(0.0);
        if reserved_quantity > 0.0 {
            let reserved = instance.reserved_positions.entry(token_id).or_insert(0.0);
            *reserved = (*reserved - reserved_quantity).max(0.0);
        }
    }
}

fn desired_order_reservation(order: &OrderOwnership) -> (f64, f64) {
    let target = order
        .terminal_matched_quantity
        .unwrap_or(order.quantity)
        .min(order.quantity);
    let remaining = (target - order.filled_quantity).max(0.0);
    match order.side {
        Side::Buy => (
            remaining * order.price * (1.0 + order.fee_rate_bps as f64 / 10_000.0),
            0.0,
        ),
        Side::Sell => (0.0, remaining),
    }
}

#[derive(Debug, PartialEq)]
struct StartupReservationAggregateRepair {
    instance_id: String,
    cash_before: f64,
    cash_after: f64,
    positions: Vec<(String, f64, f64)>,
}

#[derive(Default)]
struct DerivedReservationAggregates {
    order_cash_by_instance: HashMap<String, f64>,
    order_positions_by_instance: HashMap<String, HashMap<String, f64>>,
    maintenance_cash_by_instance: HashMap<String, f64>,
    maintenance_positions_by_instance: HashMap<String, HashMap<String, f64>>,
}

fn derive_reservation_aggregates(
    state: &SharedAccountState,
) -> Result<DerivedReservationAggregates, String> {
    let mut booked_quantity_by_order = HashMap::<String, f64>::new();
    for trade in state
        .trades
        .values()
        .filter(|trade| trade.booked && !trade.failed)
    {
        *booked_quantity_by_order
            .entry(trade.ownership.client_order_id.clone())
            .or_insert(0.0) += trade.ownership.quantity;
    }
    // Terminal pruning folds a confirmed trade's economics into
    // `compacted_economic_effects` and retains only this exact ownership
    // proof. A parent order may intentionally survive that pruning while it
    // remains under startup recovery or terminal audit. Count only durable,
    // economically-booked tombstones that still match every immutable order
    // root; authenticated historical no-ops were never booked by this ledger
    // and must not contribute.
    for tombstone in state.retired_trade_ownership_tombstones.values() {
        let ownership = &tombstone.ownership;
        if tombstone.authenticated_terminal_noop || ownership.status != "CONFIRMED" {
            continue;
        }
        let Some(order) = state.orders.get(&ownership.client_order_id) else {
            continue;
        };
        if trade_ownership_matches_order_root(ownership, order) {
            *booked_quantity_by_order
                .entry(ownership.client_order_id.clone())
                .or_insert(0.0) += ownership.quantity;
        }
    }

    let mut derived = DerivedReservationAggregates::default();
    for (coid, order) in &state.orders {
        let booked = booked_quantity_by_order.get(coid).copied().unwrap_or(0.0);
        let tolerance = reconciliation_tolerance(booked, order.filled_quantity)
            .max(order.quantity.abs().max(1.0) * 1e-8);
        if (booked - order.filled_quantity).abs() > tolerance {
            return Err(format!(
                "order `{coid}` filled_quantity={} disagrees with durable trades={booked}",
                order.filled_quantity,
            ));
        }
        let terminal_and_released = matches!(
            order.status,
            OrderStatus::Cancelled | OrderStatus::Rejected | OrderStatus::Filled
        ) && !state.recovery_pending_orders.contains(coid)
            && !state.routine_cancel_audits.contains(coid);
        let (expected_cash, expected_quantity) = if terminal_and_released {
            (0.0, 0.0)
        } else {
            desired_order_reservation(order)
        };
        if (expected_cash - order.reserved_cash).abs()
            > reconciliation_tolerance(expected_cash, order.reserved_cash)
            || (expected_quantity - order.reserved_quantity).abs()
                > reconciliation_tolerance(expected_quantity, order.reserved_quantity)
        {
            return Err(format!(
                "order `{coid}` reservation disagrees with effective remaining quantity: stored_cash={} expected_cash={expected_cash} stored_quantity={} expected_quantity={expected_quantity}",
                order.reserved_cash, order.reserved_quantity,
            ));
        }
        *derived
            .order_cash_by_instance
            .entry(order.instance_id.clone())
            .or_insert(0.0) += expected_cash;
        if expected_quantity > 0.0 {
            *derived
                .order_positions_by_instance
                .entry(order.instance_id.clone())
                .or_default()
                .entry(order.token_id.clone())
                .or_insert(0.0) += expected_quantity;
        }
    }

    for operation in state.maintenance_ops.values().filter(|operation| {
        matches!(
            operation.status,
            MaintenanceOperationStatus::Reserved
                | MaintenanceOperationStatus::Submitted
                | MaintenanceOperationStatus::Uncertain
        )
    }) {
        for (instance_id, amount) in &operation.allocations {
            match operation.kind {
                MaintenanceOperationKind::Split => {
                    *derived
                        .maintenance_cash_by_instance
                        .entry(instance_id.clone())
                        .or_insert(0.0) += *amount;
                }
                MaintenanceOperationKind::Merge => {
                    let expected = derived
                        .maintenance_positions_by_instance
                        .entry(instance_id.clone())
                        .or_default();
                    for token in [&operation.up_token_id, &operation.down_token_id] {
                        *expected.entry(token.clone()).or_insert(0.0) += *amount;
                    }
                }
            }
        }
    }

    Ok(derived)
}

/// Upgrade legacy combined aggregates and restore conservative reservation
/// deficits before live admission. Version-zero ledgers stored maintenance
/// coverage in the same counters mutated by order lifecycle; their durable
/// order and maintenance roots are split exactly once during startup.
fn repair_under_reserved_instance_aggregates(
    state: &mut SharedAccountState,
) -> Result<Vec<StartupReservationAggregateRepair>, String> {
    let derived = derive_reservation_aggregates(state)?;
    let mut repairs = Vec::new();
    for (instance_id, instance) in &mut state.instances {
        let expected_cash = derived
            .order_cash_by_instance
            .get(instance_id)
            .copied()
            .unwrap_or(0.0);
        let expected_maintenance_cash = derived
            .maintenance_cash_by_instance
            .get(instance_id)
            .copied()
            .unwrap_or(0.0);
        let cash_before = instance.reserved_cash;
        let legacy_scope = instance.reservation_scope_version == 0;
        let repair_cash = legacy_scope
            || (cash_before.is_finite()
                && cash_before >= -EPS
                && expected_cash.is_finite()
                && expected_cash >= 0.0
                && expected_cash - cash_before
                    > reconciliation_tolerance(expected_cash, cash_before));
        if repair_cash {
            instance.reserved_cash = expected_cash;
        }
        if legacy_scope
            || expected_maintenance_cash - instance.maintenance_reserved_cash
                > reconciliation_tolerance(
                    expected_maintenance_cash,
                    instance.maintenance_reserved_cash,
                )
        {
            instance.maintenance_reserved_cash = expected_maintenance_cash;
        }

        let mut position_repairs = Vec::new();
        if let Some(expected_positions) = derived.order_positions_by_instance.get(instance_id) {
            let mut tokens: Vec<&String> = expected_positions.keys().collect();
            tokens.sort();
            for token in tokens {
                let expected = expected_positions.get(token).copied().unwrap_or(0.0);
                let stored = instance
                    .reserved_positions
                    .get(token)
                    .copied()
                    .unwrap_or(0.0);
                if legacy_scope
                    || (stored.is_finite()
                        && stored >= -EPS
                        && expected.is_finite()
                        && expected >= 0.0
                        && expected - stored > reconciliation_tolerance(expected, stored))
                {
                    instance.reserved_positions.insert(token.clone(), expected);
                    position_repairs.push((token.clone(), stored, expected));
                }
            }
        }
        if legacy_scope {
            instance.reserved_positions.retain(|token, _| {
                derived
                    .order_positions_by_instance
                    .get(instance_id)
                    .is_some_and(|positions| positions.contains_key(token))
            });
        }
        let expected_maintenance_positions = derived
            .maintenance_positions_by_instance
            .get(instance_id)
            .cloned()
            .unwrap_or_default();
        if legacy_scope {
            instance.maintenance_reserved_positions = expected_maintenance_positions;
            instance.reservation_scope_version = 1;
        } else {
            for (token, expected) in expected_maintenance_positions {
                let stored = instance
                    .maintenance_reserved_positions
                    .get(&token)
                    .copied()
                    .unwrap_or(0.0);
                if expected - stored > reconciliation_tolerance(expected, stored) {
                    instance
                        .maintenance_reserved_positions
                        .insert(token.clone(), expected);
                    position_repairs.push((token, stored, expected));
                }
            }
        }

        if repair_cash || legacy_scope || !position_repairs.is_empty() {
            repairs.push(StartupReservationAggregateRepair {
                instance_id: instance_id.clone(),
                cash_before,
                cash_after: instance.reserved_cash,
                positions: position_repairs,
            });
        }
    }
    Ok(repairs)
}

fn trade_ownership_matches_order_root(ownership: &TradeOwnership, order: &OrderOwnership) -> bool {
    ownership.account_id == order.account_id
        && ownership.instance_id == order.instance_id
        && ownership.client_order_id == order.client_order_id
        && normalize_order_id(&ownership.order_id) == normalize_order_id(&order.order_id)
        && ownership.token_id == order.token_id
        && ownership.side == order.side
}

fn failed_trade_keys_by_order_for_query(
    state: &SharedAccountState,
) -> HashMap<String, HashSet<String>> {
    let mut by_order: HashMap<String, HashSet<String>> = HashMap::new();
    let mut record = |trade_key: &str, ownership: &TradeOwnership| {
        let Some(order) = state.orders.get(&ownership.client_order_id) else {
            return;
        };
        if trade_ownership_matches_order_root(ownership, order) {
            by_order
                .entry(ownership.client_order_id.clone())
                .or_default()
                .insert(trade_key.to_string());
        }
    };

    for (trade_key, trade) in &state.trades {
        if trade.failed {
            record(trade_key, &trade.ownership);
        }
    }

    let now_ms = wall_clock_ms();
    for (trade_key, tombstone) in &state.retired_trade_ownership_tombstones {
        if !tombstone.authenticated_terminal_noop
            && tombstone.ownership.status == "FAILED"
            && retired_trade_tombstone_is_live(tombstone, now_ms)
        {
            record(trade_key, &tombstone.ownership);
        }
    }

    by_order
}

/// Convert only a FAILED-trade order under-reservation into a conservative,
/// queryable startup-recovery record. FAILED is terminal for the trade but can
/// return its parent maker order to the book. A crash/racy terminal callback
/// may therefore persist the durable FAILED tombstone and zero reservation
/// while the parent still needs an authoritative CLOB status lookup.
///
/// This is deliberately not a general ledger auto-fixer:
///
/// * a durable FAILED trade must own the order;
/// * the order must still carry a recovery/live marker (or an authoritative
///   terminal audit that references the failed trade);
/// * only missing reservation may be added; over-reservation and every other
///   invariant mismatch remain fatal under normal validation.
///
/// Returns `(query_order_ids, mutated)`. The ids, recovery markers and
/// worst-case reservations are all persisted so a crash during the query
/// cannot reopen availability or broaden repair matching on the next start.
fn repair_failed_trade_under_reservations_for_query(
    state: &mut SharedAccountState,
) -> Result<(HashSet<String>, bool), String> {
    let failed_trade_keys_by_order = failed_trade_keys_by_order_for_query(state);
    let mut query_orders = state.startup_query_repair_orders.clone();
    let mut mutated = false;

    for coid in &query_orders {
        if !state.recovery_pending_orders.contains(coid) {
            return Err(format!(
                "query-repair order `{coid}` is missing its recovery marker"
            ));
        }
        if !state.orders.contains_key(coid) {
            return Err(format!(
                "query-repair order `{coid}` is missing its durable order root"
            ));
        }
        if !failed_trade_keys_by_order.contains_key(coid)
            && !state.routine_cancel_audits.contains(coid)
        {
            return Err(format!(
                "query-repair order `{coid}` is missing its durable FAILED-trade or routine-cancel-audit root"
            ));
        }
    }

    for (coid, failed_trade_keys) in failed_trade_keys_by_order {
        let Some(snapshot) = state.orders.get(&coid).cloned() else {
            continue;
        };
        if snapshot.status == OrderStatus::Rejected {
            continue;
        }
        let terminal_audit_references_failed = snapshot.terminal_trade_ids.iter().any(|expected| {
            failed_trade_keys
                .iter()
                .any(|trade_key| terminal_trade_id_matches(trade_key, expected))
        });
        let lifecycle_may_be_live = !matches!(
            snapshot.status,
            OrderStatus::Cancelled | OrderStatus::Rejected | OrderStatus::Filled
        );
        let already_recovering = state.recovery_pending_orders.contains(&coid)
            || state.routine_cancel_audits.contains(&coid);
        let retains_reservation = snapshot.reserved_cash > EPS || snapshot.reserved_quantity > EPS;
        if !lifecycle_may_be_live
            && !already_recovering
            && !retains_reservation
            && !terminal_audit_references_failed
        {
            continue;
        }

        let mut repaired_order = snapshot.clone();
        let terminal_status_needs_normalization = matches!(
            repaired_order.status,
            OrderStatus::Filled | OrderStatus::Failed
        );
        if terminal_status_needs_normalization {
            repaired_order.status = if repaired_order.filled_quantity > EPS {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::Accepted
            };
        }
        if terminal_audit_references_failed {
            repaired_order.terminal_matched_quantity = None;
            repaired_order.terminal_trade_ids.clear();
            repaired_order.terminal_trade_ids_authoritative = false;
        }

        let (expected_cash, expected_quantity) = desired_order_reservation(&repaired_order);
        let cash_tolerance = reconciliation_tolerance(expected_cash, snapshot.reserved_cash);
        let quantity_tolerance =
            reconciliation_tolerance(expected_quantity, snapshot.reserved_quantity);
        let reservation_under = snapshot.reserved_cash + cash_tolerance < expected_cash
            || snapshot.reserved_quantity + quantity_tolerance < expected_quantity;
        let status_changed = terminal_status_needs_normalization
            && (terminal_audit_references_failed || reservation_under);
        if snapshot.reserved_cash > expected_cash + cash_tolerance
            || snapshot.reserved_quantity > expected_quantity + quantity_tolerance
        {
            // Never reduce a persisted reservation automatically. The normal
            // validator will reject this unrelated/unsafe corruption.
            continue;
        }
        if expected_cash <= EPS && expected_quantity <= EPS {
            continue;
        }
        if !terminal_audit_references_failed && !reservation_under {
            // A normally reserved live order may have historical FAILED trade
            // tombstones. It belongs to ordinary startup recovery, not this
            // stricter inconsistency-repair gate.
            continue;
        }

        let instance_id = snapshot.instance_id.clone();
        let token_id = snapshot.token_id.clone();
        let cash_delta = expected_cash - snapshot.reserved_cash;
        let quantity_delta = expected_quantity - snapshot.reserved_quantity;
        let order = state
            .orders
            .get_mut(&coid)
            .expect("FAILED-trade order disappeared during startup repair");
        if status_changed {
            order.status = repaired_order.status;
            mutated = true;
        }
        if terminal_audit_references_failed {
            order.terminal_matched_quantity = None;
            order.terminal_trade_ids.clear();
            order.terminal_trade_ids_authoritative = false;
            mutated = true;
        }
        if cash_delta.abs() > cash_tolerance || quantity_delta.abs() > quantity_tolerance {
            order.reserved_cash = expected_cash;
            order.reserved_quantity = expected_quantity;
            let instance = state.instances.get_mut(&instance_id).ok_or_else(|| {
                format!("query-repair order `{coid}` references missing instance `{instance_id}`")
            })?;
            instance.reserved_cash += cash_delta;
            if quantity_delta.abs() > quantity_tolerance {
                *instance.reserved_positions.entry(token_id).or_insert(0.0) += quantity_delta;
            }
            mutated = true;
        }
        mutated |= state.routine_cancel_audits.remove(&coid);
        mutated |= state.recovery_pending_orders.insert(coid.clone());
        mutated |= state.startup_query_repair_orders.insert(coid.clone());
        query_orders.insert(coid);
    }

    // A cancel response may release the local reservation immediately and a
    // subsequent partial MATCHED/MINED push can then install a routine audit
    // marker before its authoritative cancel-vs-fill query completes. If the
    // process stops in that narrow interval, the durable root is a cancelled
    // order plus `routine_cancel_audits`, not a FAILED trade. Restore the
    // remaining quantity conservatively and force the same pre-admission CLOB
    // query; never infer finality or availability from the local status.
    let routine_cancel_candidates: Vec<String> =
        state.routine_cancel_audits.iter().cloned().collect();
    for coid in routine_cancel_candidates {
        let Some(snapshot) = state.orders.get(&coid).cloned() else {
            continue;
        };
        if !matches!(
            snapshot.status,
            OrderStatus::Cancelled
                | OrderStatus::CancelUncertain
                | OrderStatus::CancelOrderTimeout
        ) {
            continue;
        }
        let (expected_cash, expected_quantity) = desired_order_reservation(&snapshot);
        let cash_tolerance = reconciliation_tolerance(expected_cash, snapshot.reserved_cash);
        let quantity_tolerance =
            reconciliation_tolerance(expected_quantity, snapshot.reserved_quantity);
        let reservation_under = snapshot.reserved_cash + cash_tolerance < expected_cash
            || snapshot.reserved_quantity + quantity_tolerance < expected_quantity;
        if !reservation_under
            || snapshot.reserved_cash > expected_cash + cash_tolerance
            || snapshot.reserved_quantity > expected_quantity + quantity_tolerance
        {
            continue;
        }

        let cash_delta = expected_cash - snapshot.reserved_cash;
        let quantity_delta = expected_quantity - snapshot.reserved_quantity;
        let instance = state.instances.get_mut(&snapshot.instance_id).ok_or_else(|| {
            format!(
                "query-repair order `{coid}` references missing instance `{}`",
                snapshot.instance_id,
            )
        })?;
        instance.reserved_cash += cash_delta;
        if quantity_delta.abs() > quantity_tolerance {
            *instance
                .reserved_positions
                .entry(snapshot.token_id.clone())
                .or_insert(0.0) += quantity_delta;
        }
        let order = state
            .orders
            .get_mut(&coid)
            .expect("routine-cancel order disappeared during startup repair");
        order.reserved_cash = expected_cash;
        order.reserved_quantity = expected_quantity;
        state.recovery_pending_orders.insert(coid.clone());
        state.startup_query_repair_orders.insert(coid.clone());
        query_orders.insert(coid);
        mutated = true;
    }

    Ok((query_orders, mutated))
}

fn order_has_startup_reservation_deficit(
    state: &SharedAccountState,
    order: &OrderOwnership,
) -> bool {
    let Some(instance) = state.instances.get(&order.instance_id) else {
        return false;
    };
    let reserved_position = instance
        .reserved_positions
        .get(&order.token_id)
        .copied()
        .unwrap_or(0.0);
    let owned_position = instance
        .positions
        .get(&order.token_id)
        .copied()
        .unwrap_or(0.0);
    (order.reserved_cash > EPS
        && instance.reserved_cash
            > instance.cash + reconciliation_tolerance(instance.cash, instance.reserved_cash))
        || (order.reserved_quantity > EPS
            && reserved_position
                > owned_position + reconciliation_tolerance(owned_position, reserved_position))
}

/// A reservation may temporarily exceed the last persisted wallet view only
/// when a matching durable recovery root owns that exact resource. Aggregate
/// reservation equality is validated later from the same roots, so this does
/// not permit arbitrary hand-edited availability or cross-instance borrowing.
fn reservation_deficit_has_recovery_root(
    state: &SharedAccountState,
    instance_id: &str,
    token_id: Option<&str>,
) -> bool {
    let order_root = state.recovery_pending_orders.iter().any(|coid| {
        state.orders.get(coid).is_some_and(|order| {
            order.instance_id == instance_id
                && match token_id {
                    Some(token) => order.token_id == token && order.reserved_quantity > EPS,
                    None => order.reserved_cash > EPS,
                }
        })
    });
    if order_root {
        return true;
    }
    state.maintenance_ops.values().any(|operation| {
        matches!(
            operation.status,
            MaintenanceOperationStatus::Reserved
                | MaintenanceOperationStatus::Submitted
                | MaintenanceOperationStatus::Uncertain
        ) && operation
            .allocations
            .get(instance_id)
            .is_some_and(|amount| *amount > EPS)
            && match (operation.kind, token_id) {
                (MaintenanceOperationKind::Split, None) => true,
                (MaintenanceOperationKind::Merge, Some(token)) => {
                    token == operation.up_token_id || token == operation.down_token_id
                }
                _ => false,
            }
    })
}

fn normalize_terminal_failed_state(state: &mut SharedAccountState) -> bool {
    let mut changed = false;
    let failed_coids: Vec<String> = state
        .orders
        .iter()
        .filter(|(_, order)| order.status == OrderStatus::Failed)
        .map(|(coid, _)| coid.clone())
        .collect();
    for trade in state.trades.values_mut().filter(|trade| trade.failed) {
        if !trade.failure_reconciled {
            trade.failure_reconciled = true;
            changed = true;
        }
    }
    for coid in failed_coids {
        let Some(order) = state.orders.get_mut(&coid) else {
            state.recovery_pending_orders.remove(&coid);
            continue;
        };
        let instance_id = order.instance_id.clone();
        let token_id = order.token_id.clone();
        let old_cash = order.reserved_cash;
        let old_qty = order.reserved_quantity;
        order.status = if order.filled_quantity > EPS {
            OrderStatus::PartiallyFilled
        } else {
            OrderStatus::Accepted
        };
        let (desired_cash, desired_qty) = desired_order_reservation(order);
        order.reserved_cash = desired_cash;
        order.reserved_quantity = desired_qty;
        if let Some(instance) = state.instances.get_mut(&instance_id) {
            instance.reserved_cash = (instance.reserved_cash + desired_cash - old_cash).max(0.0);
            if (desired_qty - old_qty).abs() > EPS {
                let reserved = instance.reserved_positions.entry(token_id).or_insert(0.0);
                *reserved = (*reserved + desired_qty - old_qty).max(0.0);
            }
        }
        state.recovery_pending_orders.remove(&coid);
        changed = true;
    }
    changed
}

fn missing_initial_token_interest_owners(
    state: &SharedAccountState,
    authoritative_tokens: &HashSet<String>,
) -> Vec<String> {
    let interests: Vec<&TokenInterest> = state
        .instances
        .values()
        .flat_map(|instance| instance.token_interests.values())
        .filter(|interest| {
            !interest.scope_key.is_empty()
                && (authoritative_tokens.contains(&interest.up_token_id)
                    || authoritative_tokens.contains(&interest.down_token_id))
        })
        .collect();
    let mut missing = Vec::new();
    for interest in interests {
        for (instance_id, instance) in &state.instances {
            if !instance.market_scopes.contains(&interest.scope_key) {
                continue;
            }
            let registered = instance.token_interests.values().any(|candidate| {
                candidate.scope_key == interest.scope_key
                    && candidate.condition_id == interest.condition_id
                    && candidate.up_token_id == interest.up_token_id
                    && candidate.down_token_id == interest.down_token_id
            });
            if !registered {
                missing.push(format!(
                    "instance={instance_id} scope={} condition={}",
                    interest.scope_key, interest.condition_id,
                ));
            }
        }
    }
    missing.sort();
    missing.dedup();
    missing
}

/// A FAILED tombstone may release risk-off only after an authoritative wallet
/// snapshot covers its token and shows no physical deficit versus the virtual
/// ledger. The tombstone itself remains for replay protection.
fn mark_failed_trades_reconciled_by_snapshot(
    state: &mut SharedAccountState,
    authoritative_tokens: &HashSet<String>,
) -> bool {
    let virtual_cash: f64 = state.instances.values().map(|instance| instance.cash).sum();
    if state.unallocated_cash < -reconciliation_tolerance(state.physical_cash, virtual_cash) {
        return false;
    }
    let token_deltas = state.unallocated_positions.clone();
    let mut changed = false;
    for trade in state.trades.values_mut().filter(|trade| {
        trade.failed
            && !trade.failure_reconciled
            && authoritative_tokens.contains(&trade.ownership.token_id)
    }) {
        let token_delta = token_deltas
            .get(&trade.ownership.token_id)
            .copied()
            .unwrap_or(0.0);
        let physical = state
            .physical_positions
            .get(&trade.ownership.token_id)
            .copied()
            .unwrap_or(0.0);
        let virtual_qty: f64 = state
            .instances
            .values()
            .map(|instance| {
                instance
                    .positions
                    .get(&trade.ownership.token_id)
                    .copied()
                    .unwrap_or(0.0)
            })
            .sum();
        if token_delta >= -reconciliation_tolerance(physical, virtual_qty) {
            trade.failure_reconciled = true;
            changed = true;
        }
    }
    changed
}

/// Prove that removing only fee-attribution inputs would make the same ledger
/// healthy. This deliberately reuses the canonical reconciliation function so
/// a lower-priority ownership, maintenance or physical deficit cannot be
/// hidden behind the first fee blocker in `uncertain_reason`.
fn fee_degradation_is_only_uncertainty(state: &SharedAccountState) -> bool {
    if !state.uncertain {
        return false;
    }
    let has_fee_degradation = !state.fee_attribution_pending.is_empty()
        || state
            .risk_blockers
            .keys()
            .any(|source| source.starts_with(FEE_ATTRIBUTION_RISK_BLOCKER_PREFIX));
    if !has_fee_degradation {
        return false;
    }

    let mut without_fees = state.clone();
    without_fees.fee_attribution_pending.clear();
    without_fees
        .risk_blockers
        .retain(|source, _| !source.starts_with(FEE_ATTRIBUTION_RISK_BLOCKER_PREFIX));
    recompute_reconciliation(&mut without_fees, "fee-only admission probe");
    !without_fees.uncertain
}

fn recompute_reconciliation(state: &mut SharedAccountState, _deficit_context: &str) {
    state.provisional_position_owners.retain(|token, owner| {
        state
            .instances
            .get(owner)
            .and_then(|instance| instance.positions.get(token))
            .is_some_and(|quantity| *quantity > EPS)
    });
    let virtual_cash: f64 = state.instances.values().map(|instance| instance.cash).sum();
    // MATCHED is the earliest reliable inventory edge for quoting, but the
    // Polygon wallet does not change until MINED/CONFIRMED. Exclude those
    // explicitly pending physical deltas from reconciliation so a perfectly
    // healthy in-flight settlement does not look like missing cash/shares.
    let mut pending_cash_delta = 0.0;
    let mut pending_position_deltas = HashMap::<String, f64>::new();
    for trade in state
        .trades
        .values()
        .filter(|trade| trade.booked && !trade.physical_booked && !trade.failed)
    {
        let ownership = &trade.ownership;
        let sign = if ownership.side == Side::Buy {
            1.0
        } else {
            -1.0
        };
        pending_cash_delta += -sign * ownership.quantity * ownership.price;
        *pending_position_deltas
            .entry(ownership.token_id.clone())
            .or_insert(0.0) += sign * ownership.quantity;
        if trade.virtual_fee_booked && !trade.physical_fee_booked {
            pending_cash_delta -= trade.usdc_fee;
            *pending_position_deltas
                .entry(ownership.token_id.clone())
                .or_insert(0.0) -= trade.shares_fee;
        }
    }
    // Fees can become known after the base trade was already marked physical.
    for trade in state.trades.values().filter(|trade| {
        trade.booked
            && trade.physical_booked
            && !trade.failed
            && trade.virtual_fee_booked
            && !trade.physical_fee_booked
    }) {
        pending_cash_delta -= trade.usdc_fee;
        *pending_position_deltas
            .entry(trade.ownership.token_id.clone())
            .or_insert(0.0) -= trade.shares_fee;
    }
    state.unallocated_cash = state.physical_cash - (virtual_cash - pending_cash_delta);
    state.unallocated_positions.clear();
    let mut all_tokens: HashSet<String> = state.physical_positions.keys().cloned().collect();
    all_tokens.extend(
        state
            .instances
            .values()
            .flat_map(|instance| instance.positions.keys().cloned()),
    );
    for token in all_tokens {
        let physical = state.physical_positions.get(&token).copied().unwrap_or(0.0);
        let virtual_qty: f64 = state
            .instances
            .values()
            .map(|instance| instance.positions.get(&token).copied().unwrap_or(0.0))
            .sum();
        let pending = pending_position_deltas.get(&token).copied().unwrap_or(0.0);
        let expected_virtual = virtual_qty - pending;
        let delta = physical - expected_virtual;
        let tolerance = reconciliation_tolerance(physical, expected_virtual);
        if delta.abs() > tolerance {
            state.unallocated_positions.insert(token, delta);
        }
    }
    if let Some((source, blocker)) = state.risk_blockers.iter().next() {
        state.uncertain = true;
        state.uncertain_reason = Some(format!("{source}: {}", blocker.reason));
        state.uncertain_since_ms = Some(blocker.since_ms);
    } else if !state.ownership_anomalies.is_empty() {
        let details = state
            .ownership_anomalies
            .iter()
            .map(|(key, reason)| format!("{key}={reason}"))
            .collect::<Vec<_>>()
            .join("; ");
        set_uncertain(
            state,
            format!(
                "ownership anomalies pending repair: count={} [{}]",
                state.ownership_anomalies.len(),
                details,
            ),
        );
    } else if let Some(reason) = state.allocation_migration_required.clone() {
        set_uncertain(state, reason);
    } else if let Some(reason) = state.instance_registry_issue.clone() {
        set_uncertain(state, reason);
    } else if !state.fee_attribution_pending.is_empty() {
        let mut pending: Vec<&str> = state
            .fee_attribution_pending
            .iter()
            .map(String::as_str)
            .collect();
        pending.sort_unstable();
        set_uncertain(
            state,
            format!(
                "trade fee attribution pending: count={} trade_ids=[{}]",
                pending.len(),
                pending.join(","),
            ),
        );
    } else if let Some(operation) = state.maintenance_ops.values().find(|operation| {
        matches!(
            operation.status,
            MaintenanceOperationStatus::Submitted | MaintenanceOperationStatus::Uncertain
        )
    }) {
        set_uncertain(
            state,
            format!(
                "maintenance operation `{}` awaits finality/recovery: {}",
                operation.operation_id,
                operation.detail.as_deref().unwrap_or("pending recovery"),
            ),
        );
    } else if let Some((trade_key, _)) = state
        .trades
        .iter()
        .find(|(_, trade)| trade.failed && !trade.failure_reconciled)
    {
        set_uncertain(
            state,
            format!("failed trade `{trade_key}` awaits startup account baseline"),
        );
    } else {
        // Wallet-vs-virtual differences remain visible as exact signed
        // `unallocated_*` residuals. They do not, by themselves, make order
        // ownership or operation finality uncertain and therefore must not
        // close account admission.
        clear_uncertain(state);
    }
}

fn validate_physical_snapshot(
    cash: f64,
    positions: &HashMap<String, f64>,
    authoritative_tokens: &HashSet<String>,
) -> Result<(), String> {
    if !cash.is_finite() || cash < 0.0 {
        return Err(format!("physical snapshot has invalid cash {cash}"));
    }
    for (token, quantity) in positions {
        if token.trim().is_empty() || !quantity.is_finite() || *quantity < 0.0 {
            return Err(format!(
                "physical snapshot has invalid position token={token:?} quantity={quantity}"
            ));
        }
        if !authoritative_tokens.contains(token) {
            return Err(format!(
                "physical snapshot position token `{token}` is outside its authoritative scope"
            ));
        }
    }
    if authoritative_tokens
        .iter()
        .any(|token| token.trim().is_empty())
    {
        return Err("physical snapshot has an empty authoritative token".to_string());
    }
    Ok(())
}

fn validate_named_values(
    field: &str,
    values: &HashMap<String, f64>,
    nonnegative: bool,
) -> Result<(), String> {
    for (key, value) in values {
        if key.trim().is_empty() || !value.is_finite() || (nonnegative && *value < -EPS) {
            return Err(format!("{field} contains invalid entry {key:?}={value}"));
        }
    }
    Ok(())
}

fn add_position_delta(positions: &mut HashMap<String, f64>, token: &str, delta: f64) {
    *positions.entry(token.to_string()).or_insert(0.0) += delta;
}

fn economic_instance_mut<'a>(
    economics: &'a mut AccountEconomicState,
    instance_id: &str,
) -> &'a mut EconomicBalance {
    economics
        .instances
        .entry(instance_id.to_string())
        .or_default()
}

fn record_internal_external_adjustment(
    state: &mut SharedAccountState,
    label: &str,
    instance_id: &str,
    cash_delta: f64,
    position_deltas: HashMap<String, f64>,
) {
    state.internal_adjustment_sequence =
        state.internal_adjustment_sequence.saturating_add(1).max(1);
    let operation_id = format!(
        "internal:{label}:{}:{}",
        state.internal_adjustment_sequence, instance_id,
    );
    state.external_adjustments.insert(
        operation_id.clone(),
        ExternalAdjustment {
            operation_id,
            instance_id: instance_id.to_string(),
            cash_delta,
            position_deltas,
            recorded_at_ms: wall_clock_ms().max(1),
        },
    );
}

fn add_economic_state(target: &mut AccountEconomicState, delta: &AccountEconomicState, scale: f64) {
    target.physical_cash += delta.physical_cash * scale;
    for (token, quantity) in &delta.physical_positions {
        add_position_delta(&mut target.physical_positions, token, quantity * scale);
    }
    for (instance_id, balance) in &delta.instances {
        let target_instance = economic_instance_mut(target, instance_id);
        target_instance.cash += balance.cash * scale;
        for (token, quantity) in &balance.positions {
            add_position_delta(&mut target_instance.positions, token, quantity * scale);
        }
    }
}

fn trade_economic_effect(trade: &AppliedTrade) -> AccountEconomicState {
    let mut effect = AccountEconomicState::default();
    if trade.failed {
        return effect;
    }
    let ownership = &trade.ownership;
    let sign = if ownership.side == Side::Buy {
        1.0
    } else {
        -1.0
    };
    let cash_delta = -sign * ownership.quantity * ownership.price;
    let position_delta = sign * ownership.quantity;
    if trade.booked {
        let instance = economic_instance_mut(&mut effect, &ownership.instance_id);
        instance.cash += cash_delta;
        add_position_delta(&mut instance.positions, &ownership.token_id, position_delta);
    }
    if trade.virtual_fee_booked {
        let instance = economic_instance_mut(&mut effect, &ownership.instance_id);
        instance.cash -= trade.usdc_fee;
        add_position_delta(
            &mut instance.positions,
            &ownership.token_id,
            -trade.shares_fee,
        );
    }
    effect
}

fn durable_root_economic_effects(state: &SharedAccountState) -> AccountEconomicState {
    let mut effects = state.compacted_economic_effects.clone();
    for trade in state.trades.values() {
        add_economic_state(&mut effects, &trade_economic_effect(trade), 1.0);
    }
    for operation in state
        .maintenance_ops
        .values()
        .filter(|operation| operation.status == MaintenanceOperationStatus::Confirmed)
    {
        let total: f64 = operation.allocations.values().sum();
        let direction = match operation.kind {
            MaintenanceOperationKind::Split => -1.0,
            MaintenanceOperationKind::Merge => 1.0,
        };
        effects.physical_cash += direction * total;
        for token in [&operation.up_token_id, &operation.down_token_id] {
            add_position_delta(&mut effects.physical_positions, token, -direction * total);
        }
        for (instance_id, amount) in &operation.allocations {
            let instance = economic_instance_mut(&mut effects, instance_id);
            instance.cash += direction * *amount;
            for token in [&operation.up_token_id, &operation.down_token_id] {
                add_position_delta(&mut instance.positions, token, -direction * *amount);
            }
        }
    }
    for adjustment in state.external_adjustments.values() {
        effects.physical_cash += adjustment.cash_delta;
        for (token, delta) in &adjustment.position_deltas {
            add_position_delta(&mut effects.physical_positions, token, *delta);
        }
        let instance = economic_instance_mut(&mut effects, &adjustment.instance_id);
        instance.cash += adjustment.cash_delta;
        for (token, delta) in &adjustment.position_deltas {
            add_position_delta(&mut instance.positions, token, *delta);
        }
    }
    for migration in state.cash_allocation_migrations.values() {
        let mut instance_ids: HashSet<String> = migration.cash_before.keys().cloned().collect();
        instance_ids.extend(migration.cash_after.keys().cloned());
        for instance_id in instance_ids {
            let before = migration
                .cash_before
                .get(&instance_id)
                .copied()
                .unwrap_or(0.0);
            let after = migration
                .cash_after
                .get(&instance_id)
                .copied()
                .unwrap_or(0.0);
            economic_instance_mut(&mut effects, &instance_id).cash += after - before;
        }
    }
    effects
}

fn current_account_economics(state: &SharedAccountState) -> AccountEconomicState {
    AccountEconomicState {
        physical_cash: state.physical_cash,
        physical_positions: state.physical_positions.clone(),
        instances: state
            .instances
            .iter()
            .map(|(instance_id, instance)| {
                (
                    instance_id.clone(),
                    EconomicBalance {
                        cash: instance.cash,
                        positions: instance.positions.clone(),
                    },
                )
            })
            .collect(),
    }
}

fn capture_seed_baseline(state: &SharedAccountState, legacy_derived: bool) -> AccountSeedBaseline {
    let current = current_account_economics(state);
    AccountSeedBaseline {
        captured_at_ms: wall_clock_ms().max(1),
        physical_cash: current.physical_cash,
        physical_positions: current.physical_positions,
        instances: current.instances,
        legacy_derived,
    }
}

fn derive_legacy_seed_baseline(state: &SharedAccountState) -> AccountSeedBaseline {
    let mut baseline_economics = current_account_economics(state);
    let effects = durable_root_economic_effects(state);
    add_economic_state(&mut baseline_economics, &effects, -1.0);
    AccountSeedBaseline {
        captured_at_ms: wall_clock_ms().max(1),
        physical_cash: baseline_economics.physical_cash,
        physical_positions: baseline_economics.physical_positions,
        instances: baseline_economics.instances,
        legacy_derived: true,
    }
}

fn replay_account_economics(state: &SharedAccountState) -> Result<AccountEconomicState, String> {
    let baseline = state
        .seed_baseline
        .as_ref()
        .ok_or_else(|| "seeded account is missing immutable seed baseline".to_string())?;
    let mut replayed = AccountEconomicState {
        physical_cash: baseline.physical_cash,
        physical_positions: baseline.physical_positions.clone(),
        instances: baseline.instances.clone(),
    };
    add_economic_state(&mut replayed, &durable_root_economic_effects(state), 1.0);
    Ok(replayed)
}

fn compare_economic_value(field: &str, stored: f64, replayed: f64) -> Result<(), String> {
    if (stored - replayed).abs() > reconciliation_tolerance(stored, replayed) {
        return Err(format!(
            "{field}={stored} disagrees with immutable-baseline replay={replayed}",
        ));
    }
    Ok(())
}

fn compare_economic_positions(
    field: &str,
    stored: &HashMap<String, f64>,
    replayed: &HashMap<String, f64>,
) -> Result<(), String> {
    let mut tokens: HashSet<String> = stored.keys().cloned().collect();
    tokens.extend(replayed.keys().cloned());
    for token in tokens {
        compare_economic_value(
            &format!("{field}[{token}]"),
            stored.get(&token).copied().unwrap_or(0.0),
            replayed.get(&token).copied().unwrap_or(0.0),
        )?;
    }
    Ok(())
}

/// A syntactically valid JSON ledger is not necessarily a valid account. This
/// validator runs before the persistence worker starts so parseable corruption,
/// incompatible old writers or hand-edited negative reservations fail closed.
fn validate_persisted_state(account_id: &str, state: &SharedAccountState) -> Result<(), String> {
    if !state.physical_cash.is_finite() || state.physical_cash < -EPS {
        return Err(format!("invalid physical_cash {}", state.physical_cash));
    }
    if !state.unallocated_cash.is_finite() {
        return Err(format!(
            "invalid unallocated_cash {}",
            state.unallocated_cash
        ));
    }
    validate_named_values("physical_positions", &state.physical_positions, true)?;
    validate_named_values("unallocated_positions", &state.unallocated_positions, false)?;

    if state.seeded {
        let baseline = state
            .seed_baseline
            .as_ref()
            .ok_or_else(|| "seeded account is missing immutable seed baseline".to_string())?;
        if baseline.captured_at_ms == 0 || !baseline.physical_cash.is_finite() {
            return Err("immutable seed baseline has invalid metadata/cash".to_string());
        }
        validate_named_values(
            "immutable seed physical positions",
            &baseline.physical_positions,
            false,
        )?;
        for (instance_id, balance) in &baseline.instances {
            if instance_id.trim().is_empty() || !balance.cash.is_finite() {
                return Err(format!(
                    "immutable seed baseline has invalid instance `{instance_id}`"
                ));
            }
            validate_named_values(
                &format!("immutable seed instance `{instance_id}` positions"),
                &balance.positions,
                false,
            )?;
        }
    }
    if !state.compacted_economic_effects.physical_cash.is_finite() {
        return Err("compacted economic effects have invalid physical cash".to_string());
    }
    validate_named_values(
        "compacted physical position effects",
        &state.compacted_economic_effects.physical_positions,
        false,
    )?;
    for (instance_id, balance) in &state.compacted_economic_effects.instances {
        if instance_id.trim().is_empty() || !balance.cash.is_finite() {
            return Err(format!(
                "compacted economic effects have invalid instance `{instance_id}`"
            ));
        }
        validate_named_values(
            &format!("compacted instance `{instance_id}` position effects"),
            &balance.positions,
            false,
        )?;
    }

    for (instance_id, instance) in &state.instances {
        if instance_id.trim().is_empty()
            || !instance.weight.is_finite()
            || instance.weight <= 0.0
            || !instance.cash.is_finite()
            || instance.cash < -EPS
            || !instance.reserved_cash.is_finite()
            || instance.reserved_cash < -EPS
            || !instance.maintenance_reserved_cash.is_finite()
            || instance.maintenance_reserved_cash < -EPS
            || instance.reservation_scope_version != 1
        {
            return Err(format!(
                "instance `{instance_id}` has invalid weight/cash/reservation"
            ));
        }
        validate_named_values(
            &format!("instance `{instance_id}` positions"),
            &instance.positions,
            true,
        )?;
        validate_named_values(
            &format!("instance `{instance_id}` reserved_positions"),
            &instance.reserved_positions,
            true,
        )?;
        validate_named_values(
            &format!("instance `{instance_id}` maintenance_reserved_positions"),
            &instance.maintenance_reserved_positions,
            true,
        )?;
        let total_reserved_cash = instance.total_reserved_cash();
        if total_reserved_cash
            > instance.cash + reconciliation_tolerance(instance.cash, total_reserved_cash)
            && !reservation_deficit_has_recovery_root(state, instance_id, None)
        {
            return Err(format!(
                "instance `{instance_id}` reserves more cash than it owns"
            ));
        }
        for token in instance
            .reserved_positions
            .keys()
            .chain(instance.maintenance_reserved_positions.keys())
        {
            let reserved = instance.total_reserved_position(token);
            let owned = instance.positions.get(token).copied().unwrap_or(0.0);
            if reserved > owned + reconciliation_tolerance(owned, reserved)
                && !reservation_deficit_has_recovery_root(state, instance_id, Some(token.as_str()))
            {
                return Err(format!(
                    "instance `{instance_id}` reserves more `{token}` than it owns"
                ));
            }
        }
        for (condition_id, interest) in &instance.token_interests {
            if condition_id.trim().is_empty()
                || interest.condition_id != *condition_id
                || interest.instance_id != *instance_id
                || interest.up_token_id.trim().is_empty()
                || interest.down_token_id.trim().is_empty()
                || interest.up_token_id == interest.down_token_id
            {
                return Err(format!(
                    "instance `{instance_id}` has invalid token interest `{condition_id}`"
                ));
            }
        }
        if instance
            .market_scopes
            .iter()
            .any(|scope| scope.trim().is_empty())
        {
            return Err(format!(
                "instance `{instance_id}` has an empty market scope"
            ));
        }
    }

    for (condition_id, reference) in &state.settled_audit_references {
        if condition_id.trim().is_empty()
            || reference.condition_id != *condition_id
            || reference.asset_ids.is_empty()
            || reference
                .asset_ids
                .iter()
                .any(|token| token.trim().is_empty())
            || reference
                .instances
                .iter()
                .any(|instance_id| !state.instances.contains_key(instance_id))
        {
            return Err(format!(
                "invalid settled audit reference for condition `{condition_id}`",
            ));
        }
    }

    for (token, owner) in &state.provisional_position_owners {
        if token.trim().is_empty()
            || !state.instances.contains_key(owner)
            || state
                .instances
                .get(owner)
                .and_then(|instance| instance.positions.get(token))
                .is_none_or(|quantity| *quantity <= EPS)
        {
            return Err(format!(
                "invalid provisional owner token={token:?} owner={owner:?}"
            ));
        }
    }

    for (coid, order) in &state.orders {
        let tolerance = order.quantity.abs().max(1.0) * 1e-8;
        if coid.trim().is_empty()
            || order.client_order_id != *coid
            || order.account_id != account_id
            || !state.instances.contains_key(&order.instance_id)
            || order.order_id.trim().is_empty()
            || order.token_id.trim().is_empty()
            || !order.quantity.is_finite()
            || order.quantity <= 0.0
            || !order.filled_quantity.is_finite()
            || order.filled_quantity < -tolerance
            || order.filled_quantity > order.quantity + tolerance
            || !order.price.is_finite()
            || order.price <= 0.0
            || order.price >= 1.0
            || !order.reserved_cash.is_finite()
            || order.reserved_cash < -EPS
            || !order.reserved_quantity.is_finite()
            || order.reserved_quantity < -EPS
            || order.terminal_matched_quantity.is_some_and(|quantity| {
                !quantity.is_finite()
                    || quantity < -tolerance
                    || quantity > order.quantity + tolerance
            })
        {
            return Err(format!(
                "order `{coid}` contains invalid ownership/accounting fields"
            ));
        }
        let unique_terminal_trade_ids: HashSet<&str> = order
            .terminal_trade_ids
            .iter()
            .map(String::as_str)
            .collect();
        if order
            .terminal_trade_ids
            .iter()
            .any(|id| id.trim().is_empty())
            || unique_terminal_trade_ids.len() != order.terminal_trade_ids.len()
            || (!order.terminal_trade_ids_authoritative && !order.terminal_trade_ids.is_empty())
            || (order.terminal_trade_ids_authoritative && order.terminal_matched_quantity.is_none())
            || (order.terminal_trade_ids_authoritative
                && order.terminal_trade_ids.is_empty()
                && order
                    .terminal_matched_quantity
                    .is_some_and(|quantity| quantity > tolerance))
        {
            return Err(format!(
                "order `{coid}` contains invalid authoritative terminal trade metadata"
            ));
        }
    }

    for (order_id, coid) in &state.oid_to_coid {
        let Some(order) = state.orders.get(coid) else {
            return Err(format!(
                "order-id mapping `{order_id}` points to missing coid `{coid}`"
            ));
        };
        if order_id.trim().is_empty() || normalize_order_id(&order.order_id) != *order_id {
            return Err(format!(
                "order-id mapping `{order_id}` disagrees with order `{coid}`"
            ));
        }
    }
    for (coid, order) in &state.orders {
        if state
            .oid_to_coid
            .get(&normalize_order_id(&order.order_id))
            .is_none_or(|mapped| mapped != coid)
        {
            return Err(format!(
                "order `{coid}` is missing its durable order-id mapping"
            ));
        }
    }

    let mut max_trade_generation = 0_u64;
    let mut expected_fee_pending = HashSet::new();
    for (trade_key, trade) in &state.trades {
        let ownership = &trade.ownership;
        max_trade_generation = max_trade_generation.max(trade.ledger_generation);
        let known_status = matches!(
            ownership.status.as_str(),
            "MATCHED" | "MINED" | "CONFIRMED" | "FAILED"
        );
        let parent_matches = state
            .orders
            .get(&ownership.client_order_id)
            .is_some_and(|order| {
                order.instance_id == ownership.instance_id
                    && normalize_order_id(&order.order_id)
                        == normalize_order_id(&ownership.order_id)
                    && order.token_id == ownership.token_id
                    && order.side == ownership.side
            });
        if trade_key.trim().is_empty()
            || ownership.trade_key != *trade_key
            || ownership.account_id != account_id
            || !state.instances.contains_key(&ownership.instance_id)
            || ownership.client_order_id.trim().is_empty()
            || ownership.order_id.trim().is_empty()
            || ownership.token_id.trim().is_empty()
            || !ownership.quantity.is_finite()
            || ownership.quantity <= 0.0
            || !ownership.price.is_finite()
            || ownership.price <= 0.0
            || ownership.price >= 1.0
            || !trade.usdc_fee.is_finite()
            || trade.usdc_fee < -EPS
            || !trade.shares_fee.is_finite()
            || trade.shares_fee < -EPS
            || !known_status
            || trade.failed != (ownership.status == "FAILED")
            || (trade.failed
                && (trade.booked
                    || trade.physical_booked
                    || trade.virtual_fee_booked
                    || trade.physical_fee_booked))
            || (!trade.failed && !trade.booked)
            || trade.physical_fee_booked && !trade.physical_booked
            || trade.virtual_fee_booked && !trade.booked
            || !parent_matches
        {
            return Err(format!(
                "trade `{trade_key}` contains invalid ownership/accounting fields"
            ));
        }
        let Some(is_maker) = trade.is_maker else {
            if !state.fee_attribution_pending.contains(trade_key)
                || trade.virtual_fee_booked
                || trade.physical_fee_booked
                || trade.usdc_fee.abs() > EPS
                || trade.shares_fee.abs() > EPS
            {
                return Err(format!(
                    "trade `{trade_key}` has unknown maker/taker role without a clean pending-attribution state"
                ));
            }
            expected_fee_pending.insert(trade_key.clone());
            continue;
        };
        let fee_tolerance = reconciliation_tolerance(trade.usdc_fee, trade.shares_fee)
            .max(ownership.quantity.abs().max(1.0) * 1e-8);
        if is_maker {
            if trade.usdc_fee.abs() > fee_tolerance || trade.shares_fee.abs() > fee_tolerance {
                return Err(format!("maker trade `{trade_key}` contains non-zero fees"));
            }
            if !trade.failed && !trade.virtual_fee_booked {
                return Err(format!(
                    "maker trade `{trade_key}` is missing its explicit zero-fee booking"
                ));
            }
        } else {
            match ownership.side {
                Side::Buy if trade.usdc_fee.abs() > fee_tolerance => {
                    return Err(format!(
                        "BUY taker trade `{trade_key}` stores fee in USDC instead of shares"
                    ));
                }
                Side::Sell if trade.shares_fee.abs() > fee_tolerance => {
                    return Err(format!(
                        "SELL taker trade `{trade_key}` stores fee in shares instead of USDC"
                    ));
                }
                _ => {}
            }
            if trade.virtual_fee_booked || trade.usdc_fee > EPS || trade.shares_fee > EPS {
                let config = state
                    .token_fee_configs
                    .get(&ownership.token_id)
                    .ok_or_else(|| {
                        format!("attributed taker trade `{trade_key}` is missing token fee curve")
                    })?;
                BinaryOption::validate_polymarket_fee_curve(config.rate, config.exponent, 0)
                    .map_err(|error| {
                        format!("trade `{trade_key}` has invalid token fee curve: {error}")
                    })?;
                let notional = ownership.quantity
                    * config.rate
                    * (ownership.price * (1.0 - ownership.price)).powf(config.exponent);
                let (expected_usdc, expected_shares) = match ownership.side {
                    Side::Buy => (0.0, notional / ownership.price),
                    Side::Sell => (notional, 0.0),
                };
                if (trade.usdc_fee - expected_usdc).abs()
                    > reconciliation_tolerance(trade.usdc_fee, expected_usdc).max(fee_tolerance)
                    || (trade.shares_fee - expected_shares).abs()
                        > reconciliation_tolerance(trade.shares_fee, expected_shares)
                            .max(fee_tolerance)
                {
                    return Err(format!(
                        "taker trade `{trade_key}` fee disagrees with its durable curve"
                    ));
                }
            }
            if !trade.failed && !trade.virtual_fee_booked {
                expected_fee_pending.insert(trade_key.clone());
            }
        }
        if !trade.failed
            && trade.physical_booked
            && trade.virtual_fee_booked
            && !trade.physical_fee_booked
        {
            return Err(format!(
                "trade `{trade_key}` has virtual fee but missing physical fee booking"
            ));
        }
    }
    if state.retired_trade_ownership_tombstones.len() > MAX_RETIRED_TRADE_TOMBSTONES {
        return Err(format!(
            "retired trade ownership tombstones exceed bound: {} > {}",
            state.retired_trade_ownership_tombstones.len(),
            MAX_RETIRED_TRADE_TOMBSTONES,
        ));
    }
    for (trade_key, tombstone) in &state.retired_trade_ownership_tombstones {
        let ownership = &tombstone.ownership;
        if trade_key.trim().is_empty()
            || ownership.trade_key != *trade_key
            || ownership.account_id != account_id
            || !state.instances.contains_key(&ownership.instance_id)
            || (!tombstone.authenticated_terminal_noop
                && ownership.client_order_id.trim().is_empty())
            || ownership.order_id.trim().is_empty()
            || ownership.token_id.trim().is_empty()
            || !ownership.quantity.is_finite()
            || ownership.quantity <= 0.0
            || !ownership.price.is_finite()
            || ownership.price <= 0.0
            || ownership.price >= 1.0
            || !matches!(ownership.status.as_str(), "CONFIRMED" | "FAILED")
            || tombstone.retired_at_ms == 0
            || state.trades.contains_key(trade_key)
            || (tombstone.authenticated_terminal_noop
                && !state.settled_token_values.contains_key(&ownership.token_id))
        {
            return Err(format!(
                "retired trade tombstone `{trade_key}` contains invalid ownership fields"
            ));
        }
    }
    if max_trade_generation > state.ledger_generation {
        return Err(format!(
            "trade generation {max_trade_generation} exceeds ledger generation {}",
            state.ledger_generation,
        ));
    }

    // `filled_quantity` and every reservation counter are derived values. A
    // parseable ledger must not be allowed to invent availability by changing
    // one side of those relationships. Rebuild the expected values solely
    // from the durable order/trade/maintenance roots and compare every leaf
    // and aggregate before the persistence worker can expose the account.
    let derived_reservations = derive_reservation_aggregates(state)?;

    for (instance_id, instance) in &state.instances {
        let expected_cash = derived_reservations
            .order_cash_by_instance
            .get(instance_id)
            .copied()
            .unwrap_or(0.0);
        if (expected_cash - instance.reserved_cash).abs()
            > reconciliation_tolerance(expected_cash, instance.reserved_cash)
        {
            return Err(format!(
                "instance `{instance_id}` reserved_cash={} disagrees with derived aggregate={expected_cash}",
                instance.reserved_cash,
            ));
        }
        let expected_maintenance_cash = derived_reservations
            .maintenance_cash_by_instance
            .get(instance_id)
            .copied()
            .unwrap_or(0.0);
        if (expected_maintenance_cash - instance.maintenance_reserved_cash).abs()
            > reconciliation_tolerance(
                expected_maintenance_cash,
                instance.maintenance_reserved_cash,
            )
        {
            return Err(format!(
                "instance `{instance_id}` maintenance_reserved_cash={} disagrees with operation aggregate={expected_maintenance_cash}",
                instance.maintenance_reserved_cash,
            ));
        }
        let expected_positions = derived_reservations
            .order_positions_by_instance
            .get(instance_id);
        let mut reservation_tokens: HashSet<String> =
            instance.reserved_positions.keys().cloned().collect();
        if let Some(expected) = expected_positions {
            reservation_tokens.extend(expected.keys().cloned());
        }
        for token in reservation_tokens {
            let stored = instance
                .reserved_positions
                .get(&token)
                .copied()
                .unwrap_or(0.0);
            let expected = expected_positions
                .and_then(|positions| positions.get(&token))
                .copied()
                .unwrap_or(0.0);
            if (stored - expected).abs() > reconciliation_tolerance(stored, expected) {
                return Err(format!(
                    "instance `{instance_id}` reserved position `{token}`={stored} disagrees with derived aggregate={expected}",
                ));
            }
        }
        let expected_maintenance_positions = derived_reservations
            .maintenance_positions_by_instance
            .get(instance_id);
        let mut maintenance_tokens: HashSet<String> = instance
            .maintenance_reserved_positions
            .keys()
            .cloned()
            .collect();
        if let Some(expected) = expected_maintenance_positions {
            maintenance_tokens.extend(expected.keys().cloned());
        }
        for token in maintenance_tokens {
            let stored = instance
                .maintenance_reserved_positions
                .get(&token)
                .copied()
                .unwrap_or(0.0);
            let expected = expected_maintenance_positions
                .and_then(|positions| positions.get(&token))
                .copied()
                .unwrap_or(0.0);
            if (stored - expected).abs() > reconciliation_tolerance(stored, expected) {
                return Err(format!(
                    "instance `{instance_id}` maintenance reserved position `{token}`={stored} disagrees with operation aggregate={expected}",
                ));
            }
        }
    }

    if state.fee_attribution_pending != expected_fee_pending {
        let missing: Vec<_> = expected_fee_pending
            .difference(&state.fee_attribution_pending)
            .cloned()
            .collect();
        let extraneous: Vec<_> = state
            .fee_attribution_pending
            .difference(&expected_fee_pending)
            .cloned()
            .collect();
        return Err(format!(
            "fee-attribution pending relationship is not bidirectional: missing={missing:?} extraneous={extraneous:?}"
        ));
    }
    for coid in &state.recovery_pending_orders {
        if !state.orders.contains_key(coid) {
            return Err(format!(
                "recovery pending set contains missing order `{coid}`"
            ));
        }
    }
    for coid in &state.routine_cancel_audits {
        let Some(order) = state.orders.get(coid) else {
            return Err(format!(
                "routine cancel audit set contains missing order `{coid}`"
            ));
        };
        if order.status != OrderStatus::Cancelled
            || order.terminal_matched_quantity.is_some()
            || (state.recovery_pending_orders.contains(coid)
                && !state.startup_query_repair_orders.contains(coid))
        {
            return Err(format!(
                "routine cancel audit `{coid}` must be a distinct cancelled order without terminal matched quantity"
            ));
        }
    }

    for (token, config) in &state.token_fee_configs {
        if token.trim().is_empty()
            || BinaryOption::validate_polymarket_fee_curve(config.rate, config.exponent, 0).is_err()
        {
            return Err(format!("invalid token fee config `{token}`"));
        }
    }
    for (token, value) in &state.settled_token_values {
        if token.trim().is_empty() || !value.is_finite() || !(*value == 0.0 || *value == 1.0) {
            return Err(format!("invalid settled token value `{token}`={value}"));
        }
    }
    for (sidecar_id, checkpoint) in &state.sidecar_checkpoints {
        if sidecar_id.trim().is_empty()
            || checkpoint.generation == 0
            || checkpoint.expected_entries == 0
            || checkpoint.recovery_payload.trim().is_empty()
        {
            return Err(format!("invalid sidecar checkpoint `{sidecar_id}`"));
        }
        if !matches!(
            serde_json::from_str::<serde_json::Value>(&checkpoint.recovery_payload),
            Ok(serde_json::Value::Object(_))
        ) {
            return Err(format!(
                "sidecar checkpoint `{sidecar_id}` has an invalid recovery payload"
            ));
        }
    }
    for (operation_id, operation) in &state.maintenance_ops {
        if operation_id.trim().is_empty()
            || operation.operation_id != *operation_id
            || operation.condition_id.trim().is_empty()
            || operation.up_token_id.trim().is_empty()
            || operation.down_token_id.trim().is_empty()
            || operation.up_token_id == operation.down_token_id
            || operation.allocations.iter().any(|(instance_id, value)| {
                !state.instances.contains_key(instance_id) || !value.is_finite() || *value < 0.0
            })
        {
            return Err(format!("invalid maintenance operation `{operation_id}`"));
        }
    }
    for (operation_id, adjustment) in &state.external_adjustments {
        if operation_id.trim().is_empty()
            || adjustment.operation_id != *operation_id
            || !state.instances.contains_key(&adjustment.instance_id)
            || !adjustment.cash_delta.is_finite()
        {
            return Err(format!("invalid external adjustment `{operation_id}`"));
        }
        validate_named_values(
            &format!("external adjustment `{operation_id}` deltas"),
            &adjustment.position_deltas,
            false,
        )?;
    }
    for (operation_id, migration) in &state.cash_allocation_migrations {
        if operation_id.trim().is_empty()
            || migration.operation_id != *operation_id
            || migration.target_weights.is_empty()
            || migration.target_weights.iter().any(|(instance_id, value)| {
                instance_id.trim().is_empty() || !value.is_finite() || *value <= 0.0
            })
            || migration.cash_before.iter().any(|(instance_id, value)| {
                instance_id.trim().is_empty() || !value.is_finite() || *value < -EPS
            })
            || migration.cash_after.iter().any(|(instance_id, value)| {
                instance_id.trim().is_empty() || !value.is_finite() || *value < -EPS
            })
        {
            return Err(format!(
                "invalid cash allocation migration `{operation_id}`"
            ));
        }
    }
    if state
        .ownership_anomalies
        .iter()
        .any(|(key, reason)| key.trim().is_empty() || reason.trim().is_empty())
    {
        return Err("invalid durable ownership anomaly".to_string());
    }
    for (order_id, hint) in &state.orphan_order_anomaly_hints {
        if order_id.trim().is_empty()
            || normalize_order_id(&hint.order_id) != *order_id
            || hint
                .client_order_id
                .as_deref()
                .is_some_and(|coid| coid.trim().is_empty())
            || hint
                .token_id
                .as_deref()
                .is_some_and(|token| token.trim().is_empty())
            || !state.ownership_anomalies.keys().any(|key| {
                key.strip_prefix("private_event:order:")
                    .is_some_and(|candidate| normalize_order_id(candidate) == *order_id)
            })
        {
            return Err(format!("invalid orphan order anomaly hint `{order_id}`"));
        }
    }
    for (order_id, audit) in &state.retired_order_audit_tombstones {
        if order_id.trim().is_empty()
            || normalize_order_id(&audit.order_id) != *order_id
            || !matches!(audit.status, OrderStatus::Cancelled | OrderStatus::Rejected)
            || !audit.original_size.is_finite()
            || (!audit.covers_any_zero_fill_size && audit.original_size <= 0.0)
            || (audit.covers_any_zero_fill_size && audit.original_size.abs() > EPS)
            || !audit.size_matched.is_finite()
            || audit.size_matched.abs() > EPS
            || !audit.associate_trades.is_empty()
            || audit.evidence.trim().is_empty()
            || audit.audited_at_ms == 0
            || audit
                .client_order_id
                .as_deref()
                .is_some_and(|coid| coid.trim().is_empty())
            || state.ownership_anomalies.keys().any(|key| {
                key.strip_prefix("private_event:order:")
                    .is_some_and(|candidate| normalize_order_id(candidate) == *order_id)
            })
            || state.orphan_order_anomaly_hints.contains_key(order_id)
        {
            return Err(format!("invalid retired orphan order audit `{order_id}`"));
        }
    }
    if state.risk_blockers.iter().any(|(source, blocker)| {
        source.trim().is_empty() || blocker.reason.trim().is_empty() || blocker.since_ms == 0
    }) {
        return Err("invalid durable risk blocker".to_string());
    }
    if state
        .unresolved_trade_match_times
        .keys()
        .any(|trade_key| trade_key.trim().is_empty())
    {
        return Err("invalid unresolved trade match-time entry".to_string());
    }

    if state.seeded {
        let replayed = replay_account_economics(state)?;
        let mut instance_ids: HashSet<String> = state.instances.keys().cloned().collect();
        instance_ids.extend(replayed.instances.keys().cloned());
        for instance_id in instance_ids {
            let stored = state.instances.get(&instance_id);
            let replayed_balance = replayed.instances.get(&instance_id);
            let empty_positions = HashMap::new();
            compare_economic_value(
                &format!("instance `{instance_id}` cash"),
                stored.map_or(0.0, |instance| instance.cash),
                replayed_balance.map_or(0.0, |balance| balance.cash),
            )?;
            compare_economic_positions(
                &format!("instance `{instance_id}` positions"),
                stored.map_or(&empty_positions, |instance| &instance.positions),
                replayed_balance.map_or(&empty_positions, |balance| &balance.positions),
            )?;
        }
    }

    let mut recomputed = state.clone();
    recompute_reconciliation(&mut recomputed, "persisted ledger validation");
    if (recomputed.unallocated_cash - state.unallocated_cash).abs()
        > reconciliation_tolerance(recomputed.unallocated_cash, state.unallocated_cash)
    {
        return Err(format!(
            "unallocated_cash={} disagrees with recomputed value={}",
            state.unallocated_cash, recomputed.unallocated_cash,
        ));
    }
    let mut unallocated_tokens: HashSet<String> =
        state.unallocated_positions.keys().cloned().collect();
    unallocated_tokens.extend(recomputed.unallocated_positions.keys().cloned());
    for token in unallocated_tokens {
        let stored = state
            .unallocated_positions
            .get(&token)
            .copied()
            .unwrap_or(0.0);
        let expected = recomputed
            .unallocated_positions
            .get(&token)
            .copied()
            .unwrap_or(0.0);
        if (stored - expected).abs() > reconciliation_tolerance(stored, expected) {
            return Err(format!(
                "unallocated position `{token}`={stored} disagrees with recomputed value={expected}",
            ));
        }
    }
    Ok(())
}

fn reconciliation_tolerance(lhs: f64, rhs: f64) -> f64 {
    RECONCILIATION_UNIT + lhs.abs().max(rhs.abs()) * 1e-12
}

fn set_ownership_anomaly(state: &mut SharedAccountState, key: String, reason: String) {
    state.ownership_anomalies.insert(key, reason.clone());
    set_uncertain(state, reason);
}

/// Return a token's authoritative binary outcome only when its registered
/// event has complementary 1/0 outcomes. A token-level settlement value alone
/// is insufficient proof because unrelated wallet changes can overlap a
/// restart snapshot.
fn proven_binary_token_value(state: &SharedAccountState, token_id: &str) -> Option<(String, f64)> {
    let mut proof = None;
    let mut mappings: Vec<(&str, &str, &str)> = state
        .instances
        .values()
        .flat_map(|instance| instance.token_interests.values())
        .filter(|interest| interest.up_token_id == token_id || interest.down_token_id == token_id)
        .map(|interest| {
            (
                interest.condition_id.as_str(),
                interest.up_token_id.as_str(),
                interest.down_token_id.as_str(),
            )
        })
        .collect();
    mappings.extend(
        state
            .maintenance_ops
            .values()
            .filter(|operation| operation.status == MaintenanceOperationStatus::Confirmed)
            .filter(|operation| {
                operation.up_token_id == token_id || operation.down_token_id == token_id
            })
            .map(|operation| {
                (
                    operation.condition_id.as_str(),
                    operation.up_token_id.as_str(),
                    operation.down_token_id.as_str(),
                )
            }),
    );
    for (condition_id, up_token_id, down_token_id) in mappings {
        let up = state.settled_token_values.get(up_token_id).copied()?;
        let down = state.settled_token_values.get(down_token_id).copied()?;
        if !((up == 1.0 && down == 0.0) || (up == 0.0 && down == 1.0)) {
            continue;
        }
        let value = if up_token_id == token_id { up } else { down };
        let candidate = (condition_id.to_string(), value);
        if proof
            .as_ref()
            .is_some_and(|existing| existing != &candidate)
        {
            return None;
        }
        proof = Some(candidate);
    }
    proof
}

/// Polymarket may redeem settled binary inventory outside the bot process.
/// Burn only tokens backed by a complete registered 1/0 outcome pair and
/// credit exactly their proven payout. Any additional positive cash remains
/// unallocated instead of being assigned to strategy inventory.
fn try_attribute_binary_redeem(state: &mut SharedAccountState) -> bool {
    if state.unallocated_cash <= EPS {
        return false;
    }
    let removed: Vec<(String, String, f64, f64)> = state
        .unallocated_positions
        .iter()
        .filter_map(|(token, delta)| {
            if *delta >= -EPS {
                return None;
            }
            proven_binary_token_value(state, token)
                .map(|(condition_id, value)| (condition_id, token.clone(), -*delta, value))
        })
        .collect();
    if removed.is_empty() {
        return false;
    }
    let removed_total: f64 = removed.iter().map(|(_, _, qty, _)| *qty).sum();
    let expected_payout: f64 = removed.iter().map(|(_, _, qty, value)| qty * value).sum();
    let tolerance = 0.02_f64.max(removed_total * 0.001);
    if expected_payout <= EPS || state.unallocated_cash + tolerance < expected_payout {
        return false;
    }
    // Attribute no more than the observed wallet cash. A tolerated
    // underpayment is spread over the proven winning payout instead of
    // crediting the theoretical $1/token and immediately creating a negative
    // reconciliation residual. Any overpayment remains unallocated.
    let attributed_payout = state.unallocated_cash.min(expected_payout);
    let payout_scale = attributed_payout / expected_payout;
    let residual_cash = state.unallocated_cash - expected_payout;
    for (_, token, qty, _) in &removed {
        let virtual_total: f64 = state
            .instances
            .values()
            .map(|instance| instance.positions.get(token).copied().unwrap_or(0.0))
            .sum();
        if virtual_total + EPS < *qty {
            return false;
        }
    }

    let observed_cash_delta = state.unallocated_cash;
    let conditions: BTreeSet<String> = removed
        .iter()
        .map(|(condition_id, _, _, _)| condition_id.clone())
        .collect();
    for (_, token, qty, value) in &removed {
        let virtual_total: f64 = state
            .instances
            .values()
            .map(|instance| instance.positions.get(token).copied().unwrap_or(0.0))
            .sum();
        if virtual_total <= EPS {
            continue;
        }
        let mut attributed = Vec::new();
        for (instance_id, instance) in &mut state.instances {
            let owned = instance.positions.get(token).copied().unwrap_or(0.0);
            if owned <= EPS {
                continue;
            }
            let burned = (owned * *qty / virtual_total).min(owned);
            *instance.positions.entry(token.clone()).or_insert(0.0) -= burned;
            let cash_delta = burned * *value * payout_scale;
            instance.cash += cash_delta;
            attributed.push((instance_id.clone(), burned, cash_delta));
        }
        for (instance_id, burned, cash_delta) in attributed {
            record_internal_external_adjustment(
                state,
                "platform_redeem",
                &instance_id,
                cash_delta,
                HashMap::from([(token.clone(), -burned)]),
            );
        }
    }
    log::info!(
        "[shared_account] inferred platform binary redeem: payout={:.6} attributed_payout={:.6} payout_scale={:.9} observed_cash_delta={:.6} residual_cash={:+.6} conditions={:?} removed={:?}",
        expected_payout,
        attributed_payout,
        payout_scale,
        observed_cash_delta,
        residual_cash,
        conditions,
        removed,
    );
    recompute_reconciliation(state, "inferred platform binary redeem");
    true
}

fn total_weight(instances: &BTreeMap<String, InstanceLedger>) -> f64 {
    instances.values().map(|instance| instance.weight).sum()
}

fn redistribute_all(state: &mut SharedAccountState) {
    let total = total_weight(&state.instances);
    if total <= 0.0 {
        state.unallocated_cash = state.physical_cash;
        state.unallocated_positions = state.physical_positions.clone();
        return;
    }
    for instance in state.instances.values_mut() {
        let fraction = instance.weight / total;
        instance.cash = state.physical_cash * fraction;
        instance.positions.clear();
    }
    let physical_tokens: HashSet<String> = state.physical_positions.keys().cloned().collect();
    let instance_ids: HashSet<String> = state.instances.keys().cloned().collect();
    state
        .provisional_position_owners
        .retain(|token, owner| physical_tokens.contains(token) && instance_ids.contains(owner));
    let has_any_interest = state
        .instances
        .values()
        .any(|instance| !instance.token_interests.is_empty());
    for (token, qty) in &state.physical_positions {
        // Backward-compatible startup fallback for callers that have not yet
        // registered any event scope. Live polymaker registers scopes before
        // seeding, so the exact-token equal-allocation branch below is used.
        if !has_any_interest {
            for instance in state.instances.values_mut() {
                instance
                    .positions
                    .insert(token.clone(), *qty * instance.weight / total);
            }
            continue;
        }
        let owners: Vec<String> = state
            .instances
            .iter()
            .filter(|(_, instance)| {
                instance.token_interests.values().any(|interest| {
                    interest.up_token_id == *token || interest.down_token_id == *token
                })
            })
            .map(|(instance_id, _)| instance_id.clone())
            .collect();
        if owners.is_empty() {
            let owner = state
                .provisional_position_owners
                .get(token)
                .filter(|owner| state.instances.contains_key(*owner))
                .cloned()
                .or_else(|| state.instances.keys().next().cloned());
            if let Some(owner) = owner {
                state
                    .provisional_position_owners
                    .insert(token.clone(), owner.clone());
                if let Some(instance) = state.instances.get_mut(&owner) {
                    instance.positions.insert(token.clone(), *qty);
                }
                log::warn!(
                    "[shared_account] provisionally attributed unmatched startup token={} quantity={:.8} owner={}",
                    token,
                    qty,
                    owner,
                );
            } else {
                state.unallocated_positions.insert(token.clone(), *qty);
            }
            continue;
        }
        state.provisional_position_owners.remove(token);
        let owner_qty = *qty / owners.len() as f64;
        for instance_id in owners {
            if let Some(instance) = state.instances.get_mut(&instance_id) {
                instance.positions.insert(token.clone(), owner_qty);
            }
        }
    }
    state.unallocated_cash = 0.0;
    recompute_reconciliation(state, "startup allocation");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn persistence_test_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::OnceLock<Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn seeded_account() -> SharedAccount {
        let account = SharedAccount::new("acct");
        account.register_instance("a", 1.0);
        account.register_instance("b", 3.0);
        account.apply_physical_snapshot(400.0, HashMap::from([("UP".into(), 40.0)]));
        account
    }

    fn persisted_buy_reservation_state(
        account_id: &str,
        instance_reserved_cash: f64,
        order_reserved_cash: f64,
    ) -> SharedAccountState {
        let mut state = SharedAccountState::default();
        state.physical_cash = 100.0;
        let mut instance = InstanceLedger::new(1.0);
        instance.cash = 100.0;
        instance.reserved_cash = instance_reserved_cash;
        state.instances.insert("btc01".to_string(), instance);
        state.orders.insert(
            "btc01-residual".to_string(),
            OrderOwnership {
                account_id: account_id.to_string(),
                instance_id: "btc01".to_string(),
                client_order_id: "btc01-residual".to_string(),
                order_id: "0xRESIDUAL".to_string(),
                token_id: "BTC-UP".to_string(),
                side: Side::Buy,
                quantity: 1.0,
                filled_quantity: 0.0,
                terminal_matched_quantity: None,
                terminal_trade_ids: Vec::new(),
                terminal_trade_ids_authoritative: false,
                price: 0.00646972,
                fee_rate_bps: 0,
                reserved_cash: order_reserved_cash,
                reserved_quantity: 0.0,
                status: OrderStatus::Accepted,
            },
        );
        state
            .oid_to_coid
            .insert("residual".to_string(), "btc01-residual".to_string());
        state
    }

    #[test]
    fn settled_token_snapshot_generation_advances_only_on_change() {
        let account = SharedAccount::new("settlement-generation");
        assert_eq!(account.settled_token_values_snapshot(), (0, HashMap::new()));

        account.record_settled_token_values(&HashMap::from([
            ("UP".to_string(), 1.0),
            ("DOWN".to_string(), 0.0),
        ]));
        let (generation, values) = account.settled_token_values_snapshot();
        assert_eq!(generation, 1);
        assert_eq!(values.get("UP"), Some(&1.0));
        assert_eq!(values.get("DOWN"), Some(&0.0));

        account.record_settled_token_values(&values);
        assert_eq!(account.settled_token_values_snapshot().0, generation);

        account.record_settled_token_values(&HashMap::from([("UP".to_string(), 0.0)]));
        let (revised_generation, revised) = account.settled_token_values_snapshot();
        assert_eq!(revised_generation, generation + 1);
        assert_eq!(revised.get("UP"), Some(&0.0));
    }

    #[test]
    fn token_event_end_is_detected_from_each_durable_terminal_marker() {
        let retired = SharedAccount::new("retired-event-marker");
        retired.register_instance("maker", 1.0);
        retired
            .register_token_interest("maker", "retired", "RET-UP", "RET-DOWN")
            .unwrap();
        assert!(!retired.token_event_has_ended("RET-UP"));
        retired.retire_token_interest("maker", "retired");
        assert!(retired.token_event_has_ended("RET-UP"));
        assert!(retired.token_event_has_ended("RET-DOWN"));

        let audited = SharedAccount::new("settled-audit-marker");
        audited.register_instance("maker", 1.0);
        audited
            .retain_settled_event_audit(
                "maker",
                "settled",
                &["AUDIT-UP".to_string(), "AUDIT-DOWN".to_string()],
            )
            .unwrap();
        assert!(audited.token_event_has_ended("AUDIT-UP"));

        let outcome = SharedAccount::new("settled-outcome-marker");
        outcome.record_settled_token_values(&HashMap::from([("WIN".to_string(), 1.0)]));
        assert!(outcome.token_event_has_ended("WIN"));
        assert!(!outcome.token_event_has_ended("UNKNOWN"));
        assert!(!outcome.token_event_has_ended(""));
    }

    #[test]
    fn weighted_snapshot_allocation_and_default_weight() {
        let account = seeded_account();
        assert_eq!(account.instance_snapshot("a").unwrap().cash, 100.0);
        assert_eq!(account.instance_snapshot("b").unwrap().cash, 300.0);
        assert_eq!(
            account.instance_snapshot("a").unwrap().positions["UP"],
            10.0
        );

        let equal = SharedAccount::new("equal");
        equal.register_instance("a", 0.0);
        equal.register_instance("b", f64::NAN);
        equal.apply_physical_snapshot(60.0, HashMap::new());
        assert_eq!(equal.instance_snapshot("a").unwrap().cash, 30.0);
        assert_eq!(equal.instance_snapshot("b").unwrap().cash, 30.0);
    }

    #[test]
    fn seeded_membership_change_requires_explicit_cash_only_migration() {
        let account = SharedAccount::new("membership");
        account.register_instance("btc", 1.0);
        account
            .register_token_interest("btc", "btc-event", "BTC-UP", "BTC-DOWN")
            .unwrap();
        account.apply_physical_snapshot(100.0, HashMap::from([("BTC-UP".to_string(), 20.0)]));

        account.register_instance("eth", 3.0);
        account
            .register_token_interest("eth", "eth-event", "ETH-UP", "ETH-DOWN")
            .unwrap();
        assert!(account.is_uncertain());
        assert_eq!(account.instance_snapshot("btc").unwrap().cash, 100.0);
        assert_eq!(account.instance_snapshot("eth").unwrap().cash, 0.0);
        assert_eq!(
            account.instance_snapshot("btc").unwrap().positions["BTC-UP"],
            20.0
        );
        assert!(account
            .instance_snapshot("eth")
            .unwrap()
            .positions
            .is_empty());

        let weights = BTreeMap::from([("btc".to_string(), 1.0), ("eth".to_string(), 3.0)]);
        let first = account
            .migrate_cash_allocation("add-eth-v1", &weights)
            .unwrap();
        let retry = account
            .migrate_cash_allocation("add-eth-v1", &weights)
            .unwrap();
        assert_eq!(first, retry);
        assert_eq!(account.instance_snapshot("btc").unwrap().cash, 25.0);
        assert_eq!(account.instance_snapshot("eth").unwrap().cash, 75.0);
        assert_eq!(
            account.instance_snapshot("btc").unwrap().positions["BTC-UP"],
            20.0
        );
        assert!(!account.is_uncertain());

        let changed = BTreeMap::from([("btc".to_string(), 1.0), ("eth".to_string(), 1.0)]);
        assert!(account
            .migrate_cash_allocation("add-eth-v1", &changed)
            .is_err());
    }

    #[test]
    fn cash_only_migration_can_retire_old_instance_into_single_replacement() {
        let account = SharedAccount::new("replacement");
        account.register_instance("btc02", 1.0);
        account
            .apply_physical_snapshot(1_490.390_728, HashMap::new())
            .unwrap();
        account.register_instance("eth02", 1.0);

        let weights = BTreeMap::from([("eth02".to_string(), 1.0)]);
        let first = account
            .migrate_cash_allocation("zhu03-btc02-to-eth02-v1", &weights)
            .unwrap();
        let retry = account
            .migrate_cash_allocation("zhu03-btc02-to-eth02-v1", &weights)
            .unwrap();

        assert_eq!(first, retry);
        assert_eq!(account.instance_snapshot("btc02").unwrap().cash, 0.0);
        assert_eq!(
            account.instance_snapshot("eth02").unwrap().cash,
            1_490.390_728,
        );
        assert!(!account.is_uncertain());
    }

    #[test]
    fn ownership_recording_bypasses_shared_cash_limit() {
        let account = seeded_account();
        let ownership = account
            .record_order_without_admission(
                "a",
                "a-local",
                "oid-local",
                "UP",
                Side::Buy,
                1_000.0,
                1.0,
                0,
            )
            .unwrap();
        assert_eq!(ownership.reserved_cash, 1_000.0);
        assert_eq!(
            account.order_owner_by_oid("oid-local").as_deref(),
            Some("a")
        );
        assert_eq!(
            account.instance_snapshot("a").unwrap().reserved_cash,
            1_000.0
        );
    }

    #[test]
    fn reservation_checks_virtual_and_physical_limits_atomically() {
        let account = seeded_account();
        account
            .reserve_order("a", "a-1", "oid-a-1", "UP", Side::Buy, 100.0, 1.0, 0)
            .unwrap();
        let locks = account.monitoring_snapshot();
        assert_eq!(locks.reservation_control_lock.acquisitions, 0);
        assert_eq!(locks.reservation_lifecycle_lock.acquisitions, 1);
        assert_eq!(locks.reservation_coid_route_lock.acquisitions, 1);
        assert_eq!(locks.reservation_oid_route_lock.acquisitions, 1);
        let err = account
            .reserve_order("a", "a-2", "oid-a-2", "UP", Side::Buy, 0.1, 1.0, 0)
            .unwrap_err();
        assert!(matches!(
            err,
            ReservationError::InsufficientVirtualCash { .. }
        ));
        account
            .reserve_order("b", "b-1", "oid-b-1", "UP", Side::Buy, 300.0, 1.0, 0)
            .unwrap();
        assert_eq!(account.availability("b", "UP").unwrap().physical_cash, 0.0);
    }

    #[test]
    fn terminal_filled_status_keeps_reservation_until_trade_audit() {
        let account = seeded_account();
        account
            .reserve_order("a", "a-fill", "oid-fill", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();

        assert_eq!(
            account.mark_filled_pending_audit("a-fill"),
            FillAuditPendingTransition::NewlyPending,
        );
        assert_eq!(
            account.mark_filled_pending_audit("a-fill"),
            FillAuditPendingTransition::AlreadyPending,
        );
        let pending = account.instance_snapshot("a").unwrap();
        assert_eq!(pending.reserved_cash, 5.0);
        assert_eq!(account.monitoring_snapshot().recovery_pending_orders, 1);
        assert!(!account.is_uncertain());
        assert_eq!(
            account.order_audit_instance_blocker("a"),
            Some(vec!["a-fill".to_string()]),
        );
        assert!(account.order_audit_instance_blocker("b").is_none());

        assert!(account
            .apply_trade_transition_with_context(
                "trade-fill",
                "MATCHED",
                "a-fill",
                "oid-fill",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                true,
                0,
            )
            .ownership()
            .is_some());
        let audited = account.instance_snapshot("a").unwrap();
        assert_eq!(audited.reserved_cash, 0.0);
        assert_eq!(account.monitoring_snapshot().recovery_pending_orders, 0);
        assert!(!account.is_uncertain());
    }

    #[test]
    fn authoritative_terminal_audit_is_atomic_exact_and_instance_scoped() {
        let account = seeded_account();
        account
            .reserve_order("a", "a-audit", "oid-audit", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();
        let before_generation = account.order_audit_generation();
        let audit = AuthoritativeOrderAudit {
            original_size: Some("10".into()),
            size_matched: Some("4".into()),
            associate_trades: vec!["trade-audit".into()],
        };

        assert_eq!(
            account
                .apply_authoritative_order_audit("a-audit", OrderStatus::Filled, &audit)
                .unwrap(),
            FillAuditPendingTransition::NewlyPending,
        );
        assert_ne!(account.order_audit_generation(), before_generation);
        let owned = account.order("a-audit").unwrap();
        assert_eq!(owned.terminal_matched_quantity, Some(4.0));
        assert_eq!(owned.terminal_trade_ids, vec!["trade-audit"]);
        assert!(owned.terminal_trade_ids_authoritative);
        assert_eq!(owned.reserved_cash, 2.0);
        assert!(!account.is_uncertain());
        assert!(account.order_audit_instance_blocker("b").is_none());
        assert!(account.order_audit_instance_blocker("a").is_none());
        assert_eq!(account.monitoring_snapshot().recovery_pending_orders, 1);
        assert!(account
            .reserve_order(
                "a",
                "a-after-audit",
                "oid-after-audit",
                "UP",
                Side::Buy,
                1.0,
                0.5,
                0,
            )
            .is_ok());
        account.release_order("a-after-audit", OrderStatus::Cancelled);

        let duplicate_generation = account.order_audit_generation();
        assert_eq!(
            account
                .apply_authoritative_order_audit("a-audit", OrderStatus::Filled, &audit)
                .unwrap(),
            FillAuditPendingTransition::AlreadyPending,
        );
        assert_eq!(account.order_audit_generation(), duplicate_generation);

        assert!(account
            .apply_trade_transition_with_context(
                "trade-audit:oid-audit",
                "MATCHED",
                "a-audit",
                "oid-audit",
                "UP",
                Side::Buy,
                4.0,
                0.5,
                true,
                0,
            )
            .ownership()
            .is_some());
        assert!(account.terminal_order_audit_complete("a-audit"));
        assert!(account.order_audit_instance_blocker("a").is_none());
        assert_eq!(account.instance_snapshot("a").unwrap().reserved_cash, 0.0);
    }

    #[test]
    fn authoritative_zero_fill_audit_releases_without_recovery() {
        let account = seeded_account();
        account
            .reserve_order("a", "a-zero", "oid-zero", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();
        let audit = AuthoritativeOrderAudit {
            original_size: Some("10".into()),
            size_matched: Some("0".into()),
            associate_trades: Vec::new(),
        };
        assert_eq!(
            account
                .apply_authoritative_order_audit("a-zero", OrderStatus::Cancelled, &audit)
                .unwrap(),
            FillAuditPendingTransition::Resolved,
        );
        assert!(account.terminal_order_audit_complete("a-zero"));
        assert_eq!(account.monitoring_snapshot().recovery_pending_orders, 0);
        assert_eq!(account.instance_snapshot("a").unwrap().reserved_cash, 0.0);
    }

    #[test]
    fn authoritative_trade_ids_cannot_be_bypassed_by_zero_residual() {
        let account = seeded_account();
        account
            .reserve_order("a", "a-exact", "oid-exact", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();
        let audit = AuthoritativeOrderAudit {
            original_size: Some("10".into()),
            size_matched: Some("4".into()),
            associate_trades: vec!["trade-exact-a".into(), "trade-exact-b".into()],
        };
        account
            .apply_authoritative_order_audit("a-exact", OrderStatus::Filled, &audit)
            .unwrap();
        account.apply_trade_transition_with_context(
            "trade-exact-a:oid-exact",
            "MATCHED",
            "a-exact",
            "oid-exact",
            "UP",
            Side::Buy,
            4.0,
            0.5,
            true,
            0,
        );

        assert_eq!(account.instance_snapshot("a").unwrap().reserved_cash, 0.0);
        assert!(!account.terminal_order_audit_complete("a-exact"));
        assert_eq!(account.monitoring_snapshot().recovery_pending_orders, 1);
        assert!(account.order_audit_instance_blocker("a").is_none());
        assert_eq!(
            account.mark_filled_pending_audit("a-exact"),
            FillAuditPendingTransition::AlreadyPending,
        );
        assert_eq!(account.monitoring_snapshot().recovery_pending_orders, 1);
    }

    #[test]
    fn partial_buy_fill_recomputes_principal_and_fee_reservation() {
        let account = seeded_account();
        let baseline_generation = account.instance_snapshot("a").unwrap().ledger_generation;
        account
            .reserve_order(
                "a",
                "a-fee-partial",
                "oid-fee-partial",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                1_000,
            )
            .unwrap();
        assert!((account.instance_snapshot("a").unwrap().reserved_cash - 5.5).abs() < 1e-12);

        account.apply_trade_transition_with_context(
            "trade-fee-partial-1",
            "MATCHED",
            "a-fee-partial",
            "oid-fee-partial",
            "UP",
            Side::Buy,
            4.0,
            0.5,
            true,
            1,
        );
        let partial = account.instance_snapshot("a").unwrap();
        assert!((partial.reserved_cash - 3.3).abs() < 1e-12);
        assert!(partial.ledger_generation > baseline_generation);
        let restored_generation = account
            .restored_trades()
            .into_iter()
            .find(|trade| trade.ownership.trade_key == "trade-fee-partial-1")
            .unwrap()
            .ledger_generation;
        assert_eq!(restored_generation, partial.ledger_generation);
        assert!((account.order("a-fee-partial").unwrap().reserved_cash - 3.3).abs() < 1e-12);

        account.apply_trade_transition_with_context(
            "trade-fee-partial-1",
            "CONFIRMED",
            "a-fee-partial",
            "oid-fee-partial",
            "UP",
            Side::Buy,
            4.0,
            0.5,
            true,
            1,
        );
        assert!((account.instance_snapshot("a").unwrap().reserved_cash - 3.3).abs() < 1e-12);

        account.apply_trade_transition_with_context(
            "trade-fee-partial-2",
            "MATCHED",
            "a-fee-partial",
            "oid-fee-partial",
            "UP",
            Side::Buy,
            6.0,
            0.5,
            true,
            2,
        );
        assert_eq!(account.instance_snapshot("a").unwrap().reserved_cash, 0.0);
        assert_eq!(account.order("a-fee-partial").unwrap().reserved_cash, 0.0);
    }

    #[test]
    fn late_http_ack_cannot_regress_filled_order_status() {
        let account = seeded_account();
        account
            .reserve_order("a", "a-race", "oid-race", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();

        account.mark_order_status("a-race", OrderStatus::Filled);
        assert_eq!(
            account.mark_order_status_effective("a-race", OrderStatus::Accepted),
            Some(OrderStatus::Filled),
        );
        assert_eq!(account.order("a-race").unwrap().status, OrderStatus::Filled);

        account.mark_order_status("a-race", OrderStatus::NewOrderTimeout);
        assert_eq!(account.order("a-race").unwrap().status, OrderStatus::Filled);
    }

    #[test]
    fn late_http_ack_cannot_regress_partially_filled_order_status() {
        let account = seeded_account();
        account
            .reserve_order(
                "a",
                "a-partial",
                "oid-partial",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();
        account.mark_order_status("a-partial", OrderStatus::PartiallyFilled);
        assert_eq!(
            account.mark_order_status_effective("a-partial", OrderStatus::Accepted),
            Some(OrderStatus::PartiallyFilled),
        );
        assert_eq!(
            account.order("a-partial").unwrap().status,
            OrderStatus::PartiallyFilled
        );
    }

    #[test]
    fn ambiguous_cancel_reply_cannot_regress_terminal_order_status() {
        let account = seeded_account();
        account
            .reserve_order(
                "a",
                "a-cancel-race",
                "oid-cancel-race",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();
        assert!(account.mark_cancelled_pending_audit("a-cancel-race"));

        assert_eq!(
            account.mark_order_status_effective("a-cancel-race", OrderStatus::CancelUncertain,),
            Some(OrderStatus::Cancelled),
        );
        assert_eq!(
            account.mark_order_status_effective("a-cancel-race", OrderStatus::CancelOrderTimeout,),
            Some(OrderStatus::Cancelled),
        );
        assert_eq!(
            account.order("a-cancel-race").unwrap().status,
            OrderStatus::Cancelled,
        );
    }

    #[test]
    fn accepted_after_cancelled_restores_remaining_reservation_atomically() {
        let account = seeded_account();
        account
            .reserve_order(
                "a",
                "a-resurrect",
                "oid-resurrect",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                1_000,
            )
            .unwrap();
        account
            .apply_trade_transition(
                "trade-resurrect",
                "MATCHED",
                "a-resurrect",
                "oid-resurrect",
                "UP",
                Side::Buy,
                4.0,
                0.5,
            )
            .unwrap();
        assert!(!account.mark_cancelled_pending_trade_audit("a-resurrect", 4.0));
        assert_eq!(account.instance_snapshot("a").unwrap().reserved_cash, 0.0);

        assert_eq!(
            account.mark_order_status_effective("a-resurrect", OrderStatus::Accepted),
            Some(OrderStatus::Accepted),
        );
        let restored = account.order("a-resurrect").unwrap();
        assert_eq!(restored.status, OrderStatus::Accepted);
        assert_eq!(restored.terminal_matched_quantity, None);
        assert!((restored.reserved_cash - 3.3).abs() < 1e-12);
        assert!((account.instance_snapshot("a").unwrap().reserved_cash - 3.3).abs() < 1e-12);
        assert_eq!(account.monitoring_snapshot().recovery_pending_orders, 0);
    }

    #[test]
    fn durable_trade_lifecycle_rejects_replayed_regressions() {
        let account = seeded_account();
        account
            .reserve_order("a", "a-life", "oid-life", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();
        account
            .apply_trade_transition(
                "trade-life",
                "CONFIRMED",
                "a-life",
                "oid-life",
                "UP",
                Side::Buy,
                10.0,
                0.5,
            )
            .unwrap();
        let before = account.monitoring_snapshot();
        let replay = account
            .apply_trade_transition(
                "trade-life",
                "MATCHED",
                "a-life",
                "oid-life",
                "UP",
                Side::Buy,
                10.0,
                0.5,
            )
            .unwrap();
        assert_eq!(replay.status, "CONFIRMED");
        let after = account.monitoring_snapshot();
        assert_eq!(after.physical_cash, before.physical_cash);
        assert_eq!(after.physical_positions, before.physical_positions);
    }

    #[test]
    fn routine_cancel_audit_keeps_reservation_without_blocking_shared_account() {
        let account = seeded_account();
        account
            .reserve_order(
                "a",
                "a-routine-cancel",
                "oid-routine-cancel",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();

        assert!(account.mark_cancelled_pending_audit("a-routine-cancel"));
        let pending = account.monitoring_snapshot();
        assert!(!pending.uncertain);
        assert_eq!(pending.recovery_pending_orders, 0);
        assert_eq!(pending.routine_cancel_audits, 1);
        assert_eq!(pending.reserved_cash, 5.0);
        assert_eq!(
            account.pending_order_audit_ids(),
            vec!["a-routine-cancel".to_string()],
        );

        assert!(!account.mark_cancelled_pending_trade_audit("a-routine-cancel", 0.0));
        let audited = account.monitoring_snapshot();
        assert!(!audited.uncertain);
        assert_eq!(audited.recovery_pending_orders, 0);
        assert_eq!(audited.routine_cancel_audits, 0);
        assert_eq!(audited.reserved_cash, 0.0);
    }

    #[test]
    fn failed_trade_on_authoritatively_cancelled_parent_releases_restored_reservation() {
        let account = seeded_account();
        account
            .reserve_order(
                "a",
                "a-cancel-fail",
                "oid-cancel-fail",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();
        assert!(account.mark_cancelled_pending_trade_audit("a-cancel-fail", 10.0));
        assert_eq!(account.monitoring_snapshot().recovery_pending_orders, 1);

        assert!(account
            .apply_trade_transition_with_context(
                "trade-cancel-fail",
                "MATCHED",
                "a-cancel-fail",
                "oid-cancel-fail",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                true,
                0,
            )
            .ownership()
            .is_some());
        assert_eq!(account.instance_snapshot("a").unwrap().reserved_cash, 0.0);
        assert_eq!(account.monitoring_snapshot().recovery_pending_orders, 0);

        assert!(account
            .apply_trade_transition_with_context(
                "trade-cancel-fail",
                "FAILED",
                "a-cancel-fail",
                "oid-cancel-fail",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                true,
                0,
            )
            .ownership()
            .is_some());
        let instance = account.instance_snapshot("a").unwrap();
        assert_eq!(instance.reserved_cash, 0.0);
        assert_eq!(account.monitoring_snapshot().recovery_pending_orders, 0);
        assert!(!account.is_uncertain());
        let order = account.order("a-cancel-fail").unwrap();
        assert_eq!(order.status, OrderStatus::Cancelled);
        assert_eq!(order.terminal_matched_quantity, Some(0.0));
    }

    #[test]
    fn failed_cancelled_trade_retains_only_other_unreplayed_trade_reservation() {
        let account = seeded_account();
        account
            .reserve_order(
                "a",
                "a-cancel-partial-fail",
                "oid-cancel-partial-fail",
                "UP",
                Side::Buy,
                15.0,
                0.5,
                0,
            )
            .unwrap();
        assert!(account.mark_cancelled_pending_trade_audit("a-cancel-partial-fail", 15.0));

        account
            .apply_trade_transition(
                "trade-cancel-partial-fail",
                "MATCHED",
                "a-cancel-partial-fail",
                "oid-cancel-partial-fail",
                "UP",
                Side::Buy,
                10.0,
                0.5,
            )
            .unwrap();
        assert_eq!(account.instance_snapshot("a").unwrap().reserved_cash, 2.5);

        account
            .apply_trade_transition(
                "trade-cancel-partial-fail",
                "FAILED",
                "a-cancel-partial-fail",
                "oid-cancel-partial-fail",
                "UP",
                Side::Buy,
                10.0,
                0.5,
            )
            .unwrap();
        let instance = account.instance_snapshot("a").unwrap();
        assert_eq!(instance.reserved_cash, 2.5);
        assert_eq!(account.monitoring_snapshot().recovery_pending_orders, 1);
        let order = account.order("a-cancel-partial-fail").unwrap();
        assert_eq!(order.filled_quantity, 0.0);
        assert_eq!(order.terminal_matched_quantity, Some(5.0));
    }

    #[test]
    fn first_sighting_failed_trade_does_not_double_reserve() {
        let account = seeded_account();
        account
            .reserve_order("a", "a-failed", "oid-failed", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();
        assert_eq!(account.instance_snapshot("a").unwrap().reserved_cash, 5.0);

        assert!(account
            .apply_trade_transition_with_context(
                "trade-failed",
                "FAILED",
                "a-failed",
                "oid-failed",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                true,
                0,
            )
            .ownership()
            .is_some());
        assert_eq!(account.instance_snapshot("a").unwrap().reserved_cash, 5.0);
        assert_eq!(account.order("a-failed").unwrap().reserved_cash, 5.0);
        assert!(!account.is_uncertain());
    }

    #[test]
    fn sell_inventory_cannot_be_double_reserved() {
        let account = SharedAccount::new("acct");
        account.register_instance("a", 1.0);
        account.register_instance("b", 1.0);
        account.apply_physical_snapshot(0.0, HashMap::from([("UP".into(), 30.0)]));
        account
            .reserve_order("a", "a-1", "oid-a", "UP", Side::Sell, 15.0, 0.5, 0)
            .unwrap();
        let err = account
            .reserve_order("a", "a-2", "oid-a2", "UP", Side::Sell, 1.0, 0.5, 0)
            .unwrap_err();
        assert!(matches!(
            err,
            ReservationError::InsufficientVirtualPosition { .. }
        ));
        account
            .reserve_order("b", "b-1", "oid-b", "UP", Side::Sell, 15.0, 0.5, 0)
            .unwrap();
        assert_eq!(
            account.availability("b", "UP").unwrap().physical_position,
            0.0
        );
    }

    #[test]
    fn trade_is_owned_and_replay_is_idempotent() {
        let account = seeded_account();
        account
            .reserve_order("a", "a-1", "oid-a", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();
        let first = account.apply_trade_transition_with_context(
            "trade:oid-a",
            "MATCHED",
            "a-1",
            "oid-a",
            "UP",
            Side::Buy,
            10.0,
            0.5,
            true,
            0,
        );
        let first = first.ownership().unwrap();
        assert_eq!(first.instance_id, "a");
        let cash = account.instance_snapshot("a").unwrap().cash;
        let matched = account.monitoring_snapshot();
        assert_eq!(
            matched.physical_cash, 400.0,
            "MATCHED changes virtual risk before the on-chain wallet"
        );
        assert_eq!(matched.physical_positions["UP"], 40.0);
        assert!(
            !matched.uncertain,
            "pending settlement is a known reconciliation delta"
        );
        assert!(matched.unallocated_cash.abs() < EPS);
        account.apply_trade_transition_with_context(
            "trade:oid-a",
            "MINED",
            "a-1",
            "oid-a",
            "UP",
            Side::Buy,
            10.0,
            0.5,
            true,
            0,
        );
        assert_eq!(account.instance_snapshot("a").unwrap().cash, cash);
        let mined = account.monitoring_snapshot();
        assert_eq!(mined.physical_cash, 400.0);
        assert_eq!(mined.physical_positions["UP"], 40.0);
        assert!(!mined.uncertain);
        account.apply_trade_transition_with_context(
            "trade:oid-a",
            "FAILED",
            "a-1",
            "oid-a",
            "UP",
            Side::Buy,
            10.0,
            0.5,
            true,
            0,
        );
        assert_eq!(account.instance_snapshot("a").unwrap().cash, 100.0);
        let failed = account.monitoring_snapshot();
        assert_eq!(failed.physical_cash, 400.0);
        assert_eq!(failed.physical_positions["UP"], 40.0);
        assert!(
            !failed.uncertain,
            "FAILED is terminal and needs no wallet audit"
        );
        assert!(!account.is_uncertain());
        account.apply_trade_transition_with_context(
            "trade:oid-a",
            "MATCHED",
            "a-1",
            "oid-a",
            "UP",
            Side::Buy,
            10.0,
            0.5,
            true,
            0,
        );
        assert_eq!(
            account.instance_snapshot("a").unwrap().cash,
            100.0,
            "late MATCHED cannot resurrect a FAILED tombstone"
        );
    }

    #[test]
    fn normal_trade_transition_never_acquires_account_control_lock() {
        let account = seeded_account();
        account
            .reserve_order(
                "a",
                "a-sharded-fill",
                "oid-sharded-fill",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();
        let before = account.account_lock_acquisitions.load(Ordering::Relaxed);
        assert!(matches!(
            account.apply_trade_transition_with_context(
                "trade-sharded-fill",
                "MATCHED",
                "a-sharded-fill",
                "oid-sharded-fill",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                true,
                1,
            ),
            TradeTransitionResult::Applied(_)
        ));
        assert_eq!(
            account.account_lock_acquisitions.load(Ordering::Relaxed),
            before,
            "owned fills must remain entirely on the instance shard",
        );
        assert_eq!(
            account
                .trade_ownership("trade-sharded-fill")
                .unwrap()
                .instance_id,
            "a",
        );
        assert_eq!(
            account.account_lock_acquisitions.load(Ordering::Relaxed),
            before,
            "active trade ownership lookups must remain on the routed shard",
        );
        let restored =
            account.restored_trades_for_instance_tokens("a", &HashSet::from(["UP".to_string()]));
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].ownership.trade_key, "trade-sharded-fill");
        assert_eq!(
            account.account_lock_acquisitions.load(Ordering::Relaxed),
            before,
            "scoped trade restore must not fall back to the aggregate ledger",
        );
    }

    #[test]
    fn matched_not_broadcasted_persists_role_fee_and_replay_anchor_together() {
        let account = seeded_account();
        account
            .register_token_fee_config(&["UP".to_string()], 0.25, 2.0)
            .unwrap();
        account
            .reserve_order("a", "a-atomic", "oid-atomic", "UP", Side::Buy, 5.0, 0.4, 0)
            .unwrap();
        assert!(account
            .apply_trade_transition_with_context(
                "trade-atomic",
                "MATCHED_NOT_BROADCASTED",
                "a-atomic",
                "oid-atomic",
                "UP",
                Side::Buy,
                5.0,
                0.4,
                false,
                1_700_000_000,
            )
            .ownership()
            .is_some());
        let restored = account
            .restored_trades()
            .into_iter()
            .find(|trade| trade.ownership.trade_key == "trade-atomic")
            .unwrap();
        assert_eq!(restored.ownership.status, "MATCHED");
        assert!(!restored.is_maker);
        assert!(restored.shares_fee > 0.0);
        assert!(restored.virtual_fee_booked);
        assert_eq!(restored.match_time_secs, 1_700_000_000);
    }

    #[test]
    fn order_id_lookup_normalizes_hex_case_and_prefix() {
        let account = seeded_account();
        account
            .reserve_order(
                "a",
                "a-normalized",
                "0xAaBbCcDd",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();

        assert_eq!(account.order_owner_by_oid("AABBCCDD").as_deref(), Some("a"));
        let trade = account
            .apply_trade_transition(
                "trade-normalized",
                "MATCHED",
                "",
                "0XaAbBcCdD",
                "UP",
                Side::Buy,
                10.0,
                0.5,
            )
            .unwrap();
        assert_eq!(trade.client_order_id, "a-normalized");
        assert_eq!(trade.instance_id, "a");
    }

    #[test]
    fn trade_invariant_mismatch_is_risk_off_and_never_booked() {
        for (incoming_token, incoming_side, expected_fragment) in [
            ("DOWN", Side::Buy, "token=`DOWN`"),
            ("UP", Side::Sell, "side=Sell"),
        ] {
            let account = seeded_account();
            account
                .reserve_order("a", "a-guarded", "0xA1B2", "UP", Side::Buy, 10.0, 0.5, 0)
                .unwrap();
            let before = account.instance_snapshot("a").unwrap();

            assert!(account
                .apply_trade_transition(
                    "trade-mismatch",
                    "MATCHED",
                    "a-guarded",
                    "a1b2",
                    incoming_token,
                    incoming_side,
                    10.0,
                    0.5,
                )
                .is_none());

            let after = account.instance_snapshot("a").unwrap();
            assert_eq!(after.cash, before.cash);
            assert_eq!(after.positions, before.positions);
            assert!(account.trades().is_empty());
            let monitoring = account.monitoring_snapshot();
            assert!(monitoring.uncertain);
            assert!(monitoring
                .uncertain_reason
                .as_deref()
                .is_some_and(|reason| reason.contains(expected_fragment)));
        }
    }

    #[test]
    fn ownership_anomaly_survives_snapshot_and_correct_replay_clears_it() {
        let account = seeded_account();
        account
            .reserve_order("a", "a-sticky", "oid-sticky", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();

        assert!(account
            .apply_trade_transition(
                "trade-sticky",
                "MATCHED",
                "a-sticky",
                "oid-sticky",
                "DOWN",
                Side::Buy,
                10.0,
                0.5,
            )
            .is_none());
        account.apply_physical_snapshot(400.0, HashMap::from([("UP".into(), 40.0)]));
        assert!(
            account.is_uncertain(),
            "a wallet snapshot cannot repair ownership"
        );
        assert!(account
            .ownership_anomalies()
            .contains_key("trade:trade-sticky"));
        assert!(account.monitoring_snapshot().uncertain_reason.is_some());

        assert!(account
            .apply_trade_transition_with_context(
                "trade-sticky",
                "MATCHED",
                "a-sticky",
                "oid-sticky",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                true,
                0,
            )
            .ownership()
            .is_some());
        assert!(account.ownership_anomalies().is_empty());
        assert!(!account.is_uncertain());
    }

    #[test]
    fn trade_numeric_limit_cumulative_and_lifecycle_bounds_are_enforced() {
        for (trade_key, quantity, price) in [
            ("nan-quantity", f64::NAN, 0.5),
            ("zero-price", 1.0, 0.0),
            ("nan-price", 1.0, f64::NAN),
            ("above-binary-range", 1.0, 1.01),
        ] {
            let account = seeded_account();
            account
                .reserve_order("a", "a-num", "oid-num", "UP", Side::Buy, 10.0, 0.5, 0)
                .unwrap();
            assert!(account
                .apply_trade_transition(
                    trade_key,
                    "MATCHED",
                    "a-num",
                    "oid-num",
                    "UP",
                    Side::Buy,
                    quantity,
                    price,
                )
                .is_none());
            assert!(account.is_uncertain());
            assert!(account.trades().is_empty());
        }

        let account = seeded_account();
        account
            .reserve_order("a", "a-bounds", "oid-bounds", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();
        assert!(account
            .apply_trade_transition(
                "aggressive-fill",
                "MATCHED",
                "a-bounds",
                "oid-bounds",
                "UP",
                Side::Buy,
                1.0,
                0.51,
            )
            .is_none());
        assert!(account
            .apply_trade_transition(
                "oversized-fill",
                "MATCHED",
                "a-bounds",
                "oid-bounds",
                "UP",
                Side::Buy,
                11.0,
                0.5,
            )
            .is_none());

        let lifecycle = seeded_account();
        lifecycle
            .reserve_order(
                "a",
                "a-life-econ",
                "oid-life-econ",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();
        assert!(lifecycle
            .apply_trade_transition_with_context(
                "life-econ",
                "MATCHED",
                "a-life-econ",
                "oid-life-econ",
                "UP",
                Side::Buy,
                5.0,
                0.49,
                true,
                0,
            )
            .ownership()
            .is_some());
        assert!(lifecycle
            .apply_trade_transition(
                "life-econ",
                "MINED",
                "a-life-econ",
                "oid-life-econ",
                "UP",
                Side::Buy,
                5.1,
                0.49,
            )
            .is_none());
        assert!(lifecycle.is_uncertain());
        lifecycle
            .apply_trade_transition(
                "life-econ",
                "MINED",
                "a-life-econ",
                "oid-life-econ",
                "UP",
                Side::Buy,
                5.0,
                0.49,
            )
            .unwrap();
        assert!(!lifecycle.is_uncertain());
    }

    #[test]
    fn quantized_live_fill_within_one_usdc_atomic_unit_respects_buy_limit() {
        let account = seeded_account();
        let client_order_id = "btc03-1786695330861";
        let order_id = "0xf865122559664df0686a02e148f1fb9115e4ce7ecdc9ff1c343955832d208861";
        let token_id =
            "4198435257457475353965016703411921574448583789337517048843814114807930128350";
        let trade_key = "43535f84-454f-4302-b4cd-23b4510d9723:\
f865122559664df0686a02e148f1fb9115e4ce7ecdc9ff1c343955832d208861";
        account
            .reserve_order(
                "a",
                client_order_id,
                order_id,
                token_id,
                Side::Buy,
                10.0,
                0.77,
                0,
            )
            .unwrap();

        let transition = account.apply_trade_transition_with_context(
            trade_key,
            "MATCHED",
            client_order_id,
            order_id,
            token_id,
            Side::Buy,
            4.347825,
            0.770000172500043,
            true,
            1_786_695_717,
        );

        assert!(transition.ownership().is_some());
        assert!(account.ownership_anomalies().is_empty());
        assert!(!account.is_uncertain());
        assert!((account.order(client_order_id).unwrap().filled_quantity - 4.347825).abs() < 1e-12);

        let rejected = seeded_account();
        rejected
            .reserve_order(
                "a",
                "too-adverse",
                "oid-too-adverse",
                "UP",
                Side::Buy,
                10.0,
                0.77,
                0,
            )
            .unwrap();
        assert!(rejected
            .apply_trade_transition(
                "too-adverse-trade",
                "MATCHED",
                "too-adverse",
                "oid-too-adverse",
                "UP",
                Side::Buy,
                4.347825,
                0.770001,
            )
            .is_none());
        assert!(rejected.is_uncertain());
    }

    #[test]
    fn physical_snapshot_generation_ignores_duplicate_and_stale_fanout() {
        let account = SharedAccount::new("acct");
        account.register_instance("a", 1.0);
        assert!(account
            .apply_scoped_physical_snapshot_versioned(2, 400.0, HashMap::new(), HashSet::new(),)
            .unwrap());
        assert!(!account
            .apply_scoped_physical_snapshot_versioned(2, 100.0, HashMap::new(), HashSet::new(),)
            .unwrap());
        assert!(!account
            .apply_scoped_physical_snapshot_versioned(1, 200.0, HashMap::new(), HashSet::new(),)
            .unwrap());
        assert_eq!(account.monitoring_snapshot().physical_cash, 400.0);
        assert!(!account
            .apply_scoped_physical_snapshot_versioned(3, 500.0, HashMap::new(), HashSet::new(),)
            .unwrap());
        assert_eq!(account.monitoring_snapshot().physical_cash, 400.0);
    }

    #[test]
    fn invalid_physical_snapshot_is_rejected_without_consuming_generation() {
        let account = SharedAccount::new("strict-snapshot");
        account.register_instance("a", 1.0);
        let scope = HashSet::from(["UP".to_string()]);

        assert!(account
            .apply_scoped_physical_snapshot_versioned(1, f64::NAN, HashMap::new(), scope.clone(),)
            .is_err());
        assert!(account
            .apply_scoped_physical_snapshot_versioned(
                1,
                100.0,
                HashMap::from([("UP".to_string(), f64::INFINITY)]),
                scope.clone(),
            )
            .is_err());
        assert!(!account.is_seeded());
        assert!(!account.startup_snapshot_applied());
        assert!(account
            .apply_scoped_physical_snapshot_versioned(
                1,
                100.0,
                HashMap::from([("UP".to_string(), 5.0)]),
                scope,
            )
            .unwrap());
    }

    #[test]
    fn unmatched_startup_position_gets_persistent_provisional_owner() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-provisional-owner-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        {
            let account = SharedAccount::new_persistent("provisional", &path).unwrap();
            account.register_instance("a", 1.0);
            account.register_instance("b", 1.0);
            account
                .register_token_interest("b", "live", "LIVE-UP", "LIVE-DOWN")
                .unwrap();
            account
                .apply_physical_snapshot(
                    100.0,
                    HashMap::from([
                        ("LIVE-UP".to_string(), 10.0),
                        ("HISTORICAL-WIN".to_string(), 7.0),
                    ]),
                )
                .unwrap();
            let snapshot = account.monitoring_snapshot();
            assert_eq!(
                snapshot.provisional_position_owners.get("HISTORICAL-WIN"),
                Some(&"a".to_string()),
            );
            assert_eq!(
                account.instance_snapshot("a").unwrap().positions["HISTORICAL-WIN"],
                7.0,
            );
            assert!(!snapshot
                .unallocated_positions
                .contains_key("HISTORICAL-WIN"));
            account.flush_persistence(Duration::from_secs(2)).unwrap();
        }
        let restored = SharedAccount::new_persistent("provisional", &path).unwrap();
        assert_eq!(
            restored
                .monitoring_snapshot()
                .provisional_position_owners
                .get("HISTORICAL-WIN"),
            Some(&"a".to_string()),
        );
        drop(restored);
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(PathBuf::from(lock_path));
    }

    #[test]
    fn runtime_and_durable_order_mapping_conflict_is_never_booked() {
        let account = seeded_account();
        account
            .reserve_order("a", "a-owned", "oid-a", "UP", Side::Buy, 5.0, 0.5, 0)
            .unwrap();
        account
            .reserve_order("b", "b-owned", "oid-b", "UP", Side::Buy, 5.0, 0.5, 0)
            .unwrap();
        let before_a = account.instance_snapshot("a").unwrap();
        let before_b = account.instance_snapshot("b").unwrap();

        assert!(account
            .apply_trade_transition(
                "trade-conflict",
                "MATCHED",
                "b-owned",
                "oid-a",
                "UP",
                Side::Buy,
                5.0,
                0.5,
            )
            .is_none());

        assert_eq!(account.instance_snapshot("a").unwrap().cash, before_a.cash);
        assert_eq!(account.instance_snapshot("b").unwrap().cash, before_b.cash);
        assert!(account.trades().is_empty());
        let monitoring = account.monitoring_snapshot();
        assert!(monitoring.uncertain);
        assert!(monitoring
            .uncertain_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("ownership mapping conflict")));
    }

    #[test]
    fn trade_lifecycle_cannot_switch_to_another_order_owner() {
        let account = seeded_account();
        account
            .reserve_order("a", "a-owned", "oid-a", "UP", Side::Buy, 5.0, 0.5, 0)
            .unwrap();
        account
            .reserve_order("b", "b-owned", "oid-b", "UP", Side::Buy, 5.0, 0.5, 0)
            .unwrap();
        account
            .apply_trade_transition(
                "one-taker-trade",
                "MATCHED",
                "a-owned",
                "oid-a",
                "UP",
                Side::Buy,
                5.0,
                0.5,
            )
            .unwrap();
        let before_a = account.instance_snapshot("a").unwrap();
        let before_b = account.instance_snapshot("b").unwrap();

        assert!(account
            .apply_trade_transition(
                "one-taker-trade",
                "MINED",
                "b-owned",
                "oid-b",
                "UP",
                Side::Buy,
                5.0,
                0.5,
            )
            .is_none());

        assert_eq!(account.instance_snapshot("a").unwrap().cash, before_a.cash);
        assert_eq!(account.instance_snapshot("b").unwrap().cash, before_b.cash);
        let stored = account
            .trades()
            .into_iter()
            .find(|trade| trade.trade_key == "one-taker-trade")
            .unwrap();
        assert_eq!(stored.client_order_id, "a-owned");
        assert_eq!(stored.status, "MATCHED");
        let monitoring = account.monitoring_snapshot();
        assert!(monitoring.uncertain);
        assert!(monitoring
            .uncertain_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("lifecycle ownership changed")));
    }

    #[test]
    fn aggregate_equal_pending_trades_are_not_falsely_marked_physical() {
        let account = seeded_account();
        account
            .reserve_order("a", "a-buy", "oid-buy", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();
        account
            .reserve_order("b", "b-sell", "oid-sell", "UP", Side::Sell, 10.0, 0.5, 0)
            .unwrap();
        account.apply_trade_transition(
            "trade-buy",
            "MATCHED",
            "a-buy",
            "oid-buy",
            "UP",
            Side::Buy,
            10.0,
            0.5,
        );
        account.apply_trade_transition(
            "trade-sell",
            "MATCHED",
            "b-sell",
            "oid-sell",
            "UP",
            Side::Sell,
            10.0,
            0.5,
        );

        // The two pending wallet deltas cancel in aggregate. Equality alone
        // cannot prove either individual trade reached the chain.
        account.apply_physical_snapshot(400.0, HashMap::from([("UP".into(), 40.0)]));
        account.apply_trade_transition(
            "trade-buy",
            "CONFIRMED",
            "a-buy",
            "oid-buy",
            "UP",
            Side::Buy,
            10.0,
            0.5,
        );
        let after_first_confirmation = account.monitoring_snapshot();
        assert_eq!(after_first_confirmation.physical_cash, 400.0);
        assert_eq!(after_first_confirmation.physical_positions["UP"], 40.0);
    }

    #[test]
    fn physical_snapshot_is_deferred_until_trade_lifecycle_resolves() {
        let account = seeded_account();
        account
            .reserve_order("a", "a-buy", "oid-buy", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();
        assert!(account
            .apply_trade_transition_with_context(
                "trade-buy",
                "MATCHED",
                "a-buy",
                "oid-buy",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                true,
                0,
            )
            .ownership()
            .is_some());
        account
            .state
            .lock()
            .unwrap()
            .startup_snapshot_applied_this_process = false;

        // This wallet view may already contain trade-buy, but a snapshot has no
        // trade id and therefore cannot prove that fact. Preserve the trade-
        // driven physical ledger until the lifecycle edge arrives.
        assert!(!account
            .apply_scoped_physical_snapshot_versioned(
                1,
                395.0,
                HashMap::from([("UP".into(), 50.0)]),
                HashSet::from(["UP".into()]),
            )
            .unwrap());
        assert_eq!(account.monitoring_snapshot().physical_cash, 400.0);
        assert_eq!(account.monitoring_snapshot().physical_positions["UP"], 40.0);

        assert!(account
            .apply_trade_transition_with_context(
                "trade-buy",
                "CONFIRMED",
                "a-buy",
                "oid-buy",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                true,
                0,
            )
            .ownership()
            .is_some());
        assert_eq!(account.monitoring_snapshot().physical_cash, 400.0);
        assert_eq!(account.monitoring_snapshot().physical_positions["UP"], 40.0);
        assert!(account
            .apply_scoped_physical_snapshot_versioned(
                1,
                395.0,
                HashMap::from([("UP".into(), 50.0)]),
                HashSet::from(["UP".into()]),
            )
            .unwrap());
    }

    #[test]
    fn pruning_retains_order_mapping_and_fee_curve_for_nonterminal_trade() {
        let account = seeded_account();
        account
            .register_token_fee_config(&["UP".to_string()], 0.02, 1.0)
            .unwrap();
        account
            .reserve_order("a", "a-buy", "oid-buy", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();
        assert!(account
            .apply_trade_transition_with_context(
                "trade-buy",
                "MATCHED",
                "a-buy",
                "oid-buy",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                false,
                0,
            )
            .ownership()
            .is_some());
        account.release_order("a-buy", OrderStatus::Filled);

        assert_eq!(
            account.prune_terminal_history(&HashSet::from(["UP".into()])),
            (0, 0),
        );
        assert_eq!(account.order_owner_by_oid("oid-buy").as_deref(), Some("a"));
        assert!(account
            .apply_trade_transition_with_context(
                "trade-buy",
                "CONFIRMED",
                "a-buy",
                "oid-buy",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                false,
                0,
            )
            .ownership()
            .is_some());

        assert_eq!(
            account.prune_terminal_history(&HashSet::from(["UP".into()])),
            (1, 1),
        );
        assert!(account.order_owner_by_oid("oid-buy").is_none());
    }

    #[test]
    fn retired_trade_replay_is_owned_noop_and_auto_recovers_matching_anomaly() {
        let account = seeded_account();
        account
            .reserve_order(
                "a",
                "a-retired",
                "oid-retired",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();
        assert!(matches!(
            account.apply_trade_transition_with_context(
                "trade-retired",
                "CONFIRMED",
                "a-retired",
                "oid-retired",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                true,
                1,
            ),
            TradeTransitionResult::Applied(_)
        ));
        account.release_order("a-retired", OrderStatus::Filled);
        let instance_before = account.instance_snapshot("a").unwrap();
        let physical_before = account.monitoring_snapshot();

        assert_eq!(
            account.prune_terminal_history(&HashSet::from(["UP".to_string()])),
            (1, 1),
        );
        assert!(account.order_owner_by_oid("oid-retired").is_none());
        assert_eq!(
            account
                .trade_ownership("trade-retired")
                .unwrap()
                .client_order_id,
            "a-retired",
        );
        assert_eq!(
            account
                .monitoring_snapshot()
                .retired_trade_ownership_tombstones,
            1,
        );

        // A mismatching replay remains fail-closed. Only an exact replay of
        // the same durable trade proof may clear its ownership anomaly.
        let rejected = account.apply_trade_transition_with_context(
            "trade-retired",
            "CONFIRMED",
            "",
            "oid-retired",
            "UP",
            Side::Buy,
            10.0,
            0.4,
            true,
            1,
        );
        assert!(matches!(rejected, TradeTransitionResult::Rejected));
        assert!(matches!(
            account.record_authenticated_terminal_trade_noop(
                "trade-retired",
                "CONFIRMED",
                "oid-retired",
                "UP",
                Side::Buy,
                10.0,
                0.4,
                true,
            ),
            TradeTransitionResult::Rejected
        ));
        assert_eq!(account.trade_ownership("trade-retired").unwrap().price, 0.5);
        assert!(account.is_uncertain());
        assert_eq!(
            account
                .monitoring_snapshot()
                .verified_trade_replay_recoveries,
            0
        );

        let replay = account.apply_trade_transition_with_context(
            "trade-retired",
            "CONFIRMED",
            "",
            "oid-retired",
            "UP",
            Side::Buy,
            10.0,
            0.5,
            true,
            1,
        );
        assert!(matches!(replay, TradeTransitionResult::OwnedNoop(_)));
        assert!(!account.is_uncertain());
        assert!(account.ownership_anomalies().is_empty());
        assert_eq!(
            account
                .monitoring_snapshot()
                .verified_trade_replay_recoveries,
            1
        );
        assert_eq!(account.instance_snapshot("a").unwrap(), instance_before);
        let physical_after = account.monitoring_snapshot();
        assert_eq!(physical_after.physical_cash, physical_before.physical_cash);
        assert_eq!(
            physical_after.physical_positions,
            physical_before.physical_positions
        );
    }

    #[test]
    fn authenticated_settled_terminal_trade_creates_economic_free_durable_noop() {
        let account = SharedAccount::new("authenticated-history");
        account.register_instance("owner", 1.0);
        account
            .apply_physical_snapshot(100.0, HashMap::from([("TOKEN".to_string(), 1.0)]))
            .unwrap();
        account.record_settled_token_values(&HashMap::from([("TOKEN".to_string(), 1.0)]));
        let before = account.monitoring_snapshot();

        assert!(matches!(
            account.apply_trade_transition_with_context(
                "historical-trade",
                "CONFIRMED",
                "",
                "historical-oid",
                "TOKEN",
                Side::Buy,
                6.24,
                0.42,
                false,
                1,
            ),
            TradeTransitionResult::Rejected
        ));
        assert!(account.is_uncertain());

        let recovered = account.record_authenticated_terminal_trade_noop(
            "historical-trade",
            "CONFIRMED",
            "historical-oid",
            "TOKEN",
            Side::Buy,
            6.24,
            0.42,
            false,
        );
        assert!(matches!(recovered, TradeTransitionResult::OwnedNoop(_)));
        assert!(!account.is_uncertain());
        assert!(account.ownership_anomalies().is_empty());
        let ownership = account.trade_ownership("historical-trade").unwrap();
        assert_eq!(ownership.instance_id, "owner");
        assert!(ownership.client_order_id.is_empty());

        let replay = account.apply_trade_transition_with_context(
            "historical-trade",
            "CONFIRMED",
            "",
            "historical-oid",
            "TOKEN",
            Side::Buy,
            6.24,
            0.42,
            false,
            1,
        );
        assert!(matches!(replay, TradeTransitionResult::OwnedNoop(_)));
        let after = account.monitoring_snapshot();
        assert_eq!(after.physical_cash, before.physical_cash);
        assert_eq!(after.physical_positions, before.physical_positions);
        assert_eq!(account.instance_snapshot("owner").unwrap().cash, 100.0);
        assert_eq!(after.retired_trade_ownership_tombstones, 1);
    }

    #[test]
    fn active_durable_trade_exact_replay_auto_recovers_without_rebooking() {
        let account = seeded_account();
        account
            .reserve_order("a", "a-active", "oid-active", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();
        assert!(matches!(
            account.apply_trade_transition_with_context(
                "trade-active",
                "MATCHED",
                "a-active",
                "oid-active",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                true,
                1,
            ),
            TradeTransitionResult::Applied(_)
        ));
        let before = account.instance_snapshot("a").unwrap();
        assert!(matches!(
            account.apply_trade_transition_with_context(
                "trade-active",
                "MATCHED",
                "",
                "oid-active",
                "UP",
                Side::Buy,
                10.0,
                0.4,
                true,
                1,
            ),
            TradeTransitionResult::Rejected
        ));
        assert!(account.is_uncertain());

        assert!(matches!(
            account.apply_trade_transition_with_context(
                "trade-active",
                "MATCHED",
                "",
                "oid-active",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                true,
                1,
            ),
            TradeTransitionResult::OwnedNoop(_)
        ));
        assert!(!account.is_uncertain());
        assert_eq!(account.instance_snapshot("a").unwrap(), before);
        assert_eq!(
            account
                .monitoring_snapshot()
                .verified_trade_replay_recoveries,
            1
        );
    }

    #[test]
    fn instance_scoped_pruning_keeps_sibling_history_on_same_token() {
        let account = seeded_account();
        for (instance, coid, oid, trade_key) in [
            ("a", "a-same", "oid-a-same", "trade-a-same"),
            ("b", "b-same", "oid-b-same", "trade-b-same"),
        ] {
            account
                .reserve_order(instance, coid, oid, "UP", Side::Buy, 2.0, 0.5, 0)
                .unwrap();
            assert!(account
                .apply_trade_transition_with_context(
                    trade_key,
                    "CONFIRMED",
                    coid,
                    oid,
                    "UP",
                    Side::Buy,
                    2.0,
                    0.5,
                    true,
                    0,
                )
                .ownership()
                .is_some());
            account.release_order(coid, OrderStatus::Filled);
        }

        assert_eq!(
            account.prune_terminal_history_for_instance("a", &HashSet::from(["UP".to_string()]),),
            (1, 1),
        );
        assert!(account.order_owner_by_oid("oid-a-same").is_none());
        assert_eq!(
            account.order_owner_by_oid("oid-b-same").as_deref(),
            Some("b"),
        );
        assert!(account
            .trades()
            .iter()
            .any(|trade| trade.trade_key == "trade-b-same"));
    }

    #[test]
    fn settled_audit_cleanup_waits_for_every_instance_reference() {
        let account = seeded_account();
        account
            .register_token_fee_config(&["UP".to_string()], 0.02, 1.0)
            .unwrap();
        for instance in ["a", "b"] {
            account
                .retain_settled_event_audit(instance, "condition", &["UP".to_string()])
                .unwrap();
        }

        account
            .release_settled_event_audit("a", "condition", &["UP".to_string()])
            .unwrap();
        assert!(!account.has_settled_gc_candidates());
        assert!(account
            .finalize_ready_settled_audit_retirements()
            .is_empty());
        assert!(account
            .state
            .lock()
            .unwrap()
            .token_fee_configs
            .contains_key("UP"));

        account
            .release_settled_event_audit("b", "condition", &["UP".to_string()])
            .unwrap();
        assert!(account.has_settled_gc_candidates());
        assert!(account.has_settled_gc_candidate_for_token("UP"));
        assert!(!account.has_settled_gc_candidate_for_token("DOWN"));
        assert_eq!(
            account.finalize_ready_settled_audit_retirements(),
            vec![HashSet::from(["UP".to_string()])],
        );
        assert!(!account.has_settled_gc_candidates());
        let state = account.lock_state();
        assert!(!state.token_fee_configs.contains_key("UP"));
        assert!(!state.settled_audit_references.contains_key("condition"));
    }

    #[test]
    fn taker_fee_follows_virtual_physical_and_failed_lifecycle() {
        let account = seeded_account();
        account
            .register_token_fee_config(&["UP".to_string()], 0.04, 1.0)
            .unwrap();
        account
            .reserve_order("a", "a-fee", "oid-fee", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();
        assert!(account
            .apply_trade_transition_with_context(
                "trade-fee:oid-fee",
                "MATCHED",
                "a-fee",
                "oid-fee",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                false,
                0,
            )
            .ownership()
            .is_some());
        assert!(account.apply_trade_fee_transition(
            "trade-fee:oid-fee",
            OrderStatus::PartiallyFilled,
            0.0,
            0.2,
        ));
        let virtual_matched = account.instance_snapshot("a").unwrap();
        assert!((virtual_matched.positions["UP"] - 19.8).abs() < EPS);
        let physical_matched = account.monitoring_snapshot();
        assert!((physical_matched.physical_positions["UP"] - 40.0).abs() < EPS);
        assert!(!physical_matched.uncertain);

        assert!(account
            .apply_trade_transition_with_context(
                "trade-fee:oid-fee",
                "MINED",
                "a-fee",
                "oid-fee",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                false,
                0,
            )
            .ownership()
            .is_some());
        assert!(account.apply_trade_fee_transition(
            "trade-fee:oid-fee",
            OrderStatus::PartiallyFilled,
            0.0,
            0.2,
        ));
        let mined = account.monitoring_snapshot();
        assert!((mined.physical_positions["UP"] - 40.0).abs() < EPS);
        assert!(!mined.uncertain);

        assert!(account
            .apply_trade_transition_with_context(
                "trade-fee:oid-fee",
                "FAILED",
                "a-fee",
                "oid-fee",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                false,
                0,
            )
            .ownership()
            .is_some());
        assert!(account.apply_trade_fee_transition(
            "trade-fee:oid-fee",
            OrderStatus::Failed,
            0.0,
            0.2,
        ));
        let reverted = account.monitoring_snapshot();
        assert!((account.instance_snapshot("a").unwrap().positions["UP"] - 10.0).abs() < EPS);
        assert!((reverted.physical_positions["UP"] - 40.0).abs() < EPS);
    }

    #[test]
    fn missing_taker_fee_curve_is_risk_off_then_replayed_from_registry() {
        let account = seeded_account();
        account
            .reserve_order(
                "a",
                "a-cold-fee",
                "oid-cold-fee",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();
        account
            .apply_trade_transition(
                "trade-cold-fee",
                "MATCHED",
                "a-cold-fee",
                "oid-cold-fee",
                "UP",
                Side::Buy,
                10.0,
                0.5,
            )
            .unwrap();

        assert!(!account.apply_configured_trade_fee(
            "trade-cold-fee",
            OrderStatus::PartiallyFilled,
            false,
        ));
        assert!(account.is_uncertain());
        assert!(account
            .monitoring_snapshot()
            .uncertain_reason
            .unwrap_or_default()
            .contains("fee attribution pending"));

        account
            .register_token_fee_config(&["UP".to_string()], 0.02, 1.0)
            .unwrap();
        assert!(!account.is_uncertain());
        assert!((account.instance_snapshot("a").unwrap().positions["UP"] - 19.9).abs() < EPS);

        account.apply_trade_transition(
            "trade-cold-fee",
            "CONFIRMED",
            "a-cold-fee",
            "oid-cold-fee",
            "UP",
            Side::Buy,
            10.0,
            0.5,
        );
        assert!(account.apply_configured_trade_fee("trade-cold-fee", OrderStatus::Filled, false,));
        assert!((account.monitoring_snapshot().physical_positions["UP"] - 40.0).abs() < EPS);
    }

    #[test]
    fn token_fee_curve_revision_reprices_virtual_trade_and_leaves_snapshot_physical() {
        let account = seeded_account();
        account
            .register_token_fee_config(&["UP".to_string()], 0.02, 1.0)
            .unwrap();
        account
            .reserve_order(
                "a",
                "a-reprice",
                "oid-reprice",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();
        assert!(matches!(
            account.apply_trade_transition_with_context(
                "trade-reprice",
                "CONFIRMED",
                "a-reprice",
                "oid-reprice",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                false,
                1,
            ),
            TradeTransitionResult::Applied(_)
        ));
        let before_generation = account.instance_snapshot("a").unwrap().ledger_generation;
        assert!((account.instance_snapshot("a").unwrap().positions["UP"] - 19.9).abs() < EPS);
        assert!((account.monitoring_snapshot().physical_positions["UP"] - 40.0).abs() < EPS);

        account
            .register_token_fee_config(&["UP".to_string()], 0.04, 1.0)
            .unwrap();

        let instance = account.instance_snapshot("a").unwrap();
        assert!((instance.positions["UP"] - 19.8).abs() < EPS);
        assert!(instance.ledger_generation > before_generation);
        assert!((account.monitoring_snapshot().physical_positions["UP"] - 40.0).abs() < EPS);
        let restored = account
            .restored_trades()
            .into_iter()
            .find(|trade| trade.ownership.trade_key == "trade-reprice")
            .unwrap();
        assert!((restored.shares_fee - 0.2).abs() < EPS);
        assert!(restored.virtual_fee_booked);
        assert!(!account.is_uncertain());
        assert!(validate_persisted_state("acct", &account.lock_state()).is_ok());
    }

    #[test]
    fn roleless_sdk_trade_is_durable_but_risk_off_until_attributed() {
        let account = seeded_account();
        account
            .reserve_order(
                "a",
                "a-role-pending",
                "oid-role-pending",
                "UP",
                Side::Buy,
                2.0,
                0.5,
                0,
            )
            .unwrap();
        assert!(account
            .apply_trade_transition(
                "trade-role-pending",
                "MATCHED",
                "a-role-pending",
                "oid-role-pending",
                "UP",
                Side::Buy,
                2.0,
                0.5,
            )
            .is_some());
        assert!(account.is_uncertain());
        assert!(account.restored_trades().is_empty());
        assert!(validate_persisted_state("acct", &account.lock_state()).is_ok());

        account
            .register_token_fee_config(&["UP".to_string()], 0.02, 1.0)
            .unwrap();
        assert!(account.apply_configured_trade_fee(
            "trade-role-pending",
            OrderStatus::PartiallyFilled,
            false,
        ));
        assert!(!account.is_uncertain());
        assert_eq!(account.restored_trades().len(), 1);
    }

    #[test]
    fn aggregate_split_keeps_instance_attribution() {
        let account = SharedAccount::new("acct");
        account.register_instance("a", 1.0);
        account.register_instance("b", 1.0);
        account.apply_physical_snapshot(100.0, HashMap::new());
        account
            .apply_split_allocations(
                "UP",
                "DOWN",
                &HashMap::from([("a".into(), 30.0), ("b".into(), 30.0)]),
            )
            .unwrap();
        assert_eq!(account.instance_snapshot("a").unwrap().cash, 20.0);
        assert_eq!(
            account.instance_snapshot("a").unwrap().positions["UP"],
            30.0
        );
        assert_eq!(
            account.instance_snapshot("b").unwrap().positions["DOWN"],
            30.0
        );
    }

    #[test]
    fn maintenance_operation_journal_is_atomic_and_idempotent() {
        let account = seeded_account();
        let allocations = HashMap::from([("a".into(), 10.0), ("b".into(), 30.0)]);
        account
            .reserve_maintenance_operation(
                "split-op-1",
                MaintenanceOperationKind::Split,
                "condition",
                "UP",
                "DOWN",
                &allocations,
            )
            .unwrap();
        assert_eq!(account.monitoring_snapshot().reserved_cash, 40.0);
        assert_eq!(
            account.monitoring_snapshot().pending_maintenance_operations,
            1
        );
        account
            .reserve_order(
                "a",
                "a-maintenance-isolation",
                "oid-maintenance-isolation",
                "UP",
                Side::Buy,
                2.0,
                0.5,
                0,
            )
            .unwrap();
        assert_eq!(account.monitoring_snapshot().reserved_cash, 41.0);
        account.release_order("a-maintenance-isolation", OrderStatus::Cancelled);
        // Order lifecycle releases only its own one-dollar reservation. The
        // operation-scoped split coverage remains intact.
        assert_eq!(account.monitoring_snapshot().reserved_cash, 40.0);
        account
            .mark_maintenance_operation_submitted("split-op-1", "tx-1")
            .unwrap();
        assert_eq!(
            account
                .maintenance_operation("split-op-1")
                .unwrap()
                .tx_id
                .as_deref(),
            Some("tx-1"),
        );

        account.confirm_maintenance_operation("split-op-1").unwrap();
        let after = account.monitoring_snapshot();
        assert_eq!(after.physical_cash, 360.0);
        assert_eq!(after.reserved_cash, 0.0);
        assert_eq!(after.pending_maintenance_operations, 0);
        assert_eq!(
            account.instance_snapshot("a").unwrap().positions["UP"],
            20.0
        );
        assert_eq!(
            account.instance_snapshot("b").unwrap().positions["DOWN"],
            30.0
        );

        // Recovery may observe the same terminal chain state more than once.
        account.confirm_maintenance_operation("split-op-1").unwrap();
        assert_eq!(account.monitoring_snapshot().physical_cash, 360.0);
    }

    #[test]
    fn concurrent_owner_trade_delta_does_not_overwrite_cold_economics() {
        let mut baseline_ledger = InstanceLedger::new(1.0);
        baseline_ledger.cash = 100.0;
        baseline_ledger.positions.insert("HOT".into(), 20.0);
        let account = VirtualAccount::new("a".into(), &baseline_ledger);
        let mut lifecycle = VirtualLifecycle::default();
        let mut state = SharedAccountState::default();
        let mut cold_ledger = baseline_ledger;
        cold_ledger.cash -= 10.0;
        cold_ledger.positions.insert("COLD-UP".into(), 10.0);
        cold_ledger.positions.insert("COLD-DOWN".into(), 10.0);
        state.instances.insert("a".into(), cold_ledger);

        // Reproduce a maker fill that commits on the owner shard while a
        // split confirmation is holding the cold control transaction.
        account.cash.add(5.0);
        account
            .positions
            .read()
            .unwrap()
            .get("HOT")
            .unwrap()
            .balance
            .add(-10.0);
        lifecycle.trades.insert(
            "trade".into(),
            AppliedTrade {
                ownership: TradeOwnership {
                    account_id: "acct".into(),
                    instance_id: "a".into(),
                    trade_key: "trade".into(),
                    client_order_id: "coid".into(),
                    order_id: "oid".into(),
                    token_id: "HOT".into(),
                    side: Side::Sell,
                    quantity: 10.0,
                    price: 0.5,
                    status: "MATCHED".into(),
                },
                booked: true,
                physical_booked: false,
                usdc_fee: 0.0,
                shares_fee: 0.0,
                virtual_fee_booked: true,
                physical_fee_booked: false,
                failed: false,
                failure_reconciled: false,
                is_maker: Some(true),
                match_time_secs: 1,
                ledger_generation: 1,
            },
        );
        account.trade_epoch.store(1, Ordering::Release);
        lifecycle
            .recent_trade_mutations
            .push_back(VirtualTradeMutationHint {
                epoch: 1,
                trade_key: "trade".into(),
                client_order_id: "coid".into(),
                token_id: "HOT".into(),
            });

        assert!(SharedAccount::merge_concurrent_trade_mutations(
            &mut state,
            &account,
            &mut lifecycle,
            Some(0),
        ));
        let merged = state.instances.get("a").unwrap();
        assert_eq!(merged.cash, 95.0);
        assert_eq!(merged.positions["HOT"], 10.0);
        assert_eq!(merged.positions["COLD-UP"], 10.0);
        assert_eq!(merged.positions["COLD-DOWN"], 10.0);
    }

    #[test]
    fn restart_repairs_incomplete_split_cash_then_auto_redeems_historical_event_once() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-maintenance-recovery-redeem-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        let account_id = "maintenance-recovery-redeem";
        let operation_id = "split-recovery";
        let account = SharedAccount::new(account_id);
        account.register_instance("btc", 1.0);
        account
            .register_token_interest("btc", "historical-event", "WIN", "LOSE")
            .unwrap();
        account
            .apply_physical_snapshot(100.0, HashMap::new())
            .unwrap();
        account
            .reserve_maintenance_operation(
                operation_id,
                MaintenanceOperationKind::Split,
                "historical-event",
                "WIN",
                "LOSE",
                &HashMap::from([("btc".into(), 25.0)]),
            )
            .unwrap();
        account
            .mark_maintenance_operation_submitted(operation_id, "0xtx")
            .unwrap();
        let before_confirmation = account.lock_state().clone();
        account.confirm_maintenance_operation(operation_id).unwrap();
        let mut after_confirmation = account.lock_state().clone();
        // The original live incident was recovered after the strategy had
        // already pruned this historical event from its interest registry.
        after_confirmation
            .instances
            .get_mut("btc")
            .unwrap()
            .token_interests
            .clear();

        write_persisted_account(
            &path,
            &PersistedAccount {
                version: PERSISTENCE_VERSION,
                account_id: account_id.into(),
                persistence_generation: 0,
                state: before_confirmation.clone(),
            },
        )
        .unwrap();
        reset_persistence_wal(&path).unwrap();
        let mut changes = persistence_json_diff(
            &serde_json::to_value(before_confirmation).unwrap(),
            &serde_json::to_value(after_confirmation).unwrap(),
        );
        changes.retain(|change| {
            !matches!(change,
                PersistenceWalChange::Set { path, .. }
                    if path == &vec!["instances".to_string(), "btc".to_string(), "cash".to_string()]
            )
        });
        let record = PersistenceWalRecord {
            version: PERSISTENCE_WAL_VERSION,
            account_id: account_id.into(),
            generation: 1,
            changes,
        };
        let mut wal_len = 0;
        append_persistence_wal(&path, &record, &mut wal_len).unwrap();

        let restored = SharedAccount::new_persistent(account_id, &path).unwrap();
        assert_eq!(restored.instance_snapshot("btc").unwrap().cash, 75.0);
        assert_eq!(
            restored.instance_snapshot("btc").unwrap().positions["WIN"],
            25.0
        );
        let recovered_scope = restored
            .token_interests()
            .into_iter()
            .find(|interest| interest.condition_id == "historical-event")
            .unwrap();
        assert_eq!(recovered_scope.up_token_id, "WIN");
        assert_eq!(recovered_scope.down_token_id, "LOSE");
        assert_eq!(recovered_scope.retire_after_ms, Some(0));
        restored.record_settled_token_values(&HashMap::from([
            ("WIN".into(), 1.0),
            ("LOSE".into(), 0.0),
        ]));
        assert!(restored.observe_platform_binary_redeem(
            100.0,
            &HashMap::new(),
            &HashSet::from(["WIN".into(), "LOSE".into()]),
        ));
        let after_redeem = restored.instance_snapshot("btc").unwrap();
        assert_eq!(after_redeem.cash, 100.0);
        assert!(after_redeem
            .positions
            .values()
            .all(|quantity| quantity.abs() <= EPS));
        restored.flush_persistence(Duration::from_secs(2)).unwrap();
        drop(restored);

        let reloaded = SharedAccount::new_persistent(account_id, &path).unwrap();
        assert_eq!(reloaded.instance_snapshot("btc").unwrap().cash, 100.0);
        assert!(reloaded
            .instance_snapshot("btc")
            .unwrap()
            .positions
            .values()
            .all(|quantity| quantity.abs() <= EPS));
        assert!(!reloaded.observe_platform_binary_redeem(
            100.0,
            &HashMap::new(),
            &HashSet::from(["WIN".into(), "LOSE".into()]),
        ));
        drop(reloaded);
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(persistence_wal_path(&path));
        let _ = std::fs::remove_file(PathBuf::from(lock_path));
    }

    #[test]
    fn confirmed_maintenance_clears_only_its_attribution_blocker() {
        let account = seeded_account();
        account
            .reserve_maintenance_operation(
                "split-scoped",
                MaintenanceOperationKind::Split,
                "condition-scoped",
                "UP-SCOPED",
                "DOWN-SCOPED",
                &HashMap::from([("a".into(), 10.0)]),
            )
            .unwrap();
        account
            .mark_maintenance_operation_submitted("split-scoped", "tx-scoped")
            .unwrap();
        account.mark_maintenance_attribution_uncertain(
            "split-scoped",
            "confirmed maintenance split attribution failed cid=condition-scoped: fixture",
        );
        assert!(account.is_uncertain());
        account
            .confirm_maintenance_operation("split-scoped")
            .unwrap();
        assert!(!account.is_uncertain());

        // Reproduce the persisted blocker emitted by the old generic/manual
        // call after the operation had already been confirmed.
        account.mark_uncertain_with_reason(
            "confirmed maintenance split attribution failed cid=condition-scoped: fixture",
        );
        assert!(account.is_uncertain());
        assert_eq!(account.repair_confirmed_maintenance_risk_blockers(), 1);
        assert!(!account.is_uncertain());

        account.mark_uncertain_with_reason("independent operator risk hold");
        assert_eq!(account.repair_confirmed_maintenance_risk_blockers(), 0);
        assert!(account.is_uncertain());
    }

    #[test]
    fn legacy_combined_maintenance_reservation_migrates_from_operation_root() {
        let account = seeded_account();
        account
            .reserve_maintenance_operation(
                "legacy-split",
                MaintenanceOperationKind::Split,
                "condition",
                "UP",
                "DOWN",
                &HashMap::from([("a".into(), 10.0)]),
            )
            .unwrap();
        let mut state = account.lock_state();
        let instance = state.instances.get_mut("a").unwrap();
        instance.reservation_scope_version = 0;
        instance.maintenance_reserved_cash = 0.0;
        // Reproduce the old bug: an order terminal transition consumed part
        // of the aggregate maintenance coverage before a restart.
        instance.reserved_cash = 7.5;
        let repairs = repair_under_reserved_instance_aggregates(&mut state).unwrap();
        let instance = state.instances.get("a").unwrap();
        assert_eq!(instance.reserved_cash, 0.0);
        assert_eq!(instance.maintenance_reserved_cash, 10.0);
        assert_eq!(instance.reservation_scope_version, 1);
        assert_eq!(repairs.len(), 1);
        assert!(validate_persisted_state("acct", &state).is_ok());
    }

    #[test]
    fn persistent_submitted_maintenance_operation_forces_restart_recovery() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-maintenance-ledger-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        {
            let account = SharedAccount::new_persistent("maintenance", &path).unwrap();
            account.register_instance("a", 1.0);
            account.apply_physical_snapshot(100.0, HashMap::new());
            account
                .reserve_maintenance_operation(
                    "split-restart",
                    MaintenanceOperationKind::Split,
                    "condition",
                    "UP",
                    "DOWN",
                    &HashMap::from([("a".into(), 25.0)]),
                )
                .unwrap();
            account
                .mark_maintenance_operation_submitted("split-restart", "tx-restart")
                .unwrap();
            account.flush_persistence(Duration::from_secs(2)).unwrap();
        }

        let restored = SharedAccount::new_persistent("maintenance", &path).unwrap();
        assert!(restored.is_uncertain());
        assert_eq!(restored.monitoring_snapshot().reserved_cash, 25.0);
        assert_eq!(restored.pending_maintenance_operations().len(), 1);
        restored.fail_maintenance_operation("split-restart", "test chain failure");
        assert!(!restored.is_uncertain());
        assert_eq!(restored.monitoring_snapshot().reserved_cash, 0.0);
        drop(restored);
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(PathBuf::from(lock_path));
    }

    #[test]
    fn aggregate_split_and_orders_share_one_atomic_cash_reservation() {
        let account = SharedAccount::new("acct");
        account.register_instance("a", 1.0);
        account.register_instance("b", 1.0);
        account.apply_physical_snapshot(100.0, HashMap::new());
        let allocations = HashMap::from([("a".into(), 30.0), ("b".into(), 30.0)]);
        account.reserve_split_allocations(&allocations).unwrap();
        let err = account
            .reserve_order("a", "a-order", "a-oid", "UP", Side::Buy, 21.0, 1.0, 0)
            .unwrap_err();
        assert!(matches!(
            err,
            ReservationError::InsufficientVirtualCash { .. }
        ));
        account
            .reserve_order("a", "a-order", "a-oid", "UP", Side::Buy, 20.0, 1.0, 0)
            .unwrap();
        assert_eq!(account.availability("b", "UP").unwrap().physical_cash, 20.0);
        account
            .confirm_reserved_split("UP", "DOWN", &allocations)
            .unwrap();
        assert_eq!(account.instance_snapshot("a").unwrap().cash, 20.0);
        assert_eq!(
            account.instance_snapshot("b").unwrap().positions["DOWN"],
            30.0
        );
    }

    #[test]
    fn redeem_payout_follows_virtual_token_ownership() {
        let account = SharedAccount::new("acct");
        account.register_instance("a", 1.0);
        account.register_instance("b", 1.0);
        account.apply_physical_snapshot(
            100.0,
            HashMap::from([("WIN".into(), 100.0), ("LOSE".into(), 100.0)]),
        );
        account
            .apply_redeemed_legs(&[("WIN".into(), 100.0, 100.0), ("LOSE".into(), 100.0, 0.0)])
            .unwrap();
        assert_eq!(account.instance_snapshot("a").unwrap().cash, 100.0);
        assert_eq!(account.instance_snapshot("b").unwrap().cash, 100.0);
        assert_eq!(
            account.instance_snapshot("a").unwrap().positions["WIN"],
            0.0
        );
    }

    #[test]
    fn aggregate_merge_preserves_instance_ownership() {
        let account = SharedAccount::new("acct");
        account.register_instance("a", 1.0);
        account.register_instance("b", 1.0);
        account.apply_physical_snapshot(
            40.0,
            HashMap::from([("UP".into(), 60.0), ("DOWN".into(), 60.0)]),
        );
        let allocations = HashMap::from([("a".into(), 10.0), ("b".into(), 20.0)]);
        account
            .apply_merge_allocations("UP", "DOWN", &allocations)
            .unwrap();
        assert_eq!(account.instance_snapshot("a").unwrap().cash, 30.0);
        assert_eq!(account.instance_snapshot("b").unwrap().cash, 40.0);
        assert_eq!(
            account.instance_snapshot("a").unwrap().positions["UP"],
            20.0
        );
        assert_eq!(
            account.instance_snapshot("b").unwrap().positions["DOWN"],
            10.0
        );
    }

    #[test]
    fn token_inventory_is_allocated_only_to_instances_trading_that_token() {
        let account = SharedAccount::new("multi-asset");
        account.register_instance("btc-a", 1.0);
        // Cash weights remain configurable, but cold token ownership is equal
        // among only the instances that trade that exact event/token.
        account.register_instance("btc-b", 3.0);
        account.register_instance("eth", 1.0);
        account
            .register_token_interest("btc-a", "btc-event", "BTC-UP", "BTC-DOWN")
            .unwrap();
        account
            .register_token_interest("btc-b", "btc-event", "BTC-UP", "BTC-DOWN")
            .unwrap();
        account
            .register_token_interest("eth", "eth-event", "ETH-UP", "ETH-DOWN")
            .unwrap();
        account.apply_physical_snapshot(
            300.0,
            HashMap::from([
                ("BTC-UP".into(), 40.0),
                ("BTC-DOWN".into(), 40.0),
                ("ETH-UP".into(), 30.0),
                ("ETH-DOWN".into(), 30.0),
            ]),
        );

        let btc_a = account.instance_snapshot("btc-a").unwrap();
        let btc_b = account.instance_snapshot("btc-b").unwrap();
        let eth = account.instance_snapshot("eth").unwrap();
        assert_eq!(btc_a.cash, 60.0);
        assert_eq!(btc_b.cash, 180.0);
        assert_eq!(eth.cash, 60.0);
        assert_eq!(btc_a.positions["BTC-UP"], 20.0);
        assert_eq!(btc_b.positions["BTC-UP"], 20.0);
        assert_eq!(eth.positions["ETH-UP"], 30.0);
        assert!(!eth.positions.contains_key("BTC-UP"));
        assert!(!btc_a.positions.contains_key("ETH-UP"));
    }

    #[test]
    fn first_snapshot_waits_for_every_same_series_token_owner() {
        let account = SharedAccount::new("registration-barrier");
        account.register_instance("btc-a", 1.0);
        account.register_instance("btc-b", 1.0);
        account.register_instance("eth", 1.0);
        account.register_market_scope("btc-a", "btc-up-or-down-5m");
        account.register_market_scope("btc-b", "btc-up-or-down-5m");
        account.register_market_scope("eth", "eth-up-or-down-5m");
        account
            .register_token_interest("btc-a", "btc-event", "BTC-UP", "BTC-DOWN")
            .unwrap();
        let tokens = HashSet::from(["BTC-UP".to_string(), "BTC-DOWN".to_string()]);
        assert!(!account
            .apply_scoped_physical_snapshot_versioned(
                7,
                90.0,
                HashMap::from([("BTC-UP".into(), 40.0), ("BTC-DOWN".into(), 40.0)]),
                tokens.clone(),
            )
            .unwrap());
        assert!(!account.monitoring_snapshot().seeded);
        account
            .register_token_interest("btc-b", "btc-event", "BTC-UP", "BTC-DOWN")
            .unwrap();
        assert!(account
            .apply_scoped_physical_snapshot_versioned(
                7,
                90.0,
                HashMap::from([("BTC-UP".into(), 40.0), ("BTC-DOWN".into(), 40.0)]),
                tokens,
            )
            .unwrap());
        assert_eq!(
            account.instance_snapshot("btc-a").unwrap().positions["BTC-UP"],
            20.0
        );
        assert_eq!(
            account.instance_snapshot("btc-b").unwrap().positions["BTC-UP"],
            20.0
        );
        assert!(!account
            .instance_snapshot("eth")
            .unwrap()
            .positions
            .contains_key("BTC-UP"));
    }

    #[test]
    fn first_snapshot_barrier_times_out_without_stealing_registered_token_ownership() {
        let account = SharedAccount::new("registration-barrier-timeout");
        account.register_instance("btc-a", 1.0);
        account.register_instance("btc-b", 1.0);
        account.register_market_scope("btc-a", "btc-up-or-down-5m");
        account.register_market_scope("btc-b", "btc-up-or-down-5m");
        account
            .register_token_interest("btc-a", "btc-event", "BTC-UP", "BTC-DOWN")
            .unwrap();
        {
            let mut state = account.lock_state();
            state.initial_token_barrier_started_ms = Some(
                wall_clock_ms()
                    .saturating_sub(INITIAL_TOKEN_BARRIER_TIMEOUT_MS)
                    .saturating_sub(1),
            );
        }
        assert!(account
            .apply_scoped_physical_snapshot_versioned(
                9,
                100.0,
                HashMap::from([("BTC-UP".into(), 40.0)]),
                HashSet::from(["BTC-UP".into(), "BTC-DOWN".into()]),
            )
            .unwrap());
        assert_eq!(account.instance_snapshot("btc-a").unwrap().cash, 50.0);
        assert_eq!(account.instance_snapshot("btc-b").unwrap().cash, 50.0);
        assert_eq!(
            account.instance_snapshot("btc-a").unwrap().positions["BTC-UP"],
            40.0
        );
        assert!(!account
            .instance_snapshot("btc-b")
            .unwrap()
            .positions
            .contains_key("BTC-UP"));
        assert_eq!(
            account
                .state
                .lock()
                .unwrap()
                .initial_token_barrier_degraded_members
                .len(),
            1,
        );
    }

    #[test]
    fn reconciliation_keeps_exact_wallet_residuals_without_uncertainty() {
        let account = SharedAccount::new("rounding-tolerance");
        account.register_instance("a", 1.0);
        account.apply_physical_snapshot(100.0, HashMap::new());
        {
            let mut state = account.lock_state();
            state.instances.get_mut("a").unwrap().cash += 0.5e-6;
            recompute_reconciliation(&mut state, "rounding test");
        }
        assert!(!account.is_uncertain());
        assert!((account.monitoring_snapshot().unallocated_cash + 0.5e-6).abs() < 1e-12);

        {
            let mut state = account.lock_state();
            state.instances.get_mut("a").unwrap().cash += 2.0e-6;
            recompute_reconciliation(&mut state, "rounding test");
        }
        assert!(!account.is_uncertain());
        assert!((account.monitoring_snapshot().unallocated_cash + 2.5e-6).abs() < 1e-12);
    }

    #[test]
    fn persistent_restart_resets_legacy_wallet_residual_uncertainty() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-legacy-wallet-residual-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        {
            let account = SharedAccount::new_persistent("legacy-wallet-residual", &path).unwrap();
            account.register_instance("a", 1.0);
            account
                .apply_physical_snapshot(100.0, HashMap::new())
                .unwrap();
            {
                let mut state = account.lock_state();
                state.physical_cash = 99.894124;
                recompute_reconciliation(&mut state, "legacy wallet residual fixture");
                state.uncertain = true;
                state.uncertain_reason = Some(
                    "token fee curve registration/revision: cash_delta=-0.105876 negative_tokens=[]"
                        .to_string(),
                );
                state.uncertain_since_ms = Some(wall_clock_ms().max(1));
                account.schedule_persist(&state);
            }
            account.flush_persistence(Duration::from_secs(2)).unwrap();
        }

        let restored = SharedAccount::new_persistent("legacy-wallet-residual", &path).unwrap();
        let snapshot = restored.monitoring_snapshot();
        assert!(!snapshot.uncertain);
        assert!(snapshot.uncertain_reason.is_none());
        assert!(snapshot.uncertain_since_ms.is_none());
        assert!((snapshot.unallocated_cash + 0.105876).abs() <= EPS);
        restored.flush_persistence(Duration::from_secs(2)).unwrap();
        drop(restored);

        let reloaded = SharedAccount::new_persistent("legacy-wallet-residual", &path).unwrap();
        assert!(!reloaded.is_uncertain());
        assert!((reloaded.monitoring_snapshot().unallocated_cash + 0.105876).abs() <= EPS);
        drop(reloaded);
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(PathBuf::from(lock_path));
    }

    #[test]
    fn negative_wallet_position_residual_does_not_set_uncertain() {
        let account = SharedAccount::new("position-residual");
        account.register_instance("a", 1.0);
        account
            .register_token_interest("a", "event", "UP", "DOWN")
            .unwrap();
        account
            .apply_physical_snapshot(
                100.0,
                HashMap::from([("UP".to_string(), 10.0), ("DOWN".to_string(), 10.0)]),
            )
            .unwrap();

        {
            let mut state = account.lock_state();
            state.physical_positions.insert("UP".to_string(), 9.5);
            recompute_reconciliation(&mut state, "wallet position residual fixture");
        }

        assert!(!account.is_uncertain());
        assert_eq!(
            account
                .monitoring_snapshot()
                .unallocated_positions
                .get("UP"),
            Some(&-0.5),
        );
    }

    #[test]
    fn scoped_snapshot_does_not_zero_another_assets_positions() {
        let account = SharedAccount::new("multi-asset");
        account.register_instance("btc", 1.0);
        account.register_instance("eth", 1.0);
        account
            .register_token_interest("btc", "btc-event", "BTC-UP", "BTC-DOWN")
            .unwrap();
        account
            .register_token_interest("eth", "eth-event", "ETH-UP", "ETH-DOWN")
            .unwrap();
        account.apply_scoped_physical_snapshot(
            100.0,
            HashMap::from([("BTC-UP".into(), 10.0), ("ETH-UP".into(), 20.0)]),
            HashSet::from([
                "BTC-UP".into(),
                "BTC-DOWN".into(),
                "ETH-UP".into(),
                "ETH-DOWN".into(),
            ]),
        );
        account
            .state
            .lock()
            .unwrap()
            .startup_snapshot_applied_this_process = false;
        account.apply_scoped_physical_snapshot(
            100.0,
            HashMap::new(),
            HashSet::from(["BTC-UP".into(), "BTC-DOWN".into()]),
        );

        let metric = account.monitoring_snapshot();
        assert!(!metric.physical_positions.contains_key("BTC-UP"));
        assert_eq!(metric.physical_positions["ETH-UP"], 20.0);
        assert_eq!(
            account.instance_snapshot("eth").unwrap().positions["ETH-UP"],
            20.0
        );
        assert!(!account.is_uncertain());
        assert_eq!(metric.unallocated_positions.get("BTC-UP"), Some(&-10.0));
    }

    #[test]
    fn external_adjustment_is_idempotent_and_advances_both_ledgers() {
        let account = SharedAccount::new("external");
        account.register_instance("a", 1.0);
        account.register_instance("b", 1.0);
        account.apply_physical_snapshot(100.0, HashMap::new());
        account
            .attribute_external_adjustment("deposit-1", "a", 20.0, HashMap::new())
            .unwrap();
        account
            .attribute_external_adjustment("deposit-1", "a", 20.0, HashMap::new())
            .unwrap();
        assert_eq!(account.instance_snapshot("a").unwrap().cash, 70.0);
        assert!(!account.is_uncertain());
        assert_eq!(account.monitoring_snapshot().physical_cash, 120.0);

        account.apply_physical_snapshot(120.0, HashMap::new());
        assert!(!account.is_uncertain());
        assert_eq!(account.monitoring_snapshot().unallocated_cash, 0.0);
    }

    #[test]
    fn platform_one_for_one_redeem_follows_existing_token_ownership() {
        let account = SharedAccount::new("platform-redeem");
        account.register_instance("btc", 1.0);
        account.register_instance("eth", 1.0);
        account
            .register_token_interest("btc", "btc-event", "BTC-WIN", "BTC-LOSE")
            .unwrap();
        account
            .register_token_interest("eth", "eth-event", "ETH-WIN", "ETH-LOSE")
            .unwrap();
        account.apply_physical_snapshot(
            100.0,
            HashMap::from([("BTC-WIN".into(), 30.0), ("ETH-WIN".into(), 20.0)]),
        );
        account.record_settled_token_values(&HashMap::from([
            ("BTC-WIN".into(), 1.0),
            ("BTC-LOSE".into(), 0.0),
        ]));
        account
            .state
            .lock()
            .unwrap()
            .startup_snapshot_applied_this_process = false;

        account.apply_physical_snapshot(130.0, HashMap::from([("ETH-WIN".into(), 20.0)]));
        assert!(!account.is_uncertain());
        assert_eq!(account.instance_snapshot("btc").unwrap().cash, 80.0);
        assert_eq!(account.instance_snapshot("eth").unwrap().cash, 50.0);
        assert_eq!(account.monitoring_snapshot().unallocated_cash, 0.0);
    }

    #[test]
    fn persistent_restart_attributes_offline_auto_redeem_including_losing_leg() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-offline-auto-redeem-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        {
            let account = SharedAccount::new_persistent("offline-redeem", &path).unwrap();
            account.register_instance("btc03", 1.0);
            account.register_instance("eth03", 1.0);
            account
                .register_token_interest("eth03", "eth-settled", "ETH-WIN", "ETH-LOSE")
                .unwrap();
            account.apply_physical_snapshot(
                100.0,
                HashMap::from([("ETH-WIN".into(), 80.0), ("ETH-LOSE".into(), 80.0)]),
            );
            account.record_settled_token_values(&HashMap::from([
                ("ETH-WIN".into(), 1.0),
                ("ETH-LOSE".into(), 0.0),
            ]));
            account.flush_persistence(Duration::from_secs(2)).unwrap();
        }

        let restored = SharedAccount::new_persistent("offline-redeem", &path).unwrap();
        // eth03 may be disabled for new trading, but it remains configured so
        // its historical cash and token ownership must survive reconciliation.
        restored.reconcile_configured_instances(&HashSet::from([
            "btc03".to_string(),
            "eth03".to_string(),
        ]));
        restored.apply_physical_snapshot(180.0, HashMap::new());

        let eth = restored.instance_snapshot("eth03").unwrap();
        assert_eq!(eth.cash, 130.0);
        assert!(eth.positions.get("ETH-WIN").copied().unwrap_or(0.0).abs() <= EPS);
        assert!(eth.positions.get("ETH-LOSE").copied().unwrap_or(0.0).abs() <= EPS);
        assert_eq!(restored.instance_snapshot("btc03").unwrap().cash, 50.0);
        assert_eq!(restored.monitoring_snapshot().unallocated_cash, 0.0);
        assert!(restored
            .monitoring_snapshot()
            .unallocated_positions
            .is_empty());
        assert!(!restored.is_uncertain());
    }

    #[test]
    fn persistent_restart_attributes_multiple_redeems_and_leaves_extra_cash_unallocated() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-offline-multiple-auto-redeem-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        {
            let account = SharedAccount::new_persistent("offline-multiple-redeem", &path).unwrap();
            account.register_instance("btc03", 1.0);
            account.register_instance("eth03", 1.0);
            account
                .register_token_interest("btc03", "btc-settled", "BTC-WIN", "BTC-LOSE")
                .unwrap();
            account
                .register_token_interest("eth03", "eth-settled", "ETH-WIN", "ETH-LOSE")
                .unwrap();
            account
                .apply_physical_snapshot(
                    100.0,
                    HashMap::from([
                        ("BTC-WIN".into(), 80.0),
                        ("BTC-LOSE".into(), 80.0),
                        ("ETH-WIN".into(), 80.0),
                        ("ETH-LOSE".into(), 80.0),
                    ]),
                )
                .unwrap();
            account.record_settled_token_values(&HashMap::from([
                ("BTC-WIN".into(), 1.0),
                ("BTC-LOSE".into(), 0.0),
                ("ETH-WIN".into(), 1.0),
                ("ETH-LOSE".into(), 0.0),
            ]));
            account.flush_persistence(Duration::from_secs(2)).unwrap();
        }

        let restored = SharedAccount::new_persistent("offline-multiple-redeem", &path).unwrap();
        restored.reconcile_configured_instances(&HashSet::from([
            "btc03".to_string(),
            "eth03".to_string(),
        ]));
        restored
            .apply_physical_snapshot(269.3462, HashMap::new())
            .unwrap();

        assert!((restored.instance_snapshot("btc03").unwrap().cash - 130.0).abs() <= EPS);
        assert!((restored.instance_snapshot("eth03").unwrap().cash - 130.0).abs() <= EPS);
        assert!((restored.monitoring_snapshot().unallocated_cash - 9.3462).abs() <= EPS);
        assert!(restored
            .monitoring_snapshot()
            .unallocated_positions
            .is_empty());
        assert!(!restored.is_uncertain());
        restored.flush_persistence(Duration::from_secs(2)).unwrap();
        drop(restored);

        let reloaded = SharedAccount::new_persistent("offline-multiple-redeem", &path).unwrap();
        assert!((reloaded.instance_snapshot("btc03").unwrap().cash - 130.0).abs() <= EPS);
        assert!((reloaded.monitoring_snapshot().unallocated_cash - 9.3462).abs() <= EPS);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn settled_outcomes_retry_a_restart_snapshot_that_arrived_first() {
        let account = SharedAccount::new("outcome-after-snapshot");
        account.register_instance("eth03", 1.0);
        account
            .register_token_interest("eth03", "eth-settled", "ETH-WIN", "ETH-LOSE")
            .unwrap();
        account
            .apply_physical_snapshot(
                100.0,
                HashMap::from([("ETH-WIN".into(), 80.0), ("ETH-LOSE".into(), 80.0)]),
            )
            .unwrap();
        account
            .state
            .lock()
            .unwrap()
            .startup_snapshot_applied_this_process = false;

        account
            .apply_physical_snapshot(189.0, HashMap::new())
            .unwrap();
        assert!(!account.is_uncertain());
        assert!((account.monitoring_snapshot().unallocated_cash - 89.0).abs() <= EPS);
        account.record_settled_token_values(&HashMap::from([
            ("ETH-WIN".into(), 1.0),
            ("ETH-LOSE".into(), 0.0),
        ]));

        let instance = account.instance_snapshot("eth03").unwrap();
        assert!((instance.cash - 180.0).abs() <= EPS);
        assert!(instance
            .positions
            .values()
            .all(|quantity| quantity.abs() <= EPS));
        assert!((account.monitoring_snapshot().unallocated_cash - 9.0).abs() <= EPS);
        assert!(!account.is_uncertain());
    }

    #[test]
    fn incomplete_binary_outcome_does_not_attribute_restart_redeem() {
        let account = SharedAccount::new("incomplete-outcome");
        account.register_instance("eth03", 1.0);
        account
            .register_token_interest("eth03", "eth-settled", "ETH-WIN", "ETH-LOSE")
            .unwrap();
        account
            .apply_physical_snapshot(100.0, HashMap::from([("ETH-WIN".into(), 80.0)]))
            .unwrap();
        account
            .state
            .lock()
            .unwrap()
            .startup_snapshot_applied_this_process = false;
        account
            .apply_physical_snapshot(189.0, HashMap::new())
            .unwrap();

        account.record_settled_token_values(&HashMap::from([("ETH-WIN".into(), 1.0)]));

        assert_eq!(account.instance_snapshot("eth03").unwrap().cash, 100.0);
        assert_eq!(
            account.instance_snapshot("eth03").unwrap().positions["ETH-WIN"],
            80.0
        );
        let metric = account.monitoring_snapshot();
        assert!(!metric.uncertain);
        assert!((metric.unallocated_cash - 89.0).abs() <= EPS);
        assert_eq!(metric.unallocated_positions.get("ETH-WIN"), Some(&-80.0));
    }

    #[test]
    fn runtime_platform_redeem_is_attributed_without_applying_a_snapshot() {
        let account = SharedAccount::new("runtime-platform-redeem");
        account.register_instance("btc", 1.0);
        account.register_instance("eth", 1.0);
        account
            .register_token_interest("btc", "btc-event", "BTC-WIN", "BTC-LOSE")
            .unwrap();
        account
            .register_token_interest("eth", "eth-event", "ETH-UP", "ETH-DOWN")
            .unwrap();
        account.apply_physical_snapshot(
            100.0,
            HashMap::from([("BTC-WIN".into(), 30.0), ("ETH-UP".into(), 20.0)]),
        );
        account.record_settled_token_values(&HashMap::from([
            ("BTC-WIN".into(), 1.0),
            ("BTC-LOSE".into(), 0.0),
        ]));
        assert!(account.observe_platform_binary_redeem(
            140.0,
            &HashMap::from([("ETH-UP".into(), 20.0)]),
            &HashSet::from(["BTC-WIN".into(), "ETH-UP".into()]),
        ));
        assert_eq!(account.monitoring_snapshot().physical_cash, 140.0);
        assert_eq!(account.instance_snapshot("btc").unwrap().cash, 80.0);
        assert_eq!(account.instance_snapshot("eth").unwrap().cash, 50.0);
        assert_eq!(account.monitoring_snapshot().unallocated_cash, 10.0);
        assert_eq!(
            account.instance_snapshot("eth").unwrap().positions["ETH-UP"],
            20.0
        );
        assert!(!account.observe_platform_binary_redeem(
            140.0,
            &HashMap::from([("ETH-UP".into(), 20.0)]),
            &HashSet::from(["BTC-WIN".into(), "ETH-UP".into()]),
        ));
        assert!(!account.is_uncertain());
    }

    #[test]
    fn runtime_platform_redeem_prorates_a_tolerated_underpayment() {
        let account = SharedAccount::new("runtime-platform-redeem-underpayment");
        account.register_instance("a", 1.0);
        account.register_instance("b", 1.0);
        account
            .register_token_interest("a", "btc-event", "BTC-WIN", "BTC-LOSE")
            .unwrap();
        account
            .register_token_interest("b", "btc-event", "BTC-WIN", "BTC-LOSE")
            .unwrap();
        account
            .apply_physical_snapshot(
                100.0,
                HashMap::from([("BTC-WIN".into(), 80.0), ("BTC-LOSE".into(), 80.0)]),
            )
            .unwrap();
        account.record_settled_token_values(&HashMap::from([
            ("BTC-WIN".into(), 1.0),
            ("BTC-LOSE".into(), 0.0),
        ]));

        assert!(account.observe_platform_binary_redeem(
            179.95,
            &HashMap::new(),
            &HashSet::from(["BTC-WIN".into(), "BTC-LOSE".into()]),
        ));

        let metric = account.monitoring_snapshot();
        assert!((metric.physical_cash - 179.95).abs() <= EPS);
        assert!(metric.unallocated_cash.abs() <= EPS);
        assert!(metric
            .physical_positions
            .values()
            .all(|qty| qty.abs() <= EPS));
        assert!((account.instance_snapshot("a").unwrap().cash - 89.975).abs() <= EPS);
        assert!((account.instance_snapshot("b").unwrap().cash - 89.975).abs() <= EPS);
        assert!(!account.is_uncertain());
    }

    #[test]
    fn restart_platform_redeem_prorates_a_tolerated_underpayment() {
        let account = SharedAccount::new("restart-platform-redeem-underpayment");
        account.register_instance("btc", 1.0);
        account
            .register_token_interest("btc", "btc-event", "BTC-WIN", "BTC-LOSE")
            .unwrap();
        account
            .apply_physical_snapshot(
                100.0,
                HashMap::from([("BTC-WIN".into(), 80.0), ("BTC-LOSE".into(), 80.0)]),
            )
            .unwrap();
        account.record_settled_token_values(&HashMap::from([
            ("BTC-WIN".into(), 1.0),
            ("BTC-LOSE".into(), 0.0),
        ]));
        account
            .state
            .lock()
            .unwrap()
            .startup_snapshot_applied_this_process = false;

        account
            .apply_physical_snapshot(179.95, HashMap::new())
            .unwrap();

        let metric = account.monitoring_snapshot();
        assert!((metric.physical_cash - 179.95).abs() <= EPS);
        assert!(metric.unallocated_cash.abs() <= EPS);
        assert!((account.instance_snapshot("btc").unwrap().cash - 179.95).abs() <= EPS);
        assert!(!account.is_uncertain());
    }

    #[test]
    fn expired_interest_with_persisted_inventory_waits_for_offline_settlement_proof() {
        let account = SharedAccount::new("offline-settlement-proof");
        account.register_instance("eth03", 1.0);
        account
            .register_token_interest("eth03", "eth-event", "ETH-WIN", "ETH-LOSE")
            .unwrap();
        account.apply_physical_snapshot(
            100.0,
            HashMap::from([("ETH-WIN".into(), 80.0), ("ETH-LOSE".into(), 80.0)]),
        );
        {
            let mut state = account.lock_state();
            state
                .instances
                .get_mut("eth03")
                .unwrap()
                .token_interests
                .get_mut("eth-event")
                .unwrap()
                .retire_after_ms = Some(0);
        }

        assert_eq!(account.token_interests().len(), 1);
        assert!(account.settled_token_values_snapshot().1.is_empty());
    }

    #[test]
    fn expired_interest_is_retained_until_settled_winner_reaches_zero() {
        let account = SharedAccount::new("late-platform-redeem");
        account.register_instance("btc", 1.0);
        account
            .register_token_interest("btc", "btc-event", "BTC-WIN", "BTC-LOSE")
            .unwrap();
        account.apply_physical_snapshot(100.0, HashMap::from([("BTC-WIN".into(), 30.0)]));
        account.record_settled_token_values(&HashMap::from([
            ("BTC-WIN".into(), 1.0),
            ("BTC-LOSE".into(), 0.0),
        ]));
        {
            let mut state = account.lock_state();
            state
                .instances
                .get_mut("btc")
                .unwrap()
                .token_interests
                .get_mut("btc-event")
                .unwrap()
                .retire_after_ms = Some(0);
        }

        assert_eq!(account.token_interests().len(), 1);
        account
            .state
            .lock()
            .unwrap()
            .startup_snapshot_applied_this_process = false;
        account.apply_scoped_physical_snapshot(
            130.0,
            HashMap::new(),
            HashSet::from(["BTC-WIN".into(), "BTC-LOSE".into()]),
        );
        assert!(account.token_interests().is_empty());
        assert_eq!(account.instance_snapshot("btc").unwrap().cash, 130.0);
    }

    #[test]
    fn equal_cash_and_token_delta_is_not_redeem_without_settlement_proof() {
        let account = SharedAccount::new("ordinary-sale");
        account.register_instance("btc", 1.0);
        account
            .register_token_interest("btc", "live-event", "LIVE-UP", "LIVE-DOWN")
            .unwrap();
        account.apply_physical_snapshot(100.0, HashMap::from([("LIVE-UP".into(), 30.0)]));
        account
            .state
            .lock()
            .unwrap()
            .startup_snapshot_applied_this_process = false;
        account.apply_physical_snapshot(130.0, HashMap::new());
        assert!(!account.is_uncertain());
        assert_eq!(account.instance_snapshot("btc").unwrap().cash, 100.0);
        assert_eq!(
            account.instance_snapshot("btc").unwrap().positions["LIVE-UP"],
            30.0
        );
        let metric = account.monitoring_snapshot();
        assert!((metric.unallocated_cash - 30.0).abs() <= EPS);
        assert_eq!(metric.unallocated_positions.get("LIVE-UP"), Some(&-30.0));
    }

    #[test]
    fn recovered_order_without_audit_retains_reservation_but_allows_quotes() {
        let account = seeded_account();
        account
            .reserve_order("a", "old-1", "oid-old-1", "UP", Side::Buy, 5.0, 0.5, 0)
            .unwrap();
        account.begin_order_recovery(["old-1"]);
        assert!(!account.is_uncertain());
        assert_eq!(
            account.order_audit_instance_blocker("a"),
            Some(vec!["old-1".to_string()]),
        );
        assert!(account.order_audit_instance_blocker("b").is_none());
        assert!(matches!(
            account.reserve_split_allocations(&HashMap::from([("a".to_string(), 1.0)])),
            Err(ReservationError::AccountInstanceBlocked { .. }),
        ));
        let sibling_split = HashMap::from([("b".to_string(), 1.0)]);
        assert!(account.reserve_split_allocations(&sibling_split).is_ok());
        account.release_split_allocations(&sibling_split);
        let availability = account.availability("a", "UP").unwrap();
        assert!(availability.effective_cash > 0.0);
        assert!(account
            .reserve_order(
                "a",
                "allowed-a",
                "oid-allowed-a",
                "UP",
                Side::Buy,
                1.0,
                0.5,
                0,
            )
            .is_ok());
        assert!(account
            .reserve_order(
                "b",
                "allowed-b",
                "oid-allowed-b",
                "UP",
                Side::Buy,
                1.0,
                0.5,
                0,
            )
            .is_ok());
        assert_eq!(account.monitoring_snapshot().recovery_pending_orders, 1);
        account.apply_physical_snapshot(400.0, HashMap::from([("UP".into(), 40.0)]));
        assert!(!account.is_uncertain());
        account.release_order("old-1", OrderStatus::Cancelled);
        account.finish_order_recovery("old-1");
        assert!(account.order_audit_instance_blocker("a").is_none());
        assert!(!account.is_uncertain());
    }

    #[test]
    fn subsystem_risk_blocker_survives_unrelated_reconciliation() {
        let account = seeded_account();
        let before_noop = account.account_lock_acquisitions.load(Ordering::Relaxed);
        assert!(!account.clear_risk_blocker("fee_attribution:absent"));
        assert_eq!(
            account.account_lock_acquisitions.load(Ordering::Relaxed),
            before_noop,
            "ordinary trade-scoped blocker clears must stay off the cold account lock",
        );
        account.set_risk_blocker("sidecar_persistence:maker", "sidecar fsync failed");
        assert!(account.is_uncertain());
        account
            .apply_physical_snapshot(400.0, HashMap::from([("UP".into(), 40.0)]))
            .unwrap();
        assert!(account.is_uncertain());
        assert!(!account.clear_risk_blocker("order_audit:other"));
        assert!(account.is_uncertain());
        assert!(account.clear_risk_blocker("sidecar_persistence:maker"));
        assert!(!account.is_uncertain());
    }

    #[test]
    fn fee_only_degradation_allows_only_passive_order_admission() {
        let account = seeded_account();
        account.set_risk_blocker(
            "fee_attribution:event-a",
            "v2 fee attribution rebuild pending",
        );
        assert!(account.is_uncertain());
        assert!(account.is_fee_degraded_only());
        assert!(account.passive_order_admission_allowed());
        assert_eq!(account.availability("a", "UP").unwrap().effective_cash, 0.0);
        assert!(
            account
                .passive_quote_availability("a", "UP")
                .unwrap()
                .effective_cash
                > 0.0
        );
        assert!(matches!(
            account.reserve_order(
                "a",
                "taker-blocked",
                "oid-taker",
                "UP",
                Side::Buy,
                1.0,
                0.5,
                0,
            ),
            Err(ReservationError::AccountUncertain),
        ));
        assert!(account
            .reserve_passive_order(
                "a",
                "maker-allowed",
                "oid-maker",
                "UP",
                Side::Buy,
                1.0,
                0.5,
                0,
            )
            .is_ok());

        account.set_risk_blocker("manual-test", "independent account fault");
        assert!(!account.is_fee_degraded_only());
        assert!(!account.passive_order_admission_allowed());
        assert!(matches!(
            account.reserve_passive_order(
                "a",
                "maker-blocked",
                "oid-maker-2",
                "UP",
                Side::Buy,
                1.0,
                0.5,
                0,
            ),
            Err(ReservationError::AccountUncertain),
        ));
    }

    #[test]
    fn removed_config_instance_with_owned_funds_fails_closed() {
        let account = seeded_account();
        account.reconcile_configured_instances(&HashSet::from(["a".to_string()]));
        assert!(account.is_uncertain());
        account.reconcile_configured_instances(&HashSet::from(["a".to_string(), "b".to_string()]));
        assert!(!account.is_uncertain());
    }

    #[test]
    fn persistent_ledger_restores_ownership_orders_and_reservations() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-shared-account-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        {
            let account = SharedAccount::new_persistent("durable", &path).unwrap();
            account.register_instance("btc", 1.0);
            account.register_instance("eth", 1.0);
            account
                .register_token_interest("btc", "btc-event", "BTC-UP", "BTC-DOWN")
                .unwrap();
            account
                .register_token_interest("eth", "eth-event", "ETH-UP", "ETH-DOWN")
                .unwrap();
            account
                .register_token_fee_config(
                    &["BTC-UP".to_string(), "BTC-DOWN".to_string()],
                    0.02,
                    1.0,
                )
                .unwrap();
            account.apply_physical_snapshot(
                200.0,
                HashMap::from([("BTC-UP".into(), 20.0), ("ETH-UP".into(), 30.0)]),
            );
            account
                .reserve_order(
                    "btc",
                    "btc-1",
                    "oid-btc-1",
                    "BTC-UP",
                    Side::Sell,
                    5.0,
                    0.5,
                    0,
                )
                .unwrap();
            account.flush_persistence(Duration::from_secs(2)).unwrap();
            let metric = account.monitoring_snapshot();
            assert!(metric.persistence_writes > 0);
            assert!(metric.persistence_flushes > 0);
            assert!(metric.persistence_write_max_us >= metric.persistence_write_last_us);
            assert!(metric.persistence_flush_max_us >= metric.persistence_flush_last_us);
            assert!(
                SharedAccount::new_persistent("durable", &path).is_err(),
                "a second process must not open the live ledger",
            );
        }

        let restored = SharedAccount::new_persistent("durable", &path).unwrap();
        assert_eq!(
            restored.order_owner_by_oid("oid-btc-1").as_deref(),
            Some("btc")
        );
        let btc = restored.instance_snapshot("btc").unwrap();
        assert_eq!(btc.positions["BTC-UP"], 20.0);
        assert_eq!(btc.reserved_positions["BTC-UP"], 5.0);
        assert_eq!(
            restored.instance_snapshot("eth").unwrap().positions["ETH-UP"],
            30.0
        );
        restored
            .reserve_order(
                "btc",
                "btc-2",
                "oid-btc-2",
                "BTC-UP",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();
        restored.apply_trade_transition(
            "trade-after-restart",
            "MATCHED",
            "btc-2",
            "oid-btc-2",
            "BTC-UP",
            Side::Buy,
            10.0,
            0.5,
        );
        assert!(restored.apply_configured_trade_fee(
            "trade-after-restart",
            OrderStatus::PartiallyFilled,
            false,
        ));
        assert!(!restored.is_uncertain());
        drop(restored);
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(PathBuf::from(lock_path));
    }

    #[test]
    fn persistent_ledger_rebuilds_pending_trade_finality_replay_anchor() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-pending-trade-finality-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        {
            let account = SharedAccount::new_persistent("pending-finality", &path).unwrap();
            account.register_instance("btc", 1.0);
            account
                .register_token_fee_config(&["TOKEN".to_string()], 0.0, 1.0)
                .unwrap();
            account
                .apply_physical_snapshot(100.0, HashMap::new())
                .unwrap();
            account
                .reserve_order(
                    "btc",
                    "btc-1",
                    "oid-1",
                    "TOKEN",
                    Side::Buy,
                    10.0,
                    0.5,
                    0,
                )
                .unwrap();
            assert!(matches!(
                account.apply_trade_transition_with_context(
                    "trade-1",
                    "MATCHED",
                    "btc-1",
                    "oid-1",
                    "TOKEN",
                    Side::Buy,
                    2.0,
                    0.5,
                    true,
                    123,
                ),
                TradeTransitionResult::Applied(_)
            ));
            account.flush_persistence(Duration::from_secs(2)).unwrap();
        }

        let restored = SharedAccount::new_persistent("pending-finality", &path).unwrap();
        assert_eq!(restored.earliest_unresolved_trade_match_time(), Some(123));
        assert!(restored.startup_snapshot_deferred_by_pending_lifecycle());
        assert!(matches!(
            restored.apply_trade_transition_with_context(
                "trade-1",
                "CONFIRMED",
                "btc-1",
                "oid-1",
                "TOKEN",
                Side::Buy,
                2.0,
                0.5,
                true,
                123,
            ),
            TradeTransitionResult::Applied(_)
        ));
        restored.resolve_unresolved_trade_match_time("trade-1");
        assert!(
            !restored.startup_snapshot_deferred_by_pending_lifecycle(),
            "confirmed trade remained unsettled: {:?}",
            restored.lock_state().trades.get("trade-1"),
        );
    }

    #[test]
    fn persistence_wal_replays_and_compacts_on_restart() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-shared-account-wal-replay-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        let wal_path = persistence_wal_path(&path);
        {
            let account = SharedAccount::new_persistent("wal-replay", &path).unwrap();
            account.register_instance("a", 1.0);
            account
                .apply_physical_snapshot(100.0, HashMap::new())
                .unwrap();
            account
                .reserve_order("a", "a-wal", "oid-wal", "UP", Side::Buy, 10.0, 0.5, 0)
                .unwrap();
            account.flush_persistence(Duration::from_secs(2)).unwrap();

            let snapshot: PersistedAccount =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            assert!(snapshot.state.orders.is_empty());
            assert!(std::fs::metadata(&wal_path).unwrap().len() > 0);
        }

        {
            let restored = SharedAccount::new_persistent("wal-replay", &path).unwrap();
            assert_eq!(restored.order_owner_by_oid("oid-wal").as_deref(), Some("a"));
            assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), 0);
            let compacted: PersistedAccount =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            assert!(compacted.persistence_generation > 0);
            assert!(compacted.state.orders.contains_key("a-wal"));
        }
        remove_persistence_test_files(&path);
    }

    #[test]
    fn typed_trade_wal_replays_virtual_economics_without_rewriting_physical_snapshot() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-shared-account-typed-trade-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        {
            let account = SharedAccount::new_persistent("typed-trade", &path).unwrap();
            account.register_instance("maker", 1.0);
            account
                .apply_physical_snapshot(100.0, HashMap::new())
                .unwrap();
            account
                .reserve_order(
                    "maker",
                    "maker-order",
                    "maker-oid",
                    "UP",
                    Side::Buy,
                    10.0,
                    0.5,
                    0,
                )
                .unwrap();
            // Drain every cold setup job so both lifecycle edges below must use
            // the typed-delta path rather than coalescing into a full fallback.
            account.flush_persistence(Duration::from_secs(2)).unwrap();
            assert!(matches!(
                account.apply_trade_transition_with_context(
                    "maker-trade",
                    "MATCHED",
                    "maker-order",
                    "maker-oid",
                    "UP",
                    Side::Buy,
                    10.0,
                    0.5,
                    true,
                    1,
                ),
                TradeTransitionResult::Applied(_)
            ));
            assert!(matches!(
                account.apply_trade_transition_with_context(
                    "maker-trade",
                    "MINED",
                    "maker-order",
                    "maker-oid",
                    "UP",
                    Side::Buy,
                    10.0,
                    0.5,
                    true,
                    1,
                ),
                TradeTransitionResult::Applied(_)
            ));
            account.flush_persistence(Duration::from_secs(2)).unwrap();
        }

        let restored = SharedAccount::new_persistent("typed-trade", &path).unwrap();
        let snapshot = restored.monitoring_snapshot();
        assert_eq!(snapshot.physical_cash, 100.0);
        assert_eq!(snapshot.physical_positions.get("UP").copied(), None);
        let maker = restored.instance_snapshot("maker").unwrap();
        assert_eq!(maker.cash, 95.0);
        assert_eq!(maker.positions.get("UP").copied(), Some(10.0));
        assert!(!snapshot.uncertain);
        assert_eq!(
            restored.trade_ownership("maker-trade").unwrap().status,
            "MINED",
        );
        drop(restored);
        remove_persistence_test_files(&path);
    }

    #[test]
    fn restart_repairs_wal_proven_stale_lifecycle_only_trade_snapshot() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-stale-trade-snapshot-recovery-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        let account_id = "stale-trade-snapshot-recovery";
        let account = SharedAccount::new(account_id);
        account.register_instance("maker", 1.0);
        account
            .apply_physical_snapshot(100.0, HashMap::from([("UP".into(), 80.0)]))
            .unwrap();
        account
            .reserve_order(
                "maker",
                "maker-order",
                "maker-oid",
                "UP",
                Side::Sell,
                10.0,
                0.62,
                0,
            )
            .unwrap();
        assert!(matches!(
            account.apply_trade_transition_with_context(
                "maker-trade",
                "MATCHED",
                "maker-order",
                "maker-oid",
                "UP",
                Side::Sell,
                10.0,
                0.62,
                true,
                1,
            ),
            TradeTransitionResult::Applied(_)
        ));
        let before = account.lock_state().clone();
        assert_eq!(before.instances["maker"].positions["UP"], 70.0);

        assert!(matches!(
            account.apply_trade_transition_with_context(
                "maker-trade",
                "MINED",
                "maker-order",
                "maker-oid",
                "UP",
                Side::Sell,
                10.0,
                0.62,
                true,
                1,
            ),
            TradeTransitionResult::Applied(_)
        ));
        let stale = account.lock_state().clone();
        // Reproduce the historical full-snapshot race: the lifecycle-only
        // MINED frame carried the pre-fill token balance even though the
        // MATCHED trade was already durably booked.

        write_persisted_account(
            &path,
            &PersistedAccount {
                version: PERSISTENCE_VERSION,
                account_id: account_id.into(),
                persistence_generation: 0,
                state: before.clone(),
            },
        )
        .unwrap();
        reset_persistence_wal(&path).unwrap();
        let stale_ledger = &stale.instances["maker"];
        let changes = materialize_persistence_job(&PersistenceJob {
            generation: 1,
            payload: PersistenceJobPayload::VirtualTrade(VirtualTradePersistenceDelta {
                instance_id: "maker".into(),
                cash: stale_ledger.cash,
                reserved_cash: stale_ledger.reserved_cash,
                token_id: "UP".into(),
                position: 80.0,
                reserved_position: stale_ledger.reserved_positions["UP"],
                client_order_id: "maker-order".into(),
                order: Some(stale.orders["maker-order"].clone()),
                trade_key: "maker-trade".into(),
                trade: Some(stale.trades["maker-trade"].clone()),
                fee_attribution_pending: stale.fee_attribution_pending.contains("maker-trade"),
                recovery_pending: stale.recovery_pending_orders.contains("maker-order"),
                routine_cancel_audit: stale.routine_cancel_audits.contains("maker-order"),
                ledger_generation: stale.ledger_generation,
            }),
        })
        .unwrap();
        let record = PersistenceWalRecord {
            version: PERSISTENCE_WAL_VERSION,
            account_id: account_id.into(),
            generation: 1,
            changes,
        };
        let mut wal_len = 0;
        append_persistence_wal(&path, &record, &mut wal_len).unwrap();

        let restored = SharedAccount::new_persistent(account_id, &path).unwrap();
        let maker = restored.instance_snapshot("maker").unwrap();
        assert_eq!(maker.positions["UP"], 70.0);
        assert_eq!(
            restored.trade_ownership("maker-trade").unwrap().status,
            "MINED",
        );
        assert!(!restored.is_uncertain());
        drop(restored);
        remove_persistence_test_files(&path);
    }

    #[test]
    fn persistence_wal_ignores_only_a_torn_final_frame() {
        use std::io::Write as _;

        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-shared-account-wal-torn-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        let wal_path = persistence_wal_path(&path);
        {
            let account = SharedAccount::new_persistent("wal-torn", &path).unwrap();
            account.register_instance("a", 1.0);
            account.flush_persistence(Duration::from_secs(2)).unwrap();
        }
        std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap()
            .write_all(b"17 deadbeef partial")
            .unwrap();

        let restored = SharedAccount::new_persistent("wal-torn", &path).unwrap();
        assert!(restored.instance_snapshot("a").is_some());
        assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), 0);
        drop(restored);
        remove_persistence_test_files(&path);
    }

    #[test]
    fn persistence_wal_rejects_a_corrupt_complete_frame() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-shared-account-wal-corrupt-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        let wal_path = persistence_wal_path(&path);
        {
            let account = SharedAccount::new_persistent("wal-corrupt", &path).unwrap();
            account.register_instance("a", 1.0);
            account.flush_persistence(Duration::from_secs(2)).unwrap();
        }
        let mut wal = std::fs::read(&wal_path).unwrap();
        let first_space = wal.iter().position(|byte| *byte == b' ').unwrap();
        let second_space = first_space
            + 1
            + wal[first_space + 1..]
                .iter()
                .position(|byte| *byte == b' ')
                .unwrap();
        wal[second_space + 1] ^= 1;
        std::fs::write(&wal_path, wal).unwrap();

        let error = SharedAccount::new_persistent("wal-corrupt", &path).unwrap_err();
        assert!(error.contains("checksum mismatch"), "{error}");
        remove_persistence_test_files(&path);
    }

    #[test]
    fn persistence_wal_writes_a_small_delta_for_a_large_ledger() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-shared-account-wal-delta-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        let wal_path = persistence_wal_path(&path);
        {
            let account = SharedAccount::new_persistent("wal-delta", &path).unwrap();
            let mut state = account.lock_state();
            for index in 0..5_000 {
                state
                    .settled_token_values
                    .insert(format!("token-{index:05}"), 1.0);
            }
            state.settled_token_values_generation = 1;
            account.schedule_persist(&state);
            drop(state);
            account.flush_persistence(Duration::from_secs(2)).unwrap();
        }

        {
            let account = SharedAccount::new_persistent("wal-delta", &path).unwrap();
            let snapshot_len = std::fs::metadata(&path).unwrap().len();
            let mut state = account.lock_state();
            state
                .settled_token_values
                .insert("token-00000".to_string(), 0.0);
            state.settled_token_values_generation += 1;
            account.schedule_persist(&state);
            drop(state);
            account.flush_persistence(Duration::from_secs(2)).unwrap();
            let wal_len = std::fs::metadata(&wal_path).unwrap().len();
            assert!(
                wal_len * 10 < snapshot_len,
                "incremental WAL {wal_len} should be far smaller than snapshot {snapshot_len}"
            );
        }
        remove_persistence_test_files(&path);
    }

    #[test]
    fn read_mostly_control_snapshots_do_not_take_the_account_lock() {
        let account = SharedAccount::new("lock-free-control-snapshots");
        {
            let mut state = account.lock_state();
            state.startup_snapshot_applied_this_process = true;
            state.uncertain_reason = Some("test blocker".to_string());
            state
                .settled_token_values
                .insert("winner-token".to_string(), 1.0);
            state.settled_token_values_generation = 7;
        }
        let acquisitions = account.account_lock_acquisitions.load(Ordering::Acquire);

        assert!(account.startup_snapshot_applied());
        assert_eq!(
            account
                .uncertain_reason_snapshot()
                .as_deref()
                .map(String::as_str),
            Some("test blocker"),
        );
        let outcomes = account.settled_token_values_snapshot_arc();
        assert_eq!(outcomes.generation, 7);
        assert_eq!(outcomes.values.get("winner-token"), Some(&1.0));
        assert_eq!(
            account.account_lock_acquisitions.load(Ordering::Acquire),
            acquisitions,
        );
    }

    fn remove_persistence_test_files(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(persistence_wal_path(path));
        let mut tmp_path = path.as_os_str().to_os_string();
        tmp_path.push(".tmp");
        let _ = std::fs::remove_file(PathBuf::from(tmp_path));
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let _ = std::fs::remove_file(PathBuf::from(lock_path));
    }

    #[test]
    fn sidecar_checkpoint_is_monotonic_and_durable() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-sidecar-checkpoint-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        {
            let account = SharedAccount::new_persistent("sidecar", &path).unwrap();
            assert!(account
                .record_sidecar_checkpoint(
                    "maker-a",
                    DurableSidecarCheckpoint {
                        generation: 2,
                        expected_entries: 3,
                        recovery_payload: "{\"generation\":2}".to_string(),
                    },
                )
                .unwrap());
            assert!(!account
                .record_sidecar_checkpoint(
                    "maker-a",
                    DurableSidecarCheckpoint {
                        generation: 1,
                        expected_entries: 1,
                        recovery_payload: "stale".to_string(),
                    },
                )
                .unwrap());
            account.flush_persistence(Duration::from_secs(2)).unwrap();
        }
        let restored = SharedAccount::new_persistent("sidecar", &path).unwrap();
        let checkpoint = restored.sidecar_checkpoint("maker-a").unwrap();
        assert_eq!(checkpoint.generation, 2);
        assert_eq!(checkpoint.expected_entries, 3);
        drop(restored);
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(PathBuf::from(lock_path));
    }

    #[test]
    fn invalid_max_generation_sidecar_checkpoint_can_be_repaired() {
        let account = SharedAccount::new("sidecar-repair");
        account
            .record_sidecar_checkpoint(
                "maker-a",
                DurableSidecarCheckpoint {
                    generation: u64::MAX,
                    expected_entries: 9,
                    recovery_payload: "{\"invalid\":true}".to_string(),
                },
            )
            .unwrap();
        assert!(account
            .repair_sidecar_checkpoint(
                "maker-a",
                u64::MAX,
                DurableSidecarCheckpoint {
                    generation: 7,
                    expected_entries: 2,
                    recovery_payload: "{\"generation\":7}".to_string(),
                },
            )
            .unwrap());
        let repaired = account.sidecar_checkpoint("maker-a").unwrap();
        assert_eq!(repaired.generation, 7);
        assert_eq!(repaired.expected_entries, 2);
        assert!(!account
            .repair_sidecar_checkpoint(
                "maker-a",
                u64::MAX,
                DurableSidecarCheckpoint {
                    generation: 8,
                    expected_entries: 2,
                    recovery_payload: "{\"generation\":8}".to_string(),
                },
            )
            .unwrap());
    }

    #[test]
    fn parseable_ledger_with_negative_reservation_is_rejected() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-invalid-ledger-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        let mut state = SharedAccountState::default();
        state.seeded = true;
        state.physical_cash = 100.0;
        let mut instance = InstanceLedger::new(1.0);
        instance.cash = 100.0;
        instance.reserved_cash = -5.0;
        state.instances.insert("a".to_string(), instance);
        write_persisted_account(
            &path,
            &PersistedAccount {
                version: PERSISTENCE_VERSION,
                account_id: "invalid".to_string(),
                persistence_generation: 0,
                state,
            },
        )
        .unwrap();
        let error = SharedAccount::new_persistent("invalid", &path).unwrap_err();
        assert!(error.contains("invalid account ledger"), "{error}");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn live_query_repair_restores_failed_trade_order_reservation_before_open() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-failed-trade-query-repair-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        let account_id = "query-repair";
        let instance_id = "btc";
        let coid = "btc-failed-1";
        let oid = "0xFAILED";
        let token = "BTC-UP";
        let trade_key = "trade-failed:0xFAILED";
        let mut state = SharedAccountState::default();
        state
            .instances
            .insert(instance_id.to_string(), InstanceLedger::new(1.0));
        state.orders.insert(
            coid.to_string(),
            OrderOwnership {
                account_id: account_id.to_string(),
                instance_id: instance_id.to_string(),
                client_order_id: coid.to_string(),
                order_id: oid.to_string(),
                token_id: token.to_string(),
                side: Side::Sell,
                quantity: 40.0,
                filled_quantity: 0.0,
                terminal_matched_quantity: Some(40.0),
                terminal_trade_ids: vec!["trade-failed".to_string()],
                terminal_trade_ids_authoritative: true,
                price: 0.5,
                fee_rate_bps: 0,
                reserved_cash: 0.0,
                reserved_quantity: 0.0,
                status: OrderStatus::Filled,
            },
        );
        state
            .oid_to_coid
            .insert(normalize_order_id(oid), coid.to_string());
        state.recovery_pending_orders.insert(coid.to_string());
        state.trades.insert(
            trade_key.to_string(),
            AppliedTrade {
                ownership: TradeOwnership {
                    account_id: account_id.to_string(),
                    instance_id: instance_id.to_string(),
                    trade_key: trade_key.to_string(),
                    client_order_id: coid.to_string(),
                    order_id: oid.to_string(),
                    token_id: token.to_string(),
                    side: Side::Sell,
                    quantity: 40.0,
                    price: 0.5,
                    status: "FAILED".to_string(),
                },
                booked: false,
                physical_booked: false,
                usdc_fee: 0.0,
                shares_fee: 0.0,
                virtual_fee_booked: false,
                physical_fee_booked: false,
                failed: true,
                failure_reconciled: true,
                is_maker: Some(true),
                match_time_secs: 1,
                ledger_generation: 0,
            },
        );
        write_persisted_account(
            &path,
            &PersistedAccount {
                version: PERSISTENCE_VERSION,
                account_id: account_id.to_string(),
                persistence_generation: 0,
                state,
            },
        )
        .unwrap();

        let strict_error = SharedAccount::new_persistent(account_id, &path).unwrap_err();
        assert!(
            strict_error.contains("reservation disagrees with effective remaining quantity"),
            "{strict_error}",
        );

        let restored = SharedAccount::new_persistent_for_query_repair(account_id, &path).unwrap();
        assert_eq!(
            restored.startup_query_repair_pending_order_ids(),
            vec![coid.to_string()],
        );
        let order = restored.order(coid).unwrap();
        assert_eq!(order.status, OrderStatus::Accepted);
        assert_eq!(order.reserved_quantity, 40.0);
        assert_eq!(order.terminal_matched_quantity, None);
        assert!(order.terminal_trade_ids.is_empty());
        assert!(!order.terminal_trade_ids_authoritative);
        let instance = restored.instance_snapshot(instance_id).unwrap();
        assert_eq!(instance.reserved_positions[token], 40.0);
        assert_eq!(restored.monitoring_snapshot().recovery_pending_orders, 1);
        restored.flush_persistence(Duration::from_secs(2)).unwrap();
        drop(restored);

        // A crash after the conservative reservation was fsynced but before
        // the CLOB query completed must remain a fail-closed query repair on
        // the next process start, without double-counting the reservation.
        let unfinished_error = SharedAccount::new_persistent(account_id, &path).unwrap_err();
        assert!(
            unfinished_error.contains("unfinished authoritative startup query repair"),
            "{unfinished_error}",
        );
        let reopened = SharedAccount::new_persistent_for_query_repair(account_id, &path).unwrap();
        assert_eq!(
            reopened.startup_query_repair_pending_order_ids(),
            vec![coid.to_string()],
        );
        assert_eq!(
            reopened
                .instance_snapshot(instance_id)
                .unwrap()
                .reserved_positions[token],
            40.0,
        );
        assert!(reopened.mark_cancelled_pending_audit(coid));
        assert!(reopened.startup_query_repair_pending_order_ids().is_empty());
        drop(reopened);
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(PathBuf::from(lock_path));
    }

    #[test]
    fn live_query_repair_restores_cancel_audit_remaining_reservation() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-cancel-audit-query-repair-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        let account_id = "cancel-audit-query-repair";
        let instance_id = "btc";
        let coid = "btc-cancelled-partial";
        let token = "BTC-UP";
        let mut state = SharedAccountState::default();
        let mut instance = InstanceLedger::new(1.0);
        instance.positions.insert(token.to_string(), 100.0);
        state.instances.insert(instance_id.to_string(), instance);
        state.orders.insert(
            coid.to_string(),
            OrderOwnership {
                account_id: account_id.to_string(),
                instance_id: instance_id.to_string(),
                client_order_id: coid.to_string(),
                order_id: "0xCANCELLEDPARTIAL".to_string(),
                token_id: token.to_string(),
                side: Side::Sell,
                quantity: 10.0,
                filled_quantity: 2.25,
                terminal_matched_quantity: None,
                terminal_trade_ids: Vec::new(),
                terminal_trade_ids_authoritative: false,
                price: 0.43,
                fee_rate_bps: 0,
                reserved_cash: 0.0,
                reserved_quantity: 0.0,
                status: OrderStatus::Cancelled,
            },
        );
        state.trades.insert(
            "trade-mined:0xCANCELLEDPARTIAL".to_string(),
            AppliedTrade {
                ownership: TradeOwnership {
                    account_id: account_id.to_string(),
                    instance_id: instance_id.to_string(),
                    trade_key: "trade-mined:0xCANCELLEDPARTIAL".to_string(),
                    client_order_id: coid.to_string(),
                    order_id: "0xCANCELLEDPARTIAL".to_string(),
                    token_id: token.to_string(),
                    side: Side::Sell,
                    quantity: 2.25,
                    price: 0.43,
                    status: "MINED".to_string(),
                },
                booked: true,
                physical_booked: true,
                usdc_fee: 0.0,
                shares_fee: 0.0,
                virtual_fee_booked: true,
                physical_fee_booked: true,
                failed: false,
                failure_reconciled: false,
                is_maker: Some(true),
                match_time_secs: 1,
                ledger_generation: 0,
            },
        );
        state.routine_cancel_audits.insert(coid.to_string());
        write_persisted_account(
            &path,
            &PersistedAccount {
                version: PERSISTENCE_VERSION,
                account_id: account_id.to_string(),
                persistence_generation: 0,
                state,
            },
        )
        .unwrap();

        let strict_error = SharedAccount::new_persistent(account_id, &path).unwrap_err();
        assert!(
            strict_error.contains("reservation disagrees with effective remaining quantity"),
            "{strict_error}",
        );
        let restored = SharedAccount::new_persistent_for_query_repair(account_id, &path).unwrap();
        assert_eq!(
            restored.startup_query_repair_pending_order_ids(),
            vec![coid.to_string()],
        );
        assert_eq!(restored.order(coid).unwrap().reserved_quantity, 7.75);
        assert_eq!(
            restored
                .instance_snapshot(instance_id)
                .unwrap()
                .reserved_positions[token],
            7.75,
        );
        restored.flush_persistence(Duration::from_secs(2)).unwrap();
        drop(restored);

        // Crash-reopen remains admitted only through the same conservative
        // query path; ordinary strict startup still refuses it.
        assert!(SharedAccount::new_persistent(account_id, &path).is_err());
        let reopened = SharedAccount::new_persistent_for_query_repair(account_id, &path).unwrap();
        assert_eq!(reopened.order(coid).unwrap().reserved_quantity, 7.75);
        drop(reopened);
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(PathBuf::from(lock_path));
    }

    #[test]
    fn live_query_repair_uses_retired_failed_trade_tombstone() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-retired-failed-query-repair-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        let account_id = "retired-query-repair";
        let instance_id = "btc";
        let coid = "btc-retired-failed-1";
        let oid = "0xRETIREDFAILED";
        let token = "BTC-UP";
        let trade_key = "trade-retired-failed:0xRETIREDFAILED";
        let mut state = SharedAccountState::default();
        state
            .instances
            .insert(instance_id.to_string(), InstanceLedger::new(1.0));
        state.orders.insert(
            coid.to_string(),
            OrderOwnership {
                account_id: account_id.to_string(),
                instance_id: instance_id.to_string(),
                client_order_id: coid.to_string(),
                order_id: oid.to_string(),
                token_id: token.to_string(),
                side: Side::Sell,
                quantity: 40.0,
                filled_quantity: 0.0,
                terminal_matched_quantity: Some(40.0),
                terminal_trade_ids: vec!["trade-retired-failed".to_string()],
                terminal_trade_ids_authoritative: true,
                price: 0.5,
                fee_rate_bps: 0,
                reserved_cash: 0.0,
                reserved_quantity: 0.0,
                status: OrderStatus::Filled,
            },
        );
        state
            .oid_to_coid
            .insert(normalize_order_id(oid), coid.to_string());
        state.recovery_pending_orders.insert(coid.to_string());
        state.retired_trade_ownership_tombstones.insert(
            trade_key.to_string(),
            RetiredTradeOwnershipTombstone {
                ownership: TradeOwnership {
                    account_id: account_id.to_string(),
                    instance_id: instance_id.to_string(),
                    trade_key: trade_key.to_string(),
                    client_order_id: coid.to_string(),
                    order_id: oid.to_string(),
                    token_id: token.to_string(),
                    side: Side::Sell,
                    quantity: 40.0,
                    price: 0.5,
                    status: "FAILED".to_string(),
                },
                is_maker: Some(true),
                authenticated_terminal_noop: false,
                retired_at_ms: wall_clock_ms(),
            },
        );
        write_persisted_account(
            &path,
            &PersistedAccount {
                version: PERSISTENCE_VERSION,
                account_id: account_id.to_string(),
                persistence_generation: 0,
                state,
            },
        )
        .unwrap();

        let strict_error = SharedAccount::new_persistent(account_id, &path).unwrap_err();
        assert!(
            strict_error.contains("reservation disagrees with effective remaining quantity"),
            "{strict_error}",
        );

        let restored = SharedAccount::new_persistent_for_query_repair(account_id, &path).unwrap();
        assert_eq!(
            restored.startup_query_repair_pending_order_ids(),
            vec![coid.to_string()],
        );
        let order = restored.order(coid).unwrap();
        assert_eq!(order.status, OrderStatus::Accepted);
        assert_eq!(order.reserved_quantity, 40.0);
        assert_eq!(order.terminal_matched_quantity, None);
        assert!(order.terminal_trade_ids.is_empty());
        assert_eq!(
            restored
                .instance_snapshot(instance_id)
                .unwrap()
                .reserved_positions[token],
            40.0,
        );
        assert_eq!(
            restored.trade_ownership(trade_key).unwrap().client_order_id,
            coid,
        );
        drop(restored);
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(PathBuf::from(lock_path));
    }

    #[test]
    fn live_query_repair_does_not_use_confirmed_tombstone_as_failed_proof() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-confirmed-tombstone-query-repair-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        let account_id = "confirmed-tombstone";
        let instance_id = "maker";
        let coid = "maker-confirmed-1";
        let oid = "0xCONFIRMED";
        let token = "UP";
        let trade_key = "trade-confirmed:0xCONFIRMED";
        let mut state = SharedAccountState::default();
        state
            .instances
            .insert(instance_id.to_string(), InstanceLedger::new(1.0));
        state.orders.insert(
            coid.to_string(),
            OrderOwnership {
                account_id: account_id.to_string(),
                instance_id: instance_id.to_string(),
                client_order_id: coid.to_string(),
                order_id: oid.to_string(),
                token_id: token.to_string(),
                side: Side::Sell,
                quantity: 20.0,
                filled_quantity: 10.0,
                terminal_matched_quantity: None,
                terminal_trade_ids: Vec::new(),
                terminal_trade_ids_authoritative: false,
                price: 0.5,
                fee_rate_bps: 0,
                reserved_cash: 0.0,
                reserved_quantity: 0.0,
                status: OrderStatus::Accepted,
            },
        );
        state
            .oid_to_coid
            .insert(normalize_order_id(oid), coid.to_string());
        state.recovery_pending_orders.insert(coid.to_string());
        state.retired_trade_ownership_tombstones.insert(
            trade_key.to_string(),
            RetiredTradeOwnershipTombstone {
                ownership: TradeOwnership {
                    account_id: account_id.to_string(),
                    instance_id: instance_id.to_string(),
                    trade_key: trade_key.to_string(),
                    client_order_id: coid.to_string(),
                    order_id: oid.to_string(),
                    token_id: token.to_string(),
                    side: Side::Sell,
                    quantity: 10.0,
                    price: 0.5,
                    status: "CONFIRMED".to_string(),
                },
                is_maker: Some(true),
                authenticated_terminal_noop: false,
                retired_at_ms: wall_clock_ms(),
            },
        );
        write_persisted_account(
            &path,
            &PersistedAccount {
                version: PERSISTENCE_VERSION,
                account_id: account_id.to_string(),
                persistence_generation: 0,
                state,
            },
        )
        .unwrap();

        let error = SharedAccount::new_persistent_for_query_repair(account_id, &path).unwrap_err();
        assert!(
            error.contains("reservation disagrees with effective remaining quantity"),
            "{error}",
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn live_query_repair_does_not_admit_unowned_under_reservation() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-unowned-query-repair-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        let mut state = SharedAccountState::default();
        state
            .instances
            .insert("maker".to_string(), InstanceLedger::new(1.0));
        state.orders.insert(
            "maker-1".to_string(),
            OrderOwnership {
                account_id: "unowned".to_string(),
                instance_id: "maker".to_string(),
                client_order_id: "maker-1".to_string(),
                order_id: "0xABC".to_string(),
                token_id: "UP".to_string(),
                side: Side::Sell,
                quantity: 10.0,
                filled_quantity: 0.0,
                terminal_matched_quantity: None,
                terminal_trade_ids: Vec::new(),
                terminal_trade_ids_authoritative: false,
                price: 0.5,
                fee_rate_bps: 0,
                reserved_cash: 0.0,
                reserved_quantity: 0.0,
                status: OrderStatus::Accepted,
            },
        );
        write_persisted_account(
            &path,
            &PersistedAccount {
                version: PERSISTENCE_VERSION,
                account_id: "unowned".to_string(),
                persistence_generation: 0,
                state,
            },
        )
        .unwrap();

        let error = SharedAccount::new_persistent_for_query_repair("unowned", &path).unwrap_err();
        assert!(
            error.contains("reservation disagrees with effective remaining quantity"),
            "{error}",
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn persistent_ledger_admits_order_owned_position_deficit_for_startup_recovery() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-recoverable-reservation-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        let mut state = SharedAccountState::default();
        let mut instance = InstanceLedger::new(1.0);
        instance
            .reserved_positions
            .insert("ENDED-UP".to_string(), 10.0);
        state.instances.insert("maker".to_string(), instance);
        state.orders.insert(
            "maker-1".to_string(),
            OrderOwnership {
                account_id: "recoverable".to_string(),
                instance_id: "maker".to_string(),
                client_order_id: "maker-1".to_string(),
                order_id: "0xABC".to_string(),
                token_id: "ENDED-UP".to_string(),
                side: Side::Sell,
                quantity: 10.0,
                filled_quantity: 0.0,
                terminal_matched_quantity: None,
                terminal_trade_ids: Vec::new(),
                terminal_trade_ids_authoritative: false,
                price: 0.5,
                fee_rate_bps: 0,
                reserved_cash: 0.0,
                reserved_quantity: 10.0,
                status: OrderStatus::Accepted,
            },
        );
        write_persisted_account(
            &path,
            &PersistedAccount {
                version: PERSISTENCE_VERSION,
                account_id: "recoverable".to_string(),
                persistence_generation: 0,
                state,
            },
        )
        .unwrap();

        let restored = SharedAccount::new_persistent("recoverable", &path).unwrap();
        let snapshot = restored.monitoring_snapshot();
        assert_eq!(snapshot.recovery_pending_orders, 1);
        assert!(!snapshot.uncertain);
        assert_eq!(
            restored.order_audit_instance_blocker("maker"),
            Some(vec!["maker-1".to_string()]),
        );
        assert_eq!(
            restored
                .instance_snapshot("maker")
                .unwrap()
                .reserved_positions["ENDED-UP"],
            10.0,
        );
        drop(restored);
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(PathBuf::from(lock_path));
    }

    #[test]
    fn persisted_state_validator_rejects_dangling_mappings_and_unknown_trade_status() {
        let mut state = SharedAccountState::default();
        state.physical_cash = 100.0;
        let mut instance = InstanceLedger::new(1.0);
        instance.cash = 100.0;
        instance.reserved_cash = 5.0;
        state.instances.insert("maker".to_string(), instance);
        state.orders.insert(
            "maker-order".to_string(),
            OrderOwnership {
                account_id: "account".to_string(),
                instance_id: "maker".to_string(),
                client_order_id: "maker-order".to_string(),
                order_id: "0xABC".to_string(),
                token_id: "UP".to_string(),
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
                status: OrderStatus::Accepted,
            },
        );
        let mapping_error = validate_persisted_state("account", &state).unwrap_err();
        assert!(mapping_error.contains("missing its durable order-id mapping"));

        state
            .oid_to_coid
            .insert("abc".to_string(), "maker-order".to_string());
        assert!(validate_persisted_state("account", &state).is_ok());

        state.ledger_generation = 1;
        state.trades.insert(
            "trade".to_string(),
            AppliedTrade {
                ownership: TradeOwnership {
                    account_id: "account".to_string(),
                    instance_id: "maker".to_string(),
                    trade_key: "trade".to_string(),
                    client_order_id: "maker-order".to_string(),
                    order_id: "0xABC".to_string(),
                    token_id: "UP".to_string(),
                    side: Side::Buy,
                    quantity: 1.0,
                    price: 0.5,
                    status: "FUTURE_STATUS".to_string(),
                },
                booked: true,
                physical_booked: false,
                usdc_fee: 0.0,
                shares_fee: 0.0,
                virtual_fee_booked: false,
                physical_fee_booked: false,
                failed: false,
                failure_reconciled: false,
                is_maker: Some(true),
                match_time_secs: 1,
                ledger_generation: 1,
            },
        );
        let trade_error = validate_persisted_state("account", &state).unwrap_err();
        assert!(trade_error.contains("trade `trade` contains invalid"));
    }

    #[test]
    fn persisted_state_validator_recomputes_fill_and_reservation_derivatives() {
        let mut state = SharedAccountState::default();
        state.physical_cash = 100.0;
        let mut instance = InstanceLedger::new(1.0);
        instance.cash = 99.5;
        instance.reserved_cash = 4.5;
        instance.positions.insert("UP".to_string(), 1.0);
        state.instances.insert("maker".to_string(), instance);
        state.orders.insert(
            "maker-order".to_string(),
            OrderOwnership {
                account_id: "account".to_string(),
                instance_id: "maker".to_string(),
                client_order_id: "maker-order".to_string(),
                order_id: "0xABC".to_string(),
                token_id: "UP".to_string(),
                side: Side::Buy,
                quantity: 10.0,
                filled_quantity: 1.0,
                terminal_matched_quantity: None,
                terminal_trade_ids: Vec::new(),
                terminal_trade_ids_authoritative: false,
                price: 0.5,
                fee_rate_bps: 0,
                reserved_cash: 4.5,
                reserved_quantity: 0.0,
                status: OrderStatus::PartiallyFilled,
            },
        );
        state
            .oid_to_coid
            .insert("abc".to_string(), "maker-order".to_string());
        state.ledger_generation = 1;
        state.trades.insert(
            "trade".to_string(),
            AppliedTrade {
                ownership: TradeOwnership {
                    account_id: "account".to_string(),
                    instance_id: "maker".to_string(),
                    trade_key: "trade".to_string(),
                    client_order_id: "maker-order".to_string(),
                    order_id: "0xABC".to_string(),
                    token_id: "UP".to_string(),
                    side: Side::Buy,
                    quantity: 1.0,
                    price: 0.5,
                    status: "MATCHED".to_string(),
                },
                booked: true,
                physical_booked: false,
                usdc_fee: 0.0,
                shares_fee: 0.0,
                virtual_fee_booked: true,
                physical_fee_booked: false,
                failed: false,
                failure_reconciled: false,
                is_maker: Some(true),
                match_time_secs: 1,
                ledger_generation: 1,
            },
        );
        assert!(validate_persisted_state("account", &state).is_ok());

        state.instances.get_mut("maker").unwrap().reserved_cash = 4.0;
        let aggregate_error = validate_persisted_state("account", &state).unwrap_err();
        assert!(
            aggregate_error.contains("reserved_cash"),
            "{aggregate_error}"
        );
        state.instances.get_mut("maker").unwrap().reserved_cash = 4.5;

        state.orders.get_mut("maker-order").unwrap().filled_quantity = 0.5;
        let fill_error = validate_persisted_state("account", &state).unwrap_err();
        assert!(fill_error.contains("durable trades"), "{fill_error}");
    }

    #[test]
    fn startup_repairs_only_under_reserved_instance_aggregates() {
        let mut state = persisted_buy_reservation_state("account", 0.0, 0.00646972);
        state.physical_positions.insert("BTC-DOWN".to_string(), 2.0);
        state
            .instances
            .get_mut("btc01")
            .unwrap()
            .positions
            .insert("BTC-DOWN".to_string(), 2.0);
        state.orders.insert(
            "btc01-sell".to_string(),
            OrderOwnership {
                account_id: "account".to_string(),
                instance_id: "btc01".to_string(),
                client_order_id: "btc01-sell".to_string(),
                order_id: "0xSELL".to_string(),
                token_id: "BTC-DOWN".to_string(),
                side: Side::Sell,
                quantity: 2.0,
                filled_quantity: 0.0,
                terminal_matched_quantity: None,
                terminal_trade_ids: Vec::new(),
                terminal_trade_ids_authoritative: false,
                price: 0.5,
                fee_rate_bps: 0,
                reserved_cash: 0.0,
                reserved_quantity: 2.0,
                status: OrderStatus::Accepted,
            },
        );
        state
            .oid_to_coid
            .insert("sell".to_string(), "btc01-sell".to_string());

        let before = validate_persisted_state("account", &state).unwrap_err();
        assert!(before.contains("reserved_cash"), "{before}");
        let repairs = repair_under_reserved_instance_aggregates(&mut state).unwrap();
        assert_eq!(repairs.len(), 1);
        assert_eq!(repairs[0].instance_id, "btc01");
        assert_eq!(repairs[0].cash_before, 0.0);
        assert_eq!(repairs[0].cash_after, 0.00646972);
        assert_eq!(
            repairs[0].positions,
            vec![("BTC-DOWN".to_string(), 0.0, 2.0)],
        );
        assert!(validate_persisted_state("account", &state).is_ok());
        assert!(repair_under_reserved_instance_aggregates(&mut state)
            .unwrap()
            .is_empty());

        let mut over_reserved = persisted_buy_reservation_state("account", 0.01, 0.00646972);
        assert!(
            repair_under_reserved_instance_aggregates(&mut over_reserved)
                .unwrap()
                .is_empty()
        );
        let over_error = validate_persisted_state("account", &over_reserved).unwrap_err();
        assert!(over_error.contains("reserved_cash"), "{over_error}");

        let mut invalid_leaf = persisted_buy_reservation_state("account", 0.0, 0.0);
        let leaf_error = repair_under_reserved_instance_aggregates(&mut invalid_leaf).unwrap_err();
        assert!(
            leaf_error.contains("reservation disagrees with effective remaining quantity"),
            "{leaf_error}",
        );
        assert_eq!(
            invalid_leaf.instances["btc01"].reserved_cash, 0.0,
            "an invalid order leaf must not be masked by aggregate repair",
        );
    }

    #[test]
    fn persistent_startup_repairs_and_snapshots_reservation_aggregate() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-startup-aggregate-repair-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        let account_id = "aggregate-repair";
        write_persisted_account(
            &path,
            &PersistedAccount {
                version: PERSISTENCE_VERSION,
                account_id: account_id.to_string(),
                persistence_generation: 0,
                state: persisted_buy_reservation_state(account_id, 0.0, 0.00646972),
            },
        )
        .unwrap();

        {
            let restored =
                SharedAccount::new_persistent_for_query_repair(account_id, &path).unwrap();
            assert_eq!(
                restored.instance_snapshot("btc01").unwrap().reserved_cash,
                0.00646972,
            );
        }
        let persisted: PersistedAccount =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            persisted.state.instances["btc01"].reserved_cash, 0.00646972,
            "the startup snapshot must durably fold the repair before admission",
        );
        {
            let reopened = SharedAccount::new_persistent(account_id, &path).unwrap();
            assert_eq!(
                reopened.instance_snapshot("btc01").unwrap().reserved_cash,
                0.00646972,
            );
        }

        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let _ = std::fs::remove_file(persistence_wal_path(&path));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(PathBuf::from(lock_path));
    }

    #[test]
    fn persisted_state_validator_counts_exact_confirmed_trade_tombstones_for_fill() {
        let _persistence_guard = persistence_test_guard();
        let account = seeded_account();
        account
            .reserve_order("a", "maker-order", "0xABC", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();
        assert!(matches!(
            account.apply_trade_transition_with_context(
                "trade",
                "CONFIRMED",
                "maker-order",
                "0xABC",
                "UP",
                Side::Buy,
                1.0,
                0.5,
                true,
                1,
            ),
            TradeTransitionResult::Applied(_),
        ));
        assert_eq!(
            account.prune_terminal_history(&HashSet::from(["UP".to_string()])),
            (0, 1),
        );
        let state = account.lock_state().clone();
        assert!(state.trades.is_empty());
        assert_eq!(state.retired_trade_ownership_tombstones.len(), 1);
        let tombstone_validation = validate_persisted_state("acct", &state);
        assert!(tombstone_validation.is_ok(), "{tombstone_validation:?}",);

        let path = std::env::temp_dir().join(format!(
            "hexagent-confirmed-tombstone-fill-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        write_persisted_account(
            &path,
            &PersistedAccount {
                version: PERSISTENCE_VERSION,
                account_id: "acct".to_string(),
                persistence_generation: 0,
                state: state.clone(),
            },
        )
        .unwrap();
        {
            let restored = SharedAccount::new_persistent("acct", &path).unwrap();
            assert_eq!(restored.order("maker-order").unwrap().filled_quantity, 1.0);
            assert!(restored.trades().is_empty());
        }
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(PathBuf::from(lock_path));

        let mut historical_noop = state.clone();
        historical_noop
            .settled_token_values
            .insert("UP".to_string(), 1.0);
        historical_noop
            .retired_trade_ownership_tombstones
            .get_mut("trade")
            .unwrap()
            .authenticated_terminal_noop = true;
        let noop_error = validate_persisted_state("acct", &historical_noop).unwrap_err();
        assert!(noop_error.contains("durable trades=0"), "{noop_error}");

        let mut mismatched = state;
        mismatched
            .retired_trade_ownership_tombstones
            .get_mut("trade")
            .unwrap()
            .ownership
            .order_id = "0xOTHER".to_string();
        let mismatch_error = validate_persisted_state("acct", &mismatched).unwrap_err();
        assert!(
            mismatch_error.contains("durable trades=0"),
            "{mismatch_error}",
        );
    }

    #[test]
    fn persisted_state_validator_replays_instance_economics_from_immutable_seed() {
        let account = seeded_account();
        account
            .reserve_order(
                "a",
                "replay-order",
                "replay-oid",
                "UP",
                Side::Buy,
                2.0,
                0.5,
                0,
            )
            .unwrap();
        assert!(matches!(
            account.apply_trade_transition_with_context(
                "replay-trade",
                "CONFIRMED",
                "replay-order",
                "replay-oid",
                "UP",
                Side::Buy,
                2.0,
                0.5,
                true,
                1,
            ),
            TradeTransitionResult::Applied(_)
        ));
        assert_eq!(
            account.prune_terminal_history(&HashSet::from(["UP".to_string()])),
            (1, 1),
        );

        let mut state = account.lock_state().clone();
        assert!(validate_persisted_state("acct", &state).is_ok());

        // Keep aggregate cash unchanged so the old reconciliation-only check
        // cannot see the corruption. Immutable-root replay must reject the
        // per-instance transfer that has no durable migration/adjustment row.
        state.instances.get_mut("a").unwrap().cash += 1.0;
        state.instances.get_mut("b").unwrap().cash -= 1.0;
        let error = validate_persisted_state("acct", &state).unwrap_err();
        assert!(error.contains("immutable-baseline replay"), "{error}");
    }

    #[test]
    fn persisted_state_validator_requires_roles_fee_currency_and_pending_bijection() {
        let account = seeded_account();
        account
            .reserve_order("a", "fee-order", "fee-oid", "UP", Side::Buy, 2.0, 0.5, 0)
            .unwrap();
        assert!(matches!(
            account.apply_trade_transition_with_context(
                "fee-trade",
                "MATCHED",
                "fee-order",
                "fee-oid",
                "UP",
                Side::Buy,
                2.0,
                0.5,
                false,
                1,
            ),
            TradeTransitionResult::Applied(_)
        ));

        let state = account.lock_state().clone();
        assert!(state.fee_attribution_pending.contains("fee-trade"));
        assert!(validate_persisted_state("acct", &state).is_ok());

        let mut missing_pending = state.clone();
        missing_pending.fee_attribution_pending.clear();
        let pending_error = validate_persisted_state("acct", &missing_pending).unwrap_err();
        assert!(
            pending_error.contains("not bidirectional"),
            "{pending_error}"
        );

        let mut unknown_role = state.clone();
        unknown_role.trades.get_mut("fee-trade").unwrap().is_maker = None;
        assert!(validate_persisted_state("acct", &unknown_role).is_ok());
        unknown_role.fee_attribution_pending.clear();
        let role_error = validate_persisted_state("acct", &unknown_role).unwrap_err();
        assert!(
            role_error.contains("unknown maker/taker role"),
            "{role_error}"
        );

        let mut wrong_currency = state;
        wrong_currency.trades.get_mut("fee-trade").unwrap().usdc_fee = 0.1;
        let currency_error = validate_persisted_state("acct", &wrong_currency).unwrap_err();
        assert!(
            currency_error.contains("instead of shares"),
            "{currency_error}"
        );

        assert!(account
            .register_token_fee_config(&["UP".to_string()], 1.01, 1.0)
            .is_err());
        assert!(account
            .register_token_fee_config(&["UP".to_string()], 0.02, 0.0)
            .is_err());
    }

    #[test]
    fn provisional_owner_follows_virtual_inventory_across_physical_redemption() {
        let mut state = SharedAccountState::default();
        let mut instance = InstanceLedger::new(1.0);
        instance.positions.insert("HISTORICAL-WIN".to_string(), 7.0);
        state.instances.insert("maker".to_string(), instance);
        state
            .provisional_position_owners
            .insert("HISTORICAL-WIN".to_string(), "maker".to_string());
        state
            .unallocated_positions
            .insert("HISTORICAL-WIN".to_string(), -7.0);
        state.uncertain = true;

        // A wallet snapshot may no longer contain a redeemed historical token
        // while its virtual owner remains necessary for attribution.
        assert!(validate_persisted_state("account", &state).is_ok());

        state
            .instances
            .get_mut("maker")
            .unwrap()
            .positions
            .remove("HISTORICAL-WIN");
        recompute_reconciliation(&mut state, "test provisional cleanup");
        assert!(!state
            .provisional_position_owners
            .contains_key("HISTORICAL-WIN"));
    }

    #[test]
    fn retired_trade_ownership_tombstone_survives_persistent_restart() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-retired-trade-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        {
            let account = SharedAccount::new_persistent("retired-replay", &path).unwrap();
            account.register_instance("owner", 1.0);
            account
                .apply_physical_snapshot(100.0, HashMap::new())
                .unwrap();
            account
                .reserve_order(
                    "owner",
                    "retired-order",
                    "retired-oid",
                    "TOKEN",
                    Side::Buy,
                    2.0,
                    0.5,
                    0,
                )
                .unwrap();
            assert!(matches!(
                account.apply_trade_transition_with_context(
                    "retired-trade",
                    "CONFIRMED",
                    "retired-order",
                    "retired-oid",
                    "TOKEN",
                    Side::Buy,
                    2.0,
                    0.5,
                    true,
                    1,
                ),
                TradeTransitionResult::Applied(_)
            ));
            account.release_order("retired-order", OrderStatus::Filled);
            assert_eq!(
                account.prune_terminal_history(&HashSet::from(["TOKEN".to_string()])),
                (1, 1),
            );
            account.flush_persistence(Duration::from_secs(2)).unwrap();
        }

        {
            let restored = SharedAccount::new_persistent("retired-replay", &path).unwrap();
            assert_eq!(
                restored
                    .trade_ownership("retired-trade")
                    .unwrap()
                    .client_order_id,
                "retired-order",
            );
            let before = restored.monitoring_snapshot();
            assert!(matches!(
                restored.apply_trade_transition_with_context(
                    "retired-trade",
                    "CONFIRMED",
                    "",
                    "retired-oid",
                    "TOKEN",
                    Side::Buy,
                    2.0,
                    0.5,
                    true,
                    1,
                ),
                TradeTransitionResult::OwnedNoop(_)
            ));
            let after = restored.monitoring_snapshot();
            assert_eq!(after.physical_cash, before.physical_cash);
            assert_eq!(after.physical_positions, before.physical_positions);
            assert!(!after.uncertain);
        }

        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(PathBuf::from(lock_path));
    }

    #[test]
    fn authenticated_terminal_noop_tombstone_survives_persistent_restart() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-authenticated-noop-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        {
            let account = SharedAccount::new_persistent("authenticated-noop", &path).unwrap();
            account.register_instance("owner", 1.0);
            account
                .apply_physical_snapshot(100.0, HashMap::from([("TOKEN".to_string(), 1.0)]))
                .unwrap();
            account.record_settled_token_values(&HashMap::from([("TOKEN".to_string(), 1.0)]));
            assert!(matches!(
                account.record_authenticated_terminal_trade_noop(
                    "historical-trade",
                    "CONFIRMED",
                    "historical-oid",
                    "TOKEN",
                    Side::Buy,
                    6.24,
                    0.42,
                    false,
                ),
                TradeTransitionResult::OwnedNoop(_)
            ));
            account.flush_persistence(Duration::from_secs(2)).unwrap();
        }

        {
            let restored = SharedAccount::new_persistent("authenticated-noop", &path).unwrap();
            let before = restored.monitoring_snapshot();
            assert!(matches!(
                restored.apply_trade_transition_with_context(
                    "historical-trade",
                    "CONFIRMED",
                    "",
                    "historical-oid",
                    "TOKEN",
                    Side::Buy,
                    6.24,
                    0.42,
                    false,
                    1,
                ),
                TradeTransitionResult::OwnedNoop(_)
            ));
            let after = restored.monitoring_snapshot();
            assert_eq!(after.physical_cash, before.physical_cash);
            assert_eq!(after.physical_positions, before.physical_positions);
            assert!(!after.uncertain);
        }

        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(PathBuf::from(lock_path));
    }

    #[test]
    fn persistent_restart_rejects_unjournaled_cross_instance_balance_transfer() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-economic-replay-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        {
            let account = SharedAccount::new_persistent("economic-replay", &path).unwrap();
            account.register_instance("a", 1.0);
            account.register_instance("b", 1.0);
            account
                .apply_physical_snapshot(100.0, HashMap::new())
                .unwrap();
            account.flush_persistence(Duration::from_secs(2)).unwrap();
        }
        // Runtime mutations are WAL-backed; reopening folds them into the
        // snapshot before this test deliberately tampers with that snapshot.
        drop(SharedAccount::new_persistent("economic-replay", &path).unwrap());

        let mut persisted: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let a = persisted["state"]["instances"]["a"]["cash"]
            .as_f64()
            .unwrap();
        let b = persisted["state"]["instances"]["b"]["cash"]
            .as_f64()
            .unwrap();
        persisted["state"]["instances"]["a"]["cash"] = serde_json::json!(a + 1.0);
        persisted["state"]["instances"]["b"]["cash"] = serde_json::json!(b - 1.0);
        std::fs::write(&path, serde_json::to_vec(&persisted).unwrap()).unwrap();

        let error = match SharedAccount::new_persistent("economic-replay", &path) {
            Ok(_) => panic!("unjournaled per-instance transfer must fail closed"),
            Err(error) => error,
        };
        assert!(error.contains("immutable-baseline replay"), "{error}");
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(PathBuf::from(lock_path));
    }

    #[test]
    fn legacy_seed_is_derived_once_and_persisted_as_immutable_baseline() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-legacy-seed-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        {
            let account = SharedAccount::new_persistent("legacy-seed", &path).unwrap();
            account.register_instance("a", 1.0);
            account
                .apply_physical_snapshot(100.0, HashMap::new())
                .unwrap();
            account.flush_persistence(Duration::from_secs(2)).unwrap();
        }
        // Materialize the WAL-backed seed before simulating a legacy snapshot
        // that predates the immutable baseline field.
        drop(SharedAccount::new_persistent("legacy-seed", &path).unwrap());
        let mut persisted: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        persisted["state"]
            .as_object_mut()
            .unwrap()
            .remove("seed_baseline");
        std::fs::write(&path, serde_json::to_vec(&persisted).unwrap()).unwrap();

        {
            let restored = SharedAccount::new_persistent("legacy-seed", &path).unwrap();
            restored.flush_persistence(Duration::from_secs(2)).unwrap();
        }
        let upgraded: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            upgraded["state"]["seed_baseline"]["legacy_derived"],
            serde_json::json!(true),
        );
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(PathBuf::from(lock_path));
    }

    #[test]
    fn duplicate_trade_context_replay_does_not_flush_unchanged_ledger() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-duplicate-trade-ledger-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        {
            let account = SharedAccount::new_persistent("duplicate-trade", &path).unwrap();
            account.register_instance("a", 1.0);
            account.apply_physical_snapshot(100.0, HashMap::new());
            account
                .reserve_order(
                    "a",
                    "a-duplicate",
                    "oid-duplicate",
                    "UP",
                    Side::Buy,
                    10.0,
                    0.5,
                    0,
                )
                .unwrap();
            assert!(account
                .apply_trade_transition_with_context(
                    "trade-duplicate",
                    "MATCHED",
                    "a-duplicate",
                    "oid-duplicate",
                    "UP",
                    Side::Buy,
                    10.0,
                    0.5,
                    false,
                    123,
                )
                .ownership()
                .is_some());
            let before = account.monitoring_snapshot();
            assert!(account
                .apply_trade_transition_with_context(
                    "trade-duplicate",
                    "MATCHED",
                    "a-duplicate",
                    "oid-duplicate",
                    "UP",
                    Side::Buy,
                    10.0,
                    0.5,
                    false,
                    123,
                )
                .ownership()
                .is_some());
            let after = account.monitoring_snapshot();
            assert_eq!(after.persistence_flushes, before.persistence_flushes);
            assert_eq!(after.persistence_writes, before.persistence_writes);
        }
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(PathBuf::from(lock_path));
    }

    #[test]
    fn order_admission_does_not_wait_for_a_failed_wal_writer() {
        let _persistence_guard = persistence_test_guard();
        let root = std::env::temp_dir().join(format!(
            "hexagent-order-async-persistence-{}-{}",
            std::process::id(),
            wall_clock_ms(),
        ));
        let moved = root.with_extension("moved");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("account.json");
        let account = SharedAccount::new_persistent("order-async", &path).unwrap();
        account.register_instance("a", 1.0);
        account
            .apply_physical_snapshot(100.0, HashMap::new())
            .unwrap();
        account.flush_persistence(Duration::from_secs(2)).unwrap();

        // Keep the ledger lock alive while making every new WAL open fail.
        // Order admission must still update the in-memory shared reservation
        // and return without invoking a persistence flush.
        std::fs::rename(&root, &moved).unwrap();
        std::fs::write(&root, b"not-a-directory").unwrap();
        account
            .register_token_interest("a", "condition", "UP", "DOWN")
            .unwrap();
        account
            .flush_persistence(Duration::from_secs(2))
            .unwrap_err();
        let flushes_before = account.monitoring_snapshot().persistence_flushes;

        let ownership = account
            .reserve_order("a", "a-order", "oid-order", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();
        assert_eq!(ownership.client_order_id, "a-order");
        let after = account.monitoring_snapshot();
        assert_eq!(after.persistence_flushes, flushes_before);
        assert!(after.persistence_error.is_some());
        assert_eq!(account.instance_snapshot("a").unwrap().reserved_cash, 5.0);

        drop(account);
        let _ = std::fs::remove_file(&root);
        let _ = std::fs::remove_dir_all(&moved);
    }

    #[test]
    fn applied_trade_does_not_wait_for_persistence_failure_and_blocks_next_admission() {
        let _persistence_guard = persistence_test_guard();
        let root = std::env::temp_dir().join(format!(
            "hexagent-trade-persistence-failure-{}-{}",
            std::process::id(),
            wall_clock_ms(),
        ));
        let moved = root.with_extension("moved");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("account.json");
        let account = SharedAccount::new_persistent("trade-pending", &path).unwrap();
        account.register_instance("a", 1.0);
        account.apply_physical_snapshot(100.0, HashMap::new());
        account
            .reserve_order(
                "a",
                "a-pending",
                "oid-pending",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();
        account.flush_persistence(Duration::from_secs(2)).unwrap();

        // Keep the already-open ledger lock alive but make all future atomic
        // writes fail deterministically: the parent path is now a plain file.
        std::fs::rename(&root, &moved).unwrap();
        std::fs::write(&root, b"not-a-directory").unwrap();
        let result = account.apply_trade_transition_with_context(
            "trade-pending",
            "MATCHED",
            "a-pending",
            "oid-pending",
            "UP",
            Side::Buy,
            10.0,
            0.5,
            false,
            123,
        );
        assert!(matches!(result, TradeTransitionResult::Applied(_)));
        let snapshot = account.instance_snapshot("a").unwrap();
        assert_eq!(snapshot.positions.get("UP").copied(), Some(10.0));
        assert_eq!(snapshot.cash, 95.0);
        // The private-event caller returns immediately. Wait only in the test
        // for the background writer to expose its deterministic failure, then
        // prove the next admission observes it without an fsync barrier.
        for _ in 0..200 {
            if account.monitoring_snapshot().persistence_error.is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(account.monitoring_snapshot().persistence_error.is_some());
        assert!(matches!(
            account.reserve_order("a", "blocked", "oid-blocked", "UP", Side::Buy, 1.0, 0.5, 0,),
            Err(ReservationError::PersistenceUnavailable(_))
                | Err(ReservationError::AccountUncertain)
        ));
        assert!(account.is_uncertain());

        drop(account);
        let _ = std::fs::remove_file(&root);
        let _ = std::fs::remove_dir_all(&moved);
    }

    #[test]
    fn virtual_instance_reservations_do_not_wait_for_account_state_mutex() {
        let account = Arc::new(seeded_account());
        let cold_state = account.state.lock().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let mut workers = Vec::new();
        for (instance_id, coid, oid, quantity) in [
            ("a", "a-fast", "oid-a-fast", 100.0),
            ("b", "b-fast", "oid-b-fast", 300.0),
        ] {
            let account = Arc::clone(&account);
            let tx = tx.clone();
            workers.push(std::thread::spawn(move || {
                let result = account.reserve_order(
                    instance_id,
                    coid,
                    oid,
                    "UP",
                    Side::Buy,
                    quantity,
                    1.0,
                    0,
                );
                tx.send(result.is_ok()).unwrap();
            }));
        }
        drop(tx);
        assert!(rx.recv_timeout(Duration::from_secs(1)).unwrap());
        assert!(rx.recv_timeout(Duration::from_secs(1)).unwrap());
        drop(cold_state);
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(account.instance_snapshot("a").unwrap().reserved_cash, 100.0);
        assert_eq!(account.instance_snapshot("b").unwrap().reserved_cash, 300.0);
        assert_eq!(
            account
                .monitoring_snapshot()
                .reservation_control_lock
                .acquisitions,
            0,
            "instance reservation must not acquire the account control gate",
        );
    }

    #[test]
    fn cold_control_snapshot_preserves_concurrent_instance_reservation() {
        let account = Arc::new(seeded_account());
        let cold_state = account.lock_state();
        let (tx, rx) = std::sync::mpsc::channel();
        let worker_account = Arc::clone(&account);
        let worker = std::thread::spawn(move || {
            let result = worker_account.reserve_order(
                "a",
                "a-during-control",
                "oid-during-control",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                0,
            );
            tx.send(result).unwrap();
        });

        let ownership = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("reservation must not wait for the control transaction")
            .unwrap();
        drop(cold_state);
        worker.join().unwrap();

        assert_eq!(account.order(&ownership.client_order_id), Some(ownership));
        assert_eq!(account.instance_snapshot("a").unwrap().reserved_cash, 5.0);
        assert_eq!(
            account
                .monitoring_snapshot()
                .reservation_control_lock
                .acquisitions,
            0,
        );
    }

    #[test]
    fn order_lookup_repairs_route_hole_without_account_uncertainty() {
        let account = seeded_account();
        let ownership = account
            .reserve_order(
                "a",
                "a-route-repair",
                "oid-route-repair",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();
        account.coid_routes.remove(&ownership.client_order_id);
        account
            .oid_routes
            .remove(&normalize_order_id(&ownership.order_id));

        assert_eq!(
            account.order(&ownership.client_order_id),
            Some(ownership.clone())
        );
        assert_eq!(
            account.order_owner_by_oid(&ownership.order_id).as_deref(),
            Some("a"),
        );
        assert!(!account.is_uncertain());
    }

    #[test]
    fn order_oid_lookup_recovers_retired_route_before_clearing_validated_anomaly() {
        let account = seeded_account();
        let ownership = account
            .reserve_order(
                "a",
                "probe:a:oid-late-probe",
                "oid-late-probe",
                "UP",
                Side::Buy,
                10.0,
                0.01,
                0,
            )
            .unwrap();
        account.release_order(&ownership.client_order_id, OrderStatus::Cancelled);
        let ownership = account.order(&ownership.client_order_id).unwrap();
        account.coid_routes.remove(&ownership.client_order_id);
        account
            .oid_routes
            .remove(&normalize_order_id(&ownership.order_id));
        account.mark_private_order_event_anomaly_with_token(
            &ownership.order_id,
            None,
            Some(&ownership.token_id),
            "late lifecycle after runtime route retirement",
        );

        assert_eq!(
            account.order_by_oid(&ownership.order_id),
            Some(ownership.clone())
        );
        assert_eq!(
            account.order_owner_by_oid(&ownership.order_id).as_deref(),
            Some("a"),
        );
        assert!(
            account.is_uncertain(),
            "lookup alone is not event validation"
        );
        assert_eq!(
            account.reconcile_order_route(&ownership.client_order_id, &ownership.order_id),
            Some(ownership),
        );
        assert!(!account.is_uncertain());
        assert!(account.ownership_anomalies().is_empty());
    }

    #[test]
    fn mirrored_order_backfill_is_idempotent_and_clears_exact_private_anomaly() {
        let account = seeded_account();
        let ownership = account
            .reserve_order(
                "a",
                "a-row-backfill",
                "oid-row-backfill",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();
        account.mark_private_event_anomaly(
            "order:oid-row-backfill",
            "test runtime mapping without lifecycle row",
        );
        let virtual_account = account.virtual_account("a").unwrap();
        virtual_account
            .lifecycle
            .lock()
            .unwrap()
            .orders
            .remove(&ownership.client_order_id);

        assert_eq!(
            account.backfill_order_ownership(&ownership),
            Some(ownership.clone()),
        );
        assert_eq!(
            account.backfill_order_ownership(&ownership),
            Some(ownership)
        );
        assert_eq!(account.instance_snapshot("a").unwrap().reserved_cash, 5.0);
        assert!(!account.is_uncertain());
        assert!(account.ownership_anomalies().is_empty());
    }

    #[test]
    fn legacy_orphan_order_anomaly_is_enumerated_without_a_ledger_row() {
        let account = seeded_account();
        account.mark_private_event_anomaly(
            "order:ABCDEF",
            "invalid Polymarket private event `order:abcdef`: order lifecycle event has runtime mapping but no ledger row coid `a-123`",
        );
        assert_eq!(
            account.persisted_orphan_order_anomalies(),
            vec![PersistedOrphanOrderAnomaly {
                anomaly_key: "private_event:order:ABCDEF".to_string(),
                order_id: "ABCDEF".to_string(),
                client_order_id: Some("a-123".to_string()),
                token_id: None,
            }],
        );
        assert!(account.record_terminal_orphan_order_audit(
            "abcdef",
            Some("a-123"),
            OrderStatus::Cancelled,
            1.0,
            0.0,
            &[],
            "authenticated legacy zero-fill audit",
        ));
        assert!(account.ownership_anomalies().is_empty());
    }

    #[test]
    fn zero_fill_terminal_orphan_audit_tombstone_reopens_and_covers_replay() {
        let account = seeded_account();
        account.mark_private_order_event_anomaly(
            "0xABCDEF",
            Some("a-123"),
            "missing instance lifecycle row",
        );
        assert!(account.is_uncertain());
        assert!(account.record_terminal_orphan_order_audit(
            "abcdef",
            Some("a-123"),
            OrderStatus::Cancelled,
            10.0,
            0.0,
            &[],
            "authenticated zero-fill cancellation",
        ));
        assert!(!account.is_uncertain());
        assert!(account.ownership_anomalies().is_empty());
        assert!(account.retired_order_audit_covers("0xABCDEF", 10.0, 0.0));
        account.mark_private_order_event_anomaly(
            "0xabcdef",
            Some("a-123"),
            "late duplicate lifecycle",
        );
        assert!(account.ownership_anomalies().is_empty());
    }

    #[test]
    fn authoritative_absent_orphan_tombstone_covers_only_zero_fill_replay() {
        let account = seeded_account();
        account.mark_private_order_event_anomaly_with_token(
            "0xCOMPACTED",
            Some("a-1700000000000"),
            Some("TOKEN"),
            "historical route without ledger row",
        );
        assert_eq!(
            account.persisted_orphan_order_anomalies()[0]
                .token_id
                .as_deref(),
            Some("TOKEN"),
        );
        assert!(account.record_authoritative_absent_orphan_order_audit(
            "compacted",
            Some("a-1700000000000"),
            "authenticated order not-found; complete trades audit; event settled",
        ));
        assert!(account.retired_order_audit_covers("0xCOMPACTED", 37.5, 0.0));
        assert!(!account.retired_order_audit_covers("0xCOMPACTED", 37.5, 0.1));
        assert!(!account.is_uncertain());
    }

    #[test]
    fn orphan_order_hint_and_terminal_audit_survive_restart() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-orphan-order-audit-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        {
            let account = SharedAccount::new_persistent("orphan-audit", &path).unwrap();
            account.mark_private_order_event_anomaly(
                "0xDURABLE",
                Some("maker-42"),
                "missing lifecycle row",
            );
            account.flush_persistence(Duration::from_secs(2)).unwrap();
        }
        {
            let account = SharedAccount::new_persistent("orphan-audit", &path).unwrap();
            assert_eq!(
                account.persisted_orphan_order_anomalies(),
                vec![PersistedOrphanOrderAnomaly {
                    anomaly_key: "private_event:order:durable".to_string(),
                    order_id: "0xDURABLE".to_string(),
                    client_order_id: Some("maker-42".to_string()),
                    token_id: None,
                }],
            );
            assert!(account.record_terminal_orphan_order_audit(
                "durable",
                Some("maker-42"),
                OrderStatus::Cancelled,
                5.0,
                0.0,
                &[],
                "authenticated terminal zero-fill lookup",
            ));
            account.flush_persistence(Duration::from_secs(2)).unwrap();
        }
        let restored = SharedAccount::new_persistent("orphan-audit", &path).unwrap();
        assert!(restored.persisted_orphan_order_anomalies().is_empty());
        assert!(restored.retired_order_audit_covers("0xDURABLE", 5.0, 0.0));
        drop(restored);
        remove_persistence_test_files(&path);
    }

    #[test]
    fn virtual_order_lifecycle_does_not_wait_for_account_state_mutex() {
        let account = Arc::new(seeded_account());
        account
            .reserve_order(
                "a",
                "a-lifecycle-fast",
                "oid-lifecycle-fast",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();
        let cold_state = account.state.lock().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let worker_account = Arc::clone(&account);
        let worker = std::thread::spawn(move || {
            assert_eq!(
                worker_account
                    .mark_order_status_effective("a-lifecycle-fast", OrderStatus::Accepted,),
                Some(OrderStatus::Accepted),
            );
            worker_account.begin_order_recovery(["a-lifecycle-fast"]);
            assert_eq!(
                worker_account.recovery_pending_order_ids(),
                vec!["a-lifecycle-fast".to_string()]
            );
            assert_eq!(
                worker_account.pending_order_audit_ids(),
                vec!["a-lifecycle-fast".to_string()]
            );
            assert_eq!(
                worker_account.order_audit_instance_blocker("a"),
                Some(vec!["a-lifecycle-fast".to_string()])
            );
            worker_account.finish_order_recovery("a-lifecycle-fast");
            assert!(worker_account.recovery_pending_order_ids().is_empty());
            assert!(worker_account.order_audit_instance_blocker("a").is_none());
            assert!(worker_account
                .record_sidecar_checkpoint(
                    "a",
                    DurableSidecarCheckpoint {
                        generation: 1,
                        expected_entries: 1,
                        recovery_payload: "instance-a".to_string(),
                    },
                )
                .unwrap());
            assert_eq!(
                worker_account.sidecar_checkpoint("a").unwrap().generation,
                1
            );
            assert!(worker_account.mark_cancelled_pending_audit("a-lifecycle-fast"));
            assert!(!worker_account.mark_cancelled_pending_trade_audit("a-lifecycle-fast", 0.0));
            worker_account.release_order("a-lifecycle-fast", OrderStatus::Cancelled);
            tx.send(()).unwrap();
        });
        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drop(cold_state);
        worker.join().unwrap();
        let order = account.order("a-lifecycle-fast").unwrap();
        assert_eq!(order.status, OrderStatus::Cancelled);
        assert_eq!(order.reserved_cash, 0.0);
    }

    #[test]
    fn valid_virtual_cancel_audit_repairs_prior_cold_anomaly() {
        let account = seeded_account();
        account
            .reserve_order(
                "a",
                "a-cancel-repair",
                "oid-cancel-repair",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();
        assert!(account.mark_cancelled_pending_trade_audit("a-cancel-repair", 11.0));
        assert!(account.is_uncertain());
        assert!(!account.mark_cancelled_pending_trade_audit("a-cancel-repair", 0.0));
        assert!(!account.is_uncertain());
        assert!(account.ownership_anomalies().is_empty());
    }

    #[test]
    fn durable_trade_completion_does_not_reenter_account_state_on_next_admission() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-virtual-post-trade-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        let account = Arc::new(SharedAccount::new_persistent("virtual-post-trade", &path).unwrap());
        account.register_instance("a", 1.0);
        account
            .apply_physical_snapshot(100.0, HashMap::new())
            .unwrap();
        account
            .reserve_order(
                "a",
                "a-filled-fast",
                "oid-filled-fast",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();
        assert!(account
            .apply_trade_transition_with_context(
                "trade-filled-fast",
                "MATCHED",
                "a-filled-fast",
                "oid-filled-fast",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                true,
                123,
            )
            .ownership()
            .is_some());
        account.flush_persistence(Duration::from_secs(2)).unwrap();
        // The writer can finish before `track_trade_persistence_generation`
        // returns. Re-arm the already-durable generation so this test always
        // exercises the quote/admission cleanup edge.
        let durable_generation = account.persistence.as_ref().unwrap().scheduled_generation();
        account
            .trade_persistence_pending_generation
            .store(durable_generation, Ordering::Release);

        let cold_state = account.state.lock().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let worker_account = Arc::clone(&account);
        let worker = std::thread::spawn(move || {
            tx.send(
                worker_account
                    .reserve_order(
                        "a",
                        "a-after-trade-fast",
                        "oid-after-trade-fast",
                        "UP",
                        Side::Buy,
                        1.0,
                        0.5,
                        0,
                    )
                    .is_ok(),
            )
            .unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_secs(1)).unwrap());
        drop(cold_state);
        worker.join().unwrap();
        drop(account);
        remove_persistence_test_files(&path);
    }

    #[test]
    fn normal_private_trade_does_not_wait_for_account_control_gate() {
        let account = Arc::new(seeded_account());
        account
            .reserve_order(
                "a",
                "a-trade-no-control",
                "oid-trade-no-control",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
            .unwrap();
        // Hold a real cold transaction, including its pre-trade aggregate
        // snapshot. The fill must both complete without the gate and survive
        // the guard's later shard publication.
        let control = account.lock_state();
        let worker_account = Arc::clone(&account);
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let transition = worker_account.apply_trade_transition_with_context(
                "trade-no-control",
                "MATCHED",
                "a-trade-no-control",
                "oid-trade-no-control",
                "UP",
                Side::Buy,
                1.0,
                0.5,
                true,
                123,
            );
            tx.send(transition.ownership().is_some()).unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_secs(1)).unwrap());
        drop(control);
        worker.join().unwrap();
        let trade = account.trade_ownership("trade-no-control").unwrap();
        assert_eq!(trade.client_order_id, "a-trade-no-control");
        assert_eq!(
            account.order("a-trade-no-control").unwrap().filled_quantity,
            1.0
        );
    }

    #[test]
    fn normal_trade_replay_anchor_miss_does_not_wait_for_control_gate() {
        let account = Arc::new(seeded_account());
        let control = account.lock_state();
        let worker_account = Arc::clone(&account);
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            worker_account.resolve_unresolved_trade_match_time("ordinary-trade");
            tx.send(()).unwrap();
        });
        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drop(control);
        worker.join().unwrap();
    }

    #[test]
    fn filled_retirement_readiness_does_not_wait_for_control_gate() {
        let account = Arc::new(seeded_account());
        account
            .reserve_order(
                "a",
                "a-filled-ready",
                "oid-filled-ready",
                "UP",
                Side::Buy,
                1.0,
                0.5,
                0,
            )
            .unwrap();
        assert!(account
            .apply_trade_transition_with_context(
                "trade-filled-ready",
                "MATCHED",
                "a-filled-ready",
                "oid-filled-ready",
                "UP",
                Side::Buy,
                1.0,
                0.5,
                true,
                123,
            )
            .ownership()
            .is_some());
        let control = account.lock_state();
        let worker_account = Arc::clone(&account);
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            tx.send(worker_account.filled_order_ready_for_retirement("a-filled-ready"))
                .unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_secs(1)).unwrap());
        drop(control);
        worker.join().unwrap();
    }

    #[test]
    fn normal_status_update_does_not_wait_for_control_gate_and_survives_publish() {
        let account = Arc::new(seeded_account());
        account
            .reserve_order(
                "a",
                "a-status-no-control",
                "oid-status-no-control",
                "UP",
                Side::Buy,
                1.0,
                0.5,
                0,
            )
            .unwrap();
        let control = account.lock_state();
        let worker_account = Arc::clone(&account);
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            tx.send(
                worker_account
                    .mark_order_status_effective("a-status-no-control", OrderStatus::Accepted),
            )
            .unwrap();
        });
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Some(OrderStatus::Accepted),
        );
        drop(control);
        worker.join().unwrap();
        assert_eq!(
            account.order("a-status-no-control").unwrap().status,
            OrderStatus::Accepted,
        );
    }

    #[test]
    fn identical_rebind_is_zero_write_and_virtual_lifecycle_restarts() {
        let _persistence_guard = persistence_test_guard();
        let path = std::env::temp_dir().join(format!(
            "hexagent-virtual-lifecycle-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        {
            let account = SharedAccount::new_persistent("virtual-lifecycle", &path).unwrap();
            account.register_instance("a", 1.0);
            account
                .apply_physical_snapshot(100.0, HashMap::new())
                .unwrap();
            account
                .reserve_order(
                    "a",
                    "a-durable-fast",
                    "0xABCDEF",
                    "UP",
                    Side::Buy,
                    10.0,
                    0.5,
                    0,
                )
                .unwrap();
            account.flush_persistence(Duration::from_secs(2)).unwrap();
            let generation = account.persistence.as_ref().unwrap().scheduled_generation();
            assert!(account.rebind_order_id("a-durable-fast", "abcdef"));
            assert_eq!(
                account.persistence.as_ref().unwrap().scheduled_generation(),
                generation,
            );
            assert!(account.mark_cancelled_pending_audit("a-durable-fast"));
            assert!(!account.mark_cancelled_pending_trade_audit("a-durable-fast", 0.0));
            account.release_order("a-durable-fast", OrderStatus::Cancelled);
            account.flush_persistence(Duration::from_secs(2)).unwrap();
        }
        let restored = SharedAccount::new_persistent("virtual-lifecycle", &path).unwrap();
        let order = restored.order("a-durable-fast").unwrap();
        assert_eq!(order.status, OrderStatus::Cancelled);
        assert_eq!(order.reserved_cash, 0.0);
        drop(restored);
        remove_persistence_test_files(&path);
    }
}
