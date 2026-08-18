//! Account-scoped physical/virtual bookkeeping for shared-wallet strategies.
//!
//! One [`SharedAccount`] is owned by one exchange account. It is the
//! admission-control source of truth shared by every strategy instance on the
//! wallet: physical funds/positions are the hard ceiling, while each
//! instance's weighted virtual balance/inventory is its private ceiling.

use hexagent_types::types::{AuthoritativeOrderAudit, BinaryOption, OrderStatus, Side};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
const TRADE_PERSISTENCE_RISK_BLOCKER: &str = "account_persistence:trade";
const FEE_ATTRIBUTION_RISK_BLOCKER_PREFIX: &str = "fee_attribution:";
/// Settled-event FIFO eviction may race a pinned gap replay by many hours.
/// Keep a lightweight, durable ownership proof long after the full order and
/// trade rows have been compacted so an already-applied fill remains an
/// attributable no-op instead of becoming an `unowned trade`.
const RETIRED_TRADE_TOMBSTONE_TTL_MS: u64 = 90 * 24 * 60 * 60 * 1_000;
const MAX_RETIRED_TRADE_TOMBSTONES: usize = 100_000;

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
    reserved_cash: f64,
    reserved_positions: HashMap<String, f64>,
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
            token_interests: BTreeMap::new(),
            market_scopes: HashSet::new(),
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
    state: SharedAccountState,
}

#[derive(Debug)]
struct PersistJob {
    generation: u64,
    snapshot: PersistedAccount,
}

#[derive(Debug)]
struct PersistenceProgress {
    completed_generation: u64,
    last_error: Option<String>,
    writes: u64,
    write_last_us: u64,
    write_max_us: u64,
}

/// Latest-value asynchronous writer. Serialization and filesystem I/O stay on
/// the writer thread; admission paths may wait for its fsync generation before
/// sending externally visible work. Bursts coalesce to the newest generation.
#[derive(Debug)]
struct AccountPersistence {
    path: PathBuf,
    _lock_file: std::fs::File,
    pending: Arc<Mutex<Option<PersistJob>>>,
    wake: std::sync::mpsc::SyncSender<()>,
    next_generation: AtomicU64,
    progress: Arc<(Mutex<PersistenceProgress>, Condvar)>,
    flushes: AtomicU64,
    flush_last_us: AtomicU64,
    flush_max_us: AtomicU64,
}

impl AccountPersistence {
    fn start(path: PathBuf) -> Result<Self, String> {
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
        let pending = Arc::new(Mutex::new(None::<PersistJob>));
        let progress = Arc::new((
            Mutex::new(PersistenceProgress {
                completed_generation: 0,
                last_error: None,
                writes: 0,
                write_last_us: 0,
                write_max_us: 0,
            }),
            Condvar::new(),
        ));
        let (wake, rx) = std::sync::mpsc::sync_channel::<()>(1);
        let thread_pending = Arc::clone(&pending);
        let thread_progress = Arc::clone(&progress);
        let thread_path = path.clone();
        std::thread::Builder::new()
            .name(format!(
                "account-ledger-{}",
                path.file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("writer")
            ))
            .spawn(move || {
                hexagent_runtime::os_tune::pin_background("account-ledger-writer");
                while rx.recv().is_ok() {
                    loop {
                        let Some(job) = thread_pending.lock().unwrap().take() else {
                            break;
                        };
                        let started = std::time::Instant::now();
                        let result = write_persisted_account(&thread_path, &job.snapshot);
                        let elapsed_us = started.elapsed().as_micros().min(u64::MAX as u128) as u64;
                        let (lock, cv) = &*thread_progress;
                        let mut state = lock.lock().unwrap();
                        state.completed_generation = state.completed_generation.max(job.generation);
                        state.last_error = result.err();
                        state.writes = state.writes.saturating_add(1);
                        state.write_last_us = elapsed_us;
                        state.write_max_us = state.write_max_us.max(elapsed_us);
                        cv.notify_all();
                    }
                }
            })
            .map_err(|error| format!("spawn account ledger writer: {error}"))?;
        Ok(Self {
            path,
            _lock_file: lock_file,
            pending,
            wake,
            next_generation: AtomicU64::new(0),
            progress,
            flushes: AtomicU64::new(0),
            flush_last_us: AtomicU64::new(0),
            flush_max_us: AtomicU64::new(0),
        })
    }

    fn schedule(&self, snapshot: PersistedAccount) {
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed) + 1;
        *self.pending.lock().unwrap() = Some(PersistJob {
            generation,
            snapshot,
        });
        let _ = self.wake.try_send(());
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
        let _ = self.wake.try_send(());
        let (lock, cv) = &*self.progress;
        let progress = lock.lock().unwrap();
        let (progress, wait) = cv
            .wait_timeout_while(progress, timeout, |p| p.completed_generation < target)
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

    /// Error-only admission rollback barrier. Returning before this generation
    /// is durable could restore a never-submitted reservation after a crash.
    fn flush_blocking(&self) -> Result<(), String> {
        let target = self.next_generation.load(Ordering::Relaxed);
        if target == 0 {
            return Ok(());
        }
        let _ = self.wake.try_send(());
        let (lock, cv) = &*self.progress;
        let progress = lock
            .lock()
            .map_err(|_| "account ledger writer progress lock poisoned".to_string())?;
        let progress = cv
            .wait_while(progress, |state| state.completed_generation < target)
            .map_err(|_| "account ledger writer progress lock poisoned".to_string())?;
        if let Some(error) = &progress.last_error {
            return Err(error.clone());
        }
        Ok(())
    }

    fn last_error(&self) -> Option<String> {
        self.progress
            .0
            .lock()
            .ok()
            .and_then(|p| p.last_error.clone())
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
    state: Mutex<SharedAccountState>,
    persistence: Option<AccountPersistence>,
    /// Highest account-persistence generation whose trade mutation timed out
    /// at the synchronous durability barrier. Non-blocking health reads clear
    /// the corresponding blocker once the worker proves this generation.
    trade_persistence_pending_generation: AtomicU64,
    /// Edge-triggered wakeup for the account-scoped order-audit worker. The
    /// generation prevents missed notifications between the worker's health
    /// snapshot and its wait call.
    order_audit_wakeup: (Mutex<u64>, Condvar),
}

impl SharedAccount {
    pub fn new(account_id: impl Into<String>) -> Self {
        Self {
            account_id: account_id.into(),
            state: Mutex::new(SharedAccountState::default()),
            persistence: None,
            trade_persistence_pending_generation: AtomicU64::new(0),
            order_audit_wakeup: (Mutex::new(0), Condvar::new()),
        }
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
        let (state, migrated_state) = if path.exists() {
            let bytes = std::fs::read(&path)
                .map_err(|error| format!("read account ledger {}: {error}", path.display()))?;
            let persisted: PersistedAccount = serde_json::from_slice(&bytes)
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
            let mut state = persisted.state;
            if !allow_query_repair && !state.startup_query_repair_orders.is_empty() {
                let mut pending: Vec<String> = state
                    .startup_query_repair_orders
                    .iter()
                    .cloned()
                    .collect();
                pending.sort();
                return Err(format!(
                    "account ledger {} has unfinished authoritative startup query repair(s): coids={pending:?}",
                    path.display(),
                ));
            }
            let persisted_uncertainty = state.uncertain.then(|| {
                (
                    state.uncertain_reason.clone(),
                    state.uncertain_since_ms,
                )
            });
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
            let mut migrated = normalize_terminal_failed_state(&mut state);
            if migrated {
                recompute_reconciliation(&mut state, "terminal FAILED ledger migration");
            }
            if state.seeded && state.seed_baseline.is_none() {
                state.seed_baseline = Some(derive_legacy_seed_baseline(&state));
                migrated = true;
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
            migrated |= role_migrated;
            if role_migrated {
                recompute_reconciliation(&mut state, "legacy trade-role attribution migration");
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
                migrated = true;
                recompute_reconciliation(&mut state, "durable trade-persistence blocker recovery");
            }
            if allow_query_repair {
                let (query_orders, repair_mutated) =
                    repair_failed_trade_under_reservations_for_query(&mut state)?;
                if repair_mutated {
                    migrated = true;
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
                migrated = true;
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
                    migrated = true;
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
            validate_persisted_state(&account_id, &state)
                .map_err(|error| format!("invalid account ledger {}: {error}", path.display()))?;
            (state, migrated)
        } else {
            (SharedAccountState::default(), false)
        };
        let persistence = AccountPersistence::start(path)?;
        let account = Self {
            account_id,
            state: Mutex::new(state),
            persistence: Some(persistence),
            trade_persistence_pending_generation: AtomicU64::new(0),
            order_audit_wakeup: (Mutex::new(0), Condvar::new()),
        };
        if migrated_state {
            let state = account.state.lock().unwrap();
            account.schedule_persist(&state);
        }
        Ok(account)
    }

    /// Query-repair orders that still lack authoritative terminal/live
    /// resolution. An empty result is the live-startup admission condition.
    pub fn startup_query_repair_pending_order_ids(&self) -> Vec<String> {
        let state = self.state.lock().unwrap();
        let mut pending: Vec<String> =
            state.startup_query_repair_orders.iter().cloned().collect();
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

    fn schedule_persist(&self, state: &SharedAccountState) {
        if let Some(persistence) = &self.persistence {
            persistence.schedule(PersistedAccount {
                version: PERSISTENCE_VERSION,
                account_id: self.account_id.clone(),
                state: state.clone(),
            });
        }
    }

    pub fn flush_persistence(&self, timeout: Duration) -> Result<(), String> {
        self.persistence
            .as_ref()
            .map_or(Ok(()), |p| p.flush(timeout))
    }

    fn refresh_trade_persistence_blocker(&self) {
        let generation = self
            .trade_persistence_pending_generation
            .load(Ordering::Acquire);
        if generation == 0 {
            return;
        }
        let durable = self
            .persistence
            .as_ref()
            .is_none_or(|persistence| persistence.generation_is_durable(generation));
        if !durable {
            return;
        }
        if self
            .trade_persistence_pending_generation
            .compare_exchange(generation, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.clear_risk_blocker(TRADE_PERSISTENCE_RISK_BLOCKER);
        }
    }

    fn flush_rollback_persistence(&self) -> Result<(), String> {
        self.persistence
            .as_ref()
            .map_or(Ok(()), AccountPersistence::flush_blocking)
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
            let state = self.state.lock().unwrap();
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
        self.state
            .lock()
            .unwrap()
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
        let mut state = self.state.lock().unwrap();
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
        let mut state = self.state.lock().unwrap();
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
        let mut state = self.state.lock().unwrap();
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
        let mut state = self.state.lock().unwrap();
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

        let mut state = self.state.lock().unwrap();
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
            instance.reserved_cash > EPS
                || instance.reserved_positions.values().any(|qty| *qty > EPS)
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
        let mut state = self.state.lock().unwrap();
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
        let mut state = self.state.lock().unwrap();
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
        let interests = state
            .instances
            .values()
            .flat_map(|instance| instance.token_interests.values().cloned())
            .collect();
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
        let state = self.state.lock().unwrap();
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
        let mut state = self.state.lock().unwrap();
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
        let mut state = self.state.lock().unwrap();
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
        self.schedule_persist(&state);
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
        let mut state = self.state.lock().unwrap();
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
        self.schedule_persist(&state);
        Ok(())
    }

    /// Atomically claim every zero-reference event whose durable audit is fully
    /// terminal, compact its account rows and fee curves, and return token sets
    /// for the exchange feed's in-memory tombstone cleanup.
    pub fn finalize_ready_settled_audit_retirements(&self) -> Vec<HashSet<String>> {
        let mut state = self.state.lock().unwrap();
        let ready: Vec<(String, HashSet<String>)> = state
            .settled_audit_references
            .iter()
            .filter(|(_, reference)| reference.instances.is_empty())
            .filter_map(|(condition_id, reference)| {
                let tokens: HashSet<String> = reference.asset_ids.iter().cloned().collect();
                (!settled_audit_has_revisable_rows(&state, &tokens))
                    .then(|| (condition_id.clone(), tokens))
            })
            .collect();
        if ready.is_empty() {
            return Vec::new();
        }
        let mut retired = Vec::with_capacity(ready.len());
        for (condition_id, tokens) in ready {
            let _ = prune_terminal_history_locked(&mut state, None, &tokens);
            state.settled_audit_references.remove(&condition_id);
            retired.push(tokens);
        }
        self.schedule_persist(&state);
        retired
    }

    /// Validate the final configured membership after every instance sharing
    /// this account has registered. Persisted owners missing from config keep
    /// their ledger rows for late attribution, but make admission fail closed
    /// until an explicit external ownership migration is recorded.
    pub fn reconcile_configured_instances(&self, configured: &HashSet<String>) {
        let mut state = self.state.lock().unwrap();
        let mut stale = Vec::new();
        for (instance_id, instance) in &state.instances {
            if configured.contains(instance_id) {
                continue;
            }
            let owned_cash = instance.cash.abs() > EPS || instance.reserved_cash > EPS;
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
        let mut state = self.state.lock().unwrap();
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

    /// Account-wide authoritative outcome snapshot. Strategies compare the
    /// generation before cloning the map, then revise active and retained
    /// settled event baselines.
    pub fn settled_token_values_snapshot(&self) -> (u64, HashMap<String, f64>) {
        let state = self.state.lock().unwrap();
        let generation = if state.settled_token_values.is_empty() {
            state.settled_token_values_generation
        } else {
            // Ledgers written before this field existed load it as zero.
            state.settled_token_values_generation.max(1)
        };
        (generation, state.settled_token_values.clone())
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
        let mut state = self.state.lock().unwrap();
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
        // loop below. Both virtual and physical ledgers receive the exact fee
        // delta, so the state is valid at every persistence boundary.
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
            if previous.physical_fee_booked && changed {
                state.physical_cash += usdc_delta;
                *state
                    .physical_positions
                    .entry(previous.ownership.token_id.clone())
                    .or_insert(0.0) += shares_delta;
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
        let state = self.state.lock().unwrap();
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
        let mut state = self.state.lock().unwrap();
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
        self.state.lock().unwrap().seeded
    }
    pub fn startup_snapshot_applied(&self) -> bool {
        self.state
            .lock()
            .unwrap()
            .startup_snapshot_applied_this_process
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
        let mut state = self.state.lock().unwrap();
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
            || self.state.lock().unwrap().uncertain
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
        fee_degradation_is_only_uncertainty(&self.state.lock().unwrap())
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
        let state = self.state.lock().unwrap();
        state.seeded && (!state.uncertain || fee_degradation_is_only_uncertainty(&state))
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
        let mut state = self.state.lock().unwrap();
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
        set_uncertain(&mut state, format!("{source}: {reason}"));
        self.schedule_persist(&state);
    }

    /// Clear exactly one subsystem blocker, then re-evaluate every remaining
    /// derived account invariant. Callers cannot accidentally reopen admission
    /// for a different source.
    pub fn clear_risk_blocker(&self, source: &str) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.risk_blockers.remove(source.trim()).is_none() {
            return false;
        }
        recompute_reconciliation(&mut state, "risk blocker cleared");
        self.schedule_persist(&state);
        true
    }

    /// Mark potentially-live orders restored from disk. Their durable order
    /// reservations continue to cover quoting while recovery runs; unrelated
    /// balance-changing maintenance remains fail-closed until authoritative
    /// terminal metadata arrives.
    pub fn begin_order_recovery<'a>(&self, client_order_ids: impl IntoIterator<Item = &'a str>) {
        let mut state = self.state.lock().unwrap();
        let before = state.recovery_pending_orders.len();
        state.recovery_pending_orders.extend(
            client_order_ids
                .into_iter()
                .filter(|id| !id.is_empty())
                .map(str::to_string),
        );
        recompute_reconciliation(&mut state, "startup order recovery");
        self.schedule_persist(&state);
        if state.recovery_pending_orders.len() > before {
            self.notify_order_audit_worker();
        }
    }

    pub fn finish_order_recovery(&self, client_order_id: &str) {
        let mut state = self.state.lock().unwrap();
        let recovery_removed = state.recovery_pending_orders.remove(client_order_id);
        let query_repair_removed = state
            .startup_query_repair_orders
            .remove(client_order_id);
        if recovery_removed || query_repair_removed {
            recompute_reconciliation(&mut state, "startup order recovery");
            self.schedule_persist(&state);
        }
    }

    pub fn recovery_pending_order_ids(&self) -> Vec<String> {
        let state = self.state.lock().unwrap();
        let mut pending: Vec<String> = state.recovery_pending_orders.iter().cloned().collect();
        pending.sort();
        pending
    }

    /// All orders still participating in order/trade recovery. Entries that
    /// lack authoritative terminal metadata block balance-changing maintenance
    /// for their owner instance; quote admission continues under retained
    /// reservations while the worker retries metadata and exact private trades.
    pub fn pending_order_audit_ids(&self) -> Vec<String> {
        let state = self.state.lock().unwrap();
        let mut pending: HashSet<String> = state.recovery_pending_orders.clone();
        pending.extend(state.routine_cancel_audits.iter().cloned());
        let mut pending: Vec<String> = pending.into_iter().collect();
        pending.sort();
        pending
    }

    /// Diagnostic for terminal orders still missing a complete authoritative
    /// order audit, scoped to their owner instance. Quote admission does not
    /// use this signal because the original reservation already represents
    /// worst-case exposure; balance-changing maintenance remains blocked.
    pub fn order_audit_instance_blocker(&self, instance_id: &str) -> Option<Vec<String>> {
        let state = self.state.lock().unwrap();
        let mut pending = instance_pending_order_ids_requiring_metadata(&state, instance_id);
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
        let mut state = self.state.lock().unwrap();
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
        let mut state = self.state.lock().unwrap();
        let pages = pages as u64;
        state.gap_replay_last_pages = pages;
        state.gap_replay_max_pages = state.gap_replay_max_pages.max(pages);
        state.gap_replay_total_pages = state.gap_replay_total_pages.saturating_add(pages);
    }

    pub fn record_maintenance_queue_wait(&self, wait: Duration) {
        let mut state = self.state.lock().unwrap();
        let wait_ms = wait.as_millis().min(u64::MAX as u128) as u64;
        state.maintenance_queue_last_wait_ms = wait_ms;
        state.maintenance_queue_max_wait_ms = state.maintenance_queue_max_wait_ms.max(wait_ms);
        state.maintenance_queue_jobs = state.maintenance_queue_jobs.saturating_add(1);
    }

    pub fn monitoring_snapshot(&self) -> AccountMonitoringSnapshot {
        self.refresh_trade_persistence_blocker();
        let state = self.state.lock().unwrap();
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
            instances.push(InstanceAccountSnapshot {
                instance_id: instance_id.clone(),
                weight: instance.weight,
                ledger_generation: state.ledger_generation,
                cash: instance.cash,
                positions: instance.positions.clone(),
                reserved_cash: instance.reserved_cash,
                reserved_positions: instance.reserved_positions.clone(),
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
                .map(|instance| instance.reserved_cash)
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
            retired_trade_ownership_tombstones: state
                .retired_trade_ownership_tombstones
                .values()
                .filter(|tombstone| retired_trade_tombstone_is_live(tombstone, wall_clock_ms()))
                .count(),
            verified_trade_replay_recoveries: state.verified_trade_replay_recoveries,
            persistence_path: self.persistence.as_ref().map(|p| p.path.clone()),
            persistence_error,
            persistence_writes: persistence_metrics.0,
            persistence_write_last_us: persistence_metrics.1,
            persistence_write_max_us: persistence_metrics.2,
            persistence_flushes: persistence_metrics.3,
            persistence_flush_last_us: persistence_metrics.4,
            persistence_flush_max_us: persistence_metrics.5,
        }
    }

    pub fn orders(&self) -> Vec<OrderOwnership> {
        self.state
            .lock()
            .unwrap()
            .orders
            .values()
            .cloned()
            .collect()
    }

    pub fn instance_snapshot(&self, instance_id: &str) -> Option<InstanceAccountSnapshot> {
        let state = self.state.lock().unwrap();
        state
            .instances
            .get(instance_id)
            .map(|instance| InstanceAccountSnapshot {
            instance_id: instance_id.to_string(),
            weight: instance.weight,
            ledger_generation: state.ledger_generation,
            cash: instance.cash,
            positions: instance.positions.clone(),
            reserved_cash: instance.reserved_cash,
            reserved_positions: instance.reserved_positions.clone(),
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
        let state = self.state.lock().unwrap();
        let instance = state.instances.get(instance_id)?;
        // A negative unexplained reconciliation delta means the physical
        // wallet is below the sum of the virtual ledgers. Fail closed until
        // reconciliation instead of letting one instance consume another's
        // allocation.
        // Order/trade audit recovery is not an availability gate: the durable
        // order reservation already excludes its worst-case cash or shares.
        if persistence_failed
            || (state.uncertain
                && !(allow_fee_degraded && fee_degradation_is_only_uncertainty(&state)))
        {
            return Some(AccountAvailability {
                virtual_cash: 0.0,
                physical_cash: 0.0,
                effective_cash: 0.0,
                virtual_position: 0.0,
                physical_position: 0.0,
                effective_position: 0.0,
            });
        }
        let total_reserved_cash: f64 = state.instances.values().map(|i| i.reserved_cash).sum();
        let total_reserved_position: f64 = state
            .instances
            .values()
            .map(|i| i.reserved_positions.get(token).copied().unwrap_or(0.0))
            .sum();
        let virtual_cash = (instance.cash - instance.reserved_cash).max(0.0);
        let physical_cash = (state.physical_cash - total_reserved_cash).max(0.0);
        let virtual_position = (instance.positions.get(token).copied().unwrap_or(0.0)
            - instance
                .reserved_positions
                .get(token)
                .copied()
                .unwrap_or(0.0))
        .max(0.0);
        let physical_position = (state.physical_positions.get(token).copied().unwrap_or(0.0)
            - total_reserved_position)
            .max(0.0);
        Some(AccountAvailability {
            virtual_cash,
            physical_cash,
            effective_cash: virtual_cash.min(physical_cash),
            virtual_position,
            physical_position,
            effective_position: virtual_position.min(physical_position),
        })
    }

    /// Reserve an order and bind both locally-known identifiers before the
    /// network POST. This is the account-wide admission-control gate.
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
        // A previous asynchronous mutation may have failed to persist. Retry
        // the complete current snapshot before changing any reservation.
        self.ensure_admission_persistence()?;
        let mut state = self.state.lock().unwrap();
        if !state.seeded {
            return Err(ReservationError::AccountNotSeeded);
        }
        if state.uncertain && !(allow_fee_degraded && fee_degradation_is_only_uncertainty(&state)) {
            return Err(ReservationError::AccountUncertain);
        }
        if let Some(existing) = state.orders.get(client_order_id) {
            if normalize_order_id(&existing.order_id) == normalize_order_id(order_id)
                && existing.instance_id == instance_id
            {
                let ownership = existing.clone();
                // Retry the latest durable snapshot if a prior async write
                // failed; never send the idempotent network order until its
                // ownership and reservation are fsynced.
                self.schedule_persist(&state);
                drop(state);
                self.flush_admission_persistence()?;
                return Ok(ownership);
            }
            return Err(ReservationError::DuplicateClientOrderId(
                client_order_id.into(),
            ));
        }
        if !state.instances.contains_key(instance_id) {
            return Err(ReservationError::UnknownInstance(instance_id.into()));
        }
        let normalized_order_id = normalize_order_id(order_id);
        if let Some(existing_coid) = state.oid_to_coid.get(&normalized_order_id) {
            return Err(ReservationError::InvalidOrder(format!(
                "order_id `{order_id}` is already owned by client_order_id `{existing_coid}`",
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

        let total_reserved_cash: f64 = state.instances.values().map(|i| i.reserved_cash).sum();
        let total_reserved_qty: f64 = state
            .instances
            .values()
            .map(|i| i.reserved_positions.get(token_id).copied().unwrap_or(0.0))
            .sum();
        let instance = state.instances.get(instance_id).expect("checked above");
        let virtual_cash = (instance.cash - instance.reserved_cash).max(0.0);
        let physical_cash = (state.physical_cash - total_reserved_cash).max(0.0);
        let virtual_qty = (instance.positions.get(token_id).copied().unwrap_or(0.0)
            - instance
                .reserved_positions
                .get(token_id)
                .copied()
                .unwrap_or(0.0))
        .max(0.0);
        let physical_qty = (state
            .physical_positions
            .get(token_id)
            .copied()
            .unwrap_or(0.0)
            - total_reserved_qty)
            .max(0.0);
        if reserve_cash > virtual_cash + EPS {
            return Err(ReservationError::InsufficientVirtualCash {
                required: reserve_cash,
                available: virtual_cash,
            });
        }
        if reserve_cash > physical_cash + EPS {
            return Err(ReservationError::InsufficientPhysicalCash {
                required: reserve_cash,
                available: physical_cash,
            });
        }
        if reserve_qty > virtual_qty + EPS {
            return Err(ReservationError::InsufficientVirtualPosition {
                token: token_id.into(),
                required: reserve_qty,
                available: virtual_qty,
            });
        }
        if reserve_qty > physical_qty + EPS {
            return Err(ReservationError::InsufficientPhysicalPosition {
                token: token_id.into(),
                required: reserve_qty,
                available: physical_qty,
            });
        }

        let instance = state.instances.get_mut(instance_id).expect("checked above");
        instance.reserved_cash += reserve_cash;
        if reserve_qty > 0.0 {
            *instance
                .reserved_positions
                .entry(token_id.into())
                .or_insert(0.0) += reserve_qty;
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
        state
            .oid_to_coid
            .insert(normalized_order_id, client_order_id.into());
        state
            .orders
            .insert(client_order_id.into(), ownership.clone());
        self.schedule_persist(&state);
        drop(state);
        // The local EIP-712 order id is known before POST. Make its instance
        // ownership/reservation crash-durable before the network can observe
        // the order; this closes the restart window that produced unowned
        // fills. The dedicated writer keeps serialization off the hot thread;
        // this wait is only for the final atomic fsync generation.
        if let Err(error) = self.flush_admission_persistence() {
            // Nothing has been sent yet. Roll back the in-memory reservation
            // so a persistence outage cannot leak one lock per quote retry.
            let mut state = self.state.lock().unwrap();
            if let Some(order) = state.orders.remove(client_order_id) {
                state
                    .oid_to_coid
                    .remove(&normalize_order_id(&order.order_id));
                if let Some(instance) = state.instances.get_mut(&order.instance_id) {
                    instance.reserved_cash =
                        (instance.reserved_cash - order.reserved_cash).max(0.0);
                    if order.reserved_quantity > 0.0 {
                        let reserved = instance
                            .reserved_positions
                            .entry(order.token_id)
                            .or_insert(0.0);
                        *reserved = (*reserved - order.reserved_quantity).max(0.0);
                    }
                }
            }
            self.schedule_persist(&state);
            drop(state);
            if let Err(rollback_error) = self.flush_rollback_persistence() {
                return Err(ReservationError::PersistenceUnavailable(format!(
                    "{error}; admission rollback also failed: {rollback_error}"
                )));
            }
            return Err(error);
        }
        Ok(ownership)
    }

    pub fn rebind_order_id(&self, client_order_id: &str, order_id: &str) -> bool {
        if client_order_id.is_empty() || order_id.is_empty() {
            return false;
        }
        let mut state = self.state.lock().unwrap();
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

    /// Explicit operator repair hook for an ownership anomaly after external
    /// audit. Runtime code should prefer a correct order/trade replay, which
    /// clears its own anomaly automatically.
    pub fn ownership_anomalies(&self) -> BTreeMap<String, String> {
        self.state.lock().unwrap().ownership_anomalies.clone()
    }

    pub fn mark_private_event_anomaly(&self, payload_key: &str, reason: impl Into<String>) {
        if payload_key.is_empty() {
            return;
        }
        let mut state = self.state.lock().unwrap();
        set_ownership_anomaly(
            &mut state,
            format!("private_event:{payload_key}"),
            reason.into(),
        );
        self.schedule_persist(&state);
    }

    pub fn resolve_private_event_anomaly(&self, payload_key: &str) {
        if payload_key.is_empty() {
            return;
        }
        let mut state = self.state.lock().unwrap();
        if state
            .ownership_anomalies
            .remove(&format!("private_event:{payload_key}"))
            .is_some()
        {
            recompute_reconciliation(&mut state, "corrected private event replay");
            self.schedule_persist(&state);
        }
    }

    pub fn mark_unresolved_trade_match_time(&self, trade_key: &str, match_time_secs: u64) {
        if trade_key.is_empty() || match_time_secs == 0 {
            return;
        }
        let mut state = self.state.lock().unwrap();
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
    }

    pub fn resolve_unresolved_trade_match_time(&self, trade_key: &str) {
        if trade_key.is_empty() {
            return;
        }
        let mut state = self.state.lock().unwrap();
        if state
            .unresolved_trade_match_times
            .remove(trade_key)
            .is_some()
        {
            self.schedule_persist(&state);
        }
    }

    pub fn earliest_unresolved_trade_match_time(&self) -> Option<u64> {
        self.state
            .lock()
            .unwrap()
            .unresolved_trade_match_times
            .values()
            .copied()
            .min()
    }

    pub fn repair_ownership_anomaly(&self, anomaly_key: &str) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.ownership_anomalies.remove(anomaly_key).is_none() {
            return false;
        }
        recompute_reconciliation(&mut state, "explicit ownership repair");
        self.schedule_persist(&state);
        true
    }

    pub fn order_owner_by_coid(&self, client_order_id: &str) -> Option<String> {
        self.state
            .lock()
            .unwrap()
            .orders
            .get(client_order_id)
            .map(|order| order.instance_id.clone())
    }

    pub fn order_owner_by_oid(&self, order_id: &str) -> Option<String> {
        let state = self.state.lock().unwrap();
        let coid = state.oid_to_coid.get(&normalize_order_id(order_id))?;
        state
            .orders
            .get(coid)
            .map(|order| order.instance_id.clone())
    }

    pub fn order(&self, client_order_id: &str) -> Option<OrderOwnership> {
        self.state
            .lock()
            .unwrap()
            .orders
            .get(client_order_id)
            .cloned()
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
        let mut state = self.state.lock().unwrap();
        if let Some(current_status) = state.orders.get(client_order_id).map(|order| order.status) {
            // REST placement acknowledgements can arrive after the private
            // feed has already advanced the order. FAILED/FILLED are sticky;
            // PartiallyFilled is also monotonic against the weaker Accepted
            // state because an observed match cannot be undone by a late ACK.
            if (matches!(current_status, OrderStatus::Failed | OrderStatus::Filled)
                && status != current_status)
                || (current_status == OrderStatus::PartiallyFilled
                    && status == OrderStatus::Accepted)
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
                let (instance_id, token_id, old_cash, old_qty, desired_cash, desired_qty) = {
                    let order = state
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
                        order.instance_id.clone(),
                        order.token_id.clone(),
                        old_cash,
                        old_qty,
                        desired_cash,
                        desired_qty,
                    )
                };
                if let Some(instance) = state.instances.get_mut(&instance_id) {
                    instance.reserved_cash =
                        (instance.reserved_cash + desired_cash - old_cash).max(0.0);
                    if (desired_qty - old_qty).abs() > EPS {
                        let reserved = instance.reserved_positions.entry(token_id).or_insert(0.0);
                        *reserved = (*reserved + desired_qty - old_qty).max(0.0);
                    }
                }
                state.recovery_pending_orders.remove(client_order_id);
                state.routine_cancel_audits.remove(client_order_id);
                state
                    .ownership_anomalies
                    .remove(&format!("order_cancel_audit:{client_order_id}"));
                recompute_reconciliation(&mut state, "cancelled order resurrected live");
            } else {
                state
                    .orders
                    .get_mut(client_order_id)
                    .expect("order status read above")
                    .status = status;
            }
            self.schedule_persist(&state);
            return Some(status);
        }
        None
    }

    /// A terminal exchange order status does not prove that every fill leg has
    /// reached the trade ledger. Preserve any unconsumed reservation and enter
    /// the sticky audit gate until those fills are booked. The edge result lets
    /// callers suppress duplicate WARNs from repeated Filled lifecycle rows.
    pub fn mark_filled_pending_audit(&self, client_order_id: &str) -> FillAuditPendingTransition {
        let mut state = self.state.lock().unwrap();
        let Some(order) = state.orders.get_mut(client_order_id) else {
            return FillAuditPendingTransition::NotTracked;
        };
        order.status = OrderStatus::Filled;
        let has_exact_terminal_audit = order.terminal_trade_ids_authoritative;
        let residual_pending = order.reserved_cash > EPS || order.reserved_quantity > EPS;
        let pending = if has_exact_terminal_audit {
            !terminal_order_audit_complete_locked(&state, client_order_id)
        } else {
            residual_pending
        };
        let already_pending = state.recovery_pending_orders.contains(client_order_id);
        state.routine_cancel_audits.remove(client_order_id);
        if pending {
            state
                .recovery_pending_orders
                .insert(client_order_id.to_string());
            recompute_reconciliation(&mut state, "terminal fill audit");
        }
        self.schedule_persist(&state);
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

        let mut state = self.state.lock().unwrap();
        let Some(existing) = state.orders.get(client_order_id).cloned() else {
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

        let already_pending = state.recovery_pending_orders.contains(client_order_id);
        let (instance_id, token_id, old_cash, old_qty, desired_cash, desired_qty) = {
            let order = state
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
                order.instance_id.clone(),
                order.token_id.clone(),
                old_cash,
                old_qty,
                desired_cash,
                desired_qty,
            )
        };
        if let Some(instance) = state.instances.get_mut(&instance_id) {
            instance.reserved_cash = (instance.reserved_cash + desired_cash - old_cash).max(0.0);
            if (desired_qty - old_qty).abs() > EPS {
                let reserved = instance.reserved_positions.entry(token_id).or_insert(0.0);
                *reserved = (*reserved + desired_qty - old_qty).max(0.0);
            }
        }
        state.routine_cancel_audits.remove(client_order_id);

        let complete = terminal_order_audit_complete_locked(&state, client_order_id);
        if complete {
            release_order_reservation_locked(&mut state, client_order_id);
            state.recovery_pending_orders.remove(client_order_id);
        } else {
            state
                .recovery_pending_orders
                .insert(client_order_id.to_string());
        }
        state
            .ownership_anomalies
            .remove(&format!("order_cancel_audit:{client_order_id}"));
        recompute_reconciliation(&mut state, "authoritative terminal order audit");
        self.schedule_persist(&state);

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
        Ok(transition)
    }

    pub fn terminal_order_audit_complete(&self, client_order_id: &str) -> bool {
        terminal_order_audit_complete_locked(&self.state.lock().unwrap(), client_order_id)
    }

    pub fn mark_cancelled_pending_trade_audit(
        &self,
        client_order_id: &str,
        size_matched: f64,
    ) -> bool {
        let mut state = self.state.lock().unwrap();
        let already_pending = state.recovery_pending_orders.contains(client_order_id);
        state.routine_cancel_audits.remove(client_order_id);
        let Some(existing) = state.orders.get(client_order_id) else {
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
            set_ownership_anomaly(
                &mut state,
                format!("order_cancel_audit:{client_order_id}"),
                format!("invalid cancellation audit coid={client_order_id} size_matched={size_matched} filled={filled} quantity={quantity}"),
            );
            self.schedule_persist(&state);
            return true;
        }
        let order = state
            .orders
            .get_mut(client_order_id)
            .expect("checked above");
        order.status = OrderStatus::Cancelled;
        order.terminal_matched_quantity = Some(size_matched.clamp(0.0, quantity));
        order.terminal_trade_ids.clear();
        order.terminal_trade_ids_authoritative = false;
        let instance_id = order.instance_id.clone();
        let token_id = order.token_id.clone();
        let old_cash = order.reserved_cash;
        let old_qty = order.reserved_quantity;
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
        state
            .ownership_anomalies
            .remove(&format!("order_cancel_audit:{client_order_id}"));
        let pending = desired_cash > EPS || desired_qty > EPS;
        if pending {
            state
                .recovery_pending_orders
                .insert(client_order_id.to_string());
        } else {
            state.recovery_pending_orders.remove(client_order_id);
        }
        recompute_reconciliation(&mut state, "terminal cancellation trade audit");
        self.schedule_persist(&state);
        if pending && !already_pending {
            self.notify_order_audit_worker();
        }
        pending
    }

    /// DELETE acknowledgements have no matched quantity; preserve the full
    /// residual lock until an order-specific audit arrives, without globally
    /// pausing unrelated instances on the same account.
    pub fn mark_cancelled_pending_audit(&self, client_order_id: &str) -> bool {
        let mut state = self.state.lock().unwrap();
        let Some(order) = state.orders.get_mut(client_order_id) else {
            return false;
        };
        order.status = OrderStatus::Cancelled;
        order.terminal_matched_quantity = None;
        order.terminal_trade_ids.clear();
        order.terminal_trade_ids_authoritative = false;
        state.recovery_pending_orders.remove(client_order_id);
        state.startup_query_repair_orders.remove(client_order_id);
        let newly_pending = state
            .routine_cancel_audits
            .insert(client_order_id.to_string());
        recompute_reconciliation(&mut state, "routine cancellation audit queued");
        self.schedule_persist(&state);
        if newly_pending {
            self.notify_order_audit_worker();
        }
        true
    }

    /// Release the still-unfilled reservation after an authoritative terminal
    /// order outcome. Ownership is retained for late fill attribution.
    pub fn release_order(&self, client_order_id: &str, status: OrderStatus) {
        let mut state = self.state.lock().unwrap();
        let Some(mut order) = state.orders.remove(client_order_id) else {
            return;
        };
        if let Some(instance) = state.instances.get_mut(&order.instance_id) {
            instance.reserved_cash = (instance.reserved_cash - order.reserved_cash).max(0.0);
            if order.reserved_quantity > 0.0 {
                let entry = instance
                    .reserved_positions
                    .entry(order.token_id.clone())
                    .or_insert(0.0);
                *entry = (*entry - order.reserved_quantity).max(0.0);
            }
        }
        order.reserved_cash = 0.0;
        order.reserved_quantity = 0.0;
        order.status = status;
        state.orders.insert(client_order_id.into(), order);
        if state.routine_cancel_audits.remove(client_order_id) {
            recompute_reconciliation(&mut state, "routine cancellation audit released");
        }
        self.schedule_persist(&state);
    }

    pub fn release_all_orders(&self) {
        let coids: Vec<String> = self.state.lock().unwrap().orders.keys().cloned().collect();
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
        let mut state = self.state.lock().unwrap();
        if !state.seeded {
            return Err(ReservationError::AccountNotSeeded);
        }
        if state.uncertain {
            return Err(ReservationError::AccountUncertain);
        }
        reject_allocation_audit_blockers(&state, allocations)?;
        let total: f64 = allocations.values().copied().sum();
        let total_reserved_cash: f64 = state.instances.values().map(|i| i.reserved_cash).sum();
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
            let available = (instance.cash - instance.reserved_cash).max(0.0);
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
        let mut state = self.state.lock().unwrap();
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
        let mut state = self.state.lock().unwrap();
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
        let mut state = self.state.lock().unwrap();
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
        let mut state = self.state.lock().unwrap();
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
        let mut state = self.state.lock().unwrap();
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
        let mut state = self.state.lock().unwrap();
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
        let mut state = self.state.lock().unwrap();
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
                    .map(|instance| instance.reserved_cash)
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
                    let available = (instance.cash - instance.reserved_cash).max(0.0);
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
            }
            MaintenanceOperationKind::Merge => {
                for token in [up_token_id, down_token_id] {
                    let total_reserved: f64 = state
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
                            - instance
                                .reserved_positions
                                .get(token)
                                .copied()
                                .unwrap_or(0.0))
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
                            .reserved_positions
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
        let mut state = self.state.lock().unwrap();
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
        let mut state = self.state.lock().unwrap();
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

    pub fn pending_maintenance_operations(&self) -> Vec<MaintenanceOperation> {
        self.state
            .lock()
            .unwrap()
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
        self.state
            .lock()
            .unwrap()
            .maintenance_ops
            .get(operation_id)
            .cloned()
    }

    pub fn fail_maintenance_operation(&self, operation_id: &str, detail: impl Into<String>) {
        let detail = detail.into();
        let mut state = self.state.lock().unwrap();
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
                        instance.reserved_cash = (instance.reserved_cash - *amount).max(0.0);
                    }
                    MaintenanceOperationKind::Merge => {
                        for token in [&existing.up_token_id, &existing.down_token_id] {
                            let reserved = instance
                                .reserved_positions
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
        let mut state = self.state.lock().unwrap();
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
                    if *amount > instance.cash.min(instance.reserved_cash) + EPS {
                        return Err(ReservationError::InsufficientVirtualCash {
                            required: *amount,
                            available: instance.cash.min(instance.reserved_cash),
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
                    instance.reserved_cash = (instance.reserved_cash - *amount).max(0.0);
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
                            .reserved_positions
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
                            .reserved_positions
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
            if let Err(error) = self.flush_persistence(Duration::from_secs(2)) {
                let generation = self
                    .persistence
                    .as_ref()
                    .map_or(0, AccountPersistence::scheduled_generation);
                self.trade_persistence_pending_generation
                    .fetch_max(generation, Ordering::AcqRel);
                self.set_risk_blocker(
                    TRADE_PERSISTENCE_RISK_BLOCKER,
                    format!(
                        "trade `{trade_key}` applied but generation {generation} is not durable: {error}"
                    ),
                );
                return if owned_noop {
                    TradeTransitionResult::OwnedNoopButPersistencePending(ownership)
                } else {
                    TradeTransitionResult::AppliedButPersistencePending(ownership)
                };
            }
            self.refresh_trade_persistence_blocker();
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
        let mut state = self.state.lock().unwrap();
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
            self.schedule_persist(&state);
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
            self.schedule_persist(&state);
            return TradeTransitionResult::Rejected;
        }
        if !state.settled_token_values.contains_key(token_id) {
            reject(
                &mut state,
                format!(
                    "authenticated historical trade `{trade_key}` token `{token_id}` has no durable settlement proof"
                ),
            );
            self.schedule_persist(&state);
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
            self.schedule_persist(&state);
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
        let was_uncertain = state.uncertain;
        state.ownership_anomalies.remove(&anomaly_key);
        state.unresolved_trade_match_times.remove(trade_key);
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
        self.schedule_persist(&state);
        drop(state);

        if let Err(error) = self.flush_persistence(Duration::from_secs(2)) {
            let generation = self
                .persistence
                .as_ref()
                .map_or(0, AccountPersistence::scheduled_generation);
            self.trade_persistence_pending_generation
                .fetch_max(generation, Ordering::AcqRel);
            self.set_risk_blocker(
                TRADE_PERSISTENCE_RISK_BLOCKER,
                format!(
                    "authenticated historical trade `{trade_key}` no-op generation {generation} is not durable: {error}"
                ),
            );
            return TradeTransitionResult::OwnedNoopButPersistencePending(ownership);
        }
        self.refresh_trade_persistence_blocker();
        TradeTransitionResult::OwnedNoop(ownership)
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
        let mut state = self.state.lock().unwrap();
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
            self.schedule_persist(&state);
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
                    self.schedule_persist(&state);
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
                    self.schedule_persist(&state);
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
                    self.schedule_persist(&state);
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
                    self.schedule_persist(&state);
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
            self.schedule_persist(&state);
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
            self.schedule_persist(&state);
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
            self.schedule_persist(&state);
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
                self.schedule_persist(&state);
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
                self.schedule_persist(&state);
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
            self.schedule_persist(&state);
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
                self.schedule_persist(&state);
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
        if should_book_physical || should_reverse_physical {
            let sign = if side == Side::Buy { 1.0 } else { -1.0 };
            let cash_delta = -sign * quantity * price;
            let position_delta = sign * quantity;
            let multiplier = if should_reverse_physical { -1.0 } else { 1.0 };
            state.physical_cash += cash_delta * multiplier;
            *state
                .physical_positions
                .entry(token_id.into())
                .or_insert(0.0) += position_delta * multiplier;
        }
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
        self.schedule_persist(&state);
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
        let mut state = self.state.lock().unwrap();
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
    /// trade. Virtual risk changes at MATCHED, physical state at
    /// MINED/CONFIRMED, and FAILED reverses whichever legs were booked.
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
        let mut state = self.state.lock().unwrap();
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
        let book_physical = !is_failed
            && existing.physical_booked
            && (!existing.physical_fee_booked || upgrades_zero_fee);
        let reverse_physical = is_failed && existing.physical_fee_booked;
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
        let physical_multiplier = multiplier(book_physical, reverse_physical);
        if physical_multiplier != 0.0 {
            state.physical_cash -= effective_usdc_fee * physical_multiplier;
            *state
                .physical_positions
                .entry(existing.ownership.token_id.clone())
                .or_insert(0.0) -= effective_shares_fee * physical_multiplier;
        }
        if let Some(trade) = state.trades.get_mut(trade_key) {
            trade.usdc_fee = effective_usdc_fee;
            trade.shares_fee = effective_shares_fee;
            trade.virtual_fee_booked =
                book_virtual || (existing.virtual_fee_booked && !reverse_virtual);
            trade.physical_fee_booked =
                book_physical || (existing.physical_fee_booked && !reverse_physical);
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
        self.state
            .lock()
            .unwrap()
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
        let state = self.state.lock().unwrap();
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

    pub fn restored_trades(&self) -> Vec<RestoredTrade> {
        self.state
            .lock()
            .unwrap()
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
        let mut state = self.state.lock().unwrap();
        let (pruned_orders, pruned_trades, pruned_fee_configs) =
            prune_terminal_history_locked(&mut state, instance_id, tokens);
        if pruned_orders > 0 || pruned_trades > 0 || pruned_fee_configs > 0 {
            self.schedule_persist(&state);
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

fn prune_terminal_history_locked(
    state: &mut SharedAccountState,
    instance_id: Option<&str>,
    tokens: &HashSet<String>,
) -> (usize, usize, usize) {
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
    prune_retired_trade_ownership_tombstones(state, retired_at_ms);
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
    let pruned_fee_configs = if instance_id.is_none() {
        tokens
            .iter()
            .filter(|token| !protected_fee_tokens.contains(*token))
            .filter(|token| state.token_fee_configs.remove(*token).is_some())
            .count()
    } else {
        0
    };
    (stale_orders.len(), stale_trades.len(), pruned_fee_configs)
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

fn prune_retired_trade_ownership_tombstones(state: &mut SharedAccountState, now_ms: u64) {
    state
        .retired_trade_ownership_tombstones
        .retain(|_, tombstone| retired_trade_tombstone_is_live(tombstone, now_ms));
    let excess = state
        .retired_trade_ownership_tombstones
        .len()
        .saturating_sub(MAX_RETIRED_TRADE_TOMBSTONES);
    if excess == 0 {
        return;
    }
    let mut oldest: Vec<(u64, String)> = state
        .retired_trade_ownership_tombstones
        .iter()
        .map(|(trade_key, tombstone)| (tombstone.retired_at_ms, trade_key.clone()))
        .collect();
    oldest.sort_unstable();
    for (_, trade_key) in oldest.into_iter().take(excess) {
        state.retired_trade_ownership_tombstones.remove(&trade_key);
    }
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
    let book_physical =
        !failed && existing.physical_booked && (!existing.physical_fee_booked || upgrades_zero);
    let reverse_physical = failed && existing.physical_fee_booked;
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
    let physical_multiplier = multiplier(book_physical, reverse_physical);
    if physical_multiplier != 0.0 {
        state.physical_cash -= effective_usdc * physical_multiplier;
        *state
            .physical_positions
            .entry(existing.ownership.token_id.clone())
            .or_insert(0.0) -= effective_shares * physical_multiplier;
    }
    if let Some(trade) = state.trades.get_mut(trade_key) {
        trade.usdc_fee = effective_usdc;
        trade.shares_fee = effective_shares;
        trade.virtual_fee_booked =
            book_virtual || (existing.virtual_fee_booked && !reverse_virtual);
        trade.physical_fee_booked =
            book_physical || (existing.physical_fee_booked && !reverse_physical);
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

fn trade_ownership_matches_order_root(
    ownership: &TradeOwnership,
    order: &OrderOwnership,
) -> bool {
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
        if !failed_trade_keys_by_order.contains_key(coid) {
            return Err(format!(
                "query-repair order `{coid}` is missing its durable FAILED-trade root"
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
                format!(
                    "query-repair order `{coid}` references missing instance `{instance_id}`"
                )
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
    } else if let Some(operation) = state
        .maintenance_ops
        .values()
        .find(|operation| operation.status == MaintenanceOperationStatus::Uncertain)
    {
        set_uncertain(
            state,
            format!(
                "maintenance operation `{}` finality uncertain: {}",
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
    if trade.physical_booked {
        effect.physical_cash += cash_delta;
        add_position_delta(
            &mut effect.physical_positions,
            &ownership.token_id,
            position_delta,
        );
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
    if trade.physical_fee_booked {
        effect.physical_cash -= trade.usdc_fee;
        add_position_delta(
            &mut effect.physical_positions,
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
        if instance.reserved_cash
            > instance.cash + reconciliation_tolerance(instance.cash, instance.reserved_cash)
            && !reservation_deficit_has_recovery_root(state, instance_id, None)
        {
            return Err(format!(
                "instance `{instance_id}` reserves more cash than it owns"
            ));
        }
        for (token, reserved) in &instance.reserved_positions {
            let owned = instance.positions.get(token).copied().unwrap_or(0.0);
            if *reserved > owned + reconciliation_tolerance(owned, *reserved)
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
    let mut expected_cash_by_instance = HashMap::<String, f64>::new();
    let mut expected_positions_by_instance = HashMap::<String, HashMap<String, f64>>::new();
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
        *expected_cash_by_instance
            .entry(order.instance_id.clone())
            .or_insert(0.0) += expected_cash;
        if expected_quantity > 0.0 {
            *expected_positions_by_instance
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
                    *expected_cash_by_instance
                        .entry(instance_id.clone())
                        .or_insert(0.0) += *amount;
                }
                MaintenanceOperationKind::Merge => {
                    let expected = expected_positions_by_instance
                        .entry(instance_id.clone())
                        .or_default();
                    for token in [&operation.up_token_id, &operation.down_token_id] {
                        *expected.entry(token.clone()).or_insert(0.0) += *amount;
                    }
                }
            }
        }
    }

    for (instance_id, instance) in &state.instances {
        let expected_cash = expected_cash_by_instance
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
        let expected_positions = expected_positions_by_instance.get(instance_id);
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
            || state.recovery_pending_orders.contains(coid)
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
fn proven_binary_token_value(
    state: &SharedAccountState,
    token_id: &str,
) -> Option<(String, f64)> {
    let mut proof = None;
    for interest in state
        .instances
        .values()
        .flat_map(|instance| instance.token_interests.values())
        .filter(|interest| {
            interest.up_token_id == token_id || interest.down_token_id == token_id
        })
    {
        let up = state
            .settled_token_values
            .get(&interest.up_token_id)
            .copied()?;
        let down = state
            .settled_token_values
            .get(&interest.down_token_id)
            .copied()?;
        if !((up == 1.0 && down == 0.0) || (up == 0.0 && down == 1.0)) {
            continue;
        }
        let value = if interest.up_token_id == token_id {
            up
        } else {
            down
        };
        let candidate = (interest.condition_id.clone(), value);
        if proof.as_ref().is_some_and(|existing| existing != &candidate) {
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
            proven_binary_token_value(state, token).map(|(condition_id, value)| {
                (condition_id, token.clone(), -*delta, value)
            })
        })
        .collect();
    if removed.is_empty() {
        return false;
    }
    let removed_total: f64 = removed.iter().map(|(_, _, qty, _)| *qty).sum();
    let expected_payout: f64 = removed
        .iter()
        .map(|(_, _, qty, value)| qty * value)
        .sum();
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
    fn reservation_checks_virtual_and_physical_limits_atomically() {
        let account = seeded_account();
        account
            .reserve_order("a", "a-1", "oid-a-1", "UP", Side::Buy, 100.0, 1.0, 0)
            .unwrap();
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
        assert_eq!(mined.physical_cash, 395.0);
        assert_eq!(mined.physical_positions["UP"], 50.0);
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
        assert_eq!(after_first_confirmation.physical_cash, 395.0);
        assert_eq!(after_first_confirmation.physical_positions["UP"], 50.0);
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
        assert_eq!(account.monitoring_snapshot().physical_cash, 395.0);
        assert_eq!(account.monitoring_snapshot().physical_positions["UP"], 50.0);
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
        assert_eq!(
            account.finalize_ready_settled_audit_retirements(),
            vec![HashSet::from(["UP".to_string()])],
        );
        let state = account.state.lock().unwrap();
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
        assert!((mined.physical_positions["UP"] - 49.8).abs() < EPS);
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
        assert!((account.monitoring_snapshot().physical_positions["UP"] - 49.9).abs() < EPS);
    }

    #[test]
    fn token_fee_curve_revision_reprices_attributed_virtual_and_physical_trade() {
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
        assert!((account.monitoring_snapshot().physical_positions["UP"] - 49.9).abs() < EPS);

        account
            .register_token_fee_config(&["UP".to_string()], 0.04, 1.0)
            .unwrap();

        let instance = account.instance_snapshot("a").unwrap();
        assert!((instance.positions["UP"] - 19.8).abs() < EPS);
        assert!(instance.ledger_generation > before_generation);
        assert!((account.monitoring_snapshot().physical_positions["UP"] - 49.8).abs() < EPS);
        let restored = account
            .restored_trades()
            .into_iter()
            .find(|trade| trade.ownership.trade_key == "trade-reprice")
            .unwrap();
        assert!((restored.shares_fee - 0.2).abs() < EPS);
        assert!(restored.virtual_fee_booked);
        assert!(!account.is_uncertain());
        assert!(validate_persisted_state("acct", &account.state.lock().unwrap()).is_ok());
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
        assert!(validate_persisted_state("acct", &account.state.lock().unwrap()).is_ok());

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
            let mut state = account.state.lock().unwrap();
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
            let mut state = account.state.lock().unwrap();
            state.instances.get_mut("a").unwrap().cash += 0.5e-6;
            recompute_reconciliation(&mut state, "rounding test");
        }
        assert!(!account.is_uncertain());
        assert!((account.monitoring_snapshot().unallocated_cash + 0.5e-6).abs() < 1e-12);

        {
            let mut state = account.state.lock().unwrap();
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
                let mut state = account.state.lock().unwrap();
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
            let mut state = account.state.lock().unwrap();
            state.physical_positions.insert("UP".to_string(), 9.5);
            recompute_reconciliation(&mut state, "wallet position residual fixture");
        }

        assert!(!account.is_uncertain());
        assert_eq!(
            account.monitoring_snapshot().unallocated_positions.get("UP"),
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

        account.apply_physical_snapshot(189.0, HashMap::new()).unwrap();
        assert!(!account.is_uncertain());
        assert!((account.monitoring_snapshot().unallocated_cash - 89.0).abs() <= EPS);
        account.record_settled_token_values(&HashMap::from([
            ("ETH-WIN".into(), 1.0),
            ("ETH-LOSE".into(), 0.0),
        ]));

        let instance = account.instance_snapshot("eth03").unwrap();
        assert!((instance.cash - 180.0).abs() <= EPS);
        assert!(instance.positions.values().all(|quantity| quantity.abs() <= EPS));
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
        account.apply_physical_snapshot(189.0, HashMap::new()).unwrap();

        account.record_settled_token_values(&HashMap::from([("ETH-WIN".into(), 1.0)]));

        assert_eq!(account.instance_snapshot("eth03").unwrap().cash, 100.0);
        assert_eq!(account.instance_snapshot("eth03").unwrap().positions["ETH-WIN"], 80.0);
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
        assert!(metric.physical_positions.values().all(|qty| qty.abs() <= EPS));
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
            let mut state = account.state.lock().unwrap();
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
            let mut state = account.state.lock().unwrap();
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
                state,
            },
        )
        .unwrap();

        let strict_error = SharedAccount::new_persistent(account_id, &path).unwrap_err();
        assert!(
            strict_error.contains("reservation disagrees with effective remaining quantity"),
            "{strict_error}",
        );

        let restored =
            SharedAccount::new_persistent_for_query_repair(account_id, &path).unwrap();
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
        restored
            .flush_persistence(Duration::from_secs(2))
            .unwrap();
        drop(restored);

        // A crash after the conservative reservation was fsynced but before
        // the CLOB query completed must remain a fail-closed query repair on
        // the next process start, without double-counting the reservation.
        let unfinished_error = SharedAccount::new_persistent(account_id, &path).unwrap_err();
        assert!(
            unfinished_error.contains("unfinished authoritative startup query repair"),
            "{unfinished_error}",
        );
        let reopened =
            SharedAccount::new_persistent_for_query_repair(account_id, &path).unwrap();
        assert_eq!(
            reopened.startup_query_repair_pending_order_ids(),
            vec![coid.to_string()],
        );
        assert_eq!(
            reopened.instance_snapshot(instance_id).unwrap().reserved_positions[token],
            40.0,
        );
        assert!(reopened.mark_cancelled_pending_audit(coid));
        assert!(reopened
            .startup_query_repair_pending_order_ids()
            .is_empty());
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
                state,
            },
        )
        .unwrap();

        let strict_error = SharedAccount::new_persistent(account_id, &path).unwrap_err();
        assert!(
            strict_error.contains("reservation disagrees with effective remaining quantity"),
            "{strict_error}",
        );

        let restored =
            SharedAccount::new_persistent_for_query_repair(account_id, &path).unwrap();
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
            restored.instance_snapshot(instance_id).unwrap().reserved_positions[token],
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
                state,
            },
        )
        .unwrap();

        let error =
            SharedAccount::new_persistent_for_query_repair(account_id, &path).unwrap_err();
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
                state,
            },
        )
        .unwrap();

        let error =
            SharedAccount::new_persistent_for_query_repair("unowned", &path).unwrap_err();
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
        let state = account.state.lock().unwrap().clone();
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

        let mut state = account.state.lock().unwrap().clone();
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

        let state = account.state.lock().unwrap().clone();
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
    fn applied_trade_survives_persistence_failure_and_blocks_admission() {
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
        assert!(matches!(
            result,
            TradeTransitionResult::AppliedButPersistencePending(_)
        ));
        let snapshot = account.instance_snapshot("a").unwrap();
        assert_eq!(snapshot.positions.get("UP").copied(), Some(10.0));
        assert_eq!(snapshot.cash, 95.0);
        assert!(account.is_uncertain());
        assert!(matches!(
            account.reserve_order("a", "blocked", "oid-blocked", "UP", Side::Buy, 1.0, 0.5, 0,),
            Err(ReservationError::PersistenceUnavailable(_))
                | Err(ReservationError::AccountUncertain)
        ));

        drop(account);
        let _ = std::fs::remove_file(&root);
        let _ = std::fs::remove_dir_all(&moved);
    }
}
