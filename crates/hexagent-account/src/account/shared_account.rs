//! Account-scoped physical/virtual bookkeeping for shared-wallet strategies.
//!
//! One [`SharedAccount`] is owned by one exchange account. It is the
//! admission-control source of truth shared by every strategy instance on the
//! wallet: physical funds/positions are the hard ceiling, while each
//! instance's weighted virtual balance/inventory is its private ceiling.

use hexagent_types::types::{OrderStatus, Side};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const EPS: f64 = 1e-9;
const RECONCILIATION_UNIT: f64 = 1e-6;
const INITIAL_TOKEN_BARRIER_TIMEOUT_MS: u64 = 10_000;

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
    PersistenceUnavailable(String),
    UnknownInstance(String),
    DuplicateClientOrderId(String),
    InvalidOrder(String),
    InsufficientVirtualCash { required: f64, available: f64 },
    InsufficientPhysicalCash { required: f64, available: f64 },
    InsufficientVirtualPosition { token: String, required: f64, available: f64 },
    InsufficientPhysicalPosition { token: String, required: f64, available: f64 },
}

impl std::fmt::Display for ReservationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AccountNotSeeded => write!(f, "shared account has no physical snapshot"),
            Self::AccountUncertain => write!(f, "shared account is awaiting physical reconciliation"),
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

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SharedAccountState {
    seeded: bool,
    physical_cash: f64,
    physical_positions: HashMap<String, f64>,
    unallocated_cash: f64,
    unallocated_positions: HashMap<String, f64>,
    instances: BTreeMap<String, InstanceLedger>,
    orders: HashMap<String, OrderOwnership>,
    oid_to_coid: HashMap<String, String>,
    trades: HashMap<String, AppliedTrade>,
    /// Advances only when the virtual trade/fee ledger changes.
    #[serde(default)]
    ledger_generation: u64,
    uncertain: bool,
    #[serde(default)]
    uncertain_reason: Option<String>,
    #[serde(default)]
    uncertain_since_ms: Option<u64>,
    #[serde(default)]
    external_adjustments: HashMap<String, ExternalAdjustment>,
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
    /// Persisted per-token fee curves make cold trade replay independent of a
    /// live strategy EventContext.
    #[serde(default)]
    token_fee_configs: HashMap<String, TokenFeeConfig>,
    /// Taker trades seen before their fee curve is available remain a sticky
    /// admission blocker rather than silently booking zero fee.
    #[serde(default)]
    fee_attribution_pending: HashSet<String>,
    /// Orders whose exchange terminal state/fill audit has not yet been proved,
    /// including both restored orders and runtime Filled-before-trade races.
    /// These are a distinct sticky risk-off reason: an otherwise matching
    /// wallet snapshot must not clear them.
    #[serde(default)]
    recovery_pending_orders: HashSet<String>,
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
            .map_err(|error| format!("open account ledger lock {}: {error}", lock_path.display()))?;
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
                path.file_stem().and_then(|s| s.to_str()).unwrap_or("writer")
            ))
            .spawn(move || {
                while rx.recv().is_ok() {
                    loop {
                        let Some(job) = thread_pending.lock().unwrap().take() else { break; };
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
        *self.pending.lock().unwrap() = Some(PersistJob { generation, snapshot });
        let _ = self.wake.try_send(());
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
        if target == 0 { return Ok(()); }
        let _ = self.wake.try_send(());
        let (lock, cv) = &*self.progress;
        let progress = lock.lock()
            .map_err(|_| "account ledger writer progress lock poisoned".to_string())?;
        let progress = cv.wait_while(progress, |state| state.completed_generation < target)
            .map_err(|_| "account ledger writer progress lock poisoned".to_string())?;
        if let Some(error) = &progress.last_error { return Err(error.clone()); }
        Ok(())
    }

    fn last_error(&self) -> Option<String> {
        self.progress.0.lock().ok().and_then(|p| p.last_error.clone())
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
}

impl SharedAccount {
    pub fn new(account_id: impl Into<String>) -> Self {
        Self {
            account_id: account_id.into(),
            state: Mutex::new(SharedAccountState::default()),
            persistence: None,
        }
    }

    /// Open (or create) a durable account ledger. A corrupt, unsupported, or
    /// account-mismatched file fails startup rather than silently discarding
    /// ownership and reservations.
    pub fn new_persistent(
        account_id: impl Into<String>,
        path: impl Into<PathBuf>,
    ) -> Result<Self, String> {
        let account_id = account_id.into();
        let path = path.into();
        let (state, migrated_terminal_failures) = if path.exists() {
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
            // `oid_to_coid` used to preserve the API's exact casing/prefix.
            // Rebuild it from durable orders so old ledgers are migrated to
            // canonical keys and cannot lose attribution after restart.
            let mut normalized = HashMap::with_capacity(state.orders.len());
            for order in state.orders.values() {
                let oid = normalize_order_id(&order.order_id);
                if oid.is_empty() {
                    return Err(format!(
                        "account ledger {} has empty order id for coid `{}`",
                        path.display(), order.client_order_id,
                    ));
                }
                if let Some(other) = normalized.insert(oid.clone(), order.client_order_id.clone()) {
                    if other != order.client_order_id {
                        return Err(format!(
                            "account ledger {} maps normalized order id `{}` to both `{}` and `{}`",
                            path.display(), oid, other, order.client_order_id,
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
            let migrated = normalize_terminal_failed_state(&mut state);
            if migrated {
                recompute_reconciliation(&mut state, "terminal FAILED ledger migration");
            }
            (state, migrated)
        } else {
            (SharedAccountState::default(), false)
        };
        let persistence = AccountPersistence::start(path)?;
        let account = Self {
            account_id,
            state: Mutex::new(state),
            persistence: Some(persistence),
        };
        if migrated_terminal_failures {
            let state = account.state.lock().unwrap();
            account.schedule_persist(&state);
        }
        Ok(account)
    }

    pub fn account_id(&self) -> &str { &self.account_id }

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
        self.persistence.as_ref().map_or(Ok(()), |p| p.flush(timeout))
    }

    fn flush_rollback_persistence(&self) -> Result<(), String> {
        self.persistence.as_ref().map_or(Ok(()), AccountPersistence::flush_blocking)
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

    /// Register an instance before the first physical snapshot. Non-positive
    /// and non-finite weights are normalized to the default equal weight 1.0.
    /// Once an account has been seeded, a new member or changed weight never
    /// silently reallocates PnL: admission becomes fail-closed until an explicit
    /// [`Self::migrate_cash_allocation`] operation is durably recorded.
    pub fn register_instance(&self, instance_id: &str, weight: f64) {
        if instance_id.is_empty() { return; }
        let weight = if weight.is_finite() && weight > 0.0 { weight } else { 1.0 };
        let mut state = self.state.lock().unwrap();
        let previous = state.instances.get(instance_id).map(|instance| instance.weight);
        state.instances
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
        if instance_id.is_empty() || scope_key.is_empty() { return; }
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
        if instance.market_scopes.insert(scope_key) { self.schedule_persist(&state); }
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
            instance_id, condition_id, up_token_id, down_token_id, "",
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
        if instance_id.is_empty() || condition_id.is_empty()
            || up_token_id.is_empty() || down_token_id.is_empty()
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
            instance.market_scopes.iter().next().cloned().unwrap_or_default()
        } else {
            requested_scope
        };
        instance.token_interests.insert(condition_id.to_string(), TokenInterest {
            instance_id: instance_id.to_string(),
            condition_id: condition_id.to_string(),
            up_token_id: up_token_id.to_string(),
            down_token_id: down_token_id.to_string(),
            scope_key,
            retire_after_ms: None,
        });
        // Never redistribute an already-seeded ledger here. Live startup
        // registers every configured instance before the first fetch; a scope
        // added later must not rewrite cash, PnL, or trade-owned inventory.
        self.schedule_persist(&state);
        Ok(())
    }

    pub fn token_interests(&self) -> Vec<TokenInterest> {
        let mut state = self.state.lock().unwrap();
        let now_ms = wall_clock_ms();
        // Keep a settled winner in the explicit ERC-1155 query scope until
        // both physical and virtual quantities have been observed at zero.
        // Otherwise a platform redemption landing after the normal event grace
        // can be missed because the Data API omits zero-balance rows.
        let settled_winners_requiring_zero: HashSet<String> = state
            .settled_token_values
            .iter()
            .filter(|(_, value)| **value == 1.0)
            .filter_map(|(token, _)| {
                let physical = state.physical_positions.get(token).copied().unwrap_or(0.0);
                let virtual_qty: f64 = state
                    .instances
                    .values()
                    .map(|instance| instance.positions.get(token).copied().unwrap_or(0.0))
                    .sum();
                (physical > EPS || virtual_qty > EPS).then(|| token.clone())
            })
            .collect();
        let mut pruned = false;
        for instance in state.instances.values_mut() {
            let before = instance.token_interests.len();
            instance.token_interests.retain(|_, interest| {
                interest.retire_after_ms.is_none_or(|deadline| deadline > now_ms)
                    || settled_winners_requiring_zero.contains(&interest.up_token_id)
                    || settled_winners_requiring_zero.contains(&interest.down_token_id)
            });
            pruned |= instance.token_interests.len() != before;
        }
        let interests = state.instances.values()
            .flat_map(|instance| instance.token_interests.values().cloned())
            .collect();
        if pruned { self.schedule_persist(&state); }
        interests
    }

    /// Retire a finished/abandoned event after a ten-minute reconciliation
    /// grace. Existing virtual positions retain their direct instance ownership;
    /// only the active on-chain fetch scope eventually expires.
    pub fn retire_token_interest(&self, instance_id: &str, condition_id: &str) {
        let mut state = self.state.lock().unwrap();
        if let Some(instance) = state.instances.get_mut(instance_id) {
            if let Some(interest) = instance.token_interests.get_mut(condition_id) {
                interest.retire_after_ms = Some(wall_clock_ms().saturating_add(10 * 60 * 1000));
            }
        }
        self.schedule_persist(&state);
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
        for (token, value) in values {
            if !token.is_empty() && value.is_finite() && (*value == 0.0 || *value == 1.0) {
                state.settled_token_values.insert(token.clone(), *value);
            }
        }
        self.schedule_persist(&state);
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
            || !rate.is_finite()
            || rate < 0.0
            || !exponent.is_finite()
            || exponent < 0.0
        {
            return Err(ReservationError::InvalidOrder(
                "token fee config requires tokens and finite nonnegative rate/exponent".into(),
            ));
        }
        let token_set: HashSet<&str> = token_ids.iter().map(String::as_str).collect();
        let mut state = self.state.lock().unwrap();
        for token in token_ids {
            state.token_fee_configs.insert(
                token.clone(),
                TokenFeeConfig { rate, exponent },
            );
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
        self.schedule_persist(&state);
        drop(state);
        for (trade_key, status, is_maker) in retry {
            let _ = self.apply_configured_trade_fee(&trade_key, status, is_maker);
        }
        Ok(())
    }

    pub fn active_tokens(&self) -> HashSet<String> {
        self.token_interests().into_iter()
            .flat_map(|interest| [interest.up_token_id, interest.down_token_id])
            .collect()
    }

    /// Apply the account snapshot used to establish this process's startup
    /// baseline. Later calls in the same process are ignored.
    pub fn apply_physical_snapshot(&self, cash: f64, positions: HashMap<String, f64>) {
        let mut authoritative_tokens: HashSet<String> = positions.keys().cloned().collect();
        let state = self.state.lock().unwrap();
        authoritative_tokens.extend(state.physical_positions.keys().cloned());
        authoritative_tokens.extend(
            state.instances.values().flat_map(|instance| instance.positions.keys().cloned()),
        );
        drop(state);
        self.apply_scoped_physical_snapshot(cash, positions, authoritative_tokens);
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
    ) {
        self.apply_scoped_physical_snapshot_inner(
            None,
            cash,
            positions,
            authoritative_tokens,
        );
    }

    /// Apply one account-level startup generation at most once.
    pub fn apply_scoped_physical_snapshot_versioned(
        &self,
        generation: u64,
        cash: f64,
        positions: HashMap<String, f64>,
        authoritative_tokens: HashSet<String>,
    ) -> bool {
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
    ) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.startup_snapshot_applied_this_process {
            return false;
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
                    return false;
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
                return false;
            }
        }
        let cash = finite_nonnegative(cash);
        let positions = positions.into_iter()
            .filter_map(|(token, qty)| {
                let qty = finite_nonnegative(qty);
                (qty > EPS).then_some((token, qty))
            })
            .collect::<HashMap<_, _>>();
        if !state.seeded {
            state.seeded = true;
            state.startup_snapshot_applied_this_process = true;
            if let Some(generation) = generation {
                state.last_physical_snapshot_generation = generation;
            }
            state.physical_cash = cash;
            state.physical_positions = positions.iter()
                .filter(|(token, _)| authoritative_tokens.contains(*token))
                .map(|(token, qty)| (token.clone(), *qty))
                .collect();
            redistribute_all(&mut state);
            self.schedule_persist(&state);
            return true;
        }

        // A wallet snapshot has no trade ids. Applying it while a MATCHED trade
        // is still waiting for MINED/CONFIRMED creates an unavoidable race: the
        // snapshot may already contain that settlement, and the later lifecycle
        // edge would then apply the same physical delta a second time. Do not
        // guess individual trade finality from aggregate wallet equality. Keep
        // the trade-driven physical ledger unchanged and let the next snapshot
        // retry after every pending lifecycle has resolved.
        if has_unsettled_trade_lifecycle(&state) || has_unsettled_maintenance_operation(&state) {
            return false;
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
        true
    }

    pub fn is_seeded(&self) -> bool { self.state.lock().unwrap().seeded }
    pub fn startup_snapshot_applied(&self) -> bool {
        self.state.lock().unwrap().startup_snapshot_applied_this_process
    }

    /// Attribute only a proven 1:1 platform redemption. This deliberately
    /// does not turn a runtime wallet observation into a position snapshot.
    pub fn observe_platform_binary_redeem(
        &self,
        observed_cash: f64,
        observed_positions: &HashMap<String, f64>,
        authoritative_tokens: &HashSet<String>,
    ) -> bool {
        if !observed_cash.is_finite() || observed_cash < 0.0
            || observed_positions.values().any(|qty| !qty.is_finite() || *qty < 0.0)
        { return false; }
        let mut state = self.state.lock().unwrap();
        if !state.seeded || has_unsettled_trade_lifecycle(&state)
            || has_unsettled_maintenance_operation(&state)
        { return false; }
        let cash_delta = observed_cash - state.physical_cash;
        if cash_delta <= EPS { return false; }

        let mut removed = Vec::new();
        for token in authoritative_tokens {
            let prior = state.physical_positions.get(token).copied().unwrap_or(0.0);
            let observed = observed_positions.get(token).copied().unwrap_or(0.0);
            let delta = observed - prior;
            if delta.abs() <= reconciliation_tolerance(prior, observed) { continue; }
            if delta < 0.0 && state.settled_token_values.get(token)
                .is_some_and(|value| (*value - 1.0).abs() <= EPS)
            {
                removed.push((token.clone(), -delta));
            } else {
                return false;
            }
        }
        if removed.is_empty() { return false; }
        let removed_total: f64 = removed.iter().map(|(_, qty)| qty).sum();
        let tolerance = 0.02_f64.max(removed_total.abs().max(cash_delta.abs()) * 0.001);
        if (removed_total - cash_delta).abs() > tolerance { return false; }
        for (token, qty) in &removed {
            let virtual_total: f64 = state.instances.values()
                .map(|instance| instance.positions.get(token).copied().unwrap_or(0.0)).sum();
            if virtual_total + tolerance < *qty { return false; }
        }

        state.physical_cash += cash_delta;
        for (token, qty) in &removed {
            let physical = state.physical_positions.entry(token.clone()).or_insert(0.0);
            *physical = (*physical - *qty).max(0.0);
            let virtual_total: f64 = state.instances.values()
                .map(|instance| instance.positions.get(token).copied().unwrap_or(0.0)).sum();
            if virtual_total <= EPS { continue; }
            for instance in state.instances.values_mut() {
                let owned = instance.positions.get(token).copied().unwrap_or(0.0);
                if owned <= EPS { continue; }
                let burned = (owned * *qty / virtual_total).min(owned);
                *instance.positions.entry(token.clone()).or_insert(0.0) -= burned;
                instance.cash += cash_delta * burned / removed_total;
            }
        }
        recompute_reconciliation(&mut state, "platform automatic binary redeem");
        self.schedule_persist(&state);
        log::info!(
            "[shared_account] attributed platform automatic redeem account={} cash={:.6} removed={:?}",
            self.account_id, cash_delta, removed,
        );
        true
    }
    pub fn is_uncertain(&self) -> bool {
        self.persistence.as_ref().and_then(AccountPersistence::last_error).is_some()
            || self.state.lock().unwrap().uncertain
    }
    pub fn mark_uncertain(&self) { self.mark_uncertain_with_reason("unspecified account uncertainty"); }

    pub fn mark_uncertain_with_reason(&self, reason: impl Into<String>) {
        let mut state = self.state.lock().unwrap();
        set_uncertain(&mut state, reason.into());
        self.schedule_persist(&state);
    }

    /// Mark potentially-live orders restored from disk. Admission remains
    /// fail-closed until each order is cancelled or its complete fill history
    /// has been observed.
    pub fn begin_order_recovery<'a>(&self, client_order_ids: impl IntoIterator<Item = &'a str>) {
        let mut state = self.state.lock().unwrap();
        state.recovery_pending_orders.extend(
            client_order_ids
                .into_iter()
                .filter(|id| !id.is_empty())
                .map(str::to_string),
        );
        recompute_reconciliation(&mut state, "startup order recovery");
        self.schedule_persist(&state);
    }

    pub fn finish_order_recovery(&self, client_order_id: &str) {
        let mut state = self.state.lock().unwrap();
        if state.recovery_pending_orders.remove(client_order_id) {
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

    /// Attribute an externally-confirmed wallet operation atomically to both
    /// the physical account ledger and one instance's virtual ledger.
    pub fn attribute_external_adjustment(
        &self,
        operation_id: &str,
        instance_id: &str,
        cash_delta: f64,
        position_deltas: HashMap<String, f64>,
    ) -> Result<ExternalAdjustment, ReservationError> {
        if operation_id.is_empty() || !cash_delta.is_finite()
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
        state.external_adjustments.insert(operation_id.to_string(), adjustment.clone());
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
            reserved_cash: state.instances.values().map(|instance| instance.reserved_cash).sum(),
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
        self.state.lock().unwrap().orders.values().cloned().collect()
    }

    pub fn instance_snapshot(&self, instance_id: &str) -> Option<InstanceAccountSnapshot> {
        let state = self.state.lock().unwrap();
        state.instances.get(instance_id).map(|instance| InstanceAccountSnapshot {
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
        if state.uncertain || persistence_failed {
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
        let total_reserved_position: f64 = state.instances.values()
            .map(|i| i.reserved_positions.get(token).copied().unwrap_or(0.0))
            .sum();
        let virtual_cash = (instance.cash - instance.reserved_cash).max(0.0);
        let physical_cash = (state.physical_cash - total_reserved_cash).max(0.0);
        let virtual_position = (instance.positions.get(token).copied().unwrap_or(0.0)
            - instance.reserved_positions.get(token).copied().unwrap_or(0.0)).max(0.0);
        let physical_position = (state.physical_positions.get(token).copied().unwrap_or(0.0)
            - total_reserved_position).max(0.0);
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
        if client_order_id.is_empty() || order_id.is_empty() || token_id.is_empty()
            || !quantity.is_finite() || quantity <= 0.0
            || !price.is_finite() || price <= 0.0
        {
            return Err(ReservationError::InvalidOrder(
                "coid/oid/token must be present and quantity/price must be positive".into(),
            ));
        }
        // A previous asynchronous mutation may have failed to persist. Retry
        // the complete current snapshot before changing any reservation.
        self.ensure_admission_persistence()?;
        let mut state = self.state.lock().unwrap();
        if !state.seeded { return Err(ReservationError::AccountNotSeeded); }
        if state.uncertain { return Err(ReservationError::AccountUncertain); }
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
            return Err(ReservationError::DuplicateClientOrderId(client_order_id.into()));
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
        } else { 0.0 };
        let reserve_qty = if side == Side::Sell { quantity } else { 0.0 };

        let total_reserved_cash: f64 = state.instances.values().map(|i| i.reserved_cash).sum();
        let total_reserved_qty: f64 = state.instances.values()
            .map(|i| i.reserved_positions.get(token_id).copied().unwrap_or(0.0))
            .sum();
        let instance = state.instances.get(instance_id).expect("checked above");
        let virtual_cash = (instance.cash - instance.reserved_cash).max(0.0);
        let physical_cash = (state.physical_cash - total_reserved_cash).max(0.0);
        let virtual_qty = (instance.positions.get(token_id).copied().unwrap_or(0.0)
            - instance.reserved_positions.get(token_id).copied().unwrap_or(0.0)).max(0.0);
        let physical_qty = (state.physical_positions.get(token_id).copied().unwrap_or(0.0)
            - total_reserved_qty).max(0.0);
        if reserve_cash > virtual_cash + EPS {
            return Err(ReservationError::InsufficientVirtualCash {
                required: reserve_cash, available: virtual_cash,
            });
        }
        if reserve_cash > physical_cash + EPS {
            return Err(ReservationError::InsufficientPhysicalCash {
                required: reserve_cash, available: physical_cash,
            });
        }
        if reserve_qty > virtual_qty + EPS {
            return Err(ReservationError::InsufficientVirtualPosition {
                token: token_id.into(), required: reserve_qty, available: virtual_qty,
            });
        }
        if reserve_qty > physical_qty + EPS {
            return Err(ReservationError::InsufficientPhysicalPosition {
                token: token_id.into(), required: reserve_qty, available: physical_qty,
            });
        }

        let instance = state.instances.get_mut(instance_id).expect("checked above");
        instance.reserved_cash += reserve_cash;
        if reserve_qty > 0.0 {
            *instance.reserved_positions.entry(token_id.into()).or_insert(0.0) += reserve_qty;
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
            price,
            fee_rate_bps,
            reserved_cash: reserve_cash,
            reserved_quantity: reserve_qty,
            status: OrderStatus::Pending,
        };
        state
            .oid_to_coid
            .insert(normalized_order_id, client_order_id.into());
        state.orders.insert(client_order_id.into(), ownership.clone());
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
                state.oid_to_coid.remove(&normalize_order_id(&order.order_id));
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
        if client_order_id.is_empty() || order_id.is_empty() { return false; }
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
        if payload_key.is_empty() { return; }
        let mut state = self.state.lock().unwrap();
        set_ownership_anomaly(
            &mut state,
            format!("private_event:{payload_key}"),
            reason.into(),
        );
        self.schedule_persist(&state);
    }

    pub fn resolve_private_event_anomaly(&self, payload_key: &str) {
        if payload_key.is_empty() { return; }
        let mut state = self.state.lock().unwrap();
        if state.ownership_anomalies
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
        if state.unresolved_trade_match_times.remove(trade_key).is_some() {
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
        self.state.lock().unwrap().orders.get(client_order_id)
            .map(|order| order.instance_id.clone())
    }

    pub fn order_owner_by_oid(&self, order_id: &str) -> Option<String> {
        let state = self.state.lock().unwrap();
        let coid = state.oid_to_coid.get(&normalize_order_id(order_id))?;
        state.orders.get(coid).map(|order| order.instance_id.clone())
    }

    pub fn order(&self, client_order_id: &str) -> Option<OrderOwnership> {
        self.state.lock().unwrap().orders.get(client_order_id).cloned()
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
        if let Some(order) = state.orders.get_mut(client_order_id) {
            // REST placement acknowledgements can arrive after the private
            // feed has already advanced the order. FAILED/FILLED are sticky;
            // PartiallyFilled is also monotonic against the weaker Accepted
            // state because an observed match cannot be undone by a late ACK.
            if (matches!(order.status, OrderStatus::Failed | OrderStatus::Filled)
                && status != order.status)
                || (order.status == OrderStatus::PartiallyFilled
                    && status == OrderStatus::Accepted)
            {
                return Some(order.status);
            }
            order.status = status;
            self.schedule_persist(&state);
            return Some(status);
        }
        None
    }

    /// A terminal exchange order status does not prove that every fill leg has
    /// reached the trade ledger. Preserve any unconsumed reservation and enter
    /// the sticky audit gate until those fills are booked. Returns true while
    /// an audit is still required.
    pub fn mark_filled_pending_audit(&self, client_order_id: &str) -> bool {
        let mut state = self.state.lock().unwrap();
        let Some(order) = state.orders.get_mut(client_order_id) else {
            return false;
        };
        order.status = OrderStatus::Filled;
        let pending = order.reserved_cash > EPS || order.reserved_quantity > EPS;
        if pending {
            state
                .recovery_pending_orders
                .insert(client_order_id.to_string());
            recompute_reconciliation(&mut state, "terminal fill audit");
        }
        self.schedule_persist(&state);
        pending
    }

    pub fn mark_cancelled_pending_trade_audit(
        &self,
        client_order_id: &str,
        size_matched: f64,
    ) -> bool {
        let mut state = self.state.lock().unwrap();
        let Some(existing) = state.orders.get(client_order_id) else { return false; };
        let quantity = existing.quantity;
        let filled = existing.filled_quantity;
        let tolerance = 1e-8_f64.max(quantity.abs() * 1e-8);
        if !size_matched.is_finite() || size_matched < -tolerance
            || size_matched > quantity + tolerance || size_matched + tolerance < filled
        {
            set_ownership_anomaly(
                &mut state,
                format!("order_cancel_audit:{client_order_id}"),
                format!("invalid cancellation audit coid={client_order_id} size_matched={size_matched} filled={filled} quantity={quantity}"),
            );
            self.schedule_persist(&state);
            return true;
        }
        let order = state.orders.get_mut(client_order_id).expect("checked above");
        order.status = OrderStatus::Cancelled;
        order.terminal_matched_quantity = Some(size_matched.clamp(0.0, quantity));
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
        state.ownership_anomalies.remove(&format!("order_cancel_audit:{client_order_id}"));
        let pending = desired_cash > EPS || desired_qty > EPS;
        if pending { state.recovery_pending_orders.insert(client_order_id.to_string()); }
        else { state.recovery_pending_orders.remove(client_order_id); }
        recompute_reconciliation(&mut state, "terminal cancellation trade audit");
        self.schedule_persist(&state);
        pending
    }

    /// DELETE acknowledgements have no matched quantity; preserve the full
    /// residual lock until an order-specific audit arrives.
    pub fn mark_cancelled_pending_audit(&self, client_order_id: &str) -> bool {
        let mut state = self.state.lock().unwrap();
        let Some(order) = state.orders.get_mut(client_order_id) else { return false; };
        order.status = OrderStatus::Cancelled;
        order.terminal_matched_quantity = None;
        state.recovery_pending_orders.insert(client_order_id.to_string());
        recompute_reconciliation(&mut state, "cancellation awaits order audit");
        self.schedule_persist(&state);
        true
    }

    /// Release the still-unfilled reservation after an authoritative terminal
    /// order outcome. Ownership is retained for late fill attribution.
    pub fn release_order(&self, client_order_id: &str, status: OrderStatus) {
        let mut state = self.state.lock().unwrap();
        let Some(mut order) = state.orders.remove(client_order_id) else { return; };
        if let Some(instance) = state.instances.get_mut(&order.instance_id) {
            instance.reserved_cash = (instance.reserved_cash - order.reserved_cash).max(0.0);
            if order.reserved_quantity > 0.0 {
                let entry = instance.reserved_positions.entry(order.token_id.clone()).or_insert(0.0);
                *entry = (*entry - order.reserved_quantity).max(0.0);
            }
        }
        order.reserved_cash = 0.0;
        order.reserved_quantity = 0.0;
        order.status = status;
        state.orders.insert(client_order_id.into(), order);
        self.schedule_persist(&state);
    }

    pub fn release_all_orders(&self) {
        let coids: Vec<String> = self.state.lock().unwrap().orders.keys().cloned().collect();
        for coid in coids { self.release_order(&coid, OrderStatus::Cancelled); }
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
        if !state.seeded { return Err(ReservationError::AccountNotSeeded); }
        if state.uncertain { return Err(ReservationError::AccountUncertain); }
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
            state.instances.get_mut(instance_id).expect("validated").reserved_cash += *amount;
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
        *state.physical_positions.entry(up_token.into()).or_insert(0.0) += total;
        *state.physical_positions.entry(down_token.into()).or_insert(0.0) += total;
        for (instance_id, amount) in allocations {
            let instance = state.instances.get_mut(instance_id).expect("validated");
            instance.reserved_cash = (instance.reserved_cash - *amount).max(0.0);
            instance.cash -= *amount;
            *instance.positions.entry(up_token.into()).or_insert(0.0) += *amount;
            *instance.positions.entry(down_token.into()).or_insert(0.0) += *amount;
        }
        recompute_reconciliation(&mut state, "confirmed split");
        self.schedule_persist(&state);
        Ok(())
    }

    /// Apply confirmed redeem legs from the account-wide maintenance worker.
    /// Each token's collateral payout is allocated in proportion to the
    /// virtual quantity owned by each instance immediately before the burn.
    pub fn apply_redeemed_legs(
        &self,
        legs: &[(String, f64, f64)],
    ) -> Result<(), ReservationError> {
        let mut state = self.state.lock().unwrap();
        if !state.seeded { return Err(ReservationError::AccountNotSeeded); }
        if state.uncertain { return Err(ReservationError::AccountUncertain); }
        for (token, requested_qty, requested_payout) in legs {
            let physical_before = state.physical_positions.get(token).copied().unwrap_or(0.0);
            let removed = finite_nonnegative(*requested_qty).min(physical_before);
            if removed <= EPS { continue; }
            let virtual_total: f64 = state.instances.values()
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
                * if *requested_qty > EPS { removed / *requested_qty } else { 0.0 };
            let ownership_scale = removed / virtual_total;
            for instance in state.instances.values_mut() {
                let owned = instance.positions.get(token).copied().unwrap_or(0.0);
                if owned <= EPS { continue; }
                let burned = (owned * ownership_scale).min(owned);
                let share = burned / removed;
                *instance.positions.entry(token.clone()).or_insert(0.0) -= burned;
                instance.cash += payout * share;
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
        if !state.seeded { return Err(ReservationError::AccountNotSeeded); }
        if state.uncertain { return Err(ReservationError::AccountUncertain); }
        let total: f64 = allocations.values().copied().sum();
        for token in [up_token, down_token] {
            let physical_reserved: f64 = state.instances.values()
                .map(|instance| instance.reserved_positions.get(token).copied().unwrap_or(0.0))
                .sum();
            let physical_available = (state.physical_positions.get(token).copied().unwrap_or(0.0)
                - physical_reserved).max(0.0);
            if total > physical_available + EPS {
                return Err(ReservationError::InsufficientPhysicalPosition {
                    token: token.into(), required: total, available: physical_available,
                });
            }
            for (instance_id, amount) in allocations {
                let Some(instance) = state.instances.get(instance_id) else {
                    return Err(ReservationError::UnknownInstance(instance_id.clone()));
                };
                let available = (instance.positions.get(token).copied().unwrap_or(0.0)
                    - instance.reserved_positions.get(token).copied().unwrap_or(0.0)).max(0.0);
                if *amount > available + EPS {
                    return Err(ReservationError::InsufficientVirtualPosition {
                        token: token.into(), required: *amount, available,
                    });
                }
            }
        }
        for (instance_id, amount) in allocations {
            let instance = state.instances.get_mut(instance_id).expect("validated");
            *instance.reserved_positions.entry(up_token.into()).or_insert(0.0) += *amount;
            *instance.reserved_positions.entry(down_token.into()).or_insert(0.0) += *amount;
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
                    let reserved = instance.reserved_positions.entry(token.into()).or_insert(0.0);
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
                    token: token.into(), required: total, available: physical,
                });
            }
        }
        for (instance_id, amount) in allocations {
            let Some(instance) = state.instances.get(instance_id) else {
                return Err(ReservationError::UnknownInstance(instance_id.clone()));
            };
            for token in [up_token, down_token] {
                let owned = instance.positions.get(token).copied().unwrap_or(0.0);
                let reserved = instance.reserved_positions.get(token).copied().unwrap_or(0.0);
                if *amount > owned.min(reserved) + EPS {
                    return Err(ReservationError::InsufficientVirtualPosition {
                        token: token.into(), required: *amount, available: owned.min(reserved),
                    });
                }
            }
        }
        state.physical_cash += total;
        *state.physical_positions.entry(up_token.into()).or_insert(0.0) -= total;
        *state.physical_positions.entry(down_token.into()).or_insert(0.0) -= total;
        for (instance_id, amount) in allocations {
            let instance = state.instances.get_mut(instance_id).expect("validated");
            instance.cash += *amount;
            for token in [up_token, down_token] {
                *instance.positions.entry(token.into()).or_insert(0.0) -= *amount;
                let reserved = instance.reserved_positions.entry(token.into()).or_insert(0.0);
                *reserved = (*reserved - *amount).max(0.0);
            }
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
        let total: f64 = allocations.values().copied().sum();
        match kind {
            MaintenanceOperationKind::Split => {
                let total_reserved_cash: f64 =
                    state.instances.values().map(|instance| instance.reserved_cash).sum();
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
                            instance.reserved_positions.get(token).copied().unwrap_or(0.0)
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
                            - instance.reserved_positions.get(token).copied().unwrap_or(0.0))
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
                        *instance.reserved_positions.entry(token.to_string()).or_insert(0.0) +=
                            *amount;
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
            self.fail_maintenance_operation(operation_id, format!("reservation persistence: {error}"));
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
            return Err(format!("maintenance operation `{operation_id}` already failed"));
        }
        if operation.tx_id.as_deref().is_some_and(|existing| existing != tx_id) {
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
                            let reserved =
                                instance.reserved_positions.entry(token.clone()).or_insert(0.0);
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
                        let reserved =
                            instance.reserved_positions.get(token).copied().unwrap_or(0.0);
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
                        let reserved =
                            instance.reserved_positions.entry(token.clone()).or_insert(0.0);
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
        self.apply_trade_transition_inner(
            trade_key, status, client_order_id, order_id, token_id,
            side, quantity, price, None, &mut persistence_required,
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
    ) -> Option<TradeOwnership> {
        let mut persistence_required = false;
        let applied = self.apply_trade_transition_inner(
            trade_key, status, client_order_id, order_id, token_id,
            side, quantity, price, Some((is_maker, match_time_secs)),
            &mut persistence_required,
        );
        if applied.is_some()
            && persistence_required
            && self.flush_persistence(Duration::from_secs(2)).is_err()
        {
            self.mark_uncertain_with_reason(format!(
                "trade `{trade_key}` atomic persistence failed"
            ));
            return None;
        }
        applied
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
    ) -> Option<TradeOwnership> {
        let mut normalized = status.trim_start_matches("TRADE_STATUS_").to_ascii_uppercase();
        if normalized == "MATCHED_NOT_BROADCASTED" { normalized = "MATCHED".to_string(); }
        if normalized == "RETRYING" { return None; }
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
        let durable_coid = state.oid_to_coid.get(&normalized_order_id).cloned();
        if !client_order_id.is_empty()
            && durable_coid.as_deref().is_some_and(|coid| coid != client_order_id)
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
                format!(
                    "unowned trade `{trade_key}` coid=`{resolved_coid}` oid=`{order_id}`"
                ),
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

        let existing = state.trades.get(trade_key).cloned();
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
            let price_tolerance = 1e-10_f64.max(prior.price.abs() * 1e-8);
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
            let prior_rank = match applied.ownership.status.as_str() {
                "MATCHED" => 1,
                "MINED" => 2,
                "CONFIRMED" | "FAILED" => 3,
                _ => 0,
            };
            // Mirror the user-feed lifecycle gate inside the durable ledger:
            // terminal states are immutable and replayed same/earlier stages
            // cannot regress a CONFIRMED row or re-book inventory.
            if applied.failed
                || applied.ownership.status == "CONFIRMED"
                || lifecycle_rank <= prior_rank
            {
                let mut changed = state.ownership_anomalies.remove(&anomaly_key).is_some();
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
                        } else { OrderStatus::PartiallyFilled };
                        let fee_changed = apply_configured_trade_fee_locked(
                            &mut state, trade_key, fee_status, is_maker,
                        );
                        let uncertainty_changed = uncertainty_before != (
                            state.uncertain,
                            state.uncertain_reason.clone(),
                            state.uncertain_since_ms,
                        );
                        changed |= role_changed || fee_changed || uncertainty_changed;
                    }
                }
                if changed {
                    recompute_reconciliation(&mut state, "corrected trade ownership replay");
                    self.schedule_persist(&state);
                    *persistence_required = true;
                }
                return Some(applied.ownership.clone());
            }
        }
        let price_tolerance = 1e-10_f64.max(order.price.abs() * 1e-8);
        let violates_limit = match side {
            Side::Buy => price > order.price + price_tolerance,
            Side::Sell => price + price_tolerance < order.price,
        };
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
                    } else { OrderStatus::PartiallyFilled };
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
            *state.physical_positions.entry(token_id.into()).or_insert(0.0) +=
                position_delta * multiplier;
        }
        let mut order_fully_filled = false;
        if should_book {
            let (cash_delta, qty_delta, reservation_token) = if let Some(order) = state.orders.get_mut(&resolved_coid) {
                let cancellation_audit_pending = order.status == OrderStatus::Cancelled;
                let old_cash = order.reserved_cash;
                let old_qty = order.reserved_quantity;
                order.filled_quantity = (order.filled_quantity + quantity).min(order.quantity);
                let fill_target = order.terminal_matched_quantity
                    .unwrap_or(order.quantity).min(order.quantity);
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
                (desired_cash - old_cash, desired_qty - old_qty, order.token_id.clone())
            } else { (0.0, 0.0, token_id.to_string()) };
            if let Some(instance) = state.instances.get_mut(&instance_id) {
                instance.reserved_cash = (instance.reserved_cash + cash_delta).max(0.0);
                if qty_delta.abs() > EPS {
                    let reserved = instance.reserved_positions.entry(reservation_token).or_insert(0.0);
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
                    if order.status == OrderStatus::Cancelled {
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
                (cash_delta, qty_delta, order.token_id.clone(), recovery_pending)
            } else { (0.0, 0.0, token_id.to_string(), false) };
            if let Some(instance) = state.instances.get_mut(&instance_id) {
                instance.reserved_cash = (instance.reserved_cash + reservation_delta.0).max(0.0);
                if reservation_delta.1.abs() > EPS {
                    let reserved = instance.reserved_positions.entry(reservation_delta.2).or_insert(0.0);
                    *reserved = (*reserved + reservation_delta.1).max(0.0);
                }
            }
            if reservation_delta.3 {
                state.recovery_pending_orders.insert(resolved_coid.clone());
            } else {
                state.recovery_pending_orders.remove(&resolved_coid);
            }
        }
        if order_fully_filled {
            state.recovery_pending_orders.remove(&resolved_coid);
        }
        let ownership = TradeOwnership {
            account_id: self.account_id.clone(),
            instance_id,
            trade_key: trade_key.into(),
            client_order_id: resolved_coid,
            order_id: order_id.into(),
            token_id: token_id.into(),
            side,
            quantity,
            price,
            status: normalized,
        };
        state.trades.insert(trade_key.into(), AppliedTrade {
            ownership: ownership.clone(),
            booked: should_book || (already_booked && !should_reverse),
            physical_booked: should_book_physical
                || (physical_booked && !should_reverse_physical),
            usdc_fee: existing.as_ref().map(|trade| trade.usdc_fee).unwrap_or(0.0),
            shares_fee: existing.as_ref().map(|trade| trade.shares_fee).unwrap_or(0.0),
            virtual_fee_booked: existing.as_ref()
                .is_some_and(|trade| trade.virtual_fee_booked),
            physical_fee_booked: existing.as_ref()
                .is_some_and(|trade| trade.physical_fee_booked),
            failed: is_failed,
            failure_reconciled: is_failed
                || existing
                    .as_ref()
                    .is_some_and(|trade| trade.failure_reconciled),
            is_maker: trade_context.map(|(maker, _)| maker)
                .or_else(|| existing.as_ref().and_then(|trade| trade.is_maker)),
            match_time_secs: trade_context.map(|(_, ts)| ts).unwrap_or_else(|| {
                existing.as_ref().map(|trade| trade.match_time_secs).unwrap_or(0)
            }),
            ledger_generation: existing.as_ref()
                .map(|trade| trade.ledger_generation).unwrap_or(0),
        });
        if let Some((is_maker, _)) = trade_context {
            let fee_status = if is_failed { OrderStatus::Failed }
                else { OrderStatus::PartiallyFilled };
            let _ = apply_configured_trade_fee_locked(
                &mut state, trade_key, fee_status, is_maker,
            );
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
            set_uncertain(&mut state, format!("fee attribution missing owned trade `{trade_key}`"));
            self.schedule_persist(&state);
            return false;
        };
        if let Some(trade) = state.trades.get_mut(trade_key) {
            trade.is_maker = Some(is_maker);
        }
        let config = (!is_maker)
            .then(|| state.token_fee_configs.get(&existing.ownership.token_id).cloned())
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
            || !usdc_fee.is_finite() || usdc_fee < 0.0
            || !shares_fee.is_finite() || shares_fee < 0.0
        {
            return false;
        }
        let mut state = self.state.lock().unwrap();
        let Some(existing) = state.trades.get(trade_key).cloned() else {
            set_uncertain(&mut state, format!("fee attribution missing owned trade `{trade_key}`"));
            self.schedule_persist(&state);
            return false;
        };
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

        let effective_usdc_fee = if existing.usdc_fee > EPS { existing.usdc_fee } else { usdc_fee };
        let effective_shares_fee = if existing.shares_fee > EPS { existing.shares_fee } else { shares_fee };
        let upgrades_zero_fee = existing.usdc_fee <= EPS
            && existing.shares_fee <= EPS
            && (effective_usdc_fee > EPS || effective_shares_fee > EPS);
        let is_failed = status == OrderStatus::Failed || existing.failed;
        let book_virtual = !is_failed
            && (!existing.virtual_fee_booked || upgrades_zero_fee);
        let reverse_virtual = is_failed && existing.virtual_fee_booked;
        let book_physical = !is_failed
            && existing.physical_booked
            && (!existing.physical_fee_booked || upgrades_zero_fee);
        let reverse_physical = is_failed && existing.physical_fee_booked;
        let multiplier = |book: bool, reverse: bool| {
            if book { 1.0 } else if reverse { -1.0 } else { 0.0 }
        };
        let virtual_multiplier = multiplier(book_virtual, reverse_virtual);
        if virtual_multiplier != 0.0 {
            if let Some(instance) = state.instances.get_mut(&existing.ownership.instance_id) {
                instance.cash -= effective_usdc_fee * virtual_multiplier;
                *instance.positions.entry(existing.ownership.token_id.clone()).or_insert(0.0) -=
                    effective_shares_fee * virtual_multiplier;
            }
        }
        let physical_multiplier = multiplier(book_physical, reverse_physical);
        if physical_multiplier != 0.0 {
            state.physical_cash -= effective_usdc_fee * physical_multiplier;
            *state.physical_positions.entry(existing.ownership.token_id.clone()).or_insert(0.0) -=
                effective_shares_fee * physical_multiplier;
        }
        if let Some(trade) = state.trades.get_mut(trade_key) {
            trade.usdc_fee = effective_usdc_fee;
            trade.shares_fee = effective_shares_fee;
            trade.virtual_fee_booked = book_virtual
                || (existing.virtual_fee_booked && !reverse_virtual);
            trade.physical_fee_booked = book_physical
                || (existing.physical_fee_booked && !reverse_physical);
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
        self.state.lock().unwrap().trades.values()
            .map(|trade| trade.ownership.clone()).collect()
    }

    pub fn restored_trades(&self) -> Vec<RestoredTrade> {
        self.state.lock().unwrap().trades.values()
            .map(|trade| RestoredTrade {
                ownership: trade.ownership.clone(),
                booked: trade.booked,
                usdc_fee: if trade.virtual_fee_booked { trade.usdc_fee } else { 0.0 },
                shares_fee: if trade.virtual_fee_booked { trade.shares_fee } else { 0.0 },
                virtual_fee_booked: trade.virtual_fee_booked,
                is_maker: trade.is_maker.unwrap_or(false),
                match_time_secs: trade.match_time_secs,
                ledger_generation: trade.ledger_generation,
            })
            .collect()
    }

    /// Bound the durable per-event ownership history after the executor's
    /// late-fill mapping grace has elapsed. Potentially-live/FAILED orders and
    /// nonterminal trades are retained; only fully terminal rows for the
    /// retired token scope are removed.
    pub fn prune_terminal_history(&self, tokens: &HashSet<String>) -> (usize, usize) {
        if tokens.is_empty() {
            return (0, 0);
        }
        let mut state = self.state.lock().unwrap();
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
                    && !protected_coids.contains(*coid)
                    && !state.recovery_pending_orders.contains(*coid)
                    && order.reserved_cash <= EPS
                    && order.reserved_quantity <= EPS
                    && matches!(
                        order.status,
                        OrderStatus::Cancelled | OrderStatus::Rejected | OrderStatus::Filled
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
                    && !state.fee_attribution_pending.contains(*trade_key)
                    && (trade.failed || trade.ownership.status == "CONFIRMED")
            })
            .map(|(trade_key, _)| trade_key.clone())
            .collect();
        for trade_key in &stale_trades {
            state.trades.remove(trade_key);
            state.fee_attribution_pending.remove(trade_key);
        }
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
        let pruned_fee_configs = tokens
            .iter()
            .filter(|token| !protected_fee_tokens.contains(*token))
            .filter(|token| state.token_fee_configs.remove(*token).is_some())
            .count();
        let pruned_trades = stale_trades.len();
        if !stale_orders.is_empty() || pruned_trades > 0 || pruned_fee_configs > 0 {
            self.schedule_persist(&state);
        }
        (stale_orders.len(), pruned_trades)
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
    if value.is_finite() { value.max(0.0) } else { 0.0 }
}

fn wall_clock_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
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
            && (!trade.physical_booked
                || (trade.virtual_fee_booked && !trade.physical_fee_booked))
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

fn apply_configured_trade_fee_locked(
    state: &mut SharedAccountState,
    trade_key: &str,
    status: OrderStatus,
    is_maker: bool,
) -> bool {
    let Some(existing) = state.trades.get(trade_key).cloned() else {
        set_uncertain(state, format!("fee attribution missing owned trade `{trade_key}`"));
        return false;
    };
    if existing.is_maker.is_some_and(|stored| stored != is_maker) {
        set_uncertain(state, format!(
            "trade role replay mismatch trade={trade_key} stored_maker={:?} replay_maker={is_maker}",
            existing.is_maker,
        ));
        return false;
    }
    if let Some(trade) = state.trades.get_mut(trade_key) { trade.is_maker = Some(is_maker); }
    let config = (!is_maker)
        .then(|| state.token_fee_configs.get(&existing.ownership.token_id).cloned())
        .flatten();
    if !is_maker && config.is_none() {
        state.fee_attribution_pending.insert(trade_key.to_string());
        recompute_reconciliation(state, "missing token fee config");
        return false;
    }
    let notional = config.map_or(0.0, |config| {
        let price = existing.ownership.price.clamp(0.0, 1.0);
        existing.ownership.quantity * config.rate
            * (price * (1.0 - price)).max(0.0).powf(config.exponent)
    });
    let (usdc_fee, shares_fee) = if is_maker {
        (0.0, 0.0)
    } else if existing.ownership.side == Side::Buy {
        (0.0, if existing.ownership.price > EPS {
            notional / existing.ownership.price
        } else { 0.0 })
    } else { (notional, 0.0) };
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
        set_uncertain(state, format!("fee attribution missing owned trade `{trade_key}`"));
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
    let effective_usdc = if existing.usdc_fee > EPS { existing.usdc_fee } else { usdc_fee };
    let effective_shares = if existing.shares_fee > EPS { existing.shares_fee } else { shares_fee };
    let upgrades_zero = existing.usdc_fee <= EPS && existing.shares_fee <= EPS
        && (effective_usdc > EPS || effective_shares > EPS);
    let failed = status == OrderStatus::Failed || existing.failed;
    let book_virtual = !failed && (!existing.virtual_fee_booked || upgrades_zero);
    let reverse_virtual = failed && existing.virtual_fee_booked;
    let book_physical = !failed && existing.physical_booked
        && (!existing.physical_fee_booked || upgrades_zero);
    let reverse_physical = failed && existing.physical_fee_booked;
    let multiplier = |book, reverse| if book { 1.0 } else if reverse { -1.0 } else { 0.0 };
    let virtual_multiplier = multiplier(book_virtual, reverse_virtual);
    if virtual_multiplier != 0.0 {
        if let Some(instance) = state.instances.get_mut(&existing.ownership.instance_id) {
            instance.cash -= effective_usdc * virtual_multiplier;
            *instance.positions.entry(existing.ownership.token_id.clone()).or_insert(0.0)
                -= effective_shares * virtual_multiplier;
        }
    }
    let physical_multiplier = multiplier(book_physical, reverse_physical);
    if physical_multiplier != 0.0 {
        state.physical_cash -= effective_usdc * physical_multiplier;
        *state.physical_positions.entry(existing.ownership.token_id.clone()).or_insert(0.0)
            -= effective_shares * physical_multiplier;
    }
    if let Some(trade) = state.trades.get_mut(trade_key) {
        trade.usdc_fee = effective_usdc;
        trade.shares_fee = effective_shares;
        trade.virtual_fee_booked = book_virtual
            || (existing.virtual_fee_booked && !reverse_virtual);
        trade.physical_fee_booked = book_physical
            || (existing.physical_fee_booked && !reverse_physical);
    }
    if virtual_multiplier != 0.0 {
        advance_trade_ledger_generation(state, trade_key);
    }
    state.fee_attribution_pending.remove(trade_key);
    recompute_reconciliation(state, "trade fee lifecycle transition");
    true
}

fn desired_order_reservation(order: &OrderOwnership) -> (f64, f64) {
    let target = order.terminal_matched_quantity.unwrap_or(order.quantity).min(order.quantity);
    let remaining = (target - order.filled_quantity).max(0.0);
    match order.side {
        Side::Buy => (
            remaining * order.price * (1.0 + order.fee_rate_bps as f64 / 10_000.0),
            0.0,
        ),
        Side::Sell => (0.0, remaining),
    }
}

fn normalize_terminal_failed_state(state: &mut SharedAccountState) -> bool {
    let mut changed = false;
    let failed_coids: Vec<String> = state.orders.iter()
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
    let interests: Vec<&TokenInterest> = state.instances.values()
        .flat_map(|instance| instance.token_interests.values())
        .filter(|interest| !interest.scope_key.is_empty()
            && (authoritative_tokens.contains(&interest.up_token_id)
                || authoritative_tokens.contains(&interest.down_token_id)))
        .collect();
    let mut missing = Vec::new();
    for interest in interests {
        for (instance_id, instance) in &state.instances {
            if !instance.market_scopes.contains(&interest.scope_key) { continue; }
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
        let physical = state.physical_positions.get(&trade.ownership.token_id).copied().unwrap_or(0.0);
        let virtual_qty: f64 = state.instances.values()
            .map(|instance| instance.positions.get(&trade.ownership.token_id).copied().unwrap_or(0.0))
            .sum();
        if token_delta >= -reconciliation_tolerance(physical, virtual_qty) {
            trade.failure_reconciled = true;
            changed = true;
        }
    }
    changed
}

fn recompute_reconciliation(state: &mut SharedAccountState, deficit_context: &str) {
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
        state.instances.values().flat_map(|instance| instance.positions.keys().cloned()),
    );
    let mut negative_tokens = Vec::new();
    for token in all_tokens {
        let physical = state.physical_positions.get(&token).copied().unwrap_or(0.0);
        let virtual_qty: f64 = state.instances.values()
            .map(|instance| instance.positions.get(&token).copied().unwrap_or(0.0))
            .sum();
        let pending = pending_position_deltas.get(&token).copied().unwrap_or(0.0);
        let expected_virtual = virtual_qty - pending;
        let delta = physical - expected_virtual;
        let tolerance = reconciliation_tolerance(physical, expected_virtual);
        if delta.abs() > tolerance {
            if delta < -tolerance {
                negative_tokens.push(format!("{token}:{delta:.6}"));
            }
            state.unallocated_positions.insert(token, delta);
        }
    }
    negative_tokens.sort();
    if !state.ownership_anomalies.is_empty() {
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
    } else if !state.recovery_pending_orders.is_empty() {
        let mut pending: Vec<&str> = state
            .recovery_pending_orders
            .iter()
            .map(String::as_str)
            .collect();
        pending.sort_unstable();
        set_uncertain(
            state,
            format!(
                "order trade audit pending: count={} coids=[{}]",
                pending.len(),
                pending.join(","),
            ),
        );
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
    } else if state.unallocated_cash
        < -reconciliation_tolerance(state.physical_cash, virtual_cash - pending_cash_delta)
        || !negative_tokens.is_empty()
    {
        set_uncertain(
            state,
            format!(
                "{deficit_context}: cash_delta={:.6} negative_tokens=[{}]",
                state.unallocated_cash,
                negative_tokens.join(","),
            ),
        );
    } else {
        clear_uncertain(state);
    }

}

fn reconciliation_tolerance(lhs: f64, rhs: f64) -> f64 {
    RECONCILIATION_UNIT + lhs.abs().max(rhs.abs()) * 1e-12
}

fn set_ownership_anomaly(state: &mut SharedAccountState, key: String, reason: String) {
    state.ownership_anomalies.insert(key, reason.clone());
    set_uncertain(state, reason);
}

/// Polymarket may redeem a winning binary token outside the bot process. The
/// authoritative wallet delta is unambiguous only when removed token quantity
/// and added pUSD match 1:1. In that narrow case, burn virtual inventory and
/// credit cash to the token's existing owners. Other external operations stay
/// unallocated/uncertain until explicitly attributed by operation_id.
fn try_attribute_binary_redeem(state: &mut SharedAccountState) {
    if state.unallocated_cash <= EPS { return; }
    let removed: Vec<(String, f64)> = state.unallocated_positions.iter()
        .filter(|(token, delta)| {
            **delta < -EPS
                && state
                    .settled_token_values
                    .get(*token)
                    .is_some_and(|value| *value == 1.0)
        })
        .map(|(token, delta)| (token.clone(), -*delta))
        .collect();
    if removed.is_empty() { return; }
    let removed_total: f64 = removed.iter().map(|(_, qty)| *qty).sum();
    let tolerance = 0.02_f64.max(removed_total * 0.001);
    if (removed_total - state.unallocated_cash).abs() > tolerance { return; }
    for (token, qty) in &removed {
        let virtual_total: f64 = state.instances.values()
            .map(|instance| instance.positions.get(token).copied().unwrap_or(0.0))
            .sum();
        if virtual_total + EPS < *qty { return; }
    }

    let cash_to_credit = state.unallocated_cash;
    for (token, qty) in &removed {
        let virtual_total: f64 = state.instances.values()
            .map(|instance| instance.positions.get(token).copied().unwrap_or(0.0))
            .sum();
        if virtual_total <= EPS { continue; }
        for instance in state.instances.values_mut() {
            let owned = instance.positions.get(token).copied().unwrap_or(0.0);
            if owned <= EPS { continue; }
            let burned = (owned * *qty / virtual_total).min(owned);
            *instance.positions.entry(token.clone()).or_insert(0.0) -= burned;
            instance.cash += cash_to_credit * burned / removed_total;
        }
    }
    log::info!(
        "[shared_account] inferred platform binary redeem: cash={:.6} removed={:?}",
        cash_to_credit,
        removed,
    );
    recompute_reconciliation(state, "inferred platform binary redeem");
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
    let has_any_interest = state.instances.values()
        .any(|instance| !instance.token_interests.is_empty());
    for (token, qty) in &state.physical_positions {
        // Backward-compatible startup fallback for callers that have not yet
        // registered any event scope. Live polymaker registers scopes before
        // seeding, so the exact-token equal-allocation branch below is used.
        if !has_any_interest {
            for instance in state.instances.values_mut() {
                instance.positions.insert(
                    token.clone(),
                    *qty * instance.weight / total,
                );
            }
            continue;
        }
        let owners: Vec<String> = state.instances.iter()
            .filter(|(_, instance)| {
                instance.token_interests.values().any(|interest| {
                    interest.up_token_id == *token || interest.down_token_id == *token
                })
            })
            .map(|(instance_id, _)| instance_id.clone())
            .collect();
        if owners.is_empty() {
            state.unallocated_positions.insert(token.clone(), *qty);
            continue;
        }
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

    fn seeded_account() -> SharedAccount {
        let account = SharedAccount::new("acct");
        account.register_instance("a", 1.0);
        account.register_instance("b", 3.0);
        account.apply_physical_snapshot(400.0, HashMap::from([("UP".into(), 40.0)]));
        account
    }

    #[test]
    fn weighted_snapshot_allocation_and_default_weight() {
        let account = seeded_account();
        assert_eq!(account.instance_snapshot("a").unwrap().cash, 100.0);
        assert_eq!(account.instance_snapshot("b").unwrap().cash, 300.0);
        assert_eq!(account.instance_snapshot("a").unwrap().positions["UP"], 10.0);

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
        account.apply_physical_snapshot(
            100.0,
            HashMap::from([("BTC-UP".to_string(), 20.0)]),
        );

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
    fn reservation_checks_virtual_and_physical_limits_atomically() {
        let account = seeded_account();
        account.reserve_order("a", "a-1", "oid-a-1", "UP", Side::Buy, 100.0, 1.0, 0).unwrap();
        let err = account.reserve_order("a", "a-2", "oid-a-2", "UP", Side::Buy, 0.1, 1.0, 0)
            .unwrap_err();
        assert!(matches!(err, ReservationError::InsufficientVirtualCash { .. }));
        account.reserve_order("b", "b-1", "oid-b-1", "UP", Side::Buy, 300.0, 1.0, 0).unwrap();
        assert_eq!(account.availability("b", "UP").unwrap().physical_cash, 0.0);
    }

    #[test]
    fn terminal_filled_status_keeps_reservation_until_trade_audit() {
        let account = seeded_account();
        account
            .reserve_order("a", "a-fill", "oid-fill", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();

        assert!(account.mark_filled_pending_audit("a-fill"));
        let pending = account.instance_snapshot("a").unwrap();
        assert_eq!(pending.reserved_cash, 5.0);
        assert_eq!(account.monitoring_snapshot().recovery_pending_orders, 1);
        assert!(account.is_uncertain());

        account
            .apply_trade_transition(
                "trade-fill",
                "MATCHED",
                "a-fill",
                "oid-fill",
                "UP",
                Side::Buy,
                10.0,
                0.5,
            )
            .unwrap();
        let audited = account.instance_snapshot("a").unwrap();
        assert_eq!(audited.reserved_cash, 0.0);
        assert_eq!(account.monitoring_snapshot().recovery_pending_orders, 0);
        assert!(!account.is_uncertain());
    }

    #[test]
    fn partial_buy_fill_recomputes_principal_and_fee_reservation() {
        let account = seeded_account();
        let baseline_generation = account.instance_snapshot("a").unwrap().ledger_generation;
        account.reserve_order(
            "a", "a-fee-partial", "oid-fee-partial", "UP",
            Side::Buy, 10.0, 0.5, 1_000,
        ).unwrap();
        assert!((account.instance_snapshot("a").unwrap().reserved_cash - 5.5).abs() < 1e-12);

        account.apply_trade_transition(
            "trade-fee-partial-1", "MATCHED", "a-fee-partial",
            "oid-fee-partial", "UP", Side::Buy, 4.0, 0.5,
        ).unwrap();
        let partial = account.instance_snapshot("a").unwrap();
        assert!((partial.reserved_cash - 3.3).abs() < 1e-12);
        assert!(partial.ledger_generation > baseline_generation);
        let restored_generation = account.restored_trades().into_iter()
            .find(|trade| trade.ownership.trade_key == "trade-fee-partial-1")
            .unwrap().ledger_generation;
        assert_eq!(restored_generation, partial.ledger_generation);
        assert!((account.order("a-fee-partial").unwrap().reserved_cash - 3.3).abs() < 1e-12);

        account.apply_trade_transition(
            "trade-fee-partial-1", "CONFIRMED", "a-fee-partial",
            "oid-fee-partial", "UP", Side::Buy, 4.0, 0.5,
        ).unwrap();
        assert!((account.instance_snapshot("a").unwrap().reserved_cash - 3.3).abs() < 1e-12);

        account.apply_trade_transition(
            "trade-fee-partial-2", "MATCHED", "a-fee-partial",
            "oid-fee-partial", "UP", Side::Buy, 6.0, 0.5,
        ).unwrap();
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
            .reserve_order("a", "a-partial", "oid-partial", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();
        account.mark_order_status("a-partial", OrderStatus::PartiallyFilled);
        assert_eq!(
            account.mark_order_status_effective("a-partial", OrderStatus::Accepted),
            Some(OrderStatus::PartiallyFilled),
        );
        assert_eq!(account.order("a-partial").unwrap().status, OrderStatus::PartiallyFilled);
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
    fn failed_trade_on_authoritatively_cancelled_parent_releases_restored_reservation() {
        let account = seeded_account();
        account
            .reserve_order("a", "a-cancel-fail", "oid-cancel-fail", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();
        assert!(account.mark_cancelled_pending_trade_audit("a-cancel-fail", 10.0));
        assert_eq!(account.monitoring_snapshot().recovery_pending_orders, 1);

        account
            .apply_trade_transition(
                "trade-cancel-fail",
                "MATCHED",
                "a-cancel-fail",
                "oid-cancel-fail",
                "UP",
                Side::Buy,
                10.0,
                0.5,
            )
            .unwrap();
        assert_eq!(account.instance_snapshot("a").unwrap().reserved_cash, 0.0);
        assert_eq!(account.monitoring_snapshot().recovery_pending_orders, 0);

        account
            .apply_trade_transition(
                "trade-cancel-fail",
                "FAILED",
                "a-cancel-fail",
                "oid-cancel-fail",
                "UP",
                Side::Buy,
                10.0,
                0.5,
            )
            .unwrap();
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
            .reserve_order("a", "a-cancel-partial-fail", "oid-cancel-partial-fail", "UP", Side::Buy, 15.0, 0.5, 0)
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

        account
            .apply_trade_transition(
                "trade-failed",
                "FAILED",
                "a-failed",
                "oid-failed",
                "UP",
                Side::Buy,
                10.0,
                0.5,
            )
            .unwrap();
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
        account.reserve_order("a", "a-1", "oid-a", "UP", Side::Sell, 15.0, 0.5, 0).unwrap();
        let err = account.reserve_order("a", "a-2", "oid-a2", "UP", Side::Sell, 1.0, 0.5, 0)
            .unwrap_err();
        assert!(matches!(err, ReservationError::InsufficientVirtualPosition { .. }));
        account.reserve_order("b", "b-1", "oid-b", "UP", Side::Sell, 15.0, 0.5, 0).unwrap();
        assert_eq!(account.availability("b", "UP").unwrap().physical_position, 0.0);
    }

    #[test]
    fn trade_is_owned_and_replay_is_idempotent() {
        let account = seeded_account();
        account.reserve_order("a", "a-1", "oid-a", "UP", Side::Buy, 10.0, 0.5, 0).unwrap();
        let first = account.apply_trade_transition(
            "trade:oid-a", "MATCHED", "a-1", "oid-a", "UP", Side::Buy, 10.0, 0.5,
        ).unwrap();
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
        account.apply_trade_transition(
            "trade:oid-a", "MINED", "a-1", "oid-a", "UP", Side::Buy, 10.0, 0.5,
        );
        assert_eq!(account.instance_snapshot("a").unwrap().cash, cash);
        let mined = account.monitoring_snapshot();
        assert_eq!(mined.physical_cash, 395.0);
        assert_eq!(mined.physical_positions["UP"], 50.0);
        assert!(!mined.uncertain);
        account.apply_trade_transition(
            "trade:oid-a", "FAILED", "a-1", "oid-a", "UP", Side::Buy, 10.0, 0.5,
        );
        assert_eq!(account.instance_snapshot("a").unwrap().cash, 100.0);
        let failed = account.monitoring_snapshot();
        assert_eq!(failed.physical_cash, 400.0);
        assert_eq!(failed.physical_positions["UP"], 40.0);
        assert!(!failed.uncertain, "FAILED is terminal and needs no wallet audit");
        assert!(!account.is_uncertain());
        account.apply_trade_transition(
            "trade:oid-a",
            "MATCHED",
            "a-1",
            "oid-a",
            "UP",
            Side::Buy,
            10.0,
            0.5,
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
        account.register_token_fee_config(&["UP".to_string()], 0.25, 2.0).unwrap();
        account.reserve_order(
            "a", "a-atomic", "oid-atomic", "UP", Side::Buy, 5.0, 0.4, 0,
        ).unwrap();
        account.apply_trade_transition_with_context(
            "trade-atomic", "MATCHED_NOT_BROADCASTED", "a-atomic", "oid-atomic",
            "UP", Side::Buy, 5.0, 0.4, false, 1_700_000_000,
        ).unwrap();
        let restored = account.restored_trades().into_iter()
            .find(|trade| trade.ownership.trade_key == "trade-atomic").unwrap();
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

        assert_eq!(
            account.order_owner_by_oid("AABBCCDD").as_deref(),
            Some("a")
        );
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
                .reserve_order(
                    "a",
                    "a-guarded",
                    "0xA1B2",
                    "UP",
                    Side::Buy,
                    10.0,
                    0.5,
                    0,
                )
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
            .reserve_order(
                "a",
                "a-sticky",
                "oid-sticky",
                "UP",
                Side::Buy,
                10.0,
                0.5,
                0,
            )
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
        assert!(account.is_uncertain(), "a wallet snapshot cannot repair ownership");
        assert!(account.ownership_anomalies().contains_key("trade:trade-sticky"));
        assert!(account.monitoring_snapshot().uncertain_reason.is_some());

        assert!(account
            .apply_trade_transition(
                "trade-sticky",
                "MATCHED",
                "a-sticky",
                "oid-sticky",
                "UP",
                Side::Buy,
                10.0,
                0.5,
            )
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
            .reserve_order("a", "a-life-econ", "oid-life-econ", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();
        lifecycle
            .apply_trade_transition(
                "life-econ",
                "MATCHED",
                "a-life-econ",
                "oid-life-econ",
                "UP",
                Side::Buy,
                5.0,
                0.49,
            )
            .unwrap();
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
    fn physical_snapshot_generation_ignores_duplicate_and_stale_fanout() {
        let account = SharedAccount::new("acct");
        account.register_instance("a", 1.0);
        assert!(account.apply_scoped_physical_snapshot_versioned(
            2,
            400.0,
            HashMap::new(),
            HashSet::new(),
        ));
        assert!(!account.apply_scoped_physical_snapshot_versioned(
            2,
            100.0,
            HashMap::new(),
            HashSet::new(),
        ));
        assert!(!account.apply_scoped_physical_snapshot_versioned(
            1,
            200.0,
            HashMap::new(),
            HashSet::new(),
        ));
        assert_eq!(account.monitoring_snapshot().physical_cash, 400.0);
        assert!(!account.apply_scoped_physical_snapshot_versioned(
            3,
            500.0,
            HashMap::new(),
            HashSet::new(),
        ));
        assert_eq!(account.monitoring_snapshot().physical_cash, 400.0);
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
            "trade-buy", "MATCHED", "a-buy", "oid-buy", "UP", Side::Buy, 10.0, 0.5,
        );
        account.apply_trade_transition(
            "trade-sell", "MATCHED", "b-sell", "oid-sell", "UP", Side::Sell, 10.0, 0.5,
        );

        // The two pending wallet deltas cancel in aggregate. Equality alone
        // cannot prove either individual trade reached the chain.
        account.apply_physical_snapshot(400.0, HashMap::from([("UP".into(), 40.0)]));
        account.apply_trade_transition(
            "trade-buy", "CONFIRMED", "a-buy", "oid-buy", "UP", Side::Buy, 10.0, 0.5,
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
        account
            .apply_trade_transition(
                "trade-buy", "MATCHED", "a-buy", "oid-buy", "UP", Side::Buy, 10.0, 0.5,
            )
            .unwrap();
        account.state.lock().unwrap().startup_snapshot_applied_this_process = false;

        // This wallet view may already contain trade-buy, but a snapshot has no
        // trade id and therefore cannot prove that fact. Preserve the trade-
        // driven physical ledger until the lifecycle edge arrives.
        assert!(!account.apply_scoped_physical_snapshot_versioned(
            1,
            395.0,
            HashMap::from([("UP".into(), 50.0)]),
            HashSet::from(["UP".into()]),
        ));
        assert_eq!(account.monitoring_snapshot().physical_cash, 400.0);
        assert_eq!(account.monitoring_snapshot().physical_positions["UP"], 40.0);

        account
            .apply_trade_transition(
                "trade-buy", "CONFIRMED", "a-buy", "oid-buy", "UP", Side::Buy, 10.0, 0.5,
            )
            .unwrap();
        assert_eq!(account.monitoring_snapshot().physical_cash, 395.0);
        assert_eq!(account.monitoring_snapshot().physical_positions["UP"], 50.0);
        assert!(account.apply_scoped_physical_snapshot_versioned(
            1,
            395.0,
            HashMap::from([("UP".into(), 50.0)]),
            HashSet::from(["UP".into()]),
        ));
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
        account
            .apply_trade_transition(
                "trade-buy", "MATCHED", "a-buy", "oid-buy", "UP", Side::Buy, 10.0, 0.5,
            )
            .unwrap();
        account.release_order("a-buy", OrderStatus::Filled);

        assert_eq!(
            account.prune_terminal_history(&HashSet::from(["UP".into()])),
            (0, 0),
        );
        assert_eq!(account.order_owner_by_oid("oid-buy").as_deref(), Some("a"));
        assert!(account
            .apply_trade_transition(
                "trade-buy", "CONFIRMED", "a-buy", "oid-buy", "UP", Side::Buy, 10.0, 0.5,
            )
            .is_some());

        assert_eq!(
            account.prune_terminal_history(&HashSet::from(["UP".into()])),
            (1, 1),
        );
        assert!(account.order_owner_by_oid("oid-buy").is_none());
    }

    #[test]
    fn taker_fee_follows_virtual_physical_and_failed_lifecycle() {
        let account = seeded_account();
        account
            .reserve_order("a", "a-fee", "oid-fee", "UP", Side::Buy, 10.0, 0.5, 0)
            .unwrap();
        account
            .apply_trade_transition(
                "trade-fee:oid-fee",
                "MATCHED",
                "a-fee",
                "oid-fee",
                "UP",
                Side::Buy,
                10.0,
                0.5,
            )
            .unwrap();
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

        account
            .apply_trade_transition(
                "trade-fee:oid-fee",
                "MINED",
                "a-fee",
                "oid-fee",
                "UP",
                Side::Buy,
                10.0,
                0.5,
            )
            .unwrap();
        assert!(account.apply_trade_fee_transition(
            "trade-fee:oid-fee",
            OrderStatus::PartiallyFilled,
            0.0,
            0.2,
        ));
        let mined = account.monitoring_snapshot();
        assert!((mined.physical_positions["UP"] - 49.8).abs() < EPS);
        assert!(!mined.uncertain);

        account
            .apply_trade_transition(
                "trade-fee:oid-fee",
                "FAILED",
                "a-fee",
                "oid-fee",
                "UP",
                Side::Buy,
                10.0,
                0.5,
            )
            .unwrap();
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
            .reserve_order("a", "a-cold-fee", "oid-cold-fee", "UP", Side::Buy, 10.0, 0.5, 0)
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
        assert!(account.apply_configured_trade_fee(
            "trade-cold-fee",
            OrderStatus::Filled,
            false,
        ));
        assert!((account.monitoring_snapshot().physical_positions["UP"] - 49.9).abs() < EPS);
    }

    #[test]
    fn aggregate_split_keeps_instance_attribution() {
        let account = SharedAccount::new("acct");
        account.register_instance("a", 1.0);
        account.register_instance("b", 1.0);
        account.apply_physical_snapshot(100.0, HashMap::new());
        account.apply_split_allocations(
            "UP", "DOWN", &HashMap::from([("a".into(), 30.0), ("b".into(), 30.0)]),
        ).unwrap();
        assert_eq!(account.instance_snapshot("a").unwrap().cash, 20.0);
        assert_eq!(account.instance_snapshot("a").unwrap().positions["UP"], 30.0);
        assert_eq!(account.instance_snapshot("b").unwrap().positions["DOWN"], 30.0);
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
        assert_eq!(account.monitoring_snapshot().pending_maintenance_operations, 1);
        account
            .mark_maintenance_operation_submitted("split-op-1", "tx-1")
            .unwrap();
        assert_eq!(
            account.maintenance_operation("split-op-1").unwrap().tx_id.as_deref(),
            Some("tx-1"),
        );

        account.confirm_maintenance_operation("split-op-1").unwrap();
        let after = account.monitoring_snapshot();
        assert_eq!(after.physical_cash, 360.0);
        assert_eq!(after.reserved_cash, 0.0);
        assert_eq!(after.pending_maintenance_operations, 0);
        assert_eq!(account.instance_snapshot("a").unwrap().positions["UP"], 20.0);
        assert_eq!(account.instance_snapshot("b").unwrap().positions["DOWN"], 30.0);

        // Recovery may observe the same terminal chain state more than once.
        account.confirm_maintenance_operation("split-op-1").unwrap();
        assert_eq!(account.monitoring_snapshot().physical_cash, 360.0);
    }

    #[test]
    fn persistent_submitted_maintenance_operation_forces_restart_recovery() {
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
        let err = account.reserve_order(
            "a", "a-order", "a-oid", "UP", Side::Buy, 21.0, 1.0, 0,
        ).unwrap_err();
        assert!(matches!(err, ReservationError::InsufficientVirtualCash { .. }));
        account.reserve_order(
            "a", "a-order", "a-oid", "UP", Side::Buy, 20.0, 1.0, 0,
        ).unwrap();
        assert_eq!(account.availability("b", "UP").unwrap().physical_cash, 20.0);
        account.confirm_reserved_split("UP", "DOWN", &allocations).unwrap();
        assert_eq!(account.instance_snapshot("a").unwrap().cash, 20.0);
        assert_eq!(account.instance_snapshot("b").unwrap().positions["DOWN"], 30.0);
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
        account.apply_redeemed_legs(&[
            ("WIN".into(), 100.0, 100.0),
            ("LOSE".into(), 100.0, 0.0),
        ]).unwrap();
        assert_eq!(account.instance_snapshot("a").unwrap().cash, 100.0);
        assert_eq!(account.instance_snapshot("b").unwrap().cash, 100.0);
        assert_eq!(account.instance_snapshot("a").unwrap().positions["WIN"], 0.0);
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
        account.apply_merge_allocations("UP", "DOWN", &allocations).unwrap();
        assert_eq!(account.instance_snapshot("a").unwrap().cash, 30.0);
        assert_eq!(account.instance_snapshot("b").unwrap().cash, 40.0);
        assert_eq!(account.instance_snapshot("a").unwrap().positions["UP"], 20.0);
        assert_eq!(account.instance_snapshot("b").unwrap().positions["DOWN"], 10.0);
    }

    #[test]
    fn token_inventory_is_allocated_only_to_instances_trading_that_token() {
        let account = SharedAccount::new("multi-asset");
        account.register_instance("btc-a", 1.0);
        // Cash weights remain configurable, but cold token ownership is equal
        // among only the instances that trade that exact event/token.
        account.register_instance("btc-b", 3.0);
        account.register_instance("eth", 1.0);
        account.register_token_interest("btc-a", "btc-event", "BTC-UP", "BTC-DOWN").unwrap();
        account.register_token_interest("btc-b", "btc-event", "BTC-UP", "BTC-DOWN").unwrap();
        account.register_token_interest("eth", "eth-event", "ETH-UP", "ETH-DOWN").unwrap();
        account.apply_physical_snapshot(300.0, HashMap::from([
            ("BTC-UP".into(), 40.0),
            ("BTC-DOWN".into(), 40.0),
            ("ETH-UP".into(), 30.0),
            ("ETH-DOWN".into(), 30.0),
        ]));

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
        account.register_token_interest("btc-a", "btc-event", "BTC-UP", "BTC-DOWN").unwrap();
        let tokens = HashSet::from(["BTC-UP".to_string(), "BTC-DOWN".to_string()]);
        assert!(!account.apply_scoped_physical_snapshot_versioned(
            7, 90.0,
            HashMap::from([("BTC-UP".into(), 40.0), ("BTC-DOWN".into(), 40.0)]),
            tokens.clone(),
        ));
        assert!(!account.monitoring_snapshot().seeded);
        account.register_token_interest("btc-b", "btc-event", "BTC-UP", "BTC-DOWN").unwrap();
        assert!(account.apply_scoped_physical_snapshot_versioned(
            7, 90.0,
            HashMap::from([("BTC-UP".into(), 40.0), ("BTC-DOWN".into(), 40.0)]),
            tokens,
        ));
        assert_eq!(account.instance_snapshot("btc-a").unwrap().positions["BTC-UP"], 20.0);
        assert_eq!(account.instance_snapshot("btc-b").unwrap().positions["BTC-UP"], 20.0);
        assert!(!account.instance_snapshot("eth").unwrap().positions.contains_key("BTC-UP"));
    }

    #[test]
    fn first_snapshot_barrier_times_out_without_stealing_registered_token_ownership() {
        let account = SharedAccount::new("registration-barrier-timeout");
        account.register_instance("btc-a", 1.0);
        account.register_instance("btc-b", 1.0);
        account.register_market_scope("btc-a", "btc-up-or-down-5m");
        account.register_market_scope("btc-b", "btc-up-or-down-5m");
        account.register_token_interest("btc-a", "btc-event", "BTC-UP", "BTC-DOWN").unwrap();
        {
            let mut state = account.state.lock().unwrap();
            state.initial_token_barrier_started_ms = Some(
                wall_clock_ms()
                    .saturating_sub(INITIAL_TOKEN_BARRIER_TIMEOUT_MS)
                    .saturating_sub(1),
            );
        }
        assert!(account.apply_scoped_physical_snapshot_versioned(
            9,
            100.0,
            HashMap::from([("BTC-UP".into(), 40.0)]),
            HashSet::from(["BTC-UP".into(), "BTC-DOWN".into()]),
        ));
        assert_eq!(account.instance_snapshot("btc-a").unwrap().cash, 50.0);
        assert_eq!(account.instance_snapshot("btc-b").unwrap().cash, 50.0);
        assert_eq!(account.instance_snapshot("btc-a").unwrap().positions["BTC-UP"], 40.0);
        assert!(!account.instance_snapshot("btc-b").unwrap().positions.contains_key("BTC-UP"));
        assert_eq!(
            account.state.lock().unwrap().initial_token_barrier_degraded_members.len(),
            1,
        );
    }

    #[test]
    fn reconciliation_uses_wallet_unit_tolerance_but_keeps_exact_metrics() {
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
        assert!(account.is_uncertain());
    }

    #[test]
    fn scoped_snapshot_does_not_zero_another_assets_positions() {
        let account = SharedAccount::new("multi-asset");
        account.register_instance("btc", 1.0);
        account.register_instance("eth", 1.0);
        account.register_token_interest("btc", "btc-event", "BTC-UP", "BTC-DOWN").unwrap();
        account.register_token_interest("eth", "eth-event", "ETH-UP", "ETH-DOWN").unwrap();
        account.apply_scoped_physical_snapshot(
            100.0,
            HashMap::from([("BTC-UP".into(), 10.0), ("ETH-UP".into(), 20.0)]),
            HashSet::from(["BTC-UP".into(), "BTC-DOWN".into(), "ETH-UP".into(), "ETH-DOWN".into()]),
        );
        account.state.lock().unwrap().startup_snapshot_applied_this_process = false;
        account.apply_scoped_physical_snapshot(
            100.0,
            HashMap::new(),
            HashSet::from(["BTC-UP".into(), "BTC-DOWN".into()]),
        );

        let metric = account.monitoring_snapshot();
        assert!(!metric.physical_positions.contains_key("BTC-UP"));
        assert_eq!(metric.physical_positions["ETH-UP"], 20.0);
        assert_eq!(account.instance_snapshot("eth").unwrap().positions["ETH-UP"], 20.0);
        assert!(account.is_uncertain(), "BTC's unexplained removal must fail closed");
    }

    #[test]
    fn external_adjustment_is_idempotent_and_advances_both_ledgers() {
        let account = SharedAccount::new("external");
        account.register_instance("a", 1.0);
        account.register_instance("b", 1.0);
        account.apply_physical_snapshot(100.0, HashMap::new());
        account.attribute_external_adjustment("deposit-1", "a", 20.0, HashMap::new()).unwrap();
        account.attribute_external_adjustment("deposit-1", "a", 20.0, HashMap::new()).unwrap();
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
        account.register_token_interest("btc", "btc-event", "BTC-WIN", "BTC-LOSE").unwrap();
        account.register_token_interest("eth", "eth-event", "ETH-WIN", "ETH-LOSE").unwrap();
        account.apply_physical_snapshot(
            100.0,
            HashMap::from([("BTC-WIN".into(), 30.0), ("ETH-WIN".into(), 20.0)]),
        );
        account.record_settled_token_values(&HashMap::from([
            ("BTC-WIN".into(), 1.0),
            ("BTC-LOSE".into(), 0.0),
        ]));
        account.state.lock().unwrap().startup_snapshot_applied_this_process = false;

        account.apply_physical_snapshot(
            130.0,
            HashMap::from([("ETH-WIN".into(), 20.0)]),
        );
        assert!(!account.is_uncertain());
        assert_eq!(account.instance_snapshot("btc").unwrap().cash, 80.0);
        assert_eq!(account.instance_snapshot("eth").unwrap().cash, 50.0);
        assert_eq!(account.monitoring_snapshot().unallocated_cash, 0.0);
    }

    #[test]
    fn runtime_platform_redeem_is_attributed_without_applying_a_snapshot() {
        let account = SharedAccount::new("runtime-platform-redeem");
        account.register_instance("btc", 1.0);
        account.register_instance("eth", 1.0);
        account.register_token_interest("btc", "btc-event", "BTC-WIN", "BTC-LOSE").unwrap();
        account.register_token_interest("eth", "eth-event", "ETH-UP", "ETH-DOWN").unwrap();
        account.apply_physical_snapshot(
            100.0,
            HashMap::from([("BTC-WIN".into(), 30.0), ("ETH-UP".into(), 20.0)]),
        );
        account.record_settled_token_values(&HashMap::from([
            ("BTC-WIN".into(), 1.0), ("BTC-LOSE".into(), 0.0),
        ]));
        assert!(account.observe_platform_binary_redeem(
            130.0,
            &HashMap::from([("ETH-UP".into(), 20.0)]),
            &HashSet::from(["BTC-WIN".into(), "ETH-UP".into()]),
        ));
        assert_eq!(account.monitoring_snapshot().physical_cash, 130.0);
        assert_eq!(account.instance_snapshot("btc").unwrap().cash, 80.0);
        assert_eq!(account.instance_snapshot("eth").unwrap().cash, 50.0);
        assert_eq!(account.instance_snapshot("eth").unwrap().positions["ETH-UP"], 20.0);
        assert!(!account.is_uncertain());
    }

    #[test]
    fn expired_interest_is_retained_until_settled_winner_reaches_zero() {
        let account = SharedAccount::new("late-platform-redeem");
        account.register_instance("btc", 1.0);
        account
            .register_token_interest("btc", "btc-event", "BTC-WIN", "BTC-LOSE")
            .unwrap();
        account.apply_physical_snapshot(
            100.0,
            HashMap::from([("BTC-WIN".into(), 30.0)]),
        );
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
        account.state.lock().unwrap().startup_snapshot_applied_this_process = false;
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
        account.register_token_interest("btc", "live-event", "LIVE-UP", "LIVE-DOWN").unwrap();
        account.apply_physical_snapshot(100.0, HashMap::from([("LIVE-UP".into(), 30.0)]));
        account.state.lock().unwrap().startup_snapshot_applied_this_process = false;
        account.apply_physical_snapshot(130.0, HashMap::new());
        assert!(account.is_uncertain());
        assert_eq!(account.instance_snapshot("btc").unwrap().cash, 100.0);
        assert_eq!(account.instance_snapshot("btc").unwrap().positions["LIVE-UP"], 30.0);
    }

    #[test]
    fn recovered_order_uncertainty_survives_snapshot_until_terminal() {
        let account = seeded_account();
        account.reserve_order("a", "old-1", "oid-old-1", "UP", Side::Buy, 5.0, 0.5, 0).unwrap();
        account.begin_order_recovery(["old-1"]);
        assert!(account.is_uncertain());
        assert_eq!(account.monitoring_snapshot().recovery_pending_orders, 1);
        account.apply_physical_snapshot(400.0, HashMap::from([("UP".into(), 40.0)]));
        assert!(account.is_uncertain());
        account.release_order("old-1", OrderStatus::Cancelled);
        account.finish_order_recovery("old-1");
        assert!(!account.is_uncertain());
    }

    #[test]
    fn removed_config_instance_with_owned_funds_fails_closed() {
        let account = seeded_account();
        account.reconcile_configured_instances(&HashSet::from(["a".to_string()]));
        assert!(account.is_uncertain());
        account.reconcile_configured_instances(&HashSet::from([
            "a".to_string(), "b".to_string(),
        ]));
        assert!(!account.is_uncertain());
    }

    #[test]
    fn persistent_ledger_restores_ownership_orders_and_reservations() {
        let path = std::env::temp_dir().join(format!(
            "hexagent-shared-account-{}-{}.json",
            std::process::id(),
            wall_clock_ms(),
        ));
        {
            let account = SharedAccount::new_persistent("durable", &path).unwrap();
            account.register_instance("btc", 1.0);
            account.register_instance("eth", 1.0);
            account.register_token_interest("btc", "btc-event", "BTC-UP", "BTC-DOWN").unwrap();
            account.register_token_interest("eth", "eth-event", "ETH-UP", "ETH-DOWN").unwrap();
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
            account.reserve_order(
                "btc", "btc-1", "oid-btc-1", "BTC-UP", Side::Sell, 5.0, 0.5, 0,
            ).unwrap();
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
        assert_eq!(restored.order_owner_by_oid("oid-btc-1").as_deref(), Some("btc"));
        let btc = restored.instance_snapshot("btc").unwrap();
        assert_eq!(btc.positions["BTC-UP"], 20.0);
        assert_eq!(btc.reserved_positions["BTC-UP"], 5.0);
        assert_eq!(restored.instance_snapshot("eth").unwrap().positions["ETH-UP"], 30.0);
        restored
            .reserve_order(
                "btc", "btc-2", "oid-btc-2", "BTC-UP", Side::Buy, 10.0, 0.5, 0,
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
    fn duplicate_trade_context_replay_does_not_flush_unchanged_ledger() {
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
                    "a", "a-duplicate", "oid-duplicate", "UP", Side::Buy, 10.0, 0.5, 0,
                )
                .unwrap();
            account
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
                .unwrap();
            let before = account.monitoring_snapshot();
            account
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
                .unwrap();
            let after = account.monitoring_snapshot();
            assert_eq!(after.persistence_flushes, before.persistence_flushes);
            assert_eq!(after.persistence_writes, before.persistence_writes);
        }
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(PathBuf::from(lock_path));
    }
}
