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

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct InstanceAccountSnapshot {
    pub instance_id: String,
    pub weight: f64,
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
    pub recovery_pending_orders: usize,
    pub persistence_path: Option<PathBuf>,
    pub persistence_error: Option<String>,
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
    pub price: f64,
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
    /// resurrect it. Admission stays risk-off until a later authoritative
    /// wallet snapshot proves the reversed ledger is covered.
    #[serde(default)]
    failure_reconciled: bool,
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
    /// Orders restored from a previous process whose exchange terminal state
    /// has not yet been proved. These are a distinct, sticky risk-off reason:
    /// an otherwise matching wallet snapshot must not clear them.
    #[serde(default)]
    recovery_pending_orders: HashSet<String>,
    /// Persisted ownership for an instance no longer present in config cannot
    /// be silently reassigned without moving that instance's PnL/inventory.
    #[serde(default)]
    instance_registry_issue: Option<String>,
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
                        let result = write_persisted_account(&thread_path, &job.snapshot);
                        let (lock, cv) = &*thread_progress;
                        let mut state = lock.lock().unwrap();
                        state.completed_generation = state.completed_generation.max(job.generation);
                        state.last_error = result.err();
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
        })
    }

    fn schedule(&self, snapshot: PersistedAccount) {
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed) + 1;
        *self.pending.lock().unwrap() = Some(PersistJob { generation, snapshot });
        let _ = self.wake.try_send(());
    }

    fn flush(&self, timeout: Duration) -> Result<(), String> {
        let target = self.next_generation.load(Ordering::Relaxed);
        if target == 0 { return Ok(()); }
        let _ = self.wake.try_send(());
        let (lock, cv) = &*self.progress;
        let progress = lock.lock().unwrap();
        let (progress, wait) = cv
            .wait_timeout_while(progress, timeout, |p| p.completed_generation < target)
            .map_err(|_| "account ledger writer progress lock poisoned".to_string())?;
        if progress.completed_generation < target && wait.timed_out() {
            return Err(format!(
                "timed out persisting generation {target} to {}",
                self.path.display()
            ));
        }
        if let Some(error) = &progress.last_error {
            return Err(error.clone());
        }
        Ok(())
    }

    fn last_error(&self) -> Option<String> {
        self.progress.0.lock().ok().and_then(|p| p.last_error.clone())
    }
}

fn write_persisted_account(path: &Path, snapshot: &PersistedAccount) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(snapshot)
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
        .map_err(|error| format!("rename {} -> {}: {error}", tmp.display(), path.display()))
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
        let state = if path.exists() {
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
            persisted.state
        } else {
            SharedAccountState::default()
        };
        let persistence = AccountPersistence::start(path)?;
        Ok(Self {
            account_id,
            state: Mutex::new(state),
            persistence: Some(persistence),
        })
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
    pub fn register_instance(&self, instance_id: &str, weight: f64) {
        if instance_id.is_empty() { return; }
        let weight = if weight.is_finite() && weight > 0.0 { weight } else { 1.0 };
        let mut state = self.state.lock().unwrap();
        let old_total = total_weight(&state.instances);
        let seeded = state.seeded;
        state.instances
            .entry(instance_id.to_string())
            .and_modify(|instance| instance.weight = weight)
            .or_insert_with(|| InstanceLedger::new(weight));
        // If a worker fetched the first snapshot just before a sibling was
        // registered, rebalance the still-pristine startup allocation.
        if seeded && state.orders.is_empty() && state.trades.is_empty()
            && state.external_adjustments.is_empty()
        {
            let new_total = total_weight(&state.instances);
            if old_total > 0.0 && new_total > 0.0 { redistribute_all(&mut state); }
        }
        self.schedule_persist(&state);
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
        instance.token_interests.insert(condition_id.to_string(), TokenInterest {
            instance_id: instance_id.to_string(),
            condition_id: condition_id.to_string(),
            up_token_id: up_token_id.to_string(),
            down_token_id: down_token_id.to_string(),
            retire_after_ms: None,
        });
        // A pristine startup snapshot may have landed before all sibling
        // instruments were routed. Re-run only the startup allocation; once an
        // order/trade exists, ownership must never be silently rebalanced.
        if state.seeded && state.orders.is_empty() && state.trades.is_empty()
            && state.external_adjustments.is_empty()
        {
            redistribute_all(&mut state);
        }
        self.schedule_persist(&state);
        Ok(())
    }

    pub fn token_interests(&self) -> Vec<TokenInterest> {
        let mut state = self.state.lock().unwrap();
        let now_ms = wall_clock_ms();
        let mut pruned = false;
        for instance in state.instances.values_mut() {
            let before = instance.token_interests.len();
            instance.token_interests.retain(|_, interest| {
                interest.retire_after_ms.is_none_or(|deadline| deadline > now_ms)
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
                            | OrderStatus::Failed
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

    pub fn active_tokens(&self) -> HashSet<String> {
        self.token_interests().into_iter()
            .flat_map(|interest| [interest.up_token_id, interest.down_token_id])
            .collect()
    }

    /// Apply an authoritative physical snapshot. The first snapshot creates
    /// the weighted virtual allocation. Later snapshots update the hard
    /// physical ceiling; unexplained deltas remain unallocated instead of
    /// silently transferring PnL between instances.
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

    /// Apply a cash snapshot plus a token-scoped authoritative position view.
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
        let mut state = self.state.lock().unwrap();
        let cash = finite_nonnegative(cash);
        let positions = positions.into_iter()
            .filter_map(|(token, qty)| {
                let qty = finite_nonnegative(qty);
                (qty > EPS).then_some((token, qty))
            })
            .collect::<HashMap<_, _>>();
        if !state.seeded {
            state.seeded = true;
            state.physical_cash = cash;
            state.physical_positions = positions.iter()
                .filter(|(token, _)| authoritative_tokens.contains(*token))
                .map(|(token, qty)| (token.clone(), *qty))
                .collect();
            redistribute_all(&mut state);
            self.schedule_persist(&state);
            return;
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
        mark_fully_observed_pending_trades(&mut state);
        recompute_reconciliation(&mut state, "authoritative physical snapshot");
        if mark_failed_trades_reconciled_by_snapshot(&mut state, &authoritative_tokens) {
            recompute_reconciliation(&mut state, "authoritative physical snapshot");
        }
        try_attribute_binary_redeem(&mut state);
        self.schedule_persist(&state);
    }

    pub fn is_seeded(&self) -> bool { self.state.lock().unwrap().seeded }
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

    /// Attribute an externally-confirmed wallet operation to one instance.
    /// `operation_id` makes retries idempotent. The virtual delta may be
    /// recorded before or after the physical snapshot: a temporary mismatch
    /// fails admission closed and clears when the matching snapshot arrives.
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
        let Some(instance) = state.instances.get_mut(instance_id) else {
            return Err(ReservationError::UnknownInstance(instance_id.into()));
        };
        if instance.cash + cash_delta < -EPS {
            return Err(ReservationError::InvalidOrder(format!(
                "external adjustment would make instance `{instance_id}` cash negative"
            )));
        }
        for (token, delta) in &position_deltas {
            let current = instance.positions.get(token).copied().unwrap_or(0.0);
            if current + delta < -EPS {
                return Err(ReservationError::InvalidOrder(format!(
                    "external adjustment would make `{instance_id}` token `{token}` negative"
                )));
            }
        }
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
            recovery_pending_orders: state.recovery_pending_orders.len(),
            persistence_path: self.persistence.as_ref().map(|p| p.path.clone()),
            persistence_error,
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
            if existing.order_id == order_id && existing.instance_id == instance_id {
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
            price,
            reserved_cash: reserve_cash,
            reserved_quantity: reserve_qty,
            status: OrderStatus::Pending,
        };
        state.oid_to_coid.insert(order_id.into(), client_order_id.into());
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
                state.oid_to_coid.remove(&order.order_id);
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
            return Err(error);
        }
        Ok(ownership)
    }

    pub fn rebind_order_id(&self, client_order_id: &str, order_id: &str) {
        if client_order_id.is_empty() || order_id.is_empty() { return; }
        let mut state = self.state.lock().unwrap();
        let old_order_id = state.orders.get(client_order_id).map(|order| order.order_id.clone());
        if let Some(old) = old_order_id { state.oid_to_coid.remove(&old); }
        if let Some(order) = state.orders.get_mut(client_order_id) {
            order.order_id = order_id.into();
            state.oid_to_coid.insert(order_id.into(), client_order_id.into());
        }
        self.schedule_persist(&state);
    }

    pub fn order_owner_by_coid(&self, client_order_id: &str) -> Option<String> {
        self.state.lock().unwrap().orders.get(client_order_id)
            .map(|order| order.instance_id.clone())
    }

    pub fn order_owner_by_oid(&self, order_id: &str) -> Option<String> {
        let state = self.state.lock().unwrap();
        let coid = state.oid_to_coid.get(order_id)?;
        state.orders.get(coid).map(|order| order.instance_id.clone())
    }

    pub fn order(&self, client_order_id: &str) -> Option<OrderOwnership> {
        self.state.lock().unwrap().orders.get(client_order_id).cloned()
    }

    pub fn mark_order_status(&self, client_order_id: &str, status: OrderStatus) {
        let mut state = self.state.lock().unwrap();
        if let Some(order) = state.orders.get_mut(client_order_id) {
            order.status = status;
            self.schedule_persist(&state);
        }
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
        if trade_key.is_empty() || quantity <= 0.0 { return None; }
        let normalized = status.trim_start_matches("TRADE_STATUS_").to_ascii_uppercase();
        if normalized == "RETRYING" { return None; }
        let mut state = self.state.lock().unwrap();
        let resolved_coid = if !client_order_id.is_empty() {
            client_order_id.to_string()
        } else {
            state.oid_to_coid.get(order_id).cloned().unwrap_or_default()
        };
        let owner = state.orders.get(&resolved_coid).map(|order| order.instance_id.clone());
        let Some(instance_id) = owner else {
            set_uncertain(
                &mut state,
                format!(
                    "unowned trade `{trade_key}` coid=`{resolved_coid}` oid=`{order_id}`"
                ),
            );
            self.schedule_persist(&state);
            return None;
        };

        let existing = state.trades.get(trade_key).cloned();
        let already_booked = existing.as_ref().map(|trade| trade.booked).unwrap_or(false);
        let physical_booked = existing
            .as_ref()
            .map(|trade| trade.physical_booked)
            .unwrap_or(false);
        // FAILED is a terminal tombstone. A delayed/replayed earlier status
        // must never resurrect virtual inventory or physical settlement.
        if existing.as_ref().is_some_and(|trade| trade.failed) {
            return existing.map(|trade| trade.ownership);
        }
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
                    applied.ownership.status = normalized;
                }
                let ownership = applied.ownership.clone();
                self.schedule_persist(&state);
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
            let (cash_release, qty_release) = if let Some(order) = state.orders.get_mut(&resolved_coid) {
                let mut cash_release = if side == Side::Buy {
                    (quantity * order.price).min(order.reserved_cash)
                } else { 0.0 };
                let mut qty_release = if side == Side::Sell {
                    quantity.min(order.reserved_quantity)
                } else { 0.0 };
                order.reserved_cash -= cash_release;
                order.reserved_quantity -= qty_release;
                order.filled_quantity = (order.filled_quantity + quantity).min(order.quantity);
                if order.filled_quantity + EPS >= order.quantity {
                    cash_release += order.reserved_cash;
                    qty_release += order.reserved_quantity;
                    order.reserved_cash = 0.0;
                    order.reserved_quantity = 0.0;
                    order.status = OrderStatus::Filled;
                    order_fully_filled = true;
                } else {
                    order.status = OrderStatus::PartiallyFilled;
                }
                (cash_release, qty_release)
            } else { (0.0, 0.0) };
            if let Some(instance) = state.instances.get_mut(&instance_id) {
                instance.reserved_cash = (instance.reserved_cash - cash_release).max(0.0);
                if qty_release > 0.0 {
                    let reserved = instance.reserved_positions.entry(token_id.into()).or_insert(0.0);
                    *reserved = (*reserved - qty_release).max(0.0);
                }
            }
        }
        if is_failed {
            // A failed settlement reverses the fill, but does not prove the
            // remaining order is gone. Restore the reversed leg's lock, keep
            // the remaining reservation, and
            // halt new account admission until the next physical snapshot
            // reconciles the wallet. Releasing here could let a sibling spend
            // collateral/inventory that the exchange still considers locked.
            let relock = if let Some(order) = state.orders.get_mut(&resolved_coid) {
                let cash = if side == Side::Buy { quantity * order.price } else { 0.0 };
                let qty = if side == Side::Sell { quantity } else { 0.0 };
                order.reserved_cash += cash;
                order.reserved_quantity += qty;
                order.filled_quantity = (order.filled_quantity - quantity).max(0.0);
                order.status = OrderStatus::Failed;
                (cash, qty, order.token_id.clone())
            } else { (0.0, 0.0, token_id.to_string()) };
            if let Some(instance) = state.instances.get_mut(&instance_id) {
                instance.reserved_cash += relock.0;
                if relock.1 > 0.0 {
                    *instance.reserved_positions.entry(relock.2).or_insert(0.0) += relock.1;
                }
            }
            set_uncertain(
                &mut state,
                format!("trade `{trade_key}` entered FAILED and requires physical reconciliation"),
            );
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
            failure_reconciled: !is_failed
                && existing
                    .as_ref()
                    .is_some_and(|trade| trade.failure_reconciled),
        });
        recompute_reconciliation(&mut state, "trade lifecycle transition");
        self.schedule_persist(&state);
        Some(ownership)
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
        let is_failed = status == OrderStatus::Failed || existing.failed;
        let book_virtual = !is_failed && !existing.virtual_fee_booked;
        let reverse_virtual = is_failed && existing.virtual_fee_booked;
        let book_physical = !is_failed && existing.physical_booked && !existing.physical_fee_booked;
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
        mark_fully_observed_pending_trades(&mut state);
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

    /// Bound the durable per-event ownership history after the executor's
    /// late-fill mapping grace has elapsed. Potentially-live/FAILED orders and
    /// nonterminal trades are retained; only fully terminal rows for the
    /// retired token scope are removed.
    pub fn prune_terminal_history(&self, tokens: &HashSet<String>) -> (usize, usize) {
        if tokens.is_empty() {
            return (0, 0);
        }
        let mut state = self.state.lock().unwrap();
        let stale_orders: Vec<(String, String)> = state
            .orders
            .iter()
            .filter(|(coid, order)| {
                tokens.contains(&order.token_id)
                    && !state.recovery_pending_orders.contains(*coid)
                    && order.reserved_cash <= EPS
                    && order.reserved_quantity <= EPS
                    && matches!(
                        order.status,
                        OrderStatus::Cancelled | OrderStatus::Rejected | OrderStatus::Filled
                    )
            })
            .map(|(coid, order)| (coid.clone(), order.order_id.clone()))
            .collect();
        for (coid, oid) in &stale_orders {
            state.orders.remove(coid);
            state.oid_to_coid.remove(oid);
        }
        let before_trades = state.trades.len();
        state.trades.retain(|_, trade| {
            !tokens.contains(&trade.ownership.token_id)
                || (!trade.failed && trade.ownership.status != "CONFIRMED")
        });
        let pruned_trades = before_trades - state.trades.len();
        if !stale_orders.is_empty() || pruned_trades > 0 {
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

/// A snapshot can observe an on-chain settlement just before its MINED /
/// CONFIRMED user-feed edge arrives. In the common all-or-none case, recognize
/// that the wallet already equals the virtual ledger and mark every pending
/// base delta physical. The later lifecycle edge then advances status without
/// applying the same cash/token delta twice.
fn mark_fully_observed_pending_trades(state: &mut SharedAccountState) {
    let pending_tokens: HashSet<String> = state
        .trades
        .values()
        .filter(|trade| {
            trade.booked
                && !trade.failed
                && (!trade.physical_booked
                    || (trade.virtual_fee_booked && !trade.physical_fee_booked))
        })
        .map(|trade| trade.ownership.token_id.clone())
        .collect();
    if pending_tokens.is_empty() {
        return;
    }
    let mut aggregate_cash_delta = 0.0;
    let mut aggregate_position_deltas = HashMap::<String, f64>::new();
    for trade in state.trades.values().filter(|trade| {
        trade.booked
            && !trade.failed
            && (!trade.physical_booked
                || (trade.virtual_fee_booked && !trade.physical_fee_booked))
    }) {
        if !trade.physical_booked {
            let sign = if trade.ownership.side == Side::Buy { 1.0 } else { -1.0 };
            aggregate_cash_delta +=
                -sign * trade.ownership.quantity * trade.ownership.price;
            *aggregate_position_deltas
                .entry(trade.ownership.token_id.clone())
                .or_insert(0.0) += sign * trade.ownership.quantity;
        }
        if trade.virtual_fee_booked && !trade.physical_fee_booked {
            aggregate_cash_delta -= trade.usdc_fee;
            *aggregate_position_deltas
                .entry(trade.ownership.token_id.clone())
                .or_insert(0.0) -= trade.shares_fee;
        }
    }
    // Aggregate-equal opposing pending fills contain no evidence that either
    // individual lifecycle reached the wallet. Marking both physical would
    // make a later FAILED reversal mutate physical state that never changed.
    if aggregate_cash_delta.abs() <= EPS
        && aggregate_position_deltas.values().all(|delta| delta.abs() <= EPS)
    {
        return;
    }
    let virtual_cash: f64 = state.instances.values().map(|instance| instance.cash).sum();
    if (state.physical_cash - virtual_cash).abs() > EPS {
        return;
    }
    let all_tokens_match = pending_tokens.iter().all(|token| {
        let physical = state.physical_positions.get(token).copied().unwrap_or(0.0);
        let virtual_qty: f64 = state
            .instances
            .values()
            .map(|instance| instance.positions.get(token).copied().unwrap_or(0.0))
            .sum();
        (physical - virtual_qty).abs() <= EPS
    });
    if !all_tokens_match {
        return;
    }
    for trade in state.trades.values_mut() {
        if trade.booked && !trade.physical_booked && !trade.failed {
            trade.physical_booked = true;
        }
        if trade.virtual_fee_booked && !trade.failed {
            trade.physical_fee_booked = true;
        }
    }
}

/// A FAILED tombstone may release risk-off only after an authoritative wallet
/// snapshot covers its token and shows no physical deficit versus the virtual
/// ledger. The tombstone itself remains for replay protection.
fn mark_failed_trades_reconciled_by_snapshot(
    state: &mut SharedAccountState,
    authoritative_tokens: &HashSet<String>,
) -> bool {
    if state.unallocated_cash < -EPS {
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
        if token_delta >= -EPS {
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
    for token in all_tokens {
        let physical = state.physical_positions.get(&token).copied().unwrap_or(0.0);
        let virtual_qty: f64 = state.instances.values()
            .map(|instance| instance.positions.get(&token).copied().unwrap_or(0.0))
            .sum();
        let pending = pending_position_deltas.get(&token).copied().unwrap_or(0.0);
        let delta = physical - (virtual_qty - pending);
        if delta.abs() > EPS {
            state.unallocated_positions.insert(token, delta);
        }
    }
    let negative_tokens: Vec<String> = state.unallocated_positions.iter()
        .filter(|(_, qty)| **qty < -EPS)
        .map(|(token, qty)| format!("{token}:{qty:.6}"))
        .collect();
    if let Some(reason) = state.instance_registry_issue.clone() {
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
                "startup order recovery pending: count={} coids=[{}]",
                pending.len(),
                pending.join(","),
            ),
        );
    } else if let Some((trade_key, _)) = state
        .trades
        .iter()
        .find(|(_, trade)| trade.failed && !trade.failure_reconciled)
    {
        set_uncertain(
            state,
            format!("failed trade `{trade_key}` awaits authoritative physical snapshot"),
        );
    } else if state.unallocated_cash < -EPS || !negative_tokens.is_empty() {
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
            "trade:oid-a", "CONFIRMED", "a-1", "oid-a", "UP", Side::Buy, 10.0, 0.5,
        );
        assert_eq!(account.instance_snapshot("a").unwrap().cash, cash);
        let confirmed = account.monitoring_snapshot();
        assert_eq!(confirmed.physical_cash, 395.0);
        assert_eq!(confirmed.physical_positions["UP"], 50.0);
        assert!(!confirmed.uncertain);
        account.apply_trade_transition(
            "trade:oid-a", "FAILED", "a-1", "oid-a", "UP", Side::Buy, 10.0, 0.5,
        );
        assert_eq!(account.instance_snapshot("a").unwrap().cash, 100.0);
        let failed = account.monitoring_snapshot();
        assert_eq!(failed.physical_cash, 400.0);
        assert_eq!(failed.physical_positions["UP"], 40.0);
        assert!(
            failed.uncertain,
            "FAILED remains risk-off until a later authoritative wallet snapshot"
        );
        account.apply_physical_snapshot(400.0, HashMap::from([("UP".into(), 40.0)]));
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
                "CONFIRMED",
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
            OrderStatus::Filled,
            0.0,
            0.2,
        ));
        let confirmed = account.monitoring_snapshot();
        assert!((confirmed.physical_positions["UP"] - 49.8).abs() < EPS);
        assert!(!confirmed.uncertain);

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
    fn external_adjustment_is_idempotent_and_clears_after_matching_snapshot() {
        let account = SharedAccount::new("external");
        account.register_instance("a", 1.0);
        account.register_instance("b", 1.0);
        account.apply_physical_snapshot(100.0, HashMap::new());
        account.attribute_external_adjustment("deposit-1", "a", 20.0, HashMap::new()).unwrap();
        account.attribute_external_adjustment("deposit-1", "a", 20.0, HashMap::new()).unwrap();
        assert_eq!(account.instance_snapshot("a").unwrap().cash, 70.0);
        assert!(account.is_uncertain(), "virtual deposit precedes physical confirmation");

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
    fn equal_cash_and_token_delta_is_not_redeem_without_settlement_proof() {
        let account = SharedAccount::new("ordinary-sale");
        account.register_instance("btc", 1.0);
        account.register_token_interest("btc", "live-event", "LIVE-UP", "LIVE-DOWN").unwrap();
        account.apply_physical_snapshot(100.0, HashMap::from([("LIVE-UP".into(), 30.0)]));
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
            account.apply_physical_snapshot(
                200.0,
                HashMap::from([("BTC-UP".into(), 20.0), ("ETH-UP".into(), 30.0)]),
            );
            account.reserve_order(
                "btc", "btc-1", "oid-btc-1", "BTC-UP", Side::Sell, 5.0, 0.5, 0,
            ).unwrap();
            account.flush_persistence(Duration::from_secs(2)).unwrap();
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
        drop(restored);
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(PathBuf::from(lock_path));
    }
}
