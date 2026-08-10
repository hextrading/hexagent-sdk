//! Account-scoped physical/virtual bookkeeping for shared-wallet strategies.
//!
//! One [`SharedAccount`] is owned by one exchange account. It is the
//! admission-control source of truth shared by every strategy instance on the
//! wallet: physical funds/positions are the hard ceiling, while each
//! instance's weighted virtual balance/inventory is its private ceiling.

use hexagent_types::types::{OrderStatus, Side};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

const EPS: f64 = 1e-9;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct InstanceAccountSnapshot {
    pub instance_id: String,
    pub weight: f64,
    pub cash: f64,
    pub positions: HashMap<String, f64>,
    pub reserved_cash: f64,
    pub reserved_positions: HashMap<String, f64>,
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

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone)]
struct InstanceLedger {
    weight: f64,
    cash: f64,
    positions: HashMap<String, f64>,
    reserved_cash: f64,
    reserved_positions: HashMap<String, f64>,
}

impl InstanceLedger {
    fn new(weight: f64) -> Self {
        Self {
            weight,
            cash: 0.0,
            positions: HashMap::new(),
            reserved_cash: 0.0,
            reserved_positions: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct AppliedTrade {
    ownership: TradeOwnership,
    booked: bool,
    failed: bool,
}

#[derive(Debug, Default)]
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
}

/// Thread-safe account ledger shared by every strategy instance on one wallet.
#[derive(Debug)]
pub struct SharedAccount {
    account_id: String,
    state: Mutex<SharedAccountState>,
}

impl SharedAccount {
    pub fn new(account_id: impl Into<String>) -> Self {
        Self {
            account_id: account_id.into(),
            state: Mutex::new(SharedAccountState::default()),
        }
    }

    pub fn account_id(&self) -> &str { &self.account_id }

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
        if seeded && state.orders.is_empty() && state.trades.is_empty() {
            let new_total = total_weight(&state.instances);
            if old_total > 0.0 && new_total > 0.0 { redistribute_all(&mut state); }
        }
    }

    /// Apply an authoritative physical snapshot. The first snapshot creates
    /// the weighted virtual allocation. Later snapshots update the hard
    /// physical ceiling; unexplained deltas remain unallocated instead of
    /// silently transferring PnL between instances.
    pub fn apply_physical_snapshot(&self, cash: f64, positions: HashMap<String, f64>) {
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
            state.physical_positions = positions;
            redistribute_all(&mut state);
            return;
        }

        let virtual_cash: f64 = state.instances.values().map(|instance| instance.cash).sum();
        state.physical_cash = cash;
        state.unallocated_cash = cash - virtual_cash;

        let mut all_tokens: std::collections::HashSet<String> =
            state.physical_positions.keys().cloned().collect();
        all_tokens.extend(positions.keys().cloned());
        for token in all_tokens {
            let physical = positions.get(&token).copied().unwrap_or(0.0);
            let virtual_qty: f64 = state.instances.values()
                .map(|instance| instance.positions.get(&token).copied().unwrap_or(0.0))
                .sum();
            let delta = physical - virtual_qty;
            if delta.abs() > EPS {
                state.unallocated_positions.insert(token.clone(), delta);
            } else {
                state.unallocated_positions.remove(&token);
            }
        }
        state.physical_positions = positions;
        state.uncertain = state.unallocated_cash < -EPS
            || state.unallocated_positions.values().any(|qty| *qty < -EPS);
    }

    pub fn is_seeded(&self) -> bool { self.state.lock().unwrap().seeded }
    pub fn is_uncertain(&self) -> bool { self.state.lock().unwrap().uncertain }
    pub fn mark_uncertain(&self) { self.state.lock().unwrap().uncertain = true; }

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
        let state = self.state.lock().unwrap();
        let instance = state.instances.get(instance_id)?;
        // A negative unexplained reconciliation delta means the physical
        // wallet is below the sum of the virtual ledgers. Fail closed until
        // reconciliation instead of letting one instance consume another's
        // allocation.
        if state.uncertain {
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
        let mut state = self.state.lock().unwrap();
        if !state.seeded { return Err(ReservationError::AccountNotSeeded); }
        if state.uncertain { return Err(ReservationError::AccountUncertain); }
        if let Some(existing) = state.orders.get(client_order_id) {
            if existing.order_id == order_id && existing.instance_id == instance_id {
                return Ok(existing.clone());
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
        if let Some(order) = self.state.lock().unwrap().orders.get_mut(client_order_id) {
            order.status = status;
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
        Ok(())
    }

    pub fn release_split_allocations(&self, allocations: &HashMap<String, f64>) {
        let mut state = self.state.lock().unwrap();
        for (instance_id, amount) in allocations {
            if let Some(instance) = state.instances.get_mut(instance_id) {
                instance.reserved_cash = (instance.reserved_cash - *amount).max(0.0);
            }
        }
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
                state.uncertain = true;
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
            state.uncertain = true;
            return None;
        };

        let existing = state.trades.get(trade_key).cloned();
        let already_booked = existing.as_ref().map(|trade| trade.booked).unwrap_or(false);
        let already_failed = existing.as_ref().map(|trade| trade.failed).unwrap_or(false);
        let is_failed = normalized == "FAILED";
        if already_failed || (!is_failed && already_booked) {
            if let Some(applied) = state.trades.get_mut(trade_key) {
                if !applied.failed {
                    applied.ownership.status = normalized;
                }
                return Some(applied.ownership.clone());
            }
        }

        let should_book = !is_failed && !already_booked;
        let should_reverse = is_failed && already_booked;
        if should_book || should_reverse {
            let sign = if side == Side::Buy { 1.0 } else { -1.0 };
            let cash_delta = -sign * quantity * price;
            let position_delta = sign * quantity;
            let multiplier = if should_reverse { -1.0 } else { 1.0 };
            state.physical_cash += cash_delta * multiplier;
            *state.physical_positions.entry(token_id.into()).or_insert(0.0) +=
                position_delta * multiplier;
            if let Some(instance) = state.instances.get_mut(&instance_id) {
                instance.cash += cash_delta * multiplier;
                *instance.positions.entry(token_id.into()).or_insert(0.0) +=
                    position_delta * multiplier;
            }
        }
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
            state.uncertain = true;
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
            failed: is_failed,
        });
        Some(ownership)
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
}

fn finite_nonnegative(value: f64) -> f64 {
    if value.is_finite() { value.max(0.0) } else { 0.0 }
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
        instance.positions = state.physical_positions.iter()
            .map(|(token, qty)| (token.clone(), qty * fraction))
            .collect();
    }
    state.unallocated_cash = 0.0;
    state.unallocated_positions.clear();
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
        account.apply_trade_transition(
            "trade:oid-a", "CONFIRMED", "a-1", "oid-a", "UP", Side::Buy, 10.0, 0.5,
        );
        assert_eq!(account.instance_snapshot("a").unwrap().cash, cash);
        account.apply_trade_transition(
            "trade:oid-a", "FAILED", "a-1", "oid-a", "UP", Side::Buy, 10.0, 0.5,
        );
        assert_eq!(account.instance_snapshot("a").unwrap().cash, 100.0);
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
}
