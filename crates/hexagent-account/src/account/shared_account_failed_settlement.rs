//! Offline correction for an authenticated CONFIRMED trade that an older
//! binary had permanently treated as FAILED. Receipt verification is performed
//! by the caller. The exact durable identity and economic preconditions are
//! rechecked here, and the whole correction is validated before publication.
use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailedSettlementCashRepair {
    pub expected: TradeOwnership,
    pub transaction_hash: String,
    pub cash_received: f64,
    pub expected_physical_cash: f64,
    pub expected_instance_cash: f64,
}

impl SharedAccount {
    /// Only supports fully redeemed zero-value SELL outcomes. Other outcomes
    /// need a separate settlement correction, never a guessed cash transfer.
    pub fn repair_failed_settlement_cash(
        &self,
        proof: &FailedSettlementCashRepair,
    ) -> Result<bool, String> {
        if self.account_owner_lane_bound.load(Ordering::Acquire)
            || self.account_lifecycle_lane_bound.load(Ordering::Acquire)
        {
            return Err("failed settlement repair requires an offline account".into());
        }
        let expected = &proof.expected;
        let tx = proof.transaction_hash.strip_prefix("0x").unwrap_or("");
        if tx.len() != 64
            || !tx.bytes().all(|b| b.is_ascii_hexdigit())
            || expected.account_id != self.account_id
            || expected.status != "FAILED"
            || expected.side != Side::Sell
            || !proof.cash_received.is_finite()
            || proof.cash_received <= 0.0
            || proof.cash_received > expected.quantity * expected.price + 1e-6
            || !proof.expected_physical_cash.is_finite()
            || !proof.expected_instance_cash.is_finite()
        {
            return Err("invalid failed settlement receipt identity/economics".into());
        }
        let operation = format!(
            "confirmed-after-failed:{}:{}",
            proof.transaction_hash, expected.trade_key
        );
        let mut state = self.lock_state_for_persistence();
        let tombstone = state
            .retired_trade_ownership_tombstones
            .get(&expected.trade_key)
            .ok_or("failed trade has no retained ownership tombstone")?;
        if let Some(adjustment) = state.external_adjustments.get(&operation) {
            let mut confirmed = expected.clone();
            confirmed.status = "CONFIRMED".into();
            if adjustment.instance_id == expected.instance_id
                && adjustment.cash_delta == proof.cash_received
                && adjustment.position_deltas.is_empty()
                && tombstone.ownership == confirmed
            {
                return Ok(false);
            }
            return Err("settlement receipt conflicts with existing correction".into());
        }
        let instance = state
            .instances
            .get(&expected.instance_id)
            .ok_or("unknown receipt owner")?;
        let order = state
            .orders
            .get(&expected.client_order_id)
            .ok_or("missing parent order")?;
        if !state.seeded
            || tombstone.ownership != *expected
            || tombstone.authenticated_terminal_noop
            || state.trades.contains_key(&expected.trade_key)
            || !trade_ownership_matches_order_root(expected, order)
            || !order.terminal_trade_ids_authoritative
            || !order
                .terminal_trade_ids
                .iter()
                .any(|id| terminal_trade_id_matches(&expected.trade_key, id))
            || order.reserved_cash != 0.0
            || order.reserved_quantity != 0.0
            || (state.physical_cash - proof.expected_physical_cash).abs() > EPS
            || (instance.cash - proof.expected_instance_cash).abs() > EPS
            || proof.cash_received > state.unallocated_cash + EPS
            || state.settled_token_values.get(&expected.token_id) != Some(&0.0)
            || instance
                .positions
                .get(&expected.token_id)
                .copied()
                .unwrap_or(0.0)
                .abs()
                > EPS
            || state
                .physical_positions
                .get(&expected.token_id)
                .copied()
                .unwrap_or(0.0)
                .abs()
                > EPS
            || pending_physical_deltas_from_trades(state.trades.values()).unsettled
            || has_unsettled_maintenance_operation(&state)
        {
            return Err(
                "failed settlement correction preconditions changed or remain unsettled".into(),
            );
        }
        let mut candidate = state.clone();
        // Both the real token balance and its settlement are already zero.
        // Record the net post-settlement cash effect, never re-create shares.
        candidate
            .instances
            .get_mut(&expected.instance_id)
            .unwrap()
            .cash += proof.cash_received;
        candidate.external_adjustments.insert(
            operation.clone(),
            ExternalAdjustment {
                operation_id: operation,
                instance_id: expected.instance_id.clone(),
                cash_delta: proof.cash_received,
                position_deltas: HashMap::new(),
                recorded_at_ms: wall_clock_ms(),
            },
        );
        candidate
            .retired_trade_ownership_tombstones
            .get_mut(&expected.trade_key)
            .unwrap()
            .ownership
            .status = "CONFIRMED".into();
        let order = candidate.orders.get_mut(&expected.client_order_id).unwrap();
        order.filled_quantity += expected.quantity;
        if order.filled_quantity > order.quantity + EPS
            || order
                .terminal_matched_quantity
                .is_none_or(|matched| order.filled_quantity > matched + EPS)
        {
            return Err("confirmed receipt exceeds parent cumulative fill audit".into());
        }
        order.status = if order.filled_quantity + EPS >= order.quantity {
            OrderStatus::Filled
        } else {
            OrderStatus::Cancelled
        };
        candidate
            .recovery_pending_orders
            .remove(&expected.client_order_id);
        candidate
            .startup_query_repair_orders
            .remove(&expected.client_order_id);
        candidate
            .routine_cancel_audits
            .remove(&expected.client_order_id);
        let key = format!("trade:{}", expected.trade_key);
        if candidate
            .ownership_anomalies
            .get(&key)
            .is_some_and(|reason| reason.starts_with("confirmed_after_failed_retirement"))
        {
            candidate.ownership_anomalies.remove(&key);
        }
        recompute_reconciliation(&mut candidate, "confirmed receipt after failed settlement");
        validate_persisted_state(&self.account_id, &candidate)?;
        *state = candidate;
        self.schedule_persist(&state);
        Ok(true)
    }
}
