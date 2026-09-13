//! Immutable GC delta captured by the cold account owner. JSON paths and
//! serialization belong exclusively to the existing bounded WAL writer.
use super::*;

#[derive(Debug, Clone)]
pub(super) struct SettledPrunePersistenceDelta {
    outcomes: Vec<PruneRows>,
    // Typed economic summary, never the complete account/history snapshot.
    compacted: Option<AccountEconomicState>,
    audits: Vec<(String, Option<SettledAuditReference>)>,
}

#[derive(Debug, Clone)]
struct PruneRows {
    orders: Vec<PrunedOrder>,
    trades: Vec<PrunedTrade>,
    expired_tombstones: Vec<String>,
    fees: Vec<(String, Option<TokenFeeConfig>)>,
}
#[derive(Debug, Clone)]
struct PrunedOrder {
    coid: String,
    order: Option<OrderOwnership>,
    oid: String,
    mapped_coid: Option<String>,
    recovery: bool,
    startup: bool,
    audit: bool,
}
#[derive(Debug, Clone)]
struct PrunedTrade {
    key: String,
    trade: Option<AppliedTrade>,
    tombstone: Option<RetiredTradeOwnershipTombstone>,
    fee_pending: bool,
}

impl SettledPrunePersistenceDelta {
    pub(super) fn capture(
        state: &SharedAccountState,
        outcomes: &[SettledPruneOutcome],
        conditions: &[String],
    ) -> Self {
        Self {
            outcomes: outcomes
                .iter()
                .map(|outcome| PruneRows {
                    orders: outcome
                        .orders
                        .iter()
                        .map(|(coid, oid)| {
                            let oid = normalize_order_id(oid);
                            PrunedOrder {
                                coid: coid.clone(),
                                order: state.orders.get(coid).cloned(),
                                mapped_coid: state.oid_to_coid.get(&oid).cloned(),
                                oid,
                                recovery: state.recovery_pending_orders.contains(coid),
                                startup: state.startup_query_repair_orders.contains(coid),
                                audit: state.routine_cancel_audits.contains(coid),
                            }
                        })
                        .collect(),
                    trades: outcome
                        .trades
                        .iter()
                        .map(|key| PrunedTrade {
                            key: key.clone(),
                            trade: state.trades.get(key).cloned(),
                            tombstone: state.retired_trade_ownership_tombstones.get(key).cloned(),
                            fee_pending: state.fee_attribution_pending.contains(key),
                        })
                        .collect(),
                    expired_tombstones: outcome.expired_tombstones.clone(),
                    fees: outcome
                        .fee_tokens
                        .iter()
                        .map(|key| (key.clone(), state.token_fee_configs.get(key).cloned()))
                        .collect(),
                })
                .collect(),
            compacted: outcomes
                .iter()
                .any(|outcome| !outcome.trades.is_empty())
                .then(|| state.compacted_economic_effects.clone()),
            audits: conditions
                .iter()
                .map(|key| {
                    (
                        key.clone(),
                        state.settled_audit_references.get(key).cloned(),
                    )
                })
                .collect(),
        }
    }

    pub(super) fn materialize(&self) -> Result<Vec<PersistenceWalChange>, String> {
        let mut changes = Vec::new();
        for outcome in &self.outcomes {
            for order in &outcome.orders {
                persistence_wal_map_entry(
                    &mut changes,
                    "orders",
                    &order.coid,
                    order.order.as_ref(),
                )?;
                persistence_wal_map_entry(
                    &mut changes,
                    "oid_to_coid",
                    &order.oid,
                    order.mapped_coid.as_ref(),
                )?;
                for (set, present) in [
                    ("recovery_pending_orders", order.recovery),
                    ("startup_query_repair_orders", order.startup),
                    ("routine_cancel_audits", order.audit),
                ] {
                    persistence_wal_set_membership(&mut changes, set, &order.coid, present)?;
                }
            }
            for trade in &outcome.trades {
                persistence_wal_map_entry(
                    &mut changes,
                    "trades",
                    &trade.key,
                    trade.trade.as_ref(),
                )?;
                persistence_wal_map_entry(
                    &mut changes,
                    "retired_trade_ownership_tombstones",
                    &trade.key,
                    trade.tombstone.as_ref(),
                )?;
                persistence_wal_set_membership(
                    &mut changes,
                    "fee_attribution_pending",
                    &trade.key,
                    trade.fee_pending,
                )?;
            }
            for key in &outcome.expired_tombstones {
                persistence_wal_map_entry::<RetiredTradeOwnershipTombstone>(
                    &mut changes,
                    "retired_trade_ownership_tombstones",
                    key,
                    None,
                )?;
            }
            for (key, fee) in &outcome.fees {
                persistence_wal_map_entry(&mut changes, "token_fee_configs", key, fee.as_ref())?;
            }
        }
        if let Some(compacted) = &self.compacted {
            persistence_wal_set(
                &mut changes,
                ["compacted_economic_effects".to_string()],
                compacted,
            )?;
        }
        for (key, audit) in &self.audits {
            persistence_wal_map_entry(
                &mut changes,
                "settled_audit_references",
                key,
                audit.as_ref(),
            )?;
        }
        Ok(changes)
    }
}
