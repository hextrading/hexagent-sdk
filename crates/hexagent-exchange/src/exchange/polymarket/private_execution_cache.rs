//! Single-writer private delivery economics. Allocated before the worker runs;
//! no shared mutable account access, I/O, or growing container on lookup.
use super::*;
use hexagent_account::account::shared_account::{FrozenTradeExecution, PrivateExecutionSeed};

const CAPACITY: usize = 32_768;

#[derive(Debug, Clone)]
struct CachedExecution {
    identity: u128,
    quantity: f64,
    side: Side,
    is_maker: bool,
    execution: Option<FrozenTradeExecution>,
    terminal: bool,
    queued: bool,
    repair_delivery_pending: bool,
    repair_ownership: Option<Box<hexagent_account::account::shared_account::TradeOwnership>>,
    ticket: ExecutionAckTicket,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ExecutionAckTicket {
    slot: usize,
    generation: u64,
}

/// Per-account completion message slots, allocated before the worker starts.
/// Cold writer publishes only monotonic generations; private owner reads them.
#[derive(Debug, Clone)]
pub(super) struct ExecutionAckLane(Arc<[AtomicU64]>);

impl ExecutionAckLane {
    pub(super) fn acknowledge(&self, ticket: ExecutionAckTicket) {
        self.0[ticket.slot].fetch_max(ticket.generation, Ordering::Release);
    }
}

#[derive(Debug)]
pub(super) struct PrivateExecutionCache {
    rows: HashMap<u128, CachedExecution>,
    reclaimable: VecDeque<u128>,
    acknowledgements: ExecutionAckLane,
    free_slots: Vec<usize>,
    generation: u64,
}

fn identity(order: &str, token: &str) -> u128 {
    private_route_fingerprint(
        b'e',
        &[
            order
                .trim()
                .trim_start_matches("0x")
                .trim_start_matches("0X"),
            token,
        ],
    )
}

fn key(trade: &str, order: &str, is_maker: bool) -> u128 {
    if is_maker {
        private_route_fingerprint(b'm', &[trade, order])
    } else {
        private_route_fingerprint(b't', &[trade])
    }
}

impl PrivateExecutionCache {
    pub(super) fn new(seeds: Vec<PrivateExecutionSeed>) -> Result<Self, String> {
        if seeds.len() > CAPACITY {
            return Err("private execution startup seed exceeds fixed capacity".into());
        }
        let mut this = Self {
            rows: HashMap::with_capacity(CAPACITY * 2),
            reclaimable: VecDeque::with_capacity(CAPACITY),
            acknowledgements: ExecutionAckLane(
                (0..CAPACITY)
                    .map(|_| AtomicU64::new(0))
                    .collect::<Vec<_>>()
                    .into(),
            ),
            free_slots: (0..CAPACITY).rev().collect(),
            generation: 0,
        };
        for seed in seeds {
            let trade = &seed.ownership;
            let venue_trade = if seed.is_maker {
                trade
                    .trade_key
                    .split(':')
                    .next()
                    .unwrap_or(&trade.trade_key)
            } else {
                &trade.trade_key
            };
            let key = key(venue_trade, &trade.order_id, seed.is_maker);
            let terminal = matches!(trade.status.as_str(), "CONFIRMED" | "FAILED");
            this.generation += 1;
            let ticket = ExecutionAckTicket {
                slot: this.free_slots.pop().unwrap(),
                generation: this.generation,
            };
            this.rows.insert(
                key,
                CachedExecution {
                    identity: identity(&trade.order_id, &trade.token_id),
                    quantity: trade.quantity,
                    side: trade.side,
                    is_maker: seed.is_maker,
                    execution: seed.execution,
                    terminal,
                    queued: terminal && seed.execution.is_some(),
                    repair_delivery_pending: false,
                    repair_ownership: None,
                    ticket,
                },
            );
            if terminal && seed.execution.is_some() {
                this.reclaimable.push_back(key);
            }
        }
        Ok(this)
    }

    pub(super) fn lookup(
        &self,
        trade: &str,
        order: &str,
        token: &str,
        side: Side,
        quantity: f64,
        raw_price: f64,
        is_maker: bool,
    ) -> Result<Option<FrozenTradeExecution>, String> {
        let Some(row) = self.rows.get(&key(trade, order, is_maker)) else {
            return Ok(None);
        };
        if row.identity != identity(order, token)
            || row.side != side
            || row.is_maker != is_maker
            || (row.quantity - quantity).abs() > quantity.abs().max(1.0) * 1e-8
        {
            return Err("private frozen trade identity changed".into());
        }
        let execution = row
            .execution
            .ok_or_else(|| "historical private trade fee is not attributed".to_string())?;
        if execution.gross_notional.is_none()
            && (execution.raw_price - raw_price).abs() > 1e-7
            && (execution.price - raw_price).abs() > 1e-7
        {
            return Err("private frozen raw execution price changed".into());
        }
        Ok(Some(execution))
    }

    pub(super) fn insert(
        &mut self,
        trade: &str,
        order: &str,
        token: &str,
        side: Side,
        quantity: f64,
        is_maker: bool,
        execution: FrozenTradeExecution,
    ) -> Result<(), String> {
        let key = key(trade, order, is_maker);
        if self.rows.contains_key(&key) {
            return Ok(());
        }
        if self.rows.len() >= CAPACITY && self.reclaimable.is_empty() {
            // Advisory ACK may overflow; exact completion slots retain proof.
            // A scan is bounded and occurs only at the capacity boundary.
            for (&key, row) in self.rows.iter_mut() {
                if !row.queued
                    && row.execution.is_some()
                    && !row.repair_delivery_pending
                    && self.acknowledgements.0[row.ticket.slot].load(Ordering::Acquire)
                        == row.ticket.generation
                {
                    row.terminal = true;
                    row.queued = true;
                    self.reclaimable.push_back(key);
                }
            }
        }
        while self.rows.len() >= CAPACITY {
            let Some(old) = self.reclaimable.pop_front() else {
                return Err(
                    "private execution capacity exhausted by unacknowledged/nonterminal trades"
                        .into(),
                );
            };
            if self
                .rows
                .get(&old)
                .is_some_and(|row| row.terminal && row.queued && !row.repair_delivery_pending)
            {
                if let Some(row) = self.rows.remove(&old) {
                    self.free_slots.push(row.ticket.slot);
                }
            } else if let Some(row) = self.rows.get_mut(&old) {
                row.queued = false;
            }
        }
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| "private execution ticket generation exhausted".to_string())?;
        let ticket = ExecutionAckTicket {
            slot: self.free_slots.pop().expect("fixed table has free slot"),
            generation: self.generation,
        };
        self.rows.insert(
            key,
            CachedExecution {
                identity: identity(order, token),
                quantity,
                side,
                is_maker,
                execution: Some(execution),
                terminal: false,
                queued: false,
                repair_delivery_pending: false,
                repair_ownership: None,
                ticket,
            },
        );
        Ok(())
    }

    pub(super) fn acknowledge(&mut self, identity: PrivateRouteIdentity) {
        let PrivateRouteIdentity::TradeLifecycle { fingerprint, rank } = identity;
        if rank < 3 {
            return;
        }
        if let Some(row) = self.rows.get_mut(&fingerprint) {
            row.terminal = true;
            if !row.queued && !row.repair_delivery_pending && row.execution.is_some() {
                row.queued = true;
                self.reclaimable.push_back(fingerprint);
            }
        }
    }
    pub(super) fn ack_lane(&self) -> ExecutionAckLane {
        self.acknowledgements.clone()
    }

    pub(super) fn ticket(&self, identity: PrivateRouteIdentity) -> Option<ExecutionAckTicket> {
        let PrivateRouteIdentity::TradeLifecycle { fingerprint, .. } = identity;
        self.rows.get(&fingerprint).map(|row| row.ticket)
    }

    pub(super) fn needs_repair(&self, trade: &str, order: &str, maker: bool) -> bool {
        self.rows
            .get(&key(trade, order, maker))
            .is_some_and(|row| row.execution.is_none())
    }

    pub(super) fn needs_delivery(&self, trade: &str, order: &str, is_maker: bool) -> bool {
        self.rows
            .get(&key(trade, order, is_maker))
            .is_some_and(|row| row.repair_delivery_pending)
    }

    pub(super) fn repair_ownership(
        &self,
        trade: &str,
        order: &str,
    ) -> Option<&hexagent_account::account::shared_account::TradeOwnership> {
        self.rows
            .get(&key(trade, order, false))?
            .repair_ownership
            .as_deref()
    }

    pub(super) fn repair_instance(&self, identity: PrivateRouteIdentity) -> Option<&str> {
        let PrivateRouteIdentity::TradeLifecycle { fingerprint, .. } = identity;
        Some(
            &self
                .rows
                .get(&fingerprint)?
                .repair_ownership
                .as_ref()?
                .instance_id,
        )
    }

    pub(super) fn mark_delivered(&mut self, identity: PrivateRouteIdentity) {
        let PrivateRouteIdentity::TradeLifecycle { fingerprint, .. } = identity;
        if let Some(row) = self.rows.get_mut(&fingerprint) {
            row.repair_delivery_pending = false;
            row.repair_ownership = None;
            if row.terminal && !row.queued && row.execution.is_some() {
                row.queued = true;
                self.reclaimable.push_back(fingerprint);
            }
        }
    }

    pub(super) fn repair(&mut self, seed: PrivateExecutionSeed) -> Result<(), String> {
        let trade = &seed.ownership;
        let venue_trade = if seed.is_maker {
            trade
                .trade_key
                .split(':')
                .next()
                .unwrap_or(&trade.trade_key)
        } else {
            &trade.trade_key
        };
        let Some(execution) = seed.execution else {
            return Err("historical private fee proof remains pending".into());
        };
        let key = key(venue_trade, &trade.order_id, seed.is_maker);
        if let Some(row) = self.rows.get_mut(&key) {
            if row.identity != identity(&trade.order_id, &trade.token_id)
                || row.side != trade.side
                || (row.quantity - trade.quantity).abs() > trade.quantity.abs().max(1.0) * 1e-8
            {
                return Err("private cold fee repair changed identity".into());
            }
            if let Some(old) = row.execution {
                if old != execution {
                    return Err("private cold fee repair changed frozen economics".into());
                }
            } else {
                if row.execution.is_none() {
                    row.repair_delivery_pending = true;
                    row.repair_ownership = Some(Box::new(trade.clone()));
                }
                row.execution = Some(execution);
            }
            Ok(())
        } else {
            self.insert(
                venue_trade,
                &trade.order_id,
                &trade.token_id,
                trade.side,
                trade.quantity,
                seed.is_maker,
                execution,
            )
        }
    }
}

#[cfg(test)]
#[path = "private_execution_cache_tests.rs"]
mod tests;
