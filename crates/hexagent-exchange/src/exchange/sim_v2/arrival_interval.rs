//! Retrospective arrival-window diagnostics. This observer never schedules an
//! order and its future observations never feed back into exchange admission.
//! Bounds supplied by synthetic RTT are explicitly modeled, not venue evidence.
use crate::types::OrderRequest;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};

pub const ARRIVAL_INTERVAL_CAPACITY: usize = 16_384;

#[derive(Clone, Debug, Serialize)]
pub struct ArrivalIntervalRow {
    pub coid: String,
    pub iid: String,
    pub token: String,
    pub lower_ns: u64,
    pub upper_ns: u64,
    pub selected_ns: u64,
    pub evidence: &'static str,
    pub observations: u64,
    pub crossing_observations: u64,
    pub noncrossing_observations: u64,
    pub unknown_observations: u64,
    pub classification: &'static str,
}

struct Pending {
    order: OrderRequest,
    row: ArrivalIntervalRow,
}

#[derive(Default, Clone, Copy, Debug, Serialize)]
pub struct ArrivalIntervalStats {
    pub emitted: u64,
    pub observed_crossing_only: u64,
    pub observed_noncrossing_only: u64,
    pub observed_flip: u64,
    pub incomplete_or_unknown: u64,
    pub high_water: usize,
    pub pending: usize,
    pub queued: usize,
    pub capacity: usize,
}

pub struct ArrivalIntervalAudit {
    enabled: bool,
    pending: HashMap<String, Pending>,
    rows: VecDeque<ArrivalIntervalRow>,
    stats: ArrivalIntervalStats,
}

impl ArrivalIntervalAudit {
    pub fn new(enabled: bool) -> Self {
        let capacity = if enabled {
            ARRIVAL_INTERVAL_CAPACITY
        } else {
            0
        };
        Self {
            enabled,
            pending: HashMap::with_capacity(capacity),
            rows: VecDeque::with_capacity(capacity),
            stats: ArrivalIntervalStats {
                capacity,
                ..Default::default()
            },
        }
    }
    pub fn enabled(&self) -> bool {
        self.enabled
    }
    pub fn start(
        &mut self,
        order: &OrderRequest,
        lower: u64,
        upper: u64,
        selected: u64,
        evidence: &'static str,
    ) {
        if !self.enabled {
            return;
        }
        assert!(
            lower <= selected && selected <= upper,
            "arrival diagnostic selected point outside modeled interval"
        );
        assert!(
            self.pending.len() < ARRIVAL_INTERVAL_CAPACITY
                && !self.pending.contains_key(&order.client_order_id),
            "arrival diagnostic capacity/identity violation"
        );
        let row = ArrivalIntervalRow {
            coid: order.client_order_id.clone(),
            iid: order.instance_id.clone(),
            token: order.symbol.clone(),
            lower_ns: lower,
            upper_ns: upper,
            selected_ns: selected,
            evidence,
            observations: 0,
            crossing_observations: 0,
            noncrossing_observations: 0,
            unknown_observations: 0,
            classification: "incomplete_or_unknown",
        };
        self.pending.insert(
            order.client_order_id.clone(),
            Pending {
                order: order.clone(),
                row,
            },
        );
        self.stats.high_water = self.stats.high_water.max(self.pending.len());
    }
    /// Only sample at distinct server updates and once at start/end. The labels
    /// mean observed consistency, never verified continuous venue coverage.
    pub fn observe(
        &mut self,
        now: u64,
        mut crossing: impl FnMut(&OrderRequest, u64) -> Option<bool>,
    ) {
        if !self.enabled {
            return;
        }
        self.pending.retain(|_, p| {
            if now < p.row.lower_ns {
                return true;
            }
            if now <= p.row.upper_ns {
                p.row.observations += 1;
                match crossing(&p.order, now) {
                    Some(true) => p.row.crossing_observations += 1,
                    Some(false) => p.row.noncrossing_observations += 1,
                    None => p.row.unknown_observations += 1,
                }
            }
            if now < p.row.upper_ns {
                return true;
            }
            p.row.classification = if p.row.observations == 0 || p.row.unknown_observations > 0 {
                self.stats.incomplete_or_unknown += 1;
                "incomplete_or_unknown"
            } else if p.row.crossing_observations > 0 && p.row.noncrossing_observations > 0 {
                self.stats.observed_flip += 1;
                "observed_flip"
            } else if p.row.crossing_observations > 0 {
                self.stats.observed_crossing_only += 1;
                "observed_crossing_only"
            } else {
                self.stats.observed_noncrossing_only += 1;
                "observed_noncrossing_only"
            };
            assert!(
                self.rows.len() < ARRIVAL_INTERVAL_CAPACITY,
                "arrival diagnostic rows must be drained outside callbacks"
            );
            self.rows.push_back(p.row.clone());
            self.stats.emitted += 1;
            false
        });
    }
    pub fn drain(&mut self) -> std::collections::vec_deque::Drain<'_, ArrivalIntervalRow> {
        self.rows.drain(..)
    }
    pub fn finish(&mut self) {
        for (_, mut p) in self.pending.drain() {
            assert!(
                self.rows.len() < ARRIVAL_INTERVAL_CAPACITY,
                "arrival diagnostic rows must be drained before finish"
            );
            p.row.classification = "incomplete_or_unknown";
            self.rows.push_back(p.row);
            self.stats.emitted += 1;
            self.stats.incomplete_or_unknown += 1;
        }
    }
    pub fn stats(&self) -> ArrivalIntervalStats {
        ArrivalIntervalStats {
            pending: self.pending.len(),
            queued: self.rows.len(),
            ..self.stats
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Exchange, Side};
    fn order(id: &str) -> OrderRequest {
        let mut o =
            OrderRequest::new_limit(Exchange::Polymarket, "yes".into(), Side::Buy, 0.5, 10.0);
        o.client_order_id = id.into();
        o.instance_id = "a".into();
        o
    }
    #[test]
    fn interval_flips_and_unknowns_are_distinct() {
        let mut a = ArrivalIntervalAudit::new(true);
        a.start(&order("1"), 10, 20, 15, "modeled_rtt_budget");
        a.observe(10, |_, _| Some(false));
        a.observe(15, |_, _| Some(true));
        a.observe(20, |_, _| Some(true));
        assert_eq!(a.drain().next().unwrap().classification, "observed_flip");
        a.start(&order("2"), 21, 30, 25, "client_interval_unaligned");
        a.observe(22, |_, _| None);
        a.observe(30, |_, _| Some(false));
        assert_eq!(
            a.drain().next().unwrap().classification,
            "incomplete_or_unknown"
        );
        assert_eq!(a.stats().emitted, 2);
    }
    #[test]
    fn disabled_observer_does_not_collect_or_call_book_reader() {
        let mut a = ArrivalIntervalAudit::new(false);
        a.start(&order("1"), 0, 20, 10, "modeled");
        a.observe(10, |_, _| panic!("disabled"));
        assert_eq!(a.stats().pending, 0);
    }
}
