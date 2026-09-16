//! Single-lane event scheduler for sim_v2.
//!
//! A min-heap keyed by `(when, deadline_priority, seq)`. `when` is wall-clock ns; `seq` is a
//! monotonic insertion counter giving a deterministic FIFO tiebreak at equal
//! `when` (so a v2 run is byte-reproducible given the same RNG seed). The
//! The simulator owns one instance for the server lane and one for the
//! strategy-delivery lane. Event payload rides inside the heap item; `Ord`
//! compares the scheduling key only, so `SimEvent` itself need not be `Ord`.
//! Explicit evidence invalidations precede ordinary work at the same time;
//! deadline timers run after ordinary replies. Legacy relative ordering is unchanged.

use std::collections::BinaryHeap;

use super::event::SimEvent;

struct HeapItem {
    when: u64,
    seq: u64,
    deadline_priority: u8,
    ev: SimEvent,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.when == other.when
            && self.deadline_priority == other.deadline_priority
            && self.seq == other.seq
    }
}
impl Eq for HeapItem {}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // BinaryHeap is a max-heap; invert so the smallest (when, seq) pops
        // first.
        (other.when, other.deadline_priority, other.seq).cmp(&(
            self.when,
            self.deadline_priority,
            self.seq,
        ))
    }
}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Default)]
pub struct Scheduler {
    heap: BinaryHeap<HeapItem>,
    next_seq: u64,
}

impl Scheduler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Schedule `ev` to fire at wall-clock time `when`.
    pub fn push(&mut self, when: u64, ev: SimEvent) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.heap.push(HeapItem {
            when,
            seq,
            deadline_priority: 1,
            ev,
        });
    }

    /// A response exactly on its deadline wins; normal equal-time events keep
    /// their existing FIFO ordering. Only request-deadline timers use this lane.
    pub fn push_deadline(&mut self, when: u64, ev: SimEvent) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.heap.push(HeapItem {
            when,
            seq,
            deadline_priority: 2,
            ev,
        });
    }

    /// Explicit gap/reconnect invalidation precedes feed and order work at
    /// the same time. Used only by an opt-in offline evidence journal.
    pub fn push_invalidation(&mut self, when: u64, ev: SimEvent) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.heap.push(HeapItem {
            when,
            seq,
            deadline_priority: 0,
            ev,
        });
    }

    pub fn peek_is_invalidation(&self) -> bool {
        self.heap
            .peek()
            .is_some_and(|item| item.deadline_priority == 0)
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }
    pub fn capacity(&self) -> usize {
        self.heap.capacity()
    }

    /// Wall-clock time of the next event, without consuming it.
    pub fn peek_when(&self) -> Option<u64> {
        self.heap.peek().map(|h| h.when)
    }

    /// Pop the earliest `(when, ev)`.
    pub fn pop(&mut self) -> Option<(u64, SimEvent)> {
        self.heap.pop().map(|h| (h.when, h.ev))
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Exchange, OrderStatus, OrderUpdate, Side};

    fn dummy_ack(coid: &str) -> SimEvent {
        SimEvent::AckToStrategy(OrderUpdate {
            client_order_id: coid.to_string(),
            exchange: Exchange::Polymarket,
            symbol: "t".into(),
            side: Side::Buy,
            exchange_order_id: None,
            status: OrderStatus::Accepted,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: 0.0,
            avg_fill_price: 0.0,
            timestamp_ns: 0,
            exchange_event_timestamp_ns: None,
            trade_id: None,
            trade_fee: None,
            order_audit: None,
            error: None,
            order_slot: Default::default(),
        })
    }

    fn coid_of(ev: &SimEvent) -> String {
        match ev {
            SimEvent::AckToStrategy(u) => u.client_order_id.clone(),
            _ => String::new(),
        }
    }

    #[test]
    fn heap_orders_by_when() {
        let mut s = Scheduler::new();
        s.push(300, dummy_ack("c"));
        s.push(100, dummy_ack("a"));
        s.push(200, dummy_ack("b"));
        let order: Vec<u64> = std::iter::from_fn(|| s.pop()).map(|(w, _)| w).collect();
        assert_eq!(order, vec![100, 200, 300]);
    }

    #[test]
    fn heap_stable_tiebreak_by_seq() {
        let mut s = Scheduler::new();
        // Same `when`; must pop in insertion order.
        s.push(100, dummy_ack("first"));
        s.push(100, dummy_ack("second"));
        s.push(100, dummy_ack("third"));
        let order: Vec<String> = std::iter::from_fn(|| s.pop())
            .map(|(_, e)| coid_of(&e))
            .collect();
        assert_eq!(order, vec!["first", "second", "third"]);
    }

    #[test]
    fn peek_does_not_consume() {
        let mut s = Scheduler::new();
        s.push(42, dummy_ack("x"));
        assert_eq!(s.peek_when(), Some(42));
        assert_eq!(s.peek_when(), Some(42));
        assert!(s.pop().is_some());
        assert_eq!(s.peek_when(), None);
        assert!(s.is_empty());
    }
}
