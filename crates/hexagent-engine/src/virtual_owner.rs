//! Deterministic, backtest-only owner lanes. No wall clocks, threads or sleeps.
//!
//! The replay coordinator is the sole writer. Market snapshots replace an
//! earlier pending snapshot only within one barrier epoch; private events are
//! FIFO and higher priority. Every lane is preallocated and bounded. Overflow
//! fails the offline run instead of silently losing an execution event. A
//! callback takes its input when service starts, then completes after the
//! configured service time. Messages received during service cannot replace
//! that input or interrupt the callback. These durations are MODEL inputs,
//! not measurements reconstructed from market timestamps.
use anyhow::{ensure, Result};
use serde::Serialize;
use std::collections::VecDeque;

#[derive(Clone, Copy, Debug)]
pub(crate) struct OwnerScheduleConfig {
    pub apply_delay_ns: u64,
    pub service_time_ns: u64,
    pub capacity: usize,
    pub watchdog_interval_ns: u64,
    pub start_ns: u64,
    pub end_ns: u64,
}

struct Pending<K, T> {
    received_ns: u64,
    ready_ns: u64,
    epoch: u64,
    key: Option<K>,
    payload: T,
}

pub(crate) enum OwnerWork<T> {
    Event(T),
    Watchdog,
}

pub(crate) struct OwnerCompletion<T> {
    pub owner: usize,
    pub received_ns: u64,
    pub apply_ns: u64,
    pub completed_ns: u64,
    pub work: OwnerWork<T>,
    private_lane: bool,
    history_lane: bool,
}

struct Owner<K, T> {
    market: VecDeque<Pending<K, T>>,
    private: VecDeque<Pending<K, T>>,
    history: Option<Pending<K, T>>,
    running: Option<OwnerCompletion<T>>,
    epoch: u64,
    next_watchdog_ns: u64,
    last_watchdog_ns: u64,
    available_ns: u64,
    paused_until_ns: u64,
    stats: OwnerStats,
}

#[derive(Default, Serialize)]
pub(crate) struct OwnerStats {
    received: u64,
    completed: u64,
    coalesced_market: u64,
    retired_market: u64,
    offline_market_discarded: u64,
    superseded_history: u64,
    watchdogs: u64,
    queue_high_water: usize,
    overflow: u64,
    receive_to_apply: TimingHistogram,
    apply_to_completion: TimingHistogram,
}

/// Fixed 64 buckets per power of two. Quantiles are bucket UPPER BOUNDS
/// (at most 1/64 of the relevant power of two wider); maxima are exact.
struct TimingHistogram {
    buckets: Box<[u64; 4096]>,
    count: u64,
    maximum_ns: u64,
}
impl Default for TimingHistogram {
    fn default() -> Self {
        Self {
            buckets: Box::new([0; 4096]),
            count: 0,
            maximum_ns: 0,
        }
    }
}
impl TimingHistogram {
    fn record(&mut self, ns: u64) {
        let exponent = 63 - ns.max(1).leading_zeros() as usize;
        let base = 1u64 << exponent;
        let fraction = (((ns.saturating_sub(base)) as u128 * 64) / base as u128) as usize;
        self.buckets[exponent * 64 + fraction.min(63)] += 1;
        self.count += 1;
        self.maximum_ns = self.maximum_ns.max(ns);
    }
    fn upper_quantile(&self, numerator: u64, denominator: u64) -> u64 {
        if self.count == 0 || self.maximum_ns == 0 {
            return 0;
        }
        let target =
            ((self.count as u128 * numerator as u128).div_ceil(denominator as u128)) as u64;
        let mut cumulative = 0;
        for (index, count) in self.buckets.iter().enumerate() {
            cumulative += count;
            if cumulative >= target {
                let base = 1u128 << (index / 64);
                let upper = (base + (base * ((index % 64 + 1) as u128)).div_ceil(64) - 1)
                    .min(u64::MAX as u128) as u64;
                return upper.min(self.maximum_ns);
            }
        }
        self.maximum_ns
    }
}
impl Serialize for TimingHistogram {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("TimingHistogram", 5)?;
        s.serialize_field("count", &self.count)?;
        s.serialize_field("median_ns_upper", &self.upper_quantile(1, 2))?;
        s.serialize_field("p99_ns_upper", &self.upper_quantile(99, 100))?;
        s.serialize_field("p999_ns_upper", &self.upper_quantile(999, 1000))?;
        s.serialize_field("maximum_ns", &self.maximum_ns)?;
        s.end()
    }
}

pub(crate) struct VirtualOwnerScheduler<K, T> {
    config: OwnerScheduleConfig,
    owners: Vec<Owner<K, T>>,
}

impl<K: PartialEq, T> VirtualOwnerScheduler<K, T> {
    pub fn new(count: usize, config: OwnerScheduleConfig) -> Result<Self> {
        ensure!(
            config.capacity > 0,
            "virtual owner capacity must be positive"
        );
        ensure!(
            config.end_ns >= config.start_ns,
            "virtual owner end precedes start"
        );
        let owners = (0..count)
            .map(|_| Owner {
                market: VecDeque::with_capacity(config.capacity),
                private: VecDeque::with_capacity(config.capacity),
                history: None,
                running: None,
                epoch: 0,
                next_watchdog_ns: if config.watchdog_interval_ns == 0 {
                    u64::MAX
                } else {
                    config.start_ns.saturating_add(config.watchdog_interval_ns)
                },
                last_watchdog_ns: config.start_ns,
                available_ns: config.start_ns,
                paused_until_ns: 0,
                stats: OwnerStats::default(),
            })
            .collect();
        Ok(Self { config, owners })
    }

    pub fn enqueue_market(
        &mut self,
        owner: usize,
        received_ns: u64,
        key: Option<K>,
        payload: T,
    ) -> Result<()> {
        let state = &mut self.owners[owner];
        state.stats.received += 1;
        if let Some(ref key) = key {
            // Stop at a barrier, exactly as the live latest-value lane does.
            if let Some(pending) = state
                .market
                .iter_mut()
                .rev()
                .take_while(|p| p.epoch == state.epoch)
                .find(|p| p.key.as_ref() == Some(key))
            {
                pending.payload = payload;
                pending.received_ns = received_ns;
                // The queued marker retains its eligibility. The replacement
                // is already received before service starts, so no future data
                // can be read even if the marker was queued earlier.
                state.stats.coalesced_market += 1;
                return Ok(());
            }
        } else {
            state.epoch = state.epoch.wrapping_add(1);
        }
        if state.market.len() == self.config.capacity {
            state.stats.overflow += 1;
            anyhow::bail!(
                "virtual owner {owner} market lane overflow (capacity {}); run invalid",
                self.config.capacity
            );
        }
        state.market.push_back(Pending {
            received_ns,
            ready_ns: received_ns.saturating_add(self.config.apply_delay_ns),
            epoch: state.epoch,
            key,
            payload,
        });
        state.stats.queue_high_water = state
            .stats
            .queue_high_water
            .max(state.market.len() + state.private.len());
        Ok(())
    }

    pub fn enqueue_private(&mut self, owner: usize, received_ns: u64, payload: T) -> Result<()> {
        let state = &mut self.owners[owner];
        state.stats.received += 1;
        if state.private.len() == self.config.capacity {
            state.stats.overflow += 1;
            anyhow::bail!("virtual owner {owner} private lane overflow (capacity {}); lossless replay cannot continue", self.config.capacity);
        }
        state.private.push_back(Pending {
            received_ns,
            ready_ns: received_ns.saturating_add(self.config.apply_delay_ns),
            epoch: state.epoch,
            key: None,
            payload,
        });
        state.stats.queue_high_water = state
            .stats
            .queue_high_water
            .max(state.market.len() + state.private.len());
        Ok(())
    }

    /// Recovery metadata must precede buffered books for its new token generation.
    /// Existing metadata callbacks are removed by the caller before this prepend.
    pub fn prepend_market(&mut self, owner: usize, received_ns: u64, payload: T) -> Result<()> {
        let state = &mut self.owners[owner];
        if state.market.len() == self.config.capacity {
            state.stats.overflow += 1;
            anyhow::bail!("virtual owner {owner} recovery metadata lane overflow; run invalid");
        }
        state.epoch = state.epoch.wrapping_add(1);
        state.market.push_front(Pending {
            received_ns,
            ready_ns: received_ns.saturating_add(self.config.apply_delay_ns),
            epoch: state.epoch,
            key: None,
            payload,
        });
        state.stats.queue_high_water = state
            .stats
            .queue_high_water
            .max(state.market.len() + state.private.len());
        Ok(())
    }

    pub fn retire_owner_market(&mut self, owner: usize, mut predicate: impl FnMut(&T) -> bool) {
        let state = &mut self.owners[owner];
        let before = state.market.len();
        state.market.retain(|pending| !predicate(&pending.payload));
        state.stats.retired_market += (before - state.market.len()) as u64;
        state.epoch = state.epoch.wrapping_add(1);
    }

    /// Includes callbacks already in service; applying a message, not merely
    /// receiving it, is what advances the owner checkpoint.
    pub fn private_matches(&self, owner: usize, mut predicate: impl FnMut(&T) -> bool) -> bool {
        let state = &self.owners[owner];
        state
            .private
            .iter()
            .any(|pending| predicate(&pending.payload))
            || state.running.as_ref().is_some_and(|running| {
                running.private_lane
                    && match &running.work {
                        OwnerWork::Event(payload) => predicate(payload),
                        OwnerWork::Watchdog => false,
                    }
            })
    }

    /// Latest history job replaces a pending completion from a superseded
    /// instrument generation. It never overwrites an already running callback.
    pub fn schedule_history(&mut self, owner: usize, ready_ns: u64, payload: T) {
        let state = &mut self.owners[owner];
        if state.history.is_some() {
            state.stats.superseded_history += 1;
        }
        state.history = Some(Pending {
            received_ns: ready_ns,
            ready_ns,
            epoch: state.epoch,
            key: None,
            payload,
        });
    }

    /// Router-control retirement invalidates old generation snapshots without
    /// discarding private lifecycle messages for still-live exchange orders.
    pub fn retire_market(&mut self, mut should_retire: impl FnMut(&T) -> bool) {
        for state in &mut self.owners {
            state.epoch = state.epoch.wrapping_add(1);
            let before = state.market.len();
            state.market.retain(|p| !should_retire(&p.payload));
            state.stats.retired_market += (before - state.market.len()) as u64;
        }
    }

    fn ready_for(&self, state: &Owner<K, T>) -> u64 {
        if state.paused_until_ns == u64::MAX {
            return u64::MAX;
        }
        if let Some(running) = &state.running {
            return running.completed_ns.max(state.paused_until_ns);
        }
        let market = state.market.front().map_or(u64::MAX, |p| p.ready_ns);
        let private = state.private.front().map_or(u64::MAX, |p| p.ready_ns);
        let history = state.history.as_ref().map_or(u64::MAX, |p| p.ready_ns);
        let watchdog = if state.next_watchdog_ns <= self.config.end_ns {
            if market != u64::MAX {
                state
                    .next_watchdog_ns
                    .max(state.last_watchdog_ns.saturating_add(500_000_000))
            } else {
                state.next_watchdog_ns
            }
        } else {
            u64::MAX
        };
        market
            .min(private)
            .min(history)
            .min(watchdog)
            .max(state.available_ns)
            .max(state.paused_until_ns)
    }

    pub fn peek_when(&self) -> Option<u64> {
        self.owners
            .iter()
            .map(|o| self.ready_for(o))
            .min()
            .filter(|ts| *ts != u64::MAX)
    }

    /// Must be called only AFTER all replay ingress at `now_ns` was enqueued.
    /// Returns None when a nonzero service interval has just started.
    pub fn step(&mut self, now_ns: u64) -> Option<OwnerCompletion<T>> {
        let owner = self
            .owners
            .iter()
            .enumerate()
            .filter(|(_, s)| self.ready_for(s) <= now_ns)
            .min_by_key(|(i, s)| (self.ready_for(s), *i))
            .map(|(i, _)| i)?;
        let state = &mut self.owners[owner];
        if let Some(completion) = state.running.take() {
            state.available_ns = now_ns;
            state.stats.completed += 1;
            state
                .stats
                .apply_to_completion
                .record(completion.completed_ns - completion.apply_ns);
            return Some(completion);
        }
        let private_lane = state.private.front().is_some_and(|p| p.ready_ns <= now_ns);
        let history_lane =
            !private_lane && state.history.as_ref().is_some_and(|p| p.ready_ns <= now_ns);
        let pending = if private_lane {
            state.private.pop_front()
        } else if history_lane {
            state.history.take()
        } else if state.next_watchdog_ns <= self.config.end_ns
            && state.next_watchdog_ns <= now_ns
            && (state.market.is_empty()
                || now_ns.saturating_sub(state.last_watchdog_ns) >= 500_000_000)
        {
            None
        } else {
            state.market.pop_front()
        };
        let (received_ns, work) = if let Some(pending) = pending {
            (pending.received_ns, OwnerWork::Event(pending.payload))
        } else {
            let received = state.next_watchdog_ns;
            state.last_watchdog_ns = now_ns;
            // Periodic ticks coalesce while the owner is busy, as a live tick
            // channel does. Never create an unbounded catch-up timer burst.
            let missed = now_ns.saturating_sub(state.next_watchdog_ns)
                / self.config.watchdog_interval_ns.max(1);
            state.next_watchdog_ns = state
                .next_watchdog_ns
                .saturating_add((missed + 1).saturating_mul(self.config.watchdog_interval_ns));
            state.stats.watchdogs += 1;
            (received, OwnerWork::Watchdog)
        };
        state
            .stats
            .receive_to_apply
            .record(now_ns.saturating_sub(received_ns));
        let completion = OwnerCompletion {
            owner,
            received_ns,
            apply_ns: now_ns,
            completed_ns: now_ns.saturating_add(self.config.service_time_ns),
            work,
            private_lane,
            history_lane,
        };
        if self.config.service_time_ns == 0 {
            state.available_ns = now_ns;
            state.stats.completed += 1;
            state.stats.apply_to_completion.record(0);
            Some(completion)
        } else {
            state.running = Some(completion);
            None
        }
    }

    /// Snapshot unapplied market inputs into an offline archive query before
    /// suspension clears them. An in-service callback has not mutated state.
    pub fn visit_unapplied_market(
        &self,
        owner: usize,
        mut visit: impl FnMut(&T) -> Result<()>,
    ) -> Result<()> {
        let state = &self.owners[owner];
        for pending in &state.market {
            visit(&pending.payload)?;
        }
        if let Some(running) = &state.running {
            if !running.private_lane && !running.history_lane {
                if let OwnerWork::Event(payload) = &running.work {
                    visit(payload)?;
                }
            }
        }
        Ok(())
    }

    /// Warm checkpoint stop: the callback has not run until completion, so an
    /// in-service private message can safely return to the head of its lane.
    /// Market transients are discarded; account mutation stays frozen. The
    /// single history job is part of the warm checkpoint, retaining its original
    /// request cutoff/generation so same-event restart can still become ready.
    pub fn suspend_owner(&mut self, owner: usize, now_ns: u64) -> Result<()> {
        let state = &mut self.owners[owner];
        state.paused_until_ns = u64::MAX;
        state.epoch = state.epoch.wrapping_add(1);
        state.stats.offline_market_discarded += state.market.len() as u64;
        state.market.clear();
        if let Some(running) = state.running.take() {
            if running.private_lane {
                ensure!(
                    state.private.len() < self.config.capacity,
                    "virtual owner {owner} private recovery spool overflow; replay invalid"
                );
                if let OwnerWork::Event(payload) = running.work {
                    state.private.push_front(Pending {
                        received_ns: running.received_ns,
                        ready_ns: now_ns,
                        epoch: state.epoch,
                        key: None,
                        payload,
                    });
                }
            } else if running.history_lane {
                if state.history.is_none() {
                    if let OwnerWork::Event(payload) = running.work {
                        state.history = Some(Pending {
                            received_ns: running.received_ns,
                            ready_ns: now_ns,
                            epoch: state.epoch,
                            key: None,
                            payload,
                        });
                    }
                } else {
                    state.stats.superseded_history += 1;
                }
            } else {
                state.stats.offline_market_discarded += 1;
            }
        }
        Ok(())
    }

    /// Messages buffered during a process outage are delivered by recovery at
    /// the query completion boundary, not at their hypothetical live receipt.
    /// Event/trade identity and exchange-event time remain in the payload.
    pub fn rebase_private_recovery_receive(
        &mut self,
        owner: usize,
        now_ns: u64,
        mut update: impl FnMut(&mut T, u64),
    ) {
        for pending in &mut self.owners[owner].private {
            pending.received_ns = now_ns;
            pending.ready_ns = now_ns.saturating_add(self.config.apply_delay_ns);
            update(&mut pending.payload, now_ns);
        }
    }

    pub fn resume_owner(&mut self, owner: usize, now_ns: u64) {
        let state = &mut self.owners[owner];
        state.paused_until_ns = 0;
        state.available_ns = now_ns;
        state.last_watchdog_ns = now_ns;
        state.next_watchdog_ns = if self.config.watchdog_interval_ns == 0 {
            u64::MAX
        } else {
            now_ns.saturating_add(self.config.watchdog_interval_ns)
        };
    }

    pub fn discard_offline_market(&mut self, owner: usize) {
        self.owners[owner].stats.offline_market_discarded += 1;
    }

    pub fn private_drained(&self, owner: usize) -> bool {
        let state = &self.owners[owner];
        state.private.is_empty() && !state.running.as_ref().is_some_and(|c| c.private_lane)
    }

    pub fn stats(&self) -> Vec<&OwnerStats> {
        self.owners.iter().map(|o| &o.stats).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn scheduler(count: usize, capacity: usize) -> VirtualOwnerScheduler<u8, u32> {
        VirtualOwnerScheduler::new(
            count,
            OwnerScheduleConfig {
                apply_delay_ns: 10,
                service_time_ns: 0,
                capacity,
                watchdog_interval_ns: 0,
                start_ns: 0,
                end_ns: 1000,
            },
        )
        .unwrap()
    }
    fn event(s: &mut VirtualOwnerScheduler<u8, u32>, when: u64) -> (usize, u32) {
        let c = s.step(when).unwrap();
        match c.work {
            OwnerWork::Event(v) => (c.owner, v),
            _ => panic!("unexpected timer"),
        }
    }
    #[test]
    fn fixed_private_watermark_includes_in_service_but_not_newer_owner_messages() {
        let mut s = scheduler(2, 8);
        s.config.service_time_ns = 5;
        s.enqueue_private(0, 0, 1).unwrap();
        s.enqueue_private(0, 1, 2).unwrap();
        s.enqueue_private(1, 20, 1).unwrap();
        assert!(s.private_matches(0, |id| *id < 2));
        assert!(s.step(10).is_none());
        assert!(
            s.private_matches(0, |id| *id < 2),
            "receiving/starting service is not application"
        );
        assert_eq!(event(&mut s, 15), (0, 1));
        assert!(!s.private_matches(0, |id| *id < 2));
        assert!(
            !s.private_drained(0),
            "ongoing new messages may remain after query catchup"
        );
        assert!(s.private_matches(1, |id| *id < 2), "owner isolation");
    }

    #[test]
    fn bursts_coalesce_without_crossing_trade_or_reconnect_barriers() {
        let mut s = scheduler(1, 8);
        s.enqueue_market(0, 0, Some(1), 1).unwrap();
        s.enqueue_market(0, 1, Some(1), 2).unwrap();
        s.enqueue_market(0, 2, None, 3).unwrap();
        s.enqueue_market(0, 3, Some(1), 4).unwrap();
        s.enqueue_market(0, 4, Some(1), 5).unwrap();
        assert_eq!(event(&mut s, 10), (0, 2));
        assert_eq!(event(&mut s, 12), (0, 3));
        assert_eq!(event(&mut s, 13), (0, 5));
        assert_eq!(s.owners[0].stats.coalesced_market, 2);
    }
    #[test]
    fn equal_timestamp_private_priority_is_fifo_and_owner_isolated() {
        let mut s = scheduler(2, 8);
        s.enqueue_market(0, 0, Some(1), 1).unwrap();
        s.enqueue_market(1, 0, Some(1), 2).unwrap();
        s.enqueue_private(0, 0, 3).unwrap();
        s.enqueue_private(0, 0, 4).unwrap();
        assert_eq!(event(&mut s, 10), (0, 3));
        assert_eq!(event(&mut s, 10), (0, 4));
        assert_eq!(event(&mut s, 10), (0, 1));
        assert_eq!(event(&mut s, 10), (1, 2));
    }
    #[test]
    fn callbacks_cannot_consume_future_replacements_or_preempt_private() {
        let mut s = scheduler(1, 8);
        s.config.service_time_ns = 20;
        s.enqueue_market(0, 0, Some(1), 1).unwrap();
        assert!(s.step(10).is_none());
        s.enqueue_market(0, 11, Some(1), 2).unwrap();
        s.enqueue_private(0, 12, 3).unwrap();
        assert_eq!(event(&mut s, 30), (0, 1));
        assert!(s.step(30).is_none());
        assert_eq!(event(&mut s, 50), (0, 3));
        assert!(s.step(50).is_none());
        assert_eq!(event(&mut s, 70), (0, 2));
    }
    #[test]
    fn every_lane_is_bounded_and_private_overflow_is_explicit() {
        let mut s = scheduler(1, 1);
        s.enqueue_market(0, 0, Some(1), 1).unwrap();
        s.enqueue_market(0, 1, Some(1), 2).unwrap();
        assert!(s.enqueue_market(0, 1, None, 3).is_err());
        s.enqueue_private(0, 1, 4).unwrap();
        assert!(s.enqueue_private(0, 1, 5).is_err());
        assert_eq!(event(&mut s, 11), (0, 4));
    }
    #[test]
    fn quiet_market_timer_and_history_completion_are_independent() {
        let mut s = scheduler(1, 8);
        s.config.watchdog_interval_ns = 100;
        s.owners[0].next_watchdog_ns = 100;
        assert_eq!(s.peek_when(), Some(100));
        assert!(matches!(s.step(100).unwrap().work, OwnerWork::Watchdog));
        s.schedule_history(0, 150, 1);
        s.schedule_history(0, 160, 2);
        assert_eq!(s.peek_when(), Some(160));
        assert_eq!(event(&mut s, 160), (0, 2));
        assert_eq!(s.owners[0].stats.superseded_history, 1);
        assert_eq!(s.peek_when(), Some(200));
    }
    #[test]
    fn generation_retirement_keeps_private_orders() {
        let mut s = scheduler(1, 8);
        s.enqueue_market(0, 0, Some(1), 1).unwrap();
        s.enqueue_private(0, 0, 2).unwrap();
        s.retire_market(|v| *v == 1);
        assert_eq!(event(&mut s, 10), (0, 2));
        assert_eq!(s.peek_when(), None);
    }
    #[test]
    fn same_event_warm_restart_retains_pending_and_in_service_history() {
        let mut pending = scheduler(1, 8);
        pending.schedule_history(0, 40, 7);
        pending.suspend_owner(0, 10).unwrap();
        assert_eq!(pending.peek_when(), None);
        pending.resume_owner(0, 50);
        assert_eq!(event(&mut pending, 50), (0, 7));
        assert_eq!(pending.peek_when(), None);

        let mut running = scheduler(1, 8);
        running.config.service_time_ns = 20;
        running.schedule_history(0, 0, 7);
        assert!(running.step(0).is_none());
        running.suspend_owner(0, 5).unwrap();
        assert_eq!(running.peek_when(), None);
        running.resume_owner(0, 30);
        assert!(running.step(30).is_none());
        assert_eq!(event(&mut running, 50), (0, 7));
        assert_eq!(running.peek_when(), None);

        running.schedule_history(0, 60, 8);
        assert!(running.step(60).is_none());
        running.schedule_history(0, 70, 9); // a newer generation already won
        running.suspend_owner(0, 65).unwrap();
        running.resume_owner(0, 100);
        assert!(running.step(100).is_none());
        assert_eq!(event(&mut running, 120), (0, 9));
        assert_eq!(running.peek_when(), None);
    }

    #[test]
    fn stop_freezes_owner_but_private_fill_and_cancel_catch_up_in_order() {
        let mut s = scheduler(2, 8);
        s.config.service_time_ns = 20;
        s.enqueue_private(0, 0, 10).unwrap();
        assert!(s.step(10).is_none());
        s.suspend_owner(0, 11).unwrap();
        s.enqueue_private(0, 12, 20).unwrap(); // fill while process is down
        s.enqueue_private(0, 13, 30).unwrap(); // cancel receipt while down
        s.enqueue_private(1, 15, 40).unwrap();
        assert!(s.step(25).is_none());
        assert_eq!(event(&mut s, 45), (1, 40));
        assert_eq!(s.peek_when(), None);
        assert!(!s.private_drained(0));
        s.resume_owner(0, 100);
        for (start, expected) in [(100, 10), (120, 20), (140, 30)] {
            assert!(s.step(start).is_none());
            assert_eq!(event(&mut s, start + 20), (0, expected));
        }
        assert!(s.private_drained(0));
    }

    #[test]
    fn private_recovery_backlog_overflow_never_discards_a_fill() {
        let mut s = scheduler(1, 1);
        s.suspend_owner(0, 0).unwrap();
        s.enqueue_private(0, 1, 10).unwrap();
        assert!(s.enqueue_private(0, 2, 11).is_err());
        s.resume_owner(0, 100);
        assert_eq!(event(&mut s, 100), (0, 10));
    }

    #[test]
    #[ignore = "focused offline replay scheduler microbenchmark"]
    fn virtual_owner_coordination_benchmark() {
        let mut scheduler = scheduler(1, 64);
        scheduler.config.apply_delay_ns = 0;
        let mut direct = TimingHistogram::default();
        let mut coordinated = TimingHistogram::default();
        let mut sum = 0u64;
        for sequence in 0..100_000u64 {
            let start = std::time::Instant::now();
            sum = std::hint::black_box(sum.wrapping_add(sequence));
            direct.record(start.elapsed().as_nanos() as u64);
            let start = std::time::Instant::now();
            scheduler
                .enqueue_market(0, sequence, Some(1), sequence as u32)
                .unwrap();
            let completion = scheduler.step(sequence).unwrap();
            if let OwnerWork::Event(value) = completion.work {
                sum = std::hint::black_box(sum.wrapping_add(value as u64));
            }
            coordinated.record(start.elapsed().as_nanos() as u64);
        }
        assert_eq!(sum, 2 * (99_999u64 * 100_000 / 2));
        println!(
            "{}",
            serde_json::json!({
                "measurement": "wall duration of synthetic callback vs enqueue + owner selection + completion + synthetic callback; excludes histogram recording and strategy work",
                "events": 100_000, "baseline_direct_ns": direct, "virtual_owner_ns": coordinated,
                "scheduler": scheduler.stats(), "live_path_changed": false,
            })
        );
    }

    #[test]
    fn duplicate_private_payloads_are_not_silently_deduplicated() {
        // Lifecycle idempotence belongs to the owning account, just as live;
        // this transport must deliver duplicate exchange events faithfully.
        let mut s = scheduler(1, 8);
        s.enqueue_private(0, 0, 7).unwrap();
        s.enqueue_private(0, 1, 7).unwrap();
        assert_eq!(event(&mut s, 10), (0, 7));
        assert_eq!(event(&mut s, 11), (0, 7));
    }
}
