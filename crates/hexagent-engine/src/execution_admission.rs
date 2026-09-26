//! Account-local transport admission, owned exclusively by the execution router.
//!
//! Connection owners publish complete, sequenced observations on bounded lanes.
//! Replacing an observation is safe because failure/slow totals are cumulative.
//! The router never reads a connection owner's mutable state through this type.
//! Construction allocates the lane tables; observation, refresh and admission do
//! not allocate, lock, log, perform I/O, or consult any process-global state.

use hexagent_runtime::http1_pool::{
    BusinessHttpOutcome, BusinessHttpOutcomeSnapshot, PermitHealthSnapshot, Role,
};
use hexagent_types::types::{Exchange, ExecutionAdmission, ExecutionAdmissionState};

const SLOW_HTTP_NS: u64 = 500_000_000;

#[derive(Clone, Copy, Debug)]
pub(crate) struct AdmissionConfig {
    pub stale_after_ns: u64,
    pub failure_cluster_window_ns: u64,
    pub recovery_pause_ns: u64,
    pub max_recovery_pause_ns: u64,
    pub recovery_successes: u8,
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        Self {
            // Idle owners heartbeat every 100 ms. A two-second place deadline
            // fits inside this lease; a stuck/hedged cancel fails closed.
            stale_after_ns: 3_000_000_000,
            failure_cluster_window_ns: 750_000_000,
            recovery_pause_ns: 250_000_000,
            max_recovery_pause_ns: 2_000_000_000,
            recovery_successes: 3,
        }
    }
}

/// Full replaceable owner snapshot. Sequence and cumulative counters are scoped
/// to one role/slot for the lifetime of this account's execution router. They
/// must not reset when the HTTP pool generation changes. GET warmups and local
/// preflight rejections must not produce a `business` outcome.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LaneObservation {
    pub sequence: u64,
    pub observed_at_ns: u64,
    pub health: PermitHealthSnapshot,
    pub business: Option<BusinessHttpOutcomeSnapshot>,
    pub busy: bool,
    pub cumulative_failures: u64,
    pub cumulative_slow: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct LaneState {
    sequence: u64,
    observed_at_ns: u64,
    generation: u64,
    quarantined: bool,
    busy: bool,
    dispatched_at_ns: Option<u64>,
    business_attempt: u64,
    cumulative_failures: u64,
    cumulative_slow: u64,
    retired_generation: Option<u64>,
    verified: bool,
    delivery_fault: bool,
    failure_window: u64,
    reserved_cancel: bool,
    business_failed: bool,
    failure_eligible_after_ns: u64,
    failure_streak: u32,
}

impl LaneState {
    fn fresh(&self, now_ns: u64, stale_after_ns: u64) -> bool {
        self.sequence != 0
            && !self.delivery_fault
            && self.observed_at_ns <= now_ns
            && now_ns.saturating_sub(self.observed_at_ns) <= stale_after_ns
    }

    fn ready(&self) -> bool {
        !self.quarantined && self.retired_generation != Some(self.generation)
    }

    fn cancel_ready(&self) -> bool {
        self.ready() && !self.business_failed
    }

    fn cancel_probe_eligible(&self, now_ns: u64) -> bool {
        self.ready() && (!self.business_failed || now_ns >= self.failure_eligible_after_ns)
    }
}

#[derive(Clone, Copy, Debug)]
struct RecoveryProbe {
    slot: usize,
    generation: u64,
    after_attempt: u64,
    dispatched_at_ns: u64,
}

/// Sole writer: the existing account execution router, never a strategy thread.
/// Each account receives a separate instance, including separate failure windows
/// and recovery probes. `Fast` and `Cancel` slots form the complete registry;
/// query/reconcile/replay observations cannot affect order admission.
pub(crate) struct AccountExecutionAdmission {
    config: AdmissionConfig,
    fast: Vec<LaneState>,
    cancel: Vec<LaneState>,
    admission: ExecutionAdmission,
    recovery_required: bool,
    recovery_successes: u8,
    probe: Option<RecoveryProbe>,
    pause_until_ns: u64,
    failure_streak: u32,
    failure_window: u64,
    failure_window_started_ns: u64,
    failure_window_lanes: usize,
}

impl AccountExecutionAdmission {
    pub(crate) fn new(fast_slots: usize, cancel_slots: usize, now_ns: u64) -> Self {
        Self::with_config(fast_slots, cancel_slots, now_ns, AdmissionConfig::default())
    }

    pub(crate) fn with_config(
        fast_slots: usize,
        cancel_slots: usize,
        now_ns: u64,
        config: AdmissionConfig,
    ) -> Self {
        Self {
            config,
            fast: vec![LaneState::default(); fast_slots],
            cancel: vec![LaneState::default(); cancel_slots],
            admission: ExecutionAdmission {
                exchange: Exchange::Polymarket,
                epoch: 0,
                state: ExecutionAdmissionState::Paused,
                available_place_slots: 0,
                observed_at_ns: now_ns,
            },
            recovery_required: true,
            recovery_successes: 0,
            probe: None,
            pause_until_ns: 0,
            failure_streak: 0,
            failure_window: 0,
            failure_window_started_ns: 0,
            failure_window_lanes: 0,
        }
    }

    /// Called only by this account's dispatcher after an explicit private
    /// transport reset message. Old generation heartbeats/business ACKs must
    /// not heal these lanes; new prewarmed generations enter normal recovery.
    pub(crate) fn retire_transport_generations(&mut self, now_ns: u64) {
        for lane in self.fast.iter_mut().chain(self.cancel.iter_mut()) {
            lane.retired_generation = Some(lane.generation);
            lane.verified = false;
        }
        self.probe = None;
        self.recovery_successes = 0;
        self.pause_for_recovery(now_ns);
    }

    pub(crate) fn current(&self) -> ExecutionAdmission {
        self.admission
    }

    /// Startup-only designation: emergency-only cancel capacity cannot justify
    /// admitting new places whose ordinary cancels use a different owner lane.
    pub(crate) fn reserve_cancel_slot(&mut self, slot: usize) {
        if let Some(lane) = self.cancel.get_mut(slot) {
            lane.reserved_cancel = true;
        }
        self.refresh(self.admission.observed_at_ns);
    }

    /// Invalid/out-of-order messages never refresh a lease or heal a lane. A
    /// sequence gap is allowed: totals preserve lost bad outcomes, and skipped
    /// successful samples never contribute to recovery.
    pub(crate) fn observe(
        &mut self,
        role: Role,
        slot: usize,
        observation: LaneObservation,
        now_ns: u64,
    ) -> ExecutionAdmission {
        let Some(previous) = self.lane(role, slot).copied() else {
            return self.refresh(now_ns);
        };
        if observation.sequence <= previous.sequence
            || observation.health.pool_generation < previous.generation
        {
            return self.refresh(now_ns);
        }
        if observation.observed_at_ns > now_ns
            || observation.observed_at_ns < previous.observed_at_ns
            || observation.cumulative_failures < previous.cumulative_failures
            || observation.cumulative_slow < previous.cumulative_slow
        {
            return self.mark_delivery_fault(role, slot, now_ns);
        }

        let slow = observation.cumulative_slow > previous.cumulative_slow;
        let business = observation.business.filter(|outcome| {
            outcome.attempt_id > previous.business_attempt
                && outcome.pool_generation == observation.health.pool_generation
        });
        let healthy = business.is_some_and(|outcome| {
            outcome.outcome == BusinessHttpOutcome::Healthy && outcome.elapsed_ns < SLOW_HTTP_NS
        });
        let failure = observation.cumulative_failures > previous.cumulative_failures
            || business.is_some_and(|outcome| outcome.outcome == BusinessHttpOutcome::Failure);
        let business_slow = business.is_some_and(|outcome| {
            outcome.outcome == BusinessHttpOutcome::Slow
                || (outcome.outcome == BusinessHttpOutcome::Healthy
                    && outcome.elapsed_ns >= SLOW_HTTP_NS)
        });
        let failed_probe = failure
            && self.probe.is_some_and(|probe| {
                role == Role::Fast
                    && slot == probe.slot
                    && observation.observed_at_ns >= probe.dispatched_at_ns
            });

        let config = self.config;
        let lane = self.lane_mut(role, slot).expect("validated lane");
        if observation.health.pool_generation != lane.generation {
            lane.verified = false;
        }
        lane.sequence = observation.sequence;
        lane.observed_at_ns = observation.observed_at_ns;
        lane.generation = observation.health.pool_generation;
        lane.quarantined = observation.health.quarantined;
        lane.cumulative_failures = observation.cumulative_failures;
        lane.cumulative_slow = observation.cumulative_slow;
        lane.delivery_fault = false;
        if let Some(outcome) = observation.business {
            lane.business_attempt = lane.business_attempt.max(outcome.attempt_id);
        }
        // An already queued idle heartbeat cannot clear a root-side dispatch.
        // Owner heartbeats read occupied after root has marked it at enqueue.
        if lane
            .dispatched_at_ns
            .is_none_or(|dispatched| observation.observed_at_ns >= dispatched)
        {
            lane.busy = observation.busy;
            if !observation.busy {
                lane.dispatched_at_ns = None;
            }
        }
        if failure || slow || business_slow {
            lane.verified = false;
        }
        if failure || slow || business_slow {
            lane.business_failed = true;
            let pause = config
                .recovery_pause_ns
                .saturating_mul(1_u64 << lane.failure_streak.min(3))
                .min(config.max_recovery_pause_ns);
            lane.failure_streak = lane.failure_streak.saturating_add(1);
            lane.failure_eligible_after_ns = now_ns.saturating_add(pause);
        }
        if healthy && !slow && lane.retired_generation != Some(lane.generation) {
            // Owners serialize requests on each lane. A new healthy business
            // result is later than that lane's preceding failures, including
            // failures represented only by a coalesced cumulative counter.
            // Merely advancing the prewarmed generation cannot clear this.
            lane.business_failed = false;
            lane.failure_streak = 0;
            lane.failure_eligible_after_ns = 0;
        }
        if business_slow || (slow && lane.generation == previous.generation) {
            // A warmup/fast response on the same generation cannot resurrect a
            // socket retired for a slow business result.
            lane.retired_generation = Some(lane.generation);
        } else if healthy && !failure && !slow && lane.ready() {
            lane.verified = true;
        }

        if failure {
            self.note_failure(role, slot, now_ns);
        }
        if failed_probe && now_ns >= self.pause_until_ns {
            self.pause_for_recovery(now_ns);
        }
        if failure || slow || business_slow {
            self.recovery_successes = 0;
            // Any new adverse observation invalidates a probe already in flight.
            // Its eventual success was not dispatched after this evidence.
            self.probe = None;
        }

        if let Some(probe) = self.probe {
            if role == Role::Fast
                && slot == probe.slot
                && observation.observed_at_ns >= probe.dispatched_at_ns
                && !observation.busy
            {
                self.probe = None;
                let fresh_probe_success = healthy
                    && !failure
                    && !slow
                    && observation.health.pool_generation == probe.generation
                    && business.is_some_and(|outcome| outcome.attempt_id > probe.after_attempt);
                if fresh_probe_success {
                    self.recovery_successes = self.recovery_successes.saturating_add(1);
                }
                // No business result means local not_sent/preflight. Release the
                // sole permit without manufacturing a successful HTTP probe.
            }
        }
        if self.recovery_successes >= self.config.recovery_successes.max(1)
            && self.cancel.iter().any(|lane| {
                !lane.reserved_cancel
                    && lane.fresh(now_ns, self.config.stale_after_ns)
                    && lane.cancel_ready()
            })
        {
            self.recovery_required = false;
            self.failure_streak = 0;
        }
        self.refresh(now_ns)
    }

    /// Use for a disconnected owner or a delivery failure that cannot retain a
    /// complete cumulative snapshot. A later newer valid snapshot is required;
    /// waiting out a timer alone cannot reopen admission.
    pub(crate) fn mark_delivery_fault(
        &mut self,
        role: Role,
        slot: usize,
        now_ns: u64,
    ) -> ExecutionAdmission {
        if let Some(lane) = self.lane_mut(role, slot) {
            lane.delivery_fault = true;
        }
        self.refresh(now_ns)
    }

    pub(crate) fn refresh(&mut self, now_ns: u64) -> ExecutionAdmission {
        let fresh = self
            .fast
            .iter()
            .chain(self.cancel.iter())
            .all(|lane| lane.fresh(now_ns, self.config.stale_after_ns));
        let ready_fast = self
            .fast
            .iter()
            .filter(|lane| lane.fresh(now_ns, self.config.stale_after_ns) && lane.ready())
            .count();
        let ready_cancel = self
            .cancel
            .iter()
            .filter(|lane| {
                !lane.reserved_cancel
                    && lane.fresh(now_ns, self.config.stale_after_ns)
                    && lane.cancel_ready()
            })
            .count();
        let eligible_cancel = self
            .cancel
            .iter()
            .filter(|lane| {
                !lane.reserved_cancel
                    && lane.fresh(now_ns, self.config.stale_after_ns)
                    && lane.cancel_probe_eligible(now_ns)
            })
            .count();
        let total_cancel = self
            .cancel
            .iter()
            .filter(|lane| !lane.reserved_cancel)
            .count();
        let busy_fast = self.fast.iter().filter(|lane| lane.busy).count();
        let eligible_busy_fast = self
            .fast
            .iter()
            .filter(|lane| {
                lane.fresh(now_ns, self.config.stale_after_ns) && lane.ready() && lane.busy
            })
            .count();
        let free_fast = self
            .fast
            .iter()
            .filter(|lane| {
                lane.fresh(now_ns, self.config.stale_after_ns) && lane.ready() && !lane.busy
            })
            .count();
        let paused = ready_fast == 0 || eligible_cancel == 0 || now_ns < self.pause_until_ns;
        if ready_cancel == 0 {
            // A failed cancel route becomes only a limited recovery candidate
            // after isolation, so lack of existing orders cannot deadlock all
            // future evidence. A timer or /time never proves cancel recovery.
            self.recovery_required = true;
        }
        let (state, slots) = if paused {
            self.recovery_required = true;
            self.recovery_successes = 0;
            self.probe = None;
            (ExecutionAdmissionState::Paused, 0)
        } else if self.recovery_required {
            // Recovery promises at most one request in flight account-wide.
            // An unavailable owner is not evidence its unknown POST completed.
            let slots = usize::from(busy_fast == 0 && self.probe.is_none() && free_fast > 0);
            (ExecutionAdmissionState::Recovering, slots)
        } else if fresh
            && ready_fast == self.fast.len()
            && ready_cancel == total_cancel
            && self.fast.iter().all(|lane| lane.verified)
        {
            (ExecutionAdmissionState::Healthy, free_fast)
        } else {
            // A partial pool continues quoting at reduced concurrency. Slow
            // generations stay excluded while a repaired generation is eligible
            // for real business evidence, never healed by its /time warmup.
            // An isolated busy lane keeps its own dispatch/unknown ownership,
            // but must not also consume this remaining eligible pool's budget.
            let budget = (ready_fast / 2).max(1);
            (
                ExecutionAdmissionState::Degraded,
                budget.saturating_sub(eligible_busy_fast).min(free_fast),
            )
        };
        let slots = slots.min(u16::MAX as usize) as u16;
        if self.admission.state != state || self.admission.available_place_slots != slots {
            self.admission.epoch = self.admission.epoch.saturating_add(1);
        }
        self.admission.state = state;
        self.admission.available_place_slots = slots;
        self.admission.observed_at_ns = now_ns;
        self.admission
    }

    pub(crate) fn can_place(&mut self, now_ns: u64) -> bool {
        self.refresh(now_ns).allows_place()
    }

    pub(crate) fn lane_place_allowed(&mut self, slot: usize, now_ns: u64) -> bool {
        self.can_place(now_ns)
            && self.fast.get(slot).is_some_and(|lane| {
                lane.fresh(now_ns, self.config.stale_after_ns) && lane.ready() && !lane.busy
            })
    }

    pub(crate) fn lane_generation(&self, slot: usize) -> Option<u64> {
        self.fast.get(slot).and_then(|lane| {
            lane.fresh(self.admission.observed_at_ns, self.config.stale_after_ns)
                .then_some(lane.generation)
        })
    }

    /// Call immediately after successful owner enqueue, before considering the
    /// next place. This closes the propagation race for the one-probe budget.
    pub(crate) fn place_dispatched(&mut self, slot: usize, now_ns: u64) {
        if let Some(lane) = self.fast.get_mut(slot) {
            if self.recovery_required && self.probe.is_none() {
                self.probe = Some(RecoveryProbe {
                    slot,
                    generation: lane.generation,
                    after_attempt: lane.business_attempt,
                    dispatched_at_ns: now_ns,
                });
            }
            lane.busy = true;
            lane.dispatched_at_ns = Some(now_ns);
        }
        self.refresh(now_ns);
    }

    /// For a root-owned dispatch known not to have reached an owner. Ordinary
    /// owner preflight completions release through their complete observation.
    pub(crate) fn place_not_sent(&mut self, slot: usize, now_ns: u64) {
        if let Some(lane) = self.fast.get_mut(slot) {
            lane.busy = false;
            lane.dispatched_at_ns = None;
        }
        if self.probe.is_some_and(|probe| probe.slot == slot) {
            self.probe = None;
        }
        self.refresh(now_ns);
    }

    fn lane(&self, role: Role, slot: usize) -> Option<&LaneState> {
        match role {
            Role::Fast => self.fast.get(slot),
            Role::Cancel => self.cancel.get(slot),
            Role::Reconcile | Role::Query | Role::GapReplay => None,
        }
    }

    fn lane_mut(&mut self, role: Role, slot: usize) -> Option<&mut LaneState> {
        match role {
            Role::Fast => self.fast.get_mut(slot),
            Role::Cancel => self.cancel.get_mut(slot),
            Role::Reconcile | Role::Query | Role::GapReplay => None,
        }
    }

    fn note_failure(&mut self, role: Role, slot: usize, now_ns: u64) {
        if self.failure_window == 0
            || now_ns.saturating_sub(self.failure_window_started_ns)
                > self.config.failure_cluster_window_ns
        {
            self.failure_window = self.failure_window.saturating_add(1);
            self.failure_window_started_ns = now_ns;
            self.failure_window_lanes = 0;
        }
        let window = self.failure_window;
        if let Some(lane) = self.lane_mut(role, slot) {
            if lane.failure_window != window {
                lane.failure_window = window;
                self.failure_window_lanes += 1;
            }
        }
        // At most one pause per cluster window. Ordinary slow successes never
        // enter this path; a noisy slot cannot masquerade as multiple sockets.
        if self.failure_window_lanes == 2 {
            self.failure_window_lanes += 1;
            self.pause_for_recovery(now_ns);
        }
    }

    fn pause_for_recovery(&mut self, now_ns: u64) {
        let shift = self.failure_streak.min(3);
        let duration = self
            .config
            .recovery_pause_ns
            .saturating_mul(1_u64 << shift)
            .min(self.config.max_recovery_pause_ns);
        self.failure_streak = self.failure_streak.saturating_add(1);
        self.pause_until_ns = now_ns.saturating_add(duration);
        self.recovery_required = true;
        self.recovery_successes = 0;
        self.probe = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Harness {
        state: AccountExecutionAdmission,
        fast: Vec<LaneObservation>,
        cancel: Vec<LaneObservation>,
        now: u64,
        attempt: u64,
    }

    impl Harness {
        fn new(fast: usize, cancel: usize) -> Self {
            let now = 10_000;
            let observation = LaneObservation {
                sequence: 0,
                observed_at_ns: now,
                health: PermitHealthSnapshot {
                    pool_generation: 1,
                    quarantined: false,
                },
                business: None,
                busy: false,
                cumulative_failures: 0,
                cumulative_slow: 0,
            };
            let mut harness = Self {
                state: AccountExecutionAdmission::new(fast, cancel, now),
                fast: vec![observation; fast],
                cancel: vec![observation; cancel],
                now,
                attempt: 0,
            };
            for slot in 0..fast {
                harness.emit(Role::Fast, slot, |_| {});
            }
            for slot in 0..cancel {
                harness.emit(Role::Cancel, slot, |_| {});
            }
            harness
        }

        fn emit(&mut self, role: Role, slot: usize, edit: impl FnOnce(&mut LaneObservation)) {
            self.now += 1_000;
            let observation = match role {
                Role::Fast => &mut self.fast[slot],
                Role::Cancel => &mut self.cancel[slot],
                _ => panic!("test role"),
            };
            edit(observation);
            observation.sequence += 1;
            observation.observed_at_ns = self.now;
            self.state.observe(role, slot, *observation, self.now);
        }

        fn outcome(&mut self, role: Role, slot: usize, outcome: BusinessHttpOutcome) {
            self.attempt += 1;
            let attempt = self.attempt;
            self.emit(role, slot, |observation| {
                match outcome {
                    BusinessHttpOutcome::Healthy => {}
                    BusinessHttpOutcome::Slow => observation.cumulative_slow += 1,
                    BusinessHttpOutcome::Failure => observation.cumulative_failures += 1,
                }
                observation.busy = false;
                observation.business = Some(BusinessHttpOutcomeSnapshot {
                    attempt_id: attempt,
                    pool_generation: observation.health.pool_generation,
                    elapsed_ns: if outcome == BusinessHttpOutcome::Slow {
                        SLOW_HTTP_NS
                    } else {
                        40_000_000
                    },
                    outcome,
                    status_code: if outcome == BusinessHttpOutcome::Failure {
                        503
                    } else {
                        200
                    },
                });
            });
        }

        fn success(&mut self, slot: usize) {
            self.now += 1_000;
            assert!(self.state.lane_place_allowed(slot, self.now));
            self.state.place_dispatched(slot, self.now);
            self.outcome(Role::Fast, slot, BusinessHttpOutcome::Healthy);
        }

        fn healthy(fast: usize, cancel: usize) -> Self {
            let mut harness = Self::new(fast, cancel);
            for _ in 0..3 {
                harness.success(0);
            }
            for slot in 1..fast {
                harness.success(slot);
            }
            assert_eq!(
                harness.state.current().state,
                ExecutionAdmissionState::Healthy
            );
            harness
        }

        fn make_fast_busy_and_stale(&mut self, stalled_slot: usize) {
            self.state.place_dispatched(stalled_slot, self.now);
            self.fast[stalled_slot].busy = true;
            // Other owners continue their full heartbeat snapshots throughout
            // the stall, so the account never suffers an unrelated lease gap.
            for _ in 0..4 {
                self.now += self.state.config.stale_after_ns / 3;
                for slot in 0..self.fast.len() {
                    if slot != stalled_slot {
                        self.emit(Role::Fast, slot, |_| {});
                    }
                }
                for slot in 0..self.cancel.len() {
                    self.emit(Role::Cancel, slot, |_| {});
                }
            }
        }
    }

    #[test]
    fn startup_prewarm_is_only_eligibility_and_three_real_serial_results_recover() {
        let mut harness = Harness::new(2, 1);
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Recovering
        );
        assert_eq!(harness.state.current().available_place_slots, 1);
        // A response dispatched before this coordinator's recovery permission
        // cannot prove recovery, even if its attempt ID is new.
        harness.outcome(Role::Fast, 1, BusinessHttpOutcome::Healthy);
        assert_eq!(harness.state.recovery_successes, 0);
        harness.success(0);
        harness.success(0);
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Recovering
        );
        harness.success(0);
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Healthy
        );
    }

    #[test]
    fn recovery_reserves_before_owner_feedback_and_ignores_queued_idle_snapshot() {
        let mut harness = Harness::new(2, 1);
        let old_time = harness.now;
        harness.now += 10_000;
        harness.state.place_dispatched(0, harness.now);
        assert!(!harness.state.lane_place_allowed(1, harness.now));
        let mut queued_idle = harness.fast[0];
        queued_idle.sequence += 1;
        queued_idle.observed_at_ns = old_time;
        harness
            .state
            .observe(Role::Fast, 0, queued_idle, harness.now);
        assert!(harness.state.fast[0].busy);
        assert!(harness.state.probe.is_some());
        assert!(!harness.state.can_place(harness.now));
    }

    #[test]
    fn private_transport_reset_requires_new_generations_and_is_account_local() {
        let mut account = Harness::healthy(2, 1);
        let mut sibling = Harness::healthy(2, 1);
        account.state.retire_transport_generations(account.now);
        for slot in 0..2 {
            account.emit(Role::Fast, slot, |_| {});
            account.outcome(Role::Fast, slot, BusinessHttpOutcome::Healthy);
            assert!(!account.state.lane_place_allowed(slot, account.now));
        }
        assert!(sibling.state.can_place(sibling.now));
        account.now = account.state.pause_until_ns + 1;
        account.emit(Role::Cancel, 0, |observation| observation.health.pool_generation += 1);
        account.emit(Role::Fast, 0, |observation| observation.health.pool_generation += 1);
        assert!(account.state.lane_place_allowed(0, account.now));
        assert!(!account.state.lane_place_allowed(1, account.now));
        // The second reset fences the replacement too; a coalesced warm
        // heartbeat cannot resurrect a generation preceding that reset.
        account.state.retire_transport_generations(account.now);
        account.emit(Role::Fast, 0, |_| {});
        assert!(!account.state.lane_place_allowed(0, account.now));
    }

    #[test]
    fn slow_success_retires_exact_generation_without_pausing_other_capacity() {
        let mut harness = Harness::healthy(4, 2);
        harness.outcome(Role::Fast, 0, BusinessHttpOutcome::Slow);
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Degraded
        );
        assert_eq!(harness.state.current().available_place_slots, 1);
        assert!(!harness.state.lane_place_allowed(0, harness.now));
        assert!(harness.state.lane_place_allowed(1, harness.now));
        // Neither a warmup nor a fast result on that retired generation heals it.
        harness.emit(Role::Fast, 0, |_| {});
        harness.outcome(Role::Fast, 0, BusinessHttpOutcome::Healthy);
        assert!(!harness.state.lane_place_allowed(0, harness.now));
        harness.emit(Role::Fast, 0, |observation| {
            observation.health.pool_generation = 2
        });
        assert!(harness.state.lane_place_allowed(0, harness.now));
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Degraded
        );
        harness.success(0);
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Healthy
        );
    }

    #[test]
    fn two_slow_lanes_keep_spare_capacity_instead_of_starting_a_five_second_gate() {
        let mut harness = Harness::healthy(6, 2);
        harness.outcome(Role::Fast, 0, BusinessHttpOutcome::Slow);
        harness.outcome(Role::Fast, 1, BusinessHttpOutcome::Slow);
        assert_eq!(harness.state.pause_until_ns, 0);
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Degraded
        );
        assert_eq!(harness.state.current().available_place_slots, 2);
        assert!(!harness.state.lane_place_allowed(0, harness.now));
        assert!(!harness.state.lane_place_allowed(1, harness.now));
        assert!(harness.state.lane_place_allowed(2, harness.now));
        harness.success(2);
        assert!(harness.state.can_place(harness.now));
    }

    #[test]
    fn missing_cancel_capacity_pauses_even_with_all_fast_lanes_ready() {
        let mut harness = Harness::healthy(2, 2);
        harness.emit(Role::Cancel, 0, |observation| {
            observation.health.quarantined = true
        });
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Degraded
        );
        harness.emit(Role::Cancel, 1, |observation| {
            observation.health.quarantined = true
        });
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Paused
        );
        assert!(!harness.state.can_place(harness.now));
        harness.emit(Role::Cancel, 0, |observation| {
            observation.health.quarantined = false;
            observation.health.pool_generation += 1;
        });
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Recovering
        );
        harness.success(0);
        harness.success(0);
        harness.success(0);
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Degraded
        );
    }

    #[test]
    fn normal_busy_capacity_does_not_create_an_incident_or_reset_evidence() {
        let mut harness = Harness::healthy(1, 1);
        harness.state.place_dispatched(0, harness.now);
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Healthy
        );
        assert!(!harness.state.can_place(harness.now));
        // A busy cancel remains a ready connection, not a transport failure.
        harness.emit(Role::Cancel, 0, |observation| observation.busy = true);
        harness.outcome(Role::Fast, 0, BusinessHttpOutcome::Healthy);
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Healthy
        );
        assert!(harness.state.can_place(harness.now));
    }

    #[test]
    fn emergency_cancel_slot_alone_cannot_admit_new_orders() {
        let mut harness = Harness::healthy(2, 2);
        harness.state.reserve_cancel_slot(0);
        harness.emit(Role::Cancel, 1, |observation| {
            observation.health.quarantined = true
        });
        assert!(harness.state.cancel[0].ready());
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Paused
        );
        assert!(!harness.state.can_place(harness.now));
        harness.emit(Role::Cancel, 1, |observation| {
            observation.health.pool_generation += 1;
            observation.health.quarantined = false;
        });
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Recovering
        );
        // Reserved safety owners are still covered by the freshness contract.
        for _ in 0..3 {
            harness.success(0);
        }
        harness
            .state
            .mark_delivery_fault(Role::Cancel, 0, harness.now);
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Degraded
        );
    }

    #[test]
    fn failed_cancel_timer_only_allows_probes_real_cancel_evidence_releases_full_capacity() {
        for outcome in [BusinessHttpOutcome::Failure, BusinessHttpOutcome::Slow] {
            let mut harness = Harness::healthy(2, 1);
            // HTTP 503 need not quarantine a socket, but the only ordinary cancel
            // route still cannot justify admitting additional exposure.
            harness.outcome(Role::Cancel, 0, outcome);
            assert!(!harness.cancel[0].health.quarantined);
            assert_eq!(
                harness.state.current().state,
                ExecutionAdmissionState::Paused
            );
            harness.now += 500_000_000;
            harness.emit(Role::Cancel, 0, |observation| {
                observation.health.pool_generation += 1
            });
            assert_eq!(
                harness.state.current().state,
                ExecutionAdmissionState::Recovering
            );
            harness.emit(Role::Cancel, 0, |_| {});
            assert_eq!(
                harness.state.current().state,
                ExecutionAdmissionState::Recovering
            );
            for _ in 0..3 {
                harness.success(0);
            }
            assert_eq!(
                harness.state.current().state,
                ExecutionAdmissionState::Recovering
            );
            assert_eq!(harness.state.current().available_place_slots, 1);
            // Cancel work continues independently of place admission. Its real
            // response restores cancel eligibility, then new places probe serially.
            harness.outcome(Role::Cancel, 0, BusinessHttpOutcome::Healthy);
            assert_eq!(
                harness.state.current().state,
                ExecutionAdmissionState::Healthy
            );
        }
    }

    #[test]
    fn distinct_hard_failures_pause_only_the_affected_account() {
        let mut first = Harness::healthy(3, 2);
        let mut second = Harness::healthy(3, 2);
        first.outcome(Role::Fast, 0, BusinessHttpOutcome::Failure);
        assert_ne!(first.state.current().state, ExecutionAdmissionState::Paused);
        first.outcome(Role::Fast, 0, BusinessHttpOutcome::Failure);
        assert_ne!(first.state.current().state, ExecutionAdmissionState::Paused);
        first.outcome(Role::Cancel, 0, BusinessHttpOutcome::Failure);
        assert_eq!(first.state.current().state, ExecutionAdmissionState::Paused);
        assert_eq!(
            second.state.current().state,
            ExecutionAdmissionState::Healthy
        );
        assert!(second.state.can_place(second.now));
        let until = first.state.pause_until_ns;
        assert_eq!(
            first.state.refresh(until - 1).state,
            ExecutionAdmissionState::Paused
        );
        first.now = until;
        assert_eq!(
            first.state.refresh(until).state,
            ExecutionAdmissionState::Recovering
        );
        // Existing in-flight successes have no coordinator-issued probe fence.
        first.outcome(Role::Fast, 1, BusinessHttpOutcome::Healthy);
        assert_eq!(first.state.recovery_successes, 0);
        for _ in 0..3 {
            first.success(0);
        }
        assert_eq!(
            first.state.current().state,
            ExecutionAdmissionState::Degraded
        );
        first.outcome(Role::Cancel, 0, BusinessHttpOutcome::Healthy);
        assert_eq!(
            first.state.current().state,
            ExecutionAdmissionState::Healthy
        );
    }

    #[test]
    fn a_new_fault_invalidates_an_inflight_recovery_probe() {
        let mut harness = Harness::new(2, 1);
        harness.state.place_dispatched(0, harness.now);
        harness.outcome(Role::Cancel, 0, BusinessHttpOutcome::Failure);
        harness.outcome(Role::Fast, 0, BusinessHttpOutcome::Healthy);
        assert_eq!(harness.state.recovery_successes, 0);
        harness.outcome(Role::Cancel, 0, BusinessHttpOutcome::Healthy);
        for _ in 0..3 {
            harness.success(0);
        }
        assert_ne!(
            harness.state.current().state,
            ExecutionAdmissionState::Recovering
        );
    }

    #[test]
    fn a_failed_recovery_probe_backs_off_even_with_only_one_fast_lane() {
        let mut harness = Harness::new(1, 1);
        for expected_pause in [250_000_000, 500_000_000, 1_000_000_000] {
            assert!(harness.state.lane_place_allowed(0, harness.now));
            harness.state.place_dispatched(0, harness.now);
            harness.outcome(Role::Fast, 0, BusinessHttpOutcome::Failure);
            assert_eq!(
                harness.state.current().state,
                ExecutionAdmissionState::Paused
            );
            assert_eq!(harness.state.pause_until_ns - harness.now, expected_pause);
            harness.now = harness.state.pause_until_ns;
            harness.state.refresh(harness.now);
        }
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Recovering
        );
        for _ in 0..3 {
            harness.success(0);
        }
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Healthy
        );
        assert_eq!(harness.state.failure_streak, 0);
    }

    #[test]
    fn duplicate_or_old_generation_cannot_refresh_lease_or_heal_a_probe() {
        let mut harness = Harness::new(1, 1);
        harness.success(0);
        let duplicate = harness.fast[0];
        harness
            .state
            .observe(Role::Fast, 0, duplicate, harness.now + 1);
        assert_eq!(harness.state.recovery_successes, 1);
        harness.emit(Role::Fast, 0, |observation| {
            observation.health.pool_generation = 2
        });
        harness.state.place_dispatched(0, harness.now);
        let mut stale = harness.fast[0];
        stale.sequence += 1;
        stale.health.pool_generation = 1;
        stale.observed_at_ns = harness.now + 1;
        harness.state.observe(Role::Fast, 0, stale, harness.now + 1);
        assert!(harness.state.fast[0].busy);
        assert_eq!(harness.state.recovery_successes, 1);
        let expired = harness.now + harness.state.config.stale_after_ns + 1;
        assert_eq!(
            harness.state.refresh(expired).state,
            ExecutionAdmissionState::Paused
        );
        harness.state.observe(Role::Fast, 0, stale, expired);
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Paused
        );
    }

    #[test]
    fn coalesced_snapshots_preserve_adverse_evidence_and_do_not_invent_successes() {
        let mut harness = Harness::healthy(3, 1);
        harness.emit(Role::Fast, 0, |observation| {
            observation.sequence += 20;
            observation.cumulative_slow += 1;
            // The latest result can be fast even though an earlier replaced
            // observation retired this same generation.
            observation.business.as_mut().unwrap().attempt_id += 100;
        });
        assert!(!harness.state.lane_place_allowed(0, harness.now));
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Degraded
        );
        harness.emit(Role::Fast, 1, |observation| {
            observation.sequence += 20;
            observation.cumulative_failures += 5;
        });
        assert_ne!(
            harness.state.current().state,
            ExecutionAdmissionState::Paused
        );
        harness.emit(Role::Cancel, 0, |observation| {
            observation.cumulative_failures += 3
        });
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Paused
        );
    }

    #[test]
    fn lost_delivery_and_counter_reset_isolate_the_lane_until_a_fresh_snapshot() {
        let mut harness = Harness::healthy(2, 1);
        harness
            .state
            .mark_delivery_fault(Role::Fast, 0, harness.now);
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Degraded
        );
        assert!(!harness.state.lane_place_allowed(0, harness.now));
        assert!(harness.state.lane_place_allowed(1, harness.now));
        harness
            .state
            .observe(Role::Fast, 0, harness.fast[0], harness.now);
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Degraded
        );
        harness.emit(Role::Fast, 0, |_| {});
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Healthy
        );
        harness.outcome(Role::Fast, 0, BusinessHttpOutcome::Failure);
        let mut corrupt = harness.fast[0];
        corrupt.sequence += 1;
        corrupt.cumulative_failures = 0;
        harness.state.observe(Role::Fast, 0, corrupt, harness.now);
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Degraded
        );
        harness.emit(Role::Fast, 0, |_| {});
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Degraded
        );
        harness.now += harness.state.config.stale_after_ns + 1;
        assert_eq!(
            harness.state.refresh(harness.now).state,
            ExecutionAdmissionState::Paused
        );
        harness.emit(Role::Fast, 0, |_| {});
        harness.emit(Role::Fast, 1, |_| {});
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Paused
        );
        harness.emit(Role::Cancel, 0, |_| {});
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Recovering
        );
    }

    #[test]
    fn stale_safety_or_one_fast_lane_reduces_capacity_but_all_normal_cancels_pause() {
        let mut harness = Harness::healthy(3, 3);
        harness.state.reserve_cancel_slot(0);
        // Keep all ordinary lanes fresh while the safety owner is legitimately
        // busy with a long bounded expiry-cancel/audit command.
        harness.now += harness.state.config.stale_after_ns + 1;
        for slot in 0..3 {
            let observation = &mut harness.fast[slot];
            observation.sequence += 1;
            observation.observed_at_ns = harness.now;
            harness
                .state
                .observe(Role::Fast, slot, *observation, harness.now);
        }
        for slot in 1..3 {
            harness.emit(Role::Cancel, slot, |_| {});
        }
        // The first refresh observed a capacity gap, so finish its recovery.
        for _ in 0..3 {
            harness.success(0);
        }
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Degraded
        );
        assert!(harness.state.can_place(harness.now));
        harness.state.fast[1].observed_at_ns = 1;
        assert_eq!(
            harness.state.refresh(harness.now).state,
            ExecutionAdmissionState::Degraded
        );
        assert!(!harness.state.lane_place_allowed(1, harness.now));
        assert!(harness.state.lane_place_allowed(2, harness.now));
        harness.state.cancel[1].observed_at_ns = 1;
        harness.state.cancel[2].observed_at_ns = 1;
        assert_eq!(
            harness.state.refresh(harness.now).state,
            ExecutionAdmissionState::Paused
        );
        assert!(!harness.state.can_place(harness.now));
    }

    #[test]
    fn stale_busy_fast_keeps_its_own_slot_without_consuming_healthy_degraded_budget() {
        let mut harness = Harness::healthy(4, 4);
        harness.state.reserve_cancel_slot(0);
        harness.make_fast_busy_and_stale(0);
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Degraded
        );
        assert_eq!(harness.state.current().available_place_slots, 1);
        assert!(harness.state.fast[0].busy);
        assert!(harness.state.fast[0].dispatched_at_ns.is_some());
        assert!(!harness.state.lane_place_allowed(0, harness.now));
        assert!(harness.state.lane_place_allowed(1, harness.now));

        // A new real request consumes the one eligible degraded permit. The
        // unknown old request is neither completed nor replayed by admission.
        harness.state.place_dispatched(1, harness.now);
        assert_eq!(harness.state.current().available_place_slots, 0);
        assert!(!harness.state.lane_place_allowed(2, harness.now));
        harness.outcome(Role::Fast, 1, BusinessHttpOutcome::Healthy);
        assert_eq!(harness.state.current().available_place_slots, 1);
        assert!(harness.state.fast[0].busy);

        harness
            .state
            .mark_delivery_fault(Role::Fast, 0, harness.now);
        assert_eq!(harness.state.current().available_place_slots, 1);
        harness
            .state
            .observe(Role::Fast, 0, harness.fast[0], harness.now);
        assert!(harness.state.fast[0].busy);
        assert!(!harness.state.lane_place_allowed(0, harness.now));
    }

    #[test]
    fn restored_busy_generation_rejoins_budget_without_becoming_a_free_slot() {
        let mut harness = Harness::healthy(4, 4);
        harness.state.reserve_cancel_slot(0);
        harness.make_fast_busy_and_stale(0);
        harness.emit(Role::Fast, 0, |observation| {
            observation.health.pool_generation += 1;
            observation.busy = true;
        });
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Degraded
        );
        // Four eligible lanes give budget two, but the returned busy lane still
        // consumes one. Its new warm generation does not complete the request.
        assert_eq!(harness.state.current().available_place_slots, 1);
        assert!(harness.state.fast[0].busy);
        assert!(!harness.state.lane_place_allowed(0, harness.now));
        let mut old_generation = harness.fast[0];
        old_generation.sequence += 1;
        old_generation.health.pool_generation -= 1;
        old_generation.busy = false;
        harness
            .state
            .observe(Role::Fast, 0, old_generation, harness.now);
        assert!(harness.state.fast[0].busy);
        harness.state.place_dispatched(1, harness.now);
        assert_eq!(harness.state.current().available_place_slots, 0);
        harness.outcome(Role::Fast, 1, BusinessHttpOutcome::Healthy);
        assert_eq!(harness.state.current().available_place_slots, 1);
        harness.outcome(Role::Fast, 0, BusinessHttpOutcome::Healthy);
        assert!(!harness.state.fast[0].busy);
        assert!(harness.state.fast[0].dispatched_at_ns.is_none());
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Healthy
        );
        assert_eq!(harness.state.current().available_place_slots, 4);
    }

    #[test]
    fn recovering_keeps_unknown_inflight_until_explicit_owner_completion() {
        let mut harness = Harness::healthy(4, 4);
        harness.state.reserve_cancel_slot(0);
        harness.make_fast_busy_and_stale(0);
        for slot in 1..4 {
            harness
                .state
                .mark_delivery_fault(Role::Fast, slot, harness.now);
        }
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Paused
        );
        for slot in 1..4 {
            harness.emit(Role::Fast, slot, |_| {});
        }
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Recovering
        );
        assert_eq!(harness.state.current().available_place_slots, 0);
        assert!(harness.state.fast[0].busy);
        harness.emit(Role::Fast, 0, |observation| {
            observation.health.pool_generation += 1;
            observation.busy = true;
        });
        assert_eq!(harness.state.current().available_place_slots, 0);
        harness.outcome(Role::Fast, 0, BusinessHttpOutcome::Healthy);
        assert_eq!(harness.state.recovery_successes, 0);
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Recovering
        );
        assert_eq!(harness.state.current().available_place_slots, 1);
        for _ in 0..3 {
            harness.success(0);
        }
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Healthy
        );
    }

    #[test]
    fn not_sent_completes_recovery_reservation_without_counting_an_http_success() {
        let mut harness = Harness::new(1, 1);
        harness.state.place_dispatched(0, harness.now);
        harness.emit(Role::Fast, 0, |_| {});
        assert_eq!(harness.state.recovery_successes, 0);
        assert!(harness.state.can_place(harness.now));
        harness.state.place_dispatched(0, harness.now);
        harness.state.place_not_sent(0, harness.now);
        assert_eq!(harness.state.recovery_successes, 0);
        assert!(harness.state.can_place(harness.now));
        // A fast ordinary business rejection still proves the route works.
        harness.state.place_dispatched(0, harness.now);
        harness.attempt += 1;
        let attempt = harness.attempt;
        harness.emit(Role::Fast, 0, |observation| {
            observation.business = Some(BusinessHttpOutcomeSnapshot {
                attempt_id: attempt,
                pool_generation: 1,
                elapsed_ns: 10_000_000,
                outcome: BusinessHttpOutcome::Healthy,
                status_code: 400,
            });
        });
        assert_eq!(harness.state.recovery_successes, 1);
    }

    #[test]
    fn irrelevant_roles_and_unknown_slots_do_not_create_a_global_gate() {
        let mut harness = Harness::healthy(1, 1);
        let mut fault = harness.fast[0];
        fault.sequence += 1;
        fault.cumulative_failures += 1;
        fault.health.quarantined = true;
        for role in [Role::Query, Role::GapReplay, Role::Reconcile] {
            harness.state.observe(role, 0, fault, harness.now);
        }
        harness.state.observe(Role::Fast, 99, fault, harness.now);
        assert_eq!(
            harness.state.current().state,
            ExecutionAdmissionState::Healthy
        );
    }

    #[test]
    fn missing_pool_is_never_admitted_and_epochs_only_change_on_state_or_capacity() {
        for (fast, cancel) in [(0, 1), (1, 0), (0, 0)] {
            let mut harness = Harness::new(fast, cancel);
            assert!(!harness.state.can_place(harness.now));
        }
        let mut harness = Harness::healthy(1, 1);
        let before = harness.state.current();
        harness.emit(Role::Fast, 0, |_| {});
        assert_eq!(harness.state.current().epoch, before.epoch);
        assert!(harness.state.current().observed_at_ns > before.observed_at_ns);
        harness.state.place_dispatched(0, harness.now);
        assert!(harness.state.current().epoch > before.epoch);
    }

    #[test]
    fn repeated_fault_clusters_back_off_with_a_bounded_pause() {
        let mut harness = Harness::healthy(3, 2);
        for expected_ns in [
            250_000_000,
            500_000_000,
            1_000_000_000,
            2_000_000_000,
            2_000_000_000,
        ] {
            harness.now += 800_000_000;
            // Refresh every registered owner so lease expiry is independent of
            // the hard-failure backoff under test.
            for slot in 0..3 {
                harness.emit(Role::Fast, slot, |_| {});
            }
            for slot in 0..2 {
                harness.emit(Role::Cancel, slot, |_| {});
            }
            harness.outcome(Role::Fast, 0, BusinessHttpOutcome::Failure);
            harness.outcome(Role::Cancel, 0, BusinessHttpOutcome::Failure);
            assert_eq!(harness.state.pause_until_ns - harness.now, expected_ns);
        }
    }

    /// Run with `cargo test -p hexagent-engine --release admission_owner_benchmark
    /// -- --ignored --nocapture`. This is a local owner-control microbenchmark,
    /// not a claim about production HTTP or end-to-end trading latency.
    #[test]
    #[ignore]
    fn admission_owner_benchmark() {
        use std::hint::black_box;
        use std::time::Instant;

        const N: usize = 200_000;
        let mut harness = Harness::healthy(8, 4);
        let (sender, receiver) = crossbeam_channel::bounded(1);
        let mut samples = Vec::with_capacity(N);
        let mut legacy_samples = Vec::with_capacity(N);
        let mut overflows = 0_usize;
        let mut high_water = 0_usize;
        // Legacy baseline is only its final global deadline read; it excludes
        // rejection construction, strategy churn and HTTP, just as this control
        // benchmark excludes those stages. Record both boundaries explicitly.
        let legacy_until = std::sync::atomic::AtomicU64::new(0);
        for index in 0..N {
            let started = Instant::now();
            black_box(legacy_until.load(std::sync::atomic::Ordering::Acquire) > harness.now);
            legacy_samples.push(started.elapsed().as_nanos() as u64);

            harness.now += 1;
            let slot = index % harness.fast.len();
            let observation = &mut harness.fast[slot];
            observation.sequence += 1;
            observation.observed_at_ns = harness.now;
            let started = Instant::now();
            sender.try_send(*observation).unwrap();
            high_water = high_water.max(sender.len());
            // Exercise bounded-channel overflow, retaining the complete newest
            // observation for the next send rather than losing fault counters.
            if index % 64 == 0 && sender.try_send(*observation).is_err() {
                overflows += 1;
            }
            let received = receiver.try_recv().unwrap();
            harness
                .state
                .observe(Role::Fast, slot, received, harness.now);
            black_box(harness.state.lane_place_allowed(slot, harness.now));
            samples.push(started.elapsed().as_nanos() as u64);
        }
        for (boundary, mut values) in [
            ("legacy_deadline_atomic_read", legacy_samples),
            (
                "bounded_send_receive_full_observe_final_lane_admission",
                samples,
            ),
        ] {
            values.sort_unstable();
            eprintln!(
                "admission_bench boundary={boundary} n={N} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_high_water={high_water} overflow_attempts={overflows} dropped_observations=0",
                values[N / 2], values[N * 99 / 100], values[N * 999 / 1000], values[N - 1]
            );
        }
        assert_eq!(high_water, 1);
        assert_eq!(overflows, N.div_ceil(64));
    }
}
