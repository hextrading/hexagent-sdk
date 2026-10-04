// Frozen baseline: SDK 75592d123904b2a80d1f07d0dacad65f4fe6189c.
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
use std::net::IpAddr;

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
    pub cumulative_no_response: u64,
    pub no_response_generation: u64,
    pub no_response_peer: Option<IpAddr>,
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
    cumulative_no_response: u64,
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

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct AdmissionCapacityDetail {
    pub ready_fast: u16,
    pub verified_free: u16,
    pub probation_free: u16,
    pub probation_busy: u16,
    pub eligible_cancel: u16,
    pub ready_mask: u64,
    pub verified_mask: u64,
    pub busy_mask: u64,
    pub reason: &'static str,
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
    transport_reset_pending: bool,
    transport_reset_peer: Option<IpAddr>,
    no_response_resets: u64,
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
            transport_reset_pending: false,
            transport_reset_peer: None,
            no_response_resets: 0,
        }
    }

    /// Called on this account's router after a private reset or an attempted
    /// order-endpoint request without a complete HTTP response. Old ACKs must
    /// not heal these lanes; new prewarmed generations enter normal recovery.
    pub(crate) fn retire_transport_generations(&mut self, now_ns: u64) {
        self.transport_reset_peer = None;
        for lane in self.fast.iter_mut().chain(self.cancel.iter_mut()) {
            lane.retired_generation = Some(lane.generation);
            lane.verified = false;
        }
        self.probe = None;
        self.recovery_successes = 0;
        self.transport_reset_pending = true;
        self.pause_for_recovery(now_ns);
    }

    /// Explicit early transport message, independent of business-success
    /// scoring. Query/WS success can never heal or verify an order lane.
    pub(crate) fn retire_transport_generations_avoiding(&mut self, now_ns: u64, peer: Option<IpAddr>) {
        self.retire_transport_generations(now_ns);
        self.transport_reset_peer = peer;
    }

    pub(crate) fn no_response_resets(&self) -> u64 {
        self.no_response_resets
    }

    pub(crate) fn transport_reset_peer(&self) -> Option<IpAddr> {
        self.transport_reset_peer
    }

    pub(crate) fn take_transport_reset(&mut self) -> bool {
        std::mem::take(&mut self.transport_reset_pending)
    }

    pub(crate) fn retired_generation(&self, role: Role, slot: usize) -> Option<u64> {
        self.lane(role, slot).and_then(|lane| lane.retired_generation)
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
            || observation.cumulative_no_response < previous.cumulative_no_response
            || observation.cumulative_no_response > observation.cumulative_failures
            || (observation.cumulative_no_response > 0
                && observation.no_response_generation > observation.health.pool_generation)
        {
            return self.mark_delivery_fault(role, slot, now_ns);
        }

        let transport_reset = observation.cumulative_no_response > previous.cumulative_no_response
            && previous.retired_generation.is_none_or(|retired| observation.no_response_generation > retired);
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
        lane.cumulative_no_response = observation.cumulative_no_response;
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

        if transport_reset {
            self.no_response_resets = self.no_response_resets.saturating_add(1);
            // Retire the other idle sockets now: they may share the reset peer.
            // This account-local control action neither reclassifies unknown
            // orders nor releases strategy reservations. Late failures from a
            // generation already retired by this incident cannot reset again.
            self.retire_transport_generations(now_ns);
            self.transport_reset_peer = observation.no_response_peer;
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
        let probation_busy = self
            .fast
            .iter()
            .filter(|lane| {
                lane.fresh(now_ns, self.config.stale_after_ns)
                    && lane.ready() && !lane.verified && lane.busy
            })
            .count();
        let free_fast = self
            .fast
            .iter()
            .filter(|lane| {
                lane.fresh(now_ns, self.config.stale_after_ns) && lane.ready() && !lane.busy
            })
            .count();
        let verified_free = self.fast.iter().filter(|lane| {
            lane.fresh(now_ns, self.config.stale_after_ns)
                && lane.ready() && lane.verified && !lane.busy
        }).count();
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
            // Isolate eligibility by lane: a repaired or stale sibling must
            // not halve already verified capacity. Prewarmed generations may
            // obtain real business evidence, with at most one unverified lane
            // in flight. This probation budget is separate from healthy work.
            // Account-wide recovery above remains one request in flight,
            // including unknown requests on unavailable old generations.
            let probation_free = free_fast.saturating_sub(verified_free);
            (
                ExecutionAdmissionState::Degraded,
                verified_free + usize::from(probation_busy == 0 && probation_free > 0),
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
                    && (self.recovery_required || lane.verified || !self.fast.iter().any(|other| {
                        other.fresh(now_ns, self.config.stale_after_ns)
                            && other.ready() && !other.verified && other.busy
                    }))
            })
    }

    /// Compact diagnostic copy. The router is the sole writer; formatting and
    /// export run on the existing background diagnostic owner.
    pub(crate) fn capacity_detail(&self, now_ns: u64) -> AdmissionCapacityDetail {
        let mut detail = AdmissionCapacityDetail::default();
        for (slot, lane) in self.fast.iter().enumerate() {
            let ready = lane.fresh(now_ns, self.config.stale_after_ns) && lane.ready();
            detail.ready_fast += u16::from(ready);
            detail.verified_free += u16::from(ready && lane.verified && !lane.busy);
            detail.probation_free += u16::from(ready && !lane.verified && !lane.busy);
            detail.probation_busy += u16::from(ready && !lane.verified && lane.busy);
            // Masks cover the first 64 lanes; counts cover the entire pool.
            if slot < 64 {
                detail.ready_mask |= u64::from(ready) << slot;
                detail.verified_mask |= u64::from(ready && lane.verified) << slot;
                detail.busy_mask |= u64::from(lane.busy) << slot;
            }
        }
        detail.eligible_cancel = self.cancel.iter().filter(|lane| {
            !lane.reserved_cancel && lane.fresh(now_ns, self.config.stale_after_ns)
                && lane.cancel_probe_eligible(now_ns)
        }).count().min(u16::MAX as usize) as u16;
        detail.reason = if self.admission.allows_place() { "open" }
            else if detail.ready_fast == 0 { "no_ready_fast" }
            else if detail.eligible_cancel == 0 { "no_ordinary_cancel" }
            else if now_ns < self.pause_until_ns { "recovery_backoff" }
            else if self.recovery_required { "recovery_inflight" }
            else { "eligible_capacity_inflight" };
        detail
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
