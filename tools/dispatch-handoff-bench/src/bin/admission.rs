#![allow(dead_code)]
// Exercise exact old/new admission source without linking a trading runtime.
// These immutable POD definitions match the types consumed by both modules.
extern crate self as hexagent_runtime;
extern crate self as hexagent_types;
mod http1_pool {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Role {
        Fast,
        Cancel,
        Reconcile,
        Query,
        GapReplay,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum BusinessHttpOutcome {
        Healthy,
        Slow,
        Failure,
    }
    #[derive(Clone, Copy, Debug)]
    pub struct BusinessHttpOutcomeSnapshot {
        pub attempt_id: u64,
        pub pool_generation: u64,
        pub elapsed_ns: u64,
        pub outcome: BusinessHttpOutcome,
        pub status_code: u16,
    }
    #[derive(Clone, Copy, Debug)]
    pub struct PermitHealthSnapshot {
        pub pool_generation: u64,
        pub quarantined: bool,
    }
}
mod types {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Exchange {
        Polymarket,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum ExecutionAdmissionState {
        Healthy,
        Degraded,
        Paused,
        Recovering,
    }
    #[derive(Clone, Copy, Debug)]
    pub struct ExecutionAdmission {
        pub exchange: Exchange,
        pub epoch: u64,
        pub state: ExecutionAdmissionState,
        pub available_place_slots: u16,
        pub observed_at_ns: u64,
    }
    impl ExecutionAdmission {
        pub fn allows_place(self) -> bool {
            self.state != ExecutionAdmissionState::Paused && self.available_place_slots > 0
        }
    }
}
#[path = "../admission_baseline.rs"]
mod baseline;
#[path = "../../../../crates/hexagent-engine/src/execution_admission.rs"]
mod candidate;
use http1_pool::*;
use std::{hint::black_box, time::Instant};

macro_rules! bench {
    ($module:ident, $update:expr) => {{
        const N: usize = 100_000;
        let mut state = $module::AccountExecutionAdmission::new(4, 4, 10_000);
        let mut o = $module::LaneObservation {
            sequence: 1, observed_at_ns: 10_000,
            health: PermitHealthSnapshot { pool_generation: 1, quarantined: false },
            business: Some(BusinessHttpOutcomeSnapshot { attempt_id: 1, pool_generation: 1, elapsed_ns: 10, outcome: BusinessHttpOutcome::Healthy, status_code: 200 }),
            busy: false, cumulative_failures: 0, cumulative_slow: 0, cumulative_no_response: 0, no_response_generation: 0, no_response_peer: None,
        };
        for slot in 0..4 { state.observe(Role::Fast, slot, o, 10_000); state.observe(Role::Cancel, slot, o, 10_000); }
        for _ in 0..3 { state.place_dispatched(0, 10_000); o.sequence += 1; o.business.as_mut().unwrap().attempt_id += 1; state.observe(Role::Fast, 0, o, 10_000); }
        assert_eq!(state.current().state, types::ExecutionAdmissionState::Healthy);
        let mut samples = Vec::with_capacity(N);
        for index in 0..N {
            let now = 10_000 + index as u64;
            let start = Instant::now();
            if $update { o.sequence += 1; o.observed_at_ns = now; state.observe(Role::Fast, 0, black_box(o), now); }
            for slot in 0..4 { assert!(black_box(&mut state).lane_place_allowed(black_box(slot), black_box(now))); }
            samples.push(start.elapsed().as_nanos() as u64);
        }
        samples.sort_unstable();
        println!("{{\"version\":\"{}\",\"with_observation\":{},\"boundary\":\"four_fast_candidate_checks\",\"n\":{N},\"median_ns\":{},\"p99_ns\":{},\"p999_ns\":{},\"maximum_ns\":{},\"queue_depth\":0,\"overflow\":0}}", stringify!($module), $update, samples[N/2], samples[N*99/100-1], samples[N*999/1000-1], samples[N-1]);
    }};
}
fn main() {
    #[cfg(target_os = "linux")]
    unsafe {
        let mut mask: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(0, &mut mask);
        assert_eq!(
            libc::sched_setaffinity(0, std::mem::size_of_val(&mask), &mask),
            0
        );
    }
    bench!(baseline, false);
    bench!(candidate, false);
    bench!(baseline, true);
    bench!(candidate, true);
}
