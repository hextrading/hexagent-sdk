//! Private-owner-only publication timings. Stack storage and the existing
//! bounded observation FIFO; no formatting, histogram work or new worker.
pub(super) const STAGES: &[&str] = &[
    "polymarket.account.publish.load",
    "polymarket.account.publish.identity_index",
    "polymarket.account.publish.coid_snapshot",
    "polymarket.account.publish.reverse_index",
    "polymarket.account.publish.oid_snapshot",
    "polymarket.account.publish.token_index",
    "polymarket.account.publish.token_snapshot",
    "polymarket.account.publish.swap",
    "polymarket.account.publish.release",
    "polymarket.account.publish.cpu",
    "polymarket.account.publish.off_cpu",
];

pub(super) struct Observation {
    start: crate::latency::Instant,
    previous: crate::latency::Instant,
    cpu_start: u64,
    phases: [u64; 9],
}

impl Observation {
    pub(super) fn new() -> Self {
        let start = crate::latency::Instant::now();
        Self {
            start,
            previous: start,
            cpu_start: crate::latency::thread_cpu_ns(),
            phases: [0; 9],
        }
    }

    pub(super) fn mark(&mut self, phase: usize) {
        let now = crate::latency::Instant::now();
        self.phases[phase] = now.duration_since(self.previous).as_nanos() as u64;
        self.previous = now;
    }

    pub(super) fn finish(self) {
        // Stop both clocks before emitting any observations. The unchanged
        // enclosing owner_register_publish includes this observer overhead.
        let cpu_end = crate::latency::thread_cpu_ns();
        let elapsed = self.start.elapsed().as_nanos() as u64;
        for (stage, ns) in STAGES.iter().zip(self.phases) {
            crate::latency::observe_ns(stage, ns);
        }
        if self.cpu_start != 0 && cpu_end >= self.cpu_start {
            let cpu = cpu_end - self.cpu_start;
            crate::latency::observe_ns(STAGES[9], cpu);
            crate::latency::observe_ns(STAGES[10], elapsed.saturating_sub(cpu));
        }
    }
}
