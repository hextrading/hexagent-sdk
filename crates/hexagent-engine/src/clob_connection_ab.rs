//! Bounded, opt-in crossover experiment owned by each Fast connection owner.
//! Only completed natural business attempts advance the schedule. No requests,
//! retries, health evidence, timers or new workers are manufactured here.
const SECOND: u64 = 1_000_000_000;
const BLOCK: u64 = 300 * SECOND;

pub(crate) struct Rotation {
    start: u64,
    end: u64,
    slot: usize,
    generation: u64,
    last_attempt: u64,
    count: u8,
    last_rotation: u64,
}

impl Rotation {
    /// Startup only: account,start_unix_seconds,end_unix_seconds. Absolute expiry
    /// prevents a process restart from silently extending/restarting the study.
    pub(crate) fn from_env(account: &str) -> Option<Self> {
        let value = std::env::var("HEXBOT_CLOB_CONNECTION_AB").ok()?;
        let mut parts = value.split(',');
        let target = parts.next().expect("AB account");
        let start = parts.next().and_then(|v| v.parse::<u64>().ok()).expect("AB start seconds");
        let end = parts.next().and_then(|v| v.parse::<u64>().ok()).expect("AB end seconds");
        assert!(parts.next().is_none() && !target.is_empty() && end > start
            && end - start <= 3600 && end <= u64::MAX / SECOND, "invalid bounded CLOB AB window");
        (target == account).then(|| Self::new(start * SECOND, end * SECOND))
    }

    fn new(start: u64, end: u64) -> Self {
        Self { start, end, slot: 0, generation: u64::MAX, last_attempt: 0,
            count: 0, last_rotation: 0 }
    }

    pub(crate) fn with_slot(mut self, slot: usize) -> Self { self.slot = slot; self }

    pub(crate) fn expired(&self, now: u64) -> bool { now >= self.end }

    /// Called only between commands, after lifecycle delivery. All outcomes
    /// count equally; slow outcomes retain their independent normal repair.
    /// Deduplication and generation fences exclude coalesced/stale observations.
    pub(crate) fn completed(&mut self, now: u64, generation: u64,
        outcome_generation: u64, attempt: u64, quarantined: bool) -> bool {
        if now < self.start {
            self.last_attempt = self.last_attempt.max(attempt);
            return false;
        }
        if now >= self.end { return false; }
        if self.generation != generation {
            self.generation = generation;
            self.count = 0;
        }
        if attempt == 0 || attempt <= self.last_attempt || outcome_generation != generation { return false; }
        self.last_attempt = attempt;
        self.count = self.count.saturating_add(1);
        let rotate_arm = (((now - self.start) / BLOCK) + self.slot as u64) % 2 == 0;
        if quarantined || !rotate_arm || self.count < 3
            || now.saturating_sub(self.last_rotation) < 5 * SECOND { return false; }
        self.count = 0;
        self.last_rotation = now;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn policy(slot: usize) -> Rotation { Rotation::new(100 * SECOND, 700 * SECOND).with_slot(slot) }
    fn done(p: &mut Rotation, sec: u64, gen: u64, attempt: u64) -> bool {
        p.completed(sec * SECOND, gen, gen, attempt, false)
    }
    #[test]
    fn three_completions_and_rate_bound_no_duplicate_or_stale_rotation() {
        let mut p = policy(0);
        assert!(!done(&mut p, 99, 0, 1));
        assert!(!done(&mut p, 100, 0, 1));
        assert!(!done(&mut p, 101, 0, 1));
        assert!(!done(&mut p, 101, 0, 2));
        assert!(!done(&mut p, 102, 0, 3));
        assert!(done(&mut p, 102, 0, 4));
        assert!(!p.completed(103 * SECOND, 1, 0, 5, false));
        assert!(!done(&mut p, 103, 1, 5));
        assert!(!done(&mut p, 104, 1, 6));
        assert!(!done(&mut p, 105, 1, 7));
        assert!(!done(&mut p, 106, 1, 7));
        assert!(done(&mut p, 107, 1, 8));
        assert!(!done(&mut p, 108, 1, 2));
    }
    #[test]
    fn crossover_expiry_and_owner_isolation() {
        let (mut a, mut b) = (policy(0), policy(1));
        for i in 1..=3 {
            assert_eq!(done(&mut a, 100+i, 0, i), i == 3);
            assert!(!done(&mut b, 100+i, 0, i));
        }
        assert!(!done(&mut a, 400, 1, 4));
        assert!(done(&mut b, 400, 0, 4));
        for i in 5..10 { assert!(!done(&mut b, 700, 1, i)); }
    }
    #[test]
    fn quarantine_and_reconnect_never_reuse_old_evidence() {
        let mut p = policy(0);
        for i in 1..=4 { assert!(!p.completed(101*SECOND, 0, 0, i, true)); }
        assert!(!p.completed(102*SECOND, 1, 0, 4, false));
        assert!(!done(&mut p, 103, 1, 5));
        assert!(!done(&mut p, 104, 1, 6));
        assert!(done(&mut p, 105, 1, 7));
    }
    #[test]
    #[ignore = "focused native latency sample"]
    fn benchmark_completed() {
        let mut p = policy(0);
        let mut samples = Vec::with_capacity(200_000);
        for i in 1..=200_000 {
            let t = std::time::Instant::now();
            std::hint::black_box(done(&mut p, 100 + i / 1000, 0, i));
            samples.push(t.elapsed().as_nanos());
        }
        samples.sort_unstable();
        println!("AB completed n={} median_ns={} p99_ns={} p999_ns={} max_ns={} queue_capacity=0 queue_depth=0 overflow=0", samples.len(), samples[100_000], samples[198_000], samples[199_800], samples[199_999]);
    }
}
